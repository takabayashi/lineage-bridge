# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks Unity Catalog provider."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0


class DatabricksUCProvider:
    """CatalogProvider implementation for Databricks Unity Catalog."""

    catalog_type: str = "UNITY_CATALOG"
    node_type: NodeType = NodeType.UC_TABLE
    system_type: SystemType = SystemType.DATABRICKS

    def __init__(
        self,
        workspace_url: str | None = None,
        token: str | None = None,
    ) -> None:
        self._workspace_url = workspace_url.rstrip("/") if workspace_url else None
        self._token = token

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a UC_TABLE node and MATERIALIZES edge from the tableflow node."""
        uc_cfg = ci_config.get("unity_catalog", {})
        catalog_name = uc_cfg.get("catalog_name", "confluent_tableflow")
        qualified = f"{catalog_name}.{cluster_id}.{topic_name}"
        uc_id = f"databricks:uc_table:{environment_id}:{catalog_name}.{cluster_id}.{topic_name}"

        node = LineageNode(
            node_id=uc_id,
            system=SystemType.DATABRICKS,
            node_type=NodeType.UC_TABLE,
            qualified_name=qualified,
            display_name=qualified,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "catalog_name": catalog_name,
                "schema_name": cluster_id,
                "table_name": topic_name,
                "workspace_url": uc_cfg.get("workspace_url"),
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=uc_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata from the Databricks UC REST API and merge into graph."""
        if not self._workspace_url or not self._token:
            logger.debug("Databricks UC enrichment skipped — no credentials configured")
            return

        uc_nodes = graph.filter_by_type(NodeType.UC_TABLE)
        if not uc_nodes:
            return

        async with httpx.AsyncClient(
            base_url=self._workspace_url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30.0,
        ) as client:
            for node in uc_nodes:
                if node.system != SystemType.DATABRICKS:
                    continue
                await self._enrich_node(client, graph, node)

    async def _enrich_node(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch and merge metadata for a single UC table node."""
        full_name = node.qualified_name  # catalog.schema.table
        url = f"/api/2.1/unity-catalog/tables/{full_name}"

        for attempt in range(_MAX_RETRIES):
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    enriched_attrs = {
                        **node.attributes,
                        "owner": data.get("owner"),
                        "table_type": data.get("table_type"),
                        "columns": data.get("columns"),
                        "storage_location": data.get("storage_location"),
                        "updated_at": data.get("updated_at"),
                    }
                    enriched = node.model_copy(update={"attributes": enriched_attrs})
                    graph.add_node(enriched)
                    return
                if resp.status_code in (401, 403, 404):
                    logger.warning(
                        "Databricks UC API returned %d for %s — skipping",
                        resp.status_code,
                        full_name,
                    )
                    return
                if resp.status_code in (429, 500, 502, 503, 504):
                    delay = _BACKOFF_BASE * (2**attempt)
                    logger.debug(
                        "Databricks UC API %d for %s — retrying in %.1fs",
                        resp.status_code,
                        full_name,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                # Other status codes — log and skip
                logger.warning(
                    "Databricks UC API unexpected %d for %s",
                    resp.status_code,
                    full_name,
                )
                return
            except httpx.HTTPError:
                logger.debug("HTTP error enriching %s (attempt %d)", full_name, attempt + 1)
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(_BACKOFF_BASE * (2**attempt))

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the table in the Databricks workspace UI."""
        workspace_url = node.attributes.get("workspace_url")
        if not workspace_url:
            return None
        parts = node.qualified_name.split(".")
        if len(parts) != 3:
            return None
        catalog, schema, table = parts
        return f"{workspace_url}/explore/data/{catalog}/{schema}/{table}"
