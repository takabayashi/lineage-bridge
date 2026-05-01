# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks Unity Catalog provider."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    PushResult,
    SystemType,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0


def _quote_sql_name(qualified_name: str) -> str:
    """Quote a catalog.schema.table name for SQL, backticking parts with special chars."""
    parts = qualified_name.split(".")
    quoted = []
    for part in parts:
        if not part.replace("_", "").isalnum() or part[0:1].isdigit():
            quoted.append(f"`{part}`")
        else:
            quoted.append(part)
    return ".".join(quoted)


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
        # Support both flat config format (API) and nested format (legacy/tests)
        uc_cfg = ci_config.get("unity_catalog", ci_config)
        catalog_name = uc_cfg.get("catalog_name", "confluent_tableflow")
        # Prefer the user-configured workspace URL (settings) over Confluent's
        # stored workspace_endpoint, which may be stale or wrong.
        workspace_url = (
            self._workspace_url
            or uc_cfg.get("workspace_endpoint")
            or uc_cfg.get("workspace_url")
        )
        # Tableflow normalizes dots → underscores in table names, but keeps
        # the raw cluster ID (with hyphens) as the schema name.
        uc_table_name = topic_name.replace(".", "_")
        qualified = f"{catalog_name}.{cluster_id}.{uc_table_name}"
        uc_id = f"databricks:uc_table:{environment_id}:{qualified}"

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
                "table_name": uc_table_name,
                "workspace_url": workspace_url,
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=uc_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata and lineage from the Databricks UC REST API."""
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

            # Discover downstream tables via Databricks lineage API
            await self._discover_lineage(client, graph, uc_nodes)

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

    async def _discover_lineage(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        seed_nodes: list[LineageNode],
    ) -> None:
        """Walk downstream lineage from seed UC nodes to discover derived tables."""
        seen_qualified: set[str] = {n.qualified_name for n in seed_nodes}
        to_visit: list[LineageNode] = list(seed_nodes)

        while to_visit:
            node = to_visit.pop()
            full_name = node.qualified_name
            url = "/api/2.0/lineage-tracking/table-lineage"

            try:
                resp = await client.get(url, params={"table_name": full_name})
            except httpx.HTTPError:
                logger.debug("Lineage API error for %s", full_name)
                continue

            if resp.status_code != 200:
                logger.debug("Lineage API %d for %s", resp.status_code, full_name)
                continue

            data = resp.json()
            for entry in data.get("downstreams", []):
                ti = entry.get("tableInfo")
                if not ti:
                    continue
                cat = ti.get("catalog_name", "")
                sch = ti.get("schema_name", "")
                tbl = ti.get("name", "")
                if not (cat and sch and tbl):
                    continue
                qualified = f"{cat}.{sch}.{tbl}"
                if qualified in seen_qualified:
                    continue
                seen_qualified.add(qualified)

                env_id = node.environment_id
                cluster_id = node.cluster_id
                new_id = f"databricks:uc_table:{env_id}:{qualified}"

                new_node = LineageNode(
                    node_id=new_id,
                    system=SystemType.DATABRICKS,
                    node_type=NodeType.UC_TABLE,
                    qualified_name=qualified,
                    display_name=qualified,
                    environment_id=env_id,
                    cluster_id=cluster_id,
                    attributes={
                        "catalog_name": cat,
                        "schema_name": sch,
                        "table_name": tbl,
                        "workspace_url": self._workspace_url,
                        "derived": True,
                    },
                )
                graph.add_node(new_node)
                edge = LineageEdge(
                    src_id=node.node_id,
                    dst_id=new_id,
                    edge_type=EdgeType.TRANSFORMS,
                )
                try:
                    graph.add_edge(edge)
                except ValueError:
                    logger.debug("Skipping lineage edge %s -> %s", node.node_id, new_id)

                # Enrich the new node with table metadata
                await self._enrich_node(client, graph, new_node)
                # Continue walking downstream
                to_visit.append(new_node)

                logger.info(
                    "Discovered derived UC table: %s (downstream of %s)",
                    qualified,
                    full_name,
                )

    async def push_lineage(
        self,
        graph: LineageGraph,
        sql_client: Any,
        *,
        set_properties: bool = True,
        set_comments: bool = True,
        create_bridge_table: bool = False,
        bridge_table_name: str | None = None,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Push Confluent lineage metadata to UC tables via SQL.

        Uses the Databricks Statement Execution API to set table properties,
        comments, and optionally create a lineage bridge table.
        """
        from lineage_bridge.clients.databricks_sql import DatabricksSQLClient

        assert isinstance(sql_client, DatabricksSQLClient)
        result = PushResult()

        uc_nodes = [
            n for n in graph.filter_by_type(NodeType.UC_TABLE) if n.system == SystemType.DATABRICKS
        ]
        if not uc_nodes:
            return result

        if on_progress:
            on_progress("Push", f"Found {len(uc_nodes)} UC tables to update")

        # Derive bridge table name from the first UC node's catalog
        if create_bridge_table:
            if not bridge_table_name:
                catalog_name = uc_nodes[0].attributes.get("catalog_name", "")
                if catalog_name:
                    bridge_table_name = f"{catalog_name}.default.confluent_lineage"
                else:
                    bridge_table_name = "default.confluent_lineage"
            await self._create_bridge_table(sql_client, bridge_table_name, result)

        now = datetime.now(UTC).isoformat()

        for node in uc_nodes:
            upstream = graph.get_upstream(node.node_id)
            if not upstream:
                continue

            if set_properties:
                await self._set_table_properties(sql_client, node, upstream, graph, now, result)

            if set_comments:
                await self._set_table_comment(sql_client, node, upstream, graph, now, result)

            if create_bridge_table:
                await self._insert_bridge_rows(
                    sql_client, bridge_table_name, node, upstream, graph, now, result
                )

            result.tables_updated += 1
            if on_progress:
                on_progress("Push", f"Updated {node.qualified_name}")

        if on_progress:
            on_progress("Push", f"Done — {result.tables_updated} tables updated")

        return result

    async def _set_table_properties(
        self,
        sql_client: Any,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Set lineage_bridge.* table properties on a UC table."""
        from lineage_bridge.catalogs.upstream_chain import build_upstream_chain, chain_to_json

        source_topics = []
        source_connectors = []
        for up_node, _edge, _hop in upstream:
            if up_node.node_type == NodeType.KAFKA_TOPIC:
                source_topics.append(up_node.qualified_name)
            elif up_node.node_type == NodeType.CONNECTOR:
                source_connectors.append(up_node.display_name)

        # Rich multi-hop chain (Flink/ksqlDB/intermediate topics/external sources).
        # Capped at ~3 KB to leave headroom under Databricks' 4 KB per-property limit.
        chain = build_upstream_chain(graph, node.node_id)
        chain_json, truncated = chain_to_json(chain, max_bytes=3 * 1024)

        props = {
            "lineage_bridge.source_topics": ",".join(source_topics) if source_topics else "",
            "lineage_bridge.source_connectors": (
                ",".join(source_connectors) if source_connectors else ""
            ),
            "lineage_bridge.upstream_chain": chain_json if chain else "",
            "lineage_bridge.upstream_truncated": "true" if truncated else "",
            "lineage_bridge.pipeline_type": "tableflow",
            "lineage_bridge.last_synced": now,
            "lineage_bridge.environment_id": node.environment_id or "",
            "lineage_bridge.cluster_id": node.cluster_id or "",
        }

        # Escape single quotes inside SQL string literals.
        prop_pairs = ", ".join(
            f"'{k}' = '{v.replace(chr(39), chr(39) * 2)}'" for k, v in props.items() if v
        )
        if not prop_pairs:
            return

        table_ref = _quote_sql_name(node.qualified_name)
        sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({prop_pairs})"
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.properties_set += 1
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                result.errors.append(f"Properties failed for {node.qualified_name}: {error}")
        except Exception as exc:
            result.errors.append(f"Properties error for {node.qualified_name}: {exc}")

    async def _set_table_comment(
        self,
        sql_client: Any,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Set a human-readable lineage comment on a UC table."""
        from lineage_bridge.catalogs.upstream_chain import (
            build_upstream_chain,
            format_chain_summary,
        )

        chain = build_upstream_chain(graph, node.node_id)

        lines = []
        summary = format_chain_summary(chain, node.display_name or node.qualified_name)
        if summary:
            lines.append(summary)
        if node.environment_id:
            lines.append(f"Environment: {node.environment_id}")
        lines.append(f"Last synced: {now}")
        lines.append("Managed by LineageBridge")

        comment_text = "\\n".join(lines).replace("'", "\\'")
        table_ref = _quote_sql_name(node.qualified_name)
        sql = f"COMMENT ON TABLE {table_ref} IS '{comment_text}'"

        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.comments_set += 1
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                result.errors.append(f"Comment failed for {node.qualified_name}: {error}")
        except Exception as exc:
            result.errors.append(f"Comment error for {node.qualified_name}: {exc}")

    async def _create_bridge_table(
        self, sql_client: Any, bridge_table_name: str, result: PushResult
    ) -> None:
        """Create the lineage bridge table if it doesn't exist.

        ``chain_json`` carries the full upstream chain (Flink/ksqlDB SQL,
        intermediate topics, connector classes, schema fields per topic) so
        downstream queries can drill into the pipeline without joining back
        to LineageBridge.
        """
        sql = f"""CREATE TABLE IF NOT EXISTS {bridge_table_name} (
    uc_table STRING,
    source_type STRING,
    source_name STRING,
    source_system STRING,
    edge_type STRING,
    environment_id STRING,
    cluster_id STRING,
    hop_distance INT,
    full_path STRING,
    chain_json STRING,
    synced_at TIMESTAMP
)"""
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") != "SUCCEEDED":
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                result.errors.append(f"Bridge table creation failed: {error}")
        except Exception as exc:
            result.errors.append(f"Bridge table creation error: {exc}")

    async def _insert_bridge_rows(
        self,
        sql_client: Any,
        bridge_table_name: str,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Insert lineage rows into the bridge table.

        Each row gets the same ``chain_json`` payload so the full pipeline is
        queryable from any single hop without re-walking the graph.
        """
        from lineage_bridge.catalogs.upstream_chain import build_upstream_chain, chain_to_json

        chain = build_upstream_chain(graph, node.node_id)
        chain_json, _ = chain_to_json(chain)
        chain_json_sql = chain_json.replace("'", "''")

        values = []
        for up_node, edge, hop in upstream:
            # Use raw qualified_name in VALUES (string literal, no quoting needed)
            values.append(
                f"('{node.qualified_name}', '{up_node.node_type.value}', "
                f"'{up_node.qualified_name}', '{up_node.system.value}', "
                f"'{edge.edge_type.value}', '{node.environment_id or ''}', "
                f"'{node.cluster_id or ''}', {hop}, '', '{chain_json_sql}', TIMESTAMP '{now}')"
            )
        if not values:
            return

        values_str = ",\n".join(values)
        bridge_ref = _quote_sql_name(bridge_table_name)
        sql = f"INSERT INTO {bridge_ref} VALUES {values_str}"
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.bridge_rows_inserted += len(values)
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                result.errors.append(f"Bridge insert failed for {node.qualified_name}: {error}")
        except Exception as exc:
            result.errors.append(f"Bridge insert error for {node.qualified_name}: {exc}")

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the table in the Databricks workspace UI."""
        # Prefer the provider's configured workspace URL (from settings) over
        # the per-node attribute, which may carry a stale value baked in by
        # Confluent's Tableflow integration config.
        workspace_url = self._workspace_url or node.attributes.get("workspace_url")
        if not workspace_url:
            return None
        parts = node.qualified_name.split(".")
        if len(parts) != 3:
            return None
        catalog, schema, table = parts
        return f"{workspace_url.rstrip('/')}/explore/data/{catalog}/{schema}/{table}"
