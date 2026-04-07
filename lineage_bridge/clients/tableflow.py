# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tableflow API client — extracts topic-to-table materialisation lineage."""

from __future__ import annotations

import logging
from typing import Any

from lineage_bridge.catalogs import get_provider
from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)

# Map API "config.kind" values to catalog registry keys
_KIND_TO_CATALOG_TYPE: dict[str, str] = {
    "Unity": "UNITY_CATALOG",
    "AwsGlue": "AWS_GLUE",
    # Legacy/direct registry keys pass through
    "UNITY_CATALOG": "UNITY_CATALOG",
    "AWS_GLUE": "AWS_GLUE",
}

# Which table formats each catalog type supports
_CATALOG_FORMATS: dict[str, set[str]] = {
    "UNITY_CATALOG": {"DELTA"},
    "AWS_GLUE": {"ICEBERG"},
}


class TableflowClient(ConfluentClient):
    """Extracts Tableflow lineage (topic → table → UC/Glue)."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment_id: str,
        *,
        cluster_ids: list[str] | None = None,
        base_url: str = "https://api.confluent.cloud",
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.environment_id = environment_id
        self._cluster_ids = cluster_ids or []

    # ── helpers ─────────────────────────────────────────────────────────

    def _topic_node_id(self, topic: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic}"

    def _tf_node_id(self, topic: str, cluster_id: str) -> str:
        return f"confluent:tableflow_table:{self.environment_id}:{cluster_id}.{topic}"

    def _uc_node_id(self, catalog: str, cluster_id: str, topic: str) -> str:
        return f"databricks:uc_table:{self.environment_id}:{catalog}.{cluster_id}.{topic}"

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        # 1. Tableflow topics (must query per cluster) --------------------
        tf_topics: list[dict[str, Any]] = []
        for cluster_id in self._cluster_ids:
            cluster_topics = await self._list_tableflow_topics(cluster_id)
            tf_topics.extend(cluster_topics)

        if not tf_topics:
            logger.info("No Tableflow topics in environment %s", self.environment_id)
            return nodes, edges

        # 2. Catalog integrations (UC / Glue) per cluster -----------------
        ci_by_cluster: dict[str, list[dict[str, Any]]] = {}
        for cluster_id in self._cluster_ids:
            integrations = await self._list_catalog_integrations(cluster_id)
            for ci in integrations:
                ci_spec = ci.get("spec", {})
                ci_cluster = ci_spec.get("kafka_cluster", {}).get("id", "")
                if ci_cluster:
                    ci_by_cluster.setdefault(ci_cluster, []).append(ci)

        for tf in tf_topics:
            spec = tf.get("spec", {})
            status = tf.get("status", {})
            # API uses "display_name" for the topic name
            topic_name = spec.get("display_name") or spec.get("topic_name", "")
            cluster_id = spec.get("kafka_cluster", {}).get("id", "")
            if not topic_name or not cluster_id:
                continue

            table_formats = spec.get("table_formats", [])
            storage = spec.get("storage", {})
            storage_kind = storage.get("kind", "")
            table_path = storage.get("table_path", "")
            phase = status.get("phase", "")

            tf_id = self._tf_node_id(topic_name, cluster_id)

            # Tableflow table node.
            nodes.append(
                LineageNode(
                    node_id=tf_id,
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.TABLEFLOW_TABLE,
                    qualified_name=f"{cluster_id}.{topic_name}",
                    display_name=f"{topic_name} (tableflow)",
                    environment_id=self.environment_id,
                    cluster_id=cluster_id,
                    attributes={
                        "table_formats": table_formats,
                        "storage_kind": storage_kind,
                        "table_path": table_path,
                        "phase": phase,
                        "suspended": spec.get("suspended", False),
                    },
                )
            )

            # Edge: kafka_topic → tableflow_table (MATERIALIZES)
            topic_id = self._topic_node_id(topic_name)
            edges.append(
                LineageEdge(
                    src_id=topic_id,
                    dst_id=tf_id,
                    edge_type=EdgeType.MATERIALIZES,
                )
            )

            # 3. Match catalog integrations by table format.
            for ci in ci_by_cluster.get(cluster_id, []):
                ci_kind = ci.get("spec", {}).get("config", {}).get("kind", "")
                ci_type = _KIND_TO_CATALOG_TYPE.get(ci_kind, ci_kind)
                required_formats = _CATALOG_FORMATS.get(ci_type, set())
                # Only build catalog node if topic's formats overlap
                if required_formats and not required_formats.intersection(table_formats):
                    continue
                cat_nodes, cat_edges = self._build_catalog_nodes(ci, tf_id, topic_name, cluster_id)
                nodes.extend(cat_nodes)
                edges.extend(cat_edges)

        logger.info(
            "Tableflow extracted %d nodes, %d edges from environment %s",
            len(nodes),
            len(edges),
            self.environment_id,
        )
        return nodes, edges

    def _build_catalog_nodes(
        self,
        ci: dict[str, Any],
        tf_id: str,
        topic_name: str,
        cluster_id: str,
    ) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        ci_spec = ci.get("spec", {})
        ci_config = ci_spec.get("config", {})
        # API returns kind ("Unity", "AwsGlue") — map to registry keys
        kind = ci_config.get("kind", "") or ci_spec.get("catalog_type", "")
        catalog_type = _KIND_TO_CATALOG_TYPE.get(kind, kind)
        catalog_config = ci_config

        provider = get_provider(catalog_type)
        if provider:
            node, edge = provider.build_node(
                catalog_config, tf_id, topic_name, cluster_id, self.environment_id
            )
            nodes.append(node)
            edges.append(edge)
        else:
            logger.debug("Unknown catalog type %s — skipping", catalog_type)

        return nodes, edges

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_tableflow_topics(self, cluster_id: str) -> list[dict[str, Any]]:
        try:
            return await self.paginate(
                "/tableflow/v1/tableflow-topics",
                params={
                    "environment": self.environment_id,
                    "spec.kafka_cluster": cluster_id,
                },
                page_size=10,
            )
        except Exception:
            logger.debug(
                "Tableflow topics unavailable for %s/%s",
                self.environment_id,
                cluster_id,
            )
            return []

    async def _list_catalog_integrations(self, cluster_id: str) -> list[dict[str, Any]]:
        try:
            return await self.paginate(
                "/tableflow/v1/catalog-integrations",
                params={
                    "environment": self.environment_id,
                    "spec.kafka_cluster": cluster_id,
                },
                page_size=10,
            )
        except Exception:
            logger.debug(
                "Catalog integrations unavailable for %s/%s",
                self.environment_id,
                cluster_id,
            )
            return []
