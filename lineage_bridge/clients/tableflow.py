"""Tableflow API client — extracts topic-to-table materialisation lineage."""

from __future__ import annotations

import logging
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)


class TableflowClient(ConfluentClient):
    """Extracts Tableflow lineage (topic → table → UC/Glue)."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment_id: str,
        *,
        base_url: str = "https://api.confluent.cloud",
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.environment_id = environment_id

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

        # 1. Tableflow topics -------------------------------------------
        tf_topics = await self._list_tableflow_topics()
        if not tf_topics:
            logger.info("No Tableflow topics in environment %s", self.environment_id)
            return nodes, edges

        # 2. Catalog integrations (UC / Glue) ----------------------------
        catalog_integrations = await self._list_catalog_integrations()
        # Index by cluster ID for lookup.
        ci_by_cluster: dict[str, dict[str, Any]] = {}
        for ci in catalog_integrations:
            ci_spec = ci.get("spec", {})
            ci_cluster = ci_spec.get("kafka_cluster", {}).get("id", "")
            if ci_cluster:
                ci_by_cluster[ci_cluster] = ci

        for tf in tf_topics:
            spec = tf.get("spec", {})
            status = tf.get("status", {})
            topic_name = spec.get("topic_name", "")
            cluster_id = spec.get("kafka_cluster", {}).get("id", "")
            if not topic_name or not cluster_id:
                continue

            table_formats = spec.get("table_formats", [])
            storage = spec.get("storage", {})
            storage_location = status.get("storage_location", "")

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
                        "storage_provider": storage.get("provider"),
                        "bucket_name": storage.get("bucket_name"),
                        "storage_location": storage_location,
                        "phase": status.get("phase"),
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

            # 3. If a catalog integration exists, create UC/Glue node.
            ci = ci_by_cluster.get(cluster_id)
            if ci:
                uc_nodes, uc_edges = self._build_catalog_nodes(ci, tf_id, topic_name, cluster_id)
                nodes.extend(uc_nodes)
                edges.extend(uc_edges)

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
        catalog_type = ci_spec.get("catalog_type", "")
        catalog_config = ci_spec.get("catalog_config", {})

        if catalog_type == "UNITY_CATALOG":
            uc_cfg = catalog_config.get("unity_catalog", {})
            catalog_name = uc_cfg.get("catalog_name", "confluent_tableflow")
            qualified = f"{catalog_name}.{cluster_id}.{topic_name}"
            uc_id = self._uc_node_id(catalog_name, cluster_id, topic_name)

            nodes.append(
                LineageNode(
                    node_id=uc_id,
                    system=SystemType.DATABRICKS,
                    node_type=NodeType.UC_TABLE,
                    qualified_name=qualified,
                    display_name=qualified,
                    environment_id=self.environment_id,
                    cluster_id=cluster_id,
                    attributes={
                        "catalog_name": catalog_name,
                        "schema_name": cluster_id,
                        "table_name": topic_name,
                        "workspace_url": uc_cfg.get("workspace_url"),
                    },
                )
            )
            edges.append(
                LineageEdge(
                    src_id=tf_id,
                    dst_id=uc_id,
                    edge_type=EdgeType.MATERIALIZES,
                )
            )

        elif catalog_type == "AWS_GLUE":
            glue_cfg = catalog_config.get("aws_glue", catalog_config)
            database = glue_cfg.get("database_name", cluster_id)
            qualified = f"glue://{database}/{topic_name}"
            glue_id = f"external:uc_table:{self.environment_id}:{qualified}"

            nodes.append(
                LineageNode(
                    node_id=glue_id,
                    system=SystemType.EXTERNAL,
                    node_type=NodeType.UC_TABLE,
                    qualified_name=qualified,
                    display_name=f"{database}.{topic_name} (glue)",
                    environment_id=self.environment_id,
                    cluster_id=cluster_id,
                    attributes={
                        "catalog_type": "AWS_GLUE",
                        "database": database,
                        "table_name": topic_name,
                    },
                )
            )
            edges.append(
                LineageEdge(
                    src_id=tf_id,
                    dst_id=glue_id,
                    edge_type=EdgeType.MATERIALIZES,
                )
            )
        else:
            logger.debug("Unknown catalog type %s — skipping", catalog_type)

        return nodes, edges

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_tableflow_topics(self) -> list[dict[str, Any]]:
        try:
            return await self.paginate(
                "/tableflow/v1/tableflow-topics",
                params={"environment": self.environment_id},
                page_size=10,
            )
        except Exception:
            # 400 typically means Tableflow is not enabled in this environment
            logger.debug(
                "Tableflow topics unavailable for %s (not enabled?)",
                self.environment_id,
            )
            return []

    async def _list_catalog_integrations(self) -> list[dict[str, Any]]:
        try:
            return await self.paginate(
                "/tableflow/v1/catalog-integrations",
                params={"environment": self.environment_id},
                page_size=10,
            )
        except Exception:
            logger.debug(
                "Catalog integrations unavailable for %s",
                self.environment_id,
            )
            return []
