# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""AWS Glue catalog provider."""

from __future__ import annotations

import logging
from typing import Any

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)


class GlueCatalogProvider:
    """CatalogProvider implementation for AWS Glue Data Catalog."""

    catalog_type: str = "AWS_GLUE"
    node_type: NodeType = NodeType.GLUE_TABLE
    system_type: SystemType = SystemType.AWS

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a GLUE_TABLE node and MATERIALIZES edge from the tableflow node."""
        glue_cfg = ci_config.get("aws_glue", ci_config)
        database = glue_cfg.get("database_name", cluster_id)
        qualified = f"glue://{database}/{topic_name}"
        glue_id = f"aws:glue_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=glue_id,
            system=SystemType.AWS,
            node_type=NodeType.GLUE_TABLE,
            qualified_name=qualified,
            display_name=f"{database}.{topic_name} (glue)",
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "catalog_type": "AWS_GLUE",
                "database": database,
                "table_name": topic_name,
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=glue_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """No-op stub — Glue enrichment not yet implemented."""

    def build_url(self, node: LineageNode) -> str | None:
        """Build an AWS Glue console URL for the given table node."""
        database = node.attributes.get("database")
        table_name = node.attributes.get("table_name")
        if not database or not table_name:
            return None
        region = node.attributes.get("aws_region", "us-east-1")
        return (
            f"https://{region}.console.aws.amazon.com/glue/home"
            f"?region={region}#/v2/data-catalog/tables/view/{table_name}"
            f"?database={database}"
        )
