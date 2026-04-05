# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""CatalogProvider protocol — interface for data catalog integrations."""

from __future__ import annotations

from typing import Any, Protocol

from lineage_bridge.models.graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


class CatalogProvider(Protocol):
    """Interface for data catalog integrations."""

    catalog_type: str  # e.g. "UNITY_CATALOG", "AWS_GLUE"
    node_type: NodeType  # The NodeType this provider creates
    system_type: SystemType  # The SystemType for nodes

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog node + MATERIALIZES edge."""
        ...

    async def enrich(self, graph: LineageGraph) -> None:
        """Enrich existing catalog nodes with metadata from the catalog's API."""
        ...

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link URL to this node in the catalog's UI."""
        ...
