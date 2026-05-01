# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""CatalogProvider protocol — interface for data catalog integrations.

V2 (per ADR-021): every provider creates `NodeType.CATALOG_TABLE` nodes
discriminated by their `catalog_type` string. The protocol no longer carries
`node_type` / `system_type` class attributes since they collapse to the same
value across all providers — `catalog_type` does the discrimination.

`push_lineage` lives on the protocol so the push service can dispatch
generically. Per-provider option keys are passed via `**options` until a
typed PushOptions discriminated union appears (deferred — current providers
have small, stable option sets).

`build_node` keeps the Tableflow-shaped signature for now. A
MaterializationContext discriminated union (Tableflow / KafkaConnect /
Iceberg / DirectIngest origins) is the natural next step when Snowflake or
Watsonx-via-Iceberg providers land in Phase 3H.
"""

from __future__ import annotations

from typing import Any, Protocol

from lineage_bridge.models.graph import LineageEdge, LineageGraph, LineageNode, PushResult


class CatalogProvider(Protocol):
    """Interface for data catalog integrations.

    All implementations create `NodeType.CATALOG_TABLE` nodes; `catalog_type`
    on the node carries the discriminator.
    """

    catalog_type: str  # e.g. "UNITY_CATALOG", "AWS_GLUE", "GOOGLE_DATA_LINEAGE"

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog node + MATERIALIZES edge from a Tableflow integration."""
        ...

    async def enrich(self, graph: LineageGraph) -> None:
        """Enrich existing catalog nodes with metadata from the catalog's API."""
        ...

    async def push_lineage(self, graph: LineageGraph, **options: Any) -> PushResult:
        """Push lineage metadata from `graph` to this catalog. Options are provider-specific."""
        ...

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link URL to this node in the catalog's UI."""
        ...
