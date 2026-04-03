"""Protocol defining the interface all lineage extractors must implement."""

from __future__ import annotations

from typing import Protocol

from lineage_bridge.models.graph import LineageEdge, LineageNode


class LineageExtractor(Protocol):
    """Interface for API-specific lineage extractors.

    Each extractor is responsible for one Confluent Cloud API surface
    (e.g., Kafka Admin, Connect, Schema Registry, Flink, ksqlDB, Tableflow).
    It queries that API and returns the discovered nodes and edges.
    """

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Extract nodes and edges from this API surface.

        Returns:
            A tuple of (nodes, edges) discovered by this extractor.
        """
        ...
