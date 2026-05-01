# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""In-memory state management for the REST API."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel

from lineage_bridge.models.graph import LineageGraph


class GraphEntry(BaseModel):
    """Wrapper around a LineageGraph with metadata."""

    graph_id: str
    created_at: datetime
    last_modified: datetime

    model_config = {"arbitrary_types_allowed": True}


class GraphSummary(BaseModel):
    """Lightweight graph info for listing."""

    graph_id: str
    node_count: int
    edge_count: int
    pipeline_count: int
    created_at: datetime
    last_modified: datetime


class GraphStore:
    """Manages named LineageGraph instances in memory."""

    def __init__(self) -> None:
        self._graphs: dict[str, LineageGraph] = {}
        self._meta: dict[str, GraphEntry] = {}

    def create(self, graph: LineageGraph | None = None) -> str:
        """Create a new graph and return its ID."""
        graph_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        self._graphs[graph_id] = graph or LineageGraph()
        self._meta[graph_id] = GraphEntry(graph_id=graph_id, created_at=now, last_modified=now)
        return graph_id

    def get(self, graph_id: str) -> LineageGraph | None:
        return self._graphs.get(graph_id)

    def get_meta(self, graph_id: str) -> GraphEntry | None:
        return self._meta.get(graph_id)

    def delete(self, graph_id: str) -> bool:
        if graph_id in self._graphs:
            del self._graphs[graph_id]
            del self._meta[graph_id]
            return True
        return False

    def touch(self, graph_id: str) -> None:
        """Update last_modified timestamp."""
        if graph_id in self._meta:
            self._meta[graph_id].last_modified = datetime.now(UTC)

    def list_all(self) -> list[GraphSummary]:
        result = []
        for gid, graph in self._graphs.items():
            meta = self._meta[gid]
            result.append(
                GraphSummary(
                    graph_id=gid,
                    node_count=graph.node_count,
                    edge_count=graph.edge_count,
                    pipeline_count=graph.pipeline_count,
                    created_at=meta.created_at,
                    last_modified=meta.last_modified,
                )
            )
        return result

    @property
    def count(self) -> int:
        return len(self._graphs)
