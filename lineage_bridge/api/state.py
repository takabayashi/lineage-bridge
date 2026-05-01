# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""GraphStore — thin adapter over a `GraphRepository`.

The public API is unchanged from the pre-Phase-1C in-memory implementation
so existing API routers and tests work without modification. Persistence is
delegated to whichever backend the factory builds (see ADR-022 + Phase 1C).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel

from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.storage.backends.memory import MemoryGraphRepository
from lineage_bridge.storage.protocol import GraphMeta, GraphRepository


class GraphEntry(BaseModel):
    """Public metadata wrapper kept for backward compatibility.

    Equivalent to `GraphMeta` from the storage layer; we expose the BaseModel
    flavour here because some routers serialise it via FastAPI.
    """

    graph_id: str
    created_at: datetime
    last_modified: datetime


class GraphSummary(BaseModel):
    """Lightweight graph info for `GET /graphs`."""

    graph_id: str
    node_count: int
    edge_count: int
    pipeline_count: int
    created_at: datetime
    last_modified: datetime


class GraphStore:
    """Manages named LineageGraph instances via a `GraphRepository`."""

    def __init__(self, repo: GraphRepository | None = None) -> None:
        self._repo: GraphRepository = repo or MemoryGraphRepository()

    def create(self, graph: LineageGraph | None = None) -> str:
        """Create a new graph and return its ID."""
        graph_id = str(uuid.uuid4())
        meta = GraphMeta.now(graph_id)
        self._repo.save(graph_id, graph or LineageGraph(), meta)
        return graph_id

    def get(self, graph_id: str) -> LineageGraph | None:
        loaded = self._repo.get(graph_id)
        return loaded[0] if loaded else None

    def get_meta(self, graph_id: str) -> GraphEntry | None:
        loaded = self._repo.get(graph_id)
        if loaded is None:
            return None
        _, meta = loaded
        return GraphEntry(
            graph_id=meta.graph_id,
            created_at=meta.created_at,
            last_modified=meta.last_modified,
        )

    def delete(self, graph_id: str) -> bool:
        return self._repo.delete(graph_id)

    def touch(self, graph_id: str) -> None:
        self._repo.touch(graph_id)

    def list_all(self) -> list[GraphSummary]:
        return [
            GraphSummary(
                graph_id=meta.graph_id,
                node_count=graph.node_count,
                edge_count=graph.edge_count,
                pipeline_count=graph.pipeline_count,
                created_at=meta.created_at,
                last_modified=meta.last_modified,
            )
            for graph, meta in self._repo.list_with_graphs()
        ]

    @property
    def count(self) -> int:
        return self._repo.count()
