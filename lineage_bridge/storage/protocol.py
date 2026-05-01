# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Repository protocols for the storage layer.

Three protocols, one per entity:

  - `GraphRepository`  — UUID -> (LineageGraph, GraphMeta)
  - `TaskRepository`   — UUID -> TaskInfo (Pydantic, JSON-serialisable)
  - `EventRepository`  — append-only RunEvent log, with by-run lookup

The protocols are **synchronous** even though ADR-022 originally specified
async. Reason: the existing `GraphStore` / `TaskStore` / `EventStore` public
APIs are sync, and many tests + routers call them as plain methods. Flipping
those to `await` would touch ~20 call sites and force async changes through
code that has no real I/O latency to amortise (memory + small JSON files).
When the SQLite backend lands in Phase 2F, the repository call sites are
already off the request hot path so the cost of `asyncio.to_thread` wrap if
needed is bounded to one helper. Postgres-async (Phase 3+) would warrant
revisiting this choice.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from lineage_bridge.api.task_store import TaskInfo
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.openlineage.models import RunEvent


@dataclass
class GraphMeta:
    """Metadata stored alongside each LineageGraph."""

    graph_id: str
    created_at: datetime
    last_modified: datetime

    @classmethod
    def now(cls, graph_id: str) -> GraphMeta:
        n = datetime.now(UTC)
        return cls(graph_id=graph_id, created_at=n, last_modified=n)


# ── graphs ──────────────────────────────────────────────────────────────


class GraphRepository(Protocol):
    """Persistence for `LineageGraph` instances + their metadata."""

    def save(self, graph_id: str, graph: LineageGraph, meta: GraphMeta) -> None: ...
    def get(self, graph_id: str) -> tuple[LineageGraph, GraphMeta] | None: ...
    def list_meta(self) -> list[GraphMeta]: ...
    def list_with_graphs(self) -> list[tuple[LineageGraph, GraphMeta]]:
        """Return every (graph, meta) pair in one pass.

        Exists so `GraphStore.list_all()` doesn't have to call `list_meta()`
        + `get()` for each graph (which doubles file I/O on the file backend).
        """
        ...

    def delete(self, graph_id: str) -> bool: ...
    def touch(self, graph_id: str) -> None: ...
    def count(self) -> int: ...


# ── tasks ───────────────────────────────────────────────────────────────


class TaskRepository(Protocol):
    """Persistence for async `TaskInfo` records.

    The store layer (`TaskStore`) owns filtering/sorting/limiting; the repo
    just persists. Backends that grow real query support (SQLite) can
    override `list` to push filters down later — see ADR-022 future work.
    """

    def save(self, task: TaskInfo) -> None: ...
    def get(self, task_id: str) -> TaskInfo | None: ...
    def list(self) -> list[TaskInfo]: ...
    def delete(self, task_id: str) -> bool: ...
    def count(self) -> int: ...


# ── events ──────────────────────────────────────────────────────────────


class EventRepository(Protocol):
    """Append-only log of OpenLineage RunEvents.

    Indexed by `run.runId` for fast per-run lookup; the store layer does
    namespace/job/time filtering across `all()`.
    """

    def add(self, events: list[RunEvent]) -> int: ...
    def all(self) -> list[RunEvent]: ...
    def by_run_id(self, run_id: str) -> list[RunEvent]: ...
    def count(self) -> int: ...
    def clear(self) -> None: ...
