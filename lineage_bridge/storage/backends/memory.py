# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""In-memory backend — the default for tests and ephemeral local runs.

Process-local: state is lost on restart. For multi-worker deployments or
durable storage, switch to the `file` backend (or, once Phase 2F lands,
sqlite).
"""

from __future__ import annotations

from datetime import UTC, datetime

from lineage_bridge.api.task_store import TaskInfo
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.openlineage.models import RunEvent
from lineage_bridge.storage.protocol import GraphMeta


class MemoryGraphRepository:
    """In-memory `GraphRepository`."""

    def __init__(self) -> None:
        self._graphs: dict[str, LineageGraph] = {}
        self._meta: dict[str, GraphMeta] = {}

    def save(self, graph_id: str, graph: LineageGraph, meta: GraphMeta) -> None:
        self._graphs[graph_id] = graph
        self._meta[graph_id] = meta

    def get(self, graph_id: str) -> tuple[LineageGraph, GraphMeta] | None:
        graph = self._graphs.get(graph_id)
        if graph is None:
            return None
        return graph, self._meta[graph_id]

    def list_meta(self) -> list[GraphMeta]:
        return list(self._meta.values())

    def list_with_graphs(self) -> list[tuple[LineageGraph, GraphMeta]]:
        return [(self._graphs[gid], self._meta[gid]) for gid in self._graphs]

    def delete(self, graph_id: str) -> bool:
        if graph_id in self._graphs:
            del self._graphs[graph_id]
            del self._meta[graph_id]
            return True
        return False

    def touch(self, graph_id: str) -> None:
        meta = self._meta.get(graph_id)
        if meta is not None:
            meta.last_modified = datetime.now(UTC)

    def count(self) -> int:
        return len(self._graphs)


class MemoryTaskRepository:
    """In-memory `TaskRepository`."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskInfo] = {}

    def save(self, task: TaskInfo) -> None:
        self._tasks[task.task_id] = task

    def get(self, task_id: str) -> TaskInfo | None:
        return self._tasks.get(task_id)

    def list(self) -> list[TaskInfo]:
        return list(self._tasks.values())

    def delete(self, task_id: str) -> bool:
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def count(self) -> int:
        return len(self._tasks)


class MemoryEventRepository:
    """In-memory append-only `EventRepository`."""

    def __init__(self) -> None:
        self._events: list[RunEvent] = []
        self._by_run_id: dict[str, list[RunEvent]] = {}

    def add(self, events: list[RunEvent]) -> int:
        for event in events:
            self._events.append(event)
            self._by_run_id.setdefault(event.run.runId, []).append(event)
        return len(events)

    def all(self) -> list[RunEvent]:
        return list(self._events)

    def by_run_id(self, run_id: str) -> list[RunEvent]:
        return list(self._by_run_id.get(run_id, []))

    def count(self) -> int:
        return len(self._events)

    def clear(self) -> None:
        self._events.clear()
        self._by_run_id.clear()
