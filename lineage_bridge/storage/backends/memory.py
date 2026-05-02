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
from lineage_bridge.services.watcher_models import (
    ExtractionRecord,
    WatcherConfig,
    WatcherEvent,
    WatcherStatus,
    WatcherSummary,
)
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


class MemoryWatcherRepository:
    """In-memory `WatcherRepository` (Phase 2G).

    State is process-local — fine for tests and single-process API runs.
    Multi-process / restart-survives use cases need the SQLite backend.
    """

    def __init__(self) -> None:
        self._configs: dict[str, WatcherConfig] = {}
        self._statuses: dict[str, WatcherStatus] = {}
        self._events: dict[str, list[WatcherEvent]] = {}
        self._extractions: dict[str, list[ExtractionRecord]] = {}

    def register(self, watcher_id: str, config: WatcherConfig) -> None:
        self._configs[watcher_id] = config
        self._events.setdefault(watcher_id, [])
        self._extractions.setdefault(watcher_id, [])

    def get_config(self, watcher_id: str) -> WatcherConfig | None:
        return self._configs.get(watcher_id)

    def update_status(self, watcher_id: str, status: WatcherStatus) -> None:
        self._statuses[watcher_id] = status

    def get_status(self, watcher_id: str) -> WatcherStatus | None:
        return self._statuses.get(watcher_id)

    def append_event(self, watcher_id: str, event: WatcherEvent) -> None:
        self._events.setdefault(watcher_id, []).append(event)

    def list_events(
        self,
        watcher_id: str,
        *,
        limit: int = 100,
        since: datetime | None = None,
    ) -> list[WatcherEvent]:
        all_events = self._events.get(watcher_id, [])
        if since is not None:
            all_events = [e for e in all_events if e.time > since]
        # The list was appended in chronological order — `reversed()` then
        # `[:limit]` yields the newest `limit` entries without sorting.
        return list(reversed(all_events))[:limit]

    def append_extraction(self, watcher_id: str, record: ExtractionRecord) -> None:
        self._extractions.setdefault(watcher_id, []).append(record)

    def list_extractions(
        self,
        watcher_id: str,
        *,
        limit: int = 50,
    ) -> list[ExtractionRecord]:
        return list(reversed(self._extractions.get(watcher_id, [])))[:limit]

    def list_watchers(self) -> list[WatcherSummary]:
        out: list[WatcherSummary] = []
        for wid, config in self._configs.items():
            status = self._statuses.get(wid)
            out.append(
                WatcherSummary(
                    watcher_id=wid,
                    state=status.state if status else "stopped",
                    started_at=status.started_at if status else None,
                    environment_ids=list(config.extraction.environment_ids),
                    poll_count=status.poll_count if status else 0,
                    event_count=status.event_count if status else 0,
                )
            )
        return out

    def deregister(self, watcher_id: str) -> bool:
        if watcher_id not in self._configs:
            return False
        self._configs.pop(watcher_id, None)
        self._statuses.pop(watcher_id, None)
        self._events.pop(watcher_id, None)
        self._extractions.pop(watcher_id, None)
        return True
