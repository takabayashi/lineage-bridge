# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""In-memory store for OpenLineage events."""

from __future__ import annotations

from datetime import datetime
from fnmatch import fnmatch

from lineage_bridge.api.openlineage.models import Dataset, Job, RunEvent


class EventStore:
    """Stores and queries OpenLineage RunEvents in memory.

    Events are indexed for fast lookup by namespace, job, dataset, and run ID.
    """

    def __init__(self) -> None:
        self._events: list[RunEvent] = []
        self._by_run_id: dict[str, list[RunEvent]] = {}

    def store_events(self, events: list[RunEvent]) -> int:
        """Store events, returning the count stored."""
        for event in events:
            self._events.append(event)
            run_id = event.run.runId
            self._by_run_id.setdefault(run_id, []).append(event)
        return len(events)

    def get_by_run_id(self, run_id: str) -> list[RunEvent]:
        return self._by_run_id.get(run_id, [])

    def query_events(
        self,
        *,
        namespace: str | None = None,
        job_name: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunEvent]:
        """Query events with optional filters. Supports glob patterns for namespace."""
        results = self._events

        if namespace:
            results = [e for e in results if fnmatch(e.job.namespace, namespace)]
        if job_name:
            results = [e for e in results if fnmatch(e.job.name, job_name)]
        if since:
            results = [e for e in results if e.eventTime >= since]
        if until:
            results = [e for e in results if e.eventTime <= until]

        return results[offset : offset + limit]

    def get_datasets(
        self,
        *,
        namespace: str | None = None,
        name: str | None = None,
    ) -> list[Dataset]:
        """Get unique datasets from all events."""
        seen: dict[tuple[str, str], Dataset] = {}

        for event in self._events:
            for ds in [*event.inputs, *event.outputs]:
                key = (ds.namespace, ds.name)
                if key in seen:
                    continue
                if namespace and not fnmatch(ds.namespace, namespace):
                    continue
                if name and not fnmatch(ds.name, name):
                    continue
                seen[key] = Dataset(namespace=ds.namespace, name=ds.name, facets=ds.facets)

        return list(seen.values())

    def get_jobs(
        self,
        *,
        namespace: str | None = None,
        name: str | None = None,
    ) -> list[Job]:
        """Get unique jobs from all events."""
        seen: dict[tuple[str, str], Job] = {}

        for event in self._events:
            key = (event.job.namespace, event.job.name)
            if key in seen:
                continue
            if namespace and not fnmatch(event.job.namespace, namespace):
                continue
            if name and not fnmatch(event.job.name, name):
                continue
            seen[key] = event.job

        return list(seen.values())

    @property
    def event_count(self) -> int:
        return len(self._events)

    def clear(self) -> None:
        self._events.clear()
        self._by_run_id.clear()
