# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""EventStore — thin adapter over an `EventRepository`.

Public API unchanged from the pre-Phase-1C in-memory implementation so the
API routers (`api/routers/lineage.py`, `api/routers/datasets.py`,
`api/routers/jobs.py`) need no changes. The repository owns persistence;
the store owns query semantics (filters, dataset/job uniqueness).
"""

from __future__ import annotations

from datetime import datetime
from fnmatch import fnmatch
from typing import TYPE_CHECKING

from lineage_bridge.openlineage.models import Dataset, Job, RunEvent

if TYPE_CHECKING:
    from lineage_bridge.storage.protocol import EventRepository


class EventStore:
    """Stores and queries OpenLineage RunEvents via an `EventRepository`."""

    def __init__(self, repo: EventRepository | None = None) -> None:
        from lineage_bridge.storage.backends.memory import MemoryEventRepository

        self._repo: EventRepository = repo or MemoryEventRepository()

    def store_events(self, events: list[RunEvent]) -> int:
        """Store events, returning the count stored."""
        return self._repo.add(events)

    def get_by_run_id(self, run_id: str) -> list[RunEvent]:
        return self._repo.by_run_id(run_id)

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
        results = self._repo.all()

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

        for event in self._repo.all():
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

        for event in self._repo.all():
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
        return self._repo.count()

    def clear(self) -> None:
        self._repo.clear()
