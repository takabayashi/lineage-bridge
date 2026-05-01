# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Dataset query and traversal endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.schemas import DatasetLineageOrigin, DatasetLineageResponse
from lineage_bridge.openlineage.models import Dataset, RunEvent
from lineage_bridge.openlineage.store import EventStore

router = APIRouter()


def _get_event_store(request: Request) -> EventStore:
    return request.app.state.event_store


@router.get("/datasets", dependencies=[Depends(require_api_key)])
async def list_datasets(
    namespace: str | None = None,
    name: str | None = None,
    store: EventStore = Depends(_get_event_store),
) -> list[Dataset]:
    """List datasets. Supports glob patterns for namespace and name."""
    return store.get_datasets(namespace=namespace, name=name)


@router.get("/datasets/detail", dependencies=[Depends(require_api_key)])
async def get_dataset(
    namespace: str,
    name: str,
    store: EventStore = Depends(_get_event_store),
) -> Dataset:
    """Get a specific dataset by namespace and name (query params)."""
    datasets = store.get_datasets(namespace=namespace, name=name)
    if not datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {namespace}/{name} not found")
    return datasets[0]


@router.get(
    "/datasets/lineage",
    dependencies=[Depends(require_api_key)],
)
async def get_dataset_lineage(
    namespace: str,
    name: str,
    direction: str = Query("upstream", pattern="^(upstream|downstream|both)$"),
    depth: int = Query(5, ge=1, le=50),
    store: EventStore = Depends(_get_event_store),
) -> DatasetLineageResponse:
    """Traverse lineage for a dataset.

    Returns the chain of jobs and datasets connected to this dataset
    in the specified direction, up to the given depth.
    """
    # Build a traversal from the event store
    visited_datasets: set[tuple[str, str]] = set()
    visited_jobs: set[tuple[str, str]] = set()
    result_events: list[RunEvent] = []

    frontier: set[tuple[str, str]] = {(namespace, name)}

    for _ in range(depth):
        next_frontier: set[tuple[str, str]] = set()
        for ds_ns, ds_name in frontier:
            if (ds_ns, ds_name) in visited_datasets:
                continue
            visited_datasets.add((ds_ns, ds_name))

            for event in store.query_events(limit=10000):
                job_key = (event.job.namespace, event.job.name)

                if direction in ("upstream", "both"):
                    for out in event.outputs:
                        if (
                            out.namespace == ds_ns
                            and out.name == ds_name
                            and job_key not in visited_jobs
                        ):
                            visited_jobs.add(job_key)
                            result_events.append(event)
                            for inp in event.inputs:
                                next_frontier.add((inp.namespace, inp.name))

                if direction in ("downstream", "both"):
                    for inp in event.inputs:
                        if (
                            inp.namespace == ds_ns
                            and inp.name == ds_name
                            and job_key not in visited_jobs
                        ):
                            visited_jobs.add(job_key)
                            result_events.append(event)
                            for out in event.outputs:
                                next_frontier.add((out.namespace, out.name))

        frontier = next_frontier - visited_datasets
        if not frontier:
            break

    return DatasetLineageResponse(
        origin=DatasetLineageOrigin(namespace=namespace, name=name),
        direction=direction,
        depth=depth,
        events=result_events,
        datasets_visited=len(visited_datasets),
        jobs_visited=len(visited_jobs),
    )
