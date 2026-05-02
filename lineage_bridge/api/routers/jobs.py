# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Job query endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.schemas import JobDetailResponse
from lineage_bridge.openlineage.models import Job, RunEvent
from lineage_bridge.openlineage.store import EventStore

router = APIRouter()


def _get_event_store(request: Request) -> EventStore:
    return request.app.state.event_store


@router.get("/jobs", dependencies=[Depends(require_api_key)])
async def list_jobs(
    namespace: str | None = None,
    name: str | None = None,
    store: EventStore = Depends(_get_event_store),
) -> list[Job]:
    """List jobs. Supports glob patterns for namespace and name."""
    return store.get_jobs(namespace=namespace, name=name)


@router.get("/jobs/detail", dependencies=[Depends(require_api_key)])
async def get_job(
    namespace: str,
    name: str,
    store: EventStore = Depends(_get_event_store),
) -> JobDetailResponse:
    """Get a job with its most recent inputs and outputs."""
    jobs = store.get_jobs(namespace=namespace, name=name)
    if not jobs:
        raise HTTPException(status_code=404, detail=f"Job {namespace}/{name} not found")

    # Get latest event for this job to show inputs/outputs
    events = store.query_events(namespace=namespace, job_name=name, limit=1)
    latest_event: RunEvent | None = events[0] if events else None

    return JobDetailResponse(
        job=jobs[0],
        latest_inputs=[i.model_dump() for i in latest_event.inputs] if latest_event else [],
        latest_outputs=[o.model_dump() for o in latest_event.outputs] if latest_event else [],
    )
