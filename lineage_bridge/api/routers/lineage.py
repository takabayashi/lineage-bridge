# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""OpenLineage event endpoints: query and ingest."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.openlineage.models import RunEvent
from lineage_bridge.api.openlineage.store import EventStore
from lineage_bridge.api.schemas import EventsIngestedResponse

router = APIRouter()


def _get_event_store(request: Request) -> EventStore:
    return request.app.state.event_store


@router.get("/events", dependencies=[Depends(require_api_key)])
async def query_events(
    namespace: str | None = None,
    job: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
    store: EventStore = Depends(_get_event_store),
) -> list[RunEvent]:
    """Query OpenLineage run events with optional filters.

    Namespace and job support glob patterns (e.g. `confluent://*`).
    """
    return store.query_events(
        namespace=namespace,
        job_name=job,
        since=since,
        until=until,
        limit=limit,
        offset=offset,
    )


@router.get("/events/{run_id}", dependencies=[Depends(require_api_key)])
async def get_events_by_run(
    run_id: str,
    store: EventStore = Depends(_get_event_store),
) -> list[RunEvent]:
    """Get all events for a specific run ID."""
    events = store.get_by_run_id(run_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"No events found for run {run_id}")
    return events


@router.post("/events", status_code=201, dependencies=[Depends(require_api_key)])
async def ingest_events(
    events: list[RunEvent],
    request: Request,
    store: EventStore = Depends(_get_event_store),
) -> EventsIngestedResponse:
    """Ingest OpenLineage RunEvent(s) from external systems.

    Accepts a list of RunEvents and stores them for querying.
    Events are also merged into the graph store via events_to_graph.
    """
    count = store.store_events(events)

    # Merge ingested events into a dedicated "ingested" graph
    from lineage_bridge.api.openlineage.translator import events_to_graph

    graph_store = request.app.state.graph_store
    ingested_graph = events_to_graph(events)

    # Find or create a graph for ingested events
    existing = [s for s in graph_store.list_all()]
    if not existing:
        graph_id = graph_store.create(ingested_graph)
    else:
        # Merge into the first available graph
        graph_id = existing[0].graph_id
        target = graph_store.get(graph_id)
        if target:
            for node in ingested_graph.nodes:
                target.add_node(node)
            for edge in ingested_graph.edges:
                target.add_edge(edge)
            graph_store.touch(graph_id)

    return EventsIngestedResponse(events_stored=count, graph_id=graph_id)
