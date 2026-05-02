# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Async task endpoints: trigger extraction/enrichment and poll status.

Routes through `lineage_bridge.services` so the API and UI hit the same code
path (see ADR-020). The `POST /extract` body is an `ExtractionRequest`
JSON; the request_builder enforces parity with the UI.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.schemas import TaskCreatedResponse
from lineage_bridge.api.task_store import TaskInfo, TaskStatus, TaskStore, TaskType
from lineage_bridge.services import (
    EnrichmentRequest,
    ExtractionRequest,
    build_extraction_request,
    run_enrichment,
    run_extraction,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Strong references to background tasks so they aren't garbage-collected
_background_tasks: set[asyncio.Task] = set()


def _get_task_store(request: Request) -> TaskStore:
    return request.app.state.task_store


@router.get("", dependencies=[Depends(require_api_key)])
async def list_tasks(
    task_type: TaskType | None = None,
    status: TaskStatus | None = None,
    limit: int = 20,
    store: TaskStore = Depends(_get_task_store),
) -> list[TaskInfo]:
    """List recent tasks with optional filters."""
    return store.list_tasks(task_type=task_type, status=status, limit=limit)


@router.get("/{task_id}", dependencies=[Depends(require_api_key)])
async def get_task(
    task_id: str,
    store: TaskStore = Depends(_get_task_store),
) -> TaskInfo:
    """Get a specific task by ID."""
    task = store.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return task


@router.post("/extract", status_code=202, dependencies=[Depends(require_api_key)])
async def trigger_extraction(
    request: Request,
    body: ExtractionRequest | None = None,
    store: TaskStore = Depends(_get_task_store),
) -> TaskCreatedResponse:
    """Trigger an async lineage extraction from Confluent Cloud.

    Body is an `ExtractionRequest` (see `services.requests.ExtractionRequest`).
    If no body is provided, all environments visible to the configured Cloud
    credentials are extracted with default flags.

    Returns immediately with a task_id. Poll GET /tasks/{task_id} for progress.
    """
    if body is None:
        # No body: extract every discoverable env with default flags. Matches
        # the previous behaviour of the no-arg POST /extract.
        body = ExtractionRequest(environment_ids=[])
    task = store.create(TaskType.EXTRACT, body.model_dump())

    bg = asyncio.create_task(_run_extraction(task.task_id, request.app, body))
    # Hold a strong ref in `_background_tasks` so the loop doesn't GC this
    # task before it runs (asyncio only weak-refs scheduled tasks). The
    # done callback drops the ref once the task finishes so the set
    # doesn't grow unbounded.
    _background_tasks.add(bg)
    bg.add_done_callback(_background_tasks.discard)

    return TaskCreatedResponse(task_id=task.task_id, status=task.status.value)


@router.post("/enrich", status_code=202, dependencies=[Depends(require_api_key)])
async def trigger_enrichment(
    request: Request,
    graph_id: str | None = None,
    body: EnrichmentRequest | None = None,
    store: TaskStore = Depends(_get_task_store),
) -> TaskCreatedResponse:
    """Trigger catalog enrichment on an existing graph.

    Returns immediately with a task_id.
    """
    if body is None:
        body = EnrichmentRequest()
    params = {"graph_id": graph_id, **body.model_dump()}
    task = store.create(TaskType.ENRICH, params)

    bg = asyncio.create_task(_run_enrichment_task(task.task_id, request.app, graph_id, body))
    _background_tasks.add(bg)
    bg.add_done_callback(_background_tasks.discard)

    return TaskCreatedResponse(task_id=task.task_id, status=task.status.value)


async def _run_extraction(
    task_id: str,
    app: Any,
    req: ExtractionRequest,
) -> None:
    """Background coroutine for lineage extraction."""
    store: TaskStore = app.state.task_store
    store.start(task_id)
    store.add_progress(task_id, "Starting extraction")

    try:
        from lineage_bridge.config.settings import Settings

        settings = Settings()
        store.add_progress(task_id, "Loaded settings")

        env_ids = list(req.environment_ids)
        if not env_ids:
            from lineage_bridge.clients.base import ConfluentClient
            from lineage_bridge.clients.discovery import list_environments

            cloud = ConfluentClient(
                "https://api.confluent.cloud",
                settings.confluent_cloud_api_key,
                settings.confluent_cloud_api_secret,
            )
            envs = await list_environments(cloud)
            env_ids = [e.id for e in envs]
            store.add_progress(task_id, f"Discovered {len(env_ids)} environments")
            # Re-route through build_extraction_request so the env_ids land in
            # a frozen request, not patched onto the existing one.
            req = build_extraction_request({**req.model_dump(), "environment_ids": env_ids})

        store.add_progress(task_id, f"Extracting from {len(env_ids)} environment(s)")

        graph = await run_extraction(req, settings)

        msg = f"Extraction complete: {graph.node_count} nodes, {graph.edge_count} edges"
        store.add_progress(task_id, msg)

        graph_store = app.state.graph_store
        graph_id = graph_store.create(graph)
        store.add_progress(task_id, f"Graph stored as {graph_id}")

        store.complete(
            task_id,
            {
                "graph_id": graph_id,
                "node_count": graph.node_count,
                "edge_count": graph.edge_count,
            },
        )

    except Exception as exc:
        logger.exception("Extraction task %s failed", task_id)
        store.fail(task_id, str(exc))


async def _run_enrichment_task(
    task_id: str,
    app: Any,
    graph_id: str | None,
    req: EnrichmentRequest,
) -> None:
    """Background coroutine for catalog enrichment."""
    store: TaskStore = app.state.task_store
    store.start(task_id)
    store.add_progress(task_id, "Starting enrichment")

    try:
        graph_store = app.state.graph_store

        if not graph_id:
            all_graphs = graph_store.list_all()
            if not all_graphs:
                store.fail(task_id, "No graphs available for enrichment")
                return
            graph_id = all_graphs[0].graph_id

        graph = graph_store.get(graph_id)
        if not graph:
            store.fail(task_id, f"Graph {graph_id} not found")
            return

        store.add_progress(task_id, f"Enriching graph {graph_id}")

        from lineage_bridge.config.settings import Settings

        settings = Settings()
        await run_enrichment(req, settings, graph)

        graph_store.touch(graph_id)
        store.complete(
            task_id,
            {
                "graph_id": graph_id,
                "node_count": graph.node_count,
                "edge_count": graph.edge_count,
            },
        )

    except Exception as exc:
        logger.exception("Enrichment task %s failed", task_id)
        store.fail(task_id, str(exc))
