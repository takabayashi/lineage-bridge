# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Async task endpoints: trigger extraction/enrichment and poll status."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.schemas import TaskCreatedResponse
from lineage_bridge.api.task_store import TaskInfo, TaskStatus, TaskStore, TaskType

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
    environment_ids: list[str] | None = None,
    store: TaskStore = Depends(_get_task_store),
) -> TaskCreatedResponse:
    """Trigger an async lineage extraction from Confluent Cloud.

    Returns immediately with a task_id. Poll GET /tasks/{task_id} for progress.
    """
    params = {"environment_ids": environment_ids or []}
    task = store.create(TaskType.EXTRACT, params)

    # Run extraction in background — fire-and-forget; task store tracks lifecycle
    bg = asyncio.create_task(_run_extraction(task.task_id, request.app, params))
    _background_tasks.add(bg)
    bg.add_done_callback(_background_tasks.discard)

    return TaskCreatedResponse(task_id=task.task_id, status=task.status.value)


@router.post("/enrich", status_code=202, dependencies=[Depends(require_api_key)])
async def trigger_enrichment(
    request: Request,
    graph_id: str | None = None,
    store: TaskStore = Depends(_get_task_store),
) -> TaskCreatedResponse:
    """Trigger catalog enrichment on an existing graph.

    Runs all registered catalog providers' enrich() methods.
    Returns immediately with a task_id.
    """
    params = {"graph_id": graph_id}
    task = store.create(TaskType.ENRICH, params)

    bg = asyncio.create_task(_run_enrichment(task.task_id, request.app, params))
    _background_tasks.add(bg)
    bg.add_done_callback(_background_tasks.discard)

    return TaskCreatedResponse(task_id=task.task_id, status=task.status.value)


async def _run_extraction(
    task_id: str,
    app: Any,
    params: dict[str, Any],
) -> None:
    """Background coroutine for lineage extraction."""
    store: TaskStore = app.state.task_store
    store.start(task_id)
    store.add_progress(task_id, "Starting extraction")

    try:
        from lineage_bridge.config.settings import Settings

        settings = Settings()
        store.add_progress(task_id, "Loaded settings")

        from lineage_bridge.extractors.orchestrator import Orchestrator

        orchestrator = Orchestrator(settings)
        store.add_progress(task_id, "Created orchestrator")

        env_ids = params.get("environment_ids", [])
        graph = await orchestrator.extract(environment_ids=env_ids or None)

        msg = f"Extraction complete: {graph.node_count} nodes, {graph.edge_count} edges"
        store.add_progress(task_id, msg)

        # Store the graph
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


async def _run_enrichment(
    task_id: str,
    app: Any,
    params: dict[str, Any],
) -> None:
    """Background coroutine for catalog enrichment."""
    store: TaskStore = app.state.task_store
    store.start(task_id)
    store.add_progress(task_id, "Starting enrichment")

    try:
        graph_store = app.state.graph_store
        graph_id = params.get("graph_id")

        # If no graph_id given, use the first available graph
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

        from lineage_bridge.catalogs import get_active_providers

        providers = get_active_providers(graph)
        store.add_progress(task_id, f"Found {len(providers)} active catalog providers")

        for provider in providers:
            store.add_progress(task_id, f"Running {provider.catalog_type} enrichment")
            try:
                await provider.enrich(graph)
                store.add_progress(task_id, f"{provider.catalog_type} enrichment complete")
            except Exception as exc:
                store.add_progress(task_id, f"{provider.catalog_type} enrichment failed: {exc}")

        graph_store.touch(graph_id)
        store.complete(
            task_id,
            {
                "graph_id": graph_id,
                "providers_run": len(providers),
                "node_count": graph.node_count,
            },
        )

    except Exception as exc:
        logger.exception("Enrichment task %s failed", task_id)
        store.fail(task_id, str(exc))
