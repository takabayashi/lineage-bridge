# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""FastAPI application factory for LineageBridge REST API."""

from __future__ import annotations

import logging

from fastapi import FastAPI

from lineage_bridge.api.auth import configure_auth
from lineage_bridge.api.routers import datasets, graphs, jobs, lineage, meta, push, tasks, watcher
from lineage_bridge.api.state import GraphStore
from lineage_bridge.api.task_store import TaskStore
from lineage_bridge.openlineage.store import EventStore
from lineage_bridge.storage import Repositories, make_repositories

logger = logging.getLogger(__name__)


def create_app(
    *,
    api_key: str | None = None,
    repositories: Repositories | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        api_key: If set, require this key in X-API-Key header.
                 If None, API runs unauthenticated (dev mode).
        repositories: Override the storage backend bundle (used by tests).
                      If None, the bundle is built from `Settings()` —
                      controlled by `LINEAGE_BRIDGE_STORAGE__BACKEND`
                      (default: memory).
    """
    app = FastAPI(
        title="LineageBridge API",
        description=(
            "OpenLineage-compatible REST API that exposes Confluent Cloud "
            "stream lineage for integration with data catalogs "
            "(Databricks UC, AWS Glue, Google Data Lineage, and more)."
        ),
        version="0.4.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    configure_auth(api_key)

    if repositories is None:
        repositories = _build_repositories_or_fallback()

    app.state.graph_store = GraphStore(repositories.graphs)
    app.state.event_store = EventStore(repositories.events)
    app.state.task_store = TaskStore(repositories.tasks)
    # Watcher uses the repository directly (no Store adapter); router reads
    # `app.state.watcher_repo` and maintains its own per-process registry of
    # live runner tasks in `app.state.watcher_runners`.
    app.state.watcher_repo = repositories.watchers

    # Register routers
    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(datasets.router, prefix="/api/v1/lineage", tags=["datasets"])
    app.include_router(jobs.router, prefix="/api/v1/lineage", tags=["jobs"])
    app.include_router(graphs.router, prefix="/api/v1/graphs", tags=["graphs"])
    app.include_router(tasks.router, prefix="/api/v1/tasks", tags=["tasks"])
    app.include_router(push.router, prefix="/api/v1/push", tags=["push"])
    app.include_router(watcher.router, prefix="/api/v1/watcher", tags=["watcher"])

    return app


def _memory_bundle() -> Repositories:
    from lineage_bridge.storage.backends.memory import (
        MemoryEventRepository,
        MemoryGraphRepository,
        MemoryTaskRepository,
        MemoryWatcherRepository,
    )

    return Repositories(
        graphs=MemoryGraphRepository(),
        tasks=MemoryTaskRepository(),
        events=MemoryEventRepository(),
        watchers=MemoryWatcherRepository(),
    )


def _build_repositories_or_fallback() -> Repositories:
    """Build the storage bundle from Settings; fall back to memory with a logged warning.

    Two failure modes get the same fallback so the API still boots:
      - Settings can't load (missing Confluent creds in test envs)
      - storage backend misconfigured (`LINEAGE_BRIDGE_STORAGE__BACKEND=fyle`)

    Each fallback is logged at WARNING so the operator can see in the API
    logs that they're running on a memory store when they expected file or
    sqlite. This was previously silent — see the Phase 1C review notes.
    """
    try:
        from lineage_bridge.config.settings import Settings

        return make_repositories(Settings())  # type: ignore[call-arg]
    except Exception as exc:
        logger.warning(
            "Storage configuration failed (%s); falling back to in-memory store. "
            "Graphs / tasks / events will not survive process restart.",
            exc,
        )
        return _memory_bundle()
