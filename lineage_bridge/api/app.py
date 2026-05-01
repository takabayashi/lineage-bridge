# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""FastAPI application factory for LineageBridge REST API."""

from __future__ import annotations

from fastapi import FastAPI

from lineage_bridge.api.auth import configure_auth
from lineage_bridge.api.routers import datasets, graphs, jobs, lineage, meta, push, tasks
from lineage_bridge.api.state import GraphStore
from lineage_bridge.api.task_store import TaskStore
from lineage_bridge.openlineage.store import EventStore
from lineage_bridge.storage import Repositories, make_repositories


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
        # Best-effort: if Settings can't load (missing required Confluent
        # creds in test environments), fall back to memory-only.
        try:
            from lineage_bridge.config.settings import Settings

            repositories = make_repositories(Settings())  # type: ignore[call-arg]
        except Exception:
            from lineage_bridge.storage.backends.memory import (
                MemoryEventRepository,
                MemoryGraphRepository,
                MemoryTaskRepository,
            )

            repositories = Repositories(
                graphs=MemoryGraphRepository(),
                tasks=MemoryTaskRepository(),
                events=MemoryEventRepository(),
            )

    app.state.graph_store = GraphStore(repositories.graphs)
    app.state.event_store = EventStore(repositories.events)
    app.state.task_store = TaskStore(repositories.tasks)

    # Register routers
    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(datasets.router, prefix="/api/v1/lineage", tags=["datasets"])
    app.include_router(jobs.router, prefix="/api/v1/lineage", tags=["jobs"])
    app.include_router(graphs.router, prefix="/api/v1/graphs", tags=["graphs"])
    app.include_router(tasks.router, prefix="/api/v1/tasks", tags=["tasks"])
    app.include_router(push.router, prefix="/api/v1/push", tags=["push"])

    return app
