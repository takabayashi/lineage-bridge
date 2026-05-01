# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""FastAPI application factory for LineageBridge REST API."""

from __future__ import annotations

from fastapi import FastAPI

from lineage_bridge.api.auth import configure_auth
from lineage_bridge.api.openlineage.store import EventStore
from lineage_bridge.api.routers import datasets, graphs, jobs, lineage, meta, tasks
from lineage_bridge.api.state import GraphStore
from lineage_bridge.api.task_store import TaskStore


def create_app(*, api_key: str | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        api_key: If set, require this key in X-API-Key header.
                 If None, API runs unauthenticated (dev mode).
    """
    app = FastAPI(
        title="LineageBridge API",
        description=(
            "OpenLineage-compatible REST API that exposes Confluent Cloud "
            "stream lineage for integration with data catalogs "
            "(Databricks UC, AWS Glue, Google Data Lineage, and more)."
        ),
        version="0.3.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Configure authentication
    configure_auth(api_key)

    # Shared state — stores live for the lifetime of the process
    app.state.graph_store = GraphStore()
    app.state.event_store = EventStore()
    app.state.task_store = TaskStore()

    # Register routers
    app.include_router(meta.router, prefix="/api/v1", tags=["meta"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(datasets.router, prefix="/api/v1/lineage", tags=["datasets"])
    app.include_router(jobs.router, prefix="/api/v1/lineage", tags=["jobs"])
    app.include_router(graphs.router, prefix="/api/v1/graphs", tags=["graphs"])
    app.include_router(tasks.router, prefix="/api/v1/tasks", tags=["tasks"])

    return app
