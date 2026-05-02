# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""REST endpoints for the watcher service (Phase 2G).

The router owns an in-process registry of live `WatcherRunner` tasks, but
all *state* lives in the storage backend so any second API process / UI
instance sees the same status, events, and history.

A note on the in-process restriction: only the API process that called
`POST /start` can `POST /stop` the runner. Cross-process stop control would
require a stop signal in the storage layer (a `requested_state` column or
similar) — left for ADR-022 follow-up. The Streamlit UI talking to the API
gets the durability + multi-instance reads for free; multi-process write
control is the explicit out-of-scope item.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.config.settings import Settings
from lineage_bridge.services.watcher_models import (
    ExtractionRecord,
    WatcherConfig,
    WatcherEvent,
    WatcherStatus,
    WatcherSummary,
)
from lineage_bridge.services.watcher_runner import WatcherRunner
from lineage_bridge.storage.protocol import WatcherRepository

router = APIRouter(dependencies=[Depends(require_api_key)])


class StartWatcherResponse(BaseModel):
    watcher_id: str = Field(description="UUID assigned to this watcher")


class ListEventsResponse(BaseModel):
    events: list[WatcherEvent]


class ListExtractionsResponse(BaseModel):
    extractions: list[ExtractionRecord]


class ListWatchersResponse(BaseModel):
    watchers: list[WatcherSummary]


def _runners(request: Request) -> dict[str, WatcherRunner]:
    """Per-app registry of live runner tasks, keyed by watcher_id.

    Lazy-init: test apps that never touch the watcher router shouldn't pay
    for the dict allocation, and `app.state` doesn't have a hook to set
    defaults. The registry is per-process by design — see the module
    docstring for the cross-process-stop limitation.
    """
    if not hasattr(request.app.state, "watcher_runners"):
        request.app.state.watcher_runners = {}
    return request.app.state.watcher_runners


def _repo(request: Request) -> WatcherRepository:
    return request.app.state.watcher_repo


def _settings(request: Request) -> Settings:
    """Per-app Settings instance (cached). Some test apps don't supply one."""
    cached = getattr(request.app.state, "settings", None)
    if cached is not None:
        return cached
    settings = Settings()  # type: ignore[call-arg]
    request.app.state.settings = settings
    return settings


# ── start / stop / deregister ──────────────────────────────────────────


@router.post(
    "",
    response_model=StartWatcherResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register and start a new watcher.",
)
async def start_watcher(config: WatcherConfig, request: Request) -> StartWatcherResponse:
    runner = WatcherRunner.spawn(config, _settings(request), _repo(request))
    await runner.start()
    _runners(request)[runner.watcher_id] = runner
    return StartWatcherResponse(watcher_id=runner.watcher_id)


@router.post(
    "/{watcher_id}/stop",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Stop a running watcher (in-process only — see module docstring).",
)
async def stop_watcher(watcher_id: str, request: Request) -> None:
    runner = _runners(request).pop(watcher_id, None)
    if runner is None:
        # Status snapshot may still exist from a prior process; report 404.
        raise HTTPException(
            status_code=404,
            detail=(
                f"No live runner for watcher {watcher_id!r} in this process. "
                "Cross-process stop is not yet supported."
            ),
        )
    await runner.stop()


@router.delete(
    "/{watcher_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Stop (if running) and deregister a watcher, dropping its history.",
)
async def deregister_watcher(watcher_id: str, request: Request) -> None:
    runner = _runners(request).pop(watcher_id, None)
    if runner is not None:
        await runner.stop()
    if not _repo(request).deregister(watcher_id):
        raise HTTPException(status_code=404, detail=f"Unknown watcher {watcher_id!r}")


# ── reads ───────────────────────────────────────────────────────────────


@router.get(
    "",
    response_model=ListWatchersResponse,
    summary="List every watcher visible in the storage backend.",
)
async def list_watchers(request: Request) -> ListWatchersResponse:
    return ListWatchersResponse(watchers=_repo(request).list_watchers())


@router.get(
    "/{watcher_id}/status",
    response_model=WatcherStatus,
    summary="Snapshot of the watcher's current state.",
)
async def get_status(watcher_id: str, request: Request) -> WatcherStatus:
    snapshot = _repo(request).get_status(watcher_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Unknown watcher {watcher_id!r}")
    return snapshot


@router.get(
    "/{watcher_id}/events",
    response_model=ListEventsResponse,
    summary="Recent change events the watcher detected.",
)
async def list_events(
    watcher_id: str,
    request: Request,
    limit: int = 100,
    since: datetime | None = None,
) -> ListEventsResponse:
    if _repo(request).get_config(watcher_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown watcher {watcher_id!r}")
    events = _repo(request).list_events(watcher_id, limit=limit, since=since)
    return ListEventsResponse(events=events)


@router.get(
    "/{watcher_id}/history",
    response_model=ListExtractionsResponse,
    summary="Recent watcher-triggered extractions.",
)
async def list_history(
    watcher_id: str,
    request: Request,
    limit: int = 50,
) -> ListExtractionsResponse:
    if _repo(request).get_config(watcher_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown watcher {watcher_id!r}")
    extractions = _repo(request).list_extractions(watcher_id, limit=limit)
    return ListExtractionsResponse(extractions=extractions)


# Re-export for consumers that want to type-check the router shape.
__all__: list[str] = ["router"]
