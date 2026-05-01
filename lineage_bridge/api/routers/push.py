# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Push endpoints: push lineage from a stored graph to a catalog provider.

Single endpoint (`POST /api/v1/push/{provider}`) replaces what would have
been four parallel routes (one per provider) by routing through
`services.run_push`. Phase 1B's catalog protocol v2 will let providers
register themselves, expanding the accepted `{provider}` set without any
changes here.

Push is synchronous on the wire — the caller blocks for the duration of the
push and gets a `PushResult` back. Push jobs are typically O(seconds-to-tens
of seconds) per catalog and have no resumable state, so background tasks
would just add complexity.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.state import GraphStore
from lineage_bridge.models.graph import PushResult
from lineage_bridge.services import PUSH_PROVIDERS, PushRequest, run_push
from lineage_bridge.services.requests import PushProviderName

router = APIRouter()


def _get_graph_store(request: Request) -> GraphStore:
    return request.app.state.graph_store


@router.get("/providers", dependencies=[Depends(require_api_key)])
async def list_providers() -> dict[str, list[str]]:
    """List the push provider names accepted by `POST /push/{provider}`."""
    return {"providers": list(PUSH_PROVIDERS)}


@router.post("/{provider}", dependencies=[Depends(require_api_key)])
async def push_lineage(
    request: Request,
    provider: PushProviderName,
    options: dict[str, Any] = Body(default_factory=dict),
    graph_id: str | None = Query(
        default=None,
        description="Graph to push from. Defaults to the most recently stored graph.",
    ),
    store: GraphStore = Depends(_get_graph_store),
) -> PushResult:
    """Push lineage from a stored graph to *provider*.

    The body is the provider's `options` dict (see `PushRequest.options` in
    `services.requests`). Returns the `PushResult` synchronously — push runs
    in the request handler.
    """
    if graph_id is None:
        all_graphs = store.list_all()
        if not all_graphs:
            raise HTTPException(status_code=404, detail="No graphs available to push from")
        graph_id = all_graphs[0].graph_id

    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

    from lineage_bridge.config.settings import Settings

    settings = Settings()
    push_req = PushRequest(provider=provider, options=options)
    return await run_push(push_req, settings, graph)
