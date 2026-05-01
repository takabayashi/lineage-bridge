# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Push service — single dispatcher across all four push providers.

Replaces the four `run_*_push` functions in the orchestrator (UC, Glue,
Google, DataZone) with one entry point. Phase 1B's catalog protocol v2 will
let this dispatcher loop over `provider.push_lineage(...)` instead of the
per-provider orchestrator wrappers.

The dispatch table maps provider names to attribute names on the orchestrator
module (not bound function references). The `getattr` lookup happens at call
time, so `mock.patch` against either `lineage_bridge.services.push_service.*`
or `lineage_bridge.extractors.orchestrator.*` resolves correctly in tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lineage_bridge.config.settings import Settings
from lineage_bridge.extractors import orchestrator as _orchestrator
from lineage_bridge.models.graph import LineageGraph, PushResult
from lineage_bridge.services.requests import PushProviderName, PushRequest

ProgressCallback = Callable[[str, str], None]

# Provider name → attribute on the orchestrator module that implements push.
_PROVIDER_TO_ATTR: dict[PushProviderName, str] = {
    "databricks_uc": "run_lineage_push",
    "aws_glue": "run_glue_push",
    "google": "run_google_push",
    "datazone": "run_datazone_push",
}

PUSH_PROVIDERS: tuple[PushProviderName, ...] = tuple(_PROVIDER_TO_ATTR)


async def run_push(
    req: PushRequest,
    settings: Settings,
    graph: LineageGraph,
    on_progress: ProgressCallback | None = None,
) -> PushResult:
    """Dispatch *req* to the matching push function and return its `PushResult`."""
    attr = _PROVIDER_TO_ATTR.get(req.provider)
    if attr is None:
        raise ValueError(
            f"Unknown push provider: {req.provider!r}. Known: {', '.join(PUSH_PROVIDERS)}."
        )
    fn = getattr(_orchestrator, attr)
    options: dict[str, Any] = dict(req.options)
    return await fn(settings, graph, on_progress=on_progress, **options)
