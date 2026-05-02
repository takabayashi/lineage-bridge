# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Extraction service — single entry point that wraps the orchestrator.

Both `ui/extraction.py` and `api/routers/tasks.py` call into this function
with an `ExtractionRequest` plus a `Settings` instance. The service merges
any per-cluster credential overrides into a copied `Settings`, then dispatches
to the orchestrator.

Settings live outside the request model on purpose — credentials are auth
context, not part of the data shape we want to serialize over the API.
"""

from __future__ import annotations

from collections.abc import Callable

from lineage_bridge.config.settings import ClusterCredential, Settings
from lineage_bridge.extractors.orchestrator import run_extraction as _orchestrator_run_extraction
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.services.requests import ExtractionRequest

ProgressCallback = Callable[[str, str], None]


def _merge_cluster_credentials(settings: Settings, ui_creds: dict[str, dict[str, str]]) -> Settings:
    """Return a copy of *settings* with `ui_creds` folded into `cluster_credentials`.

    No-op if `ui_creds` is empty. UI credentials beat what's already in the
    Settings dict (matching the previous behaviour in `ui/extraction.py`).

    Why a copy and not in-place: Settings is a long-lived process-wide object
    held by `app.state` and the watcher runner. Mutating it would leak the
    UI's per-request creds into other callers (different users, different
    extractions). `model_copy(update=...)` gives the orchestrator a request-
    scoped Settings without touching the shared instance.
    """
    if not ui_creds:
        return settings
    merged = dict(settings.cluster_credentials)
    for cluster_id, cred in ui_creds.items():
        merged[cluster_id] = ClusterCredential(**cred)
    return settings.model_copy(update={"cluster_credentials": merged})


async def run_extraction(
    req: ExtractionRequest,
    settings: Settings,
    on_progress: ProgressCallback | None = None,
) -> LineageGraph:
    """Run extraction described by *req* against *settings*.

    Returns the merged LineageGraph (mutated in place by the orchestrator).
    """
    settings = _merge_cluster_credentials(settings, req.cluster_credentials)
    return await _orchestrator_run_extraction(
        settings,
        environment_ids=req.environment_ids,
        cluster_ids=req.cluster_ids,
        enable_connect=req.enable_connect,
        enable_ksqldb=req.enable_ksqldb,
        enable_flink=req.enable_flink,
        enable_schema_registry=req.enable_schema_registry,
        enable_stream_catalog=req.enable_stream_catalog,
        enable_tableflow=req.enable_tableflow,
        enable_enrichment=req.enable_enrichment,
        enable_metrics=req.enable_metrics,
        metrics_lookback_hours=req.metrics_lookback_hours,
        sr_endpoints=req.sr_endpoints or None,
        sr_credentials=req.sr_credentials or None,
        flink_credentials=req.flink_credentials or None,
        on_progress=on_progress,
    )
