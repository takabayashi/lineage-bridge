# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Enrichment service — wraps `orchestrator.run_enrichment` with an EnrichmentRequest."""

from __future__ import annotations

from collections.abc import Callable

from lineage_bridge.config.settings import Settings
from lineage_bridge.extractors.orchestrator import run_enrichment as _orchestrator_run_enrichment
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.services.requests import EnrichmentRequest

ProgressCallback = Callable[[str, str], None]


async def run_enrichment(
    req: EnrichmentRequest,
    settings: Settings,
    graph: LineageGraph,
    on_progress: ProgressCallback | None = None,
) -> LineageGraph:
    """Run enrichment described by *req* against *graph*. Mutates and returns *graph*."""
    return await _orchestrator_run_enrichment(
        settings,
        graph,
        enable_catalog=req.enable_catalog,
        enable_metrics=req.enable_metrics,
        metrics_lookback_hours=req.metrics_lookback_hours,
        on_progress=on_progress,
    )
