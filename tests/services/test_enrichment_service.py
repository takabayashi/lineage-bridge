# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `services.enrichment_service.run_enrichment`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.services import EnrichmentRequest, run_enrichment


async def test_run_enrichment_dispatches_to_orchestrator():
    req = EnrichmentRequest(enable_catalog=False, enable_metrics=True, metrics_lookback_hours=8)
    settings = MagicMock()
    graph = LineageGraph()

    with patch(
        "lineage_bridge.services.enrichment_service._orchestrator_run_enrichment",
        new=AsyncMock(),
    ) as mock:
        await run_enrichment(req, settings, graph)

    kwargs = mock.call_args.kwargs
    assert kwargs["enable_catalog"] is False
    assert kwargs["enable_metrics"] is True
    assert kwargs["metrics_lookback_hours"] == 8
    args = mock.call_args.args
    assert args[0] is settings
    assert args[1] is graph


async def test_run_enrichment_default_request_uses_catalog_only():
    """Default EnrichmentRequest = enable_catalog=True, enable_metrics=False."""
    with patch(
        "lineage_bridge.services.enrichment_service._orchestrator_run_enrichment",
        new=AsyncMock(),
    ) as mock:
        await run_enrichment(EnrichmentRequest(), MagicMock(), LineageGraph())

    kwargs = mock.call_args.kwargs
    assert kwargs["enable_catalog"] is True
    assert kwargs["enable_metrics"] is False
    assert kwargs["metrics_lookback_hours"] == 1
