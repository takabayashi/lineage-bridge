# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for SchemaEnrichmentPhase (Phase 3 — SchemaRegistry + StreamCatalog)."""

from __future__ import annotations

from unittest.mock import patch

from lineage_bridge.extractors.phases.enrichment import SchemaEnrichmentPhase
from tests.unit.extractors.conftest import make_async_client_mock, make_context


async def test_schema_enrichment_skipped_when_no_sr_endpoint(
    no_sleep, progress_callback, progress_log
):
    """No SR endpoint discovered = both extractors skipped with progress messages."""
    ctx = make_context(sr_endpoint=None, on_progress=progress_callback)

    result = await SchemaEnrichmentPhase().execute(ctx)

    assert result.nodes == []
    sr_skipped = [m for m in progress_log if "Schema Registry" in m[1] and "no endpoint" in m[1]]
    catalog_skipped = [
        m for m in progress_log if "Stream Catalog" in m[1] and "no SR endpoint" in m[1]
    ]
    assert len(sr_skipped) == 1
    assert len(catalog_skipped) == 1


async def test_schema_enrichment_runs_sr_when_endpoint_present(no_sleep):
    """SchemaRegistryClient is constructed and extracted when sr_endpoint is set."""
    with patch("lineage_bridge.extractors.phases.enrichment.SchemaRegistryClient") as MockSR:
        MockSR.return_value = make_async_client_mock()
        ctx = make_context(
            sr_endpoint="https://psrc-test.confluent.cloud",
            sr_key="sr-key",
            sr_secret="sr-secret",
            enable_stream_catalog=False,
        )
        await SchemaEnrichmentPhase().execute(ctx)

    MockSR.assert_called_once()
    call_kwargs = MockSR.call_args[1]
    assert call_kwargs["api_key"] == "sr-key"
    assert call_kwargs["api_secret"] == "sr-secret"


async def test_schema_enrichment_calls_stream_catalog_enrich(no_sleep):
    """StreamCatalog.enrich() is called against ctx.graph when endpoint is present."""
    with patch("lineage_bridge.extractors.phases.enrichment.StreamCatalogClient") as MockCatalog:
        catalog_inst = make_async_client_mock()
        MockCatalog.return_value = catalog_inst

        ctx = make_context(
            sr_endpoint="https://psrc-test.confluent.cloud",
            sr_key="sr-key",
            sr_secret="sr-secret",
            enable_schema_registry=False,
        )
        await SchemaEnrichmentPhase().execute(ctx)

    catalog_inst.enrich.assert_awaited_once()


async def test_schema_enrichment_swallows_stream_catalog_errors(no_sleep):
    """StreamCatalog.enrich() raising does not abort the phase or propagate."""
    with patch("lineage_bridge.extractors.phases.enrichment.StreamCatalogClient") as MockCatalog:
        catalog_inst = make_async_client_mock()
        catalog_inst.enrich.side_effect = RuntimeError("catalog API down")
        MockCatalog.return_value = catalog_inst

        ctx = make_context(
            sr_endpoint="https://psrc-test.confluent.cloud",
            sr_key="sr-key",
            sr_secret="sr-secret",
            enable_schema_registry=False,
        )
        # Should not raise
        result = await SchemaEnrichmentPhase().execute(ctx)
        assert result.nodes == []
