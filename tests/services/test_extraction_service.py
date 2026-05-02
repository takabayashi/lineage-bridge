# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `services.extraction_service.run_extraction`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from lineage_bridge.config.settings import ClusterCredential
from lineage_bridge.services import ExtractionRequest, run_extraction


def _settings() -> MagicMock:
    s = MagicMock()
    s.cluster_credentials = {}
    s.model_copy = lambda update: _settings_with(update)
    return s


def _settings_with(update: dict) -> MagicMock:
    s = MagicMock()
    s.cluster_credentials = update.get("cluster_credentials", {})
    return s


async def test_run_extraction_dispatches_to_orchestrator_with_request_fields():
    """Every field on the request maps to an orchestrator kwarg of the same name."""
    req = ExtractionRequest(
        environment_ids=["env-1", "env-2"],
        cluster_ids=["lkc-a"],
        enable_connect=False,
        enable_ksqldb=True,
        enable_flink=True,
        enable_schema_registry=False,
        enable_stream_catalog=False,
        enable_tableflow=True,
        enable_enrichment=False,
        enable_metrics=True,
        metrics_lookback_hours=12,
        sr_endpoints={"env-1": "https://psrc.example"},
        sr_credentials={"env-1": {"api_key": "k", "api_secret": "s"}},
        flink_credentials={"env-2": {"api_key": "fk", "api_secret": "fs"}},
    )
    settings = _settings()

    with patch(
        "lineage_bridge.services.extraction_service._orchestrator_run_extraction",
        new=AsyncMock(),
    ) as mock:
        await run_extraction(req, settings)

    kwargs = mock.call_args.kwargs
    assert kwargs["environment_ids"] == ["env-1", "env-2"]
    assert kwargs["cluster_ids"] == ["lkc-a"]
    assert kwargs["enable_connect"] is False
    assert kwargs["enable_ksqldb"] is True
    assert kwargs["enable_flink"] is True
    assert kwargs["enable_schema_registry"] is False
    assert kwargs["enable_stream_catalog"] is False
    assert kwargs["enable_tableflow"] is True
    assert kwargs["enable_enrichment"] is False
    assert kwargs["enable_metrics"] is True
    assert kwargs["metrics_lookback_hours"] == 12
    assert kwargs["sr_endpoints"] == {"env-1": "https://psrc.example"}
    assert kwargs["sr_credentials"] == {"env-1": {"api_key": "k", "api_secret": "s"}}
    assert kwargs["flink_credentials"] == {"env-2": {"api_key": "fk", "api_secret": "fs"}}


async def test_run_extraction_passes_progress_callback_through():
    req = ExtractionRequest(environment_ids=["env-1"])

    def cb(phase: str, detail: str) -> None: ...

    with patch(
        "lineage_bridge.services.extraction_service._orchestrator_run_extraction",
        new=AsyncMock(),
    ) as mock:
        await run_extraction(req, _settings(), on_progress=cb)

    assert mock.call_args.kwargs["on_progress"] is cb


async def test_run_extraction_empty_credential_dicts_become_none():
    """Orchestrator treats {} the same as None for SR/Flink credentials — keep that contract."""
    req = ExtractionRequest(environment_ids=["env-1"])  # all credential dicts default to {}

    with patch(
        "lineage_bridge.services.extraction_service._orchestrator_run_extraction",
        new=AsyncMock(),
    ) as mock:
        await run_extraction(req, _settings())

    assert mock.call_args.kwargs["sr_endpoints"] is None
    assert mock.call_args.kwargs["sr_credentials"] is None
    assert mock.call_args.kwargs["flink_credentials"] is None


async def test_cluster_credentials_merged_into_settings_copy():
    """`req.cluster_credentials` lands in the Settings copy passed to the orchestrator."""
    req = ExtractionRequest(
        environment_ids=["env-1"],
        cluster_credentials={"lkc-x": {"api_key": "xk", "api_secret": "xs"}},
    )
    captured: dict = {}

    settings = MagicMock()
    settings.cluster_credentials = {
        "lkc-existing": ClusterCredential(api_key="ek", api_secret="es")
    }

    def _model_copy(update):
        captured.update(update)
        copy = MagicMock()
        copy.cluster_credentials = update["cluster_credentials"]
        return copy

    settings.model_copy = _model_copy

    with patch(
        "lineage_bridge.services.extraction_service._orchestrator_run_extraction",
        new=AsyncMock(),
    ) as mock:
        await run_extraction(req, settings)

    merged = captured["cluster_credentials"]
    assert "lkc-existing" in merged
    assert "lkc-x" in merged
    assert isinstance(merged["lkc-x"], ClusterCredential)
    assert merged["lkc-x"].api_key == "xk"
    # Orchestrator received the *copy*, not the original
    forwarded_settings = mock.call_args.args[0]
    assert forwarded_settings.cluster_credentials == merged


async def test_cluster_credentials_empty_skips_settings_copy():
    """No merge work when there are no per-cluster overrides."""
    req = ExtractionRequest(environment_ids=["env-1"])
    settings = _settings()

    with patch(
        "lineage_bridge.services.extraction_service._orchestrator_run_extraction",
        new=AsyncMock(),
    ) as mock:
        await run_extraction(req, settings)

    # The orchestrator received the original settings, unchanged
    assert mock.call_args.args[0] is settings
