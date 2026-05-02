# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for TableflowPhase (Phase 4)."""

from __future__ import annotations

from unittest.mock import patch

from lineage_bridge.extractors.phases.tableflow import TableflowPhase
from tests.unit.extractors.conftest import (
    cluster_payload,
    make_async_client_mock,
    make_context,
    make_settings,
)


async def test_tableflow_skipped_when_disabled(no_sleep):
    """enable_tableflow=False short-circuits without constructing the client."""
    with patch("lineage_bridge.extractors.phases.tableflow.TableflowClient") as MockTF:
        ctx = make_context(enable_tableflow=False)
        result = await TableflowPhase().execute(ctx)

    MockTF.assert_not_called()
    assert result.nodes == []


async def test_tableflow_runs_when_enabled(no_sleep):
    """TableflowClient is constructed and extracted when enabled."""
    with patch("lineage_bridge.extractors.phases.tableflow.TableflowClient") as MockTF:
        MockTF.return_value = make_async_client_mock()
        ctx = make_context()
        await TableflowPhase().execute(ctx)

    MockTF.assert_called_once()


async def test_tableflow_uses_dedicated_credentials_when_set(no_sleep):
    """settings.tableflow_api_key/secret beat the cloud credentials when present."""
    settings = make_settings(
        tableflow_api_key="tf-special-key",
        tableflow_api_secret="tf-special-secret",
    )
    with patch("lineage_bridge.extractors.phases.tableflow.TableflowClient") as MockTF:
        MockTF.return_value = make_async_client_mock()
        ctx = make_context(settings=settings)
        await TableflowPhase().execute(ctx)

    call_kwargs = MockTF.call_args[1]
    assert call_kwargs["api_key"] == "tf-special-key"
    assert call_kwargs["api_secret"] == "tf-special-secret"


async def test_tableflow_falls_back_to_cloud_credentials(no_sleep):
    """No tableflow_api_key = fall back to confluent_cloud_api_key."""
    with patch("lineage_bridge.extractors.phases.tableflow.TableflowClient") as MockTF:
        MockTF.return_value = make_async_client_mock()
        ctx = make_context()  # default settings have tableflow_api_key=None
        await TableflowPhase().execute(ctx)

    call_kwargs = MockTF.call_args[1]
    assert call_kwargs["api_key"] == "cloud-key"
    assert call_kwargs["api_secret"] == "cloud-secret"


async def test_tableflow_passes_all_cluster_ids(no_sleep):
    """Every discovered cluster_id gets passed to TableflowClient."""
    clusters = [cluster_payload(cluster_id="lkc-a"), cluster_payload(cluster_id="lkc-b")]
    with patch("lineage_bridge.extractors.phases.tableflow.TableflowClient") as MockTF:
        MockTF.return_value = make_async_client_mock()
        ctx = make_context(clusters=clusters)
        await TableflowPhase().execute(ctx)

    call_kwargs = MockTF.call_args[1]
    assert sorted(call_kwargs["cluster_ids"]) == ["lkc-a", "lkc-b"]
