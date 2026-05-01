# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for ProcessingPhase (Phase 2 — Connect + ksqlDB + Flink)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from lineage_bridge.extractors.phases.processing import ProcessingPhase
from tests.unit.extractors.conftest import (
    cluster_payload,
    make_async_client_mock,
    make_context,
    make_settings,
)


async def test_processing_skips_all_extractors_when_disabled(no_sleep):
    """All three extractors disabled = no clients constructed, empty result."""
    ctx = make_context(
        enable_connect=False,
        enable_ksqldb=False,
        enable_flink=False,
    )
    result = await ProcessingPhase().execute(ctx)
    assert result.nodes == []
    assert result.edges == []


async def test_processing_constructs_connect_client_per_cluster(no_sleep):
    """Connect runs once per cluster in the env."""
    clusters = [cluster_payload(cluster_id="lkc-a"), cluster_payload(cluster_id="lkc-b")]
    captured: list[str] = []

    with patch("lineage_bridge.extractors.phases.processing.ConnectClient") as MockConnect:

        def _make(**kwargs):
            captured.append(kwargs.get("kafka_cluster_id"))
            return make_async_client_mock()

        MockConnect.side_effect = _make
        ctx = make_context(
            clusters=clusters,
            enable_ksqldb=False,
            enable_flink=False,
        )
        await ProcessingPhase().execute(ctx)

    assert sorted(captured) == ["lkc-a", "lkc-b"]


async def test_processing_flink_org_from_cluster_spec(no_sleep):
    """Flink org_id resolved from cluster.spec.organization.id."""
    cluster = cluster_payload()
    cluster["spec"]["organization"] = {"id": "org-from-spec"}

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        MockFlink.return_value = make_async_client_mock()
        ctx = make_context(
            clusters=[cluster],
            enable_connect=False,
            enable_ksqldb=False,
        )
        await ProcessingPhase().execute(ctx)

    MockFlink.assert_called_once()
    assert MockFlink.call_args[1]["organization_id"] == "org-from-spec"


async def test_processing_flink_org_from_resource_name(no_sleep):
    """Flink org_id resolved from cluster.metadata.resource_name CRN."""
    cluster = cluster_payload()
    cluster["metadata"]["resource_name"] = (
        "crn://confluent.cloud/organization=org-res123/environment=env-test1"
    )

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        MockFlink.return_value = make_async_client_mock()
        ctx = make_context(
            clusters=[cluster],
            enable_connect=False,
            enable_ksqldb=False,
        )
        await ProcessingPhase().execute(ctx)

    assert MockFlink.call_args[1]["organization_id"] == "org-res123"


async def test_processing_flink_org_from_environment_api_fallback(no_sleep):
    """Last resort — fetch /org/v2/environments/{id} for the resource_name CRN."""
    cluster = cluster_payload()  # no org info anywhere

    cloud = AsyncMock()

    async def _get(path, **kwargs):
        if "/org/v2/environments/" in path:
            return {
                "metadata": {
                    "resource_name": (
                        "crn://confluent.cloud/organization=org-api-fallback/environment=env-test1"
                    )
                }
            }
        return {}

    cloud.get = AsyncMock(side_effect=_get)

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        MockFlink.return_value = make_async_client_mock()
        ctx = make_context(
            clusters=[cluster],
            cloud=cloud,
            enable_connect=False,
            enable_ksqldb=False,
        )
        await ProcessingPhase().execute(ctx)

    assert MockFlink.call_args[1]["organization_id"] == "org-api-fallback"


async def test_processing_flink_skipped_when_org_unresolvable(
    no_sleep, progress_callback, progress_log
):
    """No org id anywhere + env API fails = Flink is skipped with a warning."""
    cluster = cluster_payload()  # no org info
    cloud = AsyncMock()
    cloud.get = AsyncMock(side_effect=Exception("env api down"))

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        ctx = make_context(
            clusters=[cluster],
            cloud=cloud,
            enable_connect=False,
            enable_ksqldb=False,
            on_progress=progress_callback,
        )
        await ProcessingPhase().execute(ctx)

    MockFlink.assert_not_called()
    flink_warnings = [m for m in progress_log if "Flink" in m[1] and "skipped" in m[1]]
    assert len(flink_warnings) == 1


async def test_processing_flink_per_env_credentials_override_global(no_sleep):
    """Per-env Flink credentials beat settings.flink_api_key/secret."""
    settings = make_settings(
        flink_api_key="global-flink-key",
        flink_api_secret="global-flink-secret",
    )
    cluster = cluster_payload()
    cluster["spec"]["organization"] = {"id": "org-1"}

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        MockFlink.return_value = make_async_client_mock()
        ctx = make_context(
            settings=settings,
            clusters=[cluster],
            enable_connect=False,
            enable_ksqldb=False,
            flink_credentials={
                "env-test1": {
                    "api_key": "env-flink-key",
                    "api_secret": "env-flink-secret",
                }
            },
        )
        await ProcessingPhase().execute(ctx)

    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["flink_api_key"] == "env-flink-key"
    assert call_kwargs["flink_api_secret"] == "env-flink-secret"


async def test_processing_flink_falls_back_to_global_credentials(no_sleep):
    """No per-env Flink credentials = use settings.flink_api_key/secret."""
    settings = make_settings(
        flink_api_key="global-flink-key",
        flink_api_secret="global-flink-secret",
    )
    cluster = cluster_payload()
    cluster["spec"]["organization"] = {"id": "org-1"}

    with patch("lineage_bridge.extractors.phases.processing.FlinkClient") as MockFlink:
        MockFlink.return_value = make_async_client_mock()
        ctx = make_context(
            settings=settings,
            clusters=[cluster],
            enable_connect=False,
            enable_ksqldb=False,
        )
        await ProcessingPhase().execute(ctx)

    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["flink_api_key"] == "global-flink-key"
