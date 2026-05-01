# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for KafkaAdminPhase (Phase 1)."""

from __future__ import annotations

from unittest.mock import patch

from lineage_bridge.extractors.phases.kafka_admin import KafkaAdminPhase
from tests.unit.extractors.conftest import (
    cluster_payload,
    make_async_client_mock,
    make_context,
    make_settings,
)


async def test_kafka_admin_strips_sasl_prefix_from_bootstrap(no_sleep):
    """SASL_SSL:// prefix is stripped before passing to KafkaAdminClient."""
    cluster = cluster_payload()
    cluster["spec"]["kafka_bootstrap_endpoint"] = "SASL_SSL://pkc-test.us-east1.gcp:9092"
    captured: list[str | None] = []

    with patch("lineage_bridge.extractors.phases.kafka_admin.KafkaAdminClient") as MockKafka:

        def _make(**kwargs):
            captured.append(kwargs.get("bootstrap_servers"))
            return make_async_client_mock()

        MockKafka.side_effect = _make
        ctx = make_context(clusters=[cluster])
        await KafkaAdminPhase().execute(ctx)

    assert captured == ["pkc-test.us-east1.gcp:9092"]


async def test_kafka_admin_skips_cluster_with_no_rest_endpoint(
    no_sleep, progress_callback, progress_log
):
    """A cluster missing http_endpoint, region, and cloud is skipped with a warning."""
    cluster = cluster_payload()
    cluster["spec"]["http_endpoint"] = ""
    cluster["spec"]["region"] = ""
    cluster["spec"]["cloud"] = ""
    ctx = make_context(clusters=[cluster], on_progress=progress_callback)

    await KafkaAdminPhase().execute(ctx)

    warnings = [m for m in progress_log if "No REST endpoint" in m[1]]
    assert len(warnings) == 1


async def test_kafka_admin_synthesizes_endpoint_from_region(no_sleep):
    """Missing http_endpoint is recovered via {cluster_id}.{region}.{cloud}.confluent.cloud."""
    cluster = cluster_payload()
    cluster["spec"]["http_endpoint"] = ""
    cluster["spec"]["region"] = "us-east1"
    cluster["spec"]["cloud"] = "GCP"
    captured: list[str] = []

    with patch("lineage_bridge.extractors.phases.kafka_admin.KafkaAdminClient") as MockKafka:

        def _make(**kwargs):
            captured.append(kwargs.get("base_url"))
            return make_async_client_mock()

        MockKafka.side_effect = _make
        ctx = make_context(clusters=[cluster])
        await KafkaAdminPhase().execute(ctx)

    assert captured == ["https://lkc-test1.us-east1.gcp.confluent.cloud:443"]


async def test_kafka_admin_uses_per_cluster_credentials(no_sleep):
    """Settings.get_cluster_credentials() decides the per-cluster API key."""
    settings = make_settings()
    settings.get_cluster_credentials.return_value = ("special-key", "special-secret")
    captured: list[tuple[str, str]] = []

    with patch("lineage_bridge.extractors.phases.kafka_admin.KafkaAdminClient") as MockKafka:

        def _make(**kwargs):
            captured.append((kwargs.get("api_key"), kwargs.get("api_secret")))
            return make_async_client_mock()

        MockKafka.side_effect = _make
        ctx = make_context(settings=settings)
        await KafkaAdminPhase().execute(ctx)

    assert captured == [("special-key", "special-secret")]


async def test_kafka_admin_runs_one_client_per_cluster(no_sleep):
    """Each cluster gets its own KafkaAdminClient instantiation."""
    clusters = [
        cluster_payload(cluster_id="lkc-a"),
        cluster_payload(cluster_id="lkc-b"),
    ]
    captured: list[str] = []

    with patch("lineage_bridge.extractors.phases.kafka_admin.KafkaAdminClient") as MockKafka:

        def _make(**kwargs):
            captured.append(kwargs.get("cluster_id"))
            return make_async_client_mock()

        MockKafka.side_effect = _make
        ctx = make_context(clusters=clusters)
        await KafkaAdminPhase().execute(ctx)

    assert captured == ["lkc-a", "lkc-b"]
