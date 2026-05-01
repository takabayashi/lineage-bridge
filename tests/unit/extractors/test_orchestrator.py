# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for orchestrator-level entry points.

Phase-internal behaviour lives in tests/unit/extractors/phases/. This file
covers the things the orchestrator owns: run_extraction, run_enrichment,
the four push functions, the discovery helpers (_discover_environment,
_stamp_environment_names), and the main() CLI.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from lineage_bridge.extractors.orchestrator import (
    _discover_environment,
    _extract_environment,
    _stamp_environment_names,
    main,
    run_enrichment,
    run_extraction,
    run_lineage_push,
)
from lineage_bridge.models.graph import (
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from tests.unit.extractors.conftest import (
    cluster_payload,
    make_async_client_mock,
    make_settings,
)

# ── run_extraction ──────────────────────────────────────────────────────


async def test_run_extraction_all_disabled_returns_empty_graph(no_sleep):
    """run_extraction with every extractor off still returns a LineageGraph."""
    cluster = cluster_payload()
    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[cluster])
        mock_cloud.get = AsyncMock(return_value={"display_name": "test-env"})

        graph = await run_extraction(
            make_settings(),
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
        )

    assert isinstance(graph, LineageGraph)


async def test_run_extraction_closes_cloud_client(no_sleep):
    """run_extraction always closes the cloud client (success or failure)."""
    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            make_settings(),
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
        )

        mock_cloud.close.assert_awaited_once()


async def test_run_extraction_emits_done_progress(no_sleep):
    """on_progress receives a 'Done' message at the end of run_extraction."""
    messages: list[tuple[str, str]] = []
    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            make_settings(),
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            on_progress=lambda phase, detail: messages.append((phase, detail)),
        )

    assert any(m[0] == "Done" for m in messages)


async def test_run_extraction_iterates_each_environment(no_sleep):
    """Each environment_id is processed; no clusters in any of them = warning per env."""
    messages: list[tuple[str, str]] = []
    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            make_settings(),
            environment_ids=["env-1", "env-2", "env-3"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            on_progress=lambda phase, detail: messages.append((phase, detail)),
        )

    no_cluster_warnings = [m for m in messages if "No Kafka clusters" in m[1]]
    assert len(no_cluster_warnings) == 3


# ── _discover_environment ───────────────────────────────────────────────


async def test_discover_environment_no_clusters_returns_empty(
    no_sleep, progress_callback, progress_log
):
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[])

    clusters, sr_endpoint, _, _ = await _discover_environment(
        make_settings(),
        mock_cloud,
        "env-empty",
        cluster_ids=None,
        enable_schema_registry=False,
        enable_stream_catalog=False,
        sr_endpoints=None,
        sr_credentials=None,
        on_progress=progress_callback,
    )

    assert clusters == []
    assert sr_endpoint is None
    assert any("No Kafka clusters" in m[1] for m in progress_log)


async def test_discover_environment_filters_by_cluster_ids(no_sleep, progress_callback):
    """cluster_ids filter is applied to the discovered cluster list."""
    c1 = cluster_payload(cluster_id="lkc-keep")
    c2 = cluster_payload(cluster_id="lkc-skip")
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[c1, c2])

    clusters, *_ = await _discover_environment(
        make_settings(),
        mock_cloud,
        "env-test1",
        cluster_ids=["lkc-keep"],
        enable_schema_registry=False,
        enable_stream_catalog=False,
        sr_endpoints=None,
        sr_credentials=None,
        on_progress=progress_callback,
    )

    assert [c["id"] for c in clusters] == ["lkc-keep"]


async def test_discover_environment_finds_sr_via_management_api(
    no_sleep, progress_callback, progress_log
):
    """SR endpoint discovered from /srcm/v2/clusters when not configured."""
    cluster = cluster_payload()
    mock_cloud = AsyncMock()

    async def _paginate(path, **kwargs):
        if "cmk/v2/clusters" in path:
            return [cluster]
        if "srcm/v2/clusters" in path:
            return [{"spec": {"http_endpoint": "https://psrc-abc.us-east.gcp.confluent.cloud"}}]
        return []

    mock_cloud.paginate = AsyncMock(side_effect=_paginate)

    _, sr_endpoint, _, _ = await _discover_environment(
        make_settings(),
        mock_cloud,
        "env-test1",
        cluster_ids=None,
        enable_schema_registry=True,
        enable_stream_catalog=False,
        sr_endpoints=None,
        sr_credentials=None,
        on_progress=progress_callback,
    )

    assert sr_endpoint == "https://psrc-abc.us-east.gcp.confluent.cloud"
    assert any("Schema Registry found" in m[1] for m in progress_log)


async def test_discover_environment_uses_settings_sr_endpoint(
    no_sleep, progress_callback, progress_log
):
    """schema_registry_endpoint setting beats SR management API discovery."""
    settings = make_settings(schema_registry_endpoint="https://psrc-from-settings.confluent.cloud")
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster_payload()])

    _, sr_endpoint, _, _ = await _discover_environment(
        settings,
        mock_cloud,
        "env-test1",
        cluster_ids=None,
        enable_schema_registry=True,
        enable_stream_catalog=False,
        sr_endpoints=None,
        sr_credentials=None,
        on_progress=progress_callback,
    )

    assert sr_endpoint == "https://psrc-from-settings.confluent.cloud"
    assert any("cached" in m[1] for m in progress_log)


async def test_discover_environment_per_env_sr_credentials_take_priority(
    no_sleep, progress_callback
):
    """sr_credentials[env_id] beats settings.schema_registry_api_key."""
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster_payload()])

    _, _, sr_key, sr_secret = await _discover_environment(
        make_settings(),
        mock_cloud,
        "env-test1",
        cluster_ids=None,
        enable_schema_registry=True,
        enable_stream_catalog=False,
        sr_endpoints={"env-test1": "https://psrc-test.confluent.cloud"},
        sr_credentials={"env-test1": {"api_key": "sr-env-key", "api_secret": "sr-env-secret"}},
        on_progress=progress_callback,
    )

    assert sr_key == "sr-env-key"
    assert sr_secret == "sr-env-secret"


# ── _stamp_environment_names ────────────────────────────────────────────


async def test_stamp_environment_names_backfills_blank_names(no_sleep):
    """Nodes missing environment_name and cluster_name get backfilled from the cloud API."""
    cloud = AsyncMock()
    cloud.get = AsyncMock(return_value={"display_name": "production-env"})
    cluster = cluster_payload(display_name="production-cluster")

    graph = LineageGraph()
    graph.add_node(
        LineageNode(
            node_id="confluent:kafka_topic:env-test1:orders",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="orders",
            display_name="orders",
            environment_id="env-test1",
            cluster_id="lkc-test1",
        )
    )

    await _stamp_environment_names(cloud, "env-test1", [cluster], graph)

    stamped = graph.get_node("confluent:kafka_topic:env-test1:orders")
    assert stamped.environment_name == "production-env"
    assert stamped.cluster_name == "production-cluster"


# ── _extract_environment integration ────────────────────────────────────


async def test_extract_environment_runs_phases_in_order(no_sleep):
    """Phase 1 (KafkaAdmin) progress message appears before Phase 2 (Processing)."""
    settings = make_settings()
    graph = LineageGraph()
    cluster = cluster_payload()
    cloud = AsyncMock()
    cloud.paginate = AsyncMock(return_value=[cluster])
    cloud.get = AsyncMock(return_value={"display_name": "test"})
    phases_seen: list[str] = []

    with (
        patch("lineage_bridge.extractors.phases.kafka_admin.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.phases.processing.ConnectClient") as MockConnect,
        patch("lineage_bridge.extractors.phases.processing.KsqlDBClient") as MockKsql,
    ):
        MockKafka.return_value = make_async_client_mock()
        MockConnect.return_value = make_async_client_mock()
        MockKsql.return_value = make_async_client_mock()

        await _extract_environment(
            settings,
            cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=True,
            enable_ksqldb=True,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=lambda phase, detail: phases_seen.append(phase),
        )

    p1_idx = next(i for i, p in enumerate(phases_seen) if "1/4" in p)
    p2_idx = next(i for i, p in enumerate(phases_seen) if "2/4" in p)
    assert p1_idx < p2_idx


# ── run_enrichment ──────────────────────────────────────────────────────


async def test_run_enrichment_dispatches_metrics_phase(no_sleep):
    """run_enrichment with enable_metrics=True calls MetricsPhase via Confluent API."""
    settings = make_settings()
    graph = LineageGraph()
    graph.add_node(
        LineageNode(
            node_id="confluent:kafka_topic:env-test1:test-topic",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="test-topic",
            display_name="test-topic",
            environment_id="env-test1",
            cluster_id="lkc-test1",
        )
    )
    messages: list[tuple[str, str]] = []

    with patch("lineage_bridge.extractors.phases.metrics.MetricsClient") as MockMetrics:
        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(return_value=5)
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        await run_enrichment(
            settings,
            graph,
            enable_metrics=True,
            metrics_lookback_hours=1,
            on_progress=lambda p, d: messages.append((p, d)),
        )

    assert any("Enriched 5 nodes" in m[1] for m in messages)


async def test_run_enrichment_metrics_failure_is_not_fatal(no_sleep):
    """A MetricsClient.enrich() exception does not abort run_enrichment."""
    settings = make_settings()
    graph = LineageGraph()
    graph.add_node(
        LineageNode(
            node_id="confluent:kafka_topic:env-test1:test-topic",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="test-topic",
            display_name="test-topic",
            environment_id="env-test1",
            cluster_id="lkc-test1",
        )
    )

    with patch("lineage_bridge.extractors.phases.metrics.MetricsClient") as MockMetrics:
        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(side_effect=RuntimeError("metrics fail"))
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        await run_enrichment(settings, graph, enable_metrics=True, metrics_lookback_hours=1)


# ── main() CLI ──────────────────────────────────────────────────────────


def test_main_requires_env_arg():
    """main() exits with error when --env is not provided."""
    with pytest.raises(SystemExit), patch("sys.argv", ["lineage-bridge-extract"]):
        main()


# ── push functions (will move to push_service in Phase 1A) ──────────────


async def test_run_lineage_push_no_credentials():
    """run_lineage_push returns an error when workspace URL is missing."""
    result = await run_lineage_push(make_settings(), LineageGraph())
    assert len(result.errors) == 1
    assert "workspace URL" in result.errors[0]


async def test_run_lineage_push_auto_discovers_warehouse():
    """No warehouse_id configured triggers warehouse discovery."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )

    from lineage_bridge.clients.databricks_discovery import WarehouseInfo
    from lineage_bridge.models.graph import PushResult

    mock_warehouses = [WarehouseInfo(id="wh-auto", name="Auto WH", state="RUNNING")]
    mock_result = PushResult(tables_updated=1)

    with (
        patch(
            "lineage_bridge.clients.databricks_discovery.list_warehouses",
            new=AsyncMock(return_value=mock_warehouses),
        ),
        patch("lineage_bridge.extractors.orchestrator.DatabricksUCProvider") as MockProvider,
        patch("lineage_bridge.clients.databricks_sql.DatabricksSQLClient") as MockSQL,
    ):
        mock_provider = AsyncMock()
        mock_provider.push_lineage = AsyncMock(return_value=mock_result)
        MockProvider.return_value = mock_provider

        result = await run_lineage_push(settings, LineageGraph())

    assert result.tables_updated == 1
    assert MockSQL.call_args[1]["warehouse_id"] == "wh-auto"


async def test_run_lineage_push_no_warehouses_found():
    """No warehouses returned by discovery yields an error result."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )

    with patch(
        "lineage_bridge.clients.databricks_discovery.list_warehouses",
        new=AsyncMock(return_value=[]),
    ):
        result = await run_lineage_push(settings, LineageGraph())

    assert any("No SQL warehouses" in e for e in result.errors)


async def test_run_lineage_push_delegates_to_provider():
    """When configured, run_lineage_push calls DatabricksUCProvider.push_lineage."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
        databricks_warehouse_id="wh-123",
    )

    from lineage_bridge.models.graph import PushResult

    mock_result = PushResult(tables_updated=2, properties_set=2, comments_set=2)

    with (
        patch("lineage_bridge.extractors.orchestrator.DatabricksUCProvider") as MockProvider,
        patch("lineage_bridge.clients.databricks_sql.DatabricksSQLClient"),
    ):
        mock_provider = AsyncMock()
        mock_provider.push_lineage = AsyncMock(return_value=mock_result)
        MockProvider.return_value = mock_provider

        result = await run_lineage_push(settings, LineageGraph())

    assert result.tables_updated == 2
    mock_provider.push_lineage.assert_awaited_once()
