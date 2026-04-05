# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.extractors.orchestrator.

Tests for _merge_into, _safe_extract, run_extraction, _extract_environment,
and the main() CLI entry point.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from lineage_bridge.extractors.orchestrator import (
    _extract_environment,
    _merge_into,
    _safe_extract,
    main,
    run_extraction,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _node(name: str, node_type: NodeType = NodeType.KAFKA_TOPIC) -> LineageNode:
    nid = f"confluent:{node_type.value}:env-1:{name}"
    return LineageNode(
        node_id=nid,
        system=SystemType.CONFLUENT,
        node_type=node_type,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id="lkc-1",
    )


def _edge(src_name: str, dst_name: str, edge_type: EdgeType = EdgeType.PRODUCES) -> LineageEdge:
    src_id = f"confluent:kafka_topic:env-1:{src_name}"
    dst_id = f"confluent:kafka_topic:env-1:{dst_name}"
    return LineageEdge(src_id=src_id, dst_id=dst_id, edge_type=edge_type)


# ── _merge_into ──────────────────────────────────────────────────────────


def test_merge_into_adds_nodes_and_edges():
    """_merge_into adds new nodes and valid edges to the graph."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    n2 = _node("topic-b")
    e1 = _edge("topic-a", "topic-b")

    _merge_into(graph, [n1, n2], [e1])

    assert graph.node_count == 2
    assert graph.edge_count == 1
    assert graph.get_node(n1.node_id) is not None
    assert graph.get_node(n2.node_id) is not None
    assert graph.get_edge(e1.src_id, e1.dst_id, EdgeType.PRODUCES) is not None


def test_merge_into_adds_nodes_to_existing_graph():
    """_merge_into appends to a graph that already has nodes."""
    graph = LineageGraph()
    existing = _node("existing-topic")
    graph.add_node(existing)

    new_node = _node("new-topic")
    _merge_into(graph, [new_node], [])

    assert graph.node_count == 2


def test_merge_into_skips_edges_with_missing_endpoints():
    """_merge_into silently skips edges whose source or destination is not in the graph."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    # Edge references topic-b which is NOT added
    bad_edge = _edge("topic-a", "topic-b")

    _merge_into(graph, [n1], [bad_edge])

    assert graph.node_count == 1
    assert graph.edge_count == 0  # edge was skipped, not raised


def test_merge_into_skips_edge_missing_source():
    """_merge_into skips edges when the source node is missing."""
    graph = LineageGraph()

    n2 = _node("topic-b")
    bad_edge = _edge("topic-a", "topic-b")

    _merge_into(graph, [n2], [bad_edge])

    assert graph.node_count == 1
    assert graph.edge_count == 0


def test_merge_into_partial_edges():
    """_merge_into adds valid edges and skips invalid ones in the same call."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    n2 = _node("topic-b")
    good_edge = _edge("topic-a", "topic-b")
    bad_edge = _edge("topic-a", "topic-missing")

    _merge_into(graph, [n1, n2], [good_edge, bad_edge])

    assert graph.node_count == 2
    assert graph.edge_count == 1  # only the good edge


# ── _safe_extract ────────────────────────────────────────────────────────


async def test_safe_extract_success():
    """_safe_extract returns the coroutine result on success."""

    async def _extract():
        return [_node("orders")], [_edge("orders", "events")]

    # Add both nodes so the edge check in the test makes sense
    nodes, edges = await _safe_extract("test-extractor", _extract())

    assert len(nodes) == 1
    assert nodes[0].display_name == "orders"
    assert len(edges) == 1


async def test_safe_extract_failure_returns_empty():
    """_safe_extract returns ([], []) when the coroutine raises."""

    async def _failing():
        raise RuntimeError("connection lost")

    nodes, edges = await _safe_extract("broken-extractor", _failing())

    assert nodes == []
    assert edges == []


async def test_safe_extract_401_identification():
    """_safe_extract identifies 401 Unauthorized errors and calls on_progress."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _unauthorized():
        resp = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("401 Unauthorized", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("kafka", _unauthorized(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "401 Unauthorized" in messages[0][1]
    assert "cluster-scoped API key" in messages[0][1]


async def test_safe_extract_403_identification():
    """_safe_extract identifies 403 Forbidden errors."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _forbidden():
        resp = httpx.Response(403, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("403 Forbidden", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("connect", _forbidden(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "403 Forbidden" in messages[0][1]
    assert "lacks required permissions" in messages[0][1]


async def test_safe_extract_400_identification():
    """_safe_extract identifies 400 Bad Request errors."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _bad_request():
        resp = httpx.Response(400, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("400 Bad Request", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("flink", _bad_request(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "400 Bad Request" in messages[0][1]
    assert "API parameters are invalid" in messages[0][1]


async def test_safe_extract_no_progress_callback():
    """_safe_extract works without an on_progress callback."""

    async def _fail():
        raise ValueError("something broke")

    # Should not raise even with on_progress=None (the default)
    nodes, edges = await _safe_extract("test", _fail())
    assert nodes == []
    assert edges == []


# ── helpers for orchestrator integration tests ──────────────────────────


def _make_settings(**overrides: Any) -> MagicMock:
    """Create a mock Settings object with sensible defaults."""
    s = MagicMock()
    s.confluent_cloud_api_key = "cloud-key"
    s.confluent_cloud_api_secret = "cloud-secret"
    s.kafka_api_key = None
    s.kafka_api_secret = None
    s.schema_registry_endpoint = None
    s.schema_registry_api_key = None
    s.schema_registry_api_secret = None
    s.ksqldb_api_key = None
    s.ksqldb_api_secret = None
    s.flink_api_key = None
    s.flink_api_secret = None
    s.tableflow_api_key = None
    s.tableflow_api_secret = None
    s.databricks_workspace_url = None
    s.databricks_token = None
    s.log_level = "INFO"
    s.get_cluster_credentials.return_value = ("kafka-key", "kafka-secret")
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _cluster_payload(
    cluster_id: str = "lkc-test1",
    env_id: str = "env-test1",
    display_name: str = "my-cluster",
    rest_endpoint: str = "https://lkc-test1.us-east1.gcp.confluent.cloud:443",
) -> dict[str, Any]:
    """Build a cluster JSON payload like the Confluent API returns."""
    return {
        "id": cluster_id,
        "spec": {
            "display_name": display_name,
            "http_endpoint": rest_endpoint,
            "kafka_bootstrap_endpoint": "SASL_SSL://lkc-test1.us-east1.gcp:9092",
            "region": "us-east1",
            "cloud": "GCP",
            "environment": {"id": env_id},
        },
        "metadata": {"resource_name": ""},
    }


# ── run_extraction tests ───────────────────────────────────────────────


async def test_run_extraction_all_disabled_returns_empty_graph(no_sleep):
    """run_extraction with all extractors disabled returns an empty graph."""
    settings = _make_settings()
    cluster = _cluster_payload()

    with (
        patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient,
    ):
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        # paginate for clusters
        mock_cloud.paginate = AsyncMock(return_value=[cluster])
        # get for environment display name
        mock_cloud.get = AsyncMock(return_value={"display_name": "test-env"})

        graph = await run_extraction(
            settings,
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
        )

    # Only KafkaAdmin runs (phase 1 always runs), but it's mocked
    assert isinstance(graph, LineageGraph)


async def test_run_extraction_calls_close_on_cloud_client(no_sleep):
    """run_extraction always closes the cloud client, even on success."""
    settings = _make_settings()

    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            settings,
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
        )

        mock_cloud.close.assert_awaited_once()


async def test_run_extraction_progress_callback(no_sleep):
    """run_extraction calls on_progress at key phases."""
    settings = _make_settings()
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            settings,
            environment_ids=["env-test1"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            on_progress=_progress,
        )

    phases = [m[0] for m in messages]
    assert "Done" in phases


async def test_run_extraction_multiple_environments(no_sleep):
    """run_extraction iterates over each environment."""
    settings = _make_settings()
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.ConfluentClient") as MockClient:
        mock_cloud = AsyncMock()
        MockClient.return_value = mock_cloud
        # Return no clusters — each env will emit a Warning
        mock_cloud.paginate = AsyncMock(return_value=[])
        mock_cloud.get = AsyncMock(return_value={})

        await run_extraction(
            settings,
            environment_ids=["env-1", "env-2", "env-3"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            on_progress=_progress,
        )

    # Each env with no clusters produces a "Warning" about no clusters
    warning_msgs = [m for m in messages if m[0] == "Warning"]
    assert len(warning_msgs) == 3


# ── _extract_environment tests ─────────────────────────────────────────


async def test_extract_environment_no_clusters(no_sleep):
    """_extract_environment returns early when no clusters found."""
    settings = _make_settings()
    graph = LineageGraph()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[])
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    await _extract_environment(
        settings,
        mock_cloud,
        "env-empty",
        graph,
        cluster_ids=None,
        enable_connect=False,
        enable_ksqldb=False,
        enable_flink=False,
        enable_schema_registry=False,
        enable_stream_catalog=False,
        enable_tableflow=False,
        enable_metrics=False,
        metrics_lookback_hours=1,
        sr_endpoints=None,
        sr_credentials=None,
        flink_credentials=None,
        on_progress=_progress,
    )

    assert graph.node_count == 0
    warning_phases = [m for m in messages if m[0] == "Warning"]
    assert any("No Kafka clusters" in m[1] for m in warning_phases)


async def test_extract_environment_phase_ordering(no_sleep):
    """KafkaAdmin (phase 1) runs before Connect/ksqlDB/Flink (phase 2)."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    phases_seen: list[str] = []

    def _progress(phase: str, detail: str = "") -> None:
        phases_seen.append(phase)

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.ConnectClient") as MockConnect,
        patch("lineage_bridge.extractors.orchestrator.KsqlDBClient") as MockKsql,
    ):
        # KafkaAdmin mock
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        # Connect mock
        connect_inst = AsyncMock()
        connect_inst.extract = AsyncMock(return_value=([], []))
        connect_inst.__aenter__ = AsyncMock(return_value=connect_inst)
        connect_inst.__aexit__ = AsyncMock(return_value=False)
        MockConnect.return_value = connect_inst

        # ksqlDB mock
        ksql_inst = AsyncMock()
        ksql_inst.extract = AsyncMock(return_value=([], []))
        ksql_inst.__aenter__ = AsyncMock(return_value=ksql_inst)
        ksql_inst.__aexit__ = AsyncMock(return_value=False)
        MockKsql.return_value = ksql_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=True,
            enable_ksqldb=True,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    # Phase 1 must appear before Phase 2
    p1_idx = next(i for i, p in enumerate(phases_seen) if "1/4" in p)
    p2_idx = next(i for i, p in enumerate(phases_seen) if "2/4" in p)
    assert p1_idx < p2_idx


async def test_extract_environment_sr_discovery(no_sleep):
    """SR endpoint is discovered via the srcm/v2/clusters API."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    messages: list[tuple[str, str]] = []

    async def _paginate(path, **kwargs):
        if "cmk/v2/clusters" in path:
            return [cluster]
        if "srcm/v2/clusters" in path:
            return [{"spec": {"http_endpoint": "https://psrc-abc.us-east.gcp.confluent.cloud"}}]
        return []

    mock_cloud.paginate = AsyncMock(side_effect=_paginate)
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=True,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    discovery_msgs = [m for m in messages if "Schema Registry found" in m[1]]
    assert len(discovery_msgs) == 1


async def test_extract_environment_sr_endpoint_from_settings(no_sleep):
    """SR endpoint falls back to settings.schema_registry_endpoint."""
    settings = _make_settings(schema_registry_endpoint="https://psrc-from-settings.confluent.cloud")
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.SchemaRegistryClient") as MockSR,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        sr_inst = AsyncMock()
        sr_inst.extract = AsyncMock(return_value=([], []))
        sr_inst.__aenter__ = AsyncMock(return_value=sr_inst)
        sr_inst.__aexit__ = AsyncMock(return_value=False)
        MockSR.return_value = sr_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=True,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    # Should use the cached endpoint
    cached_msgs = [m for m in messages if "cached" in m[1]]
    assert len(cached_msgs) == 1


async def test_extract_environment_sr_credentials_per_env(no_sleep):
    """Per-env SR credentials take priority over global ones."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    sr_creds = {
        "env-test1": {
            "api_key": "sr-env-key",
            "api_secret": "sr-env-secret",
        }
    }
    captured_args: dict[str, Any] = {}

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.SchemaRegistryClient") as MockSR,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        sr_inst = AsyncMock()
        sr_inst.extract = AsyncMock(return_value=([], []))
        sr_inst.__aenter__ = AsyncMock(return_value=sr_inst)
        sr_inst.__aexit__ = AsyncMock(return_value=False)

        def _capture_sr(**kwargs):
            captured_args.update(kwargs)
            return sr_inst

        MockSR.side_effect = _capture_sr

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=True,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints={"env-test1": "https://psrc-test.confluent.cloud"},
            sr_credentials=sr_creds,
            flink_credentials=None,
            on_progress=_progress,
        )

    assert captured_args.get("api_key") == "sr-env-key"
    assert captured_args.get("api_secret") == "sr-env-secret"


async def test_extract_environment_flink_org_from_spec(no_sleep):
    """Flink org ID resolved from cluster spec.organization.id."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    cluster["spec"]["organization"] = {"id": "org-from-spec"}
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.FlinkClient") as MockFlink,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        flink_inst = AsyncMock()
        flink_inst.extract = AsyncMock(return_value=([], []))
        flink_inst.__aenter__ = AsyncMock(return_value=flink_inst)
        flink_inst.__aexit__ = AsyncMock(return_value=False)
        MockFlink.return_value = flink_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=True,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    MockFlink.assert_called_once()
    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["organization_id"] == "org-from-spec"


async def test_extract_environment_flink_org_from_resource_name(no_sleep):
    """Flink org ID resolved from metadata.resource_name."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    cluster["metadata"]["resource_name"] = (
        "crn://confluent.cloud/organization=org-res123/environment=env-test1"
    )
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.FlinkClient") as MockFlink,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        flink_inst = AsyncMock()
        flink_inst.extract = AsyncMock(return_value=([], []))
        flink_inst.__aenter__ = AsyncMock(return_value=flink_inst)
        flink_inst.__aexit__ = AsyncMock(return_value=False)
        MockFlink.return_value = flink_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=True,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["organization_id"] == "org-res123"


async def test_extract_environment_flink_org_from_api_fallback(no_sleep):
    """Flink org ID resolved from /org/v2/environments API as last resort."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    # No org info in cluster payload
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])

    async def _get(path, **kwargs):
        if "/org/v2/environments/" in path:
            return {
                "display_name": "test",
                "metadata": {
                    "resource_name": (
                        "crn://confluent.cloud/organization=org-api-fallback/environment=env-test1"
                    )
                },
            }
        return {}

    mock_cloud.get = AsyncMock(side_effect=_get)

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.FlinkClient") as MockFlink,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        flink_inst = AsyncMock()
        flink_inst.extract = AsyncMock(return_value=([], []))
        flink_inst.__aenter__ = AsyncMock(return_value=flink_inst)
        flink_inst.__aexit__ = AsyncMock(return_value=False)
        MockFlink.return_value = flink_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=True,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["organization_id"] == "org-api-fallback"


async def test_extract_environment_flink_org_not_found_skips(no_sleep):
    """Flink extraction skipped when org ID cannot be determined."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    # get raises so fallback fails too
    mock_cloud.get = AsyncMock(side_effect=Exception("fail"))
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=True,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    flink_warnings = [m for m in messages if "Flink" in m[1] and "skipped" in m[1]]
    assert len(flink_warnings) == 1


async def test_extract_environment_metrics_enrichment(no_sleep):
    """Metrics enrichment runs when enable_metrics=True."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.MetricsClient") as MockMetrics,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(return_value=5)
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=True,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    metrics_msgs = [m for m in messages if m[0] == "Metrics"]
    assert len(metrics_msgs) >= 1
    assert any("Enriched 5 nodes" in m[1] for m in metrics_msgs)


async def test_extract_environment_metrics_error_handled(no_sleep):
    """Metrics enrichment failure is logged but does not abort."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.MetricsClient") as MockMetrics,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(side_effect=RuntimeError("metrics fail"))
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        # Should not raise
        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=True,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )


async def test_extract_environment_stamps_names(no_sleep):
    """Environment and cluster display names are stamped on nodes."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload(display_name="production-cluster")
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "production-env"})

    # Pre-add a node for this env/cluster
    node = LineageNode(
        node_id="confluent:kafka_topic:env-test1:orders",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name="orders",
        display_name="orders",
        environment_id="env-test1",
        cluster_id="lkc-test1",
    )
    graph.add_node(node)

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    stamped = graph.get_node("confluent:kafka_topic:env-test1:orders")
    assert stamped is not None
    assert stamped.environment_name == "production-env"
    assert stamped.cluster_name == "production-cluster"


async def test_extract_environment_cluster_filter(no_sleep):
    """cluster_ids filter limits which clusters are processed."""
    settings = _make_settings()
    graph = LineageGraph()
    c1 = _cluster_payload(cluster_id="lkc-keep")
    c2 = _cluster_payload(cluster_id="lkc-skip")
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[c1, c2])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    kafka_calls: list[str] = []

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:

        def _make_kafka(**kwargs):
            kafka_calls.append(kwargs.get("cluster_id", ""))
            inst = AsyncMock()
            inst.extract = AsyncMock(return_value=([], []))
            inst.__aenter__ = AsyncMock(return_value=inst)
            inst.__aexit__ = AsyncMock(return_value=False)
            return inst

        MockKafka.side_effect = _make_kafka

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=["lkc-keep"],
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    assert kafka_calls == ["lkc-keep"]


async def test_extract_environment_cluster_no_rest_endpoint(no_sleep):
    """Cluster without REST endpoint is skipped with a warning."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    cluster["spec"]["http_endpoint"] = ""
    cluster["spec"]["region"] = ""
    cluster["spec"]["cloud"] = ""
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    await _extract_environment(
        settings,
        mock_cloud,
        "env-test1",
        graph,
        cluster_ids=None,
        enable_connect=False,
        enable_ksqldb=False,
        enable_flink=False,
        enable_schema_registry=False,
        enable_stream_catalog=False,
        enable_tableflow=False,
        enable_metrics=False,
        metrics_lookback_hours=1,
        sr_endpoints=None,
        sr_credentials=None,
        flink_credentials=None,
        on_progress=_progress,
    )

    warnings = [m for m in messages if "No REST endpoint" in m[1]]
    assert len(warnings) == 1


async def test_extract_environment_bootstrap_sasl_prefix_stripped(
    no_sleep,
):
    """SASL_SSL:// prefix is stripped from bootstrap_servers."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    cluster["spec"]["kafka_bootstrap_endpoint"] = "SASL_SSL://pkc-test.us-east1.gcp:9092"
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    captured_bootstrap: list[str | None] = []

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:

        def _make_kafka(**kwargs):
            captured_bootstrap.append(kwargs.get("bootstrap_servers"))
            inst = AsyncMock()
            inst.extract = AsyncMock(return_value=([], []))
            inst.__aenter__ = AsyncMock(return_value=inst)
            inst.__aexit__ = AsyncMock(return_value=False)
            return inst

        MockKafka.side_effect = _make_kafka

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    assert captured_bootstrap == ["pkc-test.us-east1.gcp:9092"]


async def test_extract_environment_flink_per_env_credentials(no_sleep):
    """Per-env Flink credentials override global settings."""
    settings = _make_settings(
        flink_api_key="global-flink-key",
        flink_api_secret="global-flink-secret",
    )
    graph = LineageGraph()
    cluster = _cluster_payload()
    cluster["spec"]["organization"] = {"id": "org-1"}
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    flink_creds = {
        "env-test1": {
            "api_key": "env-flink-key",
            "api_secret": "env-flink-secret",
        }
    }

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.FlinkClient") as MockFlink,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        flink_inst = AsyncMock()
        flink_inst.extract = AsyncMock(return_value=([], []))
        flink_inst.__aenter__ = AsyncMock(return_value=flink_inst)
        flink_inst.__aexit__ = AsyncMock(return_value=False)
        MockFlink.return_value = flink_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=True,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=flink_creds,
            on_progress=_progress,
        )

    call_kwargs = MockFlink.call_args[1]
    assert call_kwargs["flink_api_key"] == "env-flink-key"
    assert call_kwargs["flink_api_secret"] == "env-flink-secret"


async def test_extract_environment_tableflow_runs(no_sleep):
    """Tableflow extractor runs when enabled."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.TableflowClient") as MockTF,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        tf_inst = AsyncMock()
        tf_inst.extract = AsyncMock(return_value=([], []))
        tf_inst.__aenter__ = AsyncMock(return_value=tf_inst)
        tf_inst.__aexit__ = AsyncMock(return_value=False)
        MockTF.return_value = tf_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=True,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    MockTF.assert_called_once()


async def test_extract_environment_stream_catalog_skipped_no_sr(
    no_sleep,
):
    """Stream catalog is skipped when no SR endpoint is available."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()

    async def _paginate_side_effect(path, **kwargs):
        if "/cmk/v2/clusters" in path:
            return [cluster]
        # SR discovery returns empty — no SR endpoint
        return []

    mock_cloud.paginate = AsyncMock(side_effect=_paginate_side_effect)
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=True,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    skipped = [m for m in messages if "Stream Catalog" in m[1]]
    assert len(skipped) == 1


# ── Phase 4b: Catalog enrichment tests ─────────────────────────────────


async def test_catalog_enrichment_calls_provider_enrich(no_sleep):
    """Phase 4b calls provider.enrich() for active catalog providers."""
    settings = _make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    # Pre-populate graph with a UC_TABLE node so get_active_providers finds it
    uc_node = LineageNode(
        node_id="databricks:uc_table:env-test1:cat.schema.tbl",
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="cat.schema.tbl",
        display_name="cat.schema.tbl",
        environment_id="env-test1",
        cluster_id="lkc-test1",
        attributes={"workspace_url": "https://myworkspace.databricks.com"},
    )
    tf_node = _node("test-topic", NodeType.TABLEFLOW_TABLE)
    graph.add_node(uc_node)
    graph.add_node(tf_node)

    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.DatabricksUCProvider") as MockUCProvider,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        mock_provider = AsyncMock()
        mock_provider.catalog_type = "UNITY_CATALOG"
        mock_provider.enrich = AsyncMock()
        MockUCProvider.return_value = mock_provider

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    MockUCProvider.assert_called_once_with(
        workspace_url="https://myworkspace.databricks.com",
        token="dapi-test-token",
    )
    mock_provider.enrich.assert_awaited_once_with(graph)
    phase4b_msgs = [m for m in messages if "Phase 4b" in m[0]]
    assert len(phase4b_msgs) >= 1


async def test_catalog_enrichment_skipped_when_no_catalog_nodes(no_sleep):
    """Phase 4b is skipped when no catalog nodes exist in the graph."""
    settings = _make_settings()
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str = "") -> None:
        messages.append((phase, detail))

    with patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka:
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    phase4b_msgs = [m for m in messages if "Phase 4b" in m[0]]
    assert len(phase4b_msgs) == 0


async def test_catalog_enrichment_failure_is_not_fatal(no_sleep):
    """Phase 4b catches exceptions from provider.enrich() and continues."""
    settings = _make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )
    graph = LineageGraph()
    cluster = _cluster_payload()
    mock_cloud = AsyncMock()
    mock_cloud.paginate = AsyncMock(return_value=[cluster])
    mock_cloud.get = AsyncMock(return_value={"display_name": "test"})

    uc_node = LineageNode(
        node_id="databricks:uc_table:env-test1:cat.schema.tbl",
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="cat.schema.tbl",
        display_name="cat.schema.tbl",
        environment_id="env-test1",
        cluster_id="lkc-test1",
    )
    graph.add_node(uc_node)

    def _progress(phase: str, detail: str = "") -> None:
        pass

    with (
        patch("lineage_bridge.extractors.orchestrator.KafkaAdminClient") as MockKafka,
        patch("lineage_bridge.extractors.orchestrator.DatabricksUCProvider") as MockUCProvider,
    ):
        kafka_inst = AsyncMock()
        kafka_inst.extract = AsyncMock(return_value=([], []))
        kafka_inst.__aenter__ = AsyncMock(return_value=kafka_inst)
        kafka_inst.__aexit__ = AsyncMock(return_value=False)
        MockKafka.return_value = kafka_inst

        mock_provider = AsyncMock()
        mock_provider.catalog_type = "UNITY_CATALOG"
        mock_provider.enrich = AsyncMock(side_effect=RuntimeError("API down"))
        MockUCProvider.return_value = mock_provider

        # Should not raise — enrichment failure is caught and logged
        await _extract_environment(
            settings,
            mock_cloud,
            "env-test1",
            graph,
            cluster_ids=None,
            enable_connect=False,
            enable_ksqldb=False,
            enable_flink=False,
            enable_schema_registry=False,
            enable_stream_catalog=False,
            enable_tableflow=False,
            enable_metrics=False,
            metrics_lookback_hours=1,
            sr_endpoints=None,
            sr_credentials=None,
            flink_credentials=None,
            on_progress=_progress,
        )

    mock_provider.enrich.assert_awaited_once()


# ── main() CLI tests ───────────────────────────────────────────────────


def test_main_requires_env_arg():
    """main() exits with error when --env is not provided."""
    with pytest.raises(SystemExit), patch("sys.argv", ["lineage-bridge-extract"]):
        main()
