# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.metrics.MetricsClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.metrics import MetricsClient, MetricsSummary
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

API_KEY = "metrics-key"
API_SECRET = "metrics-secret"
TELEMETRY_BASE = "https://api.telemetry.confluent.cloud"
CLUSTER_ID = "lkc-test"


@pytest.fixture()
def metrics_client():
    return MetricsClient(api_key=API_KEY, api_secret=API_SECRET, lookback_hours=1)


def _make_node(
    name: str,
    node_type: NodeType,
    cluster_id: str = CLUSTER_ID,
    attributes: dict | None = None,
) -> LineageNode:
    nid = f"confluent:{node_type.value}:env-1:{name}"
    return LineageNode(
        node_id=nid,
        system=SystemType.CONFLUENT,
        node_type=node_type,
        qualified_name=f"env-1:{name}",
        display_name=name,
        environment_id="env-1",
        cluster_id=cluster_id,
        attributes=attributes or {},
    )


# ── query_topic_metrics ──────────────────────────────────────────────────


@respx.mock
async def test_query_topic_metrics_aggregates(metrics_client):
    """query_topic_metrics collects and maps metrics by topic name."""
    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")

    # Four calls: received_bytes, sent_bytes, received_records, sent_records
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "data": [{"metric.topic": "orders", "value": 1024.0}]
            },
        ),
        httpx.Response(
            200,
            json={
                "data": [{"metric.topic": "orders", "value": 512.0}]
            },
        ),
        httpx.Response(
            200,
            json={
                "data": [{"metric.topic": "orders", "value": 100.0}]
            },
        ),
        httpx.Response(
            200,
            json={
                "data": [{"metric.topic": "orders", "value": 50.0}]
            },
        ),
    ]

    summaries = await metrics_client.query_topic_metrics(CLUSTER_ID)

    assert "orders" in summaries
    s = summaries["orders"]
    assert s.received_bytes == 1024.0
    assert s.sent_bytes == 512.0
    assert s.received_records == 100.0
    assert s.sent_records == 50.0
    assert s.is_active is True


@respx.mock
async def test_query_topic_metrics_empty(metrics_client):
    """query_topic_metrics returns empty dict when there is no data."""
    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    summaries = await metrics_client.query_topic_metrics(CLUSTER_ID)
    assert summaries == {}


# ── enrich ───────────────────────────────────────────────────────────────


@respx.mock
async def test_enrich_updates_topic_and_connector_nodes(metrics_client):
    """enrich writes metric attributes onto matching topic and connector nodes."""
    graph = LineageGraph()
    topic_node = _make_node("orders", NodeType.KAFKA_TOPIC)
    connector_node = _make_node(
        "my-sink",
        NodeType.CONNECTOR,
        attributes={"connector_id": "my-sink"},
    )
    graph.add_node(topic_node)
    graph.add_node(connector_node)

    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")
    # 4 topic metric queries + 2 connector metric queries = 6 calls
    route.side_effect = [
        # Topic metrics
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 100.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 200.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 10.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 5.0}]}),
        # Connector metrics
        httpx.Response(
            200, json={"data": [{"resource.connector.id": "my-sink", "value": 42.0}]}
        ),
        httpx.Response(
            200, json={"data": [{"resource.connector.id": "my-sink", "value": 7.0}]}
        ),
    ]

    count = await metrics_client.enrich(graph, CLUSTER_ID)

    assert count == 2

    enriched_topic = graph.get_node(topic_node.node_id)
    assert enriched_topic is not None
    assert enriched_topic.attributes["metrics_received_bytes"] == 100.0
    assert enriched_topic.attributes["metrics_sent_bytes"] == 200.0
    assert enriched_topic.attributes["metrics_active"] is True
    assert enriched_topic.attributes["metrics_window_hours"] == 1

    enriched_conn = graph.get_node(connector_node.node_id)
    assert enriched_conn is not None
    assert enriched_conn.attributes["metrics_received_records"] == 42.0
    assert enriched_conn.attributes["metrics_sent_records"] == 7.0
    assert enriched_conn.attributes["metrics_active"] is True


@respx.mock
async def test_enrich_returns_count(metrics_client):
    """enrich returns 0 when no nodes match the cluster."""
    graph = LineageGraph()
    # Node in a different cluster
    node = _make_node("orders", NodeType.KAFKA_TOPIC, cluster_id="lkc-other")
    graph.add_node(node)

    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    count = await metrics_client.enrich(graph, CLUSTER_ID)
    assert count == 0


@respx.mock
async def test_enrich_skips_other_clusters(metrics_client):
    """enrich does not touch nodes belonging to a different cluster."""
    graph = LineageGraph()
    other_node = _make_node("orders", NodeType.KAFKA_TOPIC, cluster_id="lkc-other")
    matching_node = _make_node("events", NodeType.KAFKA_TOPIC, cluster_id=CLUSTER_ID)
    graph.add_node(other_node)
    graph.add_node(matching_node)

    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")
    # Topic metrics queries (4 for topic, 2 for connector)
    route.side_effect = [
        httpx.Response(200, json={"data": [{"metric.topic": "events", "value": 99.0}]}),
        httpx.Response(200, json={"data": []}),
        httpx.Response(200, json={"data": []}),
        httpx.Response(200, json={"data": []}),
        # Connector metrics
        httpx.Response(200, json={"data": []}),
        httpx.Response(200, json={"data": []}),
    ]

    count = await metrics_client.enrich(graph, CLUSTER_ID)
    assert count == 1

    # The other cluster's node should NOT have metrics attributes
    unchanged = graph.get_node(other_node.node_id)
    assert unchanged is not None
    assert "metrics_received_bytes" not in unchanged.attributes


@respx.mock
async def test_query_failure_returns_empty(metrics_client):
    """When the metrics API returns an error, query_topic_metrics returns empty."""
    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(500, json={"error": "internal"})
    )

    summaries = await metrics_client.query_topic_metrics(CLUSTER_ID)
    assert summaries == {}
