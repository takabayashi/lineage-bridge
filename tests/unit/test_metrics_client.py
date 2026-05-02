# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.metrics.MetricsClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.metrics import MetricsClient
from lineage_bridge.models.graph import (
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
            json={"data": [{"metric.topic": "orders", "value": 1024.0}]},
        ),
        httpx.Response(
            200,
            json={"data": [{"metric.topic": "orders", "value": 512.0}]},
        ),
        httpx.Response(
            200,
            json={"data": [{"metric.topic": "orders", "value": 100.0}]},
        ),
        httpx.Response(
            200,
            json={"data": [{"metric.topic": "orders", "value": 50.0}]},
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
        httpx.Response(200, json={"data": [{"resource.connector.id": "my-sink", "value": 42.0}]}),
        httpx.Response(200, json={"data": [{"resource.connector.id": "my-sink", "value": 7.0}]}),
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


# ── extended enrichment (Flink, consumer groups, tableflow, catalog, ksqldb) ───


@respx.mock
async def test_enrich_connector_uses_confluent_id(metrics_client):
    """Connector enrichment matches by `confluent_id` (lcc-XXX) — the
    user-facing display_name doesn't match what Telemetry groups by."""
    graph = LineageGraph()
    conn = _make_node(
        "lb-uc-postgres-sink",
        NodeType.CONNECTOR,
        attributes={"confluent_id": "lcc-7w7m2w"},
    )
    graph.add_node(conn)

    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")
    # Topic queries (4) — empty. Connector queries (2) — populated by lcc-id.
    # Flink queries (2) — empty.
    route.side_effect = [
        *(httpx.Response(200, json={"data": []}) for _ in range(4)),
        httpx.Response(
            200, json={"data": [{"resource.connector.id": "lcc-7w7m2w", "value": 99.0}]}
        ),
        httpx.Response(
            200, json={"data": [{"resource.connector.id": "lcc-7w7m2w", "value": 11.0}]}
        ),
        *(httpx.Response(200, json={"data": []}) for _ in range(2)),
    ]

    await metrics_client.enrich(graph, CLUSTER_ID)
    a = graph.get_node(conn.node_id).attributes
    assert a["metrics_received_records"] == 99.0
    assert a["metrics_sent_records"] == 11.0
    assert a["metrics_active"] is True


@respx.mock
async def test_enrich_flink_jobs(metrics_client):
    """Flink statement IDs (matching qualified_name) get records-in/out."""
    graph = LineageGraph()
    job = _make_node("statement-abc-123", NodeType.FLINK_JOB)
    # qualified_name from _make_node is "env-1:<name>" — Telemetry returns
    # the bare statement ID, so override.
    job = job.model_copy(update={"qualified_name": "statement-abc-123"})
    graph.add_node(job)

    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")
    route.side_effect = [
        *(httpx.Response(200, json={"data": []}) for _ in range(6)),  # topics + connectors
        httpx.Response(
            200,
            json={"data": [{"resource.flink_statement.id": "statement-abc-123", "value": 500.0}]},
        ),
        httpx.Response(
            200,
            json={"data": [{"resource.flink_statement.id": "statement-abc-123", "value": 480.0}]},
        ),
    ]

    await metrics_client.enrich(graph, CLUSTER_ID)
    a = graph.get_node(job.node_id).attributes
    assert a["metrics_received_records"] == 500.0
    assert a["metrics_sent_records"] == 480.0
    assert a["metrics_active"] is True


@respx.mock
async def test_enrich_consumer_group_aggregates_lag_from_edges(metrics_client):
    """CONSUMER_GROUP node gets metrics_total_lag summed from MEMBER_OF edges."""
    from lineage_bridge.models.graph import EdgeType, LineageEdge

    graph = LineageGraph()
    group = _make_node("orders-consumer", NodeType.CONSUMER_GROUP, attributes={"state": "STABLE"})
    topic_a = _make_node("orders", NodeType.KAFKA_TOPIC)
    topic_b = _make_node("events", NodeType.KAFKA_TOPIC)
    for n in (group, topic_a, topic_b):
        graph.add_node(n)
    graph.add_edge(
        LineageEdge(
            src_id=group.node_id,
            dst_id=topic_a.node_id,
            edge_type=EdgeType.MEMBER_OF,
            attributes={"max_lag": 42},
        )
    )
    graph.add_edge(
        LineageEdge(
            src_id=group.node_id,
            dst_id=topic_b.node_id,
            edge_type=EdgeType.MEMBER_OF,
            attributes={"max_lag": 8},
        )
    )

    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await metrics_client.enrich(graph, CLUSTER_ID)
    a = graph.get_node(group.node_id).attributes
    assert a["metrics_total_lag"] == 50
    assert a["metrics_active"] is True
    assert a["metrics_window_hours"] == 1


@respx.mock
async def test_enrich_tableflow_inherits_from_topic(metrics_client):
    """TABLEFLOW_TABLE inherits topic metrics via the MATERIALIZES edge."""
    from lineage_bridge.models.graph import EdgeType, LineageEdge

    graph = LineageGraph()
    topic = _make_node("orders", NodeType.KAFKA_TOPIC)
    tf = _make_node("orders-tf", NodeType.TABLEFLOW_TABLE)
    graph.add_node(topic)
    graph.add_node(tf)
    graph.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=tf.node_id, edge_type=EdgeType.MATERIALIZES)
    )

    route = respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query")
    route.side_effect = [
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 1024.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 0.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 100.0}]}),
        httpx.Response(200, json={"data": [{"metric.topic": "orders", "value": 0.0}]}),
        *(httpx.Response(200, json={"data": []}) for _ in range(4)),
    ]

    await metrics_client.enrich(graph, CLUSTER_ID)
    tf_attrs = graph.get_node(tf.node_id).attributes
    assert tf_attrs["metrics_received_bytes"] == 1024.0
    assert tf_attrs["metrics_received_records"] == 100.0
    assert tf_attrs["metrics_active"] is True


@respx.mock
async def test_enrich_catalog_table_promotes_num_rows_bytes(metrics_client):
    """CATALOG_TABLE num_rows / num_bytes promoted to standard metrics_* keys."""
    graph = LineageGraph()
    cat = LineageNode(
        node_id="aws:catalog_table:env-1:db.orders",
        system=SystemType.AWS,
        node_type=NodeType.CATALOG_TABLE,
        catalog_type="AWS_GLUE",
        qualified_name="db.orders",
        display_name="orders",
        environment_id="env-1",
        attributes={"num_rows": "582", "num_bytes": "43181"},
    )
    graph.add_node(cat)

    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await metrics_client.enrich(graph, CLUSTER_ID)
    a = graph.get_node(cat.node_id).attributes
    assert a["metrics_received_records"] == 582.0
    assert a["metrics_received_bytes"] == 43181.0
    assert a["metrics_active"] is True  # num_rows > 0


@respx.mock
async def test_enrich_ksqldb_query_active_when_running(metrics_client):
    """KSQLDB_QUERY metrics_active reflects state."""
    graph = LineageGraph()
    running = _make_node("q-1", NodeType.KSQLDB_QUERY, attributes={"state": "RUNNING"})
    paused = _make_node("q-2", NodeType.KSQLDB_QUERY, attributes={"state": "PAUSED"})
    graph.add_node(running)
    graph.add_node(paused)

    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await metrics_client.enrich(graph, CLUSTER_ID)
    assert graph.get_node(running.node_id).attributes["metrics_active"] is True
    assert graph.get_node(paused.node_id).attributes["metrics_active"] is False


@respx.mock
async def test_enrich_skips_schema_and_external_dataset(metrics_client):
    """SCHEMA + EXTERNAL_DATASET are intentionally NOT enriched (no real metric source)."""
    graph = LineageGraph()
    schema = _make_node("orders-value", NodeType.SCHEMA)
    ext = LineageNode(
        node_id="external:external_dataset:env-1:s3://bucket/",
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="s3://bucket/",
        display_name="bucket",
        environment_id="env-1",
        attributes={},
    )
    graph.add_node(schema)
    graph.add_node(ext)

    respx.post(f"{TELEMETRY_BASE}/v2/metrics/cloud/query").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await metrics_client.enrich(graph, CLUSTER_ID)
    for n in (schema, ext):
        attrs = graph.get_node(n.node_id).attributes
        assert not any(k.startswith("metrics_") for k in attrs), (
            f"{n.node_type.value} should have no metrics_* attrs"
        )
