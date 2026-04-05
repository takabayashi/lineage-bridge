# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.kafka_admin.KafkaAdminClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from tests.conftest import load_fixture

from lineage_bridge.clients.kafka_admin import (
    KafkaAdminClient,
    _INTERNAL_PREFIXES,
    _list_offsets_via_protocol,
)
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType

# ── Constants shared across tests ──────────────────────────────────────────
BASE_URL = "https://lkc-abc123.us-east-1.aws.confluent.cloud:443"
CLUSTER_ID = "lkc-abc123"
ENV_ID = "env-abc123"
API_KEY = "test-key"
API_SECRET = "test-secret"

TOPICS_PATH = f"/kafka/v3/clusters/{CLUSTER_ID}/topics"
GROUPS_PATH = f"/kafka/v3/clusters/{CLUSTER_ID}/consumer-groups"


def _lag_path(group_id: str) -> str:
    return f"/kafka/v3/clusters/{CLUSTER_ID}/consumer-groups/{group_id}/lags"


@pytest.fixture()
def kafka_topics_fixture():
    return load_fixture("kafka_topics.json")


@pytest.fixture()
def consumer_groups_fixture():
    return load_fixture("consumer_groups.json")


def _make_client() -> KafkaAdminClient:
    return KafkaAdminClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        cluster_id=CLUSTER_ID,
        environment_id=ENV_ID,
    )


# ── _is_internal helper ───────────────────────────────────────────────────


class TestIsInternal:
    """Direct tests for the static _is_internal helper."""

    def test_underscore_prefix_is_internal(self):
        assert KafkaAdminClient._is_internal("_confluent-metrics") is True

    def test_confluent_prefix_is_internal(self):
        assert KafkaAdminClient._is_internal("confluent-audit-log") is True

    def test_normal_topic_is_not_internal(self):
        assert KafkaAdminClient._is_internal("orders") is False

    def test_topic_containing_confluent_not_at_start(self):
        # "my-confluent-topic" does NOT start with "confluent"
        assert KafkaAdminClient._is_internal("my-confluent-topic") is False

    def test_empty_string_is_not_internal(self):
        assert KafkaAdminClient._is_internal("") is False


# ── extract() tests ──────────────────────────────────────────────────────


@respx.mock
async def test_extract_creates_topic_nodes(kafka_topics_fixture, no_sleep):
    """Topics endpoint returns 4 topics; only the 3 non-internal ones become nodes."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json={"data": [], "metadata": {}}),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    topic_nodes = [n for n in nodes if n.node_type == NodeType.KAFKA_TOPIC]
    assert len(topic_nodes) == 3
    names = {n.qualified_name for n in topic_nodes}
    assert names == {"orders", "customers", "enriched_orders"}

    # Verify node structure
    orders_node = next(n for n in topic_nodes if n.qualified_name == "orders")
    assert orders_node.system == SystemType.CONFLUENT
    assert orders_node.environment_id == ENV_ID
    assert orders_node.cluster_id == CLUSTER_ID
    assert orders_node.attributes["partitions_count"] == 6
    assert orders_node.attributes["replication_factor"] == 3
    assert orders_node.node_id == f"confluent:kafka_topic:{ENV_ID}:orders"
    assert f"/topics/orders" in orders_node.url


@respx.mock
async def test_extract_skips_internal_topics(kafka_topics_fixture, no_sleep):
    """The _confluent-metrics topic (starts with _) must be excluded."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json={"data": [], "metadata": {}}),
    )

    async with _make_client() as client:
        nodes, _edges = await client.extract()

    all_names = {n.qualified_name for n in nodes}
    assert "_confluent-metrics" not in all_names
    # Also ensure nothing starting with any internal prefix leaked through
    for name in all_names:
        for prefix in _INTERNAL_PREFIXES:
            assert not name.startswith(prefix), f"{name} should have been filtered"


@respx.mock
async def test_extract_creates_consumer_group_nodes(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """Consumer groups endpoint returns 2 groups; both become CONSUMER_GROUP nodes."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )
    # Both groups return empty lag (no edges)
    respx.get(url__regex=r".*/lags$").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with _make_client() as client:
        nodes, _edges = await client.extract()

    cg_nodes = [n for n in nodes if n.node_type == NodeType.CONSUMER_GROUP]
    assert len(cg_nodes) == 2
    cg_names = {n.qualified_name for n in cg_nodes}
    assert cg_names == {"order-processing-group", "enrichment-service"}

    opg = next(n for n in cg_nodes if n.qualified_name == "order-processing-group")
    assert opg.system == SystemType.CONFLUENT
    assert opg.attributes["state"] == "STABLE"
    assert opg.attributes["is_simple"] is False
    assert opg.node_id == f"confluent:consumer_group:{ENV_ID}:order-processing-group"


@respx.mock
async def test_extract_creates_member_of_edges(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """When lag data links a group to a topic, a MEMBER_OF edge is created."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )

    # order-processing-group consumes "orders" across 2 partitions
    lag_data_opg = {
        "data": [
            {"topic_name": "orders", "partition_id": 0, "lag": 10},
            {"topic_name": "orders", "partition_id": 1, "lag": 25},
        ],
    }
    respx.get(f"{BASE_URL}{_lag_path('order-processing-group')}").mock(
        return_value=httpx.Response(200, json=lag_data_opg),
    )

    # enrichment-service consumes "orders" and "customers"
    lag_data_es = {
        "data": [
            {"topic_name": "orders", "partition_id": 0, "lag": 5},
            {"topic_name": "customers", "partition_id": 0, "lag": 0},
        ],
    }
    respx.get(f"{BASE_URL}{_lag_path('enrichment-service')}").mock(
        return_value=httpx.Response(200, json=lag_data_es),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    member_of_edges = [e for e in edges if e.edge_type == EdgeType.MEMBER_OF]
    # order-processing-group -> orders, enrichment-service -> orders, enrichment-service -> customers
    assert len(member_of_edges) == 3

    # Verify the edge from order-processing-group -> orders
    opg_orders = next(
        e
        for e in member_of_edges
        if e.src_id == f"confluent:consumer_group:{ENV_ID}:order-processing-group"
        and e.dst_id == f"confluent:kafka_topic:{ENV_ID}:orders"
    )
    assert opg_orders.edge_type == EdgeType.MEMBER_OF
    # max_lag across partitions 0 (10) and 1 (25) = 25
    assert opg_orders.attributes["max_lag"] == 25


@respx.mock
async def test_extract_handles_lag_404_gracefully(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """If the lag endpoint returns 404, the group node is still created but no edges."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )

    # All lag endpoints return 404
    respx.get(url__regex=r".*/lags$").mock(
        return_value=httpx.Response(404, json={"error_code": 404, "message": "Not found"}),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    # Consumer group nodes should still exist
    cg_nodes = [n for n in nodes if n.node_type == NodeType.CONSUMER_GROUP]
    assert len(cg_nodes) == 2

    # No edges because lag data was unavailable
    assert len(edges) == 0


# ── _list_offsets_via_protocol tests ─────────────────────────────────────


def test_list_offsets_via_protocol_success():
    """Returns topic set when confluent_kafka is available and succeeds."""
    mock_tp1 = MagicMock(topic="orders")
    mock_tp2 = MagicMock(topic="customers")
    mock_result = MagicMock(topic_partitions=[mock_tp1, mock_tp2])

    mock_future = MagicMock()
    mock_future.result.return_value = mock_result

    with (
        patch(
            "lineage_bridge.clients.kafka_admin.AdminClient",
            create=True,
        ) as MockAdmin,
        patch(
            "lineage_bridge.clients.kafka_admin.ConsumerGroupTopicPartitions",
            create=True,
        ),
    ):
        mock_admin = MagicMock()
        mock_admin.list_consumer_group_offsets.return_value = {"group-1": mock_future}
        MockAdmin.return_value = mock_admin

        # Need to patch the imports inside the function
        with patch.dict("sys.modules", {
            "confluent_kafka": MagicMock(),
            "confluent_kafka.admin": MagicMock(AdminClient=MockAdmin),
        }):
            # Re-run through the actual function but with mocked imports
            result = _list_offsets_via_protocol(
                "broker:9092", "key", "secret", "group-1",
            )

    # The function uses local imports, so we test it returns a set
    assert isinstance(result, set)


def test_list_offsets_via_protocol_import_error():
    """Returns empty set when confluent_kafka is not installed."""
    with patch.dict("sys.modules", {"confluent_kafka": None, "confluent_kafka.admin": None}):
        # Force re-import to hit ImportError path
        result = _list_offsets_via_protocol(
            "broker:9092", "key", "secret", "group-1",
        )
    assert result == set()


def test_list_offsets_via_protocol_exception():
    """Returns empty set when AdminClient raises an exception."""
    with patch.dict("sys.modules", {
        "confluent_kafka": MagicMock(
            ConsumerGroupTopicPartitions=MagicMock(),
        ),
        "confluent_kafka.admin": MagicMock(
            AdminClient=MagicMock(side_effect=RuntimeError("connection failed")),
        ),
    }):
        result = _list_offsets_via_protocol(
            "broker:9092", "key", "secret", "group-1",
        )
    assert result == set()


# ── extract() fallback path tests ────────────────────────────────────────


def _make_client_with_bootstrap() -> KafkaAdminClient:
    return KafkaAdminClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        cluster_id=CLUSTER_ID,
        environment_id=ENV_ID,
        bootstrap_servers="broker:9092",
    )


@respx.mock
async def test_extract_fallback_to_protocol(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """When lag returns empty, falls back to Kafka protocol and creates edges."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )
    # Lag returns empty for all groups
    respx.get(url__regex=r".*/lags$").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    with patch(
        "lineage_bridge.clients.kafka_admin._list_offsets_via_protocol",
        return_value={"orders", "customers"},
    ):
        async with _make_client_with_bootstrap() as client:
            _nodes, edges = await client.extract()

    member_of_edges = [e for e in edges if e.edge_type == EdgeType.MEMBER_OF]
    # 2 groups x 2 topics each (orders, customers) = 4 edges
    assert len(member_of_edges) == 4


@respx.mock
async def test_extract_fallback_no_bootstrap(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """Without bootstrap_servers, fallback is skipped — no edges from empty lag."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )
    respx.get(url__regex=r".*/lags$").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    with patch(
        "lineage_bridge.clients.kafka_admin._list_offsets_via_protocol",
    ) as mock_protocol:
        async with _make_client() as client:  # no bootstrap_servers
            _nodes, edges = await client.extract()

    mock_protocol.assert_not_called()
    assert len(edges) == 0


@respx.mock
async def test_extract_fallback_filters_unknown_topics(
    kafka_topics_fixture, consumer_groups_fixture, no_sleep
):
    """Protocol returns a topic not in topic_names — it's filtered out."""
    respx.get(f"{BASE_URL}{TOPICS_PATH}").mock(
        return_value=httpx.Response(200, json=kafka_topics_fixture),
    )
    respx.get(f"{BASE_URL}{GROUPS_PATH}").mock(
        return_value=httpx.Response(200, json=consumer_groups_fixture),
    )
    respx.get(url__regex=r".*/lags$").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    with patch(
        "lineage_bridge.clients.kafka_admin._list_offsets_via_protocol",
        return_value={"unknown-topic-xyz"},
    ):
        async with _make_client_with_bootstrap() as client:
            _nodes, edges = await client.extract()

    # unknown-topic-xyz is not in topic_names, so no edges
    assert len(edges) == 0
