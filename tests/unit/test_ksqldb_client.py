# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the ksqlDB client."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.ksqldb import KsqlDBClient, _strip_quotes
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType
from tests.conftest import load_fixture


# ── Constants ──────────────────────────────────────────────────────────

ENV_ID = "env-abc123"
CLOUD_URL = "https://api.confluent.cloud"
KSQL_ENDPOINT = "https://pksqlc-abc.us-east-1.aws.confluent.cloud"
CLUSTER_ID = "lksqlc-abc"
KAFKA_CLUSTER_ID = "lkc-xyz"
API_KEY = "test-key"
API_SECRET = "test-secret"


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture()
def ksql_client():
    return KsqlDBClient(
        cloud_api_key=API_KEY,
        cloud_api_secret=API_SECRET,
        environment_id=ENV_ID,
        cloud_base_url=CLOUD_URL,
    )


@pytest.fixture()
def queries_payload() -> list:
    return load_fixture("ksqldb_queries.json")


@pytest.fixture()
def describe_payload() -> list:
    return load_fixture("ksqldb_describe.json")


def _clusters_response(clusters: list | None = None) -> dict:
    """Build a paginated clusters API response."""
    if clusters is None:
        clusters = [
            {
                "id": CLUSTER_ID,
                "spec": {
                    "http_endpoint": KSQL_ENDPOINT,
                    "kafka_cluster": {"id": KAFKA_CLUSTER_ID},
                },
                "status": {"http_endpoint": KSQL_ENDPOINT},
            }
        ]
    return {"data": clusters, "metadata": {}}


# ── _parse_source_names ────────────────────────────────────────────────


class TestParseSourceNames:
    def test_simple_from(self):
        sql = "CREATE STREAM foo AS SELECT * FROM bar EMIT CHANGES;"
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["bar"]

    def test_join(self):
        sql = (
            "CREATE STREAM enriched AS SELECT o.id, c.name "
            "FROM orders o INNER JOIN customers c ON o.cid = c.id EMIT CHANGES;"
        )
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["orders", "customers"]

    def test_left_join(self):
        sql = (
            "CREATE STREAM out AS SELECT * "
            "FROM stream_a a LEFT JOIN table_b b ON a.id = b.id EMIT CHANGES;"
        )
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["stream_a", "table_b"]

    def test_dedup(self):
        """If FROM and JOIN reference the same stream, it should appear once."""
        sql = (
            "CREATE STREAM out AS SELECT * "
            "FROM orders o JOIN orders o2 ON o.id = o2.id EMIT CHANGES;"
        )
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["orders"]

    def test_backtick_quoted_names(self):
        sql = "CREATE STREAM out AS SELECT * FROM `MyStream` EMIT CHANGES;"
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["MyStream"]

    def test_double_quoted_names(self):
        sql = 'CREATE STREAM out AS SELECT * FROM "MyStream" EMIT CHANGES;'
        result = KsqlDBClient._parse_source_names(sql)
        assert result == ["MyStream"]

    def test_backtick_hyphenated_name_known_limitation(self):
        """The regex [\w.]+ does not capture hyphens, so backtick-quoted
        names with hyphens are truncated. Document this known limitation."""
        sql = "CREATE STREAM out AS SELECT * FROM `My-Stream` EMIT CHANGES;"
        result = KsqlDBClient._parse_source_names(sql)
        # Only captures up to the hyphen due to regex pattern.
        assert result == ["My"]

    def test_no_sources(self):
        sql = "SHOW QUERIES;"
        result = KsqlDBClient._parse_source_names(sql)
        assert result == []

    def test_fixture_query(self, queries_payload):
        """Parse the SQL from the fixture and verify both source names."""
        sql = queries_payload[0]["queries"][0]["queryString"]
        result = KsqlDBClient._parse_source_names(sql)
        assert "orders" in result
        assert "customers" in result
        assert len(result) == 2


# ── _strip_quotes helper ──────────────────────────────────────────────


class TestStripQuotes:
    def test_backticks(self):
        assert _strip_quotes("`foo`") == "foo"

    def test_double_quotes(self):
        assert _strip_quotes('"foo"') == "foo"

    def test_no_quotes(self):
        assert _strip_quotes("foo") == "foo"


# ── extract ────────────────────────────────────────────────────────────


class TestExtract:
    @respx.mock
    async def test_extract_creates_query_nodes_and_edges(
        self, ksql_client, queries_payload
    ):
        # Mock cluster discovery.
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response())
        )
        # Mock SHOW QUERIES on data-plane endpoint.
        respx.post(f"{KSQL_ENDPOINT}/ksql").mock(
            return_value=httpx.Response(200, json=queries_payload)
        )

        nodes, edges = await ksql_client.extract()

        # Nodes: 1 query + 2 source topics (orders, customers) + 1 sink topic (enriched_orders)
        query_nodes = [n for n in nodes if n.node_type == NodeType.KSQLDB_QUERY]
        topic_nodes = [n for n in nodes if n.node_type == NodeType.KAFKA_TOPIC]
        assert len(query_nodes) == 1
        assert len(topic_nodes) == 3

        qn = query_nodes[0]
        assert qn.qualified_name == "CSAS_ENRICHED_ORDERS_0"
        assert qn.system == SystemType.CONFLUENT
        assert qn.environment_id == ENV_ID
        assert qn.cluster_id == KAFKA_CLUSTER_ID
        assert qn.attributes["ksqldb_cluster_id"] == CLUSTER_ID
        assert qn.attributes["state"] == "RUNNING"

    @respx.mock
    async def test_consumes_edges(self, ksql_client, queries_payload):
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response())
        )
        respx.post(f"{KSQL_ENDPOINT}/ksql").mock(
            return_value=httpx.Response(200, json=queries_payload)
        )

        nodes, edges = await ksql_client.extract()

        consumes_edges = [e for e in edges if e.edge_type == EdgeType.CONSUMES]
        assert len(consumes_edges) == 2

        query_id = f"confluent:ksqldb_query:{ENV_ID}:CSAS_ENRICHED_ORDERS_0"
        for edge in consumes_edges:
            assert edge.dst_id == query_id
            assert edge.confidence == 0.8

        source_topics = sorted(e.src_id for e in consumes_edges)
        assert f"confluent:kafka_topic:{ENV_ID}:customers" in source_topics
        assert f"confluent:kafka_topic:{ENV_ID}:orders" in source_topics

    @respx.mock
    async def test_produces_edges(self, ksql_client, queries_payload):
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response())
        )
        respx.post(f"{KSQL_ENDPOINT}/ksql").mock(
            return_value=httpx.Response(200, json=queries_payload)
        )

        nodes, edges = await ksql_client.extract()

        produces_edges = [e for e in edges if e.edge_type == EdgeType.PRODUCES]
        assert len(produces_edges) == 1

        pe = produces_edges[0]
        assert pe.src_id == f"confluent:ksqldb_query:{ENV_ID}:CSAS_ENRICHED_ORDERS_0"
        assert pe.dst_id == f"confluent:kafka_topic:{ENV_ID}:enriched_orders"

    @respx.mock
    async def test_empty_clusters(self, ksql_client):
        """When no ksqlDB clusters exist, extract returns empty lists."""
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response(clusters=[]))
        )

        nodes, edges = await ksql_client.extract()

        assert nodes == []
        assert edges == []

    @respx.mock
    async def test_cluster_failure_handling(self, ksql_client, no_sleep):
        """When a cluster's data-plane request fails, it is skipped gracefully."""
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response())
        )
        # Data-plane returns 500 (triggers retries then exception).
        respx.post(f"{KSQL_ENDPOINT}/ksql").mock(
            return_value=httpx.Response(500, json={"error_code": 50000})
        )

        nodes, edges = await ksql_client.extract()

        assert nodes == []
        assert edges == []

    @respx.mock
    async def test_cluster_without_endpoint(self, ksql_client):
        """Clusters missing an HTTP endpoint are skipped."""
        cluster_no_endpoint = {
            "id": "lksqlc-broken",
            "spec": {"kafka_cluster": {"id": "lkc-xyz"}},
            "status": {},
        }
        respx.get(f"{CLOUD_URL}/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json=_clusters_response(clusters=[cluster_no_endpoint]))
        )

        nodes, edges = await ksql_client.extract()

        assert nodes == []
        assert edges == []
