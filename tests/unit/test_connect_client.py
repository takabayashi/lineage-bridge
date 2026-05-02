# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.connect.ConnectClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.connect import (
    ConnectClient,
    _classify_connector,
    _extract_topics,
    _infer_external_dataset,
)
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType
from tests.conftest import load_fixture

# ── Constants shared across tests ──────────────────────────────────────────
BASE_URL = "https://api.confluent.cloud"
ENV_ID = "env-abc123"
CLUSTER_ID = "lkc-abc123"
API_KEY = "test-key"
API_SECRET = "test-secret"

CONNECTORS_PATH = f"/connect/v1/environments/{ENV_ID}/clusters/{CLUSTER_ID}/connectors"


def _connector_path(name: str) -> str:
    return f"{CONNECTORS_PATH}/{name}"


@pytest.fixture()
def connectors_fixture():
    return load_fixture("connectors.json")


@pytest.fixture()
def source_config_fixture():
    return load_fixture("connector_config_source.json")


@pytest.fixture()
def sink_config_fixture():
    return load_fixture("connector_config_sink.json")


@pytest.fixture()
def bigquery_sink_config_fixture():
    return load_fixture("connector_config_bigquery_sink.json")


def _make_client() -> ConnectClient:
    return ConnectClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        environment_id=ENV_ID,
        kafka_cluster_id=CLUSTER_ID,
        base_url=BASE_URL,
    )


# ── _classify_connector ──────────────────────────────────────────────────


class TestClassifyConnector:
    def test_source_by_debezium_class(self):
        assert _classify_connector("io.debezium.connector.postgresql.PostgresConnector") == "source"

    def test_source_by_suffix(self):
        assert _classify_connector("com.example.MyCustomSource") == "source"

    def test_source_by_dot_source_dot(self):
        assert _classify_connector("io.confluent.connect.source.MyConnector") == "source"

    def test_sink_by_s3sink(self):
        assert _classify_connector("io.confluent.connect.s3.S3SinkConnector") == "sink"

    def test_sink_by_suffix(self):
        assert _classify_connector("com.example.MyCustomSink") == "sink"

    def test_sink_by_dot_sink_dot(self):
        assert _classify_connector("io.confluent.connect.sink.MyConnector") == "sink"

    def test_sink_by_jdbc_sink(self):
        assert _classify_connector("io.confluent.connect.jdbc.JdbcSinkConnector") == "sink"

    def test_sink_by_bigquery_storage_sink(self):
        assert _classify_connector("BigQueryStorageSink") == "sink"

    def test_unknown_when_no_match(self):
        assert _classify_connector("com.example.SomeTransformer") == "unknown"

    def test_explicit_type_overrides_class_pattern(self):
        # Even though the class looks like a source, explicit_type wins
        assert _classify_connector("io.debezium.PostgresConnector", "sink") == "sink"

    def test_explicit_source_type(self):
        assert _classify_connector("com.example.Unknown", "source") == "source"


# ── _extract_topics ──────────────────────────────────────────────────────


class TestExtractTopics:
    def test_topics_key_comma_separated(self):
        config = {"topics": "orders,enriched_orders"}
        assert _extract_topics(config) == ["orders", "enriched_orders"]

    def test_topics_key_with_whitespace(self):
        config = {"topics": " orders , enriched_orders "}
        assert _extract_topics(config) == ["orders", "enriched_orders"]

    def test_kafka_topic_key(self):
        config = {"kafka.topic": "my-single-topic"}
        assert _extract_topics(config) == ["my-single-topic"]

    def test_topic_key(self):
        config = {"topic": "fallback-topic"}
        assert _extract_topics(config) == ["fallback-topic"]

    def test_topics_takes_precedence_over_topic(self):
        config = {"topics": "primary", "topic": "fallback"}
        assert _extract_topics(config) == ["primary"]

    def test_empty_config_returns_empty_list(self):
        assert _extract_topics({}) == []

    def test_empty_string_value_returns_empty_list(self):
        config = {"topics": ""}
        assert _extract_topics(config) == []


# ── _infer_external_dataset ──────────────────────────────────────────────


class TestInferExternalDataset:
    def test_s3_bucket(self):
        config = {"s3.bucket.name": "acme-data-lake"}
        assert _infer_external_dataset(config, "S3SinkConnector") == "s3://acme-data-lake/"

    def test_gcs_bucket(self):
        config = {"gcs.bucket.name": "my-gcs-bucket"}
        assert _infer_external_dataset(config, "GcsSinkConnector") == "gs://my-gcs-bucket/"

    def test_jdbc_host_and_db(self):
        config = {"connection.host": "db.example.com", "db.name": "mydb"}
        assert _infer_external_dataset(config, "JdbcSinkConnector") == "db.example.com/mydb"

    def test_debezium_database(self):
        config = {"database.hostname": "pg-prod.example.com", "database.dbname": "ecommerce"}
        result = _infer_external_dataset(config, "PostgresConnector")
        assert result == "pg-prod.example.com/ecommerce"

    def test_bigquery_project_and_dataset(self):
        config = {"project": "my-gcp-project", "defaultDataset": "analytics"}
        assert _infer_external_dataset(config, "BigQuerySink") == "my-gcp-project.analytics"

    def test_bigquery_v2_storage_sink(self):
        """BigQuery Storage Sink v2 uses 'datasets' (plural) config key."""
        config = {"project": "my-gcp-project", "datasets": "lineage_bridge"}
        assert (
            _infer_external_dataset(config, "BigQueryStorageSink")
            == "my-gcp-project.lineage_bridge"
        )

    def test_snowflake_db_and_schema(self):
        config = {"snowflake.database.name": "MYDB", "snowflake.schema.name": "PUBLIC"}
        assert _infer_external_dataset(config, "SnowflakeSink") == "snowflake://MYDB/PUBLIC"

    def test_snowflake_db_only(self):
        config = {"snowflake.database.name": "MYDB"}
        assert _infer_external_dataset(config, "SnowflakeSink") == "snowflake://MYDB"

    def test_elasticsearch_url(self):
        config = {"connection.url": "https://es.example.com:9200"}
        result = _infer_external_dataset(config, "ElasticsearchSink")
        assert result == "https://es.example.com:9200"

    def test_fallback_uses_connector_class(self):
        config = {}
        assert _infer_external_dataset(config, "com.example.Unknown") == "com.example.Unknown"


# ── extract() integration tests ──────────────────────────────────────────


@respx.mock
async def test_extract_source_connector(source_config_fixture, no_sleep):
    """Full extraction of a Debezium source connector produces correct nodes and edges."""
    # _list_connectors returns a list of names
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=["postgres-source-orders"]),
    )
    # _get_connector returns the detailed config
    respx.get(f"{BASE_URL}{_connector_path('postgres-source-orders')}").mock(
        return_value=httpx.Response(200, json=source_config_fixture),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    # Nodes: connector + external dataset (no topic nodes because source config
    # uses topic.prefix, not "topics" key — _extract_topics returns [])
    connector_nodes = [n for n in nodes if n.node_type == NodeType.CONNECTOR]
    assert len(connector_nodes) == 1
    conn = connector_nodes[0]
    assert conn.qualified_name == "postgres-source-orders"
    assert conn.system == SystemType.CONFLUENT
    assert conn.attributes["direction"] == "source"
    assert (
        conn.attributes["connector_class"] == "io.debezium.connector.postgresql.PostgresConnector"
    )
    assert conn.node_id == f"confluent:connector:{ENV_ID}:postgres-source-orders"

    ext_nodes = [n for n in nodes if n.node_type == NodeType.EXTERNAL_DATASET]
    assert len(ext_nodes) == 1
    ext = ext_nodes[0]
    assert ext.qualified_name == "pg-prod.example.com/ecommerce"
    assert ext.system == SystemType.EXTERNAL

    # Edge: external_dataset -> connector (PRODUCES)
    produces_edges = [e for e in edges if e.edge_type == EdgeType.PRODUCES]
    assert len(produces_edges) == 1
    assert produces_edges[0].src_id == ext.node_id
    assert produces_edges[0].dst_id == conn.node_id


@respx.mock
async def test_extract_sink_connector(sink_config_fixture, no_sleep):
    """Full extraction of an S3 sink connector produces correct nodes and edges."""
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=["s3-sink-orders"]),
    )
    respx.get(f"{BASE_URL}{_connector_path('s3-sink-orders')}").mock(
        return_value=httpx.Response(200, json=sink_config_fixture),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    # Nodes: connector + external dataset + 2 topic placeholders
    connector_nodes = [n for n in nodes if n.node_type == NodeType.CONNECTOR]
    assert len(connector_nodes) == 1
    conn = connector_nodes[0]
    assert conn.qualified_name == "s3-sink-orders"
    assert conn.attributes["direction"] == "sink"

    ext_nodes = [n for n in nodes if n.node_type == NodeType.EXTERNAL_DATASET]
    assert len(ext_nodes) == 1
    assert ext_nodes[0].qualified_name == "s3://acme-data-lake/"

    topic_nodes = [n for n in nodes if n.node_type == NodeType.KAFKA_TOPIC]
    assert len(topic_nodes) == 2
    topic_names = {n.qualified_name for n in topic_nodes}
    assert topic_names == {"orders", "enriched_orders"}

    # Edges: 2 x topic -> connector (CONSUMES) + connector -> ext (PRODUCES)
    consumes_edges = [e for e in edges if e.edge_type == EdgeType.CONSUMES]
    assert len(consumes_edges) == 2
    for ce in consumes_edges:
        assert ce.dst_id == conn.node_id

    produces_edges = [e for e in edges if e.edge_type == EdgeType.PRODUCES]
    assert len(produces_edges) == 1
    assert produces_edges[0].src_id == conn.node_id
    assert produces_edges[0].dst_id == ext_nodes[0].node_id


@respx.mock
async def test_extract_bigquery_sink_synthesizes_google_tables(
    bigquery_sink_config_fixture, no_sleep
):
    """A BigQuery sink emits one GOOGLE_TABLE per topic plus the existing EXTERNAL_DATASET."""
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=["bigquery_sink_orders"]),
    )
    respx.get(f"{BASE_URL}{_connector_path('bigquery_sink_orders')}").mock(
        return_value=httpx.Response(200, json=bigquery_sink_config_fixture),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    connector_nodes = [n for n in nodes if n.node_type == NodeType.CONNECTOR]
    assert len(connector_nodes) == 1
    conn = connector_nodes[0]

    # No EXTERNAL_DATASET node — per-topic GOOGLE_TABLE nodes carry the
    # project/dataset in their qualified names instead.
    ext_nodes = [n for n in nodes if n.node_type == NodeType.EXTERNAL_DATASET]
    assert ext_nodes == []

    # One GOOGLE_TABLE per topic
    google_nodes = [n for n in nodes if n.node_type == NodeType.CATALOG_TABLE]
    assert len(google_nodes) == 2
    qnames = {n.qualified_name for n in google_nodes}
    assert qnames == {
        "my-gcp-project.lineage_bridge.orders",
        "my-gcp-project.lineage_bridge.enriched_orders",
    }
    for g in google_nodes:
        assert g.system == SystemType.GOOGLE
        assert g.attributes["project_id"] == "my-gcp-project"
        assert g.attributes["dataset_id"] == "lineage_bridge"
        assert g.attributes["source_topic"] in ("orders", "enriched_orders")
        assert g.node_id.startswith("google:google_table:")

    # Edges: connector -> google_table (PRODUCES) for each topic, plus the
    # existing connector -> external_dataset PRODUCES edge.
    google_edges = [e for e in edges if e.dst_id in {g.node_id for g in google_nodes}]
    assert len(google_edges) == 2
    for ge in google_edges:
        assert ge.src_id == conn.node_id
        assert ge.edge_type == EdgeType.PRODUCES


@respx.mock
async def test_extract_handles_failed_connector_fetch(no_sleep):
    """If fetching a connector detail fails, extraction continues without it."""
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=["good-connector", "bad-connector"]),
    )
    # good-connector returns fine
    respx.get(f"{BASE_URL}{_connector_path('good-connector')}").mock(
        return_value=httpx.Response(
            200,
            json={
                "name": "good-connector",
                "config": {
                    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                    "topics": "orders",
                    "s3.bucket.name": "my-bucket",
                },
                "type": "sink",
            },
        ),
    )
    # bad-connector returns 500 (will fail after retries)
    respx.get(f"{BASE_URL}{_connector_path('bad-connector')}").mock(
        return_value=httpx.Response(500, json={"error": "internal"}),
    )

    async with _make_client() as client:
        nodes, _edges = await client.extract()

    # Only good-connector should produce nodes
    connector_nodes = [n for n in nodes if n.node_type == NodeType.CONNECTOR]
    assert len(connector_nodes) == 1
    assert connector_nodes[0].qualified_name == "good-connector"


@respx.mock
async def test_extract_expanded_response_format(no_sleep):
    """The expand=info,status format wraps each connector in {info, status}."""
    expanded = {
        "postgres-source": {
            "info": {
                "name": "postgres-source",
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "db.example.com",
                    "database.dbname": "mydb",
                    "topic.prefix": "cdc",
                },
                "type": "source",
            },
            "status": {
                "name": "postgres-source",
                "connector": {"state": "RUNNING", "worker_id": "w1"},
                "tasks": [],
            },
        }
    }
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=expanded),
    )

    async with _make_client() as client:
        nodes, _edges = await client.extract()

    connector_nodes = [n for n in nodes if n.node_type == NodeType.CONNECTOR]
    assert len(connector_nodes) == 1
    conn = connector_nodes[0]
    assert conn.qualified_name == "postgres-source"
    assert conn.attributes["direction"] == "source"
    assert conn.attributes["state"] == "RUNNING"


@respx.mock
async def test_extract_emits_dlq_topic_for_sink_with_id(no_sleep):
    """A sink connector with `confluent_id=lcc-XXX` emits a placeholder
    `dlq-lcc-XXX` topic + PRODUCES edge so DLQ topics aren't orphan nodes
    in the graph after kafka_admin lists them."""
    expanded = {
        "my-sink": {
            "info": {
                "name": "my-sink",
                "config": {
                    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                    "topics": "orders",
                    "s3.bucket.name": "my-bucket",
                },
                "type": "sink",
            },
            "status": {"name": "my-sink", "connector": {"state": "RUNNING"}, "tasks": []},
            "id": {"id": "lcc-7w7m2w", "id_type": "ID"},
        }
    }
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=expanded),
    )

    async with _make_client() as client:
        nodes, edges = await client.extract()

    conn = next(n for n in nodes if n.node_type == NodeType.CONNECTOR)
    assert conn.attributes["confluent_id"] == "lcc-7w7m2w"

    dlq = next(
        (
            n for n in nodes
            if n.node_type == NodeType.KAFKA_TOPIC and n.qualified_name.startswith("dlq-")
        ),
        None,
    )
    assert dlq is not None, "expected DLQ topic placeholder"
    assert dlq.qualified_name == "dlq-lcc-7w7m2w"
    assert dlq.attributes["role"] == "dlq"
    assert dlq.attributes["for_connector"] == "my-sink"

    dlq_edges = [e for e in edges if e.dst_id == dlq.node_id]
    assert len(dlq_edges) == 1
    assert dlq_edges[0].src_id == conn.node_id
    assert dlq_edges[0].edge_type == EdgeType.PRODUCES


@respx.mock
async def test_extract_does_not_emit_dlq_for_source_connectors(no_sleep):
    """Source connectors don't have auto-provisioned DLQ topics in Confluent
    Cloud, so we mustn't inject phantom `dlq-lcc-*` placeholder nodes for
    them — kafka_admin would never find a real topic to merge with."""
    expanded = {
        "datagen-orders": {
            "info": {
                "name": "datagen-orders",
                "config": {
                    "connector.class": "DatagenSource",
                    "kafka.topic": "orders",
                },
                "type": "source",
            },
            "status": {"name": "datagen-orders", "connector": {"state": "RUNNING"}, "tasks": []},
            "id": {"id": "lcc-yrxwvj", "id_type": "ID"},
        }
    }
    respx.get(f"{BASE_URL}{CONNECTORS_PATH}").mock(
        return_value=httpx.Response(200, json=expanded),
    )

    async with _make_client() as client:
        nodes, _edges = await client.extract()

    dlq_nodes = [
        n for n in nodes
        if n.node_type == NodeType.KAFKA_TOPIC and n.qualified_name.startswith("dlq-")
    ]
    assert dlq_nodes == [], "source connector must not emit a DLQ placeholder"
