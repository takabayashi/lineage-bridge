# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Shared test fixtures for LineageBridge tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> Any:
    """Load and parse a JSON fixture file by name."""
    path = FIXTURES_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture()
def fixtures_dir() -> Path:
    """Return the path to the tests/fixtures/ directory."""
    return FIXTURES_DIR


@pytest.fixture()
def no_sleep(monkeypatch):
    """Patch asyncio.sleep to return immediately."""
    monkeypatch.setattr("asyncio.sleep", AsyncMock())


@pytest.fixture()
def sample_node():
    """Factory fixture that creates a LineageNode with sensible defaults.

    Usage::

        node = sample_node("orders", NodeType.KAFKA_TOPIC)
        node = sample_node("my-conn", NodeType.CONNECTOR, environment_id="env-xyz")
    """

    def _make(
        name: str,
        node_type: NodeType = NodeType.KAFKA_TOPIC,
        *,
        system: SystemType = SystemType.CONFLUENT,
        environment_id: str = "env-abc123",
        cluster_id: str = "lkc-abc123",
        attributes: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> LineageNode:
        node_id = f"{system.value}:{node_type.value}:{environment_id}:{name}"
        return LineageNode(
            node_id=node_id,
            system=system,
            node_type=node_type,
            qualified_name=name,
            display_name=name,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes=attributes or {},
            tags=tags or [],
        )

    return _make


@pytest.fixture()
def sample_edge():
    """Factory fixture that creates a LineageEdge with sensible defaults.

    Usage::

        edge = sample_edge("src-id", "dst-id", EdgeType.PRODUCES)
    """

    def _make(
        src_id: str,
        dst_id: str,
        edge_type: EdgeType = EdgeType.PRODUCES,
        *,
        confidence: float = 1.0,
        attributes: dict[str, Any] | None = None,
    ) -> LineageEdge:
        return LineageEdge(
            src_id=src_id,
            dst_id=dst_id,
            edge_type=edge_type,
            confidence=confidence,
            attributes=attributes or {},
        )

    return _make


@pytest.fixture()
def sample_graph(sample_node, sample_edge) -> LineageGraph:
    """Return a pre-built LineageGraph representing a realistic topology.

    Topology::

        [pg-prod (ext)] --produces--> [orders (topic)] --has_schema--> [orders-value (schema)]
                                            |
                                            +--consumes--> [enrichment-svc (consumer_group)]
                                            |
        [customers (topic)] --+             |
              |                \\            v
              +--has_schema-->  [ksql-enrich (ksqldb_query)] --produces--> [enriched_orders (topic)]
              |                                                                 |
              +--has_schema--> [customers-value (schema)]                       |
                                                                                v
                                          [s3-sink (connector)] --produces--> [s3://lake (ext)]
    """
    graph = LineageGraph()

    # -- Nodes --
    nodes = [
        sample_node("pg-prod.ecommerce.orders", NodeType.EXTERNAL_DATASET),
        sample_node(
            "orders",
            NodeType.KAFKA_TOPIC,
            attributes={"partitions": 6, "replication_factor": 3},
        ),
        sample_node(
            "customers",
            NodeType.KAFKA_TOPIC,
            attributes={"partitions": 3, "replication_factor": 3},
        ),
        sample_node("enriched_orders", NodeType.KAFKA_TOPIC, attributes={"partitions": 6}),
        sample_node("orders-value", NodeType.SCHEMA),
        sample_node("customers-value", NodeType.SCHEMA),
        sample_node(
            "postgres-source-orders",
            NodeType.CONNECTOR,
            attributes={"connector.class": "PostgresSource"},
        ),
        sample_node("CSAS_ENRICHED_ORDERS_0", NodeType.KSQLDB_QUERY),
        sample_node("order-processing-group", NodeType.CONSUMER_GROUP),
        sample_node(
            "s3://acme-data-lake/orders",
            NodeType.EXTERNAL_DATASET,
            environment_id="env-xyz789",
        ),
    ]
    for n in nodes:
        graph.add_node(n)

    # -- Edges --
    def nid(name, ntype):
        return f"confluent:{ntype.value}:env-abc123:{name}"

    def ext_nid(name):
        return nid(name, NodeType.EXTERNAL_DATASET)

    def topic_nid(name):
        return nid(name, NodeType.KAFKA_TOPIC)

    def schema_nid(name):
        return nid(name, NodeType.SCHEMA)

    def ksql_nid(name):
        return nid(name, NodeType.KSQLDB_QUERY)

    def cg_nid(name):
        return nid(name, NodeType.CONSUMER_GROUP)

    edges = [
        sample_edge(ext_nid("pg-prod.ecommerce.orders"), topic_nid("orders"), EdgeType.PRODUCES),
        sample_edge(topic_nid("orders"), schema_nid("orders-value"), EdgeType.HAS_SCHEMA),
        sample_edge(topic_nid("customers"), schema_nid("customers-value"), EdgeType.HAS_SCHEMA),
        sample_edge(topic_nid("orders"), ksql_nid("CSAS_ENRICHED_ORDERS_0"), EdgeType.CONSUMES),
        sample_edge(topic_nid("customers"), ksql_nid("CSAS_ENRICHED_ORDERS_0"), EdgeType.CONSUMES),
        sample_edge(
            ksql_nid("CSAS_ENRICHED_ORDERS_0"),
            topic_nid("enriched_orders"),
            EdgeType.PRODUCES,
        ),
        sample_edge(topic_nid("orders"), cg_nid("order-processing-group"), EdgeType.CONSUMES),
        # s3 sink node has different env, build its id manually
        sample_edge(
            topic_nid("enriched_orders"),
            "confluent:external_dataset:env-xyz789:s3://acme-data-lake/orders",
            EdgeType.PRODUCES,
        ),
    ]
    for e in edges:
        graph.add_edge(e)

    return graph
