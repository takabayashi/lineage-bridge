# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.models.graph."""

from __future__ import annotations

import pytest

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    NodeType,
)

# ── test_add_node ──────────────────────────────────────────────────────


def test_add_node(sample_node):
    """Adding a node makes it retrievable via get_node."""
    graph = LineageGraph()
    node = sample_node("orders", NodeType.KAFKA_TOPIC)
    graph.add_node(node)

    retrieved = graph.get_node(node.node_id)
    assert retrieved is not None
    assert retrieved.node_id == node.node_id
    assert retrieved.display_name == "orders"
    assert graph.node_count == 1


# ── test_add_node_merge ────────────────────────────────────────────────


def test_add_node_merge(sample_node):
    """Adding the same node twice merges attributes and preserves first_seen."""
    graph = LineageGraph()

    first = sample_node("orders", NodeType.KAFKA_TOPIC, attributes={"partitions": 6})
    graph.add_node(first)
    original_first_seen = graph.get_node(first.node_id).first_seen

    second = sample_node(
        "orders",
        NodeType.KAFKA_TOPIC,
        attributes={"replication_factor": 3},
        tags=["important"],
    )
    graph.add_node(second)

    merged = graph.get_node(first.node_id)
    assert merged is not None
    assert merged.attributes["partitions"] == 6
    assert merged.attributes["replication_factor"] == 3
    assert "important" in merged.tags
    assert merged.first_seen == original_first_seen
    assert graph.node_count == 1


# ── test_add_edge ──────────────────────────────────────────────────────


def test_add_edge(sample_node, sample_edge):
    """Adding an edge between existing nodes succeeds."""
    graph = LineageGraph()
    src = sample_node("orders", NodeType.KAFKA_TOPIC)
    dst = sample_node("orders-value", NodeType.SCHEMA)
    graph.add_node(src)
    graph.add_node(dst)

    edge = sample_edge(src.node_id, dst.node_id, EdgeType.HAS_SCHEMA)
    graph.add_edge(edge)

    retrieved = graph.get_edge(src.node_id, dst.node_id, EdgeType.HAS_SCHEMA)
    assert retrieved is not None
    assert retrieved.edge_type == EdgeType.HAS_SCHEMA
    assert graph.edge_count == 1


# ── test_add_edge_missing_node ─────────────────────────────────────────


def test_add_edge_missing_source(sample_node, sample_edge):
    """Adding an edge with a missing source node raises ValueError."""
    graph = LineageGraph()
    dst = sample_node("orders-value", NodeType.SCHEMA)
    graph.add_node(dst)

    edge = sample_edge("nonexistent-src", dst.node_id, EdgeType.HAS_SCHEMA)
    with pytest.raises(ValueError, match="Source node"):
        graph.add_edge(edge)


def test_add_edge_missing_destination(sample_node, sample_edge):
    """Adding an edge with a missing destination node raises ValueError."""
    graph = LineageGraph()
    src = sample_node("orders", NodeType.KAFKA_TOPIC)
    graph.add_node(src)

    edge = sample_edge(src.node_id, "nonexistent-dst", EdgeType.HAS_SCHEMA)
    with pytest.raises(ValueError, match="Destination node"):
        graph.add_edge(edge)


# ── test_get_neighbors_upstream ────────────────────────────────────────


def test_get_neighbors_upstream(sample_node, sample_edge):
    """get_neighbors with direction='upstream' returns predecessors."""
    graph = LineageGraph()
    a = sample_node("source-db", NodeType.EXTERNAL_DATASET)
    b = sample_node("orders", NodeType.KAFKA_TOPIC)
    graph.add_node(a)
    graph.add_node(b)
    graph.add_edge(sample_edge(a.node_id, b.node_id, EdgeType.PRODUCES))

    upstream = graph.get_neighbors(b.node_id, direction="upstream")
    assert len(upstream) == 1
    assert upstream[0].node_id == a.node_id


# ── test_get_neighbors_downstream ──────────────────────────────────────


def test_get_neighbors_downstream(sample_node, sample_edge):
    """get_neighbors with direction='downstream' returns successors."""
    graph = LineageGraph()
    a = sample_node("orders", NodeType.KAFKA_TOPIC)
    b = sample_node("orders-value", NodeType.SCHEMA)
    graph.add_node(a)
    graph.add_node(b)
    graph.add_edge(sample_edge(a.node_id, b.node_id, EdgeType.HAS_SCHEMA))

    downstream = graph.get_neighbors(a.node_id, direction="downstream")
    assert len(downstream) == 1
    assert downstream[0].node_id == b.node_id


# ── test_upstream_multi_hop ────────────────────────────────────────────


def test_upstream_multi_hop(sample_node, sample_edge):
    """upstream(D, 2) on chain A->B->C->D returns B and C."""
    graph = LineageGraph()
    nodes = [sample_node(name, NodeType.KAFKA_TOPIC) for name in ["A", "B", "C", "D"]]
    for n in nodes:
        graph.add_node(n)

    for i in range(3):
        graph.add_edge(sample_edge(nodes[i].node_id, nodes[i + 1].node_id, EdgeType.PRODUCES))

    result = graph.upstream(nodes[3].node_id, hops=2)
    result_ids = {n.node_id for n in result}
    assert nodes[1].node_id in result_ids  # B
    assert nodes[2].node_id in result_ids  # C
    assert nodes[0].node_id not in result_ids  # A is 3 hops away
    assert nodes[3].node_id not in result_ids  # D itself


# ── test_downstream_multi_hop ──────────────────────────────────────────


def test_downstream_multi_hop(sample_node, sample_edge):
    """downstream(A, 2) on chain A->B->C->D returns B and C."""
    graph = LineageGraph()
    nodes = [sample_node(name, NodeType.KAFKA_TOPIC) for name in ["A", "B", "C", "D"]]
    for n in nodes:
        graph.add_node(n)

    for i in range(3):
        graph.add_edge(sample_edge(nodes[i].node_id, nodes[i + 1].node_id, EdgeType.PRODUCES))

    result = graph.downstream(nodes[0].node_id, hops=2)
    result_ids = {n.node_id for n in result}
    assert nodes[1].node_id in result_ids  # B
    assert nodes[2].node_id in result_ids  # C
    assert nodes[3].node_id not in result_ids  # D is 3 hops away
    assert nodes[0].node_id not in result_ids  # A itself


# ── test_filter_by_type ────────────────────────────────────────────────


def test_filter_by_type(sample_graph):
    """filter_by_type returns only nodes of the requested type."""
    topics = sample_graph.filter_by_type(NodeType.KAFKA_TOPIC)
    assert len(topics) == 3
    assert all(n.node_type == NodeType.KAFKA_TOPIC for n in topics)

    schemas = sample_graph.filter_by_type(NodeType.SCHEMA)
    assert len(schemas) == 2

    connectors = sample_graph.filter_by_type(NodeType.CONNECTOR)
    assert len(connectors) == 1


# ── test_filter_by_env ─────────────────────────────────────────────────


def test_filter_by_env(sample_graph):
    """filter_by_env returns only nodes in the given environment."""
    env_abc = sample_graph.filter_by_env("env-abc123")
    env_xyz = sample_graph.filter_by_env("env-xyz789")

    # 9 nodes are in env-abc123, 1 in env-xyz789
    assert len(env_abc) == 9
    assert len(env_xyz) == 1
    assert env_xyz[0].qualified_name == "s3://acme-data-lake/orders"


# ── test_search_nodes ──────────────────────────────────────────────────


def test_search_nodes(sample_graph):
    """search_nodes finds nodes by case-insensitive substring match."""
    results = sample_graph.search_nodes("order")
    names = {n.qualified_name for n in results}
    assert "orders" in names
    assert "enriched_orders" in names
    assert "order-processing-group" in names
    assert "pg-prod.ecommerce.orders" in names

    # Case-insensitive
    results_upper = sample_graph.search_nodes("ORDER")
    assert len(results_upper) == len(results)


# ── test_serialization_roundtrip ───────────────────────────────────────


def test_serialization_roundtrip(sample_graph):
    """to_dict -> from_dict preserves all nodes and edges."""
    data = sample_graph.to_dict()
    restored = LineageGraph.from_dict(data)

    assert restored.node_count == sample_graph.node_count
    assert restored.edge_count == sample_graph.edge_count

    for original in sample_graph.nodes:
        restored_node = restored.get_node(original.node_id)
        assert restored_node is not None
        assert restored_node.display_name == original.display_name
        assert restored_node.node_type == original.node_type
        assert restored_node.attributes == original.attributes


# ── test_json_file_roundtrip ───────────────────────────────────────────


def test_json_file_roundtrip(sample_graph, tmp_path):
    """to_json_file -> from_json_file preserves all data."""
    filepath = tmp_path / "test_graph.json"
    sample_graph.to_json_file(filepath)

    assert filepath.exists()

    restored = LineageGraph.from_json_file(filepath)
    assert restored.node_count == sample_graph.node_count
    assert restored.edge_count == sample_graph.edge_count

    for original in sample_graph.nodes:
        restored_node = restored.get_node(original.node_id)
        assert restored_node is not None
        assert restored_node.qualified_name == original.qualified_name


# ── test_empty_graph ───────────────────────────────────────────────────


def test_empty_graph():
    """Operations on an empty graph return empty results without errors."""
    graph = LineageGraph()

    assert graph.node_count == 0
    assert graph.edge_count == 0
    assert graph.nodes == []
    assert graph.edges == []
    assert graph.get_node("nonexistent") is None
    assert graph.get_edge("a", "b", EdgeType.PRODUCES) is None
    assert graph.get_neighbors("nonexistent") == []
    assert graph.upstream("nonexistent") == []
    assert graph.downstream("nonexistent") == []
    assert graph.filter_by_type(NodeType.KAFKA_TOPIC) == []
    assert graph.filter_by_env("env-abc123") == []
    assert graph.search_nodes("anything") == []

    data = graph.to_dict()
    assert data == {"nodes": [], "edges": []}
    restored = LineageGraph.from_dict(data)
    assert restored.node_count == 0


# ── test_edge_dedup ────────────────────────────────────────────────────


def test_edge_dedup(sample_node, sample_edge):
    """Adding the same edge twice updates last_seen but preserves first_seen."""
    graph = LineageGraph()
    src = sample_node("orders", NodeType.KAFKA_TOPIC)
    dst = sample_node("orders-value", NodeType.SCHEMA)
    graph.add_node(src)
    graph.add_node(dst)

    edge1 = sample_edge(src.node_id, dst.node_id, EdgeType.HAS_SCHEMA)
    graph.add_edge(edge1)
    original_first_seen = graph.get_edge(src.node_id, dst.node_id, EdgeType.HAS_SCHEMA).first_seen

    # Add the same edge again
    edge2 = LineageEdge(
        src_id=src.node_id,
        dst_id=dst.node_id,
        edge_type=EdgeType.HAS_SCHEMA,
    )
    graph.add_edge(edge2)

    result = graph.get_edge(src.node_id, dst.node_id, EdgeType.HAS_SCHEMA)
    assert result is not None
    assert result.first_seen == original_first_seen
    assert graph.edge_count == 1
