# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for LineageGraph.validate()."""

from __future__ import annotations

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _node(name: str, node_type: NodeType = NodeType.KAFKA_TOPIC) -> LineageNode:
    return LineageNode(
        node_id=f"confluent:{node_type.value}:env-1:{name}",
        system=SystemType.CONFLUENT,
        node_type=node_type,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
    )


def test_validate_clean_graph():
    """A graph where all nodes have edges returns no warnings."""
    graph = LineageGraph()
    n1 = _node("topic-a")
    n2 = _node("topic-b")
    graph.add_node(n1)
    graph.add_node(n2)
    graph.add_edge(LineageEdge(src_id=n1.node_id, dst_id=n2.node_id, edge_type=EdgeType.PRODUCES))

    warnings = graph.validate()
    assert warnings == []


def test_validate_orphan_node():
    """A node with no edges is flagged as orphan."""
    graph = LineageGraph()
    graph.add_node(_node("lonely-topic"))

    warnings = graph.validate()
    assert len(warnings) == 1
    assert "Orphan node" in warnings[0]
    assert "lonely-topic" in warnings[0]


def test_validate_schema_not_orphan():
    """SCHEMA nodes are excluded from orphan detection (they are leaf nodes)."""
    graph = LineageGraph()
    graph.add_node(_node("orders-value", NodeType.SCHEMA))

    warnings = graph.validate()
    assert warnings == []


def test_validate_empty_graph():
    """An empty graph returns no warnings."""
    graph = LineageGraph()
    assert graph.validate() == []


def test_validate_mixed_orphans_and_connected():
    """Only orphan non-schema nodes are flagged."""
    graph = LineageGraph()
    n1 = _node("connected-a")
    n2 = _node("connected-b")
    orphan = _node("orphan-topic")
    schema = _node("my-schema", NodeType.SCHEMA)

    graph.add_node(n1)
    graph.add_node(n2)
    graph.add_node(orphan)
    graph.add_node(schema)
    graph.add_edge(LineageEdge(src_id=n1.node_id, dst_id=n2.node_id, edge_type=EdgeType.PRODUCES))

    warnings = graph.validate()
    assert len(warnings) == 1
    assert "orphan-topic" in warnings[0]
