# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.ui.sample_data."""

from __future__ import annotations

from lineage_bridge.models.graph import EdgeType, LineageGraph, NodeType
from lineage_bridge.ui.sample_data import generate_sample_graph


class TestGenerateSampleGraphNotEmpty:
    """The generated graph should contain nodes and edges."""

    def test_has_nodes(self):
        graph = generate_sample_graph()
        assert graph.node_count > 0

    def test_has_edges(self):
        graph = generate_sample_graph()
        assert graph.edge_count > 0


class TestGenerateSampleGraphNodeTypes:
    """The graph should include all expected node types."""

    def test_has_all_node_types(self):
        graph = generate_sample_graph()
        present_types = {n.node_type for n in graph.nodes}
        for ntype in NodeType:
            assert ntype in present_types, (
                f"Missing node type {ntype.name} in sample graph"
            )


class TestGenerateSampleGraphEdgeTypes:
    """The graph should include key edge types."""

    def test_has_produces_edges(self):
        graph = generate_sample_graph()
        edge_types = {e.edge_type for e in graph.edges}
        assert EdgeType.PRODUCES in edge_types

    def test_has_consumes_edges(self):
        graph = generate_sample_graph()
        edge_types = {e.edge_type for e in graph.edges}
        assert EdgeType.CONSUMES in edge_types

    def test_has_has_schema_edges(self):
        graph = generate_sample_graph()
        edge_types = {e.edge_type for e in graph.edges}
        assert EdgeType.HAS_SCHEMA in edge_types

    def test_has_materializes_edges(self):
        graph = generate_sample_graph()
        edge_types = {e.edge_type for e in graph.edges}
        assert EdgeType.MATERIALIZES in edge_types

    def test_has_key_edge_types(self):
        graph = generate_sample_graph()
        edge_types = {e.edge_type for e in graph.edges}
        expected = {
            EdgeType.PRODUCES,
            EdgeType.CONSUMES,
            EdgeType.HAS_SCHEMA,
            EdgeType.MATERIALIZES,
        }
        assert expected.issubset(edge_types)


class TestGenerateSampleGraphSerializationRoundtrip:
    """The graph should survive serialization and deserialization."""

    def test_roundtrip_preserves_node_count(self):
        original = generate_sample_graph()
        data = original.to_dict()
        restored = LineageGraph.from_dict(data)
        assert restored.node_count == original.node_count

    def test_roundtrip_preserves_edge_count(self):
        original = generate_sample_graph()
        data = original.to_dict()
        restored = LineageGraph.from_dict(data)
        assert restored.edge_count == original.edge_count

    def test_roundtrip_preserves_node_ids(self):
        original = generate_sample_graph()
        data = original.to_dict()
        restored = LineageGraph.from_dict(data)
        orig_ids = {n.node_id for n in original.nodes}
        rest_ids = {n.node_id for n in restored.nodes}
        assert orig_ids == rest_ids

    def test_roundtrip_preserves_edge_keys(self):
        original = generate_sample_graph()
        data = original.to_dict()
        restored = LineageGraph.from_dict(data)
        orig_keys = {e.edge_key for e in original.edges}
        rest_keys = {e.edge_key for e in restored.edges}
        assert orig_keys == rest_keys

    def test_roundtrip_preserves_attributes(self):
        original = generate_sample_graph()
        data = original.to_dict()
        restored = LineageGraph.from_dict(data)
        for orig_node in original.nodes:
            rest_node = restored.get_node(orig_node.node_id)
            assert rest_node is not None
            assert rest_node.attributes == orig_node.attributes
