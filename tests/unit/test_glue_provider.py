# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for GlueCatalogProvider."""

from __future__ import annotations

from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


class TestBuildNode:

    def test_creates_correct_node_id(self):
        provider = GlueCatalogProvider()
        node, edge = provider.build_node(
            {"aws_glue": {"database_name": "my_db"}},
            "confluent:tableflow_table:env-abc:lkc-abc123.orders",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert node.node_id == "aws:glue_table:env-abc:glue://my_db/orders"

    def test_node_system_and_type(self):
        provider = GlueCatalogProvider()
        node, _ = provider.build_node(
            {"aws_glue": {"database_name": "my_db"}},
            "tf-id",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert node.system == SystemType.AWS
        assert node.node_type == NodeType.GLUE_TABLE

    def test_node_attributes(self):
        provider = GlueCatalogProvider()
        node, _ = provider.build_node(
            {"aws_glue": {"database_name": "my_db"}},
            "tf-id",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert node.attributes["catalog_type"] == "AWS_GLUE"
        assert node.attributes["database"] == "my_db"
        assert node.attributes["table_name"] == "orders"
        assert node.display_name == "my_db.orders (glue)"

    def test_edge_type_materializes(self):
        provider = GlueCatalogProvider()
        _, edge = provider.build_node(
            {"aws_glue": {"database_name": "my_db"}},
            "tf-id",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert edge.edge_type == EdgeType.MATERIALIZES
        assert edge.src_id == "tf-id"

    def test_default_database_name(self):
        provider = GlueCatalogProvider()
        node, _ = provider.build_node(
            {"aws_glue": {}}, "tf-id", "orders", "lkc-abc123", "env-abc"
        )
        assert node.attributes["database"] == "lkc-abc123"


class TestBuildUrl:

    def test_returns_url_with_database_and_table(self):
        provider = GlueCatalogProvider()
        node = LineageNode(
            node_id="test",
            system=SystemType.AWS,
            node_type=NodeType.GLUE_TABLE,
            qualified_name="glue://my_db/orders",
            display_name="my_db.orders (glue)",
            attributes={
                "database": "my_db",
                "table_name": "orders",
            },
        )
        url = provider.build_url(node)
        assert url is not None
        assert "my_db" in url
        assert "orders" in url
        assert "console.aws.amazon.com/glue" in url

    def test_returns_none_without_database(self):
        provider = GlueCatalogProvider()
        node = LineageNode(
            node_id="test",
            system=SystemType.AWS,
            node_type=NodeType.GLUE_TABLE,
            qualified_name="glue://orders",
            display_name="orders (glue)",
            attributes={"table_name": "orders"},
        )
        assert provider.build_url(node) is None


class TestEnrich:

    async def test_enrich_is_noop(self):
        provider = GlueCatalogProvider()
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="aws:glue_table:env-1:glue://db/tbl",
                system=SystemType.AWS,
                node_type=NodeType.GLUE_TABLE,
                qualified_name="glue://db/tbl",
                display_name="db.tbl (glue)",
                attributes={"database": "db", "table_name": "tbl"},
            )
        )
        original_attrs = dict(graph.nodes[0].attributes)
        await provider.enrich(graph)
        assert graph.nodes[0].attributes == original_attrs
