# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the catalog provider registry."""

from __future__ import annotations

from lineage_bridge.catalogs import get_active_providers, get_provider
from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.models.graph import (
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


class TestGetProvider:
    def test_unity_catalog_returns_databricks_provider(self):
        provider = get_provider("UNITY_CATALOG")
        assert provider is not None
        assert isinstance(provider, DatabricksUCProvider)

    def test_aws_glue_returns_glue_provider(self):
        provider = get_provider("AWS_GLUE")
        assert provider is not None
        assert isinstance(provider, GlueCatalogProvider)

    def test_unknown_returns_none(self):
        assert get_provider("UNKNOWN") is None

    def test_empty_string_returns_none(self):
        assert get_provider("") is None


class TestGetActiveProviders:
    def test_mixed_uc_and_glue_nodes(self):
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="databricks:uc_table:env-1:cat.schema.tbl",
                system=SystemType.DATABRICKS,
                node_type=NodeType.UC_TABLE,
                qualified_name="cat.schema.tbl",
                display_name="cat.schema.tbl",
            )
        )
        graph.add_node(
            LineageNode(
                node_id="aws:glue_table:env-1:glue://db/tbl",
                system=SystemType.AWS,
                node_type=NodeType.GLUE_TABLE,
                qualified_name="glue://db/tbl",
                display_name="db.tbl (glue)",
            )
        )

        providers = get_active_providers(graph)
        types = {p.catalog_type for p in providers}
        assert types == {"UNITY_CATALOG", "AWS_GLUE"}

    def test_only_uc_nodes(self):
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="databricks:uc_table:env-1:cat.schema.tbl",
                system=SystemType.DATABRICKS,
                node_type=NodeType.UC_TABLE,
                qualified_name="cat.schema.tbl",
                display_name="cat.schema.tbl",
            )
        )

        providers = get_active_providers(graph)
        assert len(providers) == 1
        assert providers[0].catalog_type == "UNITY_CATALOG"

    def test_no_catalog_nodes(self):
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="confluent:kafka_topic:env-1:orders",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name="orders",
                display_name="orders",
            )
        )

        providers = get_active_providers(graph)
        assert providers == []

    def test_empty_graph(self):
        graph = LineageGraph()
        providers = get_active_providers(graph)
        assert providers == []
