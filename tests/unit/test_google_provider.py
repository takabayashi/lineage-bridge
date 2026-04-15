# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Google Data Lineage catalog provider."""

from __future__ import annotations

import pytest

from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


@pytest.fixture()
def provider():
    return GoogleLineageProvider(project_id="my-project", location="us")


class TestBuildNode:
    def test_creates_google_table_node_and_edge(self, provider):
        ci_config = {
            "google_bigquery": {
                "project_id": "my-project",
                "dataset_id": "my_dataset",
            }
        }
        node, edge = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="confluent:tableflow_table:env:lkc.orders",
            topic_name="orders",
            cluster_id="lkc-123",
            environment_id="env-abc",
        )

        assert node.system == SystemType.GOOGLE
        assert node.node_type == NodeType.GOOGLE_TABLE
        assert node.qualified_name == "my-project.my_dataset.orders"
        assert node.attributes["project_id"] == "my-project"
        assert node.attributes["dataset_id"] == "my_dataset"
        assert node.attributes["table_name"] == "orders"
        assert node.environment_id == "env-abc"

        assert edge.edge_type == EdgeType.MATERIALIZES
        assert edge.src_id == "confluent:tableflow_table:env:lkc.orders"
        assert edge.dst_id == node.node_id

    def test_replaces_dots_and_dashes_in_table_name(self, provider):
        ci_config = {"google_bigquery": {"project_id": "p", "dataset_id": "d"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="my.topic-name",
            cluster_id="lkc",
            environment_id="env",
        )
        assert node.attributes["table_name"] == "my_topic_name"
        assert "my_topic_name" in node.qualified_name

    def test_defaults_project_from_provider(self, provider):
        ci_config = {"google_bigquery": {"dataset_id": "ds"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="t",
            cluster_id="lkc",
            environment_id="env",
        )
        assert node.attributes["project_id"] == "my-project"

    def test_defaults_dataset_from_cluster_id(self, provider):
        ci_config = {"google_bigquery": {"project_id": "p"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="t",
            cluster_id="lkc-99",
            environment_id="env",
        )
        assert node.attributes["dataset_id"] == "lkc-99"


class TestBuildUrl:
    def test_builds_bigquery_console_url(self, provider):
        node = LineageNode(
            node_id="google:google_table:env:p.d.t",
            system=SystemType.GOOGLE,
            node_type=NodeType.GOOGLE_TABLE,
            qualified_name="p.d.t",
            display_name="p.d.t",
            attributes={
                "project_id": "my-project",
                "dataset_id": "my_dataset",
                "table_name": "orders",
            },
        )
        url = provider.build_url(node)
        assert url is not None
        assert "console.cloud.google.com/bigquery" in url
        assert "project=my-project" in url
        assert "d=my_dataset" in url
        assert "t=orders" in url

    def test_returns_none_when_missing_attrs(self, provider):
        node = LineageNode(
            node_id="google:google_table:env:x",
            system=SystemType.GOOGLE,
            node_type=NodeType.GOOGLE_TABLE,
            qualified_name="x",
            display_name="x",
            attributes={},
        )
        assert provider.build_url(node) is None


class TestProviderMetadata:
    def test_catalog_type(self, provider):
        assert provider.catalog_type == "GOOGLE_DATA_LINEAGE"

    def test_node_type(self, provider):
        assert provider.node_type == NodeType.GOOGLE_TABLE

    def test_system_type(self, provider):
        assert provider.system_type == SystemType.GOOGLE


class TestEnrich:
    async def test_skips_without_project_id(self):
        provider = GoogleLineageProvider()
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="google:google_table:env:p.d.t",
                system=SystemType.GOOGLE,
                node_type=NodeType.GOOGLE_TABLE,
                qualified_name="p.d.t",
                display_name="p.d.t",
            )
        )
        # Should not raise
        await provider.enrich(graph)

    async def test_skips_empty_graph(self, provider):
        graph = LineageGraph()
        await provider.enrich(graph)


class TestPushLineage:
    async def test_skips_without_project_id(self):
        provider = GoogleLineageProvider()
        graph = LineageGraph()
        result = await provider.push_lineage(graph)
        assert len(result.errors) == 1
        assert "project_id" in result.errors[0]

    async def test_skips_empty_google_nodes(self, provider):
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="confluent:kafka_topic:env:t",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name="t",
                display_name="t",
            )
        )
        # Will fail on auth, but that's expected since no credentials
        result = await provider.push_lineage(graph)
        # No Google nodes → no errors about tables, but might have auth error
        assert result.tables_updated == 0
