# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for DatabricksUCProvider."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from tests.conftest import load_fixture

WORKSPACE_URL = "https://acme-prod.cloud.databricks.com"
TOKEN = "dapi-test-token-123"


@pytest.fixture()
def provider():
    return DatabricksUCProvider(workspace_url=WORKSPACE_URL, token=TOKEN)


@pytest.fixture()
def provider_no_creds():
    return DatabricksUCProvider()


@pytest.fixture()
def sample_ci_config():
    return {
        "unity_catalog": {
            "catalog_name": "confluent_tableflow",
            "workspace_url": WORKSPACE_URL,
        }
    }


@pytest.fixture()
def flat_ci_config():
    """Config as returned by the real Confluent Tableflow API."""
    return {
        "kind": "Unity",
        "workspace_endpoint": WORKSPACE_URL,
        "catalog_name": "confluent_tableflow",
        "client_id": "sp-abc123",
        "client_secret": "********",
    }


@pytest.fixture()
def uc_graph():
    """Graph with a single UC_TABLE node."""
    graph = LineageGraph()
    graph.add_node(
        LineageNode(
            node_id="databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders-tableflow",
            system=SystemType.DATABRICKS,
            node_type=NodeType.UC_TABLE,
            qualified_name="confluent_tableflow.lkc-abc123.orders-tableflow",
            display_name="confluent_tableflow.lkc-abc123.orders-tableflow",
            environment_id="env-abc",
            cluster_id="lkc-abc123",
            attributes={
                "catalog_name": "confluent_tableflow",
                "schema_name": "lkc-abc123",
                "table_name": "orders-tableflow",
                "workspace_url": WORKSPACE_URL,
            },
        )
    )
    return graph


class TestBuildNode:
    def test_creates_correct_node_id(self, provider, sample_ci_config):
        node, _edge = provider.build_node(
            sample_ci_config,
            "confluent:tableflow_table:env-abc:lkc-abc123.orders",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert node.node_id == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders"

    def test_node_attributes(self, provider, sample_ci_config):
        node, _ = provider.build_node(sample_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert node.system == SystemType.DATABRICKS
        assert node.node_type == NodeType.UC_TABLE
        assert node.attributes["catalog_name"] == "confluent_tableflow"
        assert node.attributes["schema_name"] == "lkc-abc123"
        assert node.attributes["table_name"] == "orders"
        assert node.attributes["workspace_url"] == WORKSPACE_URL

    def test_edge_type_materializes(self, provider, sample_ci_config):
        _, edge = provider.build_node(sample_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert edge.edge_type == EdgeType.MATERIALIZES
        assert edge.src_id == "tf-id"
        assert "uc_table" in edge.dst_id

    def test_default_catalog_name(self, provider):
        """When catalog_name is missing, defaults to confluent_tableflow."""
        node, _ = provider.build_node(
            {"unity_catalog": {}}, "tf-id", "orders", "lkc-abc123", "env-abc"
        )
        assert node.attributes["catalog_name"] == "confluent_tableflow"

    def test_flat_api_config_format(self, provider, flat_ci_config):
        """Flat config from real Confluent API (no 'unity_catalog' nesting)."""
        node, edge = provider.build_node(
            flat_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc"
        )
        assert node.node_id == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders"
        assert node.attributes["catalog_name"] == "confluent_tableflow"
        assert node.attributes["workspace_url"] == WORKSPACE_URL
        assert edge.edge_type == EdgeType.MATERIALIZES

    def test_dot_to_underscore_in_topic_name(self, provider, flat_ci_config):
        """Confluent replaces dots with underscores in UC table names."""
        node, _ = provider.build_node(
            flat_ci_config, "tf-id", "lineage_bridge.orders_v2", "lkc-abc123", "env-abc"
        )
        assert node.attributes["table_name"] == "lineage_bridge_orders_v2"
        assert node.qualified_name == "confluent_tableflow.lkc-abc123.lineage_bridge_orders_v2"
        assert "lineage_bridge_orders_v2" in node.node_id


class TestBuildUrl:
    def test_with_workspace_url(self, provider):
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.UC_TABLE,
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={"workspace_url": WORKSPACE_URL},
        )
        url = provider.build_url(node)
        assert url == f"{WORKSPACE_URL}/explore/data/catalog/schema/table"

    def test_no_workspace_url(self, provider):
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.UC_TABLE,
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={},
        )
        assert provider.build_url(node) is None

    def test_invalid_qualified_name(self, provider):
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.UC_TABLE,
            qualified_name="no_dots_here",
            display_name="no_dots_here",
            attributes={"workspace_url": WORKSPACE_URL},
        )
        assert provider.build_url(node) is None


class TestEnrich:
    @respx.mock
    async def test_enrich_merges_attributes(self, provider, uc_graph, no_sleep):
        fixture = load_fixture("databricks_table.json")
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(return_value=httpx.Response(200, json=fixture))

        await provider.enrich(uc_graph)

        node = uc_graph.nodes[0]
        assert node.attributes["owner"] == "confluent-tableflow-sp"
        assert node.attributes["table_type"] == "EXTERNAL"
        assert len(node.attributes["columns"]) == 4
        assert "storage_location" in node.attributes

    async def test_enrich_skips_without_credentials(self, provider_no_creds, uc_graph):
        """Enrichment should be a no-op when no credentials are configured."""
        original_attrs = dict(uc_graph.nodes[0].attributes)
        await provider_no_creds.enrich(uc_graph)
        assert uc_graph.nodes[0].attributes == original_attrs

    @respx.mock
    async def test_enrich_handles_401(self, provider, uc_graph, no_sleep):
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(return_value=httpx.Response(401, json={"error": "unauthorized"}))

        # Should not raise
        await provider.enrich(uc_graph)
        # Attributes should be unchanged
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_404(self, provider, uc_graph, no_sleep):
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(return_value=httpx.Response(404, json={"error": "not found"}))

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_retries_on_429(self, provider, uc_graph, no_sleep):
        """429 triggers retry; succeeds on second attempt."""
        fixture = load_fixture("databricks_table.json")
        route = respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        )
        route.side_effect = [
            httpx.Response(429, json={"error": "rate limited"}),
            httpx.Response(200, json=fixture),
        ]

        await provider.enrich(uc_graph)

        node = uc_graph.nodes[0]
        assert node.attributes["owner"] == "confluent-tableflow-sp"
        assert route.call_count == 2

    @respx.mock
    async def test_enrich_exhausts_retries_on_503(self, provider, uc_graph, no_sleep):
        """503 three times exhausts retries without raising."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(return_value=httpx.Response(503, json={"error": "unavailable"}))

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_http_error(self, provider, uc_graph, no_sleep):
        """Network-level errors are handled gracefully."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(side_effect=httpx.ConnectError("connection refused"))

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_unexpected_status(self, provider, uc_graph, no_sleep):
        """Unexpected status codes (e.g. 418) are logged and skipped."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders-tableflow"
        ).mock(return_value=httpx.Response(418, json={"error": "teapot"}))

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    async def test_enrich_empty_graph(self, provider):
        """Enriching an empty graph is a no-op."""
        graph = LineageGraph()
        await provider.enrich(graph)
        assert graph.node_count == 0
