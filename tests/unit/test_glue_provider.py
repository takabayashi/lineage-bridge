# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for GlueCatalogProvider."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _make_glue_node(database="db", table_name="tbl"):
    """Helper to create a GLUE_TABLE node for tests."""
    return LineageNode(
        node_id=f"aws:glue_table:env-1:glue://{database}/{table_name}",
        system=SystemType.AWS,
        node_type=NodeType.GLUE_TABLE,
        qualified_name=f"glue://{database}/{table_name}",
        display_name=f"{database}.{table_name} (glue)",
        attributes={"database": database, "table_name": table_name},
    )


def _mock_glue_client():
    """Create a mock boto3 Glue client with exception classes."""
    client = MagicMock()
    client.exceptions.EntityNotFoundException = type("EntityNotFoundException", (Exception,), {})
    client.exceptions.AccessDeniedException = type("AccessDeniedException", (Exception,), {})
    return client


class TestBuildNode:
    def test_creates_correct_node_id(self):
        provider = GlueCatalogProvider()
        node, _edge = provider.build_node(
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
        node, _ = provider.build_node({"aws_glue": {}}, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert node.attributes["database"] == "lkc-abc123"

    def test_dots_preserved_in_table_name(self):
        """Tableflow uses the raw topic name (with dots) as the Glue table name."""
        provider = GlueCatalogProvider()
        node, _ = provider.build_node(
            {"aws_glue": {"database_name": "my_db"}},
            "tf-id",
            "lineage_bridge.order_stats",
            "lkc-abc123",
            "env-abc",
        )
        assert node.attributes["table_name"] == "lineage_bridge.order_stats"
        assert node.display_name == "my_db.lineage_bridge.order_stats (glue)"
        assert "lineage_bridge.order_stats" in node.qualified_name


class TestBuildUrl:
    def test_returns_url_with_database_and_table(self):
        provider = GlueCatalogProvider()
        node = _make_glue_node(database="my_db", table_name="orders")
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


_has_boto3 = __import__("importlib").util.find_spec("boto3") is not None
_skip_no_boto3 = pytest.mark.skipif(not _has_boto3, reason="boto3 not installed")


@_skip_no_boto3
class TestEnrich:
    async def test_enrich_no_region_skips(self):
        """enrich() returns early when no region is configured."""
        provider = GlueCatalogProvider(region=None)
        graph = LineageGraph()
        graph.add_node(_make_glue_node())
        original_attrs = dict(graph.nodes[0].attributes)
        await provider.enrich(graph)
        assert graph.nodes[0].attributes == original_attrs

    async def test_enrich_no_glue_nodes_skips(self):
        """enrich() returns early when graph has no GLUE_TABLE nodes."""
        provider = GlueCatalogProvider(region="us-east-1")
        graph = LineageGraph()
        # Add a non-Glue node
        graph.add_node(
            LineageNode(
                node_id="test",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name="orders",
                display_name="orders",
            )
        )
        # Should not crash
        await provider.enrich(graph)

    async def test_enrich_fetches_metadata(self):
        """enrich() populates node attributes from Glue get_table response."""
        mock_client = _mock_glue_client()
        mock_client.get_table.return_value = {
            "Table": {
                "Owner": "admin",
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "order_id", "Type": "string", "Comment": "Order ID"},
                        {"Name": "amount", "Type": "double", "Comment": None},
                    ],
                    "Location": "s3://bucket/path/",
                    "InputFormat": "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
                    "OutputFormat": "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
                    },
                },
                "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                "Parameters": {"table_type": "ICEBERG"},
                "CreateTime": "2026-04-06T00:00:00",
                "UpdateTime": "2026-04-06T12:00:00",
            }
        }

        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            graph.add_node(_make_glue_node())
            await provider.enrich(graph)

        node = graph.nodes[0]
        assert node.attributes["owner"] == "admin"
        assert node.attributes["table_type"] == "EXTERNAL_TABLE"
        assert len(node.attributes["columns"]) == 2
        assert node.attributes["columns"][0]["name"] == "order_id"
        assert node.attributes["partition_keys"] == [{"name": "dt", "type": "string"}]
        assert node.attributes["storage_location"] == "s3://bucket/path/"
        assert "iceberg" in node.attributes["serde_info"].lower()
        assert node.attributes["aws_region"] == "us-east-1"
        assert node.attributes["parameters"]["table_type"] == "ICEBERG"

    async def test_enrich_table_not_found(self):
        """enrich() skips node when Glue table doesn't exist."""
        mock_client = _mock_glue_client()
        mock_client.get_table.side_effect = mock_client.exceptions.EntityNotFoundException()

        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            graph.add_node(_make_glue_node())
            original_attrs = dict(graph.nodes[0].attributes)
            await provider.enrich(graph)

        assert graph.nodes[0].attributes == original_attrs

    async def test_enrich_access_denied(self):
        """enrich() skips node when access is denied."""
        mock_client = _mock_glue_client()
        mock_client.get_table.side_effect = mock_client.exceptions.AccessDeniedException()

        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            graph.add_node(_make_glue_node())
            original_attrs = dict(graph.nodes[0].attributes)
            await provider.enrich(graph)

        assert graph.nodes[0].attributes == original_attrs

    async def test_enrich_retries_on_transient_error(self):
        """enrich() retries on transient errors and succeeds on second attempt."""
        mock_client = _mock_glue_client()
        mock_client.get_table.side_effect = [
            ConnectionError("timeout"),
            {
                "Table": {
                    "Owner": "admin",
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "int"}],
                        "Location": "s3://bucket/",
                    },
                    "PartitionKeys": [],
                    "Parameters": {},
                }
            },
        ]

        with patch("boto3.client", return_value=mock_client), patch("asyncio.sleep"):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            graph.add_node(_make_glue_node())
            await provider.enrich(graph)

        node = graph.nodes[0]
        assert node.attributes["owner"] == "admin"
        assert len(node.attributes["columns"]) == 1
        assert mock_client.get_table.call_count == 2


@_skip_no_boto3
class TestPushLineage:
    def _make_graph_with_upstream(self):
        """Create a graph with a Glue table that has upstream lineage."""
        from lineage_bridge.models.graph import EdgeType, LineageEdge

        graph = LineageGraph()
        topic = LineageNode(
            node_id="confluent:kafka_topic:env-1:orders",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="orders",
            display_name="orders",
            environment_id="env-1",
            cluster_id="lkc-abc",
        )
        glue = _make_glue_node(database="my_db", table_name="orders")
        graph.add_node(topic)
        graph.add_node(glue)
        graph.add_edge(
            LineageEdge(src_id=topic.node_id, dst_id=glue.node_id, edge_type=EdgeType.MATERIALIZES)
        )
        return graph

    async def test_push_sets_parameters_and_description(self):
        """push_lineage() updates table parameters and description."""
        mock_client = _mock_glue_client()
        mock_client.get_table.return_value = {
            "Table": {
                "Name": "orders",
                "StorageDescriptor": {"Columns": []},
                "Parameters": {"existing_key": "existing_value"},
            }
        }
        mock_client.update_table.return_value = {}

        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = self._make_graph_with_upstream()
            result = await provider.push_lineage(graph)

        assert result.tables_updated == 1
        assert result.properties_set == 1
        assert result.comments_set == 1
        assert not result.errors

        # Verify update_table was called with lineage parameters
        call_args = mock_client.update_table.call_args
        table_input = call_args.kwargs["TableInput"]
        params = table_input["Parameters"]
        assert params["lineage_bridge.source_topics"] == "orders"
        assert params["lineage_bridge.pipeline_type"] == "tableflow"
        assert "existing_key" in params  # Preserved existing params
        assert "LineageBridge" in table_input["Description"]

    async def test_push_no_region_returns_error(self):
        """push_lineage() returns error when no region configured."""
        provider = GlueCatalogProvider(region=None)
        graph = self._make_graph_with_upstream()
        result = await provider.push_lineage(graph)
        assert len(result.errors) == 1
        assert "region" in result.errors[0].lower()

    async def test_push_no_glue_tables(self):
        """push_lineage() returns empty result when no Glue tables in graph."""
        mock_client = _mock_glue_client()
        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            result = await provider.push_lineage(graph)

        assert result.tables_updated == 0
        mock_client.update_table.assert_not_called()

    async def test_push_skips_tables_without_upstream(self):
        """push_lineage() skips tables that have no upstream lineage."""
        mock_client = _mock_glue_client()
        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = LineageGraph()
            graph.add_node(_make_glue_node())  # No upstream edges
            result = await provider.push_lineage(graph)

        assert result.tables_updated == 0
        mock_client.update_table.assert_not_called()

    async def test_push_handles_update_error(self):
        """push_lineage() records errors when update_table fails."""
        mock_client = _mock_glue_client()
        mock_client.get_table.return_value = {
            "Table": {"Name": "tbl", "StorageDescriptor": {"Columns": []}, "Parameters": {}}
        }
        mock_client.update_table.side_effect = Exception("AccessDenied")

        with patch("boto3.client", return_value=mock_client):
            provider = GlueCatalogProvider(region="us-east-1")
            graph = self._make_graph_with_upstream()
            result = await provider.push_lineage(graph)

        assert result.tables_updated == 0
        assert len(result.errors) == 1
        assert "AccessDenied" in result.errors[0]
