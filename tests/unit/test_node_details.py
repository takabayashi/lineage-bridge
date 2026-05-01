# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.ui.node_details."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lineage_bridge.models.graph import (
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _make_graph(
    node_type: NodeType,
    system: SystemType = SystemType.CONFLUENT,
    qualified_name: str = "test-resource",
    attributes: dict | None = None,
    environment_id: str | None = "env-123",
    cluster_id: str | None = "lkc-abc",
    catalog_type: str | None = None,
) -> tuple[str, LineageGraph]:
    """Build a LineageGraph with a single node and return (node_id, graph)."""
    node_id = f"{system.value}:{node_type.value}:{environment_id or 'none'}:{qualified_name}"
    node = LineageNode(
        node_id=node_id,
        system=system,
        node_type=node_type,
        catalog_type=catalog_type,
        qualified_name=qualified_name,
        display_name=qualified_name,
        environment_id=environment_id,
        cluster_id=cluster_id,
        attributes=attributes or {},
    )
    graph = LineageGraph()
    graph.add_node(node)
    return node_id, graph


def _mock_col() -> MagicMock:
    """Return a MagicMock that works as a context manager (st.columns element)."""
    col = MagicMock()
    col.__enter__ = MagicMock(return_value=col)
    col.__exit__ = MagicMock(return_value=False)
    return col


def _setup_st_mock(st_mock: MagicMock, sel_id: str) -> None:
    """Configure common streamlit mock attributes."""
    st_mock.session_state = MagicMock()
    st_mock.session_state.selected_node = sel_id
    st_mock.session_state._dismissed_node = None
    st_mock.button.return_value = False

    def columns_side_effect(n):
        return [_mock_col() for _ in range(n)]

    st_mock.columns.side_effect = columns_side_effect
    st_mock.expander.return_value = _mock_col()


def _assert_markdown_contains(st_mock: MagicMock, needle: str) -> None:
    """Assert that at least one st.markdown call contains the given substring."""
    calls = st_mock.markdown.call_args_list
    found = any(needle in str(c) for c in calls)
    assert found, (
        f"Expected '{needle}' in st.markdown calls. "
        f"Got: {[str(c.args[0])[:80] for c in calls if c.args]}"
    )


# ── Test class per NodeType ────────────────────────────────────────


class TestRenderKafkaTopic:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_topic_details(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.KAFKA_TOPIC,
            attributes={"partitions_count": 6, "replication_factor": 3},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Kafka Topic")
        _assert_markdown_contains(st_mock, "Topic Configuration")


class TestRenderConnector:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_connector_details(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.CONNECTOR,
            attributes={"connector_class": "io.confluent.connect.s3.S3SinkConnector"},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Connector")
        _assert_markdown_contains(st_mock, "Connector Configuration")


class TestRenderFlinkJob:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_flink_job(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.FLINK_JOB,
            attributes={"phase": "RUNNING", "compute_pool_id": "lfcp-123"},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Flink Job")


class TestRenderKsqldbQuery:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_ksqldb_query(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.KSQLDB_QUERY,
            attributes={"state": "RUNNING"},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "ksqlDB Query")


class TestRenderTableflowTable:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_tableflow_table(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.TABLEFLOW_TABLE,
            attributes={"phase": "ACTIVE", "table_formats": ["ICEBERG"]},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Tableflow")


class TestRenderUcTable:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_uc_table(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.CATALOG_TABLE,
            system=SystemType.DATABRICKS,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            attributes={
                "catalog_name": "catalog",
                "schema_name": "schema",
                "table_name": "table",
                "workspace_url": "https://workspace.databricks.com",
            },
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Unity Catalog Table")


class TestRenderGlueTable:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_glue_table(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.CATALOG_TABLE,
            system=SystemType.AWS,
            catalog_type="AWS_GLUE",
            qualified_name="glue://mydb/mytable",
            attributes={
                "database": "mydb",
                "table_name": "mytable",
                "aws_region": "us-east-1",
            },
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "AWS Glue Table")


class TestRenderSchema:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_schema_details(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.SCHEMA,
            qualified_name="my-topic-value",
            attributes={
                "schema_type": "AVRO",
                "version": 3,
                "field_count": 12,
                "schema_id": 100042,
            },
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Schema Details")


class TestRenderConsumerGroup:
    @patch("lineage_bridge.ui.node_details.st")
    def test_renders_consumer_group(self, st_mock: MagicMock):
        sel_id, graph = _make_graph(
            NodeType.CONSUMER_GROUP,
            qualified_name="my-consumer-group",
            attributes={"state": "STABLE", "is_simple": False},
        )
        _setup_st_mock(st_mock, sel_id)

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        _assert_markdown_contains(st_mock, "Consumer Group")


# ── Edge cases ─────────────────────────────────────────────────────


class TestRenderNoSelection:
    @patch("lineage_bridge.ui.node_details.st")
    def test_returns_early_when_no_selected_node(self, st_mock: MagicMock):
        st_mock.session_state = MagicMock()
        st_mock.session_state.selected_node = None
        graph = LineageGraph()

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        st_mock.markdown.assert_not_called()

    @patch("lineage_bridge.ui.node_details.st")
    def test_returns_early_when_node_not_in_graph(self, st_mock: MagicMock):
        st_mock.session_state = MagicMock()
        st_mock.session_state.selected_node = "nonexistent:id"
        graph = LineageGraph()

        from lineage_bridge.ui.node_details import render_node_details

        render_node_details(graph)

        st_mock.markdown.assert_not_called()
