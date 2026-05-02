# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.ui.graph_renderer."""

from __future__ import annotations

import pytest

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.ui.graph_renderer import (
    _build_tooltip,
    _collect_hop_neighborhood,
    _compute_dag_layout,
    _fmt_bytes,
    _get_connected_node_ids,
    _get_topics_with_schemas,
    _trunc,
    render_graph_raw,
)
from lineage_bridge.ui.styles import TOPIC_WITH_SCHEMA_ICON

# ── Helper to build node IDs matching the conftest convention ────────


def _nid(name: str, ntype: NodeType, env: str = "env-abc123") -> str:
    return f"confluent:{ntype.value}:{env}:{name}"


TOPIC_ORDERS = _nid("orders", NodeType.KAFKA_TOPIC)
TOPIC_CUSTOMERS = _nid("customers", NodeType.KAFKA_TOPIC)
TOPIC_ENRICHED = _nid("enriched_orders", NodeType.KAFKA_TOPIC)
SCHEMA_ORDERS = _nid("orders-value", NodeType.SCHEMA)
SCHEMA_CUSTOMERS = _nid("customers-value", NodeType.SCHEMA)
CONNECTOR_PG = _nid("postgres-source-orders", NodeType.CONNECTOR)
KSQL = _nid("CSAS_ENRICHED_ORDERS_0", NodeType.KSQLDB_QUERY)
CG = _nid("order-processing-group", NodeType.CONSUMER_GROUP)
EXT_PG = _nid("pg-prod.ecommerce.orders", NodeType.EXTERNAL_DATASET)
EXT_S3 = _nid("s3://acme-data-lake/orders", NodeType.EXTERNAL_DATASET, env="env-xyz789")


# ── _trunc tests ────────────────────────────────────────────────────


class TestTrunc:
    def test_short_text_unchanged(self):
        assert _trunc("hello", 10) == "hello"

    def test_exact_length_unchanged(self):
        assert _trunc("1234567890", 10) == "1234567890"

    def test_long_text_truncated(self):
        result = _trunc("1234567890X", 10)
        assert len(result) == 10
        assert result.endswith("\u2026")
        assert result == "123456789\u2026"

    def test_default_max_len(self):
        short = "a" * 40
        long = "a" * 41
        assert _trunc(short) == short
        assert len(_trunc(long)) == 40


# ── _fmt_bytes tests ────────────────────────────────────────────────


class TestFmtBytes:
    def test_bytes(self):
        assert "B" in _fmt_bytes(500)

    def test_kilobytes(self):
        result = _fmt_bytes(2048)
        assert "KB" in result

    def test_megabytes(self):
        result = _fmt_bytes(5 * 1024 * 1024)
        assert "MB" in result

    def test_gigabytes(self):
        result = _fmt_bytes(3 * 1024**3)
        assert "GB" in result

    def test_terabytes(self):
        result = _fmt_bytes(2 * 1024**4)
        assert "TB" in result

    def test_petabytes(self):
        result = _fmt_bytes(1.5 * 1024**5)
        assert "PB" in result

    def test_zero(self):
        result = _fmt_bytes(0)
        assert result == "0.0 B"


# ── _compute_dag_layout tests ───────────────────────────────────────


class TestComputeDagLayoutEmpty:
    def test_empty_graph_returns_empty(self):
        assert _compute_dag_layout([], []) == {}


class TestComputeDagLayoutSingleNode:
    def test_single_node_gets_position(self):
        positions = _compute_dag_layout(["A"], [])
        assert "A" in positions
        assert "x" in positions["A"]
        assert "y" in positions["A"]


class TestComputeDagLayoutLinearChain:
    def test_linear_chain_layer_ordering(self):
        """A -> B -> C should produce increasing x (layers left-to-right)."""
        positions = _compute_dag_layout(
            ["A", "B", "C"],
            [("A", "B"), ("B", "C")],
        )
        assert positions["A"]["x"] < positions["B"]["x"]
        assert positions["B"]["x"] < positions["C"]["x"]

    def test_linear_chain_all_positioned(self):
        positions = _compute_dag_layout(
            ["A", "B", "C"],
            [("A", "B"), ("B", "C")],
        )
        assert len(positions) == 3
        for nid in ("A", "B", "C"):
            assert "x" in positions[nid]
            assert "y" in positions[nid]


class TestComputeDagLayoutMultipleRoots:
    def test_multiple_roots_same_layer(self):
        """Two roots feeding into the same child should be in layer 0."""
        positions = _compute_dag_layout(
            ["R1", "R2", "C"],
            [("R1", "C"), ("R2", "C")],
        )
        assert positions["R1"]["x"] == positions["R2"]["x"]
        assert positions["C"]["x"] > positions["R1"]["x"]

    def test_multiple_roots_different_y(self):
        """Two roots in the same layer should have different y positions."""
        positions = _compute_dag_layout(
            ["R1", "R2", "C"],
            [("R1", "C"), ("R2", "C")],
        )
        assert positions["R1"]["y"] != positions["R2"]["y"]


class TestComputeDagLayoutDisconnected:
    def test_disconnected_components(self):
        """Disconnected nodes all get positions in layer 0."""
        positions = _compute_dag_layout(
            ["A", "B", "C", "D"],
            [("A", "B")],
        )
        assert len(positions) == 4
        # C and D are disconnected, should be in layer 0
        assert positions["C"]["x"] == positions["A"]["x"]
        assert positions["D"]["x"] == positions["A"]["x"]
        # B is downstream of A
        assert positions["B"]["x"] > positions["A"]["x"]

    def test_two_separate_chains(self):
        """Two separate chains should each have correct internal ordering."""
        positions = _compute_dag_layout(
            ["A", "B", "X", "Y"],
            [("A", "B"), ("X", "Y")],
        )
        assert positions["A"]["x"] < positions["B"]["x"]
        assert positions["X"]["x"] < positions["Y"]["x"]


class TestComputeDagLayoutDiamond:
    def test_diamond_shape(self):
        """A -> B, A -> C, B -> D, C -> D: D should be deepest layer."""
        positions = _compute_dag_layout(
            ["A", "B", "C", "D"],
            [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")],
        )
        assert positions["A"]["x"] < positions["B"]["x"]
        assert positions["A"]["x"] < positions["C"]["x"]
        assert positions["D"]["x"] > positions["B"]["x"]
        assert positions["D"]["x"] > positions["C"]["x"]


class TestComputeDagLayoutSpacing:
    def test_custom_level_sep(self):
        positions = _compute_dag_layout(["A", "B"], [("A", "B")], level_sep=500)
        assert positions["B"]["x"] - positions["A"]["x"] == 500

    def test_custom_node_sep(self):
        positions = _compute_dag_layout(["A", "B", "C"], [("A", "B"), ("A", "C")], node_sep=200)
        # B and C are in the same layer, their y gap should be node_sep
        y_vals = sorted([positions["B"]["y"], positions["C"]["y"]])
        assert y_vals[1] - y_vals[0] == 200


# ── render_graph_raw tests ───────────────────────────────────────────


class TestRenderGraphRawDefaults:
    """render_graph_raw with all default arguments."""

    def test_schema_nodes_excluded(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        node_ids = {n["id"] for n in nodes}
        assert SCHEMA_ORDERS not in node_ids
        assert SCHEMA_CUSTOMERS not in node_ids

    def test_has_schema_edges_excluded(self, sample_graph: LineageGraph):
        _, edges = render_graph_raw(sample_graph)
        edge_types = {e["label"] for e in edges}
        assert "has schema" not in edge_types

    def test_nodes_are_dicts(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        assert len(nodes) > 0
        for n in nodes:
            assert isinstance(n, dict)
            assert "id" in n
            assert "label" in n
            assert "title" in n

    def test_edges_are_dicts(self, sample_graph: LineageGraph):
        _, edges = render_graph_raw(sample_graph)
        assert len(edges) > 0
        for e in edges:
            assert isinstance(e, dict)
            assert "from" in e
            assert "to" in e
            assert "label" in e


class TestRenderGraphRawVisJsFields:
    """Ensure vis.js-required fields are present on all nodes and edges."""

    def test_node_has_vis_js_fields(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        required_fields = {"id", "label", "shape", "image", "size"}
        for n in nodes:
            missing = required_fields - set(n.keys())
            assert not missing, f"Node {n['id']} missing fields: {missing}"

    def test_node_shape_is_image(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        for n in nodes:
            assert n["shape"] == "image"

    def test_node_size_is_positive_int(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        for n in nodes:
            assert isinstance(n["size"], int)
            assert n["size"] > 0

    def test_node_image_is_data_uri(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        for n in nodes:
            assert n["image"].startswith("data:image/svg+xml;base64,")

    def test_edge_has_vis_js_fields(self, sample_graph: LineageGraph):
        _, edges = render_graph_raw(sample_graph)
        required_fields = {"from", "to", "color", "arrows"}
        for e in edges:
            missing = required_fields - set(e.keys())
            assert not missing, f"Edge {e['from']}->{e['to']} missing fields: {missing}"

    def test_edge_arrows_to_enabled(self, sample_graph: LineageGraph):
        _, edges = render_graph_raw(sample_graph)
        for e in edges:
            assert e["arrows"]["to"]["enabled"] is True


class TestRenderGraphRawAllNodeTypes:
    """Each NodeType (except SCHEMA) should produce valid vis.js props."""

    @pytest.mark.parametrize(
        "ntype",
        [nt for nt in NodeType if nt != NodeType.SCHEMA],
    )
    def test_node_type_renders(self, ntype: NodeType):
        graph = LineageGraph()
        n1 = LineageNode(
            node_id=f"test:{ntype.value}:env:n1",
            system=SystemType.CONFLUENT,
            node_type=ntype,
            qualified_name="n1",
            display_name="n1",
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        n2 = LineageNode(
            node_id="test:kafka_topic:env:anchor",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="anchor",
            display_name="anchor",
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_edge(
            LineageEdge(src_id=n1.node_id, dst_id=n2.node_id, edge_type=EdgeType.PRODUCES)
        )
        nodes, _ = render_graph_raw(graph)
        node_ids = {n["id"] for n in nodes}
        assert n1.node_id in node_ids


class TestRenderGraphRawTypeFilter:
    """render_graph_raw with type filters."""

    def test_exclude_connectors(self, sample_graph: LineageGraph):
        filters = {NodeType.CONNECTOR: False}
        nodes, _ = render_graph_raw(sample_graph, filters=filters)
        node_types = set()
        for n in nodes:
            node = sample_graph.get_node(n["id"])
            if node:
                node_types.add(node.node_type)
        assert NodeType.CONNECTOR not in node_types

    def test_exclude_consumer_groups(self, sample_graph: LineageGraph):
        filters = {NodeType.CONSUMER_GROUP: False}
        nodes, _ = render_graph_raw(sample_graph, filters=filters)
        node_ids = {n["id"] for n in nodes}
        assert CG not in node_ids


class TestRenderGraphRawEnvironmentFilter:
    """render_graph_raw with environment_filter."""

    def test_filter_by_environment(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph, environment_filter="env-abc123")
        node_ids = {n["id"] for n in nodes}
        # s3 node has env-xyz789 so should be excluded
        assert EXT_S3 not in node_ids
        # Orders topic is in env-abc123, should be included
        assert TOPIC_ORDERS in node_ids

    def test_nonexistent_env_returns_empty(self, sample_graph: LineageGraph):
        nodes, edges = render_graph_raw(sample_graph, environment_filter="env-nonexistent")
        assert len(nodes) == 0
        assert len(edges) == 0


class TestRenderGraphRawClusterFilter:
    """render_graph_raw with cluster_filter."""

    def test_filter_by_cluster(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph, cluster_filter="lkc-abc123")
        node_ids = {n["id"] for n in nodes}
        # All nodes with cluster_id=lkc-abc123 should be present
        assert TOPIC_ORDERS in node_ids
        for n in nodes:
            node = sample_graph.get_node(n["id"])
            assert node is not None
            assert node.cluster_id == "lkc-abc123"


class TestRenderGraphRawSearchQuery:
    """render_graph_raw with search_query."""

    def test_search_by_name(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph, search_query="orders")
        node_ids = {n["id"] for n in nodes}
        # Matched nodes must be present
        assert TOPIC_ORDERS in node_ids
        assert TOPIC_ENRICHED in node_ids

    def test_search_case_insensitive(self, sample_graph: LineageGraph):
        nodes_lower, _ = render_graph_raw(sample_graph, search_query="orders")
        nodes_upper, _ = render_graph_raw(sample_graph, search_query="ORDERS")
        ids_lower = {n["id"] for n in nodes_lower}
        ids_upper = {n["id"] for n in nodes_upper}
        assert ids_lower == ids_upper

    def test_search_no_match(self, sample_graph: LineageGraph):
        nodes, edges = render_graph_raw(sample_graph, search_query="zzz_no_match_zzz")
        assert len(nodes) == 0
        assert len(edges) == 0

    def test_search_expands_neighborhood(self, sample_graph: LineageGraph):
        """Search for a node should also include its neighbors."""
        nodes, _ = render_graph_raw(sample_graph, search_query="CSAS_ENRICHED")
        node_ids = {n["id"] for n in nodes}
        # KSQL node matched, so its neighbors should be included
        assert KSQL in node_ids
        assert TOPIC_ORDERS in node_ids  # upstream
        assert TOPIC_ENRICHED in node_ids  # downstream


class TestRenderGraphRawHideDisconnected:
    """render_graph_raw with hide_disconnected=False."""

    def test_hide_disconnected_false_includes_all(self, sample_graph: LineageGraph):
        nodes_hidden, _ = render_graph_raw(sample_graph, hide_disconnected=True)
        nodes_shown, _ = render_graph_raw(sample_graph, hide_disconnected=False)
        # With hide_disconnected=False, we get at least as many nodes
        assert len(nodes_shown) >= len(nodes_hidden)


class TestRenderGraphRawHopNeighborhood:
    """render_graph_raw with selected_node + hops."""

    def test_hop_restricts_neighborhood(self, sample_graph: LineageGraph):
        nodes_full, _ = render_graph_raw(sample_graph)
        nodes_hop, _ = render_graph_raw(
            sample_graph,
            selected_node=TOPIC_ORDERS,
            hops=1,
        )
        # The hop-restricted view should have fewer or equal nodes
        assert len(nodes_hop) <= len(nodes_full)
        # The center node must be in the result
        hop_ids = {n["id"] for n in nodes_hop}
        assert TOPIC_ORDERS in hop_ids

    def test_hop_includes_direct_neighbors(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(
            sample_graph,
            selected_node=TOPIC_ORDERS,
            hops=1,
        )
        hop_ids = {n["id"] for n in nodes}
        # ext_pg produces into orders, so it's 1 hop upstream
        assert EXT_PG in hop_ids
        # ksql consumes from orders, so it's 1 hop downstream
        assert KSQL in hop_ids

    def test_nonexistent_selected_node_ignored(self, sample_graph: LineageGraph):
        """Selecting a node that doesn't exist should return the full graph."""
        nodes_full, _ = render_graph_raw(sample_graph)
        nodes_sel, _ = render_graph_raw(sample_graph, selected_node="nonexistent:id")
        assert len(nodes_sel) == len(nodes_full)


class TestRenderGraphRawTopicWithSchemaBadge:
    """Topics with HAS_SCHEMA edges get the badge icon."""

    def test_topic_with_schema_has_badge_icon(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        orders_node = next(n for n in nodes if n["id"] == TOPIC_ORDERS)
        assert orders_node["image"] == TOPIC_WITH_SCHEMA_ICON

    def test_topic_without_schema_has_normal_icon(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph)
        enriched_node = next(n for n in nodes if n["id"] == TOPIC_ENRICHED)
        assert enriched_node["image"] != TOPIC_WITH_SCHEMA_ICON


class TestRenderGraphRawEdgesOnlyBetweenIncluded:
    """Edges should only reference included nodes."""

    def test_edges_reference_included_nodes(self, sample_graph: LineageGraph):
        nodes, edges = render_graph_raw(sample_graph)
        node_ids = {n["id"] for n in nodes}
        for e in edges:
            assert e["from"] in node_ids, f"Edge src {e['from']} not in nodes"
            assert e["to"] in node_ids, f"Edge dst {e['to']} not in nodes"

    def test_edges_filtered_when_nodes_excluded(self, sample_graph: LineageGraph):
        # Exclude consumer groups -- edges to them should also disappear
        filters = {NodeType.CONSUMER_GROUP: False}
        nodes, edges = render_graph_raw(sample_graph, filters=filters)
        node_ids = {n["id"] for n in nodes}
        for e in edges:
            assert e["from"] in node_ids
            assert e["to"] in node_ids


class TestRenderGraphRawEmptyGraph:
    """render_graph_raw with an empty graph."""

    def test_empty_graph(self):
        graph = LineageGraph()
        nodes, edges = render_graph_raw(graph)
        assert nodes == []
        assert edges == []


class TestRenderGraphRawLabelTruncation:
    """Node labels are truncated to 30 chars in render_graph_raw."""

    def test_long_label_truncated(self):
        graph = LineageGraph()
        long_name = "a" * 50
        n1 = LineageNode(
            node_id="test:kafka_topic:env:long",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name=long_name,
            display_name=long_name,
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        n2 = LineageNode(
            node_id="test:connector:env:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="c",
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        graph.add_node(n1)
        graph.add_node(n2)
        graph.add_edge(
            LineageEdge(src_id=n1.node_id, dst_id=n2.node_id, edge_type=EdgeType.PRODUCES)
        )
        nodes, _ = render_graph_raw(graph)
        long_node = next(n for n in nodes if n["id"] == n1.node_id)
        assert len(long_node["label"]) == 30


# ── _build_tooltip tests ────────────────────────────────────────────


class TestBuildTooltip:
    """Tests for the _build_tooltip helper."""

    def test_basic_tooltip(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:my-topic",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="my-topic",
            display_name="My Topic",
            environment_id="env-001",
            cluster_id="lkc-001",
        )
        tip = _build_tooltip(node)
        assert "My Topic" in tip
        assert "Kafka Topic" in tip
        # Tooltip shows env/cluster in location row
        assert "env-001" in tip
        assert "lkc-001" in tip

    def test_tooltip_with_metrics(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={
                "metrics_active": True,
                "metrics_received_records": 1234567.0,
                "metrics_sent_records": 9876543.0,
            },
        )
        tip = _build_tooltip(node)
        assert "Active" in tip
        assert "1,234,567" in tip
        assert "9,876,543" in tip

    def test_tooltip_with_inactive_metric(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={"metrics_active": False},
        )
        tip = _build_tooltip(node)
        assert "Inactive" in tip

    def test_tooltip_with_tags(self):
        node = LineageNode(
            node_id="test:connector:env:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="c",
            tags=["source", "cdc"],
        )
        tip = _build_tooltip(node)
        assert "source" in tip
        assert "cdc" in tip

    def test_tooltip_type_specific_attrs(self):
        node = LineageNode(
            node_id="test:connector:env:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="c",
            attributes={"connector_class": "io.confluent.PostgresSource", "direction": "source"},
        )
        tip = _build_tooltip(node)
        assert "PostgresSource" in tip
        assert "SOURCE" in tip

    def test_tooltip_with_bytes_metrics(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={
                "metrics_received_bytes": 5242880.0,  # 5 MB
                "metrics_sent_bytes": 1073741824.0,  # 1 GB
            },
        )
        tip = _build_tooltip(node)
        assert "Bytes In" in tip
        assert "MB" in tip
        assert "Bytes Out" in tip
        assert "GB" in tip

    def test_tooltip_with_environment_name(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            environment_name="Production",
            cluster_name="Main Cluster",
        )
        tip = _build_tooltip(node)
        assert "Production" in tip
        assert "Main Cluster" in tip


class TestBuildTooltipKafkaTopic:
    def test_topic_partitions(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={"partitions_count": 12, "replication_factor": 3},
        )
        tip = _build_tooltip(node)
        assert "Partitions: 12" in tip
        assert "RF: 3" in tip

    def test_topic_internal(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={"is_internal": True},
        )
        tip = _build_tooltip(node)
        assert "Internal topic" in tip

    def test_topic_description_truncated(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            attributes={"description": "x" * 100},
        )
        tip = _build_tooltip(node)
        assert "..." in tip

    def test_topic_name_in_tooltip(self):
        node = LineageNode(
            node_id="test:kafka_topic:env:orders",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="orders",
            display_name="orders",
        )
        tip = _build_tooltip(node)
        assert "orders" in tip


class TestBuildTooltipConnector:
    def test_connector_class(self):
        node = LineageNode(
            node_id="test:connector:env:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="my-connector",
            attributes={
                "connector_class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "tasks_max": 4,
                "output_data_format": "AVRO",
            },
        )
        tip = _build_tooltip(node)
        assert "JdbcSourceConnector" in tip
        assert "Tasks: 4" in tip
        assert "Format: AVRO" in tip


class TestBuildTooltipUcTable:
    def test_uc_table_catalog_info(self):
        node = LineageNode(
            node_id="databricks:uc_table:env:my_catalog.my_schema.my_table",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="my_catalog.my_schema.my_table",
            display_name="my_table",
            attributes={
                "catalog_name": "my_catalog",
                "workspace_url": "https://myworkspace.databricks.com",
            },
        )
        tip = _build_tooltip(node)
        assert "Catalog: my_catalog" in tip
        assert "Databricks UC" in tip
        assert "Unity Catalog Table" in tip

    def test_uc_table_with_catalog_type_only(self):
        node = LineageNode(
            node_id="databricks:uc_table:env:cat.schema.tbl",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="cat.schema.tbl",
            display_name="tbl",
            attributes={"catalog_name": "cat"},
        )
        tip = _build_tooltip(node)
        assert "Catalog: cat" in tip
        assert "Databricks UC" not in tip  # no workspace_url


class TestBuildTooltipGlueTable:
    def test_glue_table_not_empty(self):
        """Glue tables are not currently in the _build_tooltip type-specific section,
        but they should still produce a non-empty tooltip with the label."""
        node = LineageNode(
            node_id="aws:glue_table:env:mydb.mytable",
            system=SystemType.AWS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="AWS_GLUE",
            qualified_name="mydb.mytable",
            display_name="mytable",
            attributes={"database": "mydb"},
        )
        tip = _build_tooltip(node)
        assert "AWS Glue Table" in tip
        assert "mytable" in tip
        assert len(tip) > 0


class TestBuildTooltipFlinkJob:
    def test_flink_job_phase(self):
        node = LineageNode(
            node_id="test:flink_job:env:fj",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="fj",
            display_name="my-flink-job",
            attributes={
                "phase": "RUNNING",
                "compute_pool_id": "lfcp-abc123",
                "principal": "sa-12345",
            },
        )
        tip = _build_tooltip(node)
        # Status now lives in the pill (header) instead of as "Phase: X" line
        assert "RUNNING" in tip
        assert "Pool: lfcp-abc123" in tip
        assert "Principal: sa-12345" in tip


class TestBuildTooltipKsqldbQuery:
    def test_ksqldb_query_state(self):
        node = LineageNode(
            node_id="test:ksqldb_query:env:q",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KSQLDB_QUERY,
            qualified_name="q",
            display_name="CSAS_ORDERS",
            attributes={"state": "RUNNING", "ksqldb_cluster_id": "lksqlc-abc"},
        )
        tip = _build_tooltip(node)
        assert "RUNNING" in tip
        assert "Cluster: lksqlc-abc" in tip


class TestBuildTooltipTableflowTable:
    def test_tableflow_details(self):
        node = LineageNode(
            node_id="test:tableflow_table:env:tf",
            system=SystemType.CONFLUENT,
            node_type=NodeType.TABLEFLOW_TABLE,
            qualified_name="tf",
            display_name="orders-table",
            attributes={
                "phase": "Stable",
                "table_formats": ["ICEBERG", "DELTA"],
                "storage_kind": "S3",
            },
        )
        tip = _build_tooltip(node)
        assert "Stable" in tip
        assert "ICEBERG" in tip
        assert "DELTA" in tip
        assert "Storage: S3" in tip

    def test_tableflow_suspended(self):
        node = LineageNode(
            node_id="test:tableflow_table:env:tf",
            system=SystemType.CONFLUENT,
            node_type=NodeType.TABLEFLOW_TABLE,
            qualified_name="tf",
            display_name="suspended-table",
            attributes={"suspended": True},
        )
        tip = _build_tooltip(node)
        assert "SUSPENDED" in tip


class TestBuildTooltipSchema:
    def test_schema_details(self):
        node = LineageNode(
            node_id="test:schema:env:s",
            system=SystemType.CONFLUENT,
            node_type=NodeType.SCHEMA,
            qualified_name="orders-value",
            display_name="orders-value",
            attributes={"schema_type": "AVRO", "version": 3, "field_count": 12},
        )
        tip = _build_tooltip(node)
        assert "AVRO" in tip
        assert "v3" in tip
        assert "12 fields" in tip


class TestBuildTooltipConsumerGroup:
    def test_consumer_group_details(self):
        node = LineageNode(
            node_id="test:consumer_group:env:cg",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONSUMER_GROUP,
            qualified_name="cg",
            display_name="my-group",
            attributes={"state": "STABLE", "is_simple": True},
        )
        tip = _build_tooltip(node)
        assert "STABLE" in tip
        assert "Simple consumer" in tip


class TestBuildTooltipExternalDataset:
    def test_external_dataset_inferred(self):
        node = LineageNode(
            node_id="test:external_dataset:env:ext",
            system=SystemType.EXTERNAL,
            node_type=NodeType.EXTERNAL_DATASET,
            qualified_name="pg.public.orders",
            display_name="pg.public.orders",
            attributes={"inferred_from": "connector config"},
        )
        tip = _build_tooltip(node)
        assert "From: connector config" in tip


class TestBuildTooltipAllNodeTypes:
    """Every NodeType should produce a non-empty tooltip."""

    @pytest.mark.parametrize("ntype", list(NodeType))
    def test_tooltip_not_empty(self, ntype: NodeType):
        system = SystemType.CONFLUENT
        if ntype == NodeType.CATALOG_TABLE:
            system = SystemType.DATABRICKS
        elif ntype == NodeType.CATALOG_TABLE:
            system = SystemType.AWS
        elif ntype == NodeType.EXTERNAL_DATASET:
            system = SystemType.EXTERNAL

        node = LineageNode(
            node_id=f"test:{ntype.value}:env:test-node",
            system=system,
            node_type=ntype,
            qualified_name="test-node",
            display_name="test-node",
        )
        tip = _build_tooltip(node)
        assert len(tip) > 0
        assert "test-node" in tip


# ── _collect_hop_neighborhood tests ─────────────────────────────────


class TestCollectHopNeighborhood:
    """Tests for the _collect_hop_neighborhood helper."""

    def test_zero_hops_returns_only_center(self, sample_graph: LineageGraph):
        ids = _collect_hop_neighborhood(sample_graph, TOPIC_ORDERS, 0)
        assert ids == {TOPIC_ORDERS}

    def test_one_hop_includes_neighbors(self, sample_graph: LineageGraph):
        ids = _collect_hop_neighborhood(sample_graph, TOPIC_ORDERS, 1)
        assert TOPIC_ORDERS in ids
        assert EXT_PG in ids  # upstream producer
        assert KSQL in ids  # downstream consumer

    def test_two_hops_reaches_further(self, sample_graph: LineageGraph):
        ids_1 = _collect_hop_neighborhood(sample_graph, TOPIC_ORDERS, 1)
        ids_2 = _collect_hop_neighborhood(sample_graph, TOPIC_ORDERS, 2)
        assert len(ids_2) >= len(ids_1)
        # enriched_orders is 2 hops: orders -> ksql -> enriched_orders
        assert TOPIC_ENRICHED in ids_2


# ── _get_connected_node_ids tests ───────────────────────────────────


class TestGetConnectedNodeIds:
    """Tests for the _get_connected_node_ids helper."""

    def test_returns_nodes_with_edges(self, sample_graph: LineageGraph):
        connected = _get_connected_node_ids(sample_graph)
        # orders topic is connected (has edges)
        assert TOPIC_ORDERS in connected

    def test_returns_both_src_and_dst(self, sample_graph: LineageGraph):
        connected = _get_connected_node_ids(sample_graph)
        # Check that non-HAS_SCHEMA edge endpoints are included
        for edge in sample_graph.edges:
            if edge.edge_type == EdgeType.HAS_SCHEMA:
                continue
            assert edge.src_id in connected
            assert edge.dst_id in connected

    def test_empty_graph_returns_empty(self):
        g = LineageGraph()
        connected = _get_connected_node_ids(g)
        assert connected == set()

    def test_has_schema_edges_not_counted(self):
        """A node connected only via HAS_SCHEMA should not be 'connected'."""
        g = LineageGraph()
        t = LineageNode(
            node_id="t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
        )
        s = LineageNode(
            node_id="s",
            system=SystemType.CONFLUENT,
            node_type=NodeType.SCHEMA,
            qualified_name="s",
            display_name="s",
        )
        g.add_node(t)
        g.add_node(s)
        g.add_edge(LineageEdge(src_id="t", dst_id="s", edge_type=EdgeType.HAS_SCHEMA))
        connected = _get_connected_node_ids(g)
        assert connected == set()


# ── _get_topics_with_schemas tests ──────────────────────────────────


class TestGetTopicsWithSchemas:
    def test_returns_topics_with_schema_edges(self, sample_graph: LineageGraph):
        topics = _get_topics_with_schemas(sample_graph)
        assert TOPIC_ORDERS in topics
        assert TOPIC_CUSTOMERS in topics

    def test_excludes_topics_without_schemas(self, sample_graph: LineageGraph):
        topics = _get_topics_with_schemas(sample_graph)
        assert TOPIC_ENRICHED not in topics

    def test_empty_graph(self):
        g = LineageGraph()
        assert _get_topics_with_schemas(g) == set()


# ── Status badge icon tests ─────────────────────────────────────────


class TestRenderGraphRawStatusBadge:
    """Nodes with phase/state attributes get status badge icons."""

    def test_flink_job_with_phase_gets_badge(self):
        graph = LineageGraph()
        flink = LineageNode(
            node_id="test:flink_job:env:fj",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="fj",
            display_name="flink-job",
            environment_id="env-1",
            cluster_id="lkc-1",
            attributes={"phase": "RUNNING"},
        )
        topic = LineageNode(
            node_id="test:kafka_topic:env:t",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t",
            display_name="t",
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        graph.add_node(flink)
        graph.add_node(topic)
        graph.add_edge(
            LineageEdge(src_id=flink.node_id, dst_id=topic.node_id, edge_type=EdgeType.PRODUCES)
        )
        nodes, _ = render_graph_raw(graph)
        flink_node = next(n for n in nodes if n["id"] == flink.node_id)
        # Should have a status badge icon, not the default icon
        from lineage_bridge.ui.styles import NODE_ICONS

        assert flink_node["image"] != NODE_ICONS[NodeType.FLINK_JOB]

    def test_topic_with_phase_does_not_get_badge(self):
        """Topics should NOT get status badges even if they have a phase attr."""
        graph = LineageGraph()
        t1 = LineageNode(
            node_id="test:kafka_topic:env:t1",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t1",
            display_name="t1",
            environment_id="env-1",
            cluster_id="lkc-1",
            attributes={"phase": "RUNNING"},
        )
        t2 = LineageNode(
            node_id="test:kafka_topic:env:t2",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="t2",
            display_name="t2",
            environment_id="env-1",
            cluster_id="lkc-1",
        )
        graph.add_node(t1)
        graph.add_node(t2)
        graph.add_edge(
            LineageEdge(src_id=t1.node_id, dst_id=t2.node_id, edge_type=EdgeType.PRODUCES)
        )
        nodes, _ = render_graph_raw(graph)
        t1_node = next(n for n in nodes if n["id"] == t1.node_id)
        from lineage_bridge.ui.styles import NODE_ICONS

        assert t1_node["image"] == NODE_ICONS[NodeType.KAFKA_TOPIC]
