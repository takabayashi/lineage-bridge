"""Unit tests for lineage_bridge.ui.graph_renderer."""

from __future__ import annotations

import pytest

from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.ui.graph_renderer import (
    _build_tooltip,
    _collect_hop_neighborhood,
    _get_connected_node_ids,
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
EXT_S3 = _nid(
    "s3://acme-data-lake/orders", NodeType.EXTERNAL_DATASET, env="env-xyz789"
)


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
        nodes, _ = render_graph_raw(
            sample_graph, environment_filter="env-abc123"
        )
        node_ids = {n["id"] for n in nodes}
        # s3 node has env-xyz789 so should be excluded
        assert EXT_S3 not in node_ids
        # Orders topic is in env-abc123, should be included
        assert TOPIC_ORDERS in node_ids

    def test_nonexistent_env_returns_empty(self, sample_graph: LineageGraph):
        nodes, edges = render_graph_raw(
            sample_graph, environment_filter="env-nonexistent"
        )
        assert len(nodes) == 0
        assert len(edges) == 0


class TestRenderGraphRawClusterFilter:
    """render_graph_raw with cluster_filter."""

    def test_filter_by_cluster(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(
            sample_graph, cluster_filter="lkc-abc123"
        )
        node_ids = {n["id"] for n in nodes}
        # All nodes with cluster_id=lkc-abc123 should be present
        assert TOPIC_ORDERS in node_ids
        # Ext datasets have cluster lkc-abc123 too (from conftest)
        # but s3 has env-xyz789 and same cluster — let's just check included
        for n in nodes:
            node = sample_graph.get_node(n["id"])
            assert node is not None
            assert node.cluster_id == "lkc-abc123"


class TestRenderGraphRawSearchQuery:
    """render_graph_raw with search_query."""

    def test_search_by_name(self, sample_graph: LineageGraph):
        nodes, _ = render_graph_raw(sample_graph, search_query="orders")
        node_ids = {n["id"] for n in nodes}
        assert TOPIC_ORDERS in node_ids
        assert TOPIC_ENRICHED in node_ids
        # customers should not match
        assert TOPIC_CUSTOMERS not in node_ids

    def test_search_case_insensitive(self, sample_graph: LineageGraph):
        nodes_lower, _ = render_graph_raw(sample_graph, search_query="orders")
        nodes_upper, _ = render_graph_raw(sample_graph, search_query="ORDERS")
        ids_lower = {n["id"] for n in nodes_lower}
        ids_upper = {n["id"] for n in nodes_upper}
        assert ids_lower == ids_upper

    def test_search_no_match(self, sample_graph: LineageGraph):
        nodes, edges = render_graph_raw(
            sample_graph, search_query="zzz_no_match_zzz"
        )
        assert len(nodes) == 0
        assert len(edges) == 0


class TestRenderGraphRawHideDisconnected:
    """render_graph_raw with hide_disconnected=False."""

    def test_hide_disconnected_false_includes_all(
        self, sample_graph: LineageGraph
    ):
        nodes_hidden, _ = render_graph_raw(
            sample_graph, hide_disconnected=True
        )
        nodes_shown, _ = render_graph_raw(
            sample_graph, hide_disconnected=False
        )
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


class TestRenderGraphRawTopicWithSchemaBadge:
    """Topics with HAS_SCHEMA edges get the badge icon."""

    def test_topic_with_schema_has_badge_icon(
        self, sample_graph: LineageGraph
    ):
        nodes, _ = render_graph_raw(sample_graph)
        orders_node = next(n for n in nodes if n["id"] == TOPIC_ORDERS)
        assert orders_node["image"] == TOPIC_WITH_SCHEMA_ICON

    def test_topic_without_schema_has_normal_icon(
        self, sample_graph: LineageGraph
    ):
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

    def test_edges_filtered_when_nodes_excluded(
        self, sample_graph: LineageGraph
    ):
        # Exclude consumer groups -- edges to them should also disappear
        filters = {NodeType.CONSUMER_GROUP: False}
        nodes, edges = render_graph_raw(sample_graph, filters=filters)
        node_ids = {n["id"] for n in nodes}
        for e in edges:
            assert e["from"] in node_ids
            assert e["to"] in node_ids


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
        assert "my-topic" in tip
        assert "Env: env-001" in tip
        assert "Cluster: lkc-001" in tip

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
        assert "Active: Yes" in tip
        assert "Records in: 1,234,567" in tip
        assert "Records out: 9,876,543" in tip

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
        assert "Active: No" in tip

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
        assert "Tags: source, cdc" in tip

    def test_tooltip_other_attrs(self):
        node = LineageNode(
            node_id="test:connector:env:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="c",
            attributes={"connector.class": "PostgresSource"},
        )
        tip = _build_tooltip(node)
        assert "connector.class: PostgresSource" in tip
        assert "---" in tip


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
        # Check that edge endpoints are included
        for edge in sample_graph.edges:
            assert edge.src_id in connected
            assert edge.dst_id in connected

    def test_empty_graph_returns_empty(self):
        g = LineageGraph()
        connected = _get_connected_node_ids(g)
        assert connected == set()
