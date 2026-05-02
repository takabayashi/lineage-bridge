# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `_collect_legend_rows` — the legend variant builder.

Locks in the contract that a legend row is emitted for each VARIANT actually
present in the graph (per-catalog brand, DLQ-badge, schema-badge), not just
one row per `NodeType`. The renderer (`_render_sidebar_legend`) is a thin
loop over what this function returns.
"""

from __future__ import annotations

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.ui.sidebar.filters import _collect_legend_rows
from lineage_bridge.ui.styles import (
    CATALOG_ICONS,
    CATALOG_STYLES,
    DLQ_TOPIC_ICON,
    NODE_ICONS,
    NODE_TYPE_LABELS,
    TOPIC_WITH_SCHEMA_ICON,
)


def _topic(name: str, *, attrs: dict | None = None) -> LineageNode:
    return LineageNode(
        node_id=f"confluent:kafka_topic:env-1:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id="lkc-1",
        attributes=attrs or {},
    )


def _catalog_table(qn: str, catalog_type: str) -> LineageNode:
    return LineageNode(
        node_id=f"x:catalog_table:env-1:{qn}",
        system=SystemType.AWS,
        node_type=NodeType.CATALOG_TABLE,
        catalog_type=catalog_type,
        qualified_name=qn,
        display_name=qn,
        environment_id="env-1",
    )


def _labels(rows: list[tuple[str, str]]) -> list[str]:
    return [label for _, label in rows]


def test_basic_topic_only():
    """Single topic, no schemas, no DLQs → one row, no badge variants."""
    g = LineageGraph()
    g.add_node(_topic("orders"))
    rows = _collect_legend_rows(g)
    assert _labels(rows) == [NODE_TYPE_LABELS[NodeType.KAFKA_TOPIC]]
    assert rows[0][0] == NODE_ICONS[NodeType.KAFKA_TOPIC]


def test_topic_with_schema_adds_badge_row():
    """A topic with a HAS_SCHEMA edge surfaces a 'Topic with schema' row."""
    g = LineageGraph()
    topic = _topic("orders")
    schema = LineageNode(
        node_id="confluent:schema:env-1:orders-value",
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name="orders-value",
        display_name="orders-value",
        environment_id="env-1",
    )
    g.add_node(topic)
    g.add_node(schema)
    g.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=schema.node_id, edge_type=EdgeType.HAS_SCHEMA)
    )

    rows = _collect_legend_rows(g)
    labels = _labels(rows)
    assert "Topic with schema" in labels
    assert (TOPIC_WITH_SCHEMA_ICON, "Topic with schema") in rows


def test_dlq_topic_adds_badge_row():
    """A topic flagged with role=dlq surfaces a 'DLQ topic' row."""
    g = LineageGraph()
    g.add_node(_topic("orders"))
    g.add_node(_topic("dlq-lcc-7w7m2w", attrs={"role": "dlq"}))
    rows = _collect_legend_rows(g)
    assert (DLQ_TOPIC_ICON, "DLQ topic") in rows


def test_catalog_brand_variants_replace_generic_row():
    """When CATALOG_TABLE nodes carry catalog_type, emit one row per brand."""
    g = LineageGraph()
    g.add_node(_catalog_table("db.uc1", "UNITY_CATALOG"))
    g.add_node(_catalog_table("db.glue1", "AWS_GLUE"))
    g.add_node(_catalog_table("p.bq1", "GOOGLE_DATA_LINEAGE"))

    rows = _collect_legend_rows(g)
    labels = _labels(rows)
    assert CATALOG_STYLES["UNITY_CATALOG"].label in labels
    assert CATALOG_STYLES["AWS_GLUE"].label in labels
    assert CATALOG_STYLES["GOOGLE_DATA_LINEAGE"].label in labels
    # Generic "Catalog Table" row should NOT appear when brands are present.
    assert NODE_TYPE_LABELS[NodeType.CATALOG_TABLE] not in labels
    # And each row uses the brand-specific icon, not the generic one.
    for ct in ("UNITY_CATALOG", "AWS_GLUE", "GOOGLE_DATA_LINEAGE"):
        assert (CATALOG_ICONS[ct], CATALOG_STYLES[ct].label) in rows


def test_brands_sorted_alphabetically_for_stable_ordering():
    """Catalog-brand rows are sorted by catalog_type so re-renders don't shuffle."""
    g = LineageGraph()
    g.add_node(_catalog_table("db.glue1", "AWS_GLUE"))
    g.add_node(_catalog_table("db.uc1", "UNITY_CATALOG"))
    rows = _collect_legend_rows(g)
    labels = _labels(rows)
    glue_idx = labels.index(CATALOG_STYLES["AWS_GLUE"].label)
    uc_idx = labels.index(CATALOG_STYLES["UNITY_CATALOG"].label)
    assert glue_idx < uc_idx, "AWS_GLUE should come before UNITY_CATALOG (alphabetical)"
