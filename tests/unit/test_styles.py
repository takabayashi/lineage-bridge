# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.ui.styles."""

from __future__ import annotations

import base64

from lineage_bridge.models.graph import (
    EdgeType,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.ui.styles import (
    EDGE_COLORS,
    EDGE_DASHES,
    EDGE_TYPE_LABELS,
    NODE_COLORS,
    NODE_ICONS,
    NODE_TYPE_LABELS,
    TOPIC_WITH_SCHEMA_ICON,
    _make_icon_svg,
    _svg_to_data_uri,
    build_edge_vis_props,
    build_node_url,
    build_node_vis_props,
)

# ── _svg_to_data_uri tests ──────────────────────────────────────────


class TestSvgToDataUri:
    def test_format_prefix(self):
        uri = _svg_to_data_uri("<svg></svg>")
        assert uri.startswith("data:image/svg+xml;base64,")

    def test_roundtrip_decode(self):
        svg = '<svg xmlns="http://www.w3.org/2000/svg"><circle/></svg>'
        uri = _svg_to_data_uri(svg)
        encoded_part = uri.split(",", 1)[1]
        decoded = base64.b64decode(encoded_part).decode("utf-8")
        assert decoded == svg


# ── _make_icon_svg tests ────────────────────────────────────────────


class TestMakeIconSvg:
    def test_contains_color(self):
        svg = _make_icon_svg("#FF0000", "<rect/>")
        assert "#FF0000" in svg

    def test_contains_symbol(self):
        symbol = '<path d="M0,0 L10,10"/>'
        svg = _make_icon_svg("#000", symbol)
        assert symbol in svg

    def test_is_valid_svg(self):
        svg = _make_icon_svg("#123456", "")
        assert svg.startswith("<svg")
        assert svg.endswith("</svg>")


# ── NODE_ICONS tests ────────────────────────────────────────────────


class TestNodeIcons:
    def test_covers_all_node_types(self):
        for ntype in NodeType:
            assert ntype in NODE_ICONS, f"Missing icon for {ntype}"

    def test_all_are_data_uris(self):
        for ntype, icon in NODE_ICONS.items():
            assert icon.startswith("data:image/svg+xml;base64,"), (
                f"Icon for {ntype} is not a valid data URI"
            )


# ── build_node_vis_props tests ──────────────────────────────────────


class TestBuildNodeVisProps:
    def test_shape_is_image(self):
        for ntype in NodeType:
            props = build_node_vis_props(ntype)
            assert props["shape"] == "image"

    def test_has_image(self):
        for ntype in NodeType:
            props = build_node_vis_props(ntype)
            assert "image" in props
            assert props["image"].startswith("data:image/svg+xml;base64,")

    def test_has_size(self):
        props = build_node_vis_props(NodeType.KAFKA_TOPIC)
        assert isinstance(props["size"], int)
        assert props["size"] > 0

    def test_has_font_settings(self):
        props = build_node_vis_props(NodeType.CONNECTOR)
        assert "font" in props
        assert "size" in props["font"]

    def test_has_shadow_settings(self):
        props = build_node_vis_props(NodeType.FLINK_JOB)
        assert "shadow" in props
        assert props["shadow"]["enabled"] is True


# ── build_edge_vis_props tests ──────────────────────────────────────


class TestBuildEdgeVisProps:
    def test_has_color(self):
        for etype in EdgeType:
            props = build_edge_vis_props(etype)
            assert "color" in props
            assert "color" in props["color"]
            # Color should match the palette
            assert props["color"]["color"] == EDGE_COLORS[etype]

    def test_dashes_match_palette(self):
        for etype in EdgeType:
            props = build_edge_vis_props(etype)
            assert props["dashes"] == EDGE_DASHES[etype]

    def test_materializes_is_dashed(self):
        props = build_edge_vis_props(EdgeType.MATERIALIZES)
        assert isinstance(props["dashes"], list)

    def test_produces_is_not_dashed(self):
        props = build_edge_vis_props(EdgeType.PRODUCES)
        assert props["dashes"] is False

    def test_has_arrows(self):
        props = build_edge_vis_props(EdgeType.CONSUMES)
        assert props["arrows"]["to"]["enabled"] is True

    def test_has_smooth(self):
        props = build_edge_vis_props(EdgeType.TRANSFORMS)
        assert "smooth" in props


# ── TOPIC_WITH_SCHEMA_ICON tests ────────────────────────────────────


class TestTopicWithSchemaIcon:
    def test_is_valid_data_uri(self):
        assert TOPIC_WITH_SCHEMA_ICON.startswith("data:image/svg+xml;base64,")

    def test_contains_badge(self):
        encoded = TOPIC_WITH_SCHEMA_ICON.split(",", 1)[1]
        svg = base64.b64decode(encoded).decode("utf-8")
        # Badge has a "S" letter for schema
        assert ">S</text>" in svg


# ── Label coverage tests ────────────────────────────────────────────


class TestLabelCoverage:
    def test_node_type_labels_cover_all(self):
        for ntype in NodeType:
            assert ntype in NODE_TYPE_LABELS, f"Missing label for NodeType.{ntype.name}"

    def test_edge_type_labels_cover_all(self):
        for etype in EdgeType:
            assert etype in EDGE_TYPE_LABELS, f"Missing label for EdgeType.{etype.name}"

    def test_node_colors_cover_all(self):
        for ntype in NodeType:
            assert ntype in NODE_COLORS, f"Missing color for NodeType.{ntype.name}"


# ── build_node_url tests ───────────────────────────────────────────


class TestBuildNodeUrl:
    def _make_node(
        self,
        node_type: NodeType,
        system: SystemType = SystemType.CONFLUENT,
        qualified_name: str = "test",
        attributes: dict | None = None,
        environment_id: str | None = "env-123",
        cluster_id: str | None = "lkc-abc",
        catalog_type: str | None = None,
    ) -> LineageNode:
        return LineageNode(
            node_id=f"{system.value}:{node_type.value}:{environment_id or 'none'}:{qualified_name}",
            system=system,
            node_type=node_type,
            catalog_type=catalog_type,
            qualified_name=qualified_name,
            display_name=qualified_name,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes=attributes or {},
        )

    def test_uc_table_with_workspace_url(self):
        node = self._make_node(
            NodeType.CATALOG_TABLE,
            system=SystemType.DATABRICKS,
            catalog_type="UNITY_CATALOG",
            qualified_name="my_catalog.my_schema.my_table",
            attributes={"workspace_url": "https://myworkspace.databricks.com"},
        )
        url = build_node_url(node)
        assert url is not None
        assert "myworkspace.databricks.com" in url
        assert "/explore/data/my_catalog/my_schema/my_table" in url

    def test_uc_table_without_workspace_url(self):
        node = self._make_node(
            NodeType.CATALOG_TABLE,
            system=SystemType.DATABRICKS,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            attributes={},
        )
        url = build_node_url(node)
        assert url is None

    def test_glue_table_with_database_and_table_name(self):
        node = self._make_node(
            NodeType.CATALOG_TABLE,
            system=SystemType.AWS,
            catalog_type="AWS_GLUE",
            qualified_name="glue://mydb/mytable",
            attributes={
                "database": "mydb",
                "table_name": "mytable",
                "aws_region": "us-west-2",
            },
        )
        url = build_node_url(node)
        assert url is not None
        assert "us-west-2.console.aws.amazon.com/glue" in url
        assert "mytable" in url
        assert "database=mydb" in url

    def test_glue_table_missing_attributes(self):
        node = self._make_node(
            NodeType.CATALOG_TABLE,
            system=SystemType.AWS,
            catalog_type="AWS_GLUE",
            qualified_name="glue://unknown",
            attributes={},
        )
        url = build_node_url(node)
        assert url is None

    def test_kafka_topic_delegates_to_confluent(self):
        node = self._make_node(
            NodeType.KAFKA_TOPIC,
            qualified_name="my-topic",
        )
        url = build_node_url(node)
        assert url is not None
        assert "confluent.cloud" in url
        assert "/topics/my-topic/" in url

    def test_unsupported_type_returns_none(self):
        node = self._make_node(
            NodeType.EXTERNAL_DATASET,
            system=SystemType.EXTERNAL,
            qualified_name="ext-dataset",
            environment_id=None,
            cluster_id=None,
        )
        url = build_node_url(node)
        assert url is None
