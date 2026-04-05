# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Visual styling constants for the lineage graph UI."""

from __future__ import annotations

import base64
from typing import Any

from lineage_bridge.models.graph import EdgeType, NodeType

# ── Color palette per node type ────────────────────────────────────────
NODE_COLORS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "#1976D2",  # strong blue
    NodeType.CONNECTOR: "#E65100",  # deep orange
    NodeType.KSQLDB_QUERY: "#7B1FA2",  # deep purple
    NodeType.FLINK_JOB: "#00796B",  # dark teal
    NodeType.TABLEFLOW_TABLE: "#2E7D32",  # dark green
    NodeType.UC_TABLE: "#F57F17",  # dark amber
    NodeType.GLUE_TABLE: "#0D47A1",  # AWS dark blue
    NodeType.SCHEMA: "#546E7A",  # blue-gray
    NodeType.EXTERNAL_DATASET: "#C62828",  # dark red
    NodeType.CONSUMER_GROUP: "#0277BD",  # dark light-blue
}

# ── Node sizes (agraph pixel diameter) ─────────────────────────────────
NODE_SIZES: dict[NodeType, int] = {
    NodeType.KAFKA_TOPIC: 40,
    NodeType.CONNECTOR: 38,
    NodeType.KSQLDB_QUERY: 38,
    NodeType.FLINK_JOB: 38,
    NodeType.TABLEFLOW_TABLE: 36,
    NodeType.UC_TABLE: 36,
    NodeType.GLUE_TABLE: 36,
    NodeType.SCHEMA: 28,
    NodeType.EXTERNAL_DATASET: 34,
    NodeType.CONSUMER_GROUP: 30,
}

# ── Edge colors per type ───────────────────────────────────────────────
EDGE_COLORS: dict[EdgeType, str] = {
    EdgeType.PRODUCES: "#388E3C",  # green
    EdgeType.CONSUMES: "#1976D2",  # blue
    EdgeType.TRANSFORMS: "#7B1FA2",  # purple
    EdgeType.MATERIALIZES: "#E65100",  # orange
    EdgeType.HAS_SCHEMA: "#90A4AE",  # light blue-gray
    EdgeType.MEMBER_OF: "#78909C",  # blue-gray
}

# ── Edge dash patterns (vis.js dashes) ─────────────────────────────────
EDGE_DASHES: dict[EdgeType, bool | list[int]] = {
    EdgeType.PRODUCES: False,
    EdgeType.CONSUMES: False,
    EdgeType.TRANSFORMS: False,
    EdgeType.MATERIALIZES: [8, 4],  # dashed
    EdgeType.HAS_SCHEMA: [4, 4],  # dotted
    EdgeType.MEMBER_OF: [4, 4],  # dotted
}

# ── Edge widths ────────────────────────────────────────────────────────
EDGE_WIDTHS: dict[EdgeType, int] = {
    EdgeType.PRODUCES: 2,
    EdgeType.CONSUMES: 2,
    EdgeType.TRANSFORMS: 3,
    EdgeType.MATERIALIZES: 2,
    EdgeType.HAS_SCHEMA: 1,
    EdgeType.MEMBER_OF: 1,
}

# ── Human-readable labels ─────────────────────────────────────────────
NODE_TYPE_LABELS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "Kafka Topic",
    NodeType.CONNECTOR: "Connector",
    NodeType.KSQLDB_QUERY: "ksqlDB Query",
    NodeType.FLINK_JOB: "Flink Job",
    NodeType.TABLEFLOW_TABLE: "Tableflow Table",
    NodeType.UC_TABLE: "Unity Catalog Table",
    NodeType.GLUE_TABLE: "AWS Glue Table",
    NodeType.SCHEMA: "Schema",
    NodeType.EXTERNAL_DATASET: "External Dataset",
    NodeType.CONSUMER_GROUP: "Consumer Group",
}

EDGE_TYPE_LABELS: dict[EdgeType, str] = {
    EdgeType.PRODUCES: "produces",
    EdgeType.CONSUMES: "consumes",
    EdgeType.TRANSFORMS: "transforms",
    EdgeType.MATERIALIZES: "materializes",
    EdgeType.HAS_SCHEMA: "has schema",
    EdgeType.MEMBER_OF: "member of",
}

# ── Node shape mapping (vis.js / agraph shapes) ───────────────────────
NODE_SHAPES: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "image",
    NodeType.CONNECTOR: "image",
    NodeType.KSQLDB_QUERY: "image",
    NodeType.FLINK_JOB: "image",
    NodeType.TABLEFLOW_TABLE: "image",
    NodeType.UC_TABLE: "image",
    NodeType.GLUE_TABLE: "image",
    NodeType.SCHEMA: "image",
    NodeType.EXTERNAL_DATASET: "image",
    NodeType.CONSUMER_GROUP: "image",
}

# ── SVG icons for each node type ──────────────────────────────────────
# Each icon is an SVG rendered as a data URI for vis.js image nodes.
# Icons are designed at 64x64 with a colored circle background and
# a white symbol representing the resource type.


def _svg_to_data_uri(svg: str) -> str:
    """Convert an SVG string to a base64 data URI."""
    encoded = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


def _make_icon_svg(color: str, symbol_path: str) -> str:
    """Build a 64x64 SVG with a colored circle and white symbol."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="64" height="64" viewBox="0 0 64 64">'
        f'<circle cx="32" cy="32" r="30" fill="{color}" '
        'stroke="#fff" stroke-width="2"/>'
        f'<g fill="#fff" transform="translate(32,32)">'
        f"{symbol_path}</g></svg>"
    )


# Symbol paths (centered at origin, fit within ~±14px)
_SYMBOLS: dict[NodeType, str] = {
    # Kafka Topic — stream/wave lines
    NodeType.KAFKA_TOPIC: (
        '<path d="M-12,-4 Q-6,-12 0,-4 Q6,4 12,-4" '
        'fill="none" stroke="#fff" stroke-width="2.5"/>'
        '<path d="M-12,4 Q-6,-4 0,4 Q6,12 12,4" '
        'fill="none" stroke="#fff" stroke-width="2.5"/>'
    ),
    # Connector — plug icon
    NodeType.CONNECTOR: (
        '<rect x="-4" y="-12" width="8" height="10" rx="1"/>'
        '<rect x="-8" y="-2" width="16" height="6" rx="2"/>'
        '<rect x="-3" y="4" width="6" height="8" rx="1"/>'
    ),
    # ksqlDB — SQL text
    NodeType.KSQLDB_QUERY: (
        '<text x="0" y="5" text-anchor="middle" '
        'font-size="16" font-weight="bold" '
        'font-family="monospace" fill="#fff">SQL</text>'
    ),
    # Flink — bolt/lightning
    NodeType.FLINK_JOB: ('<polygon points="2,-14 -8,2 -1,2 -4,14 8,-2 1,-2"/>'),
    # Tableflow — table grid
    NodeType.TABLEFLOW_TABLE: (
        '<rect x="-12" y="-10" width="24" height="20" '
        'rx="2" fill="none" stroke="#fff" stroke-width="2"/>'
        '<line x1="-12" y1="-3" x2="12" y2="-3" '
        'stroke="#fff" stroke-width="2"/>'
        '<line x1="-12" y1="4" x2="12" y2="4" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<line x1="0" y1="-3" x2="0" y2="10" '
        'stroke="#fff" stroke-width="1.5"/>'
    ),
    # UC Table — database cylinder
    NodeType.UC_TABLE: (
        '<ellipse cx="0" cy="-8" rx="11" ry="5" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
        '<path d="M-11,-8 L-11,6 Q-11,12 0,12 '
        'Q11,12 11,6 L11,-8" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
    ),
    # Glue Table — table grid with bottom bar
    NodeType.GLUE_TABLE: (
        '<rect x="-12" y="-10" width="24" height="20" '
        'rx="2" fill="none" stroke="#fff" stroke-width="2"/>'
        '<line x1="-12" y1="-3" x2="12" y2="-3" '
        'stroke="#fff" stroke-width="2"/>'
        '<line x1="-12" y1="4" x2="12" y2="4" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-4" y1="-3" x2="-4" y2="10" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<line x1="4" y1="-3" x2="4" y2="10" '
        'stroke="#fff" stroke-width="1.5"/>'
    ),
    # Schema — document with lines
    NodeType.SCHEMA: (
        '<path d="M-7,-12 L5,-12 L10,-7 L10,12 '
        'L-7,12 Z" fill="none" stroke="#fff" '
        'stroke-width="2"/>'
        '<line x1="-3" y1="-3" x2="6" y2="-3" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-3" y1="2" x2="6" y2="2" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-3" y1="7" x2="4" y2="7" '
        'stroke="#fff" stroke-width="1.5"/>'
    ),
    # External Dataset — cloud
    NodeType.EXTERNAL_DATASET: (
        '<path d="M-6,4 Q-14,4 -14,-2 Q-14,-8 -8,-8 '
        "Q-6,-14 0,-12 Q4,-16 8,-12 Q14,-12 14,-6 "
        'Q16,-2 12,0 Q14,4 8,4 Z" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
    ),
    # Consumer Group — two people
    NodeType.CONSUMER_GROUP: (
        '<circle cx="-5" cy="-6" r="4" fill="none" '
        'stroke="#fff" stroke-width="2"/>'
        '<path d="M-12,8 Q-12,0 -5,-1 Q2,0 2,8" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
        '<circle cx="6" cy="-7" r="3.5" fill="none" '
        'stroke="#fff" stroke-width="1.5"/>'
        '<path d="M0,7 Q0,0 6,-2 Q12,0 12,7" '
        'fill="none" stroke="#fff" stroke-width="1.5"/>'
    ),
}


def _build_node_icons() -> dict[NodeType, str]:
    """Pre-build SVG data URIs for all node types."""
    icons: dict[NodeType, str] = {}
    for ntype in NodeType:
        color = NODE_COLORS.get(ntype, "#757575")
        symbol = _SYMBOLS.get(ntype, "")
        svg = _make_icon_svg(color, symbol)
        icons[ntype] = _svg_to_data_uri(svg)
    return icons


NODE_ICONS: dict[NodeType, str] = _build_node_icons()


def _make_icon_with_badge(
    color: str,
    symbol_path: str,
    badge_color: str,
    badge_text: str = "S",
) -> str:
    """Build a 64x64 SVG with a small badge in the bottom-right corner."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="64" height="64" viewBox="0 0 64 64">'
        f'<circle cx="32" cy="32" r="30" fill="{color}" '
        'stroke="#fff" stroke-width="2"/>'
        f'<g fill="#fff" transform="translate(32,32)">'
        f"{symbol_path}</g>"
        # Badge circle in bottom-right
        f'<circle cx="52" cy="52" r="11" fill="{badge_color}" '
        'stroke="#fff" stroke-width="2"/>'
        f'<text x="52" y="56" text-anchor="middle" '
        f'font-size="13" font-weight="bold" '
        f'font-family="Inter,system-ui,sans-serif" '
        f'fill="#fff">{badge_text}</text>'
        "</svg>"
    )


def build_topic_with_schema_icon() -> str:
    """Return a data URI for a Kafka topic icon with a schema badge."""
    color = NODE_COLORS[NodeType.KAFKA_TOPIC]
    symbol = _SYMBOLS[NodeType.KAFKA_TOPIC]
    badge_color = NODE_COLORS[NodeType.SCHEMA]
    svg = _make_icon_with_badge(color, symbol, badge_color)
    return _svg_to_data_uri(svg)


TOPIC_WITH_SCHEMA_ICON: str = build_topic_with_schema_icon()


# ── Status badge colors & labels ─────────────────────────────────────
# Maps status/phase/state values to (badge_color, badge_letter).
STATUS_BADGE_MAP: dict[str, tuple[str, str]] = {
    # Running / Active states — green
    "RUNNING": ("#4CAF50", "\u25b6"),  # ▶
    "ACTIVE": ("#4CAF50", "\u25b6"),  # ▶
    "STABLE": ("#4CAF50", "\u25b6"),  # ▶
    "Stable": ("#4CAF50", "\u25b6"),  # ▶
    # Completed — blue
    "COMPLETED": ("#1976D2", "\u2713"),  # ✓
    # Paused / Degraded — amber
    "PAUSED": ("#FF9800", "\u23f8"),  # ⏸ (approx)
    "DEGRADED": ("#FF9800", "!"),
    "REBALANCING": ("#FF9800", "~"),
    # Failed / Error — red
    "FAILED": ("#F44336", "\u2717"),  # ✗
    "ERROR": ("#F44336", "\u2717"),  # ✗
    "STOPPED": ("#F44336", "\u25a0"),  # ■
    # Suspended (tableflow) — red
    "SUSPENDED": ("#F44336", "\u25a0"),  # ■
    # Unknown / other — gray
    "UNKNOWN": ("#9E9E9E", "?"),
}


# Cache for status-badged icons: (node_type, status) -> data URI
_status_icon_cache: dict[tuple[NodeType, str], str] = {}


def build_status_badge_icon(ntype: NodeType, status: str) -> str | None:
    """Return a data URI for a node icon with a status badge.

    Returns None if the status is not in STATUS_BADGE_MAP.
    """
    status_upper = status.upper() if status else ""
    badge_info = STATUS_BADGE_MAP.get(status, STATUS_BADGE_MAP.get(status_upper))
    if not badge_info:
        return None

    cache_key = (ntype, status_upper)
    if cache_key in _status_icon_cache:
        return _status_icon_cache[cache_key]

    color = NODE_COLORS.get(ntype, "#757575")
    symbol = _SYMBOLS.get(ntype, "")
    badge_color, badge_text = badge_info
    svg = _make_icon_with_badge(color, symbol, badge_color, badge_text)
    uri = _svg_to_data_uri(svg)
    _status_icon_cache[cache_key] = uri
    return uri


# ── Legend emoji/unicode markers ──────────────────────────────────────
NODE_TYPE_EMOJI: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "\u224b",  # ≋ (wave)
    NodeType.CONNECTOR: "\u2693",  # ⚓ (anchor/plug)
    NodeType.KSQLDB_QUERY: "\u2a37",  # ⨷ (query)
    NodeType.FLINK_JOB: "\u26a1",  # ⚡ (bolt)
    NodeType.TABLEFLOW_TABLE: "\u2637",  # ☷ (grid)
    NodeType.UC_TABLE: "\u26c1",  # ⛁ (database)
    NodeType.GLUE_TABLE: "\u26c1",  # ⛁ (database)
    NodeType.SCHEMA: "\u2637",  # ☷ (document)
    NodeType.EXTERNAL_DATASET: "\u2601",  # ☁ (cloud)
    NodeType.CONSUMER_GROUP: "\u2638",  # ☸ (group)
}


def build_node_vis_props(ntype: NodeType) -> dict[str, Any]:
    """Return vis.js node properties for a given node type."""
    return {
        "size": NODE_SIZES.get(ntype, 30),
        "shape": "image",
        "image": NODE_ICONS.get(ntype, ""),
        "borderWidth": 0,
        "font": {
            "size": 11,
            "color": "#333333",
            "face": "Inter, system-ui, sans-serif",
            "strokeWidth": 3,
            "strokeColor": "#ffffff",
        },
        "shadow": {
            "enabled": True,
            "color": "rgba(0,0,0,0.15)",
            "size": 6,
            "x": 1,
            "y": 2,
        },
    }


def build_confluent_cloud_url(node: Any) -> str | None:
    """Build a Confluent Cloud console URL for the given node.

    Returns None if the node type is not supported or required IDs are missing.
    """
    from lineage_bridge.models.graph import NodeType

    base = "https://confluent.cloud/environments"
    env = node.environment_id
    cluster = node.cluster_id
    name = node.qualified_name

    if not env:
        return None

    ntype = node.node_type

    if ntype == NodeType.KAFKA_TOPIC:
        if not cluster:
            return None
        return f"{base}/{env}/clusters/{cluster}/topics/{name}/overview"

    if ntype == NodeType.CONNECTOR:
        if not cluster:
            return None
        return f"{base}/{env}/clusters/{cluster}/connectors/{name}/overview"

    if ntype == NodeType.FLINK_JOB:
        return f"{base}/{env}/flink/compute-pools"

    if ntype == NodeType.KSQLDB_QUERY:
        if not cluster:
            return None
        return f"{base}/{env}/clusters/{cluster}/ksqldb"

    if ntype == NodeType.SCHEMA:
        return f"{base}/{env}/schema-registry/schemas"

    if ntype == NodeType.TABLEFLOW_TABLE:
        # Tableflow surfaces under the topic's cloud tab
        if not cluster:
            return None
        # qualified_name is "cluster_id.topic_name", extract the topic part
        topic_name = name.split(".", 1)[-1] if "." in name else name
        return f"{base}/{env}/clusters/{cluster}/topics/{topic_name}/overview?tab=cloud"

    return None


def build_node_url(node: Any) -> str | None:
    """Build a URL for any node type — dispatches to catalog providers for catalog nodes."""
    from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
    from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
    from lineage_bridge.models.graph import NodeType

    if node.node_type == NodeType.UC_TABLE:
        return DatabricksUCProvider().build_url(node)
    if node.node_type == NodeType.GLUE_TABLE:
        return GlueCatalogProvider().build_url(node)
    return build_confluent_cloud_url(node)


def build_edge_vis_props(etype: EdgeType) -> dict[str, Any]:
    """Return vis.js edge properties for a given edge type."""
    dashes = EDGE_DASHES.get(etype, False)
    return {
        "color": {
            "color": EDGE_COLORS.get(etype, "#757575"),
            "highlight": EDGE_COLORS.get(etype, "#757575"),
            "opacity": 0.8,
        },
        "width": EDGE_WIDTHS.get(etype, 2),
        "dashes": dashes,
        "arrows": {"to": {"enabled": True, "scaleFactor": 0.7}},
        "smooth": {"type": "curvedCW", "roundness": 0.15},
        "font": {
            "size": 9,
            "color": "#888888",
            "strokeWidth": 2,
            "strokeColor": "#ffffff",
            "align": "top",
        },
    }
