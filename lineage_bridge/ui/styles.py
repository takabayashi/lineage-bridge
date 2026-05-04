# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Visual styling constants for the lineage graph UI."""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from lineage_bridge.models.graph import EdgeType, NodeType

_ICONS_DIR = Path(__file__).parent / "assets" / "icons"


# ── Per-catalog visual style ───────────────────────────────────────────
# All catalog tables share `NodeType.CATALOG_TABLE` (per ADR-021); each
# `catalog_type` value gets its own color / label / brand icon below.
# A single dict-of-dataclass keeps the four attributes in sync — adding a new
# catalog (e.g. Snowflake) means one entry here, not three or four.


@dataclass(frozen=True)
class CatalogStyle:
    """Visual style for one catalog_type — color, label, optional brand icon."""

    color: str
    label: str
    icon: _IconSpec | None = None  # None = generic database-cylinder fallback


# ── Color palette per node type ────────────────────────────────────────
NODE_COLORS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "#1976D2",  # Confluent blue
    NodeType.CONNECTOR: "#0D47A1",  # Confluent dark blue
    NodeType.KSQLDB_QUERY: "#42A5F5",  # Confluent light blue
    NodeType.FLINK_JOB: "#D32F2F",  # Flink red
    NodeType.TABLEFLOW_TABLE: "#1565C0",  # Confluent medium blue
    NodeType.CATALOG_TABLE: "#F9A825",  # default catalog color (amber)
    NodeType.SCHEMA: "#90CAF9",  # Confluent pale blue
    NodeType.EXTERNAL_DATASET: "#757575",  # neutral gray
    NodeType.CONSUMER_GROUP: "#2196F3",  # Confluent mid blue
    NodeType.NOTEBOOK: "#FF3621",  # Databricks brand red
}

# ── Node sizes (agraph pixel diameter) ─────────────────────────────────
NODE_SIZES: dict[NodeType, int] = {
    NodeType.KAFKA_TOPIC: 40,
    NodeType.CONNECTOR: 38,
    NodeType.KSQLDB_QUERY: 38,
    NodeType.FLINK_JOB: 38,
    NodeType.TABLEFLOW_TABLE: 36,
    NodeType.CATALOG_TABLE: 36,
    NodeType.SCHEMA: 28,
    NodeType.EXTERNAL_DATASET: 34,
    NodeType.CONSUMER_GROUP: 30,
    NodeType.NOTEBOOK: 38,
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
    NodeType.CATALOG_TABLE: "Catalog Table",  # generic; use label_for_node() for per-catalog
    NodeType.SCHEMA: "Schema",
    NodeType.EXTERNAL_DATASET: "External Dataset",
    NodeType.CONSUMER_GROUP: "Consumer Group",
    NodeType.NOTEBOOK: "Notebook",
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
    NodeType.CATALOG_TABLE: "image",
    NodeType.SCHEMA: "image",
    NodeType.EXTERNAL_DATASET: "image",
    NodeType.CONSUMER_GROUP: "image",
    NodeType.NOTEBOOK: "image",
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
    """Build a 64x64 SVG with a colored rounded-square and white symbol."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="64" height="64" viewBox="0 0 64 64">'
        f'<rect x="2" y="2" width="60" height="60" rx="14" '
        f'fill="{color}" stroke="#fff" stroke-width="2"/>'
        f'<g fill="#fff" transform="translate(32,32)">'
        f"{symbol_path}</g></svg>"
    )


# Symbol paths (centered at origin, fit within ~±14px)
_SYMBOLS: dict[NodeType, str] = {
    # Kafka Topic — vertical cylinder (log/stream storage)
    NodeType.KAFKA_TOPIC: (
        '<ellipse cx="0" cy="-10" rx="12" ry="5" fill="#fff"/>'
        '<rect x="-12" y="-10" width="24" height="18" fill="#fff"/>'
        '<ellipse cx="0" cy="8" rx="12" ry="5" fill="#fff"/>'
        '<ellipse cx="0" cy="-10" rx="12" ry="5" fill="none" '
        'stroke="rgba(0,0,0,0.15)" stroke-width="1"/>'
        '<ellipse cx="0" cy="-4" rx="12" ry="4" fill="none" '
        'stroke="rgba(0,0,0,0.1)" stroke-width="0.8"/>'
    ),
    # Connector — bidirectional arrows with center hub (data bridge)
    NodeType.CONNECTOR: (
        '<circle cx="0" cy="0" r="5" fill="#fff"/>'
        # left arrow
        '<rect x="-14" y="-1.5" width="9" height="3" rx="1" fill="#fff"/>'
        '<polygon points="-14,-5 -14,5 -19,0" fill="#fff"/>'
        # right arrow
        '<rect x="5" y="-1.5" width="9" height="3" rx="1" fill="#fff"/>'
        '<polygon points="14,-5 14,5 19,0" fill="#fff"/>'
    ),
    # ksqlDB — terminal prompt with ">" cursor
    NodeType.KSQLDB_QUERY: (
        '<rect x="-14" y="-12" width="28" height="24" rx="3" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
        # terminal top bar
        '<line x1="-14" y1="-6" x2="14" y2="-6" stroke="#fff" stroke-width="1.5"/>'
        '<circle cx="-10" cy="-9" r="1.5" fill="#fff"/>'
        '<circle cx="-5" cy="-9" r="1.5" fill="#fff"/>'
        # prompt chevron >_
        '<polyline points="-8,0 -2,4 -8,8" fill="none" '
        'stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        '<line x1="1" y1="8" x2="9" y2="8" stroke="#fff" stroke-width="2.5" '
        'stroke-linecap="round"/>'
    ),
    # Flink — gear/engine (processing engine)
    NodeType.FLINK_JOB: (
        # outer gear with 6 teeth
        '<path d="M-3,-14 L3,-14 L4,-10 L10,-7 L13,-11 L14,-5 L11,-3 L11,3 '
        "L14,5 L13,11 L10,7 L4,10 L3,14 L-3,14 L-4,10 L-10,7 L-13,11 "
        'L-14,5 L-11,3 L-11,-3 L-14,-5 L-13,-11 L-10,-7 L-4,-10 Z" fill="#fff"/>'
        # inner circle (hub)
        '<circle cx="0" cy="0" r="5" fill="none" stroke="rgba(0,0,0,0.2)" '
        'stroke-width="1.5"/>'
    ),
    # Tableflow — arrow flowing into a table (stream-to-table)
    NodeType.TABLEFLOW_TABLE: (
        # down arrow at top
        '<polygon points="-4,-14 4,-14 4,-6 8,-6 0,0 -8,-6 -4,-6" fill="#fff"/>'
        # table below
        '<rect x="-12" y="2" width="24" height="12" rx="2" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
        '<line x1="-12" y1="7" x2="12" y2="7" stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-2" y1="2" x2="-2" y2="14" stroke="#fff" stroke-width="1.5"/>'
    ),
    # Catalog table — database cylinder (classic DB icon). Used as the
    # fallback when a brand-specific SVG (UC / Glue / BigQuery) isn't loaded;
    # see _OFFICIAL_ICON_FILES_BY_CATALOG.
    NodeType.CATALOG_TABLE: (
        '<ellipse cx="0" cy="-9" rx="13" ry="6" fill="#fff"/>'
        '<rect x="-13" y="-9" width="26" height="16" fill="#fff"/>'
        '<ellipse cx="0" cy="7" rx="13" ry="6" fill="#fff"/>'
        '<ellipse cx="0" cy="-9" rx="13" ry="6" fill="none" '
        'stroke="rgba(0,0,0,0.2)" stroke-width="1"/>'
        '<ellipse cx="0" cy="-2" rx="13" ry="5" fill="none" '
        'stroke="rgba(0,0,0,0.12)" stroke-width="0.8"/>'
    ),
    # Schema — curly braces { } (schema/contract feel)
    NodeType.SCHEMA: (
        '<path d="M-5,-13 Q-10,-13 -10,-8 L-10,-3 Q-10,0 -13,0 '
        'Q-10,0 -10,3 L-10,8 Q-10,13 -5,13" '
        'fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/>'
        '<path d="M5,-13 Q10,-13 10,-8 L10,-3 Q10,0 13,0 '
        'Q10,0 10,3 L10,8 Q10,13 5,13" '
        'fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/>'
        # dots inside
        '<circle cx="-2" cy="-4" r="1.5" fill="#fff"/>'
        '<circle cx="2" cy="0" r="1.5" fill="#fff"/>'
        '<circle cx="-2" cy="4" r="1.5" fill="#fff"/>'
    ),
    # External Dataset — cloud with up/down arrow (external data source)
    NodeType.EXTERNAL_DATASET: (
        '<path d="M-4,2 Q-14,2 -14,-4 Q-14,-10 -8,-10 '
        "Q-6,-15 0,-13 Q4,-17 8,-13 Q14,-13 14,-7 "
        'Q16,-3 12,-1 Q14,2 8,2 Z" '
        'fill="#fff"/>'
        # small down arrow below cloud
        '<polygon points="-4,5 4,5 0,11" fill="#fff"/>'
    ),
    # Notebook — code-cell document (rounded page with a filled header band
    # and three content lines, evoking a Jupyter/Databricks notebook).
    NodeType.NOTEBOOK: (
        '<rect x="-12" y="-14" width="24" height="28" rx="3" '
        'fill="none" stroke="#fff" stroke-width="2"/>'
        # filled header (cell title bar)
        '<rect x="-9" y="-11" width="18" height="5" rx="1" fill="#fff"/>'
        # three content lines
        '<line x1="-9" y1="-1" x2="9" y2="-1" stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-9" y1="4" x2="6" y2="4" stroke="#fff" stroke-width="1.5"/>'
        '<line x1="-9" y1="9" x2="9" y2="9" stroke="#fff" stroke-width="1.5"/>'
    ),
    # Consumer Group — three stacked user circles (group of consumers)
    NodeType.CONSUMER_GROUP: (
        # back person
        '<circle cx="0" cy="-8" r="3" fill="#fff" opacity="0.5"/>'
        '<path d="M-6,0 Q-6,-4 0,-5 Q6,-4 6,0" fill="#fff" opacity="0.5"/>'
        # middle person
        '<circle cx="-5" cy="-4" r="3.5" fill="#fff" opacity="0.75"/>'
        '<path d="M-12,5 Q-12,0 -5,-2 Q2,0 2,5" fill="#fff" opacity="0.75"/>'
        # front person
        '<circle cx="5" cy="-3" r="4" fill="#fff"/>'
        '<path d="M-2,7 Q-2,1 5,-1 Q12,1 12,7" fill="#fff"/>'
    ),
}


# Per-icon spec: (filename, mode, fill_override).
#   mode="logo" → white rounded chip with the artwork nested at 40x40 inside.
#   mode="tile" → artwork fills the whole 64x64 chip with rounded-corner clipping
#                 (use this for icons designed as full coloured tiles, e.g. AWS
#                 Architecture Icons).
#   fill_override → swap the root <svg> fill colour, for recolouring monochrome
#                   brand marks (e.g. Databricks default red → palette amber).
class _IconSpec:
    __slots__ = ("filename", "fill_override", "mode")

    def __init__(
        self,
        filename: str,
        mode: str = "logo",
        fill_override: str | None = None,
    ) -> None:
        self.filename = filename
        self.mode = mode
        self.fill_override = fill_override


def _prepare_official_svg(spec: _IconSpec) -> str:
    """Read, sanitise, and (optionally) recolour an official brand SVG."""
    raw = (_ICONS_DIR / spec.filename).read_text(encoding="utf-8")
    raw = re.sub(r"<\?xml[^>]*\?>", "", raw)
    raw = re.sub(r"<!--.*?-->", "", raw, flags=re.DOTALL)
    raw = re.sub(r"<title>.*?</title>", "", raw, flags=re.DOTALL)
    raw = re.sub(r"<desc>.*?</desc>", "", raw, flags=re.DOTALL)
    if spec.fill_override:
        raw = re.sub(
            r'(<svg\b[^>]*?\s)fill="[^"]*"',
            rf'\1fill="{spec.fill_override}"',
            raw,
            count=1,
        )

    # Strip width/height from the root <svg> so our injected sizing isn't
    # duplicated (duplicate attrs are malformed and break some renderers).
    def _strip_size(match: re.Match[str]) -> str:
        return re.sub(r'\s(?:width|height)="[^"]*"', "", match.group(0))

    return re.sub(r"<svg\b[^>]*>", _strip_size, raw, count=1)


def _load_official_logo_body(spec: _IconSpec, fallback_stroke: str) -> str:
    """Render an official logo on a white rounded chip with brand-coloured outline."""
    raw = _prepare_official_svg(spec)
    root_fill = re.search(r'<svg[^>]*\sfill="(#[0-9A-Fa-f]+)"', raw)
    stroke = spec.fill_override or (root_fill.group(1) if root_fill else fallback_stroke)
    nested = re.sub(
        r"<svg(\s)",
        '<svg x="12" y="12" width="40" height="40" preserveAspectRatio="xMidYMid meet"\\1',
        raw,
        count=1,
    ).strip()
    return (
        '<rect x="2" y="2" width="60" height="60" rx="14" '
        f'fill="#ffffff" stroke="{stroke}" stroke-width="2"/>'
        f"{nested}"
    )


def _load_official_tile_body(spec: _IconSpec) -> str:
    """Render an official full-bleed tile artwork with rounded-corner clipping."""
    raw = _prepare_official_svg(spec)
    nested = re.sub(
        r"<svg(\s)",
        '<svg x="0" y="0" width="64" height="64" preserveAspectRatio="xMidYMid slice"\\1',
        raw,
        count=1,
    ).strip()
    return (
        '<defs><clipPath id="tile-clip">'
        '<rect x="0" y="0" width="64" height="64" rx="14"/>'
        "</clipPath></defs>"
        f'<g clip-path="url(#tile-clip)">{nested}</g>'
    )


def _load_official_chip_body(spec: _IconSpec, fallback_stroke: str) -> str:
    if spec.mode == "tile":
        return _load_official_tile_body(spec)
    return _load_official_logo_body(spec, fallback_stroke)


# Node types whose icons are loaded from official brand SVGs.
_OFFICIAL_ICON_FILES: dict[NodeType, _IconSpec] = {
    NodeType.KAFKA_TOPIC: _IconSpec("apache-kafka.svg"),
    NodeType.FLINK_JOB: _IconSpec("apache-flink.svg"),
}

# Single source of truth per catalog: color, label, optional brand icon.
# Adding a new catalog (Snowflake, Watsonx, ...) = one entry here. Keeps
# the three knobs from drifting out of sync.
CATALOG_STYLES: dict[str, CatalogStyle] = {
    "UNITY_CATALOG": CatalogStyle(
        color="#F9A825",  # Databricks amber/yellow
        label="Unity Catalog Table",
        # Databricks brand red recoloured to palette amber/yellow.
        icon=_IconSpec("databricks.svg", fill_override="#F9A825"),
    ),
    "AWS_GLUE": CatalogStyle(
        color="#E65100",  # AWS orange
        label="AWS Glue Table",
        # AWS Architecture Icon as a full-bleed tile (gradient + white squid).
        icon=_IconSpec("aws-glue.svg", mode="tile"),
    ),
    "GOOGLE_DATA_LINEAGE": CatalogStyle(
        color="#4285F4",  # Google blue
        label="Google BigQuery Table",
        icon=_IconSpec("google-bigquery.svg"),
    ),
    "AWS_DATAZONE": CatalogStyle(
        color="#FF9900",  # AWS DataZone orange
        label="AWS DataZone Asset",
        icon=None,  # generic database cylinder; DataZone has no native graph node today
    ),
}


def _get_chip_body(ntype: NodeType, catalog_type: str | None = None) -> str:
    """Return the inner body SVG for a node icon (no <svg> wrapper)."""
    catalog_style = CATALOG_STYLES.get(catalog_type) if catalog_type else None

    spec = _OFFICIAL_ICON_FILES.get(ntype)
    if spec is None and catalog_style is not None:
        spec = catalog_style.icon
    if spec is not None:
        fallback = (
            catalog_style.color if catalog_style is not None else NODE_COLORS.get(ntype, "#cccccc")
        )
        return _load_official_chip_body(spec, fallback)

    color = catalog_style.color if catalog_style is not None else NODE_COLORS.get(ntype, "#757575")
    symbol = _SYMBOLS.get(ntype, "")
    return (
        f'<rect x="2" y="2" width="60" height="60" rx="14" '
        f'fill="{color}" stroke="#fff" stroke-width="2"/>'
        f'<g fill="#fff" transform="translate(32,32)">{symbol}</g>'
    )


def _wrap_svg(body: str) -> str:
    """Wrap an SVG body fragment in the standard 64x64 root element."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="64" height="64" viewBox="0 0 64 64">'
        f"{body}</svg>"
    )


def _build_node_icons() -> dict[NodeType, str]:
    """Pre-build SVG data URIs for non-catalog node types and the generic catalog default."""
    return {ntype: _svg_to_data_uri(_wrap_svg(_get_chip_body(ntype))) for ntype in NodeType}


NODE_ICONS: dict[NodeType, str] = _build_node_icons()


def _build_catalog_icons() -> dict[str, str]:
    """Pre-build SVG data URIs per catalog_type."""
    return {
        ct: _svg_to_data_uri(_wrap_svg(_get_chip_body(NodeType.CATALOG_TABLE, ct)))
        for ct in CATALOG_STYLES
    }


CATALOG_ICONS: dict[str, str] = _build_catalog_icons()


def _badge_overlay(badge_color: str, badge_text: str) -> str:
    """SVG fragment for a small status badge in the bottom-right corner."""
    return (
        f'<circle cx="52" cy="52" r="11" fill="{badge_color}" '
        'stroke="#fff" stroke-width="2"/>'
        f'<text x="52" y="56" text-anchor="middle" '
        f'font-size="13" font-weight="bold" '
        f'font-family="Inter,system-ui,sans-serif" '
        f'fill="#fff">{badge_text}</text>'
    )


def build_topic_with_schema_icon() -> str:
    """Return a data URI for a Kafka topic icon with a schema badge."""
    body = _get_chip_body(NodeType.KAFKA_TOPIC)
    badge = _badge_overlay(NODE_COLORS[NodeType.SCHEMA], "S")
    return _svg_to_data_uri(_wrap_svg(body + badge))


TOPIC_WITH_SCHEMA_ICON: str = build_topic_with_schema_icon()


def build_dlq_topic_icon() -> str:
    """Return a data URI for a Kafka topic icon with a red 'D' DLQ badge."""
    body = _get_chip_body(NodeType.KAFKA_TOPIC)
    # Red badge — DLQs hold rejected/error records, so a warning hue reads at a glance.
    badge = _badge_overlay("#D32F2F", "D")
    return _svg_to_data_uri(_wrap_svg(body + badge))


DLQ_TOPIC_ICON: str = build_dlq_topic_icon()


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


def render_status_badge_html(status: str | None) -> str:
    """Return inline HTML for a phase/state pill, sourced from STATUS_BADGE_MAP.

    Single source of truth — node_details previously hardcoded
    ``"#4CAF50" if phase == "RUNNING" else "#FF9800"`` six places, which
    drifted from STATUS_BADGE_MAP every time the latter changed.
    """
    if not status:
        return ""
    badge_info = STATUS_BADGE_MAP.get(status, STATUS_BADGE_MAP.get(status.upper()))
    if badge_info is None:
        return (
            f"<span style='display:inline-block;padding:2px 8px;border-radius:10px;"
            f"background:rgba(158,158,158,0.15);color:#757575;font-size:0.78rem;"
            f"font-weight:600;'>{status}</span>"
        )
    color, mark = badge_info
    return (
        f"<span style='display:inline-flex;align-items:center;gap:4px;"
        f"padding:2px 8px;border-radius:10px;background:{color}22;color:{color};"
        f"font-size:0.78rem;font-weight:600;'>"
        f"<span style='font-size:0.78rem'>{mark}</span>{status}"
        f"</span>"
    )


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

    badge_color, badge_text = badge_info
    body = _get_chip_body(ntype)
    badge = _badge_overlay(badge_color, badge_text)
    uri = _svg_to_data_uri(_wrap_svg(body + badge))
    _status_icon_cache[cache_key] = uri
    return uri


# ── Legend emoji/unicode markers ──────────────────────────────────────
NODE_TYPE_EMOJI: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "\u224b",  # ≋ (wave)
    NodeType.CONNECTOR: "\u2693",  # ⚓ (anchor/plug)
    NodeType.KSQLDB_QUERY: "\u2a37",  # ⨷ (query)
    NodeType.FLINK_JOB: "\u26a1",  # ⚡ (bolt)
    NodeType.TABLEFLOW_TABLE: "\u2637",  # ☷ (grid)
    NodeType.CATALOG_TABLE: "\u26c1",  # ⛁ (database)
    NodeType.SCHEMA: "\u2637",  # ☷ (document)
    NodeType.EXTERNAL_DATASET: "\u2601",  # ☁ (cloud)
    NodeType.CONSUMER_GROUP: "\u2638",  # ☸ (group)
    NodeType.NOTEBOOK: "\u270e",  # ✎ (pencil — notebook/code)
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
        return f"{base}/{env}/clusters/{cluster}/topics/{quote(name, safe='')}/overview"

    if ntype == NodeType.CONNECTOR:
        if not cluster:
            return None
        # Confluent Cloud's UI keys connector deeplinks by the lcc-XXXXX
        # resource ID, not the logical name. Fall back to the URL-encoded
        # name for self-managed connectors that don't have an lcc id.
        attrs = getattr(node, "attributes", {}) or {}
        connector_path = attrs.get("confluent_id") or quote(name, safe="")
        return f"{base}/{env}/clusters/{cluster}/connectors/{connector_path}/overview"

    if ntype == NodeType.FLINK_JOB:
        return f"{base}/{env}/flink/compute-pools"

    if ntype == NodeType.KSQLDB_QUERY:
        # ksqlDB has its own cluster id (`lksqlc-XXXXX`) distinct from the
        # Kafka cluster (`lkc-XXXXX`). Confluent Cloud's UI keys ksqlDB
        # URLs by the lksqlc id under `/ksqldb/`, not under the Kafka
        # cluster. Falling back to the Kafka cluster URL produced a 404.
        attrs = getattr(node, "attributes", {}) or {}
        ksqldb_cluster = attrs.get("ksqldb_cluster_id")
        if not ksqldb_cluster:
            return None
        # Deep link to the persistent-queries page for that ksqlDB cluster.
        # (Per-query deeplinks vary by Cloud release — the listing always
        # works and the user lands one click from their query.)
        return (
            f"{base}/{env}/ksqldb/{quote(ksqldb_cluster, safe='')}"
            f"/persistent-queries?query={quote(name, safe='')}"
        )

    if ntype == NodeType.SCHEMA:
        return f"{base}/{env}/schema-registry/schemas"

    if ntype == NodeType.TABLEFLOW_TABLE:
        # Tableflow surfaces under the topic's cloud tab
        if not cluster:
            return None
        # qualified_name is "cluster_id.topic_name", extract the topic part
        topic_name = name.split(".", 1)[-1] if "." in name else name
        return (
            f"{base}/{env}/clusters/{cluster}/topics/"
            f"{quote(topic_name, safe='')}/overview?tab=cloud"
        )

    return None


def _build_external_dataset_url(node: Any) -> str | None:
    """Build a deep link for an EXTERNAL_DATASET node, when its source is recognised."""
    attrs = getattr(node, "attributes", {}) or {}
    connector_class = (attrs.get("connector_class") or "").lower()
    inferred_from = (attrs.get("inferred_from") or "").lower()
    qname = getattr(node, "qualified_name", "") or ""

    is_bigquery = "bigquery" in connector_class or "bigquery" in inferred_from
    if is_bigquery and "." in qname and "://" not in qname:
        project, dataset = qname.split(".", 1)
        return (
            "https://console.cloud.google.com/bigquery"
            f"?project={project}&p={project}&d={dataset}&page=dataset"
        )
    return None


def build_node_url(node: Any) -> str | None:
    """Build a URL for any node type — dispatches to catalog providers for catalog nodes."""
    from lineage_bridge.catalogs import get_provider
    from lineage_bridge.models.graph import NodeType, SystemType

    if node.node_type == NodeType.CATALOG_TABLE:
        catalog_type = getattr(node, "catalog_type", None)
        if not catalog_type:
            return None
        provider = get_provider(catalog_type)
        return provider.build_url(node) if provider else None
    if node.node_type == NodeType.EXTERNAL_DATASET:
        return _build_external_dataset_url(node)
    # Databricks notebook: route through the UC provider so the deeplink
    # uses the workspace URL we know about (and the path-based URL form
    # when we discovered notebook_path during job enrichment).
    if node.node_type == NodeType.NOTEBOOK and node.system == SystemType.DATABRICKS:
        provider = get_provider("UNITY_CATALOG")
        return provider.build_url(node) if provider else None
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


# ── Node-aware accessors ───────────────────────────────────────────────
# These resolve a node's color / label / icon, taking `catalog_type` into
# account so per-catalog distinctions (UC vs Glue vs BigQuery icons) survive
# the ADR-021 NodeType collapse. Callers with only a NodeType in hand can
# keep using the dicts directly — they'll get the catalog-default styling.


def _catalog_style(node: Any) -> CatalogStyle | None:
    """Return the CatalogStyle for a node, or None if it isn't a catalog table."""
    if node.node_type != NodeType.CATALOG_TABLE:
        return None
    ct = getattr(node, "catalog_type", None)
    return CATALOG_STYLES.get(ct) if ct else None


def color_for_node(node: Any) -> str:
    """Resolve a node's color, considering catalog_type for catalog tables."""
    style = _catalog_style(node)
    if style is not None:
        return style.color
    return NODE_COLORS.get(node.node_type, "#757575")


def label_for_node(node: Any) -> str:
    """Resolve a node's human-readable label, with per-catalog override."""
    style = _catalog_style(node)
    if style is not None:
        return style.label
    return NODE_TYPE_LABELS.get(node.node_type, node.node_type.value)


def icon_for_node(node: Any) -> str:
    """Resolve a node's pre-built icon data URI, with per-catalog override."""
    if node.node_type == NodeType.CATALOG_TABLE and getattr(node, "catalog_type", None):
        return CATALOG_ICONS.get(node.catalog_type, NODE_ICONS.get(NodeType.CATALOG_TABLE, ""))
    return NODE_ICONS.get(node.node_type, "")


# Type tags some upstream sources (sample data, certain Confluent APIs) bake
# into display_name. Stripping them at render time keeps the UI clean
# without losing data — the actual type is already conveyed by the node icon
# and the panel header. Patterns are deliberately conservative: they only
# match the known type vocabulary, never arbitrary parens or colons.
_TYPE_PAREN_TAGS: tuple[str, ...] = (
    "(Tableflow)",
    "(ksqlDB)",
    "(Flink)",
    "(UC)",
    "(Unity Catalog)",
    "(Glue)",
    "(BigQuery)",
    "(BQ)",
    "(DataZone)",
    "(DZ)",
)
_TYPE_PREFIX_TAGS: tuple[str, ...] = (
    "UC:",
    "Glue:",
    "BQ:",
    "BigQuery:",
    "DZ:",
    "DataZone:",
    "Tableflow:",
)


def clean_display_name(name: str) -> str:
    """Strip ``(Tableflow)`` / ``UC:`` / ``Glue:`` etc. from a display name.

    The type is already represented visually by the node icon and the panel
    header's type label; baking it into the name itself is redundant and
    pushes the actual identifier off-screen on long names.
    """
    if not name:
        return name
    cleaned = name.strip()
    for suffix in _TYPE_PAREN_TAGS:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)].rstrip()
            break
    for prefix in _TYPE_PREFIX_TAGS:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :].lstrip()
            break
    return cleaned or name  # fall back to original if we stripped to empty
