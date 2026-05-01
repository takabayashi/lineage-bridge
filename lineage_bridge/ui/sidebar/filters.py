# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — graph filters, legend, and per-type counts."""

from __future__ import annotations

from collections import Counter

import streamlit as st

from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.styles import (
    EDGE_COLORS,
    EDGE_DASHES,
    EDGE_TYPE_LABELS,
    NODE_ICONS,
    NODE_TYPE_LABELS,
)


def _get_type_counts(graph: LineageGraph) -> dict[NodeType, int]:
    """Count nodes per NodeType in a single pass."""
    return Counter(n.node_type for n in graph.nodes)


def _render_sidebar_graph_filters(graph: LineageGraph) -> None:
    """Graph filters organized into logical groups."""
    # ── Search ───────────────────────────────────────────────────
    st.text_input(
        "Search nodes",
        placeholder="Type to filter by name...",
        key="search_input",
        label_visibility="collapsed",
    )

    # ── Scope: Environment & Cluster ─────────────────────────────
    env_map: dict[str, str] = {}
    for n in graph.nodes:
        if n.environment_id and n.environment_id not in env_map.values():
            label = (
                f"{n.environment_name} ({n.environment_id})"
                if n.environment_name
                else n.environment_id
            )
            env_map[label] = n.environment_id

    cluster_map: dict[str, str] = {}
    for n in graph.nodes:
        if n.cluster_id and n.cluster_id not in cluster_map.values():
            label = f"{n.cluster_name} ({n.cluster_id})" if n.cluster_name else n.cluster_id
            cluster_map[label] = n.cluster_id

    if len(env_map) > 1 or len(cluster_map) > 1:
        if len(env_map) > 1:
            env_options = ["All", *sorted(env_map.keys())]
            env_sel = st.selectbox(
                "Environment",
                env_options,
                key="graph_env_filter_display",
            )
            st.session_state["graph_env_filter"] = (
                env_map.get(env_sel) if env_sel != "All" else "All"
            )
        if len(cluster_map) > 1:
            cluster_options = ["All", *sorted(cluster_map.keys())]
            cluster_sel = st.selectbox(
                "Cluster",
                cluster_options,
                key="graph_cluster_filter_display",
            )
            st.session_state["graph_cluster_filter"] = (
                cluster_map.get(cluster_sel) if cluster_sel != "All" else "All"
            )

    # ── Node type filters (compact two-column) ───────────────────
    st.caption("Node Types")
    type_counts = _get_type_counts(graph)
    type_items = [
        (ntype, NODE_TYPE_LABELS.get(ntype, ntype.value), type_counts.get(ntype, 0))
        for ntype in NodeType
    ]
    visible_types = [(nt, lbl, cnt) for nt, lbl, cnt in type_items if cnt > 0]

    # Render in two-column pairs
    for i in range(0, len(visible_types), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < len(visible_types):
                nt, lbl, cnt = visible_types[idx]
                with col:
                    st.checkbox(
                        f"{lbl} ({cnt})",
                        value=True,
                        key=f"filter_{nt.value}",
                    )

    # ── Display options ──────────────────────────────────────────
    st.checkbox(
        "Hide disconnected nodes",
        value=True,
        key="hide_disconnected",
        help="Hide nodes that have no edges.",
    )

    # ── Focus / hop controls ─────────────────────────────────────
    focus_active = st.session_state.focus_node is not None
    has_search = bool(st.session_state.get("search_input", "").strip())
    if focus_active or has_search:
        st.slider(
            "Neighborhood hops",
            min_value=1,
            max_value=100,
            value=5,
            key="hop_slider",
            help="How many hops from the focused/searched node to show.",
        )


def _render_sidebar_legend(graph: LineageGraph) -> None:
    """Compact legend with node types and edge types."""
    # ── Node types (two-column grid) ─────────────────────────────
    st.caption("Nodes")
    type_counts = _get_type_counts(graph)
    node_html_parts = []
    for ntype in NodeType:
        if type_counts.get(ntype, 0) == 0:
            continue
        icon_uri = NODE_ICONS.get(ntype, "")
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        node_html_parts.append(
            f"<div class='legend-entry'>"
            f"<img src='{icon_uri}' width='16' height='16' "
            f"style='vertical-align:middle;'/>"
            f"{label}"
            f"</div>"
        )
    if node_html_parts:
        st.markdown(
            f"<div class='legend-grid'>{''.join(node_html_parts)}</div>",
            unsafe_allow_html=True,
        )

    # ── Edge types ───────────────────────────────────────────────
    st.caption("Edges")
    edge_html_parts = []
    for etype in EDGE_TYPE_LABELS:
        color = EDGE_COLORS.get(etype, "#757575")
        label = EDGE_TYPE_LABELS[etype]
        dashes = EDGE_DASHES.get(etype, False)
        if dashes:
            swatch = f"<span class='edge-swatch-dashed' style='border-color:{color}'></span>"
        else:
            swatch = f"<span class='edge-swatch' style='background:{color}'></span>"
        edge_html_parts.append(f"<div class='edge-legend-entry'>{swatch}{label}</div>")
    if edge_html_parts:
        st.markdown(
            f"<div class='legend-grid'>{''.join(edge_html_parts)}</div>",
            unsafe_allow_html=True,
        )
