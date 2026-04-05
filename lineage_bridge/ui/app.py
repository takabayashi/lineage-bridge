# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Streamlit UI for LineageBridge — single-page architecture."""

from __future__ import annotations

import json
import subprocess
import sys
import warnings

import streamlit as st

from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.components.visjs_graph import visjs_graph
from lineage_bridge.ui.graph_renderer import (
    _compute_dag_layout,
    render_graph_raw,
)
from lineage_bridge.ui.node_details import render_node_details
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.sidebar import _render_sidebar
from lineage_bridge.ui.state import _GRAPH_VERSION, ensure_defaults, load_cached_selections

# Suppress "coroutine was never awaited" warnings from Streamlit re-runs.
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# ── Page configuration ────────────────────────────────────────────────

st.set_page_config(
    page_title="LineageBridge",
    page_icon="\U0001f310",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state defaults ────────────────────────────────────────────

ensure_defaults()
load_cached_selections()

# ── Custom CSS ────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    /* Tighten top padding */
    .block-container { padding-top: 1.5rem; }

    /* Stats bar on graph view */
    div[data-testid="stMetric"] {
        background: rgba(128,128,128,0.08);
        border: 1px solid rgba(128,128,128,0.2);
        border-radius: 8px;
        padding: 0.6rem 1rem;
    }

    /* Node detail panel — slide in from right */
    @keyframes slideInRight {
        from { opacity: 0; transform: translateX(30px); }
        to   { opacity: 1; transform: translateX(0); }
    }
    div[data-testid="stColumns"] > div:last-child {
        animation: slideInRight 0.3s ease-out;
    }

    /* Legend items */
    .legend-item {
        display: inline-block;
        margin-right: 1rem;
        font-size: 0.85rem;
        white-space: nowrap;
    }

    /* Contain graph iframe — scope to main area only */
    .main iframe { max-width: 100%; }
    .main div[data-testid="stVerticalBlock"] > div:has(iframe) {
        overflow: hidden;
        position: relative;
    }
    header[data-testid="stHeader"] {
        z-index: 999 !important;
    }

    /* Sidebar: ensure scrolling works */
    section[data-testid="stSidebar"] > div:first-child {
        overflow-y: auto !important;
        max-height: 100vh;
    }

    /* Empty state */
    .hero-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 2.5rem 3rem;
        color: #e0e0e0;
        margin-bottom: 1.5rem;
    }
    .hero-card h1 { color: #ffffff; font-size: 2.2rem; margin-bottom: 0.3rem; }
    .hero-card p { color: #b0bec5; font-size: 1.05rem; line-height: 1.6; }

    /* Step tracker */
    .step-tracker {
        display: flex; align-items: center; gap: 0.5rem;
        margin: 1.5rem 0; flex-wrap: wrap;
    }
    .step-item {
        display: flex; align-items: center; gap: 0.4rem;
        padding: 0.5rem 1rem; border-radius: 8px; font-size: 0.9rem;
    }
    .step-done { background: rgba(76,175,80,0.15); color: #4CAF50; }
    .step-current { background: rgba(21,101,192,0.15); color: #42a5f5; font-weight: 600; }
    .step-pending { background: rgba(128,128,128,0.1); color: #9e9e9e; }
    .step-arrow { color: #888; font-size: 1.2rem; }

    /* Extraction time badge */
    .extraction-time {
        font-size: 0.8rem; color: #999;
        background: rgba(128,128,128,0.1); padding: 2px 8px; border-radius: 4px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN AREA
# ═══════════════════════════════════════════════════════════════════════


def _render_main_area():
    """Main content: graph or empty state."""
    graph = st.session_state.graph
    if graph is not None:
        _render_graph_content(graph)
    else:
        _render_empty_state()


def _render_empty_state():
    """Welcome/empty state with step tracker."""
    st.markdown(
        """
        <div class="hero-card">
            <h1>\U0001f310 LineageBridge</h1>
            <p>
                Discover and visualize stream lineage across
                Confluent Cloud &mdash; Kafka topics, connectors,
                Flink jobs, ksqlDB queries, Tableflow, and Unity
                Catalog in one interactive directed graph.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Step tracker
    connected = st.session_state.connected
    has_envs = bool(st.session_state.get("env_multi_select"))

    step1_cls = "step-done" if connected else "step-current"
    step1_icon = "\u2713" if connected else "1"
    step2_cls = "step-done" if has_envs else ("step-current" if connected else "step-pending")
    step2_icon = "\u2713" if has_envs else "2"
    step3_cls = "step-current" if has_envs else "step-pending"

    st.markdown(
        f"""
        <div class="step-tracker">
            <div class="step-item {step1_cls}">
                <strong>{step1_icon}</strong> Connect
            </div>
            <span class="step-arrow">\u2192</span>
            <div class="step-item {step2_cls}">
                <strong>{step2_icon}</strong> Select Infrastructure
            </div>
            <span class="step-arrow">\u2192</span>
            <div class="step-item {step3_cls}">
                <strong>3</strong> Extract
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "Use the **sidebar** to connect, select environments, "
        "and extract lineage. Or load a demo graph to explore."
    )

    _, center, _ = st.columns([1, 1, 1])
    with center:
        if st.button(
            "\u26a1 Load Demo Graph",
            key="hero_demo_btn",
            type="primary",
            use_container_width=True,
        ):
            st.session_state.graph = generate_sample_graph()
            st.session_state.graph_version = _GRAPH_VERSION
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()

    st.markdown("---")

    st.markdown(
        """
**LineageBridge** is a proof-of-concept that discovers and
visualizes stream lineage across Confluent Cloud — connecting
Kafka topics, connectors, Flink jobs, ksqlDB queries,
Tableflow tables, and Unity Catalog into a single interactive graph.

**Created by:** Daniel Takabayashi
**Built with:** Python, Streamlit, networkx, httpx
        """
    )


def _render_graph_content(graph: LineageGraph):
    """Graph visualization with stats and detail panel."""
    # Header row
    header_left, header_right = st.columns([3, 1])
    with header_left:
        title_parts = ["#### \U0001f310 Lineage Graph"]
        if st.session_state.last_extraction_time:
            title_parts.append(
                f"<span class='extraction-time'>"
                f"Last extracted: {st.session_state.last_extraction_time}"
                f"</span>"
            )
        st.markdown(" ".join(title_parts), unsafe_allow_html=True)
    with header_right:
        export_data = json.dumps(graph.to_dict(), indent=2, default=str)
        st.download_button(
            label="\U0001f4e5 Export JSON",
            data=export_data,
            file_name="lineage_graph.json",
            mime="application/json",
            key="export_json_btn",
        )

    # Stats bar
    env_count = len({n.environment_id for n in graph.nodes if n.environment_id})
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Nodes", graph.node_count)
    m2.metric("Edges", graph.edge_count)
    m3.metric("Node Types", len({n.node_type for n in graph.nodes}))
    m4.metric("Environments", env_count)
    m5.metric("Pipelines", graph.pipeline_count)

    # Read filter values from sidebar widgets
    type_filters: dict[NodeType, bool] = {}
    for ntype in NodeType:
        key = f"filter_{ntype.value}"
        type_filters[ntype] = st.session_state.get(key, True)

    search_query = st.session_state.get("search_input", "")
    hops = st.session_state.get("hop_slider", 5)
    hide_disconnected = st.session_state.get("hide_disconnected", True)

    selected_graph_env = st.session_state.get("graph_env_filter")
    if selected_graph_env == "All":
        selected_graph_env = None
    selected_graph_cluster = st.session_state.get("graph_cluster_filter")
    if selected_graph_cluster == "All":
        selected_graph_cluster = None

    # Build filtered graph
    vis_nodes, vis_edges = render_graph_raw(
        graph,
        filters=type_filters,
        search_query=search_query,
        selected_node=st.session_state.focus_node,
        hops=hops,
        environment_filter=selected_graph_env,
        cluster_filter=selected_graph_cluster,
        hide_disconnected=hide_disconnected,
    )

    if not vis_nodes:
        st.warning("No nodes match the current filters. Adjust filters in the sidebar.")
    else:
        # Layout: graph + optional detail panel
        has_selection = st.session_state.selected_node is not None
        if has_selection:
            graph_col, detail_col = st.columns([3, 2])
        else:
            graph_col = st.container()
            detail_col = None

        with graph_col:
            st.caption(
                f"Showing {len(vis_nodes)} of {graph.node_count} nodes, {len(vis_edges)} edges"
            )

            # Compute DAG layout positions (JS may override with saved positions)
            edge_pairs = [(e["from"], e["to"]) for e in vis_edges]
            positions = _compute_dag_layout([n["id"] for n in vis_nodes], edge_pairs)
            for n in vis_nodes:
                pos = positions.get(n["id"])
                if pos:
                    n["x"] = pos["x"]
                    n["y"] = pos["y"]

            clear_positions = st.session_state.pop("_clear_positions", False)
            vis_config = {
                "layout": {"hierarchical": {"enabled": False}},
                "physics": {"enabled": False},
                "edges": {
                    "smooth": {"enabled": False},
                },
                "_clearPositions": clear_positions,
            }

            clicked_node = visjs_graph(
                nodes=vis_nodes,
                edges=vis_edges,
                config=vis_config,
                height=650,
                key="lineage_graph",
            )

            if clicked_node:
                dismissed = st.session_state._dismissed_node
                if clicked_node == dismissed:
                    pass
                elif clicked_node != st.session_state.selected_node:
                    st.session_state.selected_node = clicked_node
                    st.session_state._dismissed_node = None
                    st.rerun()

        # Node detail panel
        if detail_col is not None:
            with detail_col:
                render_node_details(graph)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN DISPATCH
# ═══════════════════════════════════════════════════════════════════════

_render_sidebar()
_render_main_area()


# ── CLI entry point ───────────────────────────────────────────────────


def run() -> None:
    """CLI entry point that launches the Streamlit app."""
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__],
        check=True,
    )
