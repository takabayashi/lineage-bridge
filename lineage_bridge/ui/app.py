# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Streamlit UI for LineageBridge — single-page architecture."""

from __future__ import annotations

import json
import subprocess
import sys
import warnings
from pathlib import Path

import streamlit as st

from lineage_bridge.catalogs import configure_providers
from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.components.visjs_graph import visjs_graph
from lineage_bridge.ui.empty_state import render_empty_state
from lineage_bridge.ui.graph_renderer import (
    _compute_dag_layout,
    render_graph_raw,
)
from lineage_bridge.ui.node_details import render_node_details
from lineage_bridge.ui.sidebar import _render_sidebar
from lineage_bridge.ui.state import ensure_defaults, load_cached_selections

_STATIC_DIR = Path(__file__).parent / "static"

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

# Seed catalog providers with the user's actual workspace URL so deeplinks
# don't fall back to whatever (possibly stale) URL Confluent has stored in
# its Tableflow catalog-integration config.
try:
    _settings = Settings()  # type: ignore[call-arg]
    configure_providers(
        databricks_workspace_url=_settings.databricks_workspace_url,
        databricks_token=_settings.databricks_token,
    )
except Exception:
    # Settings missing is fine for sample-data mode.
    pass

# ── Custom CSS (loaded from ui/static/styles.css) ─────────────────────

st.markdown(
    f"<style>{(_STATIC_DIR / 'styles.css').read_text(encoding='utf-8')}</style>",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN AREA
# ═══════════════════════════════════════════════════════════════════════


def _render_main_area():
    """Main content: tabs for graph and watcher."""
    graph = st.session_state.graph
    if graph is None:
        render_empty_state()
        return

    tab_graph, tab_watcher = st.tabs(["Lineage Graph", "Change Watcher"])

    with tab_graph:
        _render_graph_content(graph)

    with tab_watcher:
        _render_watcher_tab()


def _render_watcher_tab():
    """Render the Change Watcher tab with controls and event log."""
    from lineage_bridge.ui.watcher import render_watcher_controls, render_watcher_log

    render_watcher_controls()
    st.divider()
    render_watcher_log()


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
    cluster_count = len({n.cluster_id for n in graph.nodes if n.cluster_id})
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Nodes", graph.node_count)
    m2.metric("Edges", graph.edge_count)
    m3.metric("Node Types", len({n.node_type for n in graph.nodes}))
    m4.metric("Environments", env_count)
    m5.metric("Clusters", cluster_count)
    m6.metric("Pipelines", graph.pipeline_count)

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
        return

    has_selection = st.session_state.selected_node is not None
    if has_selection:
        graph_col, detail_col = st.columns([3, 2])
    else:
        graph_col = st.container()
        detail_col = None

    with graph_col:
        st.caption(f"Showing {len(vis_nodes)} of {graph.node_count} nodes, {len(vis_edges)} edges")

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
        with detail_col, st.container(border=True):
            render_node_details(graph)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN DISPATCH
# ═══════════════════════════════════════════════════════════════════════

_render_sidebar()
_render_main_area()

# ── Footer ────────────────────────────────────────────────────────────

st.markdown(
    """
    <div class="app-footer">
        <strong>LineageBridge</strong> &copy; 2026 Daniel Takabayashi<br>
        <a href="mailto:dtakabayashi@confluent.io">dtakabayashi@confluent.io</a><br>
        Built with Streamlit &bull; Powered by Confluent Inc.<br>
        <span style="font-size:0.8rem; margin-top:4px; display:inline-block;">
            &#11088; Like this project?
            <a href="https://github.com/takabayashi/lineage-bridge"
               target="_blank">Give it a star on GitHub</a>
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)


# ── CLI entry point ───────────────────────────────────────────────────


def run() -> None:
    """CLI entry point that launches the Streamlit app."""
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__],
        check=True,
    )
