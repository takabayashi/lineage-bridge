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
    /* Make Streamlit header transparent so it doesn't cover content,
       but keep it in the DOM so the sidebar open button remains visible */
    header[data-testid="stHeader"] {
        background: transparent !important;
        pointer-events: none !important;
    }
    header[data-testid="stHeader"] * {
        pointer-events: auto;
    }

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

    /* Sidebar: ensure scrolling works */
    section[data-testid="stSidebar"] > div:first-child {
        overflow-y: auto !important;
        max-height: 100vh;
    }

    /* Empty state — hero */
    .hero-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0d2137 100%);
        border-radius: 12px;
        padding: 2.5rem 3rem;
        color: #e0e0e0;
        margin-bottom: 1.5rem;
    }
    .hero-card h1 { color: #ffffff; font-size: 2.2rem; margin-bottom: 0.3rem; }
    .hero-card .hero-subtitle {
        color: #64B5F6; font-size: 1rem; font-weight: 500;
        margin-bottom: 0.8rem;
    }
    .hero-card p { color: #b0bec5; font-size: 1.05rem; line-height: 1.6; }

    /* Feature cards */
    .feature-grid {
        display: grid; grid-template-columns: 1fr 1fr;
        gap: 12px; margin: 1rem 0;
    }
    .feature-card {
        background: rgba(128,128,128,0.06);
        border: 1px solid rgba(128,128,128,0.12);
        border-radius: 10px; padding: 16px 18px;
    }
    .feature-card .fc-icon {
        font-size: 1.5rem; margin-bottom: 6px;
    }
    .feature-card .fc-title {
        font-size: 0.95rem; font-weight: 600;
        margin-bottom: 4px;
    }
    .feature-card .fc-desc {
        font-size: 0.82rem; color: #999; line-height: 1.5;
    }

    /* Architecture flow */
    .arch-flow {
        display: flex; align-items: center; justify-content: center;
        gap: 6px; flex-wrap: wrap;
        padding: 16px; margin: 1rem 0;
        background: rgba(128,128,128,0.04);
        border: 1px solid rgba(128,128,128,0.1);
        border-radius: 10px;
    }
    .arch-node {
        padding: 6px 14px; border-radius: 6px;
        font-size: 0.82rem; font-weight: 500;
        white-space: nowrap;
    }
    .arch-arrow { color: #888; font-size: 1.1rem; }

    /* POC badge */
    .poc-badge {
        display: inline-block;
        background: rgba(255,152,0,0.15); color: #E65100;
        font-size: 0.7rem; font-weight: 700;
        text-transform: uppercase; letter-spacing: 1px;
        padding: 2px 8px; border-radius: 4px;
        margin-left: 8px; vertical-align: middle;
    }

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

    /* Extraction log entries */
    .log-entry {
        display: flex; align-items: flex-start; gap: 6px;
        padding: 4px 8px; margin: 2px 0; border-radius: 4px;
        font-size: 0.8rem; line-height: 1.4;
    }
    .log-phase { background: rgba(33,150,243,0.08); color: #1976D2; }
    .log-discovery { background: rgba(76,175,80,0.08); color: #388E3C; }
    .log-warning { background: rgba(255,152,0,0.1); color: #E65100; }
    .log-skip { background: rgba(158,158,158,0.1); color: #757575; }
    .log-provision { background: rgba(156,39,176,0.08); color: #7B1FA2; }
    .log-icon { flex-shrink: 0; font-size: 0.85rem; }
    .log-text { flex: 1; }
    .log-label { font-weight: 600; margin-right: 4px; }

    /* Compact legend grid */
    .legend-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 4px 8px;
    }
    .legend-entry {
        display: flex; align-items: center; gap: 5px;
        font-size: 0.8rem; white-space: nowrap;
        padding: 2px 0;
    }
    .edge-legend-entry {
        display: flex; align-items: center; gap: 6px;
        font-size: 0.8rem; padding: 2px 0;
    }
    .edge-swatch {
        width: 24px; height: 3px; border-radius: 2px; flex-shrink: 0;
    }
    .edge-swatch-dashed {
        width: 24px; height: 0; flex-shrink: 0;
        border-top: 3px dashed;
    }

    /* Status badge — full-width, fixed height, centered */
    .status-badge {
        display: flex; align-items: center; justify-content: center;
        gap: 6px; width: 100%; box-sizing: border-box;
        padding: 6px 12px; border-radius: 6px;
        font-size: 0.82rem; font-weight: 500;
        line-height: 1.3;
    }
    .status-connected {
        background: rgba(76,175,80,0.12); color: #2E7D32;
        border: 1px solid rgba(76,175,80,0.2);
    }
    .status-disconnected {
        background: rgba(158,158,158,0.12); color: #757575;
        border: 1px solid rgba(158,158,158,0.15);
    }
    .status-dot {
        width: 8px; height: 8px; border-radius: 50%;
        flex-shrink: 0;
    }

    /* Sidebar section headers */
    .sidebar-section {
        font-size: 0.7rem; font-weight: 700;
        text-transform: uppercase; letter-spacing: 1.2px;
        color: #888; padding: 8px 0 4px 2px;
        margin-top: 4px;
    }
    .sidebar-divider {
        border: none; border-top: 1px solid rgba(128,128,128,0.15);
        margin: 6px 0 2px 0;
    }

    /* Footer */
    .app-footer {
        text-align: center; padding: 1.5rem 1rem 1rem;
        border-top: 1px solid rgba(128,128,128,0.15);
        margin-top: 2rem; font-size: 0.75rem;
        color: #999; line-height: 1.6;
    }
    .app-footer a {
        color: #1976D2; text-decoration: none;
    }
    .app-footer a:hover { text-decoration: underline; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN AREA
# ═══════════════════════════════════════════════════════════════════════


def _render_main_area():
    """Main content: tabs for graph and watcher."""
    graph = st.session_state.graph
    if graph is None:
        _render_empty_state()
        return

    tab_graph, tab_watcher = st.tabs(["Lineage Graph", "Change Watcher"])

    with tab_graph:
        _render_graph_content(graph)

    with tab_watcher:
        _render_watcher_tab()


def _render_empty_state():
    """Welcome/empty state with project overview."""
    # ── Hero ──────────────────────────────────────────────────────
    st.markdown(
        """
        <div class="hero-card">
            <h1>\U0001f310 LineageBridge <span class="poc-badge">POC</span></h1>
            <div class="hero-subtitle">
                Stream Lineage Discovery &amp; Visualization
            </div>
            <p>
                LineageBridge automatically discovers how data flows through
                your Confluent Cloud infrastructure and renders it as an
                interactive directed graph &mdash; giving you end-to-end
                visibility from Kafka topics through transformation layers
                all the way to external data catalogs.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── What it does ──────────────────────────────────────────────
    st.markdown("#### What does LineageBridge do?")
    st.markdown(
        """
        <div class="feature-grid">
            <div class="feature-card">
                <div class="fc-icon">\U0001f50d</div>
                <div class="fc-title">Auto-Discovery</div>
                <div class="fc-desc">
                    Connects to Confluent Cloud APIs to automatically inventory
                    topics, connectors, Flink jobs, ksqlDB queries, and Tableflow
                    tables &mdash; no manual mapping required.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f4ca</div>
                <div class="fc-title">Lineage Visualization</div>
                <div class="fc-desc">
                    Renders a DAG-based interactive graph showing how data
                    flows between resources, with filtering by type,
                    environment, cluster, and search.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f517</div>
                <div class="fc-title">Catalog Integration</div>
                <div class="fc-desc">
                    Extends lineage beyond Confluent Cloud into Databricks
                    Unity Catalog and AWS Glue via Tableflow, bridging
                    streaming and lakehouse worlds.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f4dd</div>
                <div class="fc-title">Metadata Enrichment</div>
                <div class="fc-desc">
                    Enriches nodes with schemas, tags, business metadata,
                    live throughput metrics, and deep links back to the
                    Confluent Cloud and Databricks consoles.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── How it works ─────────────────────────────────────────────
    st.markdown("#### How it works")
    st.markdown(
        """
        <div class="arch-flow" style="flex-direction:column; align-items:stretch; gap:0;">
            <div style="display:flex; align-items:center; justify-content:center;
                        gap:6px; flex-wrap:wrap; padding:8px 0;">
                <span class="arch-node" style="background:rgba(25,118,210,0.12);
                    color:#1976D2;">Confluent Cloud APIs</span>
                <span class="arch-arrow">\u2192</span>
                <span class="arch-node" style="background:rgba(25,118,210,0.08);
                    color:#1565C0;">Async Clients</span>
                <span class="arch-arrow">\u2192</span>
                <span class="arch-node" style="background:rgba(123,31,162,0.1);
                    color:#7B1FA2;">Orchestrator</span>
                <span class="arch-arrow">\u2192</span>
                <span class="arch-node" style="background:rgba(56,142,60,0.1);
                    color:#388E3C;">Lineage Graph</span>
                <span class="arch-arrow">\u2192</span>
                <span class="arch-node" style="background:rgba(249,168,37,0.12);
                    color:#F57F17;">Interactive UI</span>
            </div>
            <hr style="border:none; border-top:1px solid rgba(128,128,128,0.12);
                       margin:4px 0;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;
                        padding:10px 4px 6px;">
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 1</span>
                    <span style="color:#999;">&mdash;</span>
                    Kafka topics &amp; consumer groups
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 2</span>
                    <span style="color:#999;">&mdash;</span>
                    Connectors, ksqlDB &amp; Flink edges
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 3</span>
                    <span style="color:#999;">&mdash;</span>
                    Schemas, tags &amp; metadata enrichment
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 4</span>
                    <span style="color:#999;">&mdash;</span>
                    Tableflow &amp; catalog mapping
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "All phases run **asynchronously** for maximum throughput. "
        "Catalog providers (Databricks Unity Catalog, AWS Glue) extend "
        "lineage beyond Confluent Cloud, bridging streaming and lakehouse worlds."
    )

    # ── Step tracker ──────────────────────────────────────────────
    st.markdown("#### Get started")

    connected = st.session_state.connected
    has_envs = bool(st.session_state.get("env_select"))

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
                <strong>3</strong> Extract &amp; Explore
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "Use the **sidebar** to connect your Confluent Cloud account, "
        "select environments and clusters, then extract lineage. "
        "Or load a demo graph to explore the interface."
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


def _render_watcher_tab():
    """Render the Change Watcher tab content."""
    from lineage_bridge.ui.watcher import render_watcher_log

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
