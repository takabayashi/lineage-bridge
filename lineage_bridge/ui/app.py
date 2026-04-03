"""Streamlit UI for visualizing the LineageBridge lineage graph."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import streamlit as st
from streamlit_agraph import Config, agraph

from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.graph_renderer import render_graph
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.styles import (
    EDGE_COLORS,
    EDGE_TYPE_LABELS,
    NODE_COLORS,
    NODE_TYPE_LABELS,
)

# ── Page configuration ─────────────────────────────────────────────────

st.set_page_config(
    page_title="LineageBridge",
    page_icon="\U0001f310",  # globe
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state defaults ─────────────────────────────────────────────

if "graph" not in st.session_state:
    st.session_state.graph = None
if "selected_node" not in st.session_state:
    st.session_state.selected_node = None
if "focus_node" not in st.session_state:
    st.session_state.focus_node = None


def _load_graph_from_path(path_str: str) -> LineageGraph | None:
    """Load a graph from a file path, returning None on failure."""
    p = Path(path_str).expanduser()
    if not p.exists():
        st.sidebar.error(f"File not found: {p}")
        return None
    try:
        return LineageGraph.from_json_file(p)
    except Exception as exc:
        st.sidebar.error(f"Failed to parse graph JSON: {exc}")
        return None


def _load_graph_from_upload(uploaded_file) -> LineageGraph | None:
    """Load a graph from an uploaded file object."""
    try:
        data = json.loads(uploaded_file.getvalue())
        return LineageGraph.from_dict(data)
    except Exception as exc:
        st.sidebar.error(f"Failed to parse uploaded JSON: {exc}")
        return None


# ── Sidebar ────────────────────────────────────────────────────────────

st.sidebar.title("\U0001f310 LineageBridge")
st.sidebar.markdown("---")

# -- Data loading section --
st.sidebar.subheader("Load Graph")

load_tab_file, load_tab_upload, load_tab_demo = st.sidebar.tabs(["File Path", "Upload", "Demo"])

with load_tab_file:
    graph_path = st.text_input(
        "Graph JSON path",
        value="./lineage_graph.json",
        key="graph_path_input",
    )
    if st.button("Load", key="load_path_btn"):
        loaded = _load_graph_from_path(graph_path)
        if loaded:
            st.session_state.graph = loaded
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()

with load_tab_upload:
    uploaded = st.file_uploader("Upload graph JSON", type=["json"], key="json_upload")
    if uploaded is not None:
        loaded = _load_graph_from_upload(uploaded)
        if loaded:
            st.session_state.graph = loaded
            st.session_state.selected_node = None
            st.session_state.focus_node = None

with load_tab_demo:
    if st.button("Load sample data", key="load_demo_btn"):
        st.session_state.graph = generate_sample_graph()
        st.session_state.selected_node = None
        st.session_state.focus_node = None
        st.rerun()

graph: LineageGraph | None = st.session_state.graph

# -- Stats --
if graph:
    st.sidebar.markdown("---")
    st.sidebar.subheader("Graph Stats")
    col1, col2 = st.sidebar.columns(2)
    col1.metric("Nodes", graph.node_count)
    col2.metric("Edges", graph.edge_count)

    # -- Filters --
    st.sidebar.markdown("---")
    st.sidebar.subheader("Filters")

    # Node type checkboxes with colored labels
    type_filters: dict[NodeType, bool] = {}
    for ntype in NodeType:
        color = NODE_COLORS.get(ntype, "#757575")
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        count = len(graph.filter_by_type(ntype))
        type_filters[ntype] = st.sidebar.checkbox(
            f"{label} ({count})",
            value=True,
            key=f"filter_{ntype.value}",
            help=f"Toggle {label} nodes",
        )

    # Environment dropdown
    environments = sorted({n.environment_id for n in graph.nodes if n.environment_id})
    env_options = ["All", *environments]
    env_filter = st.sidebar.selectbox("Environment", env_options, key="env_filter")
    selected_env = None if env_filter == "All" else env_filter

    # Cluster dropdown
    clusters = sorted({n.cluster_id for n in graph.nodes if n.cluster_id})
    cluster_options = ["All", *clusters]
    cluster_filter = st.sidebar.selectbox("Cluster", cluster_options, key="cluster_filter")
    selected_cluster = None if cluster_filter == "All" else cluster_filter

    # -- Search --
    st.sidebar.markdown("---")
    st.sidebar.subheader("Search")
    search_query = st.sidebar.text_input(
        "Search nodes",
        placeholder="Type to filter by name...",
        key="search_input",
    )

    # -- Hop control --
    st.sidebar.markdown("---")
    st.sidebar.subheader("Neighborhood Hops")
    focus_active = st.session_state.focus_node is not None
    hops = st.sidebar.slider(
        "Hops from selected node",
        min_value=1,
        max_value=5,
        value=2,
        disabled=not focus_active,
        key="hop_slider",
    )
    if focus_active:
        focus_node_obj = graph.get_node(st.session_state.focus_node)
        fname = focus_node_obj.display_name if focus_node_obj else st.session_state.focus_node
        st.sidebar.info(f"Focused on: **{fname}**")
        if st.sidebar.button("Clear focus"):
            st.session_state.focus_node = None
            st.rerun()

    # -- Legend --
    st.sidebar.markdown("---")
    st.sidebar.subheader("Legend")

    st.sidebar.markdown("**Node Types**")
    for ntype in NodeType:
        color = NODE_COLORS.get(ntype, "#757575")
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        st.sidebar.markdown(
            f"<span style='color:{color}; font-size:18px;'>&#9632;</span> {label}",
            unsafe_allow_html=True,
        )

    st.sidebar.markdown("**Edge Types**")
    for etype, label in EDGE_TYPE_LABELS.items():
        color = EDGE_COLORS.get(etype, "#757575")
        st.sidebar.markdown(
            f"<span style='color:{color}; font-size:14px;'>&#9472;&#9472;&#9654;</span> {label}",
            unsafe_allow_html=True,
        )

# ── Main area ──────────────────────────────────────────────────────────

if graph is None:
    st.title("\U0001f310 LineageBridge")
    st.markdown(
        """
        Welcome to **LineageBridge** -- interactive stream-lineage visualization
        for Confluent Cloud.

        ### Getting started

        1. **Load from file** -- enter the path to a lineage graph JSON file
           (produced by the extractor) in the sidebar, then click **Load**.
        2. **Upload** -- drag and drop a JSON file into the sidebar uploader.
        3. **Demo** -- click **Load sample data** in the sidebar to explore
           a realistic demo graph without any Confluent credentials.

        Once a graph is loaded you can filter by node type, search by name,
        click nodes to view details, and focus on multi-hop neighborhoods.
        """
    )
else:
    st.title("\U0001f310 Lineage Graph")

    # Build filtered graph
    agraph_nodes, agraph_edges = render_graph(
        graph,
        filters=type_filters,
        search_query=search_query,
        selected_node=st.session_state.focus_node,
        hops=hops,
        environment_filter=selected_env,
        cluster_filter=selected_cluster,
    )

    if not agraph_nodes:
        st.warning("No nodes match the current filters. Adjust the sidebar filters or search.")
    else:
        st.caption(f"Showing {len(agraph_nodes)} nodes and {len(agraph_edges)} edges")

        # agraph config -- parameters map to vis.js network options
        config = Config(
            width=960,
            height=620,
            directed=True,
            hierarchical=True,
            levelSeparation=250,
            nodeSpacing=120,
            treeSpacing=150,
            sortMethod="directed",
            direction="LR",
            physics=True,
            nodeHighlightBehavior=True,
            highlightColor="#F5F5F5",
            collapsible=False,
        )

        clicked_node = agraph(nodes=agraph_nodes, edges=agraph_edges, config=config)

        if clicked_node:
            st.session_state.selected_node = clicked_node

    # ── Node detail panel ──────────────────────────────────────────────
    sel_id = st.session_state.selected_node
    if sel_id:
        sel_node = graph.get_node(sel_id)
        if sel_node:
            st.markdown("---")
            st.subheader(f"Node Details: {sel_node.display_name}")

            det_col1, det_col2 = st.columns(2)

            with det_col1:
                ntype_label = NODE_TYPE_LABELS.get(sel_node.node_type, sel_node.node_type.value)
                ncolor = NODE_COLORS.get(sel_node.node_type, "#757575")
                st.markdown(
                    f"**Type:** <span style='color:{ncolor};'>&#9632;</span> {ntype_label}",
                    unsafe_allow_html=True,
                )
                st.markdown(f"**Display Name:** {sel_node.display_name}")
                st.markdown(f"**Qualified Name:** `{sel_node.qualified_name}`")
                st.markdown(f"**Node ID:** `{sel_node.node_id}`")

                if sel_node.environment_id:
                    st.markdown(f"**Environment:** {sel_node.environment_id}")
                if sel_node.cluster_id:
                    st.markdown(f"**Cluster:** {sel_node.cluster_id}")

                if sel_node.url:
                    st.markdown(f"**URL:** [{sel_node.url}]({sel_node.url})")

                # Tags
                if sel_node.tags:
                    tag_html = " ".join(
                        f"<span style='background:{ncolor}; color:white; padding:2px 8px; "
                        f"border-radius:12px; margin-right:4px; font-size:13px;'>{t}</span>"
                        for t in sel_node.tags
                    )
                    st.markdown(f"**Tags:** {tag_html}", unsafe_allow_html=True)

            with det_col2:
                # Attributes table
                if sel_node.attributes:
                    st.markdown("**Attributes**")
                    attr_rows = [
                        {"Key": k, "Value": str(v)} for k, v in sel_node.attributes.items()
                    ]
                    st.table(attr_rows)

            # Neighbors
            nb_col1, nb_col2 = st.columns(2)
            with nb_col1:
                upstream = graph.get_neighbors(sel_id, direction="upstream")
                st.markdown(f"**Upstream ({len(upstream)})**")
                if upstream:
                    for nb in upstream:
                        nb_color = NODE_COLORS.get(nb.node_type, "#757575")
                        st.markdown(
                            f"<span style='color:{nb_color};'>&#9632;</span> {nb.display_name}",
                            unsafe_allow_html=True,
                        )
                else:
                    st.caption("No upstream neighbors")

            with nb_col2:
                downstream = graph.get_neighbors(sel_id, direction="downstream")
                st.markdown(f"**Downstream ({len(downstream)})**")
                if downstream:
                    for nb in downstream:
                        nb_color = NODE_COLORS.get(nb.node_type, "#757575")
                        st.markdown(
                            f"<span style='color:{nb_color};'>&#9632;</span> {nb.display_name}",
                            unsafe_allow_html=True,
                        )
                else:
                    st.caption("No downstream neighbors")

            # Focus button
            st.markdown("")
            focus_col, clear_col, _ = st.columns([1, 1, 3])
            with focus_col:
                if st.button(
                    "\U0001f50d Focus on this node",
                    key="focus_btn",
                ):
                    st.session_state.focus_node = sel_id
                    st.rerun()
            with clear_col:
                if st.button("Clear selection", key="clear_sel_btn"):
                    st.session_state.selected_node = None
                    st.rerun()

    # ── Export ─────────────────────────────────────────────────────────
    st.markdown("---")
    export_data = json.dumps(graph.to_dict(), indent=2, default=str)
    st.download_button(
        label="\U0001f4e5 Download graph as JSON",
        data=export_data,
        file_name="lineage_graph.json",
        mime="application/json",
    )


def run() -> None:
    """CLI entry point that launches the Streamlit app."""
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__],
        check=True,
    )
