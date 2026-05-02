# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar package — composition + render entry point.

Phase A redesign: the 8-section accordion (Connection / Infrastructure /
Extractors / Databricks / AWS / Google / Filters / Legend / Load Data) is
collapsed into 4 stable sections that mirror the user's mental model:

    1. Setup    — connection + environment / cluster picker + credentials
    2. Run      — extractor toggles + extract / enrich / refresh action
    3. Publish  — single panel listing every catalog target with a status pill
    4. Explore  — filters + legend + load-from-file (only when a graph exists)

Per-concern modules live alongside this file:

    sidebar/connection.py    connect / disconnect
    sidebar/scope.py         env + cluster pickers + extractor toggles
    sidebar/credentials.py   credential modals (st.dialog)
    sidebar/actions.py       extract + publish actions, logs, file load
    sidebar/filters.py       graph filters, legend, type counts
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.sidebar.actions import (
    _render_sidebar_actions,
    _render_sidebar_load_data,
    _render_sidebar_publish,
)
from lineage_bridge.ui.sidebar.connection import _render_sidebar_connection
from lineage_bridge.ui.sidebar.filters import (
    _render_sidebar_graph_filters,
    _render_sidebar_legend,
)
from lineage_bridge.ui.sidebar.scope import (
    _render_sidebar_extractors,
    _render_sidebar_scope,
)

__all__ = ["_render_sidebar"]


def _render_sidebar() -> None:
    """Persistent sidebar — 4 sections: Setup, Run, Publish, Explore."""
    with st.sidebar:
        st.markdown("### \U0001f310 LineageBridge")
        _render_status_strip()

        # ── 1. Setup ────────────────────────────────────────────────
        connected = st.session_state.connected
        with st.expander("Setup", expanded=not connected or st.session_state.graph is None):
            _render_sidebar_connection()
            if connected:
                st.divider()
                _render_sidebar_scope()

        if connected:
            # ── 2. Run ──────────────────────────────────────────────
            with st.expander("Run", expanded=st.session_state.graph is None):
                _render_sidebar_extractors()
                st.divider()
                _render_sidebar_actions()

            # ── 3. Publish ──────────────────────────────────────────
            if st.session_state.graph is not None:
                with st.expander("Publish", expanded=False):
                    _render_sidebar_publish()

        # ── 4. Explore (only when a graph is loaded) ────────────────
        graph = st.session_state.graph
        if graph is not None:
            with st.expander("Explore", expanded=True):
                _render_focus_indicator(graph)
                _render_sidebar_graph_filters(graph)
                st.divider()
                with st.expander("Legend", expanded=False):
                    _render_sidebar_legend(graph)
                st.divider()
                with st.expander("Load from file", expanded=False):
                    _render_sidebar_load_data()
        elif connected:
            with st.expander("Load from file", expanded=False):
                _render_sidebar_load_data()


def _render_status_strip() -> None:
    """Compact connection-status strip at the top of the sidebar."""
    if st.session_state.connected:
        envs = st.session_state.environments
        st.markdown(
            f"<div class='status-badge status-connected'>"
            f"<span class='status-dot' style='background:#4CAF50'></span>"
            f"Connected &mdash; {len(envs)} environment(s)"
            f"</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div class='status-badge status-disconnected'>"
            "<span class='status-dot' style='background:#9E9E9E'></span>"
            "Not connected"
            "</div>",
            unsafe_allow_html=True,
        )


def _render_focus_indicator(graph) -> None:
    """Show focus pill + clear button when a node is focused."""
    focus_active = st.session_state.focus_node is not None
    if not focus_active:
        return
    focus_obj = graph.get_node(st.session_state.focus_node)
    fname = focus_obj.display_name if focus_obj else st.session_state.focus_node
    st.markdown(
        f"<div class='status-badge' style='background:rgba(33,150,243,0.1);"
        f"color:#1565C0;border:1px solid rgba(33,150,243,0.2);'>"
        f"<span class='status-dot' style='background:#1976D2'></span>"
        f"Focused: {fname}"
        f"</div>",
        unsafe_allow_html=True,
    )
    if st.button("Clear focus", key="clear_focus_btn", width="stretch"):
        st.session_state.focus_node = None
        st.rerun()
