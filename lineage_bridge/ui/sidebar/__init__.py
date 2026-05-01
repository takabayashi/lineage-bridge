# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar package — composition, section helper, public render entry point.

Per Phase 2E (docs/plan-refactor.md), the original 1,179 LOC `sidebar.py` is
split into focused modules under this package. The composition lives here,
the actual widget code lives in the per-concern modules:

    sidebar/connection.py    connect / disconnect
    sidebar/scope.py         env + cluster pickers, extractor toggles
    sidebar/credentials.py   per-env / per-cluster credential inputs
    sidebar/actions.py       extract / enrich / push buttons + log
    sidebar/filters.py       graph filters, legend, type counts

`from lineage_bridge.ui.sidebar import _render_sidebar` still works (this
module re-exports the entry point), so `ui/app.py` needs no changes.
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.sidebar.actions import (
    _render_sidebar_actions,
    _render_sidebar_aws,
    _render_sidebar_databricks,
    _render_sidebar_google,
    _render_sidebar_load_data,
    _render_sidebar_push_log,
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


def _sidebar_section(label: str) -> None:
    """Render a styled section header with divider."""
    st.markdown(
        f"<hr class='sidebar-divider'/><div class='sidebar-section'>{label}</div>",
        unsafe_allow_html=True,
    )


def _render_sidebar() -> None:
    """Persistent sidebar: connection, scope, extractors, actions, filters, data."""
    with st.sidebar:
        st.markdown("### \U0001f310 LineageBridge")

        # ══════════════════════════════════════════════════════════════
        #  SETUP
        # ══════════════════════════════════════════════════════════════
        _sidebar_section("Setup")

        # Connection status at sidebar root level (full width)
        if st.session_state.connected:
            envs = st.session_state.environments
            st.markdown(
                f"<div class='status-badge status-connected'>"
                f"<span class='status-dot' style='background:#4CAF50'></span>"
                f"Connected &mdash; {len(envs)} environment(s)"
                f"</div>",
                unsafe_allow_html=True,
            )

        with st.expander("Connection", expanded=not st.session_state.connected):
            _render_sidebar_connection()

        if st.session_state.connected:
            with st.expander("Infrastructure", expanded=st.session_state.graph is None):
                _render_sidebar_scope()

            # ══════════════════════════════════════════════════════════
            #  EXTRACTION
            # ══════════════════════════════════════════════════════════
            _sidebar_section("Extraction")

            with st.expander("Extractors", expanded=False):
                _render_sidebar_extractors()

            _render_sidebar_actions()

            # ══════════════════════════════════════════════════════════
            #  PUBLISH
            # ══════════════════════════════════════════════════════════
            has_graph = st.session_state.graph is not None
            if has_graph:
                _sidebar_section("Publish")

                with st.expander("Databricks", expanded=False):
                    _render_sidebar_databricks()

                with st.expander("AWS", expanded=False):
                    _render_sidebar_aws()

                with st.expander("Google Data Lineage", expanded=False):
                    _render_sidebar_google()

                _render_sidebar_push_log()

        # ══════════════════════════════════════════════════════════════
        #  GRAPH
        # ══════════════════════════════════════════════════════════════
        graph = st.session_state.graph
        if graph is not None:
            _sidebar_section("Graph")

            # Focus indicator at sidebar root level (full width)
            focus_active = st.session_state.focus_node is not None
            if focus_active:
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

            with st.expander("Filters", expanded=True):
                _render_sidebar_graph_filters(graph)
            with st.expander("Legend", expanded=False):
                _render_sidebar_legend(graph)

        # ══════════════════════════════════════════════════════════════
        #  DATA
        # ══════════════════════════════════════════════════════════════
        _sidebar_section("Data")

        with st.expander("Load Data", expanded=False):
            _render_sidebar_load_data()
