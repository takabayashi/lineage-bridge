# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — connect / disconnect widgets.

Phase A redesign: dropped the in-expander "Connection active." caption +
duplicate `st.info(...)` line. The connection status badge at the sidebar
root (in `__init__.py`) is the single source of truth for connection state.
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.discovery import _make_cloud_client, _run_async, _try_load_settings


def _render_sidebar_connection() -> None:
    """Connection controls. Status itself is rendered at sidebar root."""
    settings = _try_load_settings()

    if st.session_state.connected:
        if st.button(
            "Disconnect",
            key="disconnect_btn",
            type="secondary",
            width="stretch",
        ):
            # Clear all connection and extraction state
            st.session_state.connected = False
            st.session_state.environments = []
            st.session_state.env_cache = {}
            st.session_state.graph = None
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.session_state.last_extraction_params = None
            st.session_state.extraction_log = []
            st.session_state.push_log = []
            # Clear cached credentials
            st.session_state._cached_cluster_creds = {}
            st.session_state._cached_sr_creds = {}
            st.session_state._cached_flink_creds = {}
            st.rerun()
        return

    if settings:
        api_key = settings.confluent_cloud_api_key
        masked = api_key[:4] + "..." + api_key[-4:]
        st.caption(f"Credentials: `{masked}`")

        if st.button(
            "Connect",
            key="connect_btn",
            type="primary",
            width="stretch",
        ):
            with st.status("Connecting...", expanded=True) as status:
                try:
                    from lineage_bridge.clients.discovery import list_environments

                    async def _connect():
                        async with _make_cloud_client(settings) as cloud:
                            return await list_environments(cloud)

                    envs = _run_async(_connect())
                    st.session_state.connected = True
                    st.session_state.environments = envs
                    status.update(
                        label=f"Connected — {len(envs)} env(s)",
                        state="complete",
                    )
                    st.rerun()
                except Exception as exc:
                    status.update(
                        label=f"Failed: {exc}",
                        state="error",
                    )
    else:
        st.warning("No credentials found.")
        st.code(
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...\n"
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...",
            language="bash",
        )
