# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — environment + cluster discovery and selection, extractor toggles.

The credential inputs that used to live inline in `_render_sidebar_scope` now
live in `credentials.py`; this module calls into them at the right point in
the UI tree so the sidebar layout is unchanged.
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.discovery import _discover_one, _try_load_settings
from lineage_bridge.ui.sidebar.credentials import (
    _render_cluster_credentials,
    _render_env_credentials,
)


def _render_sidebar_scope() -> None:
    """Environment and cluster selection."""
    settings = _try_load_settings()
    if not settings:
        return

    all_envs = st.session_state.environments
    cache = st.session_state.env_cache

    # Discovery buttons
    all_discovered = all(env.id in cache for env in all_envs)
    c1, c2 = st.columns(2)
    with c1:
        if st.button(
            "Discover",
            key="discover_all_btn",
            type="primary" if not all_discovered else "secondary",
            disabled=all_discovered,
            width="stretch",
        ):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                if env.id not in cache:
                    try:
                        cache[env.id] = _discover_one(settings, env.id)
                    except Exception as exc:
                        cache[env.id] = {"services": None, "error": str(exc)}
                bar.progress((i + 1) / len(all_envs))
            bar.empty()
            st.rerun()
    with c2:
        if st.button("Refresh", key="refresh_discovery_btn", width="stretch"):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                try:
                    cache[env.id] = _discover_one(settings, env.id)
                except Exception as exc:
                    cache[env.id] = {"services": None, "error": str(exc)}
                bar.progress((i + 1) / len(all_envs))
            bar.empty()
            st.rerun()

    # Show discovered environments
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]

    if not discovered_envs:
        st.caption("Click **Discover** to find services.")
        return

    # Environment selector
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    env_options = ["", *list(env_labels.keys())]
    selected_env_label = st.selectbox(
        "Environment",
        options=env_options,
        index=0,
        key="env_select",
        placeholder="Select an environment...",
    )
    selected_envs = [env_labels[selected_env_label]] if selected_env_label else []

    # Per-environment service keys (optional) — moved to credentials.py
    if selected_envs:
        _render_env_credentials(selected_envs, cache)

    # Cluster multiselect
    all_cluster_options = {}
    for env in selected_envs:
        svc = cache[env.id]["services"]
        for c in svc.clusters:
            label = f"{c.display_name} ({c.id})"
            all_cluster_options[label] = c

    if all_cluster_options:
        # Pre-seed session_state instead of passing `default=` to the widget.
        # Streamlit warns when a widget has BOTH a `key` AND a `default` if
        # the key already exists in session_state — and on reruns the key
        # always exists. Pre-seeding avoids that conflict and lets us also
        # drop stale labels left over from a previous env's cluster set.
        valid = list(all_cluster_options.keys())
        prior = st.session_state.get("cluster_select")
        if prior is None:
            st.session_state["cluster_select"] = valid
        else:
            kept = [lbl for lbl in prior if lbl in all_cluster_options]
            if kept != list(prior):
                st.session_state["cluster_select"] = kept or valid
        st.multiselect(
            "Clusters",
            options=valid,
            key="cluster_select",
        )

        # Per-cluster API keys (optional) — moved to credentials.py
        _render_cluster_credentials(all_cluster_options)


def _render_sidebar_extractors() -> None:
    """Extractor toggles."""
    cache = st.session_state.env_cache
    all_envs = st.session_state.environments

    # Check service availability
    selected_env_label = st.session_state.get("env_select", "")
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_envs = [env_labels[selected_env_label]] if selected_env_label in env_labels else []

    any_sr = any(
        cache[e.id]["services"].has_schema_registry
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )
    any_ksqldb = any(
        cache[e.id]["services"].has_ksqldb
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )
    any_flink = any(
        cache[e.id]["services"].has_flink
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )

    st.checkbox("Connectors", value=True, key="ext_connect")
    st.checkbox("ksqlDB", value=True, disabled=not any_ksqldb, key="ext_ksqldb")
    st.checkbox("Flink", value=True, disabled=not any_flink, key="ext_flink")
    st.checkbox("Schema Registry", value=True, disabled=not any_sr, key="ext_sr")
    st.checkbox("Stream Catalog", value=True, disabled=not any_sr, key="ext_catalog")
    st.checkbox("Tableflow", value=True, key="ext_tf")
    st.checkbox(
        "Metrics",
        value=True,
        key="ext_metrics",
        help="Enrich nodes with live throughput data.",
    )
    if st.session_state.get("ext_metrics"):
        st.slider(
            "Metrics lookback (hours)",
            min_value=1,
            max_value=24,
            value=1,
            key="metrics_lookback",
        )
