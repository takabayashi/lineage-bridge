# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — environment + cluster discovery and selection, extractor toggles.

Phase A redesign: per-env / per-cluster credential entry no longer uses
nested expanders; instead each row gets a status pill + "Manage" button
that opens an `st.dialog` (see `credentials.py`).
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.discovery import _discover_one, _try_load_settings
from lineage_bridge.ui.sidebar.credentials import (
    render_cluster_credentials_row,
    render_env_credentials_row,
)


def _render_sidebar_scope() -> None:
    """Environment and cluster selection."""
    settings = _try_load_settings()
    if not settings:
        return

    all_envs = st.session_state.environments
    cache = st.session_state.env_cache

    # Discovery: single primary button. Once everything's discovered, switch
    # the label to "Refresh" so the user has one button instead of two
    # adjacent buttons that look identical and silently re-poll the API.
    all_discovered = bool(all_envs) and all(env.id in cache for env in all_envs)
    btn_label = "Refresh discovery" if all_discovered else "Discover"
    if st.button(
        btn_label,
        key="discover_btn",
        type="primary" if not all_discovered else "secondary",
        width="stretch",
    ):
        bar = st.progress(0)
        for i, env in enumerate(all_envs):
            try:
                cache[env.id] = _discover_one(settings, env.id)
            except Exception as exc:
                cache[env.id] = {"services": None, "error": str(exc)}
            bar.progress((i + 1) / max(len(all_envs), 1))
        bar.empty()
        st.rerun()

    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    if not discovered_envs:
        st.caption("Click **Discover** to find services in your environments.")
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

    # Per-environment credentials: status pill + Manage button (opens dialog).
    # `settings` is passed so the pill can show "global" when the .env has
    # SR/Flink keys and turn green when per-env keys override them.
    if selected_envs:
        st.caption("Environment credentials")
        for env in selected_envs:
            svc = cache.get(env.id, {}).get("services")
            has_flink = bool(svc and svc.has_flink)
            render_env_credentials_row(env, has_flink, settings)

    # Cluster multiselect
    all_cluster_options = {}
    for env in selected_envs:
        svc = cache[env.id]["services"]
        for c in svc.clusters:
            label = f"{c.display_name} ({c.id})"
            all_cluster_options[label] = c

    if all_cluster_options:
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

        # Per-cluster credentials: only show for selected clusters to keep
        # the section compact. (Old UI showed all clusters even if not
        # selected, padding the sidebar with unused fields.)
        selected_cluster_labels = st.session_state.get("cluster_select", [])
        selected_clusters = [all_cluster_options[lbl] for lbl in selected_cluster_labels]
        if selected_clusters:
            st.caption("Cluster credentials (optional)")
            for cluster in selected_clusters:
                render_cluster_credentials_row(cluster, settings)


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

    c1, c2 = st.columns(2)
    with c1:
        st.checkbox("Connectors", value=True, key="ext_connect")
        st.checkbox("Flink", value=True, disabled=not any_flink, key="ext_flink")
        st.checkbox("Schema Registry", value=True, disabled=not any_sr, key="ext_sr")
        st.checkbox("Tableflow", value=True, key="ext_tf")
    with c2:
        st.checkbox("ksqlDB", value=True, disabled=not any_ksqldb, key="ext_ksqldb")
        st.checkbox("Stream Catalog", value=True, disabled=not any_sr, key="ext_catalog")
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
