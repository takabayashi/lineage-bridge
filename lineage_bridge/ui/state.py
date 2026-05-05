# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Session state initialization and cache restoration."""

from __future__ import annotations

import streamlit as st

from lineage_bridge.config.cache import load_cache

_GRAPH_VERSION = 3  # bump when graph model changes to invalidate caches

_DEFAULTS = {
    "graph": None,
    "graph_version": None,
    "selected_node": None,
    "focus_node": None,
    "_dismissed_node": None,
    "connected": False,
    "environments": [],
    "env_cache": {},
    "extraction_log": [],
    "push_log": [],
    "last_extraction_params": None,
    "last_extraction_time": None,
    "_cache_loaded": False,
    "_last_click_seq": None,
    "graph_height": 650,
    "watcher_engine": None,
    # Stronger error signal: set by sidebar actions when an extraction or
    # push fails or completes with warnings; consumed by the top-of-page
    # alert banner and the activity-log drawer's auto-expand.
    # Shape: {"kind": "error"|"warning", "message": str, "log": <log key>}
    "_activity_alert": None,
}


def ensure_defaults() -> None:
    """Idempotent session state initialization."""
    for key, default in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # Invalidate stale graph from old code versions
    if st.session_state.graph is not None and st.session_state.graph_version != _GRAPH_VERSION:
        st.session_state.graph = None
        st.session_state.graph_version = None
        st.session_state.selected_node = None
        st.session_state.focus_node = None


def load_cached_selections() -> None:
    """Restore selections from local encrypted cache on first run."""
    if st.session_state._cache_loaded:
        return

    disk_cache = load_cache()
    if disk_cache.get("selected_envs"):
        st.session_state["_cached_selected_envs"] = disk_cache["selected_envs"]
    if disk_cache.get("selected_clusters"):
        st.session_state["_cached_selected_clusters"] = disk_cache["selected_clusters"]
    if disk_cache.get("cluster_credentials"):
        st.session_state["_cached_cluster_creds"] = disk_cache["cluster_credentials"]
    if disk_cache.get("sr_credentials"):
        st.session_state["_cached_sr_creds"] = disk_cache["sr_credentials"]
    if disk_cache.get("flink_credentials"):
        st.session_state["_cached_flink_creds"] = disk_cache["flink_credentials"]
    if disk_cache.get("last_extraction_params"):
        st.session_state["last_extraction_params"] = disk_cache["last_extraction_params"]

    # Eager-seed the credential widget keys so the Manage Credentials dialog
    # shows cached values on its first open. The lazy `_seed_*_state` helpers
    # in `sidebar/credentials.py` would do this on render, but Streamlit's
    # dialog flow reads widget state before the inline-row seed propagates
    # to the dialog body, leaving the form blank until a second open.
    for env_id, cred in (disk_cache.get("sr_credentials") or {}).items():
        for key, val in (
            (f"sr_endpoint_{env_id}", cred.get("endpoint")),
            (f"sr_key_{env_id}", cred.get("api_key")),
            (f"sr_secret_{env_id}", cred.get("api_secret")),
        ):
            if val and key not in st.session_state:
                st.session_state[key] = val
    for env_id, cred in (disk_cache.get("flink_credentials") or {}).items():
        for key, val in (
            (f"flink_key_{env_id}", cred.get("api_key")),
            (f"flink_secret_{env_id}", cred.get("api_secret")),
        ):
            if val and key not in st.session_state:
                st.session_state[key] = val
    for cluster_id, cred in (disk_cache.get("cluster_credentials") or {}).items():
        for key, val in (
            (f"cluster_key_{cluster_id}", cred.get("api_key")),
            (f"cluster_secret_{cluster_id}", cred.get("api_secret")),
        ):
            if val and key not in st.session_state:
                st.session_state[key] = val
    audit_log = disk_cache.get("audit_log_credentials") or {}
    for key, val in (
        ("watcher_audit_bootstrap", audit_log.get("bootstrap_servers")),
        ("watcher_audit_key", audit_log.get("api_key")),
        ("watcher_audit_secret", audit_log.get("api_secret")),
    ):
        if val and key not in st.session_state:
            st.session_state[key] = val

    st.session_state._cache_loaded = True
