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
    "_log_source": None,
    "last_extraction_params": None,
    "last_extraction_time": None,
    "_cache_loaded": False,
    "watcher_engine": None,
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
    st.session_state._cache_loaded = True
