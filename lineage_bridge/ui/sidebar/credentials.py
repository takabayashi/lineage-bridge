# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — per-env (SR + Flink) and per-cluster credential inputs.

Extracted from the inline expanders inside `scope.py`'s `_render_sidebar_scope`
so the credential UI is one module's concern. UI structure is unchanged —
`scope.py` still wraps these in the same place inside the sidebar tree.
"""

from __future__ import annotations

from typing import Any

import streamlit as st


def _render_env_credentials(
    selected_envs: list[Any],
    cache: dict[str, dict[str, Any]],
) -> None:
    """Per-environment Schema Registry endpoint + SR/Flink credential inputs.

    Persisted in `st.session_state` under keys `sr_endpoint_{env_id}` /
    `sr_key_{env_id}` / `sr_secret_{env_id}` / `flink_key_{env_id}` /
    `flink_secret_{env_id}`. `_resolve_extraction_context` (in actions.py)
    reads them back at extraction time.
    """
    cached_sr_creds = st.session_state.get("_cached_sr_creds", {})
    cached_flink_creds = st.session_state.get("_cached_flink_creds", {})
    with st.expander("Environment API Keys", expanded=False):
        st.caption(
            "SR and Flink keys are environment-scoped. "
            "Enter one per environment, or leave blank "
            "to use the global keys from .env."
        )
        for env in selected_envs:
            eid = env.id
            svc = cache.get(eid, {}).get("services")
            st.markdown(f"**{env.display_name}** (`{eid}`)")

            # SR endpoint + keys
            sr_endpoint_key = f"sr_endpoint_{eid}"
            sr_key_key = f"sr_key_{eid}"
            sr_secret_key = f"sr_secret_{eid}"
            cached = cached_sr_creds.get(eid, {})
            if sr_endpoint_key not in st.session_state and cached.get("endpoint"):
                st.session_state[sr_endpoint_key] = cached["endpoint"]
            if sr_key_key not in st.session_state and cached.get("api_key"):
                st.session_state[sr_key_key] = cached["api_key"]
            if sr_secret_key not in st.session_state and cached.get("api_secret"):
                st.session_state[sr_secret_key] = cached["api_secret"]
            st.text_input(
                "Schema Registry Endpoint",
                key=sr_endpoint_key,
                placeholder="e.g. https://psrc-xxxxx.region.cloud.confluent.cloud",
            )
            st.text_input(
                "Schema Registry Key",
                key=sr_key_key,
                type="password",
                placeholder="Leave blank for global key",
            )
            st.text_input(
                "Schema Registry Secret",
                key=sr_secret_key,
                type="password",
                placeholder="Leave blank for global key",
            )

            # Flink keys
            has_flink = svc and svc.has_flink
            flink_key_key = f"flink_key_{eid}"
            flink_secret_key = f"flink_secret_{eid}"
            cached_f = cached_flink_creds.get(eid, {})
            if flink_key_key not in st.session_state and cached_f.get("api_key"):
                st.session_state[flink_key_key] = cached_f["api_key"]
            if flink_secret_key not in st.session_state and cached_f.get("api_secret"):
                st.session_state[flink_secret_key] = cached_f["api_secret"]
            st.text_input(
                "Flink Key",
                key=flink_key_key,
                type="password",
                placeholder="Leave blank for global key",
                disabled=not has_flink,
            )
            st.text_input(
                "Flink Secret",
                key=flink_secret_key,
                type="password",
                placeholder="Leave blank for global key",
                disabled=not has_flink,
            )
            st.markdown("---")


def _render_cluster_credentials(all_cluster_options: dict[str, Any]) -> None:
    """Per-cluster API key inputs (when a cluster needs cluster-scoped credentials).

    Persisted under `cluster_key_{cluster_id}` / `cluster_secret_{cluster_id}`.
    """
    cached_creds = st.session_state.get("_cached_cluster_creds", {})
    with st.expander("Cluster API Keys", expanded=False):
        st.caption(
            "Optional. If a cluster needs its own API key "
            "(e.g. cluster-scoped keys), enter it here. "
            "Leave blank to use the global key."
        )
        for _label, cluster in all_cluster_options.items():
            cid = cluster.id
            key_key = f"cluster_key_{cid}"
            secret_key = f"cluster_secret_{cid}"
            # Pre-fill from cache if not already in session state
            cached = cached_creds.get(cid, {})
            if key_key not in st.session_state and cached.get("api_key"):
                st.session_state[key_key] = cached["api_key"]
            if secret_key not in st.session_state and cached.get("api_secret"):
                st.session_state[secret_key] = cached["api_secret"]
            st.text_input(
                f"{cluster.display_name} — API Key",
                key=key_key,
                type="password",
                placeholder="Leave blank for global key",
            )
            st.text_input(
                f"{cluster.display_name} — API Secret",
                key=secret_key,
                type="password",
                placeholder="Leave blank for global key",
            )
