# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — credential modals (Schema Registry, Flink, per-cluster).

Phase A redesign: credentials no longer live in nested sidebar expanders.
Each environment / cluster gets a "Manage credentials" button in the scope
section that opens a focused `st.dialog`. The session-state shape is
preserved verbatim so `_resolve_extraction_context` (in actions.py) keeps
working.
"""

from __future__ import annotations

from typing import Any

import streamlit as st


def _seed_env_state(eid: str) -> None:
    """Pre-fill SR/Flink session keys for *eid* from the encrypted cache (idempotent)."""
    cached_sr = st.session_state.get("_cached_sr_creds", {}).get(eid, {})
    cached_flink = st.session_state.get("_cached_flink_creds", {}).get(eid, {})
    for key, val in (
        (f"sr_endpoint_{eid}", cached_sr.get("endpoint")),
        (f"sr_key_{eid}", cached_sr.get("api_key")),
        (f"sr_secret_{eid}", cached_sr.get("api_secret")),
        (f"flink_key_{eid}", cached_flink.get("api_key")),
        (f"flink_secret_{eid}", cached_flink.get("api_secret")),
    ):
        if val and key not in st.session_state:
            st.session_state[key] = val


def _seed_cluster_state(cid: str) -> None:
    """Pre-fill cluster session keys for *cid* from the encrypted cache (idempotent)."""
    cached = st.session_state.get("_cached_cluster_creds", {}).get(cid, {})
    for key, val in (
        (f"cluster_key_{cid}", cached.get("api_key")),
        (f"cluster_secret_{cid}", cached.get("api_secret")),
    ):
        if val and key not in st.session_state:
            st.session_state[key] = val


def env_creds_status(env_id: str, has_flink: bool) -> tuple[str, str]:
    """Return (status, label) tuple for an env's credentials.

    status: "configured" | "partial" | "missing"
    label: human-readable summary, e.g. "SR + Flink" or "Using global key".
    """
    sr_endpoint = st.session_state.get(f"sr_endpoint_{env_id}", "").strip()
    sr_key = st.session_state.get(f"sr_key_{env_id}", "")
    sr_secret = st.session_state.get(f"sr_secret_{env_id}", "")
    flink_key = st.session_state.get(f"flink_key_{env_id}", "")
    flink_secret = st.session_state.get(f"flink_secret_{env_id}", "")

    sr_set = bool(sr_endpoint and sr_key and sr_secret)
    flink_set = bool(flink_key and flink_secret)

    parts = []
    if sr_set:
        parts.append("SR")
    if has_flink and flink_set:
        parts.append("Flink")

    if parts:
        return "configured", " + ".join(parts)
    return "missing", "Using global key"


def cluster_creds_status(cluster_id: str) -> tuple[str, str]:
    """Return (status, label) for a cluster's credentials."""
    key = st.session_state.get(f"cluster_key_{cluster_id}", "")
    secret = st.session_state.get(f"cluster_secret_{cluster_id}", "")
    if key and secret:
        return "configured", "Cluster key set"
    return "missing", "Using global key"


@st.dialog("Environment credentials")
def env_credentials_dialog(env_id: str, env_name: str, has_flink: bool) -> None:
    """Modal for one environment's SR + Flink credentials.

    Inputs are bound to the same session-state keys as the legacy expander
    (`sr_endpoint_{eid}`, `sr_key_{eid}`, etc.) so the rest of the
    extraction pipeline reads them unchanged.
    """
    _seed_env_state(env_id)

    st.caption(
        f"Credentials for **{env_name}** (`{env_id}`). "
        "Leave any field blank to fall back to global keys from `.env`."
    )

    st.markdown("**Schema Registry**")
    st.text_input(
        "Endpoint",
        key=f"sr_endpoint_{env_id}",
        placeholder="https://psrc-xxxxx.region.cloud.confluent.cloud",
    )
    c1, c2 = st.columns(2)
    with c1:
        st.text_input("Key", key=f"sr_key_{env_id}", type="password")
    with c2:
        st.text_input("Secret", key=f"sr_secret_{env_id}", type="password")

    st.markdown("**Flink**")
    if not has_flink:
        st.caption("No Flink compute pool discovered for this environment.")
    c3, c4 = st.columns(2)
    with c3:
        st.text_input(
            "Flink Key",
            key=f"flink_key_{env_id}",
            type="password",
            disabled=not has_flink,
        )
    with c4:
        st.text_input(
            "Flink Secret",
            key=f"flink_secret_{env_id}",
            type="password",
            disabled=not has_flink,
        )

    st.divider()
    bcol1, bcol2 = st.columns([1, 1])
    with bcol1:
        if st.button(
            "Clear all",
            key=f"clear_env_creds_{env_id}",
            type="secondary",
            width="stretch",
        ):
            for suffix in ("sr_endpoint", "sr_key", "sr_secret", "flink_key", "flink_secret"):
                st.session_state[f"{suffix}_{env_id}"] = ""
            st.rerun()
    with bcol2:
        if st.button(
            "Done",
            key=f"close_env_creds_{env_id}",
            type="primary",
            width="stretch",
        ):
            st.rerun()


@st.dialog("Cluster credentials")
def cluster_credentials_dialog(cluster_id: str, cluster_name: str) -> None:
    """Modal for one cluster's API key + secret."""
    _seed_cluster_state(cluster_id)

    st.caption(
        f"Optional cluster-scoped key for **{cluster_name}** (`{cluster_id}`). "
        "Leave blank to use the global key."
    )

    c1, c2 = st.columns(2)
    with c1:
        st.text_input("API Key", key=f"cluster_key_{cluster_id}", type="password")
    with c2:
        st.text_input("API Secret", key=f"cluster_secret_{cluster_id}", type="password")

    st.divider()
    bcol1, bcol2 = st.columns([1, 1])
    with bcol1:
        if st.button(
            "Clear",
            key=f"clear_cluster_creds_{cluster_id}",
            type="secondary",
            width="stretch",
        ):
            st.session_state[f"cluster_key_{cluster_id}"] = ""
            st.session_state[f"cluster_secret_{cluster_id}"] = ""
            st.rerun()
    with bcol2:
        if st.button(
            "Done",
            key=f"close_cluster_creds_{cluster_id}",
            type="primary",
            width="stretch",
        ):
            st.rerun()


def render_env_credentials_row(env: Any, has_flink: bool) -> None:
    """Inline row for one env: pill + 'Manage' button. Opens dialog on click."""
    status, label = env_creds_status(env.id, has_flink)
    pill = _status_pill_html(status, label)

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown(pill, unsafe_allow_html=True)
    with c2:
        if st.button(
            "Manage",
            key=f"manage_env_creds_btn_{env.id}",
            width="stretch",
        ):
            env_credentials_dialog(env.id, env.display_name, has_flink)


def render_cluster_credentials_row(cluster: Any) -> None:
    """Inline row for one cluster: pill + 'Manage' button."""
    status, label = cluster_creds_status(cluster.id)
    pill = _status_pill_html(status, label)

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown(
            f"<div class='creds-row-name'>{cluster.display_name}</div>{pill}",
            unsafe_allow_html=True,
        )
    with c2:
        if st.button(
            "Manage",
            key=f"manage_cluster_creds_btn_{cluster.id}",
            width="stretch",
        ):
            cluster_credentials_dialog(cluster.id, cluster.display_name)


def _status_pill_html(status: str, label: str) -> str:
    """Small visual pill: green = configured, grey = missing/global."""
    css_class = "creds-pill-configured" if status == "configured" else "creds-pill-missing"
    dot_color = "#4CAF50" if status == "configured" else "#9E9E9E"
    return (
        f"<div class='creds-pill {css_class}'>"
        f"<span class='status-dot' style='background:{dot_color}'></span>"
        f"<span>{label}</span>"
        f"</div>"
    )
