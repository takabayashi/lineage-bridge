# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — connect / disconnect widgets + loaded-credentials summary.

Phase A redesign: the connection-status badge at the sidebar root (in
`__init__.py`) is the single source of truth for connection state.

Credentials summary: a compact list of which integrations have credentials
loaded (Confluent / Databricks / AWS / GCP), shown in both connected and
disconnected states so the user can verify what's wired up before clicking
Push or Connect. Reads from `_try_load_settings()` so it picks up both
.env values and the encrypted local cache (populated by `make demo-up`).
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from lineage_bridge.ui.discovery import _make_cloud_client, _run_async, _try_load_settings


def _mask(value: str | None, head: int = 4, tail: int = 4) -> str:
    """Return a masked representation of a secret-ish value."""
    if not value:
        return ""
    if len(value) <= head + tail:
        return "•" * len(value)
    return f"{value[:head]}…{value[-tail:]}"


def _credential_row(name: str, configured: bool, detail: str) -> str:
    """One row of the credentials summary — a status dot + label + detail."""
    color = "#4CAF50" if configured else "#9E9E9E"
    state_text = "loaded" if configured else "not set"
    pill_class = "creds-pill-explicit" if configured else "creds-pill-missing"
    return (
        f"<div class='creds-loaded-row'>"
        f"<span class='status-dot' style='background:{color}'></span>"
        f"<span class='creds-loaded-name'>{name}</span>"
        f"<span class='creds-pill {pill_class}'>{state_text}</span>"
        f"<span class='creds-loaded-detail'>{detail}</span>"
        f"</div>"
    )


def _render_credentials_summary(settings: Any | None) -> None:
    """Render the loaded-credentials summary block.

    Always rendered — gives the user visibility into which integrations
    are wired up, regardless of connection state. Without this, after the
    Connect button is clicked there's no UI signal that Databricks / AWS /
    GCP credentials were picked up from .env or the encrypted cache.
    """
    if settings is None:
        return

    rows: list[str] = []

    # Confluent Cloud — required for connect
    cc_key = getattr(settings, "confluent_cloud_api_key", None)
    rows.append(
        _credential_row(
            "Confluent Cloud",
            configured=bool(cc_key),
            detail=f"`{_mask(cc_key)}`" if cc_key else "Set CONFLUENT_CLOUD_API_KEY",
        )
    )

    # Databricks — workspace URL is the lead signal; token is required for push
    db_url = getattr(settings, "databricks_workspace_url", None)
    db_token = getattr(settings, "databricks_token", None)
    if db_url:
        host = db_url.replace("https://", "").replace("http://", "").rstrip("/")
        token_part = f" · token `{_mask(db_token)}`" if db_token else " · token missing"
        rows.append(_credential_row("Databricks UC", True, f"{host}{token_part}"))
    else:
        rows.append(_credential_row("Databricks UC", False, "Set DATABRICKS_WORKSPACE_URL"))

    # AWS — region is always set (defaults to us-east-1); flag DataZone separately
    region = getattr(settings, "aws_region", None) or ""
    dz_domain = getattr(settings, "aws_datazone_domain_id", None)
    rows.append(_credential_row("AWS Glue", True, f"region `{region}`"))
    if dz_domain:
        rows.append(_credential_row("AWS DataZone", True, f"domain `{dz_domain}`"))

    # GCP — project is the only required field for BigQuery push
    gcp_project = getattr(settings, "gcp_project_id", None)
    if gcp_project:
        rows.append(_credential_row("Google BigQuery", True, f"project `{gcp_project}`"))

    # Use st.popover (not st.expander) — this code runs inside the parent
    # Setup expander in `__init__.py`, and Streamlit forbids nesting
    # expanders. Popovers nest fine.
    with st.popover("Credentials loaded", use_container_width=True):
        st.markdown("".join(rows), unsafe_allow_html=True)
        st.caption(
            "Sources: `.env` (LINEAGE_BRIDGE_*) plus the local encrypted cache "
            "populated by `make demo-up`."
        )


def _render_sidebar_connection() -> None:
    """Connection controls + always-visible loaded-credentials summary."""
    settings = _try_load_settings()

    if st.session_state.connected:
        if st.button(
            "Disconnect",
            key="disconnect_btn",
            type="secondary",
            width="stretch",
        ):
            # Clear connection + extraction state. Do NOT wipe the
            # `_cached_*_creds` dicts — those mirror the on-disk encrypted
            # cache and are meant to survive disconnect/reconnect cycles
            # so the Manage Credentials dialog can still pre-fill from them.
            st.session_state.connected = False
            st.session_state.environments = []
            st.session_state.env_cache = {}
            st.session_state.graph = None
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.session_state.last_extraction_params = None
            st.session_state.extraction_log = []
            st.session_state.push_log = []
            st.rerun()
        # Even when connected, surface the loaded-credentials summary so the
        # user can verify Databricks / AWS / GCP creds were picked up.
        _render_credentials_summary(settings)
        return

    if settings:
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
        _render_credentials_summary(settings)
    else:
        st.warning("No credentials found.")
        st.code(
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...\n"
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...",
            language="bash",
        )
