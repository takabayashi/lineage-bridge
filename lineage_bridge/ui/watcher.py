# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Change-detection watcher UI components — Phase 2G.

The UI no longer holds a `WatcherEngine`. It holds a `watcher_id` (a string)
and polls `GET /api/v1/watcher/{id}/{status,events,history}` for state.
Restart Streamlit, the watcher keeps running on the API process; multiple
Streamlit instances see the same state.

Two failure modes the UI has to handle gracefully:
  - API not reachable (uvicorn isn't running) — show a one-time hint
  - The recorded watcher_id no longer exists on the API (process was
    restarted with `memory` storage, watcher was dropped) — clear the
    session state and let the user start a new one.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import streamlit as st

from lineage_bridge.services.watcher_models import WatcherState
from lineage_bridge.ui.discovery import _try_load_settings

logger = logging.getLogger(__name__)


_STATE_COLORS = {
    WatcherState.STOPPED: ("#9E9E9E", "rgba(158,158,158,0.1)", "Stopped"),
    WatcherState.WATCHING: ("#4CAF50", "rgba(76,175,80,0.1)", "Watching"),
    WatcherState.COOLDOWN: ("#FF9800", "rgba(255,152,0,0.1)", "Cooldown"),
    WatcherState.EXTRACTING: ("#2196F3", "rgba(33,150,243,0.1)", "Extracting"),
}

_METHOD_LABELS = {
    "poll.topics.Changed": "Topics Changed",
    "poll.connectors.Changed": "Connectors Changed",
    "poll.ksqldb_queries.Changed": "ksqlDB Changed",
    "poll.flink_statements.Changed": "Flink Statements Changed",
    "kafka.CreateTopics": "Create Topics",
    "kafka.DeleteTopics": "Delete Topics",
    "kafka.IncrementalAlterConfigs": "Alter Configs",
    "connect.CreateConnector": "Create Connector",
    "connect.DeleteConnector": "Delete Connector",
    "connect.AlterConnector": "Alter Connector",
    "connect.PauseConnector": "Pause Connector",
    "connect.ResumeConnector": "Resume Connector",
}


def _friendly_method(method: str) -> str:
    return _METHOD_LABELS.get(method, method)


def _api_url() -> str:
    """Resolve the API base URL from Settings (falls back to local default)."""
    settings = _try_load_settings()
    if settings is not None and getattr(settings, "api_url", None):
        return settings.api_url.rstrip("/")
    return "http://127.0.0.1:8000"


def _api_headers() -> dict[str, str]:
    """Auth header to send when `Settings.api_key` is configured.

    Without this, a deployment that turns on `LINEAGE_BRIDGE_API_KEY` would
    have the UI's watcher controls all 401 — the API guards them via the
    same `require_api_key` Depends as every other route.
    """
    settings = _try_load_settings()
    api_key = getattr(settings, "api_key", None) if settings else None
    return {"X-API-Key": api_key} if api_key else {}


def _api_get(path: str) -> dict[str, Any] | None:
    """GET against the API. Returns parsed JSON, or None on connection / 404."""
    url = f"{_api_url()}{path}"
    try:
        resp = httpx.get(url, headers=_api_headers(), timeout=5.0)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as exc:
        logger.debug("Watcher API GET %s failed: %s", path, exc)
        return None


def _api_post(path: str, json_body: dict | None = None) -> dict[str, Any] | None:
    url = f"{_api_url()}{path}"
    try:
        resp = httpx.post(url, json=json_body, headers=_api_headers(), timeout=10.0)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Watcher API POST %s failed: %s", path, exc)
        st.error(f"Watcher API call failed: {exc}")
        return None


def _fetch_status(watcher_id: str) -> dict[str, Any] | None:
    return _api_get(f"/api/v1/watcher/{watcher_id}/status")


def _fetch_events(watcher_id: str, limit: int = 100) -> list[dict[str, Any]]:
    payload = _api_get(f"/api/v1/watcher/{watcher_id}/events?limit={limit}")
    return payload.get("events", []) if payload else []


def _fetch_history(watcher_id: str, limit: int = 50) -> list[dict[str, Any]]:
    payload = _api_get(f"/api/v1/watcher/{watcher_id}/history?limit={limit}")
    return payload.get("extractions", []) if payload else []


def render_watcher_controls() -> None:
    """Render watcher controls in the main area (horizontal layout)."""
    watcher_id: str | None = st.session_state.get("watcher_id")
    status = _fetch_status(watcher_id) if watcher_id else None
    is_running = status is not None and status.get("state") not in (None, "stopped")

    if watcher_id and status is None:
        # The watcher disappeared (API restarted with memory backend, or someone
        # deregistered it). Clear the session-state pointer.
        st.session_state.pop("watcher_id", None)

    c_controls, _, c_right = st.columns([2, 3, 2])

    with c_controls:
        mode = st.radio(
            "Detection mode",
            ["Audit Log", "REST Polling"],
            horizontal=True,
            key="watcher_mode",
            disabled=is_running,
            help=(
                "**Audit Log**: real-time Kafka consumer (requires audit-log "
                "credentials). **REST Polling**: REST state-diffing (no extra creds)."
            ),
        )

        bootstrap_servers = ""
        audit_key = ""
        audit_secret = ""
        poll_interval = 10

        if mode == "Audit Log":
            settings = _try_load_settings()

            def _setting(name: str) -> str:
                return (getattr(settings, name, "") or "") if settings else ""

            default_bs = _setting("audit_log_bootstrap_servers")
            default_key = _setting("audit_log_api_key")
            default_secret = _setting("audit_log_api_secret")
            bootstrap_servers = st.text_input(
                "Bootstrap servers",
                value=default_bs,
                key="watcher_audit_bootstrap",
                disabled=is_running,
                placeholder="pkc-xxxxx.region.cloud.confluent.cloud:9092",
            )
            ak1, ak2 = st.columns(2)
            with ak1:
                audit_key = st.text_input(
                    "API key", value=default_key,
                    key="watcher_audit_key", type="password", disabled=is_running,
                )
            with ak2:
                audit_secret = st.text_input(
                    "API secret", value=default_secret,
                    key="watcher_audit_secret", type="password", disabled=is_running,
                )
        else:
            poll_interval = st.number_input(
                "Poll interval (s)",
                min_value=5, max_value=120, value=10, step=5,
                key="watcher_poll_interval", disabled=is_running,
            )

        cooldown = st.number_input(
            "Cooldown (s)", min_value=5, max_value=300, value=30, step=5,
            key="watcher_cooldown", disabled=is_running,
        )

        p1, p2 = st.columns(2)
        with p1:
            push_uc = st.checkbox("Push UC", key="watcher_push_uc", disabled=is_running)
        with p2:
            push_glue = st.checkbox("Push Glue", key="watcher_push_glue", disabled=is_running)
        p3, p4 = st.columns(2)
        with p3:
            push_google = st.checkbox(
                "Push Google", key="watcher_push_google", disabled=is_running
            )
        with p4:
            push_datazone = st.checkbox(
                "Push DataZone", key="watcher_push_datazone", disabled=is_running
            )

    with c_right:
        if status is not None:
            _render_status_badge(status)
            cooldown_remaining = float(status.get("cooldown_remaining_seconds", 0.0) or 0.0)
            if cooldown_remaining > 0:
                progress = 1.0 - (cooldown_remaining / max(cooldown, 1))
                st.progress(min(progress, 1.0), text=f"Cooldown: {cooldown_remaining:.0f}s")
            last_poll = status.get("last_poll_at")
            poll_count = status.get("poll_count", 0)
            if last_poll:
                st.caption(f"Last poll: {last_poll[11:19]} ({poll_count} total)")

        if is_running:
            if st.button(
                "Stop Watcher", key="watcher_stop_btn", width="stretch", type="secondary",
            ):
                _api_post(f"/api/v1/watcher/{watcher_id}/stop")
                st.session_state.pop("watcher_id", None)
                st.rerun()
        else:
            has_params = st.session_state.get("last_extraction_params")
            can_start = bool(has_params)
            start_help = ""
            if mode == "Audit Log" and not (bootstrap_servers and audit_key and audit_secret):
                can_start = False
                start_help = "Fill in audit-log credentials"

            if st.button(
                "Start Watcher", key="watcher_start_btn", width="stretch",
                type="primary", disabled=not can_start,
            ):
                _start_watcher_via_api(
                    mode=mode,
                    poll_interval=poll_interval,
                    cooldown=cooldown,
                    push_uc=push_uc,
                    push_glue=push_glue,
                    push_google=push_google,
                    push_datazone=push_datazone,
                    audit_bootstrap=bootstrap_servers,
                    audit_key=audit_key,
                    audit_secret=audit_secret,
                )
                st.rerun()

            if not has_params:
                st.caption("Run an extraction first")
            elif start_help:
                st.caption(start_help)


def render_watcher_log() -> None:
    """Render the watcher event log and extraction history (auto-refreshing)."""
    watcher_id: str | None = st.session_state.get("watcher_id")
    if not watcher_id:
        st.info(
            "Configure and start the Change Watcher above to monitor "
            "Confluent Cloud for lineage-relevant changes."
        )
        return
    _render_watcher_feed(watcher_id)


@st.fragment(run_every=5)
def _render_watcher_feed(watcher_id: str) -> None:
    """Fragment that polls /events + /history every 5s without rerunning the page."""
    status = _fetch_status(watcher_id)
    if status is None:
        st.warning("Watcher disappeared — restart it from the controls above.")
        return

    events = _fetch_events(watcher_id, limit=200)
    st.subheader(f"Event Feed ({len(events)} events)")
    if events:
        rows = []
        for e in events:
            time_str = (e.get("time") or "")[11:19]
            rows.append(
                {
                    "Time": time_str,
                    "Change": _friendly_method(e.get("method_name", "")),
                    "Resource": e.get("resource_name", ""),
                    "Environment": e.get("environment_id") or "",
                    "Cluster": e.get("cluster_id") or "",
                    "ID": e.get("id", ""),
                }
            )
        st.dataframe(rows, width="stretch", hide_index=True, height=min(400, 35 + 35 * len(rows)))
    elif status.get("state") != "stopped":
        polls = status.get("poll_count", 0)
        last = (status.get("last_poll_at") or "")[11:19] or "—"
        st.info(f"Polling — {polls} polls completed, last at {last}. No changes detected yet.")

    history = _fetch_history(watcher_id, limit=50)
    if history:
        st.subheader(f"Extraction History ({len(history)} runs)")
        rows = []
        for r in history:
            triggered_at = r.get("triggered_at") or ""
            completed_at = r.get("completed_at") or ""
            duration = ""
            if triggered_at and completed_at:
                from datetime import datetime as _dt

                try:
                    dt0 = _dt.fromisoformat(triggered_at)
                    dt1 = _dt.fromisoformat(completed_at)
                    duration = f"{(dt1 - dt0).total_seconds():.1f}s"
                except Exception:
                    pass
            rows.append(
                {
                    "Time": triggered_at[11:19],
                    "Status": "Failed" if r.get("error") else "OK",
                    "Nodes": r.get("node_count", 0),
                    "Edges": r.get("edge_count", 0),
                    "Duration": duration,
                    "Triggers": r.get("trigger_event_count", 0),
                    "Error": r.get("error") or "",
                }
            )
        st.dataframe(rows, width="stretch", hide_index=True, height=min(300, 35 + 35 * len(rows)))


def _render_status_badge(status: dict[str, Any]) -> None:
    raw_state = status.get("state", "stopped")
    try:
        state = WatcherState(raw_state)
    except ValueError:
        state = WatcherState.STOPPED
    dot_color, bg_color, label = _STATE_COLORS.get(
        state, ("#9E9E9E", "rgba(158,158,158,0.1)", "Unknown")
    )
    is_active = state in (WatcherState.WATCHING, WatcherState.COOLDOWN)
    pulse_css = ""
    if is_active:
        pulse_css = (
            f"animation: watcher-pulse 2s ease-in-out infinite;"
            f"box-shadow: 0 0 0 0 {dot_color}80;"
        )
    poll_count = status.get("poll_count", 0)
    event_count = status.get("event_count", 0)
    parts = [label]
    if poll_count:
        parts.append(f"{poll_count} polls")
    if event_count:
        parts.append(f"{event_count} events")
    detail_html = f" &mdash; {' &mdash; '.join(parts[1:])}" if len(parts) > 1 else ""

    st.markdown(
        "<style>"
        "@keyframes watcher-pulse {"
        "  0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }"
        "</style>"
        f"<div class='status-badge' style='background:{bg_color};"
        f"border:1px solid {dot_color}33;'>"
        f"<span class='status-dot' style='background:{dot_color};{pulse_css}'></span>"
        f"{label}{detail_html}"
        "</div>",
        unsafe_allow_html=True,
    )


def _start_watcher_via_api(
    *,
    mode: str,
    poll_interval: float,
    cooldown: float,
    push_uc: bool,
    push_glue: bool,
    push_google: bool,
    push_datazone: bool,
    audit_bootstrap: str = "",
    audit_key: str = "",
    audit_secret: str = "",
) -> None:
    last_params = st.session_state.get("last_extraction_params") or {}
    config_body = {
        "mode": "audit_log" if mode == "Audit Log" else "rest_polling",
        "poll_interval_seconds": poll_interval,
        "cooldown_seconds": cooldown,
        "audit_log_bootstrap_servers": audit_bootstrap or None,
        "audit_log_api_key": audit_key or None,
        "audit_log_api_secret": audit_secret or None,
        "extraction": {
            "environment_ids": last_params.get("env_ids", []),
            "cluster_ids": last_params.get("cluster_ids"),
            "enable_connect": last_params.get("enable_connect", True),
            "enable_ksqldb": last_params.get("enable_ksqldb", True),
            "enable_flink": last_params.get("enable_flink", True),
            "enable_schema_registry": last_params.get("enable_schema_registry", True),
            "enable_stream_catalog": last_params.get("enable_stream_catalog", False),
            "enable_tableflow": last_params.get("enable_tableflow", True),
            "enable_enrichment": last_params.get("enable_enrichment", True),
        },
        "push_databricks_uc": push_uc,
        "push_aws_glue": push_glue,
        "push_google": push_google,
        "push_datazone": push_datazone,
    }
    response = _api_post("/api/v1/watcher", json_body=config_body)
    if response and response.get("watcher_id"):
        st.session_state["watcher_id"] = response["watcher_id"]
