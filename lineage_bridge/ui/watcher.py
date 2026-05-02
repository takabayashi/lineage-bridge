# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Change-detection watcher UI components.

Phase C redesign:
- REST Polling is the default detection mode; Audit Log is gated behind a
  "Show advanced" expander (it requires audit-log-cluster credentials that
  almost no user has, and silently fails without them — see ADR-014).
- Two-column layout (was three with an empty middle column).
- Event feed renders as a card list instead of a `st.dataframe` with a
  UUID-ish ID column nobody reads.
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.discovery import _try_load_settings
from lineage_bridge.watcher.engine import WatcherEngine, WatcherState

_STATE_COLORS = {
    WatcherState.STOPPED: ("#9E9E9E", "rgba(158,158,158,0.1)", "Stopped"),
    WatcherState.WATCHING: ("#4CAF50", "rgba(76,175,80,0.1)", "Watching"),
    WatcherState.COOLDOWN: ("#FF9800", "rgba(255,152,0,0.1)", "Cooldown"),
    WatcherState.EXTRACTING: ("#2196F3", "rgba(33,150,243,0.1)", "Extracting"),
}

_METHOD_LABELS = {
    "poll.topics.Changed": ("Topics", "•"),
    "poll.connectors.Changed": ("Connectors", "•"),
    "poll.ksqldb_queries.Changed": ("ksqlDB", "•"),
    "poll.flink_statements.Changed": ("Flink", "•"),
    "kafka.CreateTopics": ("Create topic", "+"),
    "kafka.DeleteTopics": ("Delete topic", "✕"),
    "kafka.IncrementalAlterConfigs": ("Alter config", "·"),
    "connect.CreateConnector": ("Create connector", "+"),
    "connect.DeleteConnector": ("Delete connector", "✕"),
    "connect.AlterConnector": ("Alter connector", "·"),
    "connect.PauseConnector": ("Pause connector", "⏸"),
    "connect.ResumeConnector": ("Resume connector", "▶"),
}


def _friendly_method(method: str) -> tuple[str, str]:
    """Return (label, icon) for a watcher event method name."""
    if method in _METHOD_LABELS:
        return _METHOD_LABELS[method]
    return method, "•"


def watcher_event_count() -> int:
    """Return the current number of pending watcher events (for tab badge)."""
    engine: WatcherEngine | None = st.session_state.get("watcher_engine")
    if engine is None:
        return 0
    return len(engine.event_feed)


def render_watcher_controls() -> None:
    """Watcher controls — REST Polling default, Audit Log behind 'advanced'."""
    engine: WatcherEngine | None = st.session_state.get("watcher_engine")
    is_running = engine is not None and engine.is_running

    # Two-column layout (no dead middle column)
    c_controls, c_right = st.columns([3, 2])

    audit_creds: tuple[str, str, str] | None = None

    with c_controls:
        # ── Mode (REST Polling default) ────────────────────────────
        use_audit = st.toggle(
            "Use audit log (advanced)",
            key="watcher_use_audit",
            value=False,
            disabled=is_running,
            help=(
                "Default: REST polling (no extra credentials). "
                "Enable this only if you have audit-log cluster credentials — "
                "see ADR-014. Without those credentials the audit-log mode "
                "fails silently when started."
            ),
        )

        if use_audit:
            settings = _try_load_settings()
            default_bs = (
                getattr(settings, "audit_log_bootstrap_servers", "") or "" if settings else ""
            )
            default_key = getattr(settings, "audit_log_api_key", "") or "" if settings else ""
            default_secret = getattr(settings, "audit_log_api_secret", "") or "" if settings else ""

            with st.expander("Audit log credentials", expanded=True):
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
                        "API key",
                        value=default_key,
                        key="watcher_audit_key",
                        disabled=is_running,
                        type="password",
                    )
                with ak2:
                    audit_secret = st.text_input(
                        "API secret",
                        value=default_secret,
                        key="watcher_audit_secret",
                        disabled=is_running,
                        type="password",
                    )
            audit_creds = (bootstrap_servers, audit_key, audit_secret)
            poll_interval = 10
        else:
            poll_interval = st.number_input(
                "Poll interval (s)",
                min_value=5,
                max_value=120,
                value=10,
                step=5,
                key="watcher_poll_interval",
                disabled=is_running,
            )

        cooldown = st.number_input(
            "Cooldown (s)",
            min_value=5,
            max_value=300,
            value=30,
            step=5,
            key="watcher_cooldown",
            disabled=is_running,
        )

        st.caption("After change → publish to:")
        p1, p2 = st.columns(2)
        with p1:
            push_uc = st.checkbox("Push UC", key="watcher_push_uc", disabled=is_running)
        with p2:
            push_glue = st.checkbox("Push Glue", key="watcher_push_glue", disabled=is_running)

    with c_right:
        if engine is not None:
            _render_status_badge(engine, mode="Audit Log" if use_audit else "REST Polling")

            if engine.state == WatcherState.COOLDOWN:
                remaining = engine.cooldown_remaining
                progress = 1.0 - (remaining / engine.cooldown_seconds)
                st.progress(min(progress, 1.0), text=f"Cooldown: {remaining:.0f}s")

            if getattr(engine, "last_poll_time", None):
                st.caption(
                    f"Last poll: {engine.last_poll_time.strftime('%H:%M:%S')} "
                    f"({engine.poll_count} total)"
                )

        if is_running:
            if st.button(
                "Stop watcher",
                key="watcher_stop_btn",
                width="stretch",
                type="secondary",
            ):
                engine.stop()  # type: ignore[union-attr]
                st.rerun()
        else:
            has_params = st.session_state.get("last_extraction_params")
            can_start = bool(has_params)
            start_help = ""
            if use_audit:
                bs, ak, asec = audit_creds or ("", "", "")
                if not (bs and ak and asec):
                    can_start = False
                    start_help = "Fill audit log credentials"

            if st.button(
                "Start watcher",
                key="watcher_start_btn",
                width="stretch",
                type="primary",
                disabled=not can_start,
            ):
                if use_audit and audit_creds is not None:
                    bs, ak, asec = audit_creds
                    _start_watcher(
                        poll_interval=10,
                        cooldown=cooldown,
                        push_uc=push_uc,
                        push_glue=push_glue,
                        audit_bootstrap=bs,
                        audit_key=ak,
                        audit_secret=asec,
                    )
                else:
                    _start_watcher(
                        poll_interval=poll_interval,
                        cooldown=cooldown,
                        push_uc=push_uc,
                        push_glue=push_glue,
                    )
                st.rerun()

            if not has_params:
                st.caption("Run an extraction first")
            elif start_help:
                st.caption(start_help)


def render_watcher_log() -> None:
    """Render the watcher event log and extraction history."""
    engine: WatcherEngine | None = st.session_state.get("watcher_engine")
    if engine is None:
        st.info(
            "Configure and start the change watcher above to monitor "
            "Confluent Cloud for lineage-relevant changes."
        )
        return

    # Update the displayed graph if the watcher produced a new one
    if getattr(engine, "last_graph", None) is not None:
        prev = st.session_state.get("_watcher_graph_id")
        current_id = id(engine.last_graph)
        if prev != current_id:
            st.session_state.graph = engine.last_graph
            st.session_state._watcher_graph_id = current_id
            st.session_state._clear_positions = True
            st.session_state.selected_node = None
            st.session_state.focus_node = None

    _render_watcher_feed(engine)


@st.fragment(run_every=5)
def _render_watcher_feed(engine: WatcherEngine) -> None:
    """Auto-refreshing fragment showing event feed and extraction history."""
    events = list(engine.event_feed)
    st.subheader(f"Event feed ({len(events)} events)")

    if events:
        # Card list — drops the unreadable UUID ID column the dataframe had
        for event in reversed(events[-25:]):
            label, icon = _friendly_method(event.method_name)
            scope_parts = []
            if event.environment_id:
                scope_parts.append(event.environment_id)
            if event.cluster_id:
                scope_parts.append(event.cluster_id)
            scope = " · ".join(scope_parts) if scope_parts else ""
            st.markdown(
                f"<div class='watcher-event'>"
                f"<span class='watcher-event-time'>{event.time.strftime('%H:%M:%S')}</span>"
                f"<span class='watcher-event-icon'>{icon}</span>"
                f"<span class='watcher-event-label'>{label}</span>"
                f"<span class='watcher-event-resource'>{event.resource_name}</span>"
                f"<span class='watcher-event-scope'>{scope}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        if len(events) > 25:
            st.caption(f"Showing 25 most recent of {len(events)} events.")
    elif engine.is_running:
        poll_count = getattr(engine, "poll_count", 0)
        if poll_count > 0:
            last = getattr(engine, "last_poll_time", None)
            ts = last.strftime("%H:%M:%S") if last else "—"
            st.info(
                f"Polling every {engine.poll_interval:.0f}s — "
                f"{poll_count} polls completed, last at {ts}. "
                "No changes detected yet."
            )
        else:
            st.info("Starting poller... waiting for first poll.")

    if engine.extraction_history:
        st.subheader(f"Extraction history ({len(engine.extraction_history)} runs)")
        history_rows = []
        for record in reversed(engine.extraction_history):
            dur = ""
            if record.completed_at:
                secs = (record.completed_at - record.triggered_at).total_seconds()
                dur = f"{secs:.1f}s"
            triggers = ", ".join(_friendly_method(e.method_name)[0] for e in record.trigger_events)
            history_rows.append(
                {
                    "Time": record.triggered_at.strftime("%H:%M:%S"),
                    "Status": "Failed" if record.error else "OK",
                    "Nodes": record.node_count,
                    "Edges": record.edge_count,
                    "Duration": dur,
                    "Triggers": triggers,
                    "Error": record.error or "",
                }
            )
        st.dataframe(
            history_rows,
            width="stretch",
            hide_index=True,
            height=min(300, 35 + 35 * len(history_rows)),
        )


def _render_status_badge(engine: WatcherEngine, mode: str) -> None:
    """Render a colored status badge with pulse animation when active."""
    dot_color, bg_color, label = _STATE_COLORS.get(
        engine.state,
        ("#9E9E9E", "rgba(158,158,158,0.1)", "Unknown"),
    )
    is_active = engine.state in (WatcherState.WATCHING, WatcherState.COOLDOWN)
    pulse_css = ""
    if is_active:
        pulse_css = (
            f"animation: watcher-pulse 2s ease-in-out infinite;box-shadow: 0 0 0 0 {dot_color}80;"
        )

    event_count = len(engine.event_feed)
    poll_count = getattr(engine, "poll_count", 0)
    parts = [label, mode]
    if poll_count > 0:
        parts.append(f"{poll_count} polls")
    if event_count:
        parts.append(f"{event_count} events")
    detail = " · ".join(parts[1:])
    detail_html = f" &mdash; {detail}" if detail else ""

    st.markdown(
        "<style>"
        "@keyframes watcher-pulse {"
        "  0% { opacity: 1; }"
        "  50% { opacity: 0.4; }"
        "  100% { opacity: 1; }"
        "}"
        "</style>"
        f"<div class='status-badge' style='background:{bg_color};"
        f"border:1px solid {dot_color}33;'>"
        f"<span class='status-dot' style='background:{dot_color};{pulse_css}'>"
        f"</span>"
        f"{label}{detail_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def _start_watcher(
    poll_interval: float,
    cooldown: float,
    push_uc: bool,
    push_glue: bool,
    audit_bootstrap: str | None = None,
    audit_key: str | None = None,
    audit_secret: str | None = None,
) -> None:
    """Create and start a WatcherEngine, store in session state."""
    settings = _try_load_settings()
    if not settings:
        st.error("Settings not loaded")
        return

    if audit_bootstrap and audit_key and audit_secret:
        settings.audit_log_bootstrap_servers = audit_bootstrap
        settings.audit_log_api_key = audit_key
        settings.audit_log_api_secret = audit_secret

    last_params = st.session_state.get("last_extraction_params", {})
    extraction_params = {
        "env_ids": last_params.get("env_ids", []),
        "cluster_ids": last_params.get("cluster_ids"),
        "enable_connect": last_params.get("enable_connect", True),
        "enable_ksqldb": last_params.get("enable_ksqldb", True),
        "enable_flink": last_params.get("enable_flink", True),
        "enable_schema_registry": last_params.get("enable_schema_registry", True),
        "enable_stream_catalog": last_params.get("enable_stream_catalog", False),
        "enable_tableflow": last_params.get("enable_tableflow", True),
        "enable_enrichment": last_params.get("enable_enrichment", True),
        "push_uc": push_uc,
        "push_glue": push_glue,
    }

    engine = WatcherEngine(
        settings=settings,
        extraction_params=extraction_params,
        cooldown_seconds=cooldown,
        poll_interval=poll_interval,
    )

    try:
        engine.start()
        st.session_state["watcher_engine"] = engine
    except ValueError as exc:
        st.error(str(exc))
