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

from lineage_bridge.services.push_service import PUSH_PROVIDERS
from lineage_bridge.ui.discovery import _try_load_settings
from lineage_bridge.ui.sidebar.credentials import (
    _seed_audit_log_state,
    _status_pill_html,
    audit_log_credentials_dialog,
    audit_log_creds_status,
)
from lineage_bridge.watcher.engine import WatcherEngine, WatcherState

# Friendly checkbox labels for the "After change → publish to:" row.
# Sourced from the canonical PUSH_PROVIDERS so adding a new catalog in
# push_service.py automatically surfaces a watcher checkbox once we add
# the label here.
_PROVIDER_LABELS: dict[str, str] = {
    "databricks_uc": "UC",
    "aws_glue": "Glue",
    "google": "Dataplex",
    "datazone": "DataZone",
}

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
            # Seed inputs from the encrypted cache so the status pill +
            # downstream `_start_watcher` see persisted values without the
            # user having to open the dialog.
            _seed_audit_log_state()
            settings = _try_load_settings()
            status, label = audit_log_creds_status(settings)

            cred_col_l, cred_col_r = st.columns([3, 2])
            with cred_col_l:
                st.markdown(_status_pill_html(status, label), unsafe_allow_html=True)
            with cred_col_r:
                if st.button(
                    "Manage",
                    key="manage_audit_log_creds_btn",
                    width="stretch",
                    disabled=is_running,
                ):
                    audit_log_credentials_dialog()

            audit_creds = (
                (st.session_state.get("watcher_audit_bootstrap", "") or "").strip(),
                st.session_state.get("watcher_audit_key", "") or "",
                st.session_state.get("watcher_audit_secret", "") or "",
            )
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
        push_providers: list[str] = []
        provider_cols = st.columns(len(PUSH_PROVIDERS))
        for col, provider in zip(provider_cols, PUSH_PROVIDERS, strict=True):
            with col:
                if st.checkbox(
                    _PROVIDER_LABELS.get(provider, provider),
                    key=f"watcher_push_{provider}",
                    disabled=is_running,
                ):
                    push_providers.append(provider)

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
                # "missing" = neither cache nor .env has a usable bundle.
                # "global" is fine — _start_watcher leaves Settings alone
                # and the engine will pick up the .env values.
                audit_status, _ = audit_log_creds_status(_try_load_settings())
                if audit_status == "missing":
                    can_start = False
                    start_help = "Save audit log credentials first"

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
                        push_providers=push_providers,
                        mode="audit",
                        audit_bootstrap=bs,
                        audit_key=ak,
                        audit_secret=asec,
                    )
                else:
                    _start_watcher(
                        poll_interval=poll_interval,
                        cooldown=cooldown,
                        push_providers=push_providers,
                        mode="polling",
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
    push_providers: list[str],
    mode: str = "auto",
    audit_bootstrap: str | None = None,
    audit_key: str | None = None,
    audit_secret: str | None = None,
) -> None:
    """Create and start a WatcherEngine, store in session state."""
    from lineage_bridge.config.settings import ClusterCredential
    from lineage_bridge.watcher.engine import WatcherMode

    settings = _try_load_settings()
    if not settings:
        st.error("Settings not loaded")
        return

    if audit_bootstrap and audit_key and audit_secret:
        settings.audit_log_bootstrap_servers = audit_bootstrap
        settings.audit_log_api_key = audit_key
        settings.audit_log_api_secret = audit_secret

    last_params = st.session_state.get("last_extraction_params", {})

    # The sidebar's Extract action stashes per-cluster credentials in
    # last_extraction_params, but `run_extraction` reads cluster keys from
    # `Settings.cluster_credentials` — not from a kwarg. Merge in here so
    # the watcher hits clusters with the same key the foreground extract
    # used. Without this, a cluster whose key only exists in the encrypted
    # cache (or was typed into the sidebar dialog this session) 401s when
    # the watcher's first extraction runs.
    ui_cluster_creds = last_params.get("cluster_credentials") or {}
    if ui_cluster_creds:
        merged: dict[str, ClusterCredential] = dict(settings.cluster_credentials or {})
        for cid, cred in ui_cluster_creds.items():
            merged[cid] = ClusterCredential(
                api_key=cred.get("api_key", ""),
                api_secret=cred.get("api_secret", ""),
            )
        settings = settings.model_copy(update={"cluster_credentials": merged})

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
        "push_providers": list(push_providers),
        # Per-env overrides flow straight through to run_extraction kwargs.
        "sr_endpoints": last_params.get("sr_endpoints") or {},
        "sr_credentials": last_params.get("sr_credentials") or {},
        "flink_credentials": last_params.get("flink_credentials") or {},
    }

    engine = WatcherEngine(
        settings=settings,
        extraction_params=extraction_params,
        cooldown_seconds=cooldown,
        poll_interval=poll_interval,
        mode=WatcherMode(mode),
    )

    try:
        engine.start()
        st.session_state["watcher_engine"] = engine
    except ValueError as exc:
        st.error(str(exc))
