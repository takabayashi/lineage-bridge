# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — extract / enrich / publish controls + logs + load-data.

Phase A redesign:
- Single primary "Run extraction" button + popover with Enrich / Refresh
  (was: 3 adjacent buttons of overlapping semantics).
- Single Publish panel with one row per target (was: 3 nested expanders
  + 4 near-identical render functions).
- Split `extraction_log` and `push_log` into two session-state lists so
  pushing no longer clobbers the extraction log (was: shared list gated
  by `_log_source`).
- Removed duplicate "Load Demo Graph" button (kept only in empty state).

Backwards-compat note: the legacy session-state shape (`_cached_*`,
`extraction_log`, etc.) is otherwise preserved verbatim so the rest of
the UI doesn't need to change.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import streamlit as st

from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.discovery import _run_async, _try_load_settings
from lineage_bridge.ui.extraction import (
    _run_datazone_push,
    _run_enrichment_on_graph,
    _run_extraction_with_params,
    _run_glue_push,
    _run_google_push,
    _run_lineage_push,
    _save_selections_to_cache,
)
from lineage_bridge.ui.state import _GRAPH_VERSION

# ── extraction context ──────────────────────────────────────────────────


def _resolve_extraction_context():
    """Resolve selected environments, clusters, and credentials from UI state."""
    settings = _try_load_settings()
    if not settings:
        return None

    cache = st.session_state.env_cache
    all_envs = st.session_state.environments

    selected_env_label = st.session_state.get("env_select", "")
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_envs = [env_labels[selected_env_label]] if selected_env_label in env_labels else []

    all_cluster_options = {}
    for env in selected_envs:
        svc = cache[env.id]["services"]
        for c in svc.clusters:
            label = f"{c.display_name} ({c.id})"
            all_cluster_options[label] = c

    selected_cluster_labels = st.session_state.get("cluster_select", [])
    selected_cluster_ids = [
        all_cluster_options[lbl].id for lbl in selected_cluster_labels if lbl in all_cluster_options
    ]

    # Per-cluster credentials
    ui_cluster_creds: dict[str, dict[str, str]] = {}
    for lbl in selected_cluster_labels:
        if lbl not in all_cluster_options:
            continue
        cid = all_cluster_options[lbl].id
        k = st.session_state.get(f"cluster_key_{cid}", "")
        s = st.session_state.get(f"cluster_secret_{cid}", "")
        if k and s:
            ui_cluster_creds[cid] = {"api_key": k, "api_secret": s}

    # Per-environment SR credentials + endpoints
    ui_sr_creds: dict[str, dict[str, str]] = {}
    for env in selected_envs:
        eid = env.id
        endpoint = st.session_state.get(f"sr_endpoint_{eid}", "").strip()
        k = st.session_state.get(f"sr_key_{eid}", "")
        s = st.session_state.get(f"sr_secret_{eid}", "")
        cred: dict[str, str] = {}
        if endpoint:
            cred["endpoint"] = endpoint
        if k and s:
            cred["api_key"] = k
            cred["api_secret"] = s
        if cred:
            ui_sr_creds[eid] = cred

    # Per-environment Flink credentials
    ui_flink_creds: dict[str, dict[str, str]] = {}
    for env in selected_envs:
        eid = env.id
        k = st.session_state.get(f"flink_key_{eid}", "")
        s = st.session_state.get(f"flink_secret_{eid}", "")
        if k and s:
            ui_flink_creds[eid] = {"api_key": k, "api_secret": s}

    return {
        "settings": settings,
        "selected_envs": selected_envs,
        "selected_cluster_ids": selected_cluster_ids,
        "ui_cluster_creds": ui_cluster_creds,
        "ui_sr_creds": ui_sr_creds,
        "ui_flink_creds": ui_flink_creds,
    }


def _record_activity_alert(kind: str, message: str, log_key: str) -> None:
    """Persist a strong error/warning signal + fire a toast.

    The banner in `app.py` reads `_activity_alert` and renders a
    persistent `st.error` / `st.warning` until the user dismisses it.
    The activity-log drawer auto-expands when this flag is set so the
    user sees the offending entries without hunting for them.
    """
    import contextlib

    icon = "🚨" if kind == "error" else "⚠️"
    with contextlib.suppress(Exception):
        # st.toast may not be available in some Streamlit versions / test
        # harnesses; the persistent banner still fires regardless.
        st.toast(message, icon=icon)
    st.session_state._activity_alert = {"kind": kind, "message": message, "log": log_key}


def _post_action_alert(action_label: str, log_key: str) -> None:
    """Inspect *log_key*'s entries after an action and surface any warnings.

    Errors/warnings that happen mid-action are appended to the log via the
    progress callback; without this scan they'd be invisible unless the
    user opened the drawer. Counts them, sets `_activity_alert` so the
    banner + auto-expanded drawer surface them.
    """
    from lineage_bridge.ui.logs import severity_counts

    lines = st.session_state.get(log_key) or []
    counts = severity_counts(lines)
    err = counts.get("error", 0)
    warn = counts.get("warning", 0)
    if err:
        _record_activity_alert(
            "error",
            f"{action_label} completed with {err} error(s) and {warn} warning(s)",
            log_key,
        )
    elif warn:
        _record_activity_alert(
            "warning",
            f"{action_label} completed with {warn} warning(s)",
            log_key,
        )


def _build_extraction_params(ctx: dict) -> dict:
    """Translate the extraction context + extractor toggles into a params dict."""
    return {
        "env_ids": [e.id for e in ctx["selected_envs"]],
        "cluster_ids": ctx["selected_cluster_ids"],
        "cluster_credentials": ctx["ui_cluster_creds"],
        "sr_credentials": ctx["ui_sr_creds"],
        "flink_credentials": ctx["ui_flink_creds"],
        "enable_connect": st.session_state.get("ext_connect", True),
        "enable_ksqldb": st.session_state.get("ext_ksqldb", False),
        "enable_flink": st.session_state.get("ext_flink", False),
        "enable_schema_registry": st.session_state.get("ext_sr", False),
        "enable_stream_catalog": st.session_state.get("ext_catalog", False),
        "enable_tableflow": st.session_state.get("ext_tf", True),
        "enable_metrics": st.session_state.get("ext_metrics", False),
        "metrics_lookback_hours": st.session_state.get("metrics_lookback", 1),
        "enable_enrichment": True,
    }


# ── extract / enrich / refresh ──────────────────────────────────────────


def _render_sidebar_actions() -> None:
    """Single primary 'Run extraction' button + popover for Enrich / Refresh."""
    ctx = _resolve_extraction_context()
    if not ctx:
        return

    settings = ctx["settings"]
    selected_cluster_ids = ctx["selected_cluster_ids"]
    has_graph = st.session_state.graph is not None
    has_params = st.session_state.last_extraction_params is not None

    extract_label = "Re-extract" if has_graph else "Extract lineage"

    # Track which action was triggered
    action: str | None = None

    # Primary action — full width
    if st.button(
        extract_label,
        key="extract_btn",
        type="primary",
        disabled=not selected_cluster_ids,
        width="stretch",
        help="Extract lineage from Confluent Cloud using the current selection",
    ):
        action = "extract"

    # Secondary actions — compact popover so they don't take row real estate
    if has_graph or has_params:
        with st.popover("More actions", use_container_width=True):
            st.caption("Operates on the existing graph; does not re-extract.")
            if st.button(
                "Enrich existing graph",
                key="enrich_btn",
                disabled=not has_graph,
                width="stretch",
                help="Run catalog enrichment + metrics on the current graph",
            ):
                action = "enrich"
            if st.button(
                "Re-run with last params",
                key="refresh_extract_btn",
                disabled=not has_params,
                width="stretch",
                help="Repeat the previous extraction with the saved parameters",
            ):
                action = "refresh"

    # Status widget — outside columns so it spans the sidebar
    if action == "extract":
        params = _build_extraction_params(ctx)
        st.session_state.extraction_log = []
        st.session_state._activity_alert = None
        with st.status("Extracting lineage...", expanded=True) as status:
            try:
                result = _run_extraction_with_params(settings, params)
                st.session_state.graph = result
                st.session_state.graph_version = _GRAPH_VERSION
                st.session_state._clear_positions = True
                st.session_state.selected_node = None
                st.session_state.focus_node = None
                st.session_state.last_extraction_params = params
                st.session_state.last_extraction_time = datetime.now(UTC).strftime("%H:%M:%S UTC")
                _save_selections_to_cache(params)
                status.update(
                    label=f"Done — {result.node_count} nodes, {result.edge_count} edges",
                    state="complete",
                )
                _post_action_alert("Extraction", "extraction_log")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")
                _record_activity_alert("error", f"Extraction failed: {exc}", "extraction_log")

    elif action == "enrich":
        params = st.session_state.last_extraction_params or {}
        st.session_state.extraction_log = []
        st.session_state._activity_alert = None
        with st.status("Enriching graph...", expanded=True) as status:
            try:
                result = _run_enrichment_on_graph(settings, st.session_state.graph, params)
                st.session_state.graph = result
                st.session_state.last_extraction_time = datetime.now(UTC).strftime("%H:%M:%S UTC")
                status.update(
                    label=f"Enriched — {result.node_count} nodes, {result.edge_count} edges",
                    state="complete",
                )
                _post_action_alert("Enrichment", "extraction_log")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")
                _record_activity_alert("error", f"Enrichment failed: {exc}", "extraction_log")

    elif action == "refresh":
        params = dict(st.session_state.last_extraction_params)
        params["enable_enrichment"] = True
        st.session_state.extraction_log = []
        st.session_state._activity_alert = None
        with st.status("Refreshing...", expanded=True) as status:
            try:
                result = _run_extraction_with_params(settings, params)
                st.session_state.graph = result
                st.session_state.graph_version = _GRAPH_VERSION
                st.session_state._clear_positions = True
                st.session_state.selected_node = None
                st.session_state.focus_node = None
                st.session_state.last_extraction_time = datetime.now(UTC).strftime("%H:%M:%S UTC")
                status.update(
                    label=f"Refreshed — {result.node_count} nodes, {result.edge_count} edges",
                    state="complete",
                )
                _post_action_alert("Refresh", "extraction_log")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")
                _record_activity_alert("error", f"Refresh failed: {exc}", "extraction_log")

    # Logs render in the bottom drawer (see lineage_bridge/ui/logs.py); no
    # in-sidebar expander any more — the sidebar is too narrow for them.


# ── publish (single panel for all targets) ──────────────────────────────


@dataclass
class _PublishTarget:
    """Metadata + render hooks for one publish target row."""

    key: str
    name: str
    short_name: str
    status: str  # "ready" | "no_nodes" | "not_configured"
    detail: str  # one-line status caption
    render_options: Callable[[], None] | None = None
    push_fn: Callable[[Any, LineageGraph, dict], Any] | None = None
    push_param_keys: tuple[str, ...] = ()
    eligible_count: int = 0


def _render_sidebar_publish() -> None:
    """One Publish panel listing every target with status pill + inline push UI."""
    settings = _try_load_settings()
    graph = st.session_state.get("graph")
    if not settings or graph is None:
        return

    targets = [
        _databricks_target(settings, graph),
        _glue_target(settings, graph),
        _datazone_target(settings, graph),
        _google_target(settings, graph),
    ]

    # Order: ready → no_nodes → not_configured (most actionable first).
    order = {"ready": 0, "no_nodes": 1, "not_configured": 2}
    targets.sort(key=lambda t: order[t.status])

    for t in targets:
        _render_publish_row(settings, graph, t)

    # Push log renders in the bottom drawer (see lineage_bridge/ui/logs.py).


def _render_publish_row(settings, graph: LineageGraph, t: _PublishTarget) -> None:
    """Render one publish-target row: header (status + name) + push controls."""
    status_dot = {
        "ready": "#4CAF50",
        "no_nodes": "#FFC107",
        "not_configured": "#9E9E9E",
    }[t.status]

    header_html = (
        f"<div class='publish-row'>"
        f"<span class='status-dot' style='background:{status_dot}'></span>"
        f"<span class='publish-row-name'>{t.name}</span>"
        f"<span class='publish-row-detail'>{t.detail}</span>"
        f"</div>"
    )
    st.markdown(header_html, unsafe_allow_html=True)

    if t.status != "ready" or t.push_fn is None:
        return

    with st.popover(
        f"Push to {t.short_name}",
        use_container_width=True,
    ):
        if t.render_options:
            t.render_options()
        if st.button(
            f"Push to {t.short_name}",
            key=f"push_btn_{t.key}",
            type="primary",
            width="stretch",
        ):
            params = dict(st.session_state.last_extraction_params or {})
            for k in t.push_param_keys:
                params[k] = st.session_state.get(k, True)
            st.session_state.push_log = []
            st.session_state._activity_alert = None
            with st.status(f"Pushing to {t.short_name}...", expanded=True) as status:
                try:
                    result = t.push_fn(settings, graph, params)
                    msg = _format_push_result(t.short_name, result)
                    errs = getattr(result, "errors", None)
                    skips = getattr(result, "skipped", None)
                    state = "error" if errs else "complete"
                    status.update(label=msg, state=state)
                    if errs:
                        _record_activity_alert(
                            "error",
                            f"Push to {t.short_name}: {len(errs)} error(s)",
                            "push_log",
                        )
                    elif skips:
                        _record_activity_alert(
                            "warning",
                            f"Push to {t.short_name}: {len(skips)} skipped "
                            "(catalogs you don't own)",
                            "push_log",
                        )
                    else:
                        _post_action_alert(f"Push to {t.short_name}", "push_log")
                    st.rerun()
                except Exception as exc:
                    status.update(label=f"Failed: {exc}", state="error")
                    _record_activity_alert(
                        "error", f"Push to {t.short_name} failed: {exc}", "push_log"
                    )


def _format_push_result(target: str, result) -> str:
    parts = [f"{target}: {result.tables_updated} tables"]
    if getattr(result, "properties_set", 0):
        parts.append(f"{result.properties_set} props")
    if getattr(result, "comments_set", 0):
        parts.append(f"{result.comments_set} comments")
    if getattr(result, "skipped", None):
        parts.append(f"{len(result.skipped)} skipped")
    if getattr(result, "errors", None):
        parts.append(f"{len(result.errors)} error(s)")
    return " · ".join(parts)


# ── per-target factories ────────────────────────────────────────────────


def _databricks_target(settings, graph: LineageGraph) -> _PublishTarget:
    if not settings.databricks_workspace_url:
        return _PublishTarget(
            key="databricks",
            name="Databricks UC",
            short_name="UC",
            status="not_configured",
            detail="Set DATABRICKS_WORKSPACE_URL in .env",
        )
    uc_tables = graph.filter_catalog_nodes("UNITY_CATALOG")
    if not uc_tables:
        return _PublishTarget(
            key="databricks",
            name="Databricks UC",
            short_name="UC",
            status="no_nodes",
            detail="No UC tables in graph (enable Tableflow)",
        )

    def _render_options() -> None:
        st.caption("Options")
        st.checkbox("Set table properties", value=True, key="push_properties")
        st.checkbox("Set table comments", value=True, key="push_comments")
        st.checkbox("Create bridge table", value=False, key="push_bridge_table")
        _render_warehouse_picker(settings)

    def _push(settings, graph, params):
        wh_id = st.session_state.get("databricks_selected_warehouse_id")
        if wh_id:
            settings = settings.model_copy(update={"databricks_warehouse_id": wh_id})
        return _run_lineage_push(settings, graph, params)

    return _PublishTarget(
        key="databricks",
        name="Databricks UC",
        short_name="UC",
        status="ready",
        detail=f"{len(uc_tables)} UC table(s)",
        render_options=_render_options,
        push_fn=_push,
        push_param_keys=("push_properties", "push_comments", "push_bridge_table"),
        eligible_count=len(uc_tables),
    )


def _render_warehouse_picker(settings) -> None:
    """Inline Databricks warehouse picker (Discover + selectbox)."""
    warehouses = st.session_state.get("databricks_warehouses", [])

    if st.button(
        "Discover warehouses",
        key="discover_wh_btn",
        width="stretch",
    ):
        # Unified loading pattern: st.status (was a bare st.spinner).
        with st.status("Discovering Databricks warehouses...", expanded=True) as status:
            try:
                from lineage_bridge.clients.databricks_discovery import list_warehouses

                wh_list = _run_async(
                    list_warehouses(
                        settings.databricks_workspace_url,
                        settings.databricks_token,
                    )
                )
                st.session_state.databricks_warehouses = wh_list
                warehouses = wh_list
                status.update(label=f"Found {len(wh_list)} warehouse(s)", state="complete")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Discovery failed: {exc}", state="error")

    if warehouses:
        wh_options = {f"{wh.name} ({wh.id}) [{wh.state}]": wh for wh in warehouses}
        default_idx = 0
        if settings.databricks_warehouse_id:
            for i, wh in enumerate(warehouses):
                if wh.id == settings.databricks_warehouse_id:
                    default_idx = i
                    break
        selected_label = st.selectbox(
            "Warehouse",
            options=list(wh_options.keys()),
            index=default_idx,
            key="databricks_wh_select",
        )
        if selected_label:
            selected_wh = wh_options[selected_label]
            st.session_state.databricks_selected_warehouse_id = selected_wh.id
            if selected_wh.state != "RUNNING":
                st.warning(f"Warehouse is {selected_wh.state} — push may start it.")
    elif settings.databricks_warehouse_id:
        st.caption(f"Configured warehouse: `{settings.databricks_warehouse_id}`")


def _glue_target(settings, graph: LineageGraph) -> _PublishTarget:
    glue_tables = graph.filter_catalog_nodes("AWS_GLUE")
    if not glue_tables:
        return _PublishTarget(
            key="glue",
            name="AWS Glue",
            short_name="Glue",
            status="no_nodes",
            detail=f"No Glue tables (region: {settings.aws_region})",
        )
    enriched = sum(1 for n in glue_tables if n.attributes.get("columns"))

    def _render_options() -> None:
        st.caption("Options")
        st.checkbox("Set table parameters", value=True, key="glue_push_parameters")
        st.checkbox("Set table description", value=True, key="glue_push_description")

    def _push(settings, graph, params):
        opts = {
            "push_parameters": st.session_state.get("glue_push_parameters", True),
            "push_description": st.session_state.get("glue_push_description", True),
        }
        return _run_glue_push(settings, graph, opts)

    return _PublishTarget(
        key="glue",
        name="AWS Glue",
        short_name="Glue",
        status="ready",
        detail=f"{len(glue_tables)} table(s), {enriched} enriched",
        render_options=_render_options,
        push_fn=_push,
        eligible_count=len(glue_tables),
    )


def _datazone_target(settings, graph: LineageGraph) -> _PublishTarget:
    if not (settings.aws_datazone_domain_id and settings.aws_datazone_project_id):
        return _PublishTarget(
            key="datazone",
            name="AWS DataZone",
            short_name="DataZone",
            status="not_configured",
            detail="Set AWS_DATAZONE_DOMAIN_ID + PROJECT_ID",
        )
    # DataZone integration is a Glue companion — registers Kafka assets that
    # produce/feed Glue tables. Showing it as "ready" in a Confluent-only or
    # GCP-targeting graph creates a misleading action, so gate on Glue.
    glue_tables = graph.filter_catalog_nodes("AWS_GLUE")
    kafka_topics = graph.filter_by_type(NodeType.KAFKA_TOPIC)
    if not glue_tables:
        return _PublishTarget(
            key="datazone",
            name="AWS DataZone",
            short_name="DataZone",
            status="no_nodes",
            detail="No AWS Glue tables in graph",
        )
    if not kafka_topics:
        return _PublishTarget(
            key="datazone",
            name="AWS DataZone",
            short_name="DataZone",
            status="no_nodes",
            detail="No Kafka topics in graph",
        )

    def _push(settings, graph, params):
        return _run_datazone_push(settings, graph, {})

    return _PublishTarget(
        key="datazone",
        name="AWS DataZone",
        short_name="DataZone",
        status="ready",
        detail=f"{len(kafka_topics)} topic(s) · {len(glue_tables)} Glue table(s)",
        push_fn=_push,
        eligible_count=len(kafka_topics),
    )


def _google_target(settings, graph: LineageGraph) -> _PublishTarget:
    if not settings.gcp_project_id:
        return _PublishTarget(
            key="google",
            name="Google Data Lineage",
            short_name="Google",
            status="not_configured",
            detail="Set LINEAGE_BRIDGE_GCP_PROJECT_ID",
        )
    google_tables = graph.filter_catalog_nodes("GOOGLE_DATA_LINEAGE")
    if not google_tables:
        return _PublishTarget(
            key="google",
            name="Google Data Lineage",
            short_name="Google",
            status="no_nodes",
            detail=f"No BQ tables (project: {settings.gcp_project_id})",
        )
    enriched = sum(1 for n in google_tables if n.attributes.get("columns"))

    def _push(settings, graph, params):
        return _run_google_push(settings, graph, {})

    return _PublishTarget(
        key="google",
        name="Google Data Lineage",
        short_name="Google",
        status="ready",
        detail=f"{len(google_tables)} BQ table(s), {enriched} enriched",
        push_fn=_push,
        eligible_count=len(google_tables),
    )


# ── load data ──────────────────────────────────────────────────────────


def _render_sidebar_load_data() -> None:
    """Load from file or upload. (Demo button lives only in empty state.)"""
    graph_path = st.text_input(
        "File path",
        value="./lineage_graph.json",
        key="graph_path_input",
    )
    if st.button("Load from path", key="load_path_btn", width="stretch"):
        p = Path(graph_path).expanduser()
        if not p.exists():
            st.error(f"File not found: {p}")
        else:
            try:
                g = LineageGraph.from_json_file(p)
                st.session_state.graph = g
                st.session_state.graph_version = _GRAPH_VERSION
                st.session_state.selected_node = None
                st.session_state.focus_node = None
                st.session_state.last_extraction_params = None
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to parse: {exc}")

    uploaded = st.file_uploader("Upload JSON", type=["json"], key="json_upload")
    if uploaded is not None and st.button(
        "Parse uploaded file",
        key="parse_upload_btn",
        width="stretch",
    ):
        try:
            data = json.loads(uploaded.getvalue())
            g = LineageGraph.from_dict(data)
            st.session_state.graph = g
            st.session_state.graph_version = _GRAPH_VERSION
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.session_state.last_extraction_params = None
            st.rerun()
        except Exception as exc:
            st.error(f"Failed to parse: {exc}")


# ── legacy export (kept for tests / external imports) ──────────────────


def _render_extraction_log() -> None:
    """Backwards-compat alias — log now renders in the bottom drawer."""
    from lineage_bridge.ui.logs import render_logs_drawer

    render_logs_drawer()


def _render_sidebar_push_log() -> None:
    """Backwards-compat alias — log now renders in the bottom drawer."""
    from lineage_bridge.ui.logs import render_logs_drawer

    render_logs_drawer()
