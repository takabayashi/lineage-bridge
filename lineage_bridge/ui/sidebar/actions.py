# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — extract / enrich / push buttons + per-platform push UI + log + load-data.

These render functions all call into `lineage_bridge.services` (per ADR-020).
The legacy session-state shape (`_cached_*`, `extraction_log`, etc.) is
preserved verbatim so the rest of the UI (graph_renderer, node_details)
doesn't need to change.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

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
from lineage_bridge.ui.sample_data import generate_sample_graph
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

    # Collect per-cluster credentials
    ui_cluster_creds: dict[str, dict[str, str]] = {}
    for lbl in selected_cluster_labels:
        if lbl not in all_cluster_options:
            continue
        cid = all_cluster_options[lbl].id
        k = st.session_state.get(f"cluster_key_{cid}", "")
        s = st.session_state.get(f"cluster_secret_{cid}", "")
        if k and s:
            ui_cluster_creds[cid] = {"api_key": k, "api_secret": s}

    # Collect per-environment SR credentials + endpoints
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

    # Collect per-environment Flink credentials
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


# ── extract / enrich / refresh actions ──────────────────────────────────


def _render_sidebar_actions() -> None:
    """Extract, Enrich, and Refresh buttons with full-width status widgets."""
    ctx = _resolve_extraction_context()
    if not ctx:
        return

    settings = ctx["settings"]
    selected_envs = ctx["selected_envs"]
    selected_cluster_ids = ctx["selected_cluster_ids"]

    has_graph = st.session_state.graph is not None
    extract_label = "Re-extract" if has_graph else "Extract"

    # Track which action was triggered
    action = None

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button(
            extract_label,
            key="extract_btn",
            type="primary",
            disabled=not selected_cluster_ids,
            width="stretch",
            help="Extract lineage from Confluent Cloud",
        ):
            action = "extract"
    with c2:
        if st.button(
            "Enrich",
            key="enrich_btn",
            disabled=not has_graph,
            width="stretch",
            help="Enrich graph with catalog metadata and metrics",
        ):
            action = "enrich"
    with c3:
        has_params = st.session_state.last_extraction_params is not None
        if st.button(
            "Refresh",
            key="refresh_extract_btn",
            disabled=not has_params,
            width="stretch",
            help="Re-run last extraction with same parameters",
        ):
            action = "refresh"

    # Render status widget at full sidebar width (outside columns)
    if action == "extract":
        params = {
            "env_ids": [e.id for e in selected_envs],
            "cluster_ids": selected_cluster_ids,
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
        st.session_state.extraction_log = []
        st.session_state._log_source = "extraction"
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
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")

    elif action == "enrich":
        params = st.session_state.last_extraction_params or {}
        st.session_state.extraction_log = []
        st.session_state._log_source = "extraction"
        with st.status("Enriching graph...", expanded=True) as status:
            try:
                result = _run_enrichment_on_graph(settings, st.session_state.graph, params)
                st.session_state.graph = result
                st.session_state.last_extraction_time = datetime.now(UTC).strftime("%H:%M:%S UTC")
                status.update(
                    label=f"Enriched — {result.node_count} nodes, {result.edge_count} edges",
                    state="complete",
                )
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")

    elif action == "refresh":
        params = dict(st.session_state.last_extraction_params)
        params["enable_enrichment"] = True
        st.session_state.extraction_log = []
        st.session_state._log_source = "extraction"
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
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")

    # Show extraction log (only if this section produced it)
    if st.session_state.extraction_log and st.session_state.get("_log_source") == "extraction":
        with st.expander("Extraction Log", expanded=False):
            _render_extraction_log()


# ── push UI per platform ────────────────────────────────────────────────


def _render_sidebar_databricks() -> None:
    """Databricks warehouse discovery, push settings, and Push to UC button."""
    settings = _try_load_settings()
    if not settings or not settings.databricks_workspace_url:
        st.caption("Set `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL` in .env to enable.")
        return

    st.caption("SQL Warehouse for pushing lineage metadata.")

    # Discover warehouses button
    warehouses = st.session_state.get("databricks_warehouses", [])
    if st.button("Discover Warehouses", key="discover_wh_btn", width="stretch"):
        with st.spinner("Discovering..."):
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
                st.rerun()
            except Exception as exc:
                st.error(f"Discovery failed: {exc}")

    if warehouses:
        wh_options = {f"{wh.name} ({wh.id}) [{wh.state}]": wh for wh in warehouses}
        # Pre-select the configured warehouse or the first RUNNING one
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
                st.warning(f"Warehouse is {selected_wh.state} — push may fail or start it.")
    elif settings.databricks_warehouse_id:
        st.info(f"Using configured warehouse: `{settings.databricks_warehouse_id}`")

    # Push options
    st.checkbox("Set table properties", value=True, key="push_properties")
    st.checkbox("Set table comments", value=True, key="push_comments")
    st.checkbox("Create bridge table", value=False, key="push_bridge_table")

    # ── Push to UC button ─────────────────────────────────────────────
    graph = st.session_state.get("graph")
    has_uc_tables = graph is not None and len(graph.filter_catalog_nodes("UNITY_CATALOG")) > 0
    st.divider()
    if not has_uc_tables:
        st.caption("No UC tables in current graph — extract with Tableflow enabled.")
        return

    if st.button(
        "⤴ Push to UC",
        key="push_btn",
        type="primary",
        width="stretch",
        help="Push lineage metadata to Databricks Unity Catalog",
    ):
        params = dict(st.session_state.last_extraction_params or {})
        params["push_properties"] = st.session_state.get("push_properties", True)
        params["push_comments"] = st.session_state.get("push_comments", True)
        params["push_bridge_table"] = st.session_state.get("push_bridge_table", False)
        wh_id = st.session_state.get("databricks_selected_warehouse_id")
        if wh_id:
            settings = settings.model_copy(update={"databricks_warehouse_id": wh_id})
        st.session_state.extraction_log = []
        st.session_state._log_source = "publish"
        with st.status("Pushing lineage to UC...", expanded=True) as status:
            try:
                push_result = _run_lineage_push(settings, st.session_state.graph, params)
                msg = (
                    f"Pushed — {push_result.tables_updated} tables, "
                    f"{push_result.properties_set} properties, "
                    f"{push_result.comments_set} comments"
                )
                if push_result.errors:
                    msg += f" ({len(push_result.errors)} error(s))"
                status.update(label=msg, state="complete")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")


def _render_sidebar_aws() -> None:
    """AWS Glue + DataZone publish settings and push buttons."""
    settings = _try_load_settings()
    if not settings:
        st.caption("Configure `.env` to enable.")
        return

    st.caption(f"Region: `{settings.aws_region}`")
    st.caption("Credentials: AWS default credential chain")

    graph = st.session_state.get("graph")
    glue_tables = graph.filter_catalog_nodes("AWS_GLUE") if graph else []
    kafka_topics = graph.filter_by_type(NodeType.KAFKA_TOPIC) if graph else []
    has_glue_tables = bool(glue_tables)

    if has_glue_tables:
        enriched = sum(1 for n in glue_tables if n.attributes.get("columns"))
        st.info(f"{len(glue_tables)} Glue table(s), {enriched} enriched")
    elif graph is not None:
        st.caption("No Glue tables in current graph.")

    # ── Glue push options + button ───────────────────────────────────
    st.markdown("**Glue**")
    st.checkbox("Set table parameters", value=True, key="glue_push_parameters")
    st.checkbox("Set table description", value=True, key="glue_push_description")
    if has_glue_tables and st.button(
        "⤴ Push to Glue",
        key="glue_push_btn",
        type="primary",
        width="stretch",
        help="Push lineage metadata to AWS Glue table parameters and description",
    ):
        params = {
            "push_parameters": st.session_state.get("glue_push_parameters", True),
            "push_description": st.session_state.get("glue_push_description", True),
        }
        st.session_state.extraction_log = []
        st.session_state._log_source = "publish"
        with st.status("Pushing lineage to Glue...", expanded=True) as status:
            try:
                push_result = _run_glue_push(settings, st.session_state.graph, params)
                msg = (
                    f"Pushed — {push_result.tables_updated} Glue tables, "
                    f"{push_result.properties_set} parameters, "
                    f"{push_result.comments_set} descriptions"
                )
                if push_result.errors:
                    msg += f" ({len(push_result.errors)} error(s))"
                status.update(label=msg, state="complete")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")

    # ── DataZone push button ─────────────────────────────────────────
    st.divider()
    st.markdown("**DataZone**")
    if not (settings.aws_datazone_domain_id and settings.aws_datazone_project_id):
        st.caption(
            "Set `LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID` + `..._PROJECT_ID` in .env "
            "to enable. (Auto-wired by `make demo-glue-up` when a domain exists.)"
        )
        return

    st.caption(f"Domain: `{settings.aws_datazone_domain_id}`")
    st.caption(f"Project: `{settings.aws_datazone_project_id}`")
    if not kafka_topics:
        st.caption("No Kafka topics in current graph — extract first.")
        return
    if st.button(
        "⤴ Push to DataZone",
        key="datazone_push_btn",
        type="primary",
        width="stretch",
        help="Register Kafka topics as DataZone assets and post OpenLineage events",
    ):
        st.session_state.extraction_log = []
        st.session_state._log_source = "publish"
        with st.status("Pushing lineage to DataZone...", expanded=True) as status:
            try:
                push_result = _run_datazone_push(settings, st.session_state.graph, {})
                msg = f"Pushed — {push_result.tables_updated} events"
                if push_result.errors:
                    msg += f" ({len(push_result.errors)} error(s))"
                status.update(label=msg, state="complete")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")


def _render_sidebar_google() -> None:
    """Google Data Lineage settings and push options."""
    settings = _try_load_settings()
    if not settings:
        st.caption("Configure `.env` to enable.")
        return

    if not settings.gcp_project_id:
        st.caption("Set `LINEAGE_BRIDGE_GCP_PROJECT_ID` in .env to enable.")
        return

    st.caption(f"Project: `{settings.gcp_project_id}`")
    st.caption(f"Location: `{settings.gcp_location}`")
    st.caption("Credentials: Application Default Credentials (ADC)")

    graph = st.session_state.get("graph")
    google_tables = graph.filter_catalog_nodes("GOOGLE_DATA_LINEAGE") if graph else []
    if google_tables:
        enriched = sum(1 for n in google_tables if n.attributes.get("columns"))
        st.info(f"{len(google_tables)} BigQuery table(s), {enriched} enriched")
    elif graph is not None:
        st.caption("No Google tables in current graph.")

    # ── Push to Google button ─────────────────────────────────────────
    st.divider()
    if not google_tables:
        st.caption("No Google tables to push — extract a BQ-targeting connector first.")
        return

    if st.button(
        "⤴ Push to Google",
        key="google_push_btn",
        type="primary",
        width="stretch",
        help=(
            "Push lineage as OpenLineage events to Google Data Lineage and register "
            "Kafka topics as Dataplex Catalog entries (with schema)"
        ),
    ):
        st.session_state.extraction_log = []
        st.session_state._log_source = "publish"
        with st.status("Pushing lineage to Google...", expanded=True) as status:
            try:
                push_result = _run_google_push(settings, st.session_state.graph, {})
                msg = f"Pushed — {push_result.tables_updated} events"
                if push_result.errors:
                    msg += f" ({len(push_result.errors)} error(s))"
                status.update(label=msg, state="complete")
                st.rerun()
            except Exception as exc:
                status.update(label=f"Failed: {exc}", state="error")


# ── log rendering ───────────────────────────────────────────────────────


def _render_sidebar_push_log() -> None:
    """Render the shared Push Log expander (populated by per-platform push buttons)."""
    if st.session_state.extraction_log and st.session_state.get("_log_source") == "publish":
        with st.expander("Push Log", expanded=False):
            _render_extraction_log()


def _classify_log_entry(line: str) -> tuple[str, str, str]:
    """Classify a log line into (css_class, icon, label).

    Returns (css_class, icon_char, cleaned_text).
    """
    text = line
    # Extract bold label if present: **Label** rest
    label = ""
    if text.startswith("**"):
        end = text.find("**", 2)
        if end > 2:
            label = text[2:end]
            text = text[end + 2 :].strip()

    label_lower = label.lower()
    if "warning" in label_lower:
        return "log-warning", "⚠", text
    if "skip" in label_lower:
        return "log-skip", "⏭", text
    if "phase" in label_lower:
        return "log-phase", "▶", text
    if "discover" in label_lower:
        return "log-discovery", "\U0001f50d", text
    if "provision" in label_lower:
        return "log-provision", "\U0001f511", text
    # Default
    return "log-phase", "•", text


def _render_extraction_log() -> None:
    """Render extraction log with severity-based styling."""
    lines = st.session_state.extraction_log
    html_parts = []
    for line in lines:
        css_class, icon, text = _classify_log_entry(line)
        # Re-extract label for display
        label = ""
        raw = line
        if raw.startswith("**"):
            end = raw.find("**", 2)
            if end > 2:
                label = raw[2:end]
        label_html = f"<span class='log-label'>{label}</span>" if label else ""
        html_parts.append(
            f"<div class='log-entry {css_class}'>"
            f"<span class='log-icon'>{icon}</span>"
            f"<span class='log-text'>{label_html}{text}</span>"
            f"</div>"
        )
    st.markdown("".join(html_parts), unsafe_allow_html=True)


# ── load data ──────────────────────────────────────────────────────────


def _render_sidebar_load_data() -> None:
    """Load from file, upload, or demo."""
    if st.button(
        "Load Demo Graph",
        key="load_demo_sidebar",
        type="primary",
        width="stretch",
    ):
        st.session_state.graph = generate_sample_graph()
        st.session_state.graph_version = _GRAPH_VERSION
        st.session_state.selected_node = None
        st.session_state.focus_node = None
        st.session_state.last_extraction_params = None
        st.rerun()

    st.markdown("---")

    graph_path = st.text_input(
        "File path",
        value="./lineage_graph.json",
        key="graph_path_input",
    )
    if st.button("Load from path", key="load_path_btn"):
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
    if uploaded is not None and st.button("Parse uploaded file", key="parse_upload_btn"):
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
