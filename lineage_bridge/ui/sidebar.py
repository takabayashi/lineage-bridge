# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar rendering for the LineageBridge UI."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.discovery import (
    _discover_one,
    _make_cloud_client,
    _run_async,
    _try_load_settings,
)
from lineage_bridge.ui.extraction import (
    _run_enrichment_on_graph,
    _run_extraction_with_params,
    _run_glue_push,
    _run_google_push,
    _run_lineage_push,
    _save_selections_to_cache,
)
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.state import _GRAPH_VERSION
from lineage_bridge.ui.styles import (
    EDGE_COLORS,
    EDGE_DASHES,
    EDGE_TYPE_LABELS,
    NODE_ICONS,
    NODE_TYPE_LABELS,
)


def _sidebar_section(label: str):
    """Render a styled section header with divider."""
    st.markdown(
        f"<hr class='sidebar-divider'/><div class='sidebar-section'>{label}</div>",
        unsafe_allow_html=True,
    )


def _render_sidebar():
    """Persistent sidebar: connection, scope, extractors, filters."""
    with st.sidebar:
        st.markdown("### \U0001f310 LineageBridge")

        # ══════════════════════════════════════════════════════════════
        #  SETUP
        # ══════════════════════════════════════════════════════════════
        _sidebar_section("Setup")

        # Connection status at sidebar root level (full width)
        if st.session_state.connected:
            envs = st.session_state.environments
            st.markdown(
                f"<div class='status-badge status-connected'>"
                f"<span class='status-dot' style='background:#4CAF50'></span>"
                f"Connected &mdash; {len(envs)} environment(s)"
                f"</div>",
                unsafe_allow_html=True,
            )

        with st.expander(
            "Connection",
            expanded=not st.session_state.connected,
        ):
            _render_sidebar_connection()

        if st.session_state.connected:
            with st.expander(
                "Infrastructure",
                expanded=st.session_state.graph is None,
            ):
                _render_sidebar_scope()

            # ══════════════════════════════════════════════════════════
            #  EXTRACTION
            # ══════════════════════════════════════════════════════════
            _sidebar_section("Extraction")

            with st.expander("Extractors", expanded=False):
                _render_sidebar_extractors()

            _render_sidebar_actions()

            # ══════════════════════════════════════════════════════════
            #  PUBLISH
            # ══════════════════════════════════════════════════════════
            has_graph = st.session_state.graph is not None
            if has_graph:
                _sidebar_section("Publish")

                with st.expander("Databricks", expanded=False):
                    _render_sidebar_databricks()

                with st.expander("AWS Glue", expanded=False):
                    _render_sidebar_aws_glue()

                with st.expander("Google Data Lineage", expanded=False):
                    _render_sidebar_google()

                _render_sidebar_publish()

        # ══════════════════════════════════════════════════════════════
        #  GRAPH
        # ══════════════════════════════════════════════════════════════
        graph = st.session_state.graph
        if graph is not None:
            _sidebar_section("Graph")

            # Focus indicator at sidebar root level (full width)
            focus_active = st.session_state.focus_node is not None
            if focus_active:
                focus_obj = graph.get_node(st.session_state.focus_node)
                fname = focus_obj.display_name if focus_obj else st.session_state.focus_node
                st.markdown(
                    f"<div class='status-badge' style='background:rgba(33,150,243,0.1);"
                    f"color:#1565C0;border:1px solid rgba(33,150,243,0.2);'>"
                    f"<span class='status-dot' style='background:#1976D2'></span>"
                    f"Focused: {fname}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                if st.button(
                    "Clear focus",
                    key="clear_focus_btn",
                    width="stretch",
                ):
                    st.session_state.focus_node = None
                    st.rerun()

            with st.expander("Filters", expanded=True):
                _render_sidebar_graph_filters(graph)
            with st.expander("Legend", expanded=False):
                _render_sidebar_legend(graph)

        # ══════════════════════════════════════════════════════════════
        #  DATA
        # ══════════════════════════════════════════════════════════════
        _sidebar_section("Data")

        with st.expander("Load Data", expanded=False):
            _render_sidebar_load_data()


def _render_sidebar_connection():
    """Connection status and connect button."""
    settings = _try_load_settings()

    if st.session_state.connected:
        st.caption("Connection active. Expand to view credentials.")
        return

    if settings:
        api_key = settings.confluent_cloud_api_key
        masked = api_key[:4] + "..." + api_key[-4:]
        st.info(f"Credentials: `{masked}`")

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
    else:
        st.warning("No credentials found.")
        st.code(
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...\n"
            "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...",
            language="bash",
        )


def _render_sidebar_scope():
    """Environment and cluster selection."""
    settings = _try_load_settings()
    if not settings:
        return

    all_envs = st.session_state.environments
    cache = st.session_state.env_cache

    # Discovery buttons
    all_discovered = all(env.id in cache for env in all_envs)
    c1, c2 = st.columns(2)
    with c1:
        if st.button(
            "Discover",
            key="discover_all_btn",
            type="primary" if not all_discovered else "secondary",
            disabled=all_discovered,
            width="stretch",
        ):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                if env.id not in cache:
                    try:
                        cache[env.id] = _discover_one(settings, env.id)
                    except Exception as exc:
                        cache[env.id] = {"services": None, "error": str(exc)}
                bar.progress((i + 1) / len(all_envs))
            bar.empty()
            st.rerun()
    with c2:
        if st.button(
            "Refresh",
            key="refresh_discovery_btn",
            width="stretch",
        ):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                try:
                    cache[env.id] = _discover_one(settings, env.id)
                except Exception as exc:
                    cache[env.id] = {"services": None, "error": str(exc)}
                bar.progress((i + 1) / len(all_envs))
            bar.empty()
            st.rerun()

    # Show discovered environments
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]

    if not discovered_envs:
        st.caption("Click **Discover** to find services.")
        return

    # Environment selector
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    env_options = ["", *list(env_labels.keys())]
    selected_env_label = st.selectbox(
        "Environment",
        options=env_options,
        index=0,
        key="env_select",
        placeholder="Select an environment...",
    )
    selected_envs = [env_labels[selected_env_label]] if selected_env_label else []

    # Per-environment service keys (optional)
    if selected_envs:
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

    # Cluster multiselect
    all_cluster_options = {}
    for env in selected_envs:
        svc = cache[env.id]["services"]
        for c in svc.clusters:
            label = f"{c.display_name} ({c.id})"
            all_cluster_options[label] = c

    if all_cluster_options:
        st.multiselect(
            "Clusters",
            options=list(all_cluster_options.keys()),
            default=list(all_cluster_options.keys()),
            key="cluster_select",
        )

        # Per-cluster API keys (optional)
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


def _render_sidebar_extractors():
    """Extractor toggles."""
    cache = st.session_state.env_cache
    all_envs = st.session_state.environments

    # Check service availability
    selected_env_label = st.session_state.get("env_select", "")
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_envs = [env_labels[selected_env_label]] if selected_env_label in env_labels else []

    any_sr = any(
        cache[e.id]["services"].has_schema_registry
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )
    any_ksqldb = any(
        cache[e.id]["services"].has_ksqldb
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )
    any_flink = any(
        cache[e.id]["services"].has_flink
        for e in selected_envs
        if e.id in cache and cache[e.id].get("services")
    )

    st.checkbox("Connectors", value=True, key="ext_connect")
    st.checkbox(
        "ksqlDB",
        value=True,
        disabled=not any_ksqldb,
        key="ext_ksqldb",
    )
    st.checkbox(
        "Flink",
        value=True,
        disabled=not any_flink,
        key="ext_flink",
    )
    st.checkbox(
        "Schema Registry",
        value=True,
        disabled=not any_sr,
        key="ext_sr",
    )
    st.checkbox(
        "Stream Catalog",
        value=True,
        disabled=not any_sr,
        key="ext_catalog",
    )
    st.checkbox("Tableflow", value=True, key="ext_tf")
    st.checkbox(
        "Metrics",
        value=True,
        key="ext_metrics",
        help="Enrich nodes with live throughput data.",
    )
    if st.session_state.get("ext_metrics"):
        st.slider(
            "Metrics lookback (hours)",
            min_value=1,
            max_value=24,
            value=1,
            key="metrics_lookback",
        )


def _render_sidebar_databricks():
    """Databricks warehouse discovery and push settings."""
    settings = _try_load_settings()
    if not settings or not settings.databricks_workspace_url:
        st.caption("Set `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL` in .env to enable.")
        return

    st.caption("SQL Warehouse for pushing lineage metadata.")

    # Discover warehouses button
    warehouses = st.session_state.get("databricks_warehouses", [])
    if st.button(
        "Discover Warehouses",
        key="discover_wh_btn",
        width="stretch",
    ):
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


def _render_sidebar_aws_glue():
    """AWS Glue metadata display and push settings."""
    settings = _try_load_settings()
    if not settings:
        st.caption("Configure `.env` to enable.")
        return

    st.caption(f"Region: `{settings.aws_region}`")
    st.caption("Credentials: AWS default credential chain")

    graph = st.session_state.get("graph")
    if graph is not None:
        from lineage_bridge.models.graph import NodeType

        glue_tables = graph.filter_by_type(NodeType.GLUE_TABLE)
        if glue_tables:
            enriched = sum(1 for n in glue_tables if n.attributes.get("columns"))
            st.info(f"{len(glue_tables)} Glue table(s), {enriched} enriched")
        else:
            st.caption("No Glue tables in current graph.")

    # Push options
    st.checkbox("Set table parameters", value=True, key="glue_push_parameters")
    st.checkbox("Set table description", value=True, key="glue_push_description")


def _render_sidebar_google():
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
    if graph is not None:
        google_tables = graph.filter_by_type(NodeType.GOOGLE_TABLE)
        if google_tables:
            enriched = sum(1 for n in google_tables if n.attributes.get("columns"))
            st.info(f"{len(google_tables)} BigQuery table(s), {enriched} enriched")
        else:
            st.caption("No Google tables in current graph.")


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


def _render_sidebar_actions():
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


def _render_sidebar_publish():
    """Push lineage metadata to external catalogs."""
    settings = _try_load_settings()
    if not settings:
        return

    graph = st.session_state.graph
    if graph is None:
        return

    has_uc_tables = len(graph.filter_by_type(NodeType.UC_TABLE)) > 0
    has_glue_tables = len(graph.filter_by_type(NodeType.GLUE_TABLE)) > 0
    has_google_tables = len(graph.filter_by_type(NodeType.GOOGLE_TABLE)) > 0

    if not has_uc_tables and not has_glue_tables and not has_google_tables:
        st.caption("No catalog tables to publish to. Run extraction with Tableflow enabled.")
        return

    action = None
    num_buttons = sum([has_uc_tables, has_glue_tables, has_google_tables])
    cols = st.columns(num_buttons) if num_buttons > 1 else [st.columns(1)[0]]
    col_idx = 0

    if has_uc_tables:
        with cols[col_idx]:
            if st.button(
                "\u2934 Push to UC",
                key="push_btn",
                type="secondary",
                width="stretch",
                help="Push lineage metadata to Databricks Unity Catalog",
            ):
                action = "push_uc"
        col_idx += 1

    if has_glue_tables:
        with cols[min(col_idx, len(cols) - 1)]:
            if st.button(
                "\u2934 Push to Glue",
                key="glue_push_btn",
                type="secondary",
                width="stretch",
                help="Push lineage metadata to AWS Glue",
            ):
                action = "push_glue"
        col_idx += 1

    if has_google_tables:
        with cols[min(col_idx, len(cols) - 1)]:
            if st.button(
                "\u2934 Push to Google",
                key="google_push_btn",
                type="secondary",
                width="stretch",
                help="Push lineage as OpenLineage events to Google Data Lineage",
            ):
                action = "push_google"

    # Full-width status widgets
    if action == "push_uc":
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

    elif action == "push_glue":
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

    elif action == "push_google":
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

    # Show push log (only if this section produced it)
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
        return "log-warning", "\u26a0", text
    if "skip" in label_lower:
        return "log-skip", "\u23ed", text
    if "phase" in label_lower:
        return "log-phase", "\u25b6", text
    if "discover" in label_lower:
        return "log-discovery", "\U0001f50d", text
    if "provision" in label_lower:
        return "log-provision", "\U0001f511", text
    # Default
    return "log-phase", "\u2022", text


def _render_extraction_log():
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


def _get_type_counts(graph: LineageGraph) -> dict[NodeType, int]:
    """Count nodes per NodeType in a single pass."""
    from collections import Counter

    return Counter(n.node_type for n in graph.nodes)


def _render_sidebar_graph_filters(graph: LineageGraph):
    """Graph filters organized into logical groups."""
    # ── Search ───────────────────────────────────────────────────
    st.text_input(
        "Search nodes",
        placeholder="Type to filter by name...",
        key="search_input",
        label_visibility="collapsed",
    )

    # ── Scope: Environment & Cluster ─────────────────────────────
    env_map: dict[str, str] = {}
    for n in graph.nodes:
        if n.environment_id and n.environment_id not in env_map.values():
            label = (
                f"{n.environment_name} ({n.environment_id})"
                if n.environment_name
                else n.environment_id
            )
            env_map[label] = n.environment_id

    cluster_map: dict[str, str] = {}
    for n in graph.nodes:
        if n.cluster_id and n.cluster_id not in cluster_map.values():
            label = f"{n.cluster_name} ({n.cluster_id})" if n.cluster_name else n.cluster_id
            cluster_map[label] = n.cluster_id

    if len(env_map) > 1 or len(cluster_map) > 1:
        if len(env_map) > 1:
            env_options = ["All", *sorted(env_map.keys())]
            env_sel = st.selectbox(
                "Environment",
                env_options,
                key="graph_env_filter_display",
            )
            st.session_state["graph_env_filter"] = (
                env_map.get(env_sel) if env_sel != "All" else "All"
            )
        if len(cluster_map) > 1:
            cluster_options = ["All", *sorted(cluster_map.keys())]
            cluster_sel = st.selectbox(
                "Cluster",
                cluster_options,
                key="graph_cluster_filter_display",
            )
            st.session_state["graph_cluster_filter"] = (
                cluster_map.get(cluster_sel) if cluster_sel != "All" else "All"
            )

    # ── Node type filters (compact two-column) ───────────────────
    st.caption("Node Types")
    type_counts = _get_type_counts(graph)
    type_items = [
        (ntype, NODE_TYPE_LABELS.get(ntype, ntype.value), type_counts.get(ntype, 0))
        for ntype in NodeType
    ]
    visible_types = [(nt, lbl, cnt) for nt, lbl, cnt in type_items if cnt > 0]

    # Render in two-column pairs
    for i in range(0, len(visible_types), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < len(visible_types):
                nt, lbl, cnt = visible_types[idx]
                with col:
                    st.checkbox(
                        f"{lbl} ({cnt})",
                        value=True,
                        key=f"filter_{nt.value}",
                    )

    # ── Display options ──────────────────────────────────────────
    st.checkbox(
        "Hide disconnected nodes",
        value=True,
        key="hide_disconnected",
        help="Hide nodes that have no edges.",
    )

    # ── Focus / hop controls ─────────────────────────────────────
    focus_active = st.session_state.focus_node is not None
    has_search = bool(st.session_state.get("search_input", "").strip())
    if focus_active or has_search:
        st.slider(
            "Neighborhood hops",
            min_value=1,
            max_value=100,
            value=5,
            key="hop_slider",
            help="How many hops from the focused/searched node to show.",
        )


def _render_sidebar_legend(graph: LineageGraph):
    """Compact legend with node types and edge types."""
    # ── Node types (two-column grid) ─────────────────────────────
    st.caption("Nodes")
    type_counts = _get_type_counts(graph)
    node_html_parts = []
    for ntype in NodeType:
        if type_counts.get(ntype, 0) == 0:
            continue
        icon_uri = NODE_ICONS.get(ntype, "")
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        node_html_parts.append(
            f"<div class='legend-entry'>"
            f"<img src='{icon_uri}' width='16' height='16' "
            f"style='vertical-align:middle;'/>"
            f"{label}"
            f"</div>"
        )
    if node_html_parts:
        st.markdown(
            f"<div class='legend-grid'>{''.join(node_html_parts)}</div>",
            unsafe_allow_html=True,
        )

    # ── Edge types ───────────────────────────────────────────────
    st.caption("Edges")
    edge_html_parts = []
    for etype in EDGE_TYPE_LABELS:
        color = EDGE_COLORS.get(etype, "#757575")
        label = EDGE_TYPE_LABELS[etype]
        dashes = EDGE_DASHES.get(etype, False)
        if dashes:
            swatch = f"<span class='edge-swatch-dashed' style='border-color:{color}'></span>"
        else:
            swatch = f"<span class='edge-swatch' style='background:{color}'></span>"
        edge_html_parts.append(f"<div class='edge-legend-entry'>{swatch}{label}</div>")
    if edge_html_parts:
        st.markdown(
            f"<div class='legend-grid'>{''.join(edge_html_parts)}</div>",
            unsafe_allow_html=True,
        )


def _render_sidebar_load_data():
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

    uploaded = st.file_uploader(
        "Upload JSON",
        type=["json"],
        key="json_upload",
    )
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
