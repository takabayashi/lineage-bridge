# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar rendering for the LineageBridge UI."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

from lineage_bridge.config.provisioner import KeyProvisioner
from lineage_bridge.models.graph import LineageGraph, NodeType
from lineage_bridge.ui.discovery import (
    _discover_one,
    _make_cloud_client,
    _run_async,
    _try_load_settings,
)
from lineage_bridge.ui.extraction import (
    _run_extraction_with_params,
    _save_selections_to_cache,
)
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.state import _GRAPH_VERSION
from lineage_bridge.ui.styles import NODE_ICONS, NODE_TYPE_LABELS


def _render_sidebar():
    """Persistent sidebar: connection, scope, extractors, filters."""
    with st.sidebar:
        st.markdown("### \U0001f310 LineageBridge")

        # ── Section 1: Connection ────────────────────────────────────
        with st.expander(
            "Connection",
            expanded=not st.session_state.connected,
        ):
            _render_sidebar_connection()

        # ── Section 2: Infrastructure Scope ──────────────────────────
        if st.session_state.connected:
            with st.expander(
                "Infrastructure",
                expanded=st.session_state.graph is None,
            ):
                _render_sidebar_scope()

            # ── Section 3: Extractors ────────────────────────────────
            with st.expander("Extractors", expanded=False):
                _render_sidebar_extractors()

            # ── Section 3b: Auto-Provisioning ────────────────────────
            with st.expander("Key Provisioning", expanded=False):
                _render_sidebar_provisioning()

            # ── Extract / Refresh buttons ────────────────────────────
            _render_sidebar_actions()

        st.markdown("---")

        # ── Section 4: Graph Filters (only when graph exists) ────────
        graph = st.session_state.graph
        if graph is not None:
            with st.expander("Graph Filters", expanded=True):
                _render_sidebar_graph_filters(graph)

        # ── Section 5: Load Data ─────────────────────────────────────
        with st.expander("Load Data", expanded=False):
            _render_sidebar_load_data()


def _render_sidebar_connection():
    """Connection status and connect button."""
    settings = _try_load_settings()

    if st.session_state.connected:
        envs = st.session_state.environments
        st.success(f"Connected — {len(envs)} environment(s)")
        return

    if settings:
        api_key = settings.confluent_cloud_api_key
        masked = api_key[:4] + "..." + api_key[-4:]
        st.info(f"Credentials: `{masked}`")

        if st.button(
            "Connect",
            key="connect_btn",
            type="primary",
            use_container_width=True,
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
            use_container_width=True,
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
            use_container_width=True,
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

    # Environment multiselect
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_env_labels = st.multiselect(
        "Environments",
        options=list(env_labels.keys()),
        default=[],
        key="env_multi_select",
        placeholder="Select environments...",
    )
    selected_envs = [env_labels[lbl] for lbl in selected_env_labels]

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
    selected_env_labels = st.session_state.get("env_multi_select", [])
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_envs = [env_labels[lbl] for lbl in selected_env_labels if lbl in env_labels]

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


def _render_sidebar_provisioning():
    """Auto-provision API keys via Confluent Cloud IAM APIs."""
    st.caption(
        "Automatically create scoped API keys for extraction. "
        "Keys are cached locally (encrypted) for reuse."
    )

    st.checkbox(
        "Auto-provision missing keys",
        value=False,
        key="auto_provision",
        help="When enabled, missing cluster/SR/Flink API keys will be "
        "automatically provisioned before extraction starts.",
    )

    st.text_input(
        "Key prefix",
        value="lineage-bridge",
        key="provision_prefix",
        help="All provisioned keys will be named with this prefix.",
    )

    st.checkbox(
        "Use dedicated service account",
        value=False,
        key="provision_sa",
        help="Create a dedicated service account for provisioned keys. "
        "If unchecked, keys are owned by the authenticated user.",
    )

    # Show provisioned keys count
    provisioned = KeyProvisioner.get_all_cached_keys()
    if provisioned:
        st.info(f"{len(provisioned)} provisioned key(s) cached")
        if st.button(
            "Revoke all provisioned keys",
            key="revoke_keys_btn",
            type="secondary",
            use_container_width=True,
        ):
            settings = _try_load_settings()
            if settings:
                with st.status("Revoking keys...", expanded=True) as status:
                    try:

                        async def _revoke():
                            async with _make_cloud_client(settings) as cloud:
                                provisioner = KeyProvisioner(
                                    cloud,
                                    prefix=st.session_state.get(
                                        "provision_prefix", "lineage-bridge"
                                    ),
                                )
                                await provisioner.revoke_all()

                        _run_async(_revoke())
                        status.update(
                            label=f"Revoked {len(provisioned)} key(s)",
                            state="complete",
                        )
                        st.rerun()
                    except Exception as exc:
                        status.update(
                            label=f"Failed: {exc}",
                            state="error",
                        )


def _render_sidebar_actions():
    """Extract and Refresh buttons."""
    settings = _try_load_settings()
    if not settings:
        return

    cache = st.session_state.env_cache
    all_envs = st.session_state.environments

    # Resolve selected clusters
    selected_env_labels = st.session_state.get("env_multi_select", [])
    discovered_envs = [env for env in all_envs if env.id in cache and cache[env.id].get("services")]
    env_labels = {f"{e.display_name} ({e.id})": e for e in discovered_envs}
    selected_envs = [env_labels[lbl] for lbl in selected_env_labels if lbl in env_labels]

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

    # Collect per-cluster credentials from UI inputs
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

    has_graph = st.session_state.graph is not None
    extract_label = "Re-extract" if has_graph else "Extract Lineage"

    c1, c2 = st.columns(2)
    with c1:
        if st.button(
            extract_label,
            key="extract_btn",
            type="primary",
            disabled=not selected_cluster_ids,
            use_container_width=True,
        ):
            params = {
                "env_ids": [e.id for e in selected_envs],
                "cluster_ids": selected_cluster_ids,
                "cluster_credentials": ui_cluster_creds,
                "sr_credentials": ui_sr_creds,
                "flink_credentials": ui_flink_creds,
                "enable_connect": st.session_state.get("ext_connect", True),
                "enable_ksqldb": st.session_state.get("ext_ksqldb", False),
                "enable_flink": st.session_state.get("ext_flink", False),
                "enable_schema_registry": st.session_state.get("ext_sr", False),
                "enable_stream_catalog": st.session_state.get("ext_catalog", False),
                "enable_tableflow": st.session_state.get("ext_tf", True),
                "enable_metrics": st.session_state.get("ext_metrics", False),
                "metrics_lookback_hours": st.session_state.get("metrics_lookback", 1),
            }
            st.session_state.extraction_log = []
            with st.status("Extracting lineage...", expanded=True) as status:
                try:
                    result = _run_extraction_with_params(settings, params)
                    st.session_state.graph = result
                    st.session_state.graph_version = _GRAPH_VERSION
                    st.session_state._clear_positions = True
                    st.session_state.selected_node = None
                    st.session_state.focus_node = None
                    st.session_state.last_extraction_params = params
                    st.session_state.last_extraction_time = datetime.now(UTC).strftime(
                        "%H:%M:%S UTC"
                    )
                    # Persist selections to local cache
                    _save_selections_to_cache(params)
                    status.update(
                        label=(f"Done — {result.node_count} nodes, {result.edge_count} edges"),
                        state="complete",
                    )
                    st.rerun()
                except Exception as exc:
                    status.update(
                        label=f"Failed: {exc}",
                        state="error",
                    )

    with c2:
        has_params = st.session_state.last_extraction_params is not None
        if st.button(
            "Refresh",
            key="refresh_extract_btn",
            disabled=not has_params,
            use_container_width=True,
            help="Re-run last extraction with same parameters",
        ):
            params = st.session_state.last_extraction_params
            st.session_state.extraction_log = []
            with st.status("Refreshing...", expanded=True) as status:
                try:
                    result = _run_extraction_with_params(settings, params)
                    st.session_state.graph = result
                    st.session_state.graph_version = _GRAPH_VERSION
                    st.session_state._clear_positions = True
                    st.session_state.selected_node = None
                    st.session_state.focus_node = None
                    st.session_state.last_extraction_time = datetime.now(UTC).strftime(
                        "%H:%M:%S UTC"
                    )
                    status.update(
                        label=(f"Refreshed — {result.node_count} nodes, {result.edge_count} edges"),
                        state="complete",
                    )
                    st.rerun()
                except Exception as exc:
                    status.update(
                        label=f"Failed: {exc}",
                        state="error",
                    )

    # Show extraction log
    if st.session_state.extraction_log:
        with st.expander("Extraction log", expanded=False):
            for line in st.session_state.extraction_log:
                st.markdown(line)


def _render_sidebar_graph_filters(graph: LineageGraph):
    """Type, env, cluster, search, hop filters."""
    # Node-type filters
    st.markdown("**Filter by type**")
    for ntype in NodeType:
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        count = len(graph.filter_by_type(ntype))
        if count > 0:
            st.checkbox(
                f"{label} ({count})",
                value=True,
                key=f"filter_{ntype.value}",
            )

    st.markdown("---")

    # Environment filter
    env_map: dict[str, str] = {}  # display_label -> env_id
    for n in graph.nodes:
        if n.environment_id and n.environment_id not in env_map.values():
            label = (
                f"{n.environment_name} ({n.environment_id})"
                if n.environment_name
                else n.environment_id
            )
            env_map[label] = n.environment_id
    if len(env_map) > 1:
        env_options = ["All", *sorted(env_map.keys())]
        env_sel = st.selectbox(
            "Environment",
            env_options,
            key="graph_env_filter_display",
        )
        st.session_state["graph_env_filter"] = env_map.get(env_sel) if env_sel != "All" else "All"

    # Cluster filter
    cluster_map: dict[str, str] = {}  # display_label -> cluster_id
    for n in graph.nodes:
        if n.cluster_id and n.cluster_id not in cluster_map.values():
            label = f"{n.cluster_name} ({n.cluster_id})" if n.cluster_name else n.cluster_id
            cluster_map[label] = n.cluster_id
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

    # Search
    st.text_input(
        "Search nodes",
        placeholder="Type to filter by name...",
        key="search_input",
    )

    # Hide disconnected
    st.checkbox(
        "Hide disconnected nodes",
        value=True,
        key="hide_disconnected",
        help="Hide nodes that have no edges.",
    )

    st.markdown("---")

    # Focus / hop controls
    focus_active = st.session_state.focus_node is not None
    has_search = bool(st.session_state.get("search_input", "").strip())
    st.slider(
        "Neighborhood hops",
        min_value=1,
        max_value=100,
        value=5,
        disabled=not (focus_active or has_search),
        key="hop_slider",
        help="Controls how many hops from the focused/searched node to show.",
    )
    if focus_active:
        focus_obj = graph.get_node(st.session_state.focus_node)
        fname = focus_obj.display_name if focus_obj else st.session_state.focus_node
        st.info(f"Focused on: **{fname}**")
        if st.button("Clear focus", key="clear_focus_btn"):
            st.session_state.focus_node = None
            st.rerun()

    st.markdown("---")

    # Legend
    st.markdown("**Legend**")
    for ntype in NodeType:
        icon_uri = NODE_ICONS.get(ntype, "")
        label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        st.markdown(
            f"<span class='legend-item'>"
            f"<img src='{icon_uri}' width='18' height='18' "
            f"style='vertical-align:middle; margin-right:4px;'/>"
            f"{label}</span>",
            unsafe_allow_html=True,
        )


def _render_sidebar_load_data():
    """Load from file, upload, or demo."""
    if st.button(
        "Load Demo Graph",
        key="load_demo_sidebar",
        type="primary",
        use_container_width=True,
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
