"""Streamlit UI for LineageBridge — single-page architecture."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import warnings
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

from lineage_bridge.config.cache import load_cache, update_cache
from lineage_bridge.config.provisioner import KeyProvisioner
from lineage_bridge.config.settings import ClusterCredential
from lineage_bridge.models.graph import EdgeType, LineageGraph, NodeType
from lineage_bridge.ui.components.visjs_graph import visjs_graph
from lineage_bridge.ui.graph_renderer import render_graph_raw
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.styles import (
    NODE_COLORS,
    NODE_ICONS,
    NODE_TYPE_LABELS,
    build_confluent_cloud_url,
)

# Suppress "coroutine was never awaited" warnings from Streamlit re-runs.
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# ── Page configuration ────────────────────────────────────────────────

st.set_page_config(
    page_title="LineageBridge",
    page_icon="\U0001f310",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state defaults ────────────────────────────────────────────

_DEFAULTS = {
    "graph": None,
    "selected_node": None,
    "focus_node": None,
    "_dismissed_node": None,
    "connected": False,
    "environments": [],
    "env_cache": {},
    "extraction_log": [],
    "last_extraction_params": None,
    "last_extraction_time": None,
    "_cache_loaded": False,
}
for _key, _default in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

# ── Restore from local cache on first run ─────────────────────────────
if not st.session_state._cache_loaded:
    _disk_cache = load_cache()
    if _disk_cache.get("selected_envs"):
        st.session_state["_cached_selected_envs"] = _disk_cache["selected_envs"]
    if _disk_cache.get("selected_clusters"):
        st.session_state["_cached_selected_clusters"] = _disk_cache["selected_clusters"]
    if _disk_cache.get("cluster_credentials"):
        st.session_state["_cached_cluster_creds"] = _disk_cache["cluster_credentials"]
    if _disk_cache.get("sr_credentials"):
        st.session_state["_cached_sr_creds"] = _disk_cache["sr_credentials"]
    if _disk_cache.get("flink_credentials"):
        st.session_state["_cached_flink_creds"] = _disk_cache["flink_credentials"]
    if _disk_cache.get("last_extraction_params"):
        st.session_state["last_extraction_params"] = _disk_cache["last_extraction_params"]
    st.session_state._cache_loaded = True


# ── Helpers ───────────────────────────────────────────────────────────


def _try_load_settings():
    """Attempt to load credentials from .env / environment variables."""
    try:
        from lineage_bridge.config.settings import Settings

        return Settings()  # type: ignore[call-arg]
    except Exception:
        return None


def _run_async(coro):
    """Run an async coroutine from sync Streamlit context."""
    import threading

    from streamlit.runtime.scriptrunner import (
        add_script_run_ctx,
        get_script_run_ctx,
    )

    ctx = get_script_run_ctx()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if ctx is not None:
        add_script_run_ctx(threading.current_thread(), ctx)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())


def _make_cloud_client(settings):
    """Create a ConfluentClient for the Cloud API."""
    from lineage_bridge.clients.base import ConfluentClient

    return ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )


def _discover_one(settings, env_id):
    """Discover services for a single environment."""
    from lineage_bridge.clients.discovery import discover_services

    async def _do():
        async with _make_cloud_client(settings) as cloud:
            return await discover_services(cloud, env_id)

    services = _run_async(_do())
    return {
        "services": services,
        "fetched_at": datetime.now(UTC).strftime("%H:%M:%S UTC"),
    }


def _services_summary(services) -> str:
    """One-line summary of discovered services."""
    return (
        f"{len(services.clusters)} cluster(s), "
        f"SR={'Yes' if services.has_schema_registry else 'No'}, "
        f"ksqlDB={services.ksqldb_cluster_count}, "
        f"Flink={services.flink_pool_count}"
    )


def _save_selections_to_cache(params: dict) -> None:
    """Persist extraction selections + credentials to local cache."""
    cache_data: dict = {
        "selected_envs": st.session_state.get("env_multi_select", []),
        "selected_clusters": st.session_state.get("cluster_select", []),
        "last_extraction_params": params,
    }
    # Save per-cluster credentials (only non-empty ones)
    creds = params.get("cluster_credentials", {})
    if creds:
        cache_data["cluster_credentials"] = creds
    # Save per-environment SR credentials
    sr_creds = params.get("sr_credentials", {})
    if sr_creds:
        cache_data["sr_credentials"] = sr_creds
    # Save per-environment Flink credentials
    flink_creds = params.get("flink_credentials", {})
    if flink_creds:
        cache_data["flink_credentials"] = flink_creds
    update_cache(**cache_data)


def _build_sr_endpoints(params: dict) -> dict[str, str]:
    """Build a map of env_id -> SR endpoint from UI inputs + discovery cache."""
    sr_endpoints: dict[str, str] = {}
    # First, populate from discovery cache
    env_cache = st.session_state.get("env_cache", {})
    for env_id, cached in env_cache.items():
        svc = cached.get("services")
        if svc and svc.schema_registry_endpoint:
            sr_endpoints[env_id] = svc.schema_registry_endpoint
    # Override with manually-entered endpoints (take priority)
    sr_creds = params.get("sr_credentials", {})
    for env_id, cred in sr_creds.items():
        if cred.get("endpoint"):
            sr_endpoints[env_id] = cred["endpoint"]
    return sr_endpoints


async def _auto_provision_keys(
    settings,
    params: dict,
    sr_endpoints: dict[str, str],
    on_progress,
) -> dict:
    """Provision missing API keys before extraction.

    Returns an updated copy of params with provisioned credentials merged in.
    """
    from lineage_bridge.clients.base import ConfluentClient

    prefix = st.session_state.get("provision_prefix", "lineage-bridge")
    use_sa = st.session_state.get("provision_sa", False)

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    try:
        provisioner = KeyProvisioner(cloud, prefix=prefix)

        # Optionally create / find a dedicated service account
        owner_id: str | None = None
        if use_sa:
            on_progress("Provisioning", "Ensuring service account...")
            owner_id = await provisioner.ensure_service_account()
            on_progress("Provisioning", f"Service account: {owner_id}")

        updated_params = dict(params)

        # ── Provision per-cluster Kafka keys ──────────────────────
        cluster_creds = dict(params.get("cluster_credentials", {}))
        env_cache = st.session_state.get("env_cache", {})

        for env_id in params["env_ids"]:
            cached_env = env_cache.get(env_id, {})
            svc = cached_env.get("services")
            if not svc:
                continue

            for cluster in svc.clusters:
                cid = cluster.id
                # Skip if already has credentials
                if cid in cluster_creds:
                    continue
                # Skip if settings already has per-cluster or global kafka key
                existing_key, _ = settings.get_cluster_credentials(cid)
                if existing_key != settings.confluent_cloud_api_key:
                    continue

                on_progress("Provisioning", f"Kafka key for {cid}...")
                key = await provisioner.provision_cluster_key(
                    cluster_id=cid,
                    environment_id=env_id,
                    owner_id=owner_id,
                )
                cluster_creds[cid] = {
                    "api_key": key.api_key,
                    "api_secret": key.api_secret,
                }

        if cluster_creds:
            updated_params["cluster_credentials"] = cluster_creds

        # ── Provision per-env SR keys ─────────────────────────────
        sr_creds = dict(params.get("sr_credentials", {}))
        if params.get("enable_schema_registry") or params.get("enable_stream_catalog"):
            for env_id in params["env_ids"]:
                # Skip if already has per-env SR credentials
                if env_id in sr_creds and sr_creds[env_id].get("api_key"):
                    continue
                # Skip if global SR key is set
                if settings.schema_registry_api_key:
                    continue

                # Discover SR cluster ID via API
                sr_cluster_id = None
                try:
                    sr_items = await cloud.paginate(
                        "/srcm/v2/clusters",
                        params={"environment": env_id},
                    )
                    if sr_items:
                        sr_cluster_id = sr_items[0].get("id")
                except Exception:
                    pass

                if sr_cluster_id:
                    on_progress("Provisioning", f"SR key for {env_id}...")
                    key = await provisioner.provision_sr_key(
                        sr_cluster_id=sr_cluster_id,
                        environment_id=env_id,
                        owner_id=owner_id,
                    )
                    existing = sr_creds.get(env_id, {})
                    existing["api_key"] = key.api_key
                    existing["api_secret"] = key.api_secret
                    sr_creds[env_id] = existing

        if sr_creds:
            updated_params["sr_credentials"] = sr_creds

        # ── Provision per-env Flink keys ──────────────────────────
        flink_creds = dict(params.get("flink_credentials", {}))
        if params.get("enable_flink"):
            for env_id in params["env_ids"]:
                if env_id in flink_creds and flink_creds[env_id].get("api_key"):
                    continue
                if settings.flink_api_key:
                    continue

                # Discover Flink compute pool ID via API
                try:
                    flink_items = await cloud.paginate(
                        "/fcpm/v2/compute-pools",
                        params={"environment": env_id},
                    )
                    if flink_items:
                        pool_id = flink_items[0].get("id", "")
                        if pool_id:
                            on_progress("Provisioning", f"Flink key for {env_id}...")
                            key = await provisioner.provision_flink_key(
                                flink_pool_id=pool_id,
                                environment_id=env_id,
                                owner_id=owner_id,
                            )
                            flink_creds[env_id] = {
                                "api_key": key.api_key,
                                "api_secret": key.api_secret,
                            }
                except Exception:
                    pass

        if flink_creds:
            updated_params["flink_credentials"] = flink_creds

        return updated_params
    finally:
        await cloud.close()


def _run_extraction_with_params(settings, params: dict):
    """Run extraction with a params dict. Returns the graph or raises."""
    from lineage_bridge.extractors.orchestrator import run_extraction

    # Merge UI-provided per-cluster credentials into settings
    ui_creds = params.get("cluster_credentials", {})
    if ui_creds:
        merged = dict(settings.cluster_credentials)
        for cid, cred_dict in ui_creds.items():
            merged[cid] = ClusterCredential(**cred_dict)
        settings = settings.model_copy(
            update={"cluster_credentials": merged}
        )

    # Pass SR endpoints from discovery cache
    sr_endpoints = _build_sr_endpoints(params)

    log = st.session_state.extraction_log

    def on_progress(phase: str, detail: str = "") -> None:
        log.append(f"**{phase}** {detail}")

    async def _do_extract():
        nonlocal params, settings

        # Auto-provision missing keys if enabled
        if st.session_state.get("auto_provision", False):
            on_progress("Provisioning", "Checking for missing API keys...")
            params = await _auto_provision_keys(
                settings, params, sr_endpoints, on_progress
            )
            # Re-merge cluster credentials after provisioning
            prov_creds = params.get("cluster_credentials", {})
            if prov_creds:
                merged = dict(settings.cluster_credentials)
                for cid, cred_dict in prov_creds.items():
                    merged[cid] = ClusterCredential(**cred_dict)
                settings = settings.model_copy(
                    update={"cluster_credentials": merged}
                )
            on_progress("Provisioning", "Key provisioning complete")

        return await run_extraction(
            settings,
            environment_ids=params["env_ids"],
            cluster_ids=params["cluster_ids"],
            enable_connect=params["enable_connect"],
            enable_ksqldb=params["enable_ksqldb"],
            enable_flink=params["enable_flink"],
            enable_schema_registry=params["enable_schema_registry"],
            enable_stream_catalog=params["enable_stream_catalog"],
            enable_tableflow=params["enable_tableflow"],
            enable_metrics=params["enable_metrics"],
            metrics_lookback_hours=params["metrics_lookback_hours"],
            sr_endpoints=_build_sr_endpoints(params),
            sr_credentials=params.get("sr_credentials"),
            flink_credentials=params.get("flink_credentials"),
            on_progress=on_progress,
        )

    return _run_async(_do_extract())


# ── Custom CSS ────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    /* Tighten top padding */
    .block-container { padding-top: 1.5rem; }

    /* Stats bar on graph view */
    div[data-testid="stMetric"] {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 0.6rem 1rem;
    }

    /* Node detail panel — slide in from right */
    @keyframes slideInRight {
        from { opacity: 0; transform: translateX(30px); }
        to   { opacity: 1; transform: translateX(0); }
    }
    div[data-testid="stColumns"] > div:last-child {
        animation: slideInRight 0.3s ease-out;
    }

    /* Legend items */
    .legend-item {
        display: inline-block;
        margin-right: 1rem;
        font-size: 0.85rem;
        white-space: nowrap;
    }

    /* Contain graph iframe — scope to main area only */
    .main iframe { max-width: 100%; }
    .main div[data-testid="stVerticalBlock"] > div:has(iframe) {
        overflow: hidden;
        position: relative;
    }
    header[data-testid="stHeader"] {
        z-index: 999 !important;
    }

    /* Sidebar: ensure scrolling works */
    section[data-testid="stSidebar"] > div:first-child {
        overflow-y: auto !important;
        max-height: 100vh;
    }

    /* Empty state */
    .hero-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 2.5rem 3rem;
        color: #e0e0e0;
        margin-bottom: 1.5rem;
    }
    .hero-card h1 { color: #ffffff; font-size: 2.2rem; margin-bottom: 0.3rem; }
    .hero-card p { color: #b0bec5; font-size: 1.05rem; line-height: 1.6; }

    /* Step tracker */
    .step-tracker {
        display: flex; align-items: center; gap: 0.5rem;
        margin: 1.5rem 0; flex-wrap: wrap;
    }
    .step-item {
        display: flex; align-items: center; gap: 0.4rem;
        padding: 0.5rem 1rem; border-radius: 8px; font-size: 0.9rem;
    }
    .step-done { background: #e8f5e9; color: #2e7d32; }
    .step-current { background: #e3f2fd; color: #1565c0; font-weight: 600; }
    .step-pending { background: #f5f5f5; color: #9e9e9e; }
    .step-arrow { color: #bdbdbd; font-size: 1.2rem; }

    /* Extraction time badge */
    .extraction-time {
        font-size: 0.8rem; color: #757575;
        background: #f5f5f5; padding: 2px 8px; border-radius: 4px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═══════════════════════════════════════════════════════════════════════


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
        st.success(
            f"Connected — {len(envs)} environment(s)"
        )
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
                    from lineage_bridge.clients.discovery import (
                        list_environments,
                    )

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
    discovered_envs = [
        env
        for env in all_envs
        if env.id in cache and cache[env.id].get("services")
    ]

    if not discovered_envs:
        st.caption("Click **Discover** to find services.")
        return

    # Environment multiselect
    env_labels = {
        f"{e.display_name} ({e.id})": e
        for e in discovered_envs
    }
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
                    f"Schema Registry Endpoint",
                    key=sr_endpoint_key,
                    placeholder="e.g. https://psrc-xxxxx.region.cloud.confluent.cloud",
                )
                st.text_input(
                    f"Schema Registry Key",
                    key=sr_key_key,
                    type="password",
                    placeholder="Leave blank for global key",
                )
                st.text_input(
                    f"Schema Registry Secret",
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
                    f"Flink Key",
                    key=flink_key_key,
                    type="password",
                    placeholder="Leave blank for global key",
                    disabled=not has_flink,
                )
                st.text_input(
                    f"Flink Secret",
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
            for label, cluster in all_cluster_options.items():
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
    discovered_envs = [
        env
        for env in all_envs
        if env.id in cache and cache[env.id].get("services")
    ]
    env_labels = {
        f"{e.display_name} ({e.id})": e
        for e in discovered_envs
    }
    selected_envs = [
        env_labels[lbl]
        for lbl in selected_env_labels
        if lbl in env_labels
    ]

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
    discovered_envs = [
        env
        for env in all_envs
        if env.id in cache and cache[env.id].get("services")
    ]
    env_labels = {
        f"{e.display_name} ({e.id})": e
        for e in discovered_envs
    }
    selected_envs = [
        env_labels[lbl]
        for lbl in selected_env_labels
        if lbl in env_labels
    ]

    all_cluster_options = {}
    for env in selected_envs:
        svc = cache[env.id]["services"]
        for c in svc.clusters:
            label = f"{c.display_name} ({c.id})"
            all_cluster_options[label] = c

    selected_cluster_labels = st.session_state.get("cluster_select", [])
    selected_cluster_ids = [
        all_cluster_options[lbl].id
        for lbl in selected_cluster_labels
        if lbl in all_cluster_options
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
                    st.session_state.selected_node = None
                    st.session_state.focus_node = None
                    st.session_state.last_extraction_params = params
                    st.session_state.last_extraction_time = (
                        datetime.now(UTC).strftime("%H:%M:%S UTC")
                    )
                    # Persist selections to local cache
                    _save_selections_to_cache(params)
                    status.update(
                        label=(
                            f"Done — {result.node_count} nodes, "
                            f"{result.edge_count} edges"
                        ),
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
                    st.session_state.selected_node = None
                    st.session_state.focus_node = None
                    st.session_state.last_extraction_time = (
                        datetime.now(UTC).strftime("%H:%M:%S UTC")
                    )
                    status.update(
                        label=(
                            f"Refreshed — {result.node_count} nodes, "
                            f"{result.edge_count} edges"
                        ),
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
    graph_envs = sorted(
        {n.environment_id for n in graph.nodes if n.environment_id}
    )
    if len(graph_envs) > 1:
        st.selectbox(
            "Environment",
            ["All", *graph_envs],
            key="graph_env_filter",
        )

    # Cluster filter
    graph_clusters = sorted(
        {n.cluster_id for n in graph.nodes if n.cluster_id}
    )
    if len(graph_clusters) > 1:
        st.selectbox(
            "Cluster",
            ["All", *graph_clusters],
            key="graph_cluster_filter",
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
        fname = (
            focus_obj.display_name
            if focus_obj
            else st.session_state.focus_node
        )
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
    if uploaded is not None and st.button(
        "Parse uploaded file", key="parse_upload_btn"
    ):
        try:
            data = json.loads(uploaded.getvalue())
            g = LineageGraph.from_dict(data)
            st.session_state.graph = g
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.session_state.last_extraction_params = None
            st.rerun()
        except Exception as exc:
            st.error(f"Failed to parse: {exc}")


# ═══════════════════════════════════════════════════════════════════════
#  MAIN AREA
# ═══════════════════════════════════════════════════════════════════════


def _render_main_area():
    """Main content: graph or empty state."""
    graph = st.session_state.graph
    if graph is not None:
        _render_graph_content(graph)
    else:
        _render_empty_state()


def _render_empty_state():
    """Welcome/empty state with step tracker."""
    st.markdown(
        """
        <div class="hero-card">
            <h1>\U0001f310 LineageBridge</h1>
            <p>
                Discover and visualize stream lineage across
                Confluent Cloud &mdash; Kafka topics, connectors,
                Flink jobs, ksqlDB queries, Tableflow, and Unity
                Catalog in one interactive directed graph.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Step tracker
    connected = st.session_state.connected
    has_envs = bool(st.session_state.get("env_multi_select"))

    step1_cls = "step-done" if connected else "step-current"
    step1_icon = "\u2713" if connected else "1"
    step2_cls = (
        "step-done" if has_envs
        else ("step-current" if connected else "step-pending")
    )
    step2_icon = "\u2713" if has_envs else "2"
    step3_cls = (
        "step-current" if has_envs
        else "step-pending"
    )

    st.markdown(
        f"""
        <div class="step-tracker">
            <div class="step-item {step1_cls}">
                <strong>{step1_icon}</strong> Connect
            </div>
            <span class="step-arrow">\u2192</span>
            <div class="step-item {step2_cls}">
                <strong>{step2_icon}</strong> Select Infrastructure
            </div>
            <span class="step-arrow">\u2192</span>
            <div class="step-item {step3_cls}">
                <strong>3</strong> Extract
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "Use the **sidebar** to connect, select environments, "
        "and extract lineage. Or load a demo graph to explore."
    )

    _, center, _ = st.columns([1, 1, 1])
    with center:
        if st.button(
            "\u26a1 Load Demo Graph",
            key="hero_demo_btn",
            type="primary",
            use_container_width=True,
        ):
            st.session_state.graph = generate_sample_graph()
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()

    st.markdown("---")

    st.markdown(
        """
**LineageBridge** is a proof-of-concept that discovers and
visualizes stream lineage across Confluent Cloud — connecting
Kafka topics, connectors, Flink jobs, ksqlDB queries,
Tableflow tables, and Unity Catalog into a single interactive graph.

**Created by:** Daniel Takabayashi
**Built with:** Python, Streamlit, networkx, httpx
        """
    )


def _render_graph_content(graph: LineageGraph):
    """Graph visualization with stats and detail panel."""
    # Header row
    header_left, header_right = st.columns([3, 1])
    with header_left:
        title_parts = ["#### \U0001f310 Lineage Graph"]
        if st.session_state.last_extraction_time:
            title_parts.append(
                f"<span class='extraction-time'>"
                f"Last extracted: {st.session_state.last_extraction_time}"
                f"</span>"
            )
        st.markdown(" ".join(title_parts), unsafe_allow_html=True)
    with header_right:
        export_data = json.dumps(
            graph.to_dict(), indent=2, default=str
        )
        st.download_button(
            label="\U0001f4e5 Export JSON",
            data=export_data,
            file_name="lineage_graph.json",
            mime="application/json",
            key="export_json_btn",
        )

    # Stats bar
    env_count = len(
        {n.environment_id for n in graph.nodes if n.environment_id}
    )
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Nodes", graph.node_count)
    m2.metric("Edges", graph.edge_count)
    m3.metric("Node Types", len({n.node_type for n in graph.nodes}))
    m4.metric("Environments", env_count)

    # Read filter values from sidebar widgets
    type_filters: dict[NodeType, bool] = {}
    for ntype in NodeType:
        key = f"filter_{ntype.value}"
        type_filters[ntype] = st.session_state.get(key, True)

    search_query = st.session_state.get("search_input", "")
    hops = st.session_state.get("hop_slider", 5)
    hide_disconnected = st.session_state.get("hide_disconnected", True)

    selected_graph_env = st.session_state.get("graph_env_filter")
    if selected_graph_env == "All":
        selected_graph_env = None
    selected_graph_cluster = st.session_state.get("graph_cluster_filter")
    if selected_graph_cluster == "All":
        selected_graph_cluster = None

    # Build filtered graph
    vis_nodes, vis_edges = render_graph_raw(
        graph,
        filters=type_filters,
        search_query=search_query,
        selected_node=st.session_state.focus_node,
        hops=hops,
        environment_filter=selected_graph_env,
        cluster_filter=selected_graph_cluster,
        hide_disconnected=hide_disconnected,
    )

    if not vis_nodes:
        st.warning(
            "No nodes match the current filters. "
            "Adjust filters in the sidebar."
        )
    else:
        # Layout: graph + optional detail panel
        has_selection = st.session_state.selected_node is not None
        if has_selection:
            graph_col, detail_col = st.columns([3, 2])
        else:
            graph_col = st.container()
            detail_col = None

        with graph_col:
            st.caption(
                f"Showing {len(vis_nodes)} of "
                f"{graph.node_count} nodes, "
                f"{len(vis_edges)} edges"
            )

            vis_config = {
                "layout": {
                    "hierarchical": {
                        "enabled": True,
                        "levelSeparation": 250,
                        "nodeSpacing": 120,
                        "treeSpacing": 150,
                        "direction": "LR",
                        "sortMethod": "directed",
                        "shakeTowards": "roots",
                    }
                },
                "physics": {"enabled": False},
            }

            clicked_node = visjs_graph(
                nodes=vis_nodes,
                edges=vis_edges,
                config=vis_config,
                height=650,
                key="lineage_graph",
            )

            if clicked_node:
                dismissed = st.session_state._dismissed_node
                if clicked_node == dismissed:
                    pass
                elif clicked_node != st.session_state.selected_node:
                    st.session_state.selected_node = clicked_node
                    st.session_state._dismissed_node = None
                    st.rerun()

        # Node detail panel
        if detail_col is not None:
            with detail_col:
                _render_node_details(graph)


def _render_node_details(graph: LineageGraph):
    """Render selected node details panel."""
    sel_id = st.session_state.selected_node
    if not sel_id:
        return

    sel_node = graph.get_node(sel_id)
    if not sel_node:
        return

    ntype_label = NODE_TYPE_LABELS.get(
        sel_node.node_type, sel_node.node_type.value
    )
    ncolor = NODE_COLORS.get(sel_node.node_type, "#757575")

    # Panel header
    st.markdown(
        f"<div style='background:linear-gradient(135deg,"
        f" {ncolor}22 0%, {ncolor}08 100%);"
        f" border-left:4px solid {ncolor};"
        f" border-radius:0 8px 8px 0;"
        f" padding:12px 14px; margin-bottom:12px;'>"
        f"<div style='font-size:11px; color:{ncolor};"
        f" font-weight:600; text-transform:uppercase;"
        f" letter-spacing:0.5px;'>{ntype_label}</div>"
        f"<div style='font-size:16px; font-weight:700;"
        f" color:#1a1a2e; margin-top:2px;'>"
        f"{sel_node.display_name}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if st.button(
        "Close", key="close_detail_btn",
        use_container_width=True,
    ):
        st.session_state._dismissed_node = sel_id
        st.session_state.selected_node = None
        st.rerun()

    # Core info
    st.markdown(
        f"**Qualified Name**  \n`{sel_node.qualified_name}`"
    )
    st.markdown(f"**Node ID**  \n`{sel_node.node_id}`")

    if sel_node.environment_id:
        st.markdown(
            f"**Environment:** {sel_node.environment_id}"
        )
    if sel_node.cluster_id:
        st.markdown(f"**Cluster:** {sel_node.cluster_id}")
    # Generate Confluent Cloud deep link if not already set
    cloud_url = sel_node.url or build_confluent_cloud_url(sel_node)
    if cloud_url:
        st.markdown(
            f"<a href='{cloud_url}' target='_blank' "
            f"style='display:inline-block;margin:6px 0 4px 0;"
            f"padding:5px 12px;background:{ncolor};color:#fff;"
            f"border-radius:6px;font-size:13px;"
            f"text-decoration:none;font-weight:500;'>"
            f"Open in Confluent Cloud &#x2197;</a>",
            unsafe_allow_html=True,
        )
    if sel_node.tags:
        tag_html = " ".join(
            f"<span style='background:{ncolor};"
            f" color:white; padding:2px 8px;"
            f" border-radius:12px; margin-right:4px;"
            f" font-size:12px;'>{t}</span>"
            for t in sel_node.tags
        )
        st.markdown(
            f"**Tags:** {tag_html}",
            unsafe_allow_html=True,
        )

    # Attributes
    if sel_node.attributes:
        with st.expander("Attributes", expanded=True):
            attr_rows = [
                {"Key": k, "Value": str(v)}
                for k, v in sel_node.attributes.items()
            ]
            st.table(attr_rows)

    # Schema details (only for Kafka topics)
    if sel_node.node_type == NodeType.KAFKA_TOPIC:
        schema_edges = [
            e
            for e in graph.edges
            if e.src_id == sel_id
            and e.edge_type == EdgeType.HAS_SCHEMA
        ]
        if schema_edges:
            with st.expander(
                f"Schemas ({len(schema_edges)})",
                expanded=True,
            ):
                for idx, se in enumerate(schema_edges):
                    schema_node = graph.get_node(se.dst_id)
                    if not schema_node:
                        continue
                    role = se.attributes.get("role", "value")
                    role_color = (
                        "#1976D2" if role == "value" else "#7B1FA2"
                    )
                    st.markdown(
                        f"<span style='background:{role_color};"
                        f" color:white; padding:1px 6px;"
                        f" border-radius:4px;"
                        f" font-size:11px;'>"
                        f"{role}</span>"
                        f" **{schema_node.display_name}**",
                        unsafe_allow_html=True,
                    )
                    sa = schema_node.attributes
                    info_parts = []
                    if sa.get("schema_type"):
                        info_parts.append(
                            f"Format: {sa['schema_type']}"
                        )
                    if sa.get("version"):
                        info_parts.append(f"v{sa['version']}")
                    if sa.get("field_count"):
                        info_parts.append(
                            f"{sa['field_count']} fields"
                        )
                    if sa.get("schema_id"):
                        info_parts.append(
                            f"ID: {sa['schema_id']}"
                        )
                    if info_parts:
                        st.caption(" | ".join(info_parts))
                    if idx < len(schema_edges) - 1:
                        st.markdown(
                            "<hr style='margin:4px 0;"
                            " border-color:#eee;'/>",
                            unsafe_allow_html=True,
                        )

    # Neighbors
    upstream = graph.get_neighbors(sel_id, direction="upstream")
    downstream = graph.get_neighbors(sel_id, direction="downstream")

    nb_col1, nb_col2 = st.columns(2)
    with nb_col1:
        st.markdown(f"**Upstream ({len(upstream)})**")
        if upstream:
            for nb in upstream:
                if st.button(
                    f"{nb.display_name}",
                    key=f"nb_up_{nb.node_id}",
                    use_container_width=True,
                ):
                    st.session_state.selected_node = nb.node_id
                    st.rerun()
        else:
            st.caption("None")

    with nb_col2:
        st.markdown(f"**Downstream ({len(downstream)})**")
        if downstream:
            for nb in downstream:
                if st.button(
                    f"{nb.display_name}",
                    key=f"nb_dn_{nb.node_id}",
                    use_container_width=True,
                ):
                    st.session_state.selected_node = nb.node_id
                    st.rerun()
        else:
            st.caption("None")

    # Actions
    st.markdown("---")
    act1, act2 = st.columns(2)
    with act1:
        if st.button(
            "Focus on this node",
            key="focus_btn",
            use_container_width=True,
        ):
            st.session_state.focus_node = sel_id
            st.rerun()
    with act2:
        if st.button(
            "Clear selection",
            key="clear_sel_btn",
            use_container_width=True,
        ):
            st.session_state._dismissed_node = sel_id
            st.session_state.selected_node = None
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════
#  MAIN DISPATCH
# ═══════════════════════════════════════════════════════════════════════

_render_sidebar()
_render_main_area()


# ── CLI entry point ───────────────────────────────────────────────────


def run() -> None:
    """CLI entry point that launches the Streamlit app."""
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__],
        check=True,
    )
