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
from lineage_bridge.ui.graph_renderer import (
    _compute_dag_layout,
    render_graph_raw,
)
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

_GRAPH_VERSION = 3  # bump when graph model changes to invalidate caches

_DEFAULTS = {
    "graph": None,
    "graph_version": None,
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

# Invalidate stale graph from old code versions
if st.session_state.graph is not None and st.session_state.graph_version != _GRAPH_VERSION:
    st.session_state.graph = None
    st.session_state.graph_version = None
    st.session_state.selected_node = None
    st.session_state.focus_node = None

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
                    st.session_state.graph_version = _GRAPH_VERSION
                    st.session_state._clear_positions = True
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
                    st.session_state.graph_version = _GRAPH_VERSION
                    st.session_state._clear_positions = True
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
    env_map: dict[str, str] = {}  # display_label -> env_id
    for n in graph.nodes:
        if n.environment_id and n.environment_id not in env_map.values():
            label = f"{n.environment_name} ({n.environment_id})" if n.environment_name else n.environment_id
            env_map[label] = n.environment_id
    if len(env_map) > 1:
        env_options = ["All", *sorted(env_map.keys())]
        env_sel = st.selectbox(
            "Environment", env_options, key="graph_env_filter_display",
        )
        st.session_state["graph_env_filter"] = (
            env_map.get(env_sel) if env_sel != "All" else "All"
        )

    # Cluster filter
    cluster_map: dict[str, str] = {}  # display_label -> cluster_id
    for n in graph.nodes:
        if n.cluster_id and n.cluster_id not in cluster_map.values():
            label = f"{n.cluster_name} ({n.cluster_id})" if n.cluster_name else n.cluster_id
            cluster_map[label] = n.cluster_id
    if len(cluster_map) > 1:
        cluster_options = ["All", *sorted(cluster_map.keys())]
        cluster_sel = st.selectbox(
            "Cluster", cluster_options, key="graph_cluster_filter_display",
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
    if uploaded is not None and st.button(
        "Parse uploaded file", key="parse_upload_btn"
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
            st.session_state.graph_version = _GRAPH_VERSION
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
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Nodes", graph.node_count)
    m2.metric("Edges", graph.edge_count)
    m3.metric("Node Types", len({n.node_type for n in graph.nodes}))
    m4.metric("Environments", env_count)
    m5.metric("Pipelines", graph.pipeline_count)

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

            # Compute DAG layout positions (JS may override with saved positions)
            edge_pairs = [(e["from"], e["to"]) for e in vis_edges]
            positions = _compute_dag_layout(
                [n["id"] for n in vis_nodes], edge_pairs
            )
            for n in vis_nodes:
                pos = positions.get(n["id"])
                if pos:
                    n["x"] = pos["x"]
                    n["y"] = pos["y"]

            clear_positions = st.session_state.pop("_clear_positions", False)
            vis_config = {
                "layout": {"hierarchical": {"enabled": False}},
                "physics": {"enabled": False},
                "edges": {
                    "smooth": {"enabled": False},
                },
                "_clearPositions": clear_positions,
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


def _fmt_bytes_ui(val: float) -> str:
    """Format byte count to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(val) < 1024:
            return f"{val:,.1f} {unit}"
        val /= 1024
    return f"{val:,.1f} PB"


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

    a = sel_node.attributes
    cloud_url = sel_node.url or build_confluent_cloud_url(sel_node)
    ntype = sel_node.node_type

    # ── 1. Resource Info ─────────────────────────────────────────
    st.markdown(f"**Qualified Name**  \n`{sel_node.qualified_name}`")

    # Node ID — clickable link to Confluent Cloud
    if cloud_url:
        st.markdown(
            f"**ID:** <a href='{cloud_url}' target='_blank' "
            f"style='color:{ncolor};text-decoration:none;'>"
            f"<code>{sel_node.node_id}</code> &#x2197;</a>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(f"**ID:** `{sel_node.node_id}`")

    # System
    st.markdown(f"**System:** {sel_node.system.value}")

    # First/last seen
    seen_parts = []
    if sel_node.first_seen:
        seen_parts.append(f"First seen: {sel_node.first_seen:%Y-%m-%d %H:%M}")
    if sel_node.last_seen:
        seen_parts.append(f"Last seen: {sel_node.last_seen:%Y-%m-%d %H:%M}")
    if seen_parts:
        st.caption(" | ".join(seen_parts))

    # ── 2. Environment & Cluster ─────────────────────────────────
    if sel_node.environment_id or sel_node.cluster_id:
        st.markdown("---")
        st.markdown("**Location**")
        if sel_node.environment_id:
            env_base = "https://confluent.cloud/environments"
            env_link = (
                f"<a href='{env_base}/{sel_node.environment_id}' "
                f"target='_blank' style='color:{ncolor};text-decoration:none;'>"
                f"{sel_node.environment_id} &#x2197;</a>"
            )
            if sel_node.environment_name:
                st.markdown(
                    f"**Environment:** {sel_node.environment_name} ({env_link})",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f"**Environment:** {env_link}", unsafe_allow_html=True)

        if sel_node.cluster_id:
            cluster_link_url = (
                f"https://confluent.cloud/environments/{sel_node.environment_id}"
                f"/clusters/{sel_node.cluster_id}"
            ) if sel_node.environment_id else ""
            if cluster_link_url:
                cluster_link = (
                    f"<a href='{cluster_link_url}' target='_blank' "
                    f"style='color:{ncolor};text-decoration:none;'>"
                    f"{sel_node.cluster_id} &#x2197;</a>"
                )
            else:
                cluster_link = f"<code>{sel_node.cluster_id}</code>"
            if sel_node.cluster_name:
                st.markdown(
                    f"**Cluster:** {sel_node.cluster_name} ({cluster_link})",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f"**Cluster:** {cluster_link}", unsafe_allow_html=True)

    # ── 3. Type-specific attributes ──────────────────────────────
    st.markdown("---")

    if ntype == NodeType.KAFKA_TOPIC:
        st.markdown("**Topic Configuration**")
        tcol1, tcol2, tcol3 = st.columns(3)
        with tcol1:
            if a.get("partitions_count") is not None:
                st.metric("Partitions", a["partitions_count"])
        with tcol2:
            if a.get("replication_factor") is not None:
                st.metric("Replication Factor", a["replication_factor"])
        with tcol3:
            if a.get("is_internal"):
                st.markdown(
                    "<span style='color:#F44336;font-weight:600;'>Internal</span>",
                    unsafe_allow_html=True,
                )
        if a.get("description"):
            st.markdown(f"**Description:** {a['description']}")
        if a.get("owner"):
            st.markdown(f"**Owner:** {a['owner']}")

    elif ntype == NodeType.CONNECTOR:
        st.markdown("**Connector Configuration**")
        if a.get("connector_class"):
            st.markdown(f"**Class:** `{a['connector_class']}`")
        ccol1, ccol2 = st.columns(2)
        with ccol1:
            if a.get("direction"):
                dir_icon = "&#x2B07;" if a["direction"] == "sink" else "&#x2B06;"
                st.markdown(
                    f"**Direction:** {dir_icon} {a['direction'].upper()}",
                    unsafe_allow_html=True,
                )
            if a.get("tasks_max"):
                st.metric("Max Tasks", a["tasks_max"])
        with ccol2:
            if a.get("output_data_format"):
                st.markdown(f"**Output Format:** {a['output_data_format']}")

    elif ntype == NodeType.FLINK_JOB:
        st.markdown("**Flink Job**")
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            if a.get("phase"):
                phase = a["phase"]
                phase_color = "#4CAF50" if phase == "RUNNING" else "#FF9800"
                st.markdown(
                    f"**Phase:** <span style='color:{phase_color};"
                    f"font-weight:600;'>{phase}</span>",
                    unsafe_allow_html=True,
                )
            if a.get("compute_pool_id"):
                st.markdown(f"**Compute Pool:** `{a['compute_pool_id']}`")
        with fcol2:
            if a.get("principal"):
                st.markdown(f"**Principal:** `{a['principal']}`")
        if a.get("sql"):
            with st.expander("SQL Statement", expanded=False):
                st.code(a["sql"], language="sql")

    elif ntype == NodeType.KSQLDB_QUERY:
        st.markdown("**ksqlDB Query**")
        kcol1, kcol2 = st.columns(2)
        with kcol1:
            if a.get("state"):
                state = a["state"]
                state_color = "#4CAF50" if state == "RUNNING" else "#FF9800"
                st.markdown(
                    f"**State:** <span style='color:{state_color};"
                    f"font-weight:600;'>{state}</span>",
                    unsafe_allow_html=True,
                )
        with kcol2:
            if a.get("ksqldb_cluster_id"):
                st.markdown(f"**ksqlDB Cluster:** `{a['ksqldb_cluster_id']}`")
        if a.get("sql"):
            with st.expander("SQL Statement", expanded=False):
                st.code(a["sql"], language="sql")

    elif ntype == NodeType.TABLEFLOW_TABLE:
        st.markdown("**Tableflow**")
        tcol1, tcol2 = st.columns(2)
        with tcol1:
            if a.get("phase"):
                phase = a["phase"]
                phase_color = "#4CAF50" if phase == "ACTIVE" else "#FF9800"
                st.markdown(
                    f"**Phase:** <span style='color:{phase_color};"
                    f"font-weight:600;'>{phase}</span>",
                    unsafe_allow_html=True,
                )
            if a.get("table_formats"):
                fmts = a["table_formats"]
                fmt_str = ", ".join(fmts) if isinstance(fmts, list) else str(fmts)
                st.markdown(f"**Table Formats:** {fmt_str}")
        with tcol2:
            if a.get("storage_kind"):
                st.markdown(f"**Storage:** {a['storage_kind']}")
            if a.get("suspended"):
                st.markdown(
                    "**Status:** <span style='color:#F44336;"
                    "font-weight:600;'>SUSPENDED</span>",
                    unsafe_allow_html=True,
                )
        if a.get("table_path"):
            st.markdown(f"**Table Path:** `{a['table_path']}`")

    elif ntype == NodeType.UC_TABLE:
        st.markdown("**Unity Catalog Table**")
        ucol1, ucol2 = st.columns(2)
        with ucol1:
            if a.get("catalog_name"):
                st.markdown(f"**Catalog:** {a['catalog_name']}")
            if a.get("schema_name"):
                st.markdown(f"**Schema:** {a['schema_name']}")
        with ucol2:
            if a.get("table_name"):
                st.markdown(f"**Table:** {a['table_name']}")
            if a.get("catalog_type"):
                st.markdown(f"**Type:** {a['catalog_type']}")
        if a.get("workspace_url"):
            st.markdown(
                f"**Workspace:** <a href='{a['workspace_url']}' "
                f"target='_blank' style='color:{ncolor};'>"
                f"{a['workspace_url']} &#x2197;</a>",
                unsafe_allow_html=True,
            )
        if a.get("database"):
            st.markdown(f"**Database:** {a['database']}")

    elif ntype == NodeType.SCHEMA:
        st.markdown("**Schema Details**")
        scol1, scol2, scol3 = st.columns(3)
        with scol1:
            if a.get("schema_type"):
                st.metric("Format", a["schema_type"])
        with scol2:
            if a.get("version"):
                st.metric("Version", a["version"])
        with scol3:
            if a.get("field_count"):
                st.metric("Fields", a["field_count"])
        if a.get("schema_id"):
            st.markdown(f"**Schema ID:** `{a['schema_id']}`")
        # Show which topics use this schema
        schema_topics = [
            e for e in graph.edges
            if e.dst_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
        ]
        if schema_topics:
            st.markdown(f"**Used by {len(schema_topics)} topic(s):**")
            for se in schema_topics:
                topic_node = graph.get_node(se.src_id)
                if topic_node:
                    role = se.attributes.get("role", "value")
                    st.caption(f"- {topic_node.display_name} ({role})")

    elif ntype == NodeType.CONSUMER_GROUP:
        st.markdown("**Consumer Group**")
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            if a.get("state"):
                state = a["state"]
                state_color = "#4CAF50" if state in ("STABLE", "Stable") else "#FF9800"
                st.markdown(
                    f"**State:** <span style='color:{state_color};"
                    f"font-weight:600;'>{state}</span>",
                    unsafe_allow_html=True,
                )
        with gcol2:
            if a.get("is_simple") is not None:
                st.markdown(f"**Simple:** {'Yes' if a['is_simple'] else 'No'}")

    elif ntype == NodeType.EXTERNAL_DATASET:
        st.markdown("**External Dataset**")
        if a.get("inferred_from"):
            st.markdown(f"**Inferred from connector:** `{a['inferred_from']}`")

    # ── 4. Metrics ───────────────────────────────────────────────
    has_metrics = any(
        a.get(k) is not None
        for k in (
            "metrics_active", "metrics_received_records",
            "metrics_sent_records", "metrics_received_bytes",
            "metrics_sent_bytes",
        )
    )
    if has_metrics:
        st.markdown("---")
        st.markdown("**Metrics**")
        mcol1, mcol2 = st.columns(2)
        with mcol1:
            if a.get("metrics_active") is not None:
                status = "Active" if a["metrics_active"] else "Inactive"
                status_color = "#4CAF50" if a["metrics_active"] else "#F44336"
                st.markdown(
                    f"<span style='color:{status_color};font-weight:600;'>"
                    f"{'&#9679;' if a['metrics_active'] else '&#9675;'} "
                    f"{status}</span>",
                    unsafe_allow_html=True,
                )
            if a.get("metrics_received_records") is not None:
                st.metric("Records In", f"{a['metrics_received_records']:,.0f}")
            if a.get("metrics_received_bytes") is not None:
                st.metric("Bytes In", _fmt_bytes_ui(a["metrics_received_bytes"]))
        with mcol2:
            if a.get("metrics_window_hours"):
                st.caption(f"Window: {a['metrics_window_hours']}h")
            if a.get("metrics_sent_records") is not None:
                st.metric("Records Out", f"{a['metrics_sent_records']:,.0f}")
            if a.get("metrics_sent_bytes") is not None:
                st.metric("Bytes Out", _fmt_bytes_ui(a["metrics_sent_bytes"]))

    # ── 5. Schemas (for topics: associated schemas; for schema nodes: self) ──
    if ntype == NodeType.KAFKA_TOPIC:
        schema_edges = [
            e for e in graph.edges
            if e.src_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
        ]
        if schema_edges:
            st.markdown("---")
            st.markdown(f"**Schemas ({len(schema_edges)})**")
            for idx, se in enumerate(schema_edges):
                schema_node = graph.get_node(se.dst_id)
                if not schema_node:
                    continue
                role = se.attributes.get("role", "value")
                role_color = "#1976D2" if role == "value" else "#7B1FA2"
                sa = schema_node.attributes
                st.markdown(
                    f"<div style='padding:6px 10px;background:#f8f9fa;"
                    f"border-radius:6px;border-left:3px solid {role_color};"
                    f"margin:4px 0;'>"
                    f"<span style='background:{role_color};color:white;"
                    f"padding:1px 6px;border-radius:4px;font-size:11px;'>"
                    f"{role}</span> "
                    f"<strong>{schema_node.display_name}</strong>"
                    f"<br><span style='font-size:12px;color:#666;'>"
                    f"{sa.get('schema_type', '')} "
                    f"{'v' + str(sa['version']) if sa.get('version') else ''}"
                    f"{' | ' + str(sa['field_count']) + ' fields' if sa.get('field_count') else ''}"
                    f"{' | ID: ' + str(sa['schema_id']) if sa.get('schema_id') else ''}"
                    f"</span></div>",
                    unsafe_allow_html=True,
                )

    # ── 6. Tags & Metadata ───────────────────────────────────────
    if sel_node.tags:
        st.markdown("---")
        tag_html = " ".join(
            f"<span style='background:{ncolor};"
            f" color:white; padding:2px 8px;"
            f" border-radius:12px; margin-right:4px;"
            f" font-size:12px;'>{t}</span>"
            for t in sel_node.tags
        )
        st.markdown(f"**Tags:** {tag_html}", unsafe_allow_html=True)

    # ── 7. Other attributes (catch-all) ──────────────────────────
    _known_keys = {
        "partitions_count", "replication_factor", "is_internal",
        "description", "owner", "connector_class", "direction",
        "tasks_max", "output_data_format", "sql", "phase",
        "compute_pool_id", "principal", "ksqldb_cluster_id", "state",
        "table_formats", "storage_kind", "table_path", "suspended",
        "catalog_name", "schema_name", "table_name", "workspace_url",
        "catalog_type", "database", "schema_type", "version",
        "field_count", "schema_id", "is_simple", "inferred_from",
        "metrics_active", "metrics_received_records",
        "metrics_sent_records", "metrics_received_bytes",
        "metrics_sent_bytes", "metrics_window_hours",
    }
    extra_attrs = {k: v for k, v in a.items() if k not in _known_keys}
    if extra_attrs:
        with st.expander("Other Attributes", expanded=False):
            st.table([{"Key": k, "Value": str(v)} for k, v in extra_attrs.items()])

    # ── 8. Neighbors ─────────────────────────────────────────────
    st.markdown("---")
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
