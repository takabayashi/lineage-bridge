"""Streamlit UI for LineageBridge — connect, discover, extract, visualize."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import warnings
from pathlib import Path

import streamlit as st

from lineage_bridge.models.graph import EdgeType, LineageGraph, NodeType
from lineage_bridge.ui.components.visjs_graph import visjs_graph
from lineage_bridge.ui.graph_renderer import render_graph_raw
from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.styles import (
    NODE_COLORS,
    NODE_ICONS,
    NODE_TYPE_LABELS,
)

# Suppress "coroutine was never awaited" warnings from Streamlit re-runs.
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# ── Page configuration ────────────────────────────────────────────────

st.set_page_config(
    page_title="LineageBridge",
    page_icon="\U0001f310",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state defaults ────────────────────────────────────────────

_DEFAULTS = {
    "graph": None,
    "selected_node": None,
    "focus_node": None,
    "connected": False,
    "environments": [],
    # Cache: env_id -> {services: EnvironmentServices, fetched_at: str}
    "env_cache": {},
    "extraction_log": [],
}
for _key, _default in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default


# ── Helpers ───────────────────────────────────────────────────────────


def _try_load_settings():
    """Attempt to load credentials from .env / environment variables."""
    try:
        from lineage_bridge.config.settings import Settings

        return Settings()  # type: ignore[call-arg]
    except Exception:
        return None


def _run_async(coro):
    """Run an async coroutine from sync Streamlit context.

    Preserves the Streamlit ScriptRunContext on the new event loop
    thread to suppress 'missing ScriptRunContext' warnings.
    """
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
    from datetime import UTC, datetime

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


# ── Custom CSS ────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
    /* Tighten top padding */
    .block-container { padding-top: 1.5rem; }

    /* Welcome hero card */
    .hero-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 2.5rem 3rem;
        color: #e0e0e0;
        margin-bottom: 1.5rem;
    }
    .hero-card h1 {
        color: #ffffff;
        font-size: 2.2rem;
        margin-bottom: 0.3rem;
    }
    .hero-card p {
        color: #b0bec5;
        font-size: 1.05rem;
        line-height: 1.6;
    }

    /* Feature cards on welcome page */
    .feature-card {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 10px;
        padding: 1.3rem 1.5rem;
        height: 100%;
    }
    .feature-card h4 { margin-top: 0; color: #1a1a2e; }
    .feature-card p { color: #555; font-size: 0.92rem; }

    /* Stats bar on graph view */
    div[data-testid="stMetric"] {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 0.6rem 1rem;
    }

    /* Node detail panel */
    .node-detail-panel {
        background: #fafafa;
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
    }

    /* Legend items */
    .legend-item {
        display: inline-block;
        margin-right: 1rem;
        font-size: 0.85rem;
        white-space: nowrap;
    }

    /* Contain graph iframe and prevent overflow into header */
    iframe {
        max-width: 100%;
    }
    div[data-testid="stVerticalBlock"] > div:has(iframe) {
        overflow: hidden;
        position: relative;
    }
    header[data-testid="stHeader"] {
        z-index: 999 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Routing ───────────────────────────────────────────────────────────

graph: LineageGraph | None = st.session_state.graph


# ═══════════════════════════════════════════════════════════════════════
#  VIEW 1: WELCOME + SETUP
# ═══════════════════════════════════════════════════════════════════════

def _render_welcome():
    """Render the welcome page with setup wizard."""
    # ── Hero banner ───────────────────────────────────────────────
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

    # ── Quick-start row ───────────────────────────────────────────
    qs_left, qs_right = st.columns([3, 1])
    with qs_left:
        st.markdown(
            "Get started by connecting to Confluent Cloud below, "
            "or jump straight in with sample data."
        )
    with qs_right:
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

    st.markdown("")

    # ── Main workflow tabs ────────────────────────────────────────
    (
        tab_connect,
        tab_extract,
        tab_load,
        tab_about,
    ) = st.tabs([
        "\u2460 Connect",
        "\u2461 Discover & Extract",
        "Load from File",
        "About",
    ])

    # ── Tab 1: Connect ────────────────────────────────────────────
    with tab_connect:
        _render_connect_tab()

    # ── Tab 2: Discover & Extract ─────────────────────────────────
    with tab_extract:
        _render_extract_tab()

    # ── Tab 3: Load from file ─────────────────────────────────────
    with tab_load:
        _render_load_tab()

    # ── Tab 4: About ──────────────────────────────────────────────
    with tab_about:
        _render_about_tab()


def _render_connect_tab():
    """Connection setup tab."""
    settings = _try_load_settings()

    col_info, col_action = st.columns([2, 1])

    with col_info:
        if settings:
            api_key = settings.confluent_cloud_api_key
            masked = api_key[:4] + "..." + api_key[-4:]
            st.success(
                f"Credentials detected: `{masked}`  \n"
                "Ready to connect."
            )
        else:
            st.warning(
                "No credentials found. Set the following "
                "environment variables (or add them to `.env`):"
            )
            st.code(
                "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...\n"
                "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...",
                language="bash",
            )

    with col_action:
        if settings and st.button(
            "Connect to Confluent Cloud",
            key="connect_btn",
            type="primary",
            use_container_width=True,
            disabled=st.session_state.connected,
        ):
            with st.status(
                "Connecting...", expanded=True
            ) as status:
                try:
                    from lineage_bridge.clients.discovery import (
                        list_environments,
                    )

                    async def _connect():
                        async with (
                            _make_cloud_client(settings) as cloud
                        ):
                            return await list_environments(cloud)

                    envs = _run_async(_connect())
                    st.session_state.connected = True
                    st.session_state.environments = envs
                    n = len(envs)
                    status.update(
                        label=f"Connected \u2014 {n} env(s)",
                        state="complete",
                    )
                except Exception as exc:
                    status.update(
                        label=f"Failed: {exc}",
                        state="error",
                    )
                    st.session_state.connected = False

    if st.session_state.connected:
        envs = st.session_state.environments
        st.success(
            f"Connected. {len(envs)} environment(s) found. "
            "Proceed to the **Discover & Extract** tab."
        )
        for env in envs:
            st.markdown(
                f"- **{env.display_name}** (`{env.id}`)"
            )


def _render_extract_tab():
    """Discovery + configuration + extraction tab."""
    if not st.session_state.connected:
        st.info(
            "Connect to Confluent Cloud first "
            "(use the **Connect** tab)."
        )
        return

    settings = _try_load_settings()
    if not settings:
        st.error("Settings unavailable.")
        return

    all_envs = st.session_state.environments
    cache = st.session_state.env_cache

    # ── Discovery section ─────────────────────────────────────────
    st.subheader("Discover Environments")

    all_discovered = all(env.id in cache for env in all_envs)
    c1, c2, _c3 = st.columns([1, 1, 3])
    with c1:
        if st.button(
            "Discover All",
            key="discover_all_btn",
            type="primary",
            disabled=all_discovered,
            use_container_width=True,
        ):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                if env.id not in cache:
                    try:
                        cache[env.id] = _discover_one(
                            settings, env.id
                        )
                    except Exception as exc:
                        cache[env.id] = {
                            "services": None,
                            "error": str(exc),
                        }
                bar.progress((i + 1) / len(all_envs))
            bar.empty()
            st.rerun()
    with c2:
        if st.button(
            "Refresh All",
            key="refresh_all_btn",
            use_container_width=True,
        ):
            bar = st.progress(0)
            for i, env in enumerate(all_envs):
                try:
                    cache[env.id] = _discover_one(
                        settings, env.id
                    )
                except Exception as exc:
                    cache[env.id] = {
                        "services": None,
                        "error": str(exc),
                    }
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
        st.caption(
            "No environments discovered yet. "
            "Click **Discover All** above."
        )
        return

    for env in discovered_envs:
        svc = cache[env.id]["services"]
        ts = cache[env.id].get("fetched_at", "")
        st.markdown(
            f"**{env.display_name}** (`{env.id}`) "
            f"\u2014 {_services_summary(svc)} "
            f"*[{ts}]*"
        )

    st.markdown("---")

    # ── Scope selection ───────────────────────────────────────────
    st.subheader("Configure Extraction Scope")

    scope_left, scope_right = st.columns(2)

    with scope_left:
        # Environment multi-select
        env_labels = {
            f"{e.display_name} ({e.id})": e
            for e in discovered_envs
        }
        tooltip_lines = []
        for env in discovered_envs:
            svc = cache[env.id]["services"]
            ts = cache[env.id].get("fetched_at", "")
            tooltip_lines.append(
                f"{env.display_name} ({env.id}): "
                f"{_services_summary(svc)} [{ts}]"
            )
        env_help = "\n".join(tooltip_lines)

        selected_env_labels = st.multiselect(
            "Environments",
            options=list(env_labels.keys()),
            default=[],
            key="env_multi_select",
            help=env_help,
            placeholder="Select environments...",
        )
        selected_envs = [
            env_labels[lbl] for lbl in selected_env_labels
        ]

        # Cluster multi-select
        all_cluster_options = {}
        env_for_cluster = {}
        for env in selected_envs:
            svc = cache[env.id]["services"]
            for c in svc.clusters:
                label = (
                    f"{c.display_name} ({c.id}) "
                    f"[{env.display_name}]"
                )
                all_cluster_options[label] = c
                env_for_cluster[c.id] = env.id

        if all_cluster_options:
            selected_cluster_labels = st.multiselect(
                "Clusters",
                options=list(all_cluster_options.keys()),
                default=list(all_cluster_options.keys()),
                key="cluster_select",
            )
            selected_cluster_ids = [
                all_cluster_options[lbl].id
                for lbl in selected_cluster_labels
            ]
        else:
            selected_cluster_ids = []

    with scope_right:
        # Service availability flags
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

        st.markdown("**Extractors**")
        ext_connect = st.checkbox(
            "Connectors", value=True, key="ext_connect"
        )
        ext_ksqldb = st.checkbox(
            "ksqlDB",
            value=any_ksqldb,
            disabled=not any_ksqldb,
            key="ext_ksqldb",
        )
        ext_flink = st.checkbox(
            "Flink",
            value=any_flink,
            disabled=not any_flink,
            key="ext_flink",
        )
        ext_sr = st.checkbox(
            "Schema Registry",
            value=any_sr,
            disabled=not any_sr,
            key="ext_sr",
        )
        ext_catalog = st.checkbox(
            "Stream Catalog (tags/metadata)",
            value=any_sr,
            disabled=not any_sr,
            key="ext_catalog",
        )
        ext_tableflow = st.checkbox(
            "Tableflow", value=True, key="ext_tf"
        )
        ext_metrics = st.checkbox(
            "Metrics (real-time throughput)",
            value=False,
            key="ext_metrics",
            help=(
                "Enrich nodes with live throughput data "
                "from the Metrics API. "
                "Requires MetricsViewer role."
            ),
        )
        if ext_metrics:
            metrics_lookback = st.slider(
                "Metrics lookback (hours)",
                min_value=1,
                max_value=24,
                value=1,
                key="metrics_lookback",
            )
        else:
            metrics_lookback = 1

    # ── Run extraction ────────────────────────────────────────────
    st.markdown("---")
    run_col, _, _ = st.columns([1, 1, 2])
    with run_col:
        run_disabled = not selected_cluster_ids
        if st.button(
            "Run Extraction",
            key="extract_btn",
            type="primary",
            disabled=run_disabled,
            use_container_width=True,
        ):
            st.session_state.extraction_log = []
            st.session_state.graph = None
            st.session_state.selected_node = None
            st.session_state.focus_node = None

            with st.status(
                "Extracting lineage...", expanded=True
            ) as status:
                try:
                    from lineage_bridge.extractors.orchestrator import (
                        run_extraction,
                    )

                    log = st.session_state.extraction_log

                    def on_progress(
                        phase: str, detail: str = ""
                    ) -> None:
                        log.append(f"**{phase}** {detail}")

                    env_ids = [e.id for e in selected_envs]

                    async def _do_extract():
                        return await run_extraction(
                            settings,
                            environment_ids=env_ids,
                            cluster_ids=selected_cluster_ids,
                            enable_connect=ext_connect,
                            enable_ksqldb=ext_ksqldb,
                            enable_flink=ext_flink,
                            enable_schema_registry=ext_sr,
                            enable_stream_catalog=ext_catalog,
                            enable_tableflow=ext_tableflow,
                            enable_metrics=ext_metrics,
                            metrics_lookback_hours=(
                                metrics_lookback
                            ),
                            on_progress=on_progress,
                        )

                    result = _run_async(_do_extract())
                    st.session_state.graph = result
                    n_envs = len(env_ids)
                    status.update(
                        label=(
                            f"Done \u2014 "
                            f"{result.node_count} nodes, "
                            f"{result.edge_count} edges "
                            f"across {n_envs} env(s)"
                        ),
                        state="complete",
                    )
                    st.rerun()
                except Exception as exc:
                    status.update(
                        label=f"Extraction failed: {exc}",
                        state="error",
                    )

    # Show extraction log if present
    if st.session_state.extraction_log:
        with st.expander("Extraction log", expanded=False):
            for line in st.session_state.extraction_log:
                st.markdown(line)


def _render_load_tab():
    """Load graph from file, upload, or demo data."""
    load_file, load_upload, load_demo = st.tabs(
        ["File Path", "Upload JSON", "Demo Data"]
    )

    with load_file:
        graph_path = st.text_input(
            "Graph JSON path",
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
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to parse: {exc}")

    with load_upload:
        uploaded = st.file_uploader(
            "Upload graph JSON",
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
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to parse: {exc}")

    with load_demo:
        st.markdown(
            "Load a sample graph with connectors, topics, "
            "ksqlDB, Flink, Tableflow, and Unity Catalog."
        )
        if st.button(
            "Load sample data",
            key="load_demo_btn",
            type="primary",
        ):
            st.session_state.graph = generate_sample_graph()
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()


def _render_about_tab():
    """About section."""
    st.markdown(
        """
**LineageBridge** is a proof-of-concept that discovers and
visualizes stream lineage across Confluent Cloud \u2014 connecting
Kafka topics, connectors, Flink jobs, ksqlDB queries,
Tableflow tables, and Unity Catalog into a single interactive
graph.

It uses **only public Confluent Cloud APIs** to extract lineage
signals and assemble them into a queryable, visual map of your
data flows.

---

**Created by:** Daniel Takabayashi

**Contact:**
- daniel.takabayashi@confluent.io
- [LinkedIn](https://linkedin.com/in/dtakabayashi)

**Built with:** Python, Streamlit, networkx, httpx

*This is an experimental project exploring cross-platform data
lineage between Confluent Cloud and Databricks Unity Catalog.*
        """
    )


# ═══════════════════════════════════════════════════════════════════════
#  VIEW 2: GRAPH VISUALIZATION
# ═══════════════════════════════════════════════════════════════════════

def _render_graph_view(graph: LineageGraph):
    """Full-screen graph visualization with sidebar filters."""
    # ── Sidebar: filters + controls ───────────────────────────────
    with st.sidebar:
        st.markdown("### \U0001f310 LineageBridge")
        st.caption("Graph Controls")
        st.markdown("---")

        # Node-type filters
        st.markdown("**Filter by type**")
        type_filters: dict[NodeType, bool] = {}
        for ntype in NodeType:
            label = NODE_TYPE_LABELS.get(ntype, ntype.value)
            count = len(graph.filter_by_type(ntype))
            if count > 0:
                type_filters[ntype] = st.checkbox(
                    f"{label} ({count})",
                    value=True,
                    key=f"filter_{ntype.value}",
                )
            else:
                type_filters[ntype] = False

        st.markdown("---")

        # Environment filter
        graph_envs = sorted(
            {
                n.environment_id
                for n in graph.nodes
                if n.environment_id
            }
        )
        selected_graph_env = None
        if len(graph_envs) > 1:
            graph_env_opts = ["All", *graph_envs]
            graph_env_filter = st.selectbox(
                "Environment",
                graph_env_opts,
                key="graph_env_filter",
            )
            if graph_env_filter != "All":
                selected_graph_env = graph_env_filter

        # Cluster filter
        graph_clusters = sorted(
            {n.cluster_id for n in graph.nodes if n.cluster_id}
        )
        selected_graph_cluster = None
        if len(graph_clusters) > 1:
            graph_cluster_opts = ["All", *graph_clusters]
            graph_cluster_filter = st.selectbox(
                "Cluster",
                graph_cluster_opts,
                key="graph_cluster_filter",
            )
            if graph_cluster_filter != "All":
                selected_graph_cluster = graph_cluster_filter

        # Search
        search_query = st.text_input(
            "Search nodes",
            placeholder="Type to filter by name...",
            key="search_input",
        )

        # Hide disconnected nodes
        hide_disconnected = st.checkbox(
            "Hide disconnected nodes",
            value=True,
            key="hide_disconnected",
            help="Hide nodes that have no edges.",
        )

        st.markdown("---")

        # Focus / hop controls
        focus_active = st.session_state.focus_node is not None
        hops = st.slider(
            "Neighborhood hops",
            min_value=1,
            max_value=5,
            value=2,
            disabled=not focus_active,
            key="hop_slider",
        )
        if focus_active:
            focus_obj = graph.get_node(
                st.session_state.focus_node
            )
            fname = (
                focus_obj.display_name
                if focus_obj
                else st.session_state.focus_node
            )
            st.info(f"Focused on: **{fname}**")
            if st.button(
                "Clear focus", key="clear_focus_btn"
            ):
                st.session_state.focus_node = None
                st.rerun()

        st.markdown("---")

        # Legend with SVG icons
        st.markdown("**Legend**")
        for ntype in NodeType:
            icon_uri = NODE_ICONS.get(ntype, "")
            label = NODE_TYPE_LABELS.get(ntype, ntype.value)
            st.markdown(
                f"<span class='legend-item'>"
                f"<img src='{icon_uri}' "
                f"width='18' height='18' "
                f"style='vertical-align:middle;"
                f" margin-right:4px;'/>"
                f"{label}</span>",
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # Actions
        if st.button(
            "New extraction",
            key="back_to_setup_btn",
            use_container_width=True,
        ):
            st.session_state.graph = None
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()

    # ── Main area: stats + graph ──────────────────────────────────
    # Compact header row
    header_left, header_right = st.columns([3, 1])
    with header_left:
        st.markdown("#### \U0001f310 Lineage Graph")
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
        {
            n.environment_id
            for n in graph.nodes
            if n.environment_id
        }
    )
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Nodes", graph.node_count)
    m2.metric("Edges", graph.edge_count)
    m3.metric(
        "Node Types", len({n.node_type for n in graph.nodes})
    )
    m4.metric("Environments", env_count)

    # Build filtered graph (raw dicts for custom vis.js component)
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
        # Determine layout: graph + optional detail panel (right)
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
                st.session_state.selected_node = clicked_node
                st.rerun()

        # ── Node detail panel (right side) ───────────────────────
        if detail_col is not None:
            with detail_col:
                _render_node_details(graph)


def _render_node_details(graph: LineageGraph):
    """Render selected node details as a left slide-in panel."""
    sel_id = st.session_state.selected_node
    if not sel_id:
        return

    sel_node = graph.get_node(sel_id)
    if not sel_node:
        return

    ntype_label = NODE_TYPE_LABELS.get(
        sel_node.node_type, sel_node.node_type.value
    )
    ncolor = NODE_COLORS.get(
        sel_node.node_type, "#757575"
    )

    # ── Panel header ─────────────────────────────────────────────
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
        st.session_state.selected_node = None
        st.rerun()

    # ── Core info ────────────────────────────────────────────────
    st.markdown(
        f"**Qualified Name**  \n`{sel_node.qualified_name}`"
    )
    st.markdown(f"**Node ID**  \n`{sel_node.node_id}`")

    if sel_node.environment_id:
        st.markdown(
            f"**Environment:** {sel_node.environment_id}"
        )
    if sel_node.cluster_id:
        st.markdown(
            f"**Cluster:** {sel_node.cluster_id}"
        )
    if sel_node.url:
        st.markdown(
            f"[Open in Confluent Cloud]({sel_node.url})"
        )
    if sel_node.tags:
        tag_html = " ".join(
            f"<span style='background:{ncolor};"
            f" color:white; padding:2px 8px;"
            f" border-radius:12px;"
            f" margin-right:4px;"
            f" font-size:12px;'>{t}</span>"
            for t in sel_node.tags
        )
        st.markdown(
            f"**Tags:** {tag_html}",
            unsafe_allow_html=True,
        )

    # ── Attributes ───────────────────────────────────────────────
    if sel_node.attributes:
        with st.expander("Attributes", expanded=True):
            attr_rows = [
                {"Key": k, "Value": str(v)}
                for k, v in sel_node.attributes.items()
            ]
            st.table(attr_rows)

    # ── Schema details (only for Kafka topics) ───────────────────
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
                    role = se.attributes.get(
                        "role", "value"
                    )
                    role_color = (
                        "#1976D2"
                        if role == "value"
                        else "#7B1FA2"
                    )
                    st.markdown(
                        f"<span style='background:"
                        f"{role_color};"
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
                        info_parts.append(
                            f"v{sa['version']}"
                        )
                    if sa.get("field_count"):
                        info_parts.append(
                            f"{sa['field_count']} fields"
                        )
                    if sa.get("schema_id"):
                        info_parts.append(
                            f"ID: {sa['schema_id']}"
                        )
                    if info_parts:
                        st.caption(
                            " | ".join(info_parts)
                        )
                    if idx < len(schema_edges) - 1:
                        st.markdown(
                            "<hr style='margin:4px 0;"
                            " border-color:#eee;'/>",
                            unsafe_allow_html=True,
                        )

    # ── Neighbors ────────────────────────────────────────────────
    upstream = graph.get_neighbors(
        sel_id, direction="upstream"
    )
    downstream = graph.get_neighbors(
        sel_id, direction="downstream"
    )

    nb_col1, nb_col2 = st.columns(2)
    with nb_col1:
        st.markdown(
            f"**Upstream ({len(upstream)})**"
        )
        if upstream:
            for nb in upstream:
                if st.button(
                    f"{nb.display_name}",
                    key=f"nb_up_{nb.node_id}",
                    use_container_width=True,
                ):
                    st.session_state.selected_node = (
                        nb.node_id
                    )
                    st.rerun()
        else:
            st.caption("None")

    with nb_col2:
        st.markdown(
            f"**Downstream ({len(downstream)})**"
        )
        if downstream:
            for nb in downstream:
                if st.button(
                    f"{nb.display_name}",
                    key=f"nb_dn_{nb.node_id}",
                    use_container_width=True,
                ):
                    st.session_state.selected_node = (
                        nb.node_id
                    )
                    st.rerun()
        else:
            st.caption("None")

    # ── Actions ──────────────────────────────────────────────────
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
            st.session_state.selected_node = None
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════
#  MAIN DISPATCH
# ═══════════════════════════════════════════════════════════════════════

if graph is not None:
    _render_graph_view(graph)
else:
    _render_welcome()


# ── CLI entry point ───────────────────────────────────────────────────


def run() -> None:
    """CLI entry point that launches the Streamlit app."""
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__],
        check=True,
    )
