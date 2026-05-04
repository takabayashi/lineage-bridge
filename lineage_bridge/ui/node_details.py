# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Node detail panel rendering for the LineageBridge UI.

Phase D redesign:
- Single dismiss action (top-right "Close" only; "Clear selection" at the
  bottom was removed — same effect, two locations was confusing).
- Section order: Identity → Location → Type-specific → Tags → Metrics →
  Schemas → Neighbours → Other Attributes → Actions. Tags and metrics now
  sit next to identity instead of after a long scroll past neighbours.
- Status pills (RUNNING/PAUSED/FAILED…) route through one
  `render_status_badge_html` in `styles.py` instead of six separate inline
  ternaries that drifted from `STATUS_BADGE_MAP` every time it changed.
- Neighbour buttons show a unicode marker for the neighbour's node type so
  users can tell topic/connector/Flink apart at a glance.
- "Open in <catalog>" deep-link surfaces a hint when unavailable instead of
  silently disappearing.
"""

from __future__ import annotations

import re
from collections import defaultdict

import streamlit as st

from lineage_bridge.models.graph import EdgeType, LineageGraph, LineageNode, NodeType
from lineage_bridge.ui.styles import (
    NODE_TYPE_LABELS,
    build_node_url,
    clean_display_name,
    color_for_node,
    icon_for_node,
    label_for_node,
    render_status_badge_html,
)


def _fmt_bytes_ui(val: float) -> str:
    """Format byte count to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(val) < 1024:
            return f"{val:,.1f} {unit}"
        val /= 1024
    return f"{val:,.1f} PB"


def _status_line(label: str, status: str | None) -> None:
    """Render a labelled status pill, e.g. 'Phase: [RUNNING]'."""
    if not status:
        return
    badge = render_status_badge_html(status)
    st.markdown(
        f"<div style='margin:4px 0'><strong>{label}:</strong> {badge}</div>",
        unsafe_allow_html=True,
    )


def render_node_details(graph: LineageGraph) -> None:
    """Render selected node details panel."""
    sel_id = st.session_state.selected_node
    if not sel_id:
        return

    sel_node = graph.get_node(sel_id)
    if not sel_node:
        return

    ntype_label = label_for_node(sel_node)
    ncolor = color_for_node(sel_node)
    a = sel_node.attributes
    cloud_url = sel_node.url or build_node_url(sel_node)
    ntype = sel_node.node_type
    cleaned_name = clean_display_name(sel_node.display_name)
    type_icon_svg = icon_for_node(sel_node)

    # ── Header: type icon + name + status pill + close-X ─────────
    status_value = a.get("phase") or a.get("state")
    if not status_value and a.get("suspended"):
        status_value = "SUSPENDED"
    status_pill = render_status_badge_html(status_value) if status_value else ""

    h_left, h_right = st.columns([8, 1])
    with h_left:
        # Use the same SVG icon the legend / canvas uses so the panel
        # header matches the visual identity in the graph (was a unicode
        # marker that looked different from the actual node icon).
        icon_img = (
            f"<img src='{type_icon_svg}' width='20' height='20' "
            f"style='vertical-align:middle;margin-right:4px'/>"
            if type_icon_svg
            else ""
        )
        st.markdown(
            f"<div class='detail-head' style='border-left-color:{ncolor};"
            f"background:linear-gradient(135deg, {ncolor}22 0%, {ncolor}08 100%);'>"
            f"<div class='detail-head-row'>"
            f"{icon_img}"
            f"<span class='detail-head-type' style='color:{ncolor}'>{ntype_label}</span>"
            f"<span class='detail-head-status'>{status_pill}</span>"
            f"</div>"
            f"<div class='detail-head-name' title='{cleaned_name}'>"
            f"{cleaned_name}"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with h_right:
        if st.button("✕", key="close_detail_btn", help="Close panel"):
            st.session_state._dismissed_node = sel_id
            st.session_state.selected_node = None
            st.rerun()

    # ── Action toolbar (Focus + Open ↗; copy lives inline next to ID) ──
    _render_action_toolbar(sel_node, sel_id, cloud_url)

    # ── 1. Identity ──────────────────────────────────────────────
    # st.code blocks come with Streamlit's built-in copy-on-hover button —
    # one inline icon on each value, no separate Copy buttons in the toolbar.
    st.markdown(
        "<div class='detail-id-label'>Qualified name</div>",
        unsafe_allow_html=True,
    )
    st.code(sel_node.qualified_name, language="text")

    st.markdown(
        "<div class='detail-id-label'>Node ID</div>",
        unsafe_allow_html=True,
    )
    st.code(sel_node.node_id, language="text")
    if cloud_url:
        st.markdown(
            f"<a href='{cloud_url}' target='_blank' "
            f"style='color:{ncolor};text-decoration:none;font-size:0.78rem;'>"
            f"Open in source console &#x2197;</a>",
            unsafe_allow_html=True,
        )

    seen_parts = [f"System: {sel_node.system.value}"]
    if sel_node.first_seen:
        seen_parts.append(f"First seen {sel_node.first_seen:%Y-%m-%d %H:%M}")
    if sel_node.last_seen:
        seen_parts.append(f"Last seen {sel_node.last_seen:%Y-%m-%d %H:%M}")
    st.caption(" · ".join(seen_parts))

    # ── 2. Tags (moved up to live next to identity) ──────────────
    if sel_node.tags:
        tag_html = " ".join(
            f"<span style='background:{ncolor};"
            f" color:white; padding:2px 8px;"
            f" border-radius:12px; margin-right:4px;"
            f" font-size:12px;'>{t}</span>"
            for t in sel_node.tags
        )
        st.markdown(f"**Tags:** {tag_html}", unsafe_allow_html=True)

    # ── 3. Location ──────────────────────────────────────────────
    if sel_node.environment_id or sel_node.cluster_id:
        st.markdown("---")
        st.markdown("**Location**")
        if sel_node.environment_id:
            env_link = (
                f"<a href='https://confluent.cloud/environments/{sel_node.environment_id}' "
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
                (
                    f"https://confluent.cloud/environments/{sel_node.environment_id}"
                    f"/clusters/{sel_node.cluster_id}"
                )
                if sel_node.environment_id
                else ""
            )
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

    # ── 4. Type-specific attributes ──────────────────────────────
    st.markdown("---")
    _render_type_specific(graph, sel_node, sel_id, ntype, a, ncolor)

    # ── 5. Metrics ───────────────────────────────────────────────
    _render_metrics_section(a)

    # ── 6. Schemas ───────────────────────────────────────────────
    if ntype == NodeType.KAFKA_TOPIC:
        _render_topic_schemas(graph, sel_id, ncolor)

    # ── 7. Neighbours ────────────────────────────────────────────
    _render_neighbours(graph, sel_id)

    # ── 8. Other attributes (catch-all) ──────────────────────────
    _render_other_attributes(a)

    # Actions live in the action toolbar at the top — no second action row
    # needed here. Keeping this section anchor for future-only additions.


_METRIC_KEYS: tuple[str, ...] = (
    "metrics_active",
    "metrics_received_records",
    "metrics_sent_records",
    "metrics_received_bytes",
    "metrics_sent_bytes",
    "metrics_total_lag",
    "metrics_window_hours",
)


def _render_metrics_section(a: dict) -> None:
    """Render whatever `metrics_*` attributes are present on the node.

    Surfaces the standard counter set (records / bytes in/out + active),
    the consumer-group-specific `metrics_total_lag`, and falls back to a
    generic key/value list for any other `metrics_*` value the node carries
    (e.g. catalog-table-derived counts) so nothing the enrichment phase
    populates is silently dropped.
    """
    if not any(a.get(k) is not None for k in _METRIC_KEYS):
        return

    st.markdown("---")
    st.markdown("**Metrics**")

    mcol1, mcol2 = st.columns(2)
    with mcol1:
        if a.get("metrics_active") is not None:
            st.markdown(
                render_status_badge_html("ACTIVE" if a["metrics_active"] else "STOPPED"),
                unsafe_allow_html=True,
            )
        if a.get("metrics_received_records") is not None:
            st.metric("Records In", f"{a['metrics_received_records']:,.0f}")
        if a.get("metrics_received_bytes") is not None:
            st.metric("Bytes In", _fmt_bytes_ui(a["metrics_received_bytes"]))
        if a.get("metrics_total_lag") is not None:
            st.metric("Total lag", f"{int(a['metrics_total_lag']):,}")
    with mcol2:
        if a.get("metrics_window_hours"):
            st.caption(f"Window: {a['metrics_window_hours']}h")
        if a.get("metrics_sent_records") is not None:
            st.metric("Records Out", f"{a['metrics_sent_records']:,.0f}")
        if a.get("metrics_sent_bytes") is not None:
            st.metric("Bytes Out", _fmt_bytes_ui(a["metrics_sent_bytes"]))

    # Catch-all for any other `metrics_*` attribute we don't have an explicit
    # tile for — e.g. catalog-derived freshness, custom enrichers.
    explicit = set(_METRIC_KEYS)
    extras = {
        k: v
        for k, v in a.items()
        if k.startswith("metrics_") and k not in explicit and v is not None
    }
    if extras:
        st.caption("Other metrics")
        st.table(
            [
                {"Key": k.removeprefix("metrics_").replace("_", " ").title(), "Value": str(v)}
                for k, v in extras.items()
            ]
        )


def _render_action_toolbar(sel_node: LineageNode, sel_id: str, cloud_url: str | None) -> None:
    """Toolbar of primary actions. Copy is handled inline via st.code in
    the identity section so we only need Focus and (optionally) Open here.
    """
    del sel_node  # unused now (copy was the only consumer)
    is_focused = st.session_state.get("focus_node") == sel_id
    cols = st.columns(2 if cloud_url else 1)

    with cols[0]:
        focus_label = "Unfocus" if is_focused else "Focus on this node"
        if st.button(
            focus_label,
            key="action_focus",
            width="stretch",
            type="primary" if not is_focused else "secondary",
            help="Zoom the graph to this node's neighborhood",
        ):
            st.session_state.focus_node = None if is_focused else sel_id
            st.rerun()

    if cloud_url:
        with cols[1]:
            st.link_button(
                "Open ↗",
                url=cloud_url,
                width="stretch",
                help="Open this resource in its source console",
            )


def _render_type_specific(
    graph: LineageGraph,
    sel_node,
    sel_id: str,
    ntype: NodeType,
    a: dict,
    ncolor: str,
) -> None:
    """Type-specific attribute block."""
    if ntype == NodeType.KAFKA_TOPIC:
        st.markdown("**Topic configuration**")
        tcol1, tcol2, tcol3 = st.columns(3)
        with tcol1:
            if a.get("partitions_count") is not None:
                st.metric("Partitions", a["partitions_count"])
        with tcol2:
            if a.get("replication_factor") is not None:
                st.metric("Replication factor", a["replication_factor"])
        with tcol3:
            if a.get("is_internal"):
                st.markdown(
                    render_status_badge_html("INTERNAL"),
                    unsafe_allow_html=True,
                )
        if a.get("description"):
            st.markdown(f"**Description:** {a['description']}")
        if a.get("owner"):
            st.markdown(f"**Owner:** {a['owner']}")

    elif ntype == NodeType.CONNECTOR:
        st.markdown("**Connector configuration**")
        if a.get("connector_class"):
            st.markdown(f"**Class:** `{a['connector_class']}`")
        ccol1, ccol2 = st.columns(2)
        with ccol1:
            if a.get("direction"):
                dir_arrow = "↓" if a["direction"] == "sink" else "↑"
                st.markdown(f"**Direction:** {dir_arrow} {a['direction'].upper()}")
            if a.get("tasks_max"):
                st.metric("Max Tasks", a["tasks_max"])
        with ccol2:
            if a.get("output_data_format"):
                st.markdown(f"**Output Format:** {a['output_data_format']}")

    elif ntype == NodeType.FLINK_JOB:
        st.markdown("**Flink job**")
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            _status_line("Phase", a.get("phase"))
            if a.get("compute_pool_id"):
                st.markdown(f"**Compute Pool:** `{a['compute_pool_id']}`")
        with fcol2:
            if a.get("principal"):
                st.markdown(f"**Principal:** `{a['principal']}`")
        if a.get("sql"):
            with st.expander("SQL Statement", expanded=False):
                st.code(a["sql"], language="sql")

    elif ntype == NodeType.KSQLDB_QUERY:
        st.markdown("**ksqlDB query**")
        kcol1, kcol2 = st.columns(2)
        with kcol1:
            _status_line("State", a.get("state"))
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
            _status_line("Phase", a.get("phase"))
            if a.get("table_formats"):
                fmts = a["table_formats"]
                fmt_str = ", ".join(fmts) if isinstance(fmts, list) else str(fmts)
                st.markdown(f"**Table Formats:** {fmt_str}")
        with tcol2:
            if a.get("storage_kind"):
                st.markdown(f"**Storage:** {a['storage_kind']}")
            if a.get("suspended"):
                _status_line("Status", "SUSPENDED")
        if a.get("table_path"):
            st.markdown(f"**Table Path:** `{a['table_path']}`")

    elif ntype == NodeType.CATALOG_TABLE:
        _render_catalog_table(sel_node, a, ncolor)

    elif ntype == NodeType.SCHEMA:
        st.markdown("**Schema**")
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
        schema_topics = [
            e for e in graph.edges if e.dst_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
        ]
        if schema_topics:
            st.markdown(f"**Used by {len(schema_topics)} topic(s):**")
            for se in schema_topics:
                topic_node = graph.get_node(se.src_id)
                if topic_node:
                    role = se.attributes.get("role", "value")
                    st.caption(f"- {clean_display_name(topic_node.display_name)} ({role})")

    elif ntype == NodeType.CONSUMER_GROUP:
        st.markdown("**Consumer group**")
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            _status_line("State", a.get("state"))
        with gcol2:
            if a.get("is_simple") is not None:
                st.markdown(f"**Simple:** {'Yes' if a['is_simple'] else 'No'}")

    elif ntype == NodeType.EXTERNAL_DATASET:
        st.markdown("**External dataset**")
        if a.get("inferred_from"):
            st.markdown(f"**Inferred from connector:** `{a['inferred_from']}`")

    elif ntype == NodeType.NOTEBOOK:
        st.markdown("**Databricks notebook**")
        if a.get("notebook_name"):
            st.markdown(f"**Notebook:** {a['notebook_name']}")
        if a.get("notebook_path"):
            st.code(a["notebook_path"], language="text")
        ncol1, ncol2 = st.columns(2)
        with ncol1:
            if a.get("notebook_id") is not None:
                st.markdown(f"**Notebook ID:** `{a['notebook_id']}`")
            if a.get("workspace_id") is not None:
                st.markdown(f"**Workspace ID:** `{a['workspace_id']}`")
        with ncol2:
            if a.get("job_id") is not None:
                st.markdown(f"**Job ID:** `{a['job_id']}`")
            if a.get("job_name"):
                st.markdown(f"**Job:** {a['job_name']}")
        if a.get("schedule_cron"):
            tz = a.get("schedule_timezone") or "UTC"
            paused = " (paused)" if a.get("schedule_paused") else ""
            st.markdown(f"**Schedule:** `{a['schedule_cron']}` {tz}{paused}")
        if a.get("last_run_state"):
            result = a.get("last_run_result")
            state = a["last_run_state"]
            line = f"**Last run:** {state}" + (f" — {result}" if result else "")
            st.markdown(line)


def _render_catalog_table(sel_node, a: dict, ncolor: str) -> None:
    """Catalog-table type-specific rendering — UC / Glue / BigQuery / DataZone."""
    ct = sel_node.catalog_type
    catalog_console_label = {
        "UNITY_CATALOG": "Unity Catalog",
        "AWS_GLUE": "AWS Glue",
        "GOOGLE_DATA_LINEAGE": "BigQuery",
        "AWS_DATAZONE": "DataZone",
    }
    if ct == "UNITY_CATALOG":
        st.markdown("**Unity Catalog table**")
        ucol1, ucol2 = st.columns(2)
        with ucol1:
            if a.get("catalog_name"):
                st.markdown(f"**Catalog:** {a['catalog_name']}")
            if a.get("schema_name"):
                st.markdown(f"**Schema:** {a['schema_name']}")
        with ucol2:
            if a.get("table_name"):
                st.markdown(f"**Table:** {a['table_name']}")
        if a.get("workspace_url"):
            st.markdown(
                f"**Workspace:** <a href='{a['workspace_url']}' "
                f"target='_blank' style='color:{ncolor};'>"
                f"{a['workspace_url']} &#x2197;</a>",
                unsafe_allow_html=True,
            )
        if a.get("database"):
            st.markdown(f"**Database:** {a['database']}")

    elif ct == "AWS_GLUE":
        st.markdown("**AWS Glue table**")
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            if a.get("database"):
                st.markdown(f"**Database:** {a['database']}")
        with gcol2:
            if a.get("table_name"):
                st.markdown(f"**Table:** {a['table_name']}")
        if a.get("aws_region"):
            st.markdown(f"**Region:** {a['aws_region']}")

    elif ct == "GOOGLE_DATA_LINEAGE":
        st.markdown("**Google BigQuery table**")
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            if a.get("project_id"):
                st.markdown(f"**Project:** {a['project_id']}")
            if a.get("dataset_id"):
                st.markdown(f"**Dataset:** {a['dataset_id']}")
        with gcol2:
            if a.get("table_name"):
                st.markdown(f"**Table:** {a['table_name']}")
            if a.get("location"):
                st.markdown(f"**Location:** {a['location']}")

    if a.get("num_rows"):
        rcol1, rcol2 = st.columns(2)
        with rcol1:
            st.metric("Rows", f"{int(a['num_rows']):,}")
        with rcol2:
            if a.get("num_bytes"):
                st.metric("Size", _fmt_bytes_ui(float(a["num_bytes"])))
    if a.get("description"):
        st.markdown(f"**Description:** {a['description']}")

    # Per-catalog deep link (or a hint when unavailable)
    console_url = build_node_url(sel_node)
    console_name = catalog_console_label.get(ct or "", "console")
    if console_url:
        st.markdown(
            f"**Console:** <a href='{console_url}' target='_blank' "
            f"style='color:{ncolor};'>Open in {console_name} &#x2197;</a>",
            unsafe_allow_html=True,
        )
    else:
        st.caption(
            f"Deep link to {console_name} unavailable — "
            "set the workspace / project URL in `.env` to enable."
        )


def _render_topic_schemas(graph: LineageGraph, sel_id: str, ncolor: str) -> None:
    """Schemas-attached-to-topic block."""
    schema_edges = [
        e for e in graph.edges if e.src_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
    ]
    if not schema_edges:
        return
    st.markdown("---")
    st.markdown(f"**Schemas ({len(schema_edges)})**")
    for se in schema_edges:
        schema_node = graph.get_node(se.dst_id)
        if not schema_node:
            continue
        role = se.attributes.get("role", "value")
        role_color = "#1976D2" if role == "value" else "#7B1FA2"
        sa = schema_node.attributes
        st.markdown(
            f"<div style='padding:6px 10px;"
            f"background:rgba(128,128,128,0.08);"
            f"border-radius:6px;border-left:3px solid {role_color};"
            f"margin:4px 0;'>"
            f"<span style='background:{role_color};color:white;"
            f"padding:1px 6px;border-radius:4px;font-size:11px;'>"
            f"{role}</span> "
            f"<strong>{clean_display_name(schema_node.display_name)}</strong>"
            f"<br><span style='font-size:12px;color:#999;'>"
            f"{sa.get('schema_type', '')} "
            f"{'v' + str(sa['version']) if sa.get('version') else ''}"
            f"{' | ' + str(sa['field_count']) + ' fields' if sa.get('field_count') else ''}"
            f"{' | ID: ' + str(sa['schema_id']) if sa.get('schema_id') else ''}"
            f"</span></div>",
            unsafe_allow_html=True,
        )
        schema_str = sa.get("schema_string", "")
        if schema_str:
            schema_type = sa.get("schema_type", "AVRO")
            lang = "json" if schema_type in ("AVRO", "JSON") else "protobuf"
            if lang == "json":
                try:
                    import json as _json

                    schema_str = _json.dumps(_json.loads(schema_str), indent=2)
                except (ValueError, TypeError):
                    pass
            with st.expander(f"Schema definition ({role})", expanded=False):
                st.code(schema_str, language=lang)


_NEIGHBOUR_GROUP_THRESHOLD = 6


def _css_slug(value: str) -> str:
    """Make an arbitrary string safe to use as a CSS class suffix.

    Streamlit's `st.container(key=)` emits the key verbatim as a class
    name, so we sanitise to ``[A-Za-z0-9_-]`` to keep selectors valid.
    """
    return re.sub(r"[^A-Za-z0-9_-]", "-", value)


def _render_neighbours(graph: LineageGraph, sel_id: str) -> None:
    """Upstream / downstream neighbour buttons with type icons.

    When a side has more than _NEIGHBOUR_GROUP_THRESHOLD neighbours, group
    them by node type into collapsible expanders so a topic with 80
    consumers doesn't flood the panel with 80 buttons.

    The icon is injected as a ``::before`` background on each button
    element so it lives INSIDE the button (as a true prefix) rather than
    in a separate column. Per-button rules are emitted once at the top
    of the section, scoped via `st.container(key=…)` → `.st-key-…`.
    """
    _exclude = {NodeType.SCHEMA, NodeType.CONSUMER_GROUP}
    upstream = [
        n for n in graph.get_neighbors(sel_id, direction="upstream") if n.node_type not in _exclude
    ]
    downstream = [
        n
        for n in graph.get_neighbors(sel_id, direction="downstream")
        if n.node_type not in _exclude
    ]

    st.markdown("---")
    _inject_neighbour_icon_css(upstream + downstream)

    nb_col1, nb_col2 = st.columns(2)
    with nb_col1:
        _render_neighbour_side("up", "Upstream", upstream)
    with nb_col2:
        _render_neighbour_side("dn", "Downstream", downstream)


def _inject_neighbour_icon_css(neighbours: list[LineageNode]) -> None:
    """Emit one ``<style>`` block with ``::before`` background rules for each
    neighbour's wrapping container — puts the SVG icon inside the button.
    """
    rules: list[str] = []
    seen: set[str] = set()
    for nb in neighbours:
        if nb.node_id in seen:
            continue
        seen.add(nb.node_id)
        icon_uri = icon_for_node(nb)
        if not icon_uri:
            continue
        slug = _css_slug(nb.node_id)
        rules.append(
            f".st-key-nbrow-{slug} [data-testid='stBaseButton-secondary']::before,"
            f".st-key-nbrow-{slug} [data-testid='stBaseButton-primary']::before {{"
            f"content: '';"
            f"display: inline-block;"
            f"width: 16px; height: 16px;"
            f"background-image: url('{icon_uri}');"
            f"background-size: contain;"
            f"background-repeat: no-repeat;"
            f"background-position: center;"
            f"margin-right: 8px;"
            f"vertical-align: middle;"
            f"flex-shrink: 0;"
            f"}}"
        )
    if rules:
        st.markdown(f"<style>{''.join(rules)}</style>", unsafe_allow_html=True)


def _render_neighbour_side(prefix: str, title: str, neighbours: list[LineageNode]) -> None:
    """Render one column of neighbours: flat list if short, grouped if long."""
    st.markdown(f"**{title} ({len(neighbours)})**")
    if not neighbours:
        st.caption("None")
        return

    if len(neighbours) <= _NEIGHBOUR_GROUP_THRESHOLD:
        for nb in neighbours:
            _render_neighbour_button(prefix, nb)
        return

    # Group by NodeType for long lists. Each type group becomes an expander
    # so the whole panel stays scannable even at 80+ neighbours. Use the
    # SVG icon (same as the legend/canvas) for the group header by picking
    # any representative node from the group.
    grouped: dict[NodeType, list[LineageNode]] = defaultdict(list)
    for nb in neighbours:
        grouped[nb.node_type].append(nb)

    # Show largest groups first so the most-relevant ones are at the top.
    sorted_groups = sorted(grouped.items(), key=lambda kv: -len(kv[1]))
    for ntype, group in sorted_groups:
        type_label = NODE_TYPE_LABELS.get(ntype, ntype.value)
        with st.expander(f"{type_label} ({len(group)})", expanded=False):
            for nb in group:
                _render_neighbour_button(prefix, nb)


def _render_neighbour_button(prefix: str, nb: LineageNode) -> None:
    """Single button with the SVG icon injected as a ``::before`` prefix.

    The container's `key=` becomes a CSS class on the wrapping div
    (`.st-key-nbrow-<slug>`), which the per-button rule emitted by
    `_inject_neighbour_icon_css` targets.
    """
    cleaned = clean_display_name(nb.display_name)
    slug = _css_slug(nb.node_id)
    with st.container(key=f"nbrow-{slug}"):
        if st.button(
            cleaned,
            key=f"nb_{prefix}_{nb.node_id}",
            width="stretch",
        ):
            st.session_state.selected_node = nb.node_id
            st.rerun()


# Keys consumed by the explicit type-specific branches above. Anything not
# in this set falls through to the "Other Attributes" expander — drift
# protection is manual but at least centralized in one place now.
_KNOWN_KEYS: frozenset[str] = frozenset(
    {
        "partitions_count",
        "replication_factor",
        "is_internal",
        "description",
        "owner",
        "connector_class",
        "direction",
        "tasks_max",
        "output_data_format",
        "sql",
        "phase",
        "compute_pool_id",
        "principal",
        "ksqldb_cluster_id",
        "state",
        "table_formats",
        "storage_kind",
        "table_path",
        "suspended",
        "catalog_name",
        "schema_name",
        "table_name",
        "workspace_url",
        "catalog_type",
        "database",
        "project_id",
        "dataset_id",
        "location",
        "num_rows",
        "num_bytes",
        "creation_time",
        "last_modified_time",
        "labels",
        "columns",
        "table_type",
        "aws_region",
        "schema_type",
        "version",
        "field_count",
        "schema_id",
        "schema_string",
        "is_simple",
        "inferred_from",
        "metrics_active",
        "metrics_received_records",
        "metrics_sent_records",
        "metrics_received_bytes",
        "metrics_sent_bytes",
        "metrics_window_hours",
        "metrics_total_lag",
        "role",
    }
)


def _render_other_attributes(a: dict) -> None:
    """Catch-all expander for attributes not explicitly handled above.

    Excludes `metrics_*` keys — those go through the Metrics section so
    they don't show up twice. Scalars render as a key/value table; nested
    dicts/lists render as a pretty-printed JSON code block so structure
    is readable instead of collapsed into a single ``str()`` line.
    """
    extra = {k: v for k, v in a.items() if k not in _KNOWN_KEYS and not k.startswith("metrics_")}
    if not extra:
        return

    scalars: dict[str, str] = {}
    nested: dict[str, object] = {}
    for k, v in extra.items():
        if isinstance(v, (dict, list)) and v:
            nested[k] = v
        else:
            scalars[k] = "—" if v is None else str(v)

    with st.expander(f"Other attributes ({len(extra)})", expanded=False):
        if scalars:
            st.table([{"Key": k, "Value": v} for k, v in scalars.items()])
        for k, v in nested.items():
            st.caption(f"**{k}**")
            try:
                import json as _json

                rendered = _json.dumps(v, indent=2, default=str)
            except (TypeError, ValueError):
                rendered = str(v)
            st.code(rendered, language="json")
