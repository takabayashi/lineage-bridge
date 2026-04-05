# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Node detail panel rendering for the LineageBridge UI."""

from __future__ import annotations

import streamlit as st

from lineage_bridge.models.graph import EdgeType, LineageGraph, NodeType
from lineage_bridge.ui.styles import (
    NODE_COLORS,
    NODE_TYPE_LABELS,
    build_node_url,
)


def _fmt_bytes_ui(val: float) -> str:
    """Format byte count to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(val) < 1024:
            return f"{val:,.1f} {unit}"
        val /= 1024
    return f"{val:,.1f} PB"


def render_node_details(graph: LineageGraph) -> None:
    """Render selected node details panel."""
    sel_id = st.session_state.selected_node
    if not sel_id:
        return

    sel_node = graph.get_node(sel_id)
    if not sel_node:
        return

    ntype_label = NODE_TYPE_LABELS.get(sel_node.node_type, sel_node.node_type.value)
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
        "Close",
        key="close_detail_btn",
        use_container_width=True,
    ):
        st.session_state._dismissed_node = sel_id
        st.session_state.selected_node = None
        st.rerun()

    a = sel_node.attributes
    cloud_url = sel_node.url or build_node_url(sel_node)
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
                    f"**Phase:** <span style='color:{phase_color};font-weight:600;'>{phase}</span>",
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
                    f"**State:** <span style='color:{state_color};font-weight:600;'>{state}</span>",
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
                    f"**Phase:** <span style='color:{phase_color};font-weight:600;'>{phase}</span>",
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
                    "**Status:** <span style='color:#F44336;font-weight:600;'>SUSPENDED</span>",
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

    elif ntype == NodeType.GLUE_TABLE:
        st.markdown("**AWS Glue Table**")
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            if a.get("database"):
                st.markdown(f"**Database:** {a['database']}")
        with gcol2:
            if a.get("table_name"):
                st.markdown(f"**Table:** {a['table_name']}")
        if a.get("aws_region"):
            st.markdown(f"**Region:** {a['aws_region']}")
        glue_url = build_node_url(sel_node)
        if glue_url:
            st.markdown(
                f"**Console:** <a href='{glue_url}' "
                f"target='_blank' style='color:{ncolor};'>"
                f"Open in AWS Glue &#x2197;</a>",
                unsafe_allow_html=True,
            )

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
            e for e in graph.edges if e.dst_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
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
                    f"**State:** <span style='color:{state_color};font-weight:600;'>{state}</span>",
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
            "metrics_active",
            "metrics_received_records",
            "metrics_sent_records",
            "metrics_received_bytes",
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
            e for e in graph.edges if e.src_id == sel_id and e.edge_type == EdgeType.HAS_SCHEMA
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
        "schema_type",
        "version",
        "field_count",
        "schema_id",
        "is_simple",
        "inferred_from",
        "metrics_active",
        "metrics_received_records",
        "metrics_sent_records",
        "metrics_received_bytes",
        "metrics_sent_bytes",
        "metrics_window_hours",
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
