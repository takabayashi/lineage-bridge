# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Convert a LineageGraph to streamlit-agraph or raw vis.js dicts."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from streamlit_agraph import Edge, Node

from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
)
from lineage_bridge.ui.styles import (
    DLQ_TOPIC_ICON,
    EDGE_TYPE_LABELS,
    TOPIC_WITH_SCHEMA_ICON,
    build_confluent_cloud_url,
    build_edge_vis_props,
    build_node_vis_props,
    build_status_badge_icon,
    color_for_node,
    icon_for_node,
    label_for_node,
)


def _trunc(text: str, max_len: int = 40) -> str:
    """Truncate text with ellipsis if it exceeds max_len."""
    return text if len(text) <= max_len else text[: max_len - 1] + "\u2026"


def _short_name(node: LineageNode) -> str:
    """Return a short display name — last segment of qualified_name."""
    name = node.display_name
    if "." in name:
        return name.rsplit(".", 1)[-1]
    return name


def _build_tooltip(node: LineageNode) -> str:
    """Build an HTML tooltip card for a node.

    The vis.js component converts HTML strings (starting with ``<``)
    to DOM elements so they render as rich tooltips.  Each node type
    surfaces the most relevant quick-view info; SQL is intentionally
    omitted (too noisy for hover).
    """
    label = label_for_node(node)
    color = color_for_node(node)
    a = node.attributes

    # ── Location row (prefer names over IDs) ───────────────────────
    loc_parts: list[str] = []
    if node.environment_name or node.environment_id:
        loc_parts.append(node.environment_name or node.environment_id)
    if node.cluster_name or node.cluster_id:
        loc_parts.append(node.cluster_name or node.cluster_id)
    loc_html = ""
    if loc_parts:
        loc_html = (
            f"<div style='font-size:11px;color:#999;margin-top:4px'>{' / '.join(loc_parts)}</div>"
        )

    # ── Tags row ────────────────────────────────────────────────────
    tags_html = ""
    if node.tags:
        pills = " ".join(
            f"<span style='display:inline-block;background:{color}20;"
            f"color:{color};padding:1px 6px;border-radius:3px;"
            f"font-size:10px;margin-right:2px'>{t}</span>"
            for t in node.tags
        )
        tags_html = f"<div style='margin-top:5px'>{pills}</div>"

    # ── Metrics bar (topics & connectors) ───────────────────────────
    metrics_html = ""
    metric_parts: list[str] = []
    if a.get("metrics_active") is not None:
        active = a["metrics_active"]
        dot = "#4CAF50" if active else "#F44336"
        metric_parts.append(
            f"<span style='display:inline-block;width:7px;height:7px;"
            f"border-radius:50%;background:{dot};margin-right:3px;"
            f"vertical-align:middle'></span>"
            f"{'Active' if active else 'Inactive'}"
        )
    for mkey, mlabel in [
        ("metrics_received_records", "In"),
        ("metrics_sent_records", "Out"),
    ]:
        val = a.get(mkey)
        if val:
            metric_parts.append(f"{mlabel}: {val:,.0f}")
    for mkey, mlabel in [
        ("metrics_received_bytes", "Bytes In"),
        ("metrics_sent_bytes", "Bytes Out"),
    ]:
        val = a.get(mkey)
        if val:
            metric_parts.append(f"{mlabel}: {_fmt_bytes(val)}")
    if metric_parts:
        metrics_html = (
            f"<div style='margin-top:5px;padding:3px 7px;"
            f"background:rgba(0,0,0,0.05);border-radius:4px;"
            f"font-size:11px;color:#666'>"
            f"{' &middot; '.join(metric_parts)}</div>"
        )

    # ── Type-specific detail lines ──────────────────────────────────
    detail_lines: list[str] = []
    ntype = node.node_type

    if ntype == NodeType.KAFKA_TOPIC:
        if a.get("partitions_count"):
            detail_lines.append(f"Partitions: {a['partitions_count']}")
        if a.get("replication_factor"):
            detail_lines.append(f"RF: {a['replication_factor']}")
        if a.get("is_internal"):
            detail_lines.append("Internal topic")
        if a.get("description"):
            desc = str(a["description"])
            if len(desc) > 60:
                desc = desc[:57] + "..."
            detail_lines.append(desc)

    elif ntype == NodeType.CONNECTOR:
        if a.get("connector_class"):
            cls = str(a["connector_class"]).rsplit(".", 1)[-1]
            detail_lines.append(cls)
        if a.get("direction"):
            detail_lines.append(a["direction"].upper())
        if a.get("tasks_max"):
            detail_lines.append(f"Tasks: {a['tasks_max']}")
        if a.get("output_data_format"):
            detail_lines.append(f"Format: {a['output_data_format']}")

    elif ntype == NodeType.FLINK_JOB:
        if a.get("phase"):
            detail_lines.append(f"Phase: {a['phase']}")
        if a.get("compute_pool_id"):
            detail_lines.append(f"Pool: {a['compute_pool_id']}")
        if a.get("principal"):
            detail_lines.append(f"Principal: {a['principal']}")

    elif ntype == NodeType.KSQLDB_QUERY:
        if a.get("state"):
            detail_lines.append(f"State: {a['state']}")
        if a.get("ksqldb_cluster_id"):
            detail_lines.append(f"Cluster: {a['ksqldb_cluster_id']}")

    elif ntype == NodeType.TABLEFLOW_TABLE:
        if a.get("phase"):
            detail_lines.append(f"Phase: {a['phase']}")
        if a.get("table_formats"):
            fmts = a["table_formats"]
            detail_lines.append(f"Formats: {', '.join(fmts) if isinstance(fmts, list) else fmts}")
        if a.get("storage_kind"):
            detail_lines.append(f"Storage: {a['storage_kind']}")
        if a.get("suspended"):
            detail_lines.append("SUSPENDED")

    elif ntype == NodeType.CATALOG_TABLE:
        # Per-catalog detail rows. Falls back to a generic line if catalog_type
        # is unknown (e.g. graph created by an older provider).
        ct = node.catalog_type
        if ct == "UNITY_CATALOG":
            if a.get("catalog_name"):
                detail_lines.append(f"Catalog: {a['catalog_name']}")
            if a.get("workspace_url"):
                detail_lines.append("Databricks UC")
        elif ct == "AWS_GLUE":
            if a.get("database"):
                detail_lines.append(f"Database: {a['database']}")
        elif ct == "GOOGLE_DATA_LINEAGE":
            if a.get("project_id"):
                detail_lines.append(f"Project: {a['project_id']}")
            if a.get("dataset_id"):
                detail_lines.append(f"Dataset: {a['dataset_id']}")
        elif ct:
            detail_lines.append(ct)

    elif ntype == NodeType.SCHEMA:
        if a.get("schema_type"):
            detail_lines.append(a["schema_type"])
        if a.get("version"):
            detail_lines.append(f"v{a['version']}")
        if a.get("field_count"):
            detail_lines.append(f"{a['field_count']} fields")

    elif ntype == NodeType.CONSUMER_GROUP:
        if a.get("state"):
            detail_lines.append(f"State: {a['state']}")
        if a.get("is_simple"):
            detail_lines.append("Simple consumer")

    elif ntype == NodeType.EXTERNAL_DATASET:
        if a.get("inferred_from"):
            detail_lines.append(f"From: {a['inferred_from']}")

    details_html = ""
    if detail_lines:
        details_html = (
            "<div style='margin-top:5px;font-size:11px;"
            "color:#666;line-height:1.5'>" + " &middot; ".join(detail_lines) + "</div>"
        )

    return (
        f"<div style='font-family:Inter,system-ui,sans-serif;"
        f"max-width:320px;padding:10px 12px;"
        f"border-left:4px solid {color};"
        f"background:#fff;border-radius:0 6px 6px 0;"
        f"box-shadow:0 2px 8px rgba(0,0,0,0.18)'>"
        f"<div style='font-size:10px;font-weight:700;"
        f"text-transform:uppercase;letter-spacing:0.5px;"
        f"color:{color}'>{label}</div>"
        f"<div style='font-size:14px;font-weight:700;"
        f"color:#222;margin-top:2px;"
        f"overflow:hidden;text-overflow:ellipsis;"
        f"white-space:nowrap;max-width:300px' "
        f"title='{node.display_name}'>{_trunc(_short_name(node), 40)}</div>"
        f"{loc_html}{tags_html}{details_html}{metrics_html}"
        f"</div>"
    )


def _fmt_bytes(val: float) -> str:
    """Format byte count to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(val) < 1024:
            return f"{val:,.1f} {unit}"
        val /= 1024
    return f"{val:,.1f} PB"


def _collect_hop_neighborhood(graph: LineageGraph, center_id: str, hops: int) -> set[str]:
    """Return node IDs within *hops* of *center_id* (both dirs)."""
    upstream = graph.upstream(center_id, hops)
    downstream = graph.downstream(center_id, hops)
    ids = {n.node_id for n in upstream}
    ids |= {n.node_id for n in downstream}
    ids.add(center_id)
    return ids


def _get_connected_node_ids(graph: LineageGraph) -> set[str]:
    """Return IDs of nodes that have at least one visible edge.

    HAS_SCHEMA edges are rendered as info on topic nodes, not as
    visible graph edges, so they don't count for connectivity.
    """
    connected: set[str] = set()
    for edge in graph.edges:
        if edge.edge_type == EdgeType.HAS_SCHEMA:
            continue
        connected.add(edge.src_id)
        connected.add(edge.dst_id)
    return connected


def _get_topics_with_schemas(graph: LineageGraph) -> set[str]:
    """Return topic node IDs that have at least one HAS_SCHEMA edge."""
    return {e.src_id for e in graph.edges if e.edge_type == EdgeType.HAS_SCHEMA}


def render_graph(
    graph: LineageGraph,
    filters: dict[NodeType, bool] | None = None,
    search_query: str = "",
    selected_node: str | None = None,
    hops: int = 2,
    environment_filter: str | None = None,
    cluster_filter: str | None = None,
    hide_disconnected: bool = True,
) -> tuple[list[Node], list[Edge]]:
    """Convert a LineageGraph to agraph Node and Edge lists.

    Args:
        graph: The full lineage graph.
        filters: Map of NodeType -> bool.
        search_query: Substring filter on names.
        selected_node: If set, restrict to N-hop neighborhood.
        hops: Number of hops for neighborhood view.
        environment_filter: Filter by environment_id.
        cluster_filter: Filter by cluster_id.
        hide_disconnected: If True, hide nodes with no edges.

    Returns:
        Tuple of (agraph_nodes, agraph_edges).
    """
    # Determine which node IDs to include
    if selected_node and selected_node in {n.node_id for n in graph.nodes}:
        allowed_ids = _collect_hop_neighborhood(graph, selected_node, hops)
    else:
        allowed_ids = None

    # Pre-compute connected nodes
    connected_ids = _get_connected_node_ids(graph) if hide_disconnected else None

    included_ids: set[str] = set()
    agraph_nodes: list[Node] = []
    search_lower = search_query.strip().lower()

    for node in graph.nodes:
        # Hop filter
        if allowed_ids is not None and node.node_id not in allowed_ids:
            continue

        # Disconnected filter
        if connected_ids is not None and node.node_id not in connected_ids:
            continue

        # Type filter
        if filters and not filters.get(node.node_type, True):
            continue

        # Environment filter
        if environment_filter and node.environment_id and node.environment_id != environment_filter:
            continue

        # Cluster filter
        # Cluster filter: keep nodes in the selected cluster, plus
        # nodes with no cluster_id (e.g. Flink jobs, environment-scoped
        # resources) so cross-cluster edges remain visible.
        if cluster_filter and node.cluster_id and node.cluster_id != cluster_filter:
            continue

        # Search filter
        if search_lower and (
            search_lower not in node.display_name.lower()
            and search_lower not in node.qualified_name.lower()
        ):
            continue

        included_ids.add(node.node_id)

        # Populate Confluent Cloud URL if not already set
        if not node.url:
            node.url = build_confluent_cloud_url(node)

        vis_props = build_node_vis_props(node.node_type)
        vis_props["image"] = icon_for_node(node)
        if node.node_type == NodeType.KAFKA_TOPIC and node.attributes.get("role") == "dlq":
            vis_props["image"] = DLQ_TOPIC_ICON
        agraph_nodes.append(
            Node(
                id=node.node_id,
                label=_short_name(node),
                title=_build_tooltip(node),
                **vis_props,
            )
        )

    # Build edges (only between included nodes)
    agraph_edges: list[Edge] = []
    for edge in graph.edges:
        if edge.src_id in included_ids and edge.dst_id in included_ids:
            edge_label = EDGE_TYPE_LABELS.get(edge.edge_type, edge.edge_type.value)
            vis_props = build_edge_vis_props(edge.edge_type)
            agraph_edges.append(
                Edge(
                    source=edge.src_id,
                    target=edge.dst_id,
                    label=edge_label,
                    **vis_props,
                )
            )

    return agraph_nodes, agraph_edges


# ── Sugiyama-style DAG layout (topological layers + barycenter) ────────


def _compute_dag_layout(
    node_ids: list[str],
    edges: list[tuple[str, str]],
    *,
    level_sep: int = 280,
    node_sep: int = 100,
) -> dict[str, dict[str, float]]:
    """Compute x/y positions for a DAG using layered layout.

    1. Assign each node to a layer (longest-path from roots).
    2. Order nodes within each layer using the barycenter heuristic
       (multiple sweeps) to minimise edge crossings.
    3. Return ``{node_id: {"x": …, "y": …}}``.
    """
    if not node_ids:
        return {}

    id_set = set(node_ids)

    # Build adjacency for included nodes only
    children: dict[str, list[str]] = defaultdict(list)
    parents: dict[str, list[str]] = defaultdict(list)
    for src, dst in edges:
        if src in id_set and dst in id_set:
            children[src].append(dst)
            parents[dst].append(src)

    # ── Layer assignment (longest path from roots) ────────────────
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    for src, dst in edges:
        if src in id_set and dst in id_set:
            in_degree[dst] = in_degree.get(dst, 0) + 1

    layer_of: dict[str, int] = {}
    queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
    # If no roots (cycle), start from all nodes
    if not queue:
        queue = deque(node_ids)
        for nid in node_ids:
            layer_of[nid] = 0

    for nid in queue:
        layer_of.setdefault(nid, 0)

    while queue:
        nid = queue.popleft()
        for child in children.get(nid, []):
            new_layer = layer_of[nid] + 1
            if new_layer > layer_of.get(child, -1):
                layer_of[child] = new_layer
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Assign any remaining nodes (disconnected)
    for nid in node_ids:
        layer_of.setdefault(nid, 0)

    # Group by layer
    layers: dict[int, list[str]] = defaultdict(list)
    for nid, lyr in layer_of.items():
        layers[lyr].append(nid)

    max_layer = max(layers.keys()) if layers else 0

    # ── Barycenter ordering (reduce crossings) ────────────────────
    # Initial order: sort alphabetically within each layer for determinism
    for lyr in layers:
        layers[lyr].sort()

    def _order_index(layer_list: list[str]) -> dict[str, int]:
        return {nid: i for i, nid in enumerate(layer_list)}

    # Forward + backward sweeps
    for _ in range(4):
        # Forward sweep: order layer i by barycenter of parents in layer i-1
        for lyr in range(1, max_layer + 1):
            if lyr - 1 not in layers:
                continue
            prev_idx = _order_index(layers[lyr - 1])
            bary: list[tuple[float, str]] = []
            for nid in layers[lyr]:
                p = [prev_idx[pid] for pid in parents.get(nid, []) if pid in prev_idx]
                bc = sum(p) / len(p) if p else float("inf")
                bary.append((bc, nid))
            bary.sort()
            layers[lyr] = [nid for _, nid in bary]

        # Backward sweep: order layer i by barycenter of children in layer i+1
        for lyr in range(max_layer - 1, -1, -1):
            if lyr + 1 not in layers:
                continue
            next_idx = _order_index(layers[lyr + 1])
            bary = []
            for nid in layers[lyr]:
                c = [next_idx[cid] for cid in children.get(nid, []) if cid in next_idx]
                bc = sum(c) / len(c) if c else float("inf")
                bary.append((bc, nid))
            bary.sort()
            layers[lyr] = [nid for _, nid in bary]

    # ── Assign coordinates ────────────────────────────────────────
    positions: dict[str, dict[str, float]] = {}
    for lyr in range(max_layer + 1):
        nodes_in_layer = layers.get(lyr, [])
        n = len(nodes_in_layer)
        # Center the layer vertically
        total_height = (n - 1) * node_sep
        start_y = -total_height / 2
        x = lyr * level_sep
        for i, nid in enumerate(nodes_in_layer):
            positions[nid] = {"x": x, "y": start_y + i * node_sep}

    return positions


def render_graph_raw(
    graph: LineageGraph,
    filters: dict[NodeType, bool] | None = None,
    search_query: str = "",
    selected_node: str | None = None,
    hops: int = 2,
    environment_filter: str | None = None,
    cluster_filter: str | None = None,
    hide_disconnected: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Convert a LineageGraph to raw vis.js-compatible dicts.

    Same filtering logic as render_graph but returns plain dicts
    for the custom vis.js component.
    """
    # Determine which node IDs to include
    if selected_node and selected_node in {n.node_id for n in graph.nodes}:
        allowed_ids = _collect_hop_neighborhood(graph, selected_node, hops)
    else:
        allowed_ids = None

    connected_ids = _get_connected_node_ids(graph) if hide_disconnected else None

    # Pre-compute topics that have schemas
    topics_with_schemas = _get_topics_with_schemas(graph)

    # When searching, find matching nodes then expand to their neighborhood
    search_lower = search_query.strip().lower()
    search_neighborhood: set[str] | None = None
    if search_lower:
        matched_ids: list[str] = []
        for node in graph.nodes:
            if node.node_type == NodeType.SCHEMA:
                continue
            if (
                search_lower in node.display_name.lower()
                or search_lower in node.qualified_name.lower()
            ):
                matched_ids.append(node.node_id)
        # Expand each match to its N-hop neighborhood
        search_neighborhood = set()
        for mid in matched_ids:
            search_neighborhood |= _collect_hop_neighborhood(graph, mid, hops)

    included_ids: set[str] = set()
    raw_nodes: list[dict[str, Any]] = []

    for node in graph.nodes:
        # Skip schema nodes — they are shown as info on topics
        if node.node_type == NodeType.SCHEMA:
            continue

        if allowed_ids is not None and node.node_id not in allowed_ids:
            continue
        if connected_ids is not None and node.node_id not in connected_ids:
            continue
        if filters and not filters.get(node.node_type, True):
            continue
        if environment_filter and node.environment_id and node.environment_id != environment_filter:
            continue
        # Cluster filter: keep nodes in the selected cluster, plus
        # nodes with no cluster_id (e.g. Flink jobs, environment-scoped
        # resources) so cross-cluster edges remain visible.
        if cluster_filter and node.cluster_id and node.cluster_id != cluster_filter:
            continue
        # Search: show matched nodes + their neighborhood
        if search_neighborhood is not None and node.node_id not in search_neighborhood:
            continue

        included_ids.add(node.node_id)

        # Populate Confluent Cloud URL if not already set
        if not node.url:
            node.url = build_confluent_cloud_url(node)

        vis_props = build_node_vis_props(node.node_type)
        # Per-catalog brand icon (UC / Glue / BigQuery / DataZone) — without
        # this, all CATALOG_TABLE nodes get the generic database icon, so e.g.
        # AWS Glue tables look identical to Unity Catalog tables in the graph.
        vis_props["image"] = icon_for_node(node)

        # Use schema-badged icon for topics with schemas
        if node.node_type == NodeType.KAFKA_TOPIC and node.node_id in topics_with_schemas:
            vis_props["image"] = TOPIC_WITH_SCHEMA_ICON

        # DLQ-badged icon for connector dead-letter-queue topics. Wins over
        # the schema badge — DLQ status is the more important visual cue and
        # DLQ topics rarely carry user schemas.
        if node.node_type == NodeType.KAFKA_TOPIC and node.attributes.get("role") == "dlq":
            vis_props["image"] = DLQ_TOPIC_ICON

        # Status badge for nodes with phase/state
        status = node.attributes.get("phase") or node.attributes.get("state")
        if status and node.node_type != NodeType.KAFKA_TOPIC:
            badge_icon = build_status_badge_icon(node.node_type, status)
            if badge_icon:
                vis_props["image"] = badge_icon

        raw_nodes.append(
            {
                "id": node.node_id,
                "label": _trunc(_short_name(node), 30),
                "title": _build_tooltip(node),
                **vis_props,
            }
        )

    raw_edges: list[dict[str, Any]] = []
    for edge in graph.edges:
        # Skip HAS_SCHEMA edges — schemas are shown as topic info
        if edge.edge_type == EdgeType.HAS_SCHEMA:
            continue

        if edge.src_id in included_ids and edge.dst_id in included_ids:
            edge_label = EDGE_TYPE_LABELS.get(edge.edge_type, edge.edge_type.value)
            vis_props = build_edge_vis_props(edge.edge_type)
            raw_edges.append(
                {
                    "from": edge.src_id,
                    "to": edge.dst_id,
                    "label": edge_label,
                    **vis_props,
                }
            )

    return raw_nodes, raw_edges
