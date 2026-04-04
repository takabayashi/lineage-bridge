"""Convert a LineageGraph to streamlit-agraph or raw vis.js dicts."""

from __future__ import annotations

from typing import Any

from streamlit_agraph import Edge, Node

from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
)
from lineage_bridge.ui.styles import (
    EDGE_TYPE_LABELS,
    NODE_TYPE_LABELS,
    TOPIC_WITH_SCHEMA_ICON,
    build_edge_vis_props,
    build_node_vis_props,
)


def _build_tooltip(node: LineageNode) -> str:
    """Build a plain-text tooltip for a node (vis.js native tooltip)."""
    label = NODE_TYPE_LABELS.get(
        node.node_type, node.node_type.value
    )
    parts = [
        node.display_name,
        f"Type: {label}",
        f"Name: {node.qualified_name}",
    ]
    if node.environment_id:
        parts.append(f"Env: {node.environment_id}")
    if node.cluster_id:
        parts.append(f"Cluster: {node.cluster_id}")
    if node.tags:
        parts.append(f"Tags: {', '.join(node.tags)}")
    # Show key metrics if present
    if node.attributes.get("metrics_active") is not None:
        active = node.attributes["metrics_active"]
        parts.append(f"Active: {'Yes' if active else 'No'}")
    if node.attributes.get("metrics_received_records"):
        val = node.attributes["metrics_received_records"]
        parts.append(f"Records in: {val:,.0f}")
    if node.attributes.get("metrics_sent_records"):
        val = node.attributes["metrics_sent_records"]
        parts.append(f"Records out: {val:,.0f}")
    # Other attributes
    other_attrs = {
        k: v
        for k, v in node.attributes.items()
        if not k.startswith("metrics_")
    }
    if other_attrs:
        parts.append("---")
        for k, v in other_attrs.items():
            parts.append(f"  {k}: {v}")
    return "\n".join(parts)


def _collect_hop_neighborhood(
    graph: LineageGraph, center_id: str, hops: int
) -> set[str]:
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
    return {
        e.src_id
        for e in graph.edges
        if e.edge_type == EdgeType.HAS_SCHEMA
    }


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
    if selected_node and selected_node in {
        n.node_id for n in graph.nodes
    }:
        allowed_ids = _collect_hop_neighborhood(
            graph, selected_node, hops
        )
    else:
        allowed_ids = None

    # Pre-compute connected nodes
    connected_ids = (
        _get_connected_node_ids(graph)
        if hide_disconnected
        else None
    )

    included_ids: set[str] = set()
    agraph_nodes: list[Node] = []
    search_lower = search_query.strip().lower()

    for node in graph.nodes:
        # Hop filter
        if (
            allowed_ids is not None
            and node.node_id not in allowed_ids
        ):
            continue

        # Disconnected filter
        if (
            connected_ids is not None
            and node.node_id not in connected_ids
        ):
            continue

        # Type filter
        if filters and not filters.get(node.node_type, True):
            continue

        # Environment filter
        if (
            environment_filter
            and node.environment_id
            and node.environment_id != environment_filter
        ):
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

        vis_props = build_node_vis_props(node.node_type)
        agraph_nodes.append(
            Node(
                id=node.node_id,
                label=node.display_name,
                title=_build_tooltip(node),
                **vis_props,
            )
        )

    # Build edges (only between included nodes)
    agraph_edges: list[Edge] = []
    for edge in graph.edges:
        if (
            edge.src_id in included_ids
            and edge.dst_id in included_ids
        ):
            edge_label = EDGE_TYPE_LABELS.get(
                edge.edge_type, edge.edge_type.value
            )
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
    if selected_node and selected_node in {
        n.node_id for n in graph.nodes
    }:
        allowed_ids = _collect_hop_neighborhood(
            graph, selected_node, hops
        )
    else:
        allowed_ids = None

    connected_ids = (
        _get_connected_node_ids(graph)
        if hide_disconnected
        else None
    )

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
            search_neighborhood |= _collect_hop_neighborhood(
                graph, mid, hops
            )

    included_ids: set[str] = set()
    raw_nodes: list[dict[str, Any]] = []

    for node in graph.nodes:
        # Skip schema nodes — they are shown as info on topics
        if node.node_type == NodeType.SCHEMA:
            continue

        if (
            allowed_ids is not None
            and node.node_id not in allowed_ids
        ):
            continue
        if (
            connected_ids is not None
            and node.node_id not in connected_ids
        ):
            continue
        if filters and not filters.get(node.node_type, True):
            continue
        if (
            environment_filter
            and node.environment_id
            and node.environment_id != environment_filter
        ):
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

        vis_props = build_node_vis_props(node.node_type)

        # Use schema-badged icon for topics with schemas
        if (
            node.node_type == NodeType.KAFKA_TOPIC
            and node.node_id in topics_with_schemas
        ):
            vis_props["image"] = TOPIC_WITH_SCHEMA_ICON

        raw_nodes.append(
            {
                "id": node.node_id,
                "label": node.display_name,
                "title": _build_tooltip(node),
                **vis_props,
            }
        )

    raw_edges: list[dict[str, Any]] = []
    for edge in graph.edges:
        # Skip HAS_SCHEMA edges — schemas are shown as topic info
        if edge.edge_type == EdgeType.HAS_SCHEMA:
            continue

        if (
            edge.src_id in included_ids
            and edge.dst_id in included_ids
        ):
            edge_label = EDGE_TYPE_LABELS.get(
                edge.edge_type, edge.edge_type.value
            )
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
