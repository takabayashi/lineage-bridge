"""Convert a LineageGraph to streamlit-agraph compatible objects."""

from __future__ import annotations

from streamlit_agraph import Edge, Node

from lineage_bridge.models.graph import LineageGraph, LineageNode, NodeType
from lineage_bridge.ui.styles import (
    EDGE_COLORS,
    EDGE_TYPE_LABELS,
    NODE_COLORS,
    NODE_SHAPES,
    NODE_SIZES,
    NODE_TYPE_LABELS,
)


def _build_tooltip(node: LineageNode) -> str:
    """Build an HTML tooltip string for a node."""
    parts = [
        f"<b>{node.display_name}</b>",
        f"<i>{NODE_TYPE_LABELS.get(node.node_type, node.node_type.value)}</i>",
        "<hr style='margin:4px 0'/>",
        f"<b>Qualified name:</b> {node.qualified_name}",
    ]
    if node.environment_id:
        parts.append(f"<b>Environment:</b> {node.environment_id}")
    if node.cluster_id:
        parts.append(f"<b>Cluster:</b> {node.cluster_id}")
    if node.tags:
        tag_str = ", ".join(node.tags)
        parts.append(f"<b>Tags:</b> {tag_str}")
    if node.attributes:
        parts.append("<b>Attributes:</b>")
        for k, v in node.attributes.items():
            parts.append(f"&nbsp;&nbsp;{k}: {v}")
    return "<br>".join(parts)


def _collect_hop_neighborhood(graph: LineageGraph, center_id: str, hops: int) -> set[str]:
    """Return the set of node IDs within *hops* of *center_id* (both directions)."""
    upstream_nodes = graph.upstream(center_id, hops)
    downstream_nodes = graph.downstream(center_id, hops)
    node_ids = {n.node_id for n in upstream_nodes} | {n.node_id for n in downstream_nodes}
    node_ids.add(center_id)
    return node_ids


def render_graph(
    graph: LineageGraph,
    filters: dict[NodeType, bool] | None = None,
    search_query: str = "",
    selected_node: str | None = None,
    hops: int = 2,
    environment_filter: str | None = None,
    cluster_filter: str | None = None,
) -> tuple[list[Node], list[Edge]]:
    """Convert a LineageGraph to agraph Node and Edge lists.

    Args:
        graph: The full lineage graph.
        filters: Map of NodeType -> bool. Only node types set to True are included.
        search_query: Substring filter on display_name / qualified_name.
        selected_node: If set, restrict the view to the N-hop neighborhood.
        hops: Number of hops for neighborhood view.
        environment_filter: If set, only include nodes with this environment_id.
        cluster_filter: If set, only include nodes with this cluster_id.

    Returns:
        Tuple of (agraph_nodes, agraph_edges).
    """
    # --- Determine which node IDs to include ---
    if selected_node and selected_node in {n.node_id for n in graph.nodes}:
        allowed_ids = _collect_hop_neighborhood(graph, selected_node, hops)
    else:
        allowed_ids = None  # no restriction

    included_ids: set[str] = set()
    agraph_nodes: list[Node] = []

    search_lower = search_query.strip().lower()

    for node in graph.nodes:
        # Hop filter
        if allowed_ids is not None and node.node_id not in allowed_ids:
            continue

        # Type filter
        if filters and not filters.get(node.node_type, True):
            continue

        # Environment filter
        if environment_filter and node.environment_id != environment_filter:
            continue

        # Cluster filter
        if cluster_filter and node.cluster_id != cluster_filter:
            continue

        # Search filter
        if search_lower and (
            search_lower not in node.display_name.lower()
            and search_lower not in node.qualified_name.lower()
        ):
            continue

        included_ids.add(node.node_id)

        color = NODE_COLORS.get(node.node_type, "#757575")
        size = NODE_SIZES.get(node.node_type, 22)
        shape = NODE_SHAPES.get(node.node_type, "dot")

        agraph_nodes.append(
            Node(
                id=node.node_id,
                label=node.display_name,
                size=size,
                color=color,
                shape=shape,
                title=_build_tooltip(node),
            )
        )

    # --- Build edges (only between included nodes) ---
    agraph_edges: list[Edge] = []
    for edge in graph.edges:
        if edge.src_id in included_ids and edge.dst_id in included_ids:
            edge_color = EDGE_COLORS.get(edge.edge_type, "#757575")
            edge_label = EDGE_TYPE_LABELS.get(edge.edge_type, edge.edge_type.value)
            agraph_edges.append(
                Edge(
                    source=edge.src_id,
                    target=edge.dst_id,
                    label=edge_label,
                    color=edge_color,
                    type="CURVE_SMOOTH",
                )
            )

    return agraph_nodes, agraph_edges
