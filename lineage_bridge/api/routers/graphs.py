# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Graph view endpoints: Confluent-only, enriched, CRUD."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from lineage_bridge.api.auth import require_api_key
from lineage_bridge.api.openlineage.translator import graph_to_events
from lineage_bridge.api.schemas import (
    EdgeCreatedResponse,
    EdgeListResponse,
    GraphCreatedResponse,
    GraphDeletedResponse,
    GraphDetailResponse,
    GraphExportResponse,
    GraphStats,
    ImportResponse,
    NodeCreatedResponse,
    NodeListResponse,
    TraversalResponse,
    ViewResponse,
)
from lineage_bridge.api.state import GraphStore, GraphSummary
from lineage_bridge.models.graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

router = APIRouter()


def _get_graph_store(request: Request) -> GraphStore:
    return request.app.state.graph_store


@router.get("", dependencies=[Depends(require_api_key)])
async def list_graphs(
    store: GraphStore = Depends(_get_graph_store),
) -> list[GraphSummary]:
    """List all in-memory graphs."""
    return store.list_all()


@router.post("", status_code=201, dependencies=[Depends(require_api_key)])
async def create_graph(
    store: GraphStore = Depends(_get_graph_store),
) -> GraphCreatedResponse:
    """Create a new empty graph."""
    graph_id = store.create()
    return GraphCreatedResponse(graph_id=graph_id)


@router.get("/{graph_id}", dependencies=[Depends(require_api_key)])
async def get_graph(
    graph_id: str,
    store: GraphStore = Depends(_get_graph_store),
) -> GraphDetailResponse:
    """Get a full graph with nodes, edges, and stats."""
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    meta = store.get_meta(graph_id)
    data = graph.to_dict()
    return GraphDetailResponse(
        graph_id=graph_id,
        nodes=data.get("nodes", []),
        edges=data.get("edges", []),
        stats=GraphStats(
            node_count=graph.node_count,
            edge_count=graph.edge_count,
            pipeline_count=graph.pipeline_count,
            validation_warnings=graph.validate(),
        ),
        created_at=meta.created_at.isoformat() if meta else None,
        last_modified=meta.last_modified.isoformat() if meta else None,
    )


@router.delete("/{graph_id}", dependencies=[Depends(require_api_key)])
async def delete_graph(
    graph_id: str,
    store: GraphStore = Depends(_get_graph_store),
) -> GraphDeletedResponse:
    if not store.delete(graph_id):
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    return GraphDeletedResponse(status="deleted", graph_id=graph_id)


@router.post("/{graph_id}/import", dependencies=[Depends(require_api_key)])
async def import_graph(
    graph_id: str,
    data: dict[str, Any],
    store: GraphStore = Depends(_get_graph_store),
) -> ImportResponse:
    """Import nodes and edges from JSON into an existing graph."""
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

    imported = LineageGraph.from_dict(data)
    for node in imported.nodes:
        graph.add_node(node)
    for edge in imported.edges:
        graph.add_edge(edge)
    store.touch(graph_id)

    return ImportResponse(
        graph_id=graph_id,
        nodes_imported=imported.node_count,
        edges_imported=imported.edge_count,
    )


@router.get("/{graph_id}/export", dependencies=[Depends(require_api_key)])
async def export_graph(
    graph_id: str,
    store: GraphStore = Depends(_get_graph_store),
) -> GraphExportResponse:
    """Export graph as native LineageGraph JSON."""
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    data = graph.to_dict()
    return GraphExportResponse(nodes=data.get("nodes", []), edges=data.get("edges", []))


@router.get("/confluent/view", dependencies=[Depends(require_api_key)])
async def confluent_view(
    store: GraphStore = Depends(_get_graph_store),
) -> ViewResponse:
    """Confluent-only lineage view across all graphs.

    Returns OpenLineage events for Confluent-system nodes only,
    excluding catalog nodes (UC, Glue, Google).
    """

    all_events = []
    for summary in store.list_all():
        graph = store.get(summary.graph_id)
        if graph:
            events = graph_to_events(graph, confluent_only=True)
            all_events.extend(events)
    return ViewResponse(view="confluent", events=all_events)


@router.get("/enriched/view", dependencies=[Depends(require_api_key)])
async def enriched_view(
    systems: str | None = Query(
        None, description="Comma-separated system filter (e.g. confluent,databricks)"
    ),
    store: GraphStore = Depends(_get_graph_store),
) -> ViewResponse:
    """Full enriched lineage view across all graphs.

    Includes Confluent nodes plus all catalog nodes (UC, Glue, Google).
    Optionally filter by system types.
    """
    all_events = []
    for summary in store.list_all():
        graph = store.get(summary.graph_id)
        if graph:
            events = graph_to_events(graph, confluent_only=False)
            all_events.extend(events)

    if systems:
        allowed = {s.strip() for s in systems.split(",")}
        all_events = [e for e in all_events if any(e.job.namespace.startswith(s) for s in allowed)]

    return ViewResponse(view="enriched", systems=systems, events=all_events)


# ── Node and Edge CRUD on specific graphs ──────────────────────────────


@router.get("/{graph_id}/nodes", dependencies=[Depends(require_api_key)])
async def list_nodes(
    graph_id: str,
    type: NodeType | None = None,
    system: SystemType | None = None,
    env_id: str | None = None,
    search: str | None = None,
    store: GraphStore = Depends(_get_graph_store),
) -> NodeListResponse:
    """List nodes with optional filters."""
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

    nodes = graph.nodes
    if type:
        nodes = [n for n in nodes if n.node_type == type]
    if system:
        nodes = [n for n in nodes if n.system == system]
    if env_id:
        nodes = [n for n in nodes if n.environment_id == env_id]
    if search:
        q = search.lower()
        nodes = [n for n in nodes if q in n.display_name.lower() or q in n.qualified_name.lower()]

    return NodeListResponse(nodes=nodes, total=len(nodes))


@router.get("/{graph_id}/nodes/{node_id:path}", dependencies=[Depends(require_api_key)])
async def get_node(
    graph_id: str,
    node_id: str,
    store: GraphStore = Depends(_get_graph_store),
) -> LineageNode:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    node = graph.get_node(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    return node


@router.post("/{graph_id}/nodes", status_code=201, dependencies=[Depends(require_api_key)])
async def add_node(
    graph_id: str,
    node: LineageNode,
    store: GraphStore = Depends(_get_graph_store),
) -> NodeCreatedResponse:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    graph.add_node(node)
    store.touch(graph_id)
    return NodeCreatedResponse(status="created", node_id=node.node_id)


@router.post("/{graph_id}/edges", status_code=201, dependencies=[Depends(require_api_key)])
async def add_edge(
    graph_id: str,
    edge: LineageEdge,
    store: GraphStore = Depends(_get_graph_store),
) -> EdgeCreatedResponse:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    try:
        graph.add_edge(edge)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    store.touch(graph_id)
    return EdgeCreatedResponse(status="created", edge=f"{edge.src_id} -> {edge.dst_id}")


@router.get("/{graph_id}/edges", dependencies=[Depends(require_api_key)])
async def list_edges(
    graph_id: str,
    store: GraphStore = Depends(_get_graph_store),
) -> EdgeListResponse:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    return EdgeListResponse(edges=graph.edges, total=graph.edge_count)


# ── Traversal ──────────────────────────────────────────────────────────


@router.get(
    "/{graph_id}/query/upstream/{node_id:path}",
    dependencies=[Depends(require_api_key)],
)
async def query_upstream(
    graph_id: str,
    node_id: str,
    hops: int = Query(3, ge=1, le=50),
    store: GraphStore = Depends(_get_graph_store),
) -> TraversalResponse:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    if graph.get_node(node_id) is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    nodes = graph.upstream(node_id, hops=hops)
    return TraversalResponse(origin=node_id, direction="upstream", hops=hops, nodes=nodes)


@router.get(
    "/{graph_id}/query/downstream/{node_id:path}",
    dependencies=[Depends(require_api_key)],
)
async def query_downstream(
    graph_id: str,
    node_id: str,
    hops: int = Query(3, ge=1, le=50),
    store: GraphStore = Depends(_get_graph_store),
) -> TraversalResponse:
    graph = store.get(graph_id)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")
    if graph.get_node(node_id) is None:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
    nodes = graph.downstream(node_id, hops=hops)
    return TraversalResponse(origin=node_id, direction="downstream", hops=hops, nodes=nodes)
