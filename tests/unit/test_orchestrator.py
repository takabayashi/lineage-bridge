# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.extractors.orchestrator helpers.

Tests for _merge_into and _safe_extract, which are module-level functions.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from lineage_bridge.extractors.orchestrator import _merge_into, _safe_extract
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _node(name: str, node_type: NodeType = NodeType.KAFKA_TOPIC) -> LineageNode:
    nid = f"confluent:{node_type.value}:env-1:{name}"
    return LineageNode(
        node_id=nid,
        system=SystemType.CONFLUENT,
        node_type=node_type,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id="lkc-1",
    )


def _edge(src_name: str, dst_name: str, edge_type: EdgeType = EdgeType.PRODUCES) -> LineageEdge:
    src_id = f"confluent:kafka_topic:env-1:{src_name}"
    dst_id = f"confluent:kafka_topic:env-1:{dst_name}"
    return LineageEdge(src_id=src_id, dst_id=dst_id, edge_type=edge_type)


# ── _merge_into ──────────────────────────────────────────────────────────


def test_merge_into_adds_nodes_and_edges():
    """_merge_into adds new nodes and valid edges to the graph."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    n2 = _node("topic-b")
    e1 = _edge("topic-a", "topic-b")

    _merge_into(graph, [n1, n2], [e1])

    assert graph.node_count == 2
    assert graph.edge_count == 1
    assert graph.get_node(n1.node_id) is not None
    assert graph.get_node(n2.node_id) is not None
    assert graph.get_edge(e1.src_id, e1.dst_id, EdgeType.PRODUCES) is not None


def test_merge_into_adds_nodes_to_existing_graph():
    """_merge_into appends to a graph that already has nodes."""
    graph = LineageGraph()
    existing = _node("existing-topic")
    graph.add_node(existing)

    new_node = _node("new-topic")
    _merge_into(graph, [new_node], [])

    assert graph.node_count == 2


def test_merge_into_skips_edges_with_missing_endpoints():
    """_merge_into silently skips edges whose source or destination is not in the graph."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    # Edge references topic-b which is NOT added
    bad_edge = _edge("topic-a", "topic-b")

    _merge_into(graph, [n1], [bad_edge])

    assert graph.node_count == 1
    assert graph.edge_count == 0  # edge was skipped, not raised


def test_merge_into_skips_edge_missing_source():
    """_merge_into skips edges when the source node is missing."""
    graph = LineageGraph()

    n2 = _node("topic-b")
    bad_edge = _edge("topic-a", "topic-b")

    _merge_into(graph, [n2], [bad_edge])

    assert graph.node_count == 1
    assert graph.edge_count == 0


def test_merge_into_partial_edges():
    """_merge_into adds valid edges and skips invalid ones in the same call."""
    graph = LineageGraph()

    n1 = _node("topic-a")
    n2 = _node("topic-b")
    good_edge = _edge("topic-a", "topic-b")
    bad_edge = _edge("topic-a", "topic-missing")

    _merge_into(graph, [n1, n2], [good_edge, bad_edge])

    assert graph.node_count == 2
    assert graph.edge_count == 1  # only the good edge


# ── _safe_extract ────────────────────────────────────────────────────────


async def test_safe_extract_success():
    """_safe_extract returns the coroutine result on success."""

    async def _extract():
        return [_node("orders")], [_edge("orders", "events")]

    # Add both nodes so the edge check in the test makes sense
    nodes, edges = await _safe_extract("test-extractor", _extract())

    assert len(nodes) == 1
    assert nodes[0].display_name == "orders"
    assert len(edges) == 1


async def test_safe_extract_failure_returns_empty():
    """_safe_extract returns ([], []) when the coroutine raises."""

    async def _failing():
        raise RuntimeError("connection lost")

    nodes, edges = await _safe_extract("broken-extractor", _failing())

    assert nodes == []
    assert edges == []


async def test_safe_extract_401_identification():
    """_safe_extract identifies 401 Unauthorized errors and calls on_progress."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _unauthorized():
        resp = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("401 Unauthorized", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("kafka", _unauthorized(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "401 Unauthorized" in messages[0][1]
    assert "cluster-scoped API key" in messages[0][1]


async def test_safe_extract_403_identification():
    """_safe_extract identifies 403 Forbidden errors."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _forbidden():
        resp = httpx.Response(403, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("403 Forbidden", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("connect", _forbidden(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "403 Forbidden" in messages[0][1]
    assert "lacks required permissions" in messages[0][1]


async def test_safe_extract_400_identification():
    """_safe_extract identifies 400 Bad Request errors."""
    messages: list[tuple[str, str]] = []

    def _progress(phase: str, detail: str) -> None:
        messages.append((phase, detail))

    async def _bad_request():
        resp = httpx.Response(400, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("400 Bad Request", request=resp.request, response=resp)

    nodes, edges = await _safe_extract("flink", _bad_request(), on_progress=_progress)

    assert nodes == []
    assert edges == []
    assert len(messages) == 1
    assert "400 Bad Request" in messages[0][1]
    assert "API parameters are invalid" in messages[0][1]


async def test_safe_extract_no_progress_callback():
    """_safe_extract works without an on_progress callback."""

    async def _fail():
        raise ValueError("something broke")

    # Should not raise even with on_progress=None (the default)
    nodes, edges = await _safe_extract("test", _fail())
    assert nodes == []
    assert edges == []
