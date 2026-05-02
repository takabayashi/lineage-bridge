# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.extractors.phase utilities.

Covers: safe_extract, merge_into, PhaseRunner.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from lineage_bridge.extractors import phase as phase_module
from lineage_bridge.extractors.phase import (
    PhaseResult,
    PhaseRunner,
    merge_into,
    safe_extract,
)
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


# ── merge_into ──────────────────────────────────────────────────────────


def test_merge_into_adds_nodes_and_edges():
    graph = LineageGraph()
    n1, n2 = _node("topic-a"), _node("topic-b")
    e1 = _edge("topic-a", "topic-b")

    merge_into(graph, [n1, n2], [e1])

    assert graph.node_count == 2
    assert graph.edge_count == 1
    assert graph.get_edge(e1.src_id, e1.dst_id, EdgeType.PRODUCES) is not None


def test_merge_into_appends_to_existing_graph():
    graph = LineageGraph()
    graph.add_node(_node("existing-topic"))

    merge_into(graph, [_node("new-topic")], [])

    assert graph.node_count == 2


def test_merge_into_skips_edges_with_missing_endpoints():
    graph = LineageGraph()
    n1 = _node("topic-a")

    merge_into(graph, [n1], [_edge("topic-a", "topic-b")])

    assert graph.node_count == 1
    assert graph.edge_count == 0


def test_merge_into_skips_edge_missing_source():
    graph = LineageGraph()
    n2 = _node("topic-b")

    merge_into(graph, [n2], [_edge("topic-a", "topic-b")])

    assert graph.node_count == 1
    assert graph.edge_count == 0


def test_merge_into_partial_edges():
    graph = LineageGraph()
    n1, n2 = _node("topic-a"), _node("topic-b")
    good = _edge("topic-a", "topic-b")
    bad = _edge("topic-a", "topic-missing")

    merge_into(graph, [n1, n2], [good, bad])

    assert graph.node_count == 2
    assert graph.edge_count == 1


# ── safe_extract ────────────────────────────────────────────────────────


async def test_safe_extract_success():
    async def _extract():
        return [_node("orders")], [_edge("orders", "events")]

    nodes, edges = await safe_extract("test-extractor", _extract())

    assert len(nodes) == 1
    assert nodes[0].display_name == "orders"
    assert len(edges) == 1


async def test_safe_extract_failure_returns_empty():
    async def _failing():
        raise RuntimeError("connection lost")

    nodes, edges = await safe_extract("broken-extractor", _failing())

    assert nodes == []
    assert edges == []


async def test_safe_extract_401_identification():
    messages: list[tuple[str, str]] = []

    async def _unauthorized():
        resp = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("401 Unauthorized", request=resp.request, response=resp)

    nodes, _ = await safe_extract(
        "kafka", _unauthorized(), on_progress=lambda p, d: messages.append((p, d))
    )

    assert nodes == []
    assert len(messages) == 1
    assert "401 Unauthorized" in messages[0][1]
    assert "cluster-scoped API key" in messages[0][1]


async def test_safe_extract_403_identification():
    messages: list[tuple[str, str]] = []

    async def _forbidden():
        resp = httpx.Response(403, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("403 Forbidden", request=resp.request, response=resp)

    nodes, _ = await safe_extract(
        "connect", _forbidden(), on_progress=lambda p, d: messages.append((p, d))
    )

    assert nodes == []
    assert "403 Forbidden" in messages[0][1]
    assert "lacks required permissions" in messages[0][1]


async def test_safe_extract_400_identification():
    messages: list[tuple[str, str]] = []

    async def _bad_request():
        resp = httpx.Response(400, request=httpx.Request("GET", "https://example.com"))
        raise httpx.HTTPStatusError("400 Bad Request", request=resp.request, response=resp)

    nodes, _ = await safe_extract(
        "flink", _bad_request(), on_progress=lambda p, d: messages.append((p, d))
    )

    assert nodes == []
    assert "400 Bad Request" in messages[0][1]
    assert "API parameters are invalid" in messages[0][1]


async def test_safe_extract_no_progress_callback():
    async def _fail():
        raise ValueError("something broke")

    nodes, edges = await safe_extract("test", _fail())
    assert nodes == []
    assert edges == []


async def test_safe_extract_timeout():
    messages: list[tuple[str, str]] = []

    async def _hang_forever():
        await asyncio.sleep(9999)
        return [_node("never")], []  # pragma: no cover

    original_timeout = phase_module._EXTRACTOR_TIMEOUT
    try:
        phase_module._EXTRACTOR_TIMEOUT = 0.05  # 50ms
        nodes, _ = await safe_extract(
            "slow", _hang_forever(), on_progress=lambda p, d: messages.append((p, d))
        )
    finally:
        phase_module._EXTRACTOR_TIMEOUT = original_timeout

    assert nodes == []
    assert "timed out" in messages[0][1]


async def test_safe_extract_completes_within_timeout():
    async def _fast():
        return [_node("quick")], []

    nodes, _ = await safe_extract("fast", _fast())
    assert nodes[0].display_name == "quick"


# ── PhaseRunner ─────────────────────────────────────────────────────────


@dataclass
class _StubCtx:
    """Minimal stand-in for ExtractionContext when only graph mutation matters."""

    graph: LineageGraph


async def test_phase_runner_calls_each_phase_in_order():
    """PhaseRunner walks the phase list in order and merges each result."""
    calls: list[str] = []

    class _PhaseA:
        name = "A"

        async def execute(self, ctx):
            calls.append("A")
            return PhaseResult(nodes=[_node("topic-a")])

    class _PhaseB:
        name = "B"

        async def execute(self, ctx):
            calls.append("B")
            return PhaseResult(nodes=[_node("topic-b")])

    ctx = _StubCtx(graph=LineageGraph())
    await PhaseRunner([_PhaseA(), _PhaseB()]).run(ctx)

    assert calls == ["A", "B"]
    assert ctx.graph.node_count == 2


async def test_phase_runner_merges_phase_results_into_graph():
    """Per-phase nodes + edges land in ctx.graph via merge_into."""

    class _Phase:
        name = "P"

        async def execute(self, ctx):
            return PhaseResult(
                nodes=[_node("topic-a"), _node("topic-b")],
                edges=[_edge("topic-a", "topic-b")],
            )

    ctx = _StubCtx(graph=LineageGraph())
    await PhaseRunner([_Phase()]).run(ctx)

    assert ctx.graph.node_count == 2
    assert ctx.graph.edge_count == 1


async def test_phase_runner_tolerates_phase_that_only_mutates_graph():
    """A phase returning empty PhaseResult after mutating ctx.graph still works."""

    class _MutatingPhase:
        name = "M"

        async def execute(self, ctx):
            ctx.graph.add_node(_node("inserted-by-phase"))
            return PhaseResult()  # no nodes/edges to merge

    ctx = _StubCtx(graph=LineageGraph())
    await PhaseRunner([_MutatingPhase()]).run(ctx)

    assert ctx.graph.node_count == 1
