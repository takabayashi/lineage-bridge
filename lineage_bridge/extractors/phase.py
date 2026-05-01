# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Extraction phase contract — protocol, result type, and shared helpers.

A phase is one step in the per-environment extraction pipeline. The pipeline is
driven by `PhaseRunner.run()` which calls each phase in registration order and
merges its `PhaseResult` into `ctx.graph`. Phases own any internal parallel
fan-out (e.g. Phase 2 runs Connect/ksqlDB/Flink concurrently with `asyncio.gather`).

Cross-phase parallelism is not modelled here — the current pipeline is strictly
sequential between phases (Phase 1's output feeds Phase 4 cluster IDs, Phase 3's
SR endpoint comes from setup, etc.). When a real need for cross-phase parallelism
appears, add a `parallel_with: tuple[str, ...]` field to the protocol and a group
runner.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from lineage_bridge.models.graph import LineageEdge, LineageNode

if TYPE_CHECKING:
    from lineage_bridge.extractors.context import ExtractionContext

logger = logging.getLogger(__name__)


# ── result type ─────────────────────────────────────────────────────────


@dataclass
class PhaseResult:
    """Output of a single phase execution.

    Phases may also mutate `ctx.graph` directly (StreamCatalog enrichment does
    this). When they do, they typically return an empty PhaseResult — the
    runner's merge step is a no-op.
    """

    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)


# ── phase protocol ──────────────────────────────────────────────────────


class ExtractionPhase(Protocol):
    """One step in the per-environment extraction pipeline."""

    name: str

    async def execute(self, ctx: ExtractionContext) -> PhaseResult:
        """Run the phase against `ctx`. May mutate `ctx.graph` directly."""
        ...


# ── shared helpers ──────────────────────────────────────────────────────

_EXTRACTOR_TIMEOUT = 120  # seconds — per-extractor ceiling


async def safe_extract(
    label: str, coro: Any, on_progress: Any = None
) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Run an extractor coroutine, returning empty on failure or timeout.

    Auth errors (401/403) and 400s get human-readable progress messages so the
    UI can surface them without the user digging through logs.
    """
    try:
        return await asyncio.wait_for(coro, timeout=_EXTRACTOR_TIMEOUT)
    except TimeoutError:
        detail = f"Extractor '{label}' timed out after {_EXTRACTOR_TIMEOUT}s"
        logger.warning(detail)
        if on_progress:
            on_progress("Warning", detail)
        return [], []
    except Exception as exc:
        msg = str(exc)
        if "401" in msg or "Unauthorized" in msg:
            detail = (
                f"Extractor '{label}' got 401 Unauthorized. "
                "This likely means a cluster-scoped API key is needed. "
                "Set LINEAGE_BRIDGE_KAFKA_API_KEY in .env."
            )
        elif "403" in msg or "Forbidden" in msg:
            detail = (
                f"Extractor '{label}' got 403 Forbidden. The API key lacks required permissions."
            )
        elif "400" in msg or "Bad Request" in msg:
            detail = (
                f"Extractor '{label}' got 400 Bad Request. "
                "The API key may not have access to this environment, "
                "or the API parameters are invalid."
            )
        else:
            detail = f"Extractor '{label}' failed: {exc}"

        logger.warning(detail, exc_info=True)
        if on_progress:
            on_progress("Warning", detail)
        return [], []


def merge_into(
    graph: Any,
    nodes: list[LineageNode],
    edges: list[LineageEdge],
) -> None:
    """Add *nodes* and *edges* into *graph*, tolerating missing edge endpoints.

    Edges whose endpoints aren't (yet) in the graph are dropped — common for
    cross-cluster references that arrive in a later phase, or for partial
    extractions where some phases are disabled.
    """
    for node in nodes:
        graph.add_node(node)
    for edge in edges:
        try:
            graph.add_edge(edge)
        except ValueError:
            logger.debug(
                "Skipping edge %s -> %s (%s): endpoint not in graph",
                edge.src_id,
                edge.dst_id,
                edge.edge_type.value,
            )


# ── runner ──────────────────────────────────────────────────────────────


class PhaseRunner:
    """Run a sequence of phases against an ExtractionContext.

    The runner is the single mutation point for `ctx.graph`: each phase's
    `PhaseResult` is merged via `merge_into()` after `execute()` returns.
    """

    def __init__(self, phases: list[ExtractionPhase]):
        self.phases = phases

    async def run(self, ctx: ExtractionContext) -> None:
        for phase in self.phases:
            result = await phase.execute(ctx)
            merge_into(ctx.graph, result.nodes, result.edges)
