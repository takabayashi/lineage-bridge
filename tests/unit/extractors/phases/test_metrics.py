# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for MetricsPhase (Phase 5)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from lineage_bridge.extractors.phases.metrics import MetricsPhase
from lineage_bridge.models.graph import LineageGraph, LineageNode, NodeType, SystemType
from tests.unit.extractors.conftest import make_settings


def _topic_node(cluster_id: str = "lkc-test1") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:kafka_topic:env-test1:{cluster_id}-topic",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=f"{cluster_id}-topic",
        display_name="topic",
        environment_id="env-test1",
        cluster_id=cluster_id,
    )


async def test_metrics_phase_calls_enrich_per_cluster(no_sleep):
    """One MetricsClient.enrich() call per distinct cluster_id on graph nodes."""
    settings = make_settings()
    graph = LineageGraph()
    graph.add_node(_topic_node("lkc-a"))
    graph.add_node(_topic_node("lkc-b"))

    with patch("lineage_bridge.extractors.phases.metrics.MetricsClient") as MockMetrics:
        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(return_value=3)
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        total = await MetricsPhase().run(settings, graph)

    assert metrics_inst.enrich.await_count == 2
    assert total == 6  # 3 per cluster x 2 clusters


async def test_metrics_phase_swallows_per_cluster_failures(no_sleep):
    """A failed cluster does not abort the rest of the metrics fan-out."""
    settings = make_settings()
    graph = LineageGraph()
    graph.add_node(_topic_node())

    with patch("lineage_bridge.extractors.phases.metrics.MetricsClient") as MockMetrics:
        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(side_effect=RuntimeError("metrics fail"))
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        # Should not raise
        total = await MetricsPhase().run(settings, graph)

    assert total == 0


async def test_metrics_phase_skips_nodes_without_cluster_id(no_sleep):
    """Nodes without a cluster_id (external datasets, schemas) don't trigger calls."""
    settings = make_settings()
    graph = LineageGraph()
    extless = LineageNode(
        node_id="confluent:external_dataset:env-test1:s3://bucket",
        system=SystemType.EXTERNAL,
        node_type=NodeType.EXTERNAL_DATASET,
        qualified_name="s3://bucket",
        display_name="bucket",
        environment_id="env-test1",
    )
    graph.add_node(extless)

    with patch("lineage_bridge.extractors.phases.metrics.MetricsClient") as MockMetrics:
        metrics_inst = AsyncMock()
        metrics_inst.enrich = AsyncMock(return_value=0)
        metrics_inst.__aenter__ = AsyncMock(return_value=metrics_inst)
        metrics_inst.__aexit__ = AsyncMock(return_value=False)
        MockMetrics.return_value = metrics_inst

        await MetricsPhase().run(settings, graph)

    metrics_inst.enrich.assert_not_called()
