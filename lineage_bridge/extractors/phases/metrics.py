# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 5 — Metrics: live throughput enrichment via the Confluent Metrics API.

Runs after extraction, not as part of `_extract_environment`. Iterates the
distinct cluster IDs already on graph nodes; one Metrics API call per cluster.
Failures per cluster are logged and don't abort the rest.
"""

from __future__ import annotations

import logging

from lineage_bridge.clients.metrics import MetricsClient
from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageGraph

logger = logging.getLogger(__name__)


class MetricsPhase:
    """Phase 5 — fold throughput metrics onto existing nodes."""

    name = "Phase 5 — Metrics"

    async def run(
        self,
        settings: Settings,
        graph: LineageGraph,
        *,
        lookback_hours: int = 1,
        on_progress: object | None = None,
    ) -> int:
        """Enrich nodes with metrics. Returns total nodes enriched across all clusters."""
        if on_progress:
            on_progress("Metrics", "Enriching nodes with real-time metrics")

        metrics_client = MetricsClient(
            api_key=settings.confluent_cloud_api_key,
            api_secret=settings.confluent_cloud_api_secret,
            lookback_hours=lookback_hours,
        )
        cluster_ids = {n.cluster_id for n in graph.nodes if n.cluster_id}
        async with metrics_client:
            total_enriched = 0
            for cluster_id in cluster_ids:
                try:
                    enriched = await metrics_client.enrich(graph, cluster_id)
                    total_enriched += enriched
                except Exception as exc:
                    logger.warning(
                        "Metrics enrichment failed for %s: %s",
                        cluster_id,
                        exc,
                    )
        if on_progress:
            on_progress("Metrics", f"Enriched {total_enriched} nodes with metrics")
        return total_enriched
