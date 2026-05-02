# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 4 — Tableflow: bridge topics to UC / Glue / other catalogs.

Runs last because catalog node creation depends on topic nodes already being
in the graph (Phase 1 output). Delegates per-catalog node construction to
registered CatalogProviders via `TableflowClient.extract()`.
"""

from __future__ import annotations

import logging

from lineage_bridge.clients.tableflow import TableflowClient
from lineage_bridge.extractors.context import ExtractionContext
from lineage_bridge.extractors.phase import PhaseResult, safe_extract

logger = logging.getLogger(__name__)


class TableflowPhase:
    """Phase 4 — extract Tableflow integrations and build catalog nodes."""

    name = "Phase 4/4 — Tableflow"

    async def execute(self, ctx: ExtractionContext) -> PhaseResult:
        if not ctx.enable_tableflow:
            return PhaseResult()

        ctx.progress("Phase 4/4", "Extracting Tableflow & catalog integrations")
        # Per-env Tableflow SA key fallback (cached by demo provision scripts
        # as `lineage-bridge-tableflow-<demo>-<env_id>`). Same multi-demo
        # rationale as the ksqlDB phase.
        tf_key = ctx.settings.tableflow_api_key
        tf_secret = ctx.settings.tableflow_api_secret
        if not tf_key:
            from lineage_bridge.config.cache import find_provisioned_key

            tf_key, tf_secret = find_provisioned_key("lineage-bridge-tableflow-", ctx.env_id)
        if not tf_key:
            tf_key = ctx.settings.confluent_cloud_api_key
            tf_secret = ctx.settings.confluent_cloud_api_secret
        tf_cluster_ids = [c.get("id", "") for c in ctx.clusters if c.get("id")]
        tf_client = TableflowClient(
            api_key=tf_key,
            api_secret=tf_secret,
            environment_id=ctx.env_id,
            cluster_ids=tf_cluster_ids,
        )
        async with tf_client:
            nodes, edges = await safe_extract("Tableflow", tf_client.extract(), ctx.on_progress)
        return PhaseResult(nodes=nodes, edges=edges)
