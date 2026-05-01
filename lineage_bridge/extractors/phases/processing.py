# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 2 — Connect, ksqlDB, Flink: transformation and processing edges.

The three extractors run concurrently via `asyncio.gather` because their write
sets are disjoint — Connect emits connector + external dataset nodes, ksqlDB
emits query nodes, Flink emits job nodes; none of them touch each other's
node IDs. Topic nodes are read-only (added by Phase 1).

Flink needs an organization ID, which the cluster payload usually carries but
sometimes doesn't (older API versions, regional brownouts). This module walks
three locations in the cluster spec, then falls back to fetching the env
metadata. If all four fail, Flink is skipped with a warning rather than failing
the whole phase.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from lineage_bridge.clients.connect import ConnectClient
from lineage_bridge.clients.flink import FlinkClient
from lineage_bridge.clients.ksqldb import KsqlDBClient
from lineage_bridge.extractors.context import ExtractionContext
from lineage_bridge.extractors.phase import PhaseResult, safe_extract
from lineage_bridge.models.graph import LineageEdge, LineageNode

logger = logging.getLogger(__name__)


async def _resolve_flink_org_id(ctx: ExtractionContext) -> str:
    """Walk the cluster payload for an organization ID, then fall back to the env API."""
    for cluster in ctx.clusters:
        org_id = cluster.get("spec", {}).get("organization", {}).get("id", "")
        if org_id:
            return org_id
        org_id = cluster.get("organization", {}).get("id", "")
        if org_id:
            return org_id
        resource_name = cluster.get("metadata", {}).get("resource_name", "")
        if "/organization=" in resource_name:
            org_id = resource_name.split("/organization=")[1].split("/")[0]
            if org_id:
                return org_id

    # Last resort: fetch env metadata
    try:
        env_data = await ctx.cloud.get(f"/org/v2/environments/{ctx.env_id}")
        resource_name = env_data.get("metadata", {}).get("resource_name", "")
        if "/organization=" in resource_name:
            return resource_name.split("/organization=")[1].split("/")[0]
    except Exception:
        logger.debug("Failed to fetch org ID from environment %s", ctx.env_id, exc_info=True)
    return ""


class ProcessingPhase:
    """Phase 2 — Connect + ksqlDB + Flink, run in parallel."""

    name = "Phase 2/4 — Processing"

    async def execute(self, ctx: ExtractionContext) -> PhaseResult:
        ctx.progress("Phase 2/4", "Extracting connectors, ksqlDB, Flink")
        tasks: list[Any] = []

        if ctx.enable_connect:
            for cluster in ctx.clusters:
                cluster_id = cluster.get("id", "")
                connect_client = ConnectClient(
                    api_key=ctx.settings.confluent_cloud_api_key,
                    api_secret=ctx.settings.confluent_cloud_api_secret,
                    environment_id=ctx.env_id,
                    kafka_cluster_id=cluster_id,
                )

                async def _run_connect(
                    c: ConnectClient = connect_client,
                    cid: str = cluster_id,
                ) -> tuple[list[LineageNode], list[LineageEdge]]:
                    async with c:
                        return await safe_extract(f"Connect:{cid}", c.extract(), ctx.on_progress)

                tasks.append(_run_connect())

        if ctx.enable_ksqldb:
            ksql_client = KsqlDBClient(
                cloud_api_key=ctx.settings.confluent_cloud_api_key,
                cloud_api_secret=ctx.settings.confluent_cloud_api_secret,
                environment_id=ctx.env_id,
                ksqldb_api_key=ctx.settings.ksqldb_api_key,
                ksqldb_api_secret=ctx.settings.ksqldb_api_secret,
            )

            async def _run_ksqldb() -> tuple[list[LineageNode], list[LineageEdge]]:
                async with ksql_client:
                    return await safe_extract("ksqlDB", ksql_client.extract(), ctx.on_progress)

            tasks.append(_run_ksqldb())

        if ctx.enable_flink:
            org_id = await _resolve_flink_org_id(ctx)
            if org_id:
                if ctx.flink_credentials and ctx.env_id in ctx.flink_credentials:
                    flink_key = ctx.flink_credentials[ctx.env_id]["api_key"]
                    flink_secret = ctx.flink_credentials[ctx.env_id]["api_secret"]
                else:
                    flink_key = ctx.settings.flink_api_key
                    flink_secret = ctx.settings.flink_api_secret

                flink_client = FlinkClient(
                    cloud_api_key=ctx.settings.confluent_cloud_api_key,
                    cloud_api_secret=ctx.settings.confluent_cloud_api_secret,
                    environment_id=ctx.env_id,
                    organization_id=org_id,
                    flink_api_key=flink_key,
                    flink_api_secret=flink_secret,
                )

                async def _run_flink() -> tuple[list[LineageNode], list[LineageEdge]]:
                    async with flink_client:
                        return await safe_extract("Flink", flink_client.extract(), ctx.on_progress)

                tasks.append(_run_flink())
            else:
                ctx.progress(
                    "Warning",
                    f"Could not determine organization ID for Flink in {ctx.env_id}"
                    " — Flink extraction skipped",
                )

        if not tasks:
            return PhaseResult()

        all_nodes: list[LineageNode] = []
        all_edges: list[LineageEdge] = []
        for nodes, edges in await asyncio.gather(*tasks):
            all_nodes.extend(nodes)
            all_edges.extend(edges)
        return PhaseResult(nodes=all_nodes, edges=all_edges)
