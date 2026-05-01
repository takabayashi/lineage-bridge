# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 3 — SchemaRegistry + StreamCatalog: schema and metadata enrichment.

SchemaRegistry adds SCHEMA nodes + HAS_SCHEMA edges. StreamCatalog mutates
existing topic nodes in place with tags and business metadata, and returns
no new nodes — its `enrich()` method writes directly to `ctx.graph`.

Both run in parallel; both share the env's discovered SR endpoint.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from lineage_bridge.clients.schema_registry import SchemaRegistryClient
from lineage_bridge.clients.stream_catalog import StreamCatalogClient
from lineage_bridge.extractors.context import ExtractionContext
from lineage_bridge.extractors.phase import PhaseResult, safe_extract
from lineage_bridge.models.graph import LineageEdge, LineageNode

logger = logging.getLogger(__name__)


class SchemaEnrichmentPhase:
    """Phase 3 — SchemaRegistry + StreamCatalog enrichment, run in parallel."""

    name = "Phase 3/4 — Schema Enrichment"

    async def execute(self, ctx: ExtractionContext) -> PhaseResult:
        ctx.progress("Phase 3/4", "Enriching with schemas & catalog metadata")
        tasks: list[Any] = []

        if ctx.enable_schema_registry:
            if ctx.sr_endpoint:
                sr_client = SchemaRegistryClient(
                    base_url=ctx.sr_endpoint,
                    api_key=ctx.sr_key,
                    api_secret=ctx.sr_secret,
                    environment_id=ctx.env_id,
                )

                async def _run_sr() -> tuple[list[LineageNode], list[LineageEdge]]:
                    async with sr_client:
                        return await safe_extract(
                            "SchemaRegistry", sr_client.extract(), ctx.on_progress
                        )

                tasks.append(_run_sr())
            else:
                ctx.progress("Skipped", "Schema Registry — no endpoint discovered")

        if ctx.enable_stream_catalog and not ctx.sr_endpoint:
            ctx.progress("Skipped", "Stream Catalog — no SR endpoint discovered")
        elif ctx.enable_stream_catalog and ctx.sr_endpoint:
            catalog_client = StreamCatalogClient(
                base_url=ctx.sr_endpoint,
                api_key=ctx.sr_key,
                api_secret=ctx.sr_secret,
                environment_id=ctx.env_id,
            )

            async def _run_catalog() -> tuple[list[LineageNode], list[LineageEdge]]:
                async with catalog_client:
                    try:
                        await catalog_client.enrich(ctx.graph)
                    except Exception:
                        logger.warning("StreamCatalog enrichment failed", exc_info=True)
                    return [], []

            tasks.append(_run_catalog())

        if not tasks:
            return PhaseResult()

        all_nodes: list[LineageNode] = []
        all_edges: list[LineageEdge] = []
        for nodes, edges in await asyncio.gather(*tasks):
            all_nodes.extend(nodes)
            all_edges.extend(edges)
        return PhaseResult(nodes=all_nodes, edges=all_edges)
