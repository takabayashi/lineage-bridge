# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 4b — Catalog enrichment: providers fill in metadata for their own nodes.

Despite living under `phases/`, this is **not** an `ExtractionPhase` — it runs
post-extraction in `run_enrichment()`, not inside `PhaseRunner`. The signature
takes `(settings, graph)` directly because there is no per-environment context
at this stage; the graph already spans every environment.

Singleton providers from the registry don't carry credentials (see ADR-007),
so this enricher instantiates a fresh provider per `catalog_type` with
credentials from Settings before calling `enrich()`. Failures are logged and
swallowed — a missing IAM permission or network blip on one catalog should
not lose the rest of the graph.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from lineage_bridge.catalogs import get_active_providers
from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageGraph

logger = logging.getLogger(__name__)


async def run_catalog_enrichment(
    settings: Settings,
    graph: LineageGraph,
    on_progress: Callable[[str, str], None] | None = None,
) -> int:
    """Enrich catalog nodes in place. Returns the number of providers run."""
    active_providers = get_active_providers(graph)
    if not active_providers:
        return 0

    if on_progress:
        on_progress("Enrichment", "Enriching catalog nodes")

    for provider in active_providers:
        if provider.catalog_type == "UNITY_CATALOG":
            provider = DatabricksUCProvider(
                workspace_url=settings.databricks_workspace_url,
                token=settings.databricks_token,
            )
        elif provider.catalog_type == "AWS_GLUE":
            from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider

            provider = GlueCatalogProvider(region=settings.aws_region)
        elif provider.catalog_type == "GOOGLE_DATA_LINEAGE":
            from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider

            provider = GoogleLineageProvider(
                project_id=settings.gcp_project_id,
                location=settings.gcp_location,
            )
        try:
            await provider.enrich(graph)
        except Exception:
            logger.warning(
                "Catalog enrichment failed for %s",
                provider.catalog_type,
                exc_info=True,
            )

    if on_progress:
        on_progress("Enrichment", f"Enriched with {len(active_providers)} catalog provider(s)")
    return len(active_providers)
