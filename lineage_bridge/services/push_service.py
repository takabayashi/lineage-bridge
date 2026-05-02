# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Push service — single dispatcher across all catalog providers.

Looks up the provider in the catalogs registry by `catalog_type` and calls
its `push_lineage(graph, **options)` directly. Replaces the four
`run_*_push` wrappers that previously lived in `extractors/orchestrator.py`.

Friendly short names ("databricks_uc" / "aws_glue" / etc.) on the wire are
mapped to the canonical registry catalog_type strings so the API URL stays
stable when new catalogs land.
"""

from __future__ import annotations

from collections.abc import Callable

from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageGraph, PushResult
from lineage_bridge.services.requests import PushProviderName, PushRequest

ProgressCallback = Callable[[str, str], None]

# Friendly name (used in the API path + UI) → registry catalog_type.
_PROVIDER_TO_CATALOG_TYPE: dict[PushProviderName, str] = {
    "databricks_uc": "UNITY_CATALOG",
    "aws_glue": "AWS_GLUE",
    "google": "GOOGLE_DATA_LINEAGE",
    "datazone": "AWS_DATAZONE",
}

PUSH_PROVIDERS: tuple[PushProviderName, ...] = tuple(_PROVIDER_TO_CATALOG_TYPE)


def _provider_for(req_provider: PushProviderName, settings: Settings):
    """Return a catalog provider configured from `settings` for the given push request.

    Each call constructs a fresh provider with the relevant settings rather
    than relying on the registry singleton, because the singletons don't
    carry credentials by design (see ADR-007). The UI's `build_url`
    deeplinks pick up the workspace URL via the registry singleton, which
    `ui/app.py` and `extractors/orchestrator.run_extraction` reseed at
    startup / extraction time — push doesn't need to touch the registry.
    """
    catalog_type = _PROVIDER_TO_CATALOG_TYPE[req_provider]
    if catalog_type == "UNITY_CATALOG":
        from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider

        return DatabricksUCProvider(
            workspace_url=settings.databricks_workspace_url,
            token=settings.databricks_token,
        )
    if catalog_type == "AWS_GLUE":
        from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider

        return GlueCatalogProvider(region=settings.aws_region)
    if catalog_type == "GOOGLE_DATA_LINEAGE":
        from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider

        return GoogleLineageProvider(
            project_id=settings.gcp_project_id,
            location=settings.gcp_location,
        )
    if catalog_type == "AWS_DATAZONE":
        from lineage_bridge.catalogs.aws_datazone import AWSDataZoneProvider

        return AWSDataZoneProvider(
            domain_id=settings.aws_datazone_domain_id,
            project_id=settings.aws_datazone_project_id,
            region=settings.aws_region,
        )
    raise NotImplementedError(
        f"Push dispatch not wired for catalog_type={catalog_type!r}; "
        f"add a branch to push_service._provider_for."
    )


async def run_push(
    req: PushRequest,
    settings: Settings,
    graph: LineageGraph,
    on_progress: ProgressCallback | None = None,
) -> PushResult:
    """Dispatch *req* to the matching provider and return its `PushResult`."""
    if req.provider not in _PROVIDER_TO_CATALOG_TYPE:
        raise ValueError(
            f"Unknown push provider: {req.provider!r}. Known: {', '.join(PUSH_PROVIDERS)}."
        )

    provider = _provider_for(req.provider, settings)
    return await provider.push_lineage(graph, on_progress=on_progress, **req.options)
