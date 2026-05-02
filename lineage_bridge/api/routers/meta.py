# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Meta endpoints: health, version, catalogs, OpenAPI spec."""

from __future__ import annotations

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import Response

from lineage_bridge.api.schemas import CatalogInfo, StatusResponse, VersionResponse

router = APIRouter()


@router.get("/health")
async def health() -> StatusResponse:
    return StatusResponse(status="ok")


@router.get("/version")
async def version() -> VersionResponse:
    return VersionResponse(version="0.5.0", name="lineage-bridge")


# Map catalog_type → owning cloud system, for the legacy `system_type` field
# on CatalogInfo. Adding a new catalog = one entry here.
_CATALOG_SYSTEM = {
    "UNITY_CATALOG": "databricks",
    "AWS_GLUE": "aws",
    "GOOGLE_DATA_LINEAGE": "google",
    "AWS_DATAZONE": "aws",
}


@router.get("/catalogs")
async def catalogs() -> list[CatalogInfo]:
    """List registered catalog providers."""
    from lineage_bridge.catalogs import _PROVIDERS

    # Per ADR-021, all catalog providers create CATALOG_TABLE nodes; the
    # discriminator is `catalog_type` itself. `node_type` + `system_type`
    # are kept on the response shape (with derived values) for v0.4.x
    # client compatibility.
    return [
        CatalogInfo(catalog_type=ct, system_type=_CATALOG_SYSTEM.get(ct, ""))
        for ct in _PROVIDERS
    ]


@router.get("/openapi.yaml", include_in_schema=False)
async def openapi_yaml(request: Request) -> Response:
    """Download the OpenAPI spec as YAML."""
    spec = request.app.openapi()
    return Response(
        content=yaml.dump(spec, sort_keys=False, allow_unicode=True),
        media_type="text/yaml",
    )
