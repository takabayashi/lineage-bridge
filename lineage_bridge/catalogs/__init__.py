# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Catalog provider registry."""

from __future__ import annotations

from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider
from lineage_bridge.catalogs.protocol import CatalogProvider
from lineage_bridge.models.graph import LineageGraph

_PROVIDERS: dict[str, CatalogProvider] = {
    "UNITY_CATALOG": DatabricksUCProvider(),
    "AWS_GLUE": GlueCatalogProvider(),
    "GOOGLE_DATA_LINEAGE": GoogleLineageProvider(),
}


def get_provider(catalog_type: str) -> CatalogProvider | None:
    """Return the provider for the given catalog type, or None if unknown."""
    return _PROVIDERS.get(catalog_type)


def get_active_providers(graph: LineageGraph) -> list[CatalogProvider]:
    """Return providers that have at least one node in the graph."""
    active_types: set[str] = set()
    for node in graph.nodes:
        for provider in _PROVIDERS.values():
            if node.node_type == provider.node_type and node.system == provider.system_type:
                active_types.add(provider.catalog_type)

    return [_PROVIDERS[ct] for ct in active_types if ct in _PROVIDERS]
