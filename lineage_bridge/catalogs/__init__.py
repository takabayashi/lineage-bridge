# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Catalog provider registry.

Dispatches on the `catalog_type` string (per ADR-021); all catalog tables
share `NodeType.CATALOG_TABLE`, so we no longer need `(node_type, system)`
matching to figure out which provider owns a node.
"""

from __future__ import annotations

from lineage_bridge.catalogs.aws_datazone import AWSDataZoneProvider
from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider
from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider
from lineage_bridge.catalogs.protocol import CatalogProvider
from lineage_bridge.models.graph import LineageGraph, NodeType

_PROVIDERS: dict[str, CatalogProvider] = {
    "UNITY_CATALOG": DatabricksUCProvider(),
    "AWS_GLUE": GlueCatalogProvider(),
    "GOOGLE_DATA_LINEAGE": GoogleLineageProvider(),
    "AWS_DATAZONE": AWSDataZoneProvider(),
}


def configure_providers(
    *,
    databricks_workspace_url: str | None = None,
    databricks_token: str | None = None,
) -> None:
    """Reconfigure the registry's provider singletons with runtime settings.

    Call this once at app entry (orchestrator, UI, watcher) so that
    ``get_provider`` returns instances aware of the user's actual
    workspace URL — not whatever value Confluent happens to have stored
    in its Tableflow catalog-integration config.
    """
    _PROVIDERS["UNITY_CATALOG"] = DatabricksUCProvider(
        workspace_url=databricks_workspace_url,
        token=databricks_token,
    )


def get_provider(catalog_type: str) -> CatalogProvider | None:
    """Return the provider for the given catalog type, or None if unknown."""
    return _PROVIDERS.get(catalog_type)


def get_active_providers(graph: LineageGraph) -> list[CatalogProvider]:
    """Return providers that have at least one CATALOG_TABLE node in the graph."""
    active_types = {
        n.catalog_type
        for n in graph.nodes
        if n.node_type == NodeType.CATALOG_TABLE and n.catalog_type
    }
    return [_PROVIDERS[ct] for ct in active_types if ct in _PROVIDERS]
