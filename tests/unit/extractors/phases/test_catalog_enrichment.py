# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for run_catalog_enrichment (Phase 4b)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from lineage_bridge.extractors.phases.catalog_enrichment import run_catalog_enrichment
from lineage_bridge.models.graph import LineageGraph, LineageNode, NodeType, SystemType
from tests.unit.extractors.conftest import make_settings


def _uc_node() -> LineageNode:
    return LineageNode(
        node_id="databricks:uc_table:env-test1:cat.schema.tbl",
        system=SystemType.DATABRICKS,
        node_type=NodeType.CATALOG_TABLE,
        catalog_type="UNITY_CATALOG",
        qualified_name="cat.schema.tbl",
        display_name="cat.schema.tbl",
        environment_id="env-test1",
        cluster_id="lkc-test1",
        attributes={"workspace_url": "https://myworkspace.databricks.com"},
    )


async def test_catalog_enrichment_no_active_providers_is_a_noop(no_sleep):
    """Empty graph = no providers active = no calls made, returns 0."""
    settings = make_settings()
    graph = LineageGraph()

    count = await run_catalog_enrichment(settings, graph)

    assert count == 0


async def test_catalog_enrichment_constructs_uc_provider_with_credentials(no_sleep):
    """A UC node in the graph triggers a fresh DatabricksUCProvider with creds (per ADR-007)."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )
    graph = LineageGraph()
    graph.add_node(_uc_node())

    with patch(
        "lineage_bridge.extractors.phases.catalog_enrichment.DatabricksUCProvider"
    ) as MockUC:
        mock_provider = AsyncMock()
        mock_provider.catalog_type = "UNITY_CATALOG"
        mock_provider.enrich = AsyncMock()
        MockUC.return_value = mock_provider

        await run_catalog_enrichment(settings, graph)

    MockUC.assert_called_once_with(
        workspace_url="https://myworkspace.databricks.com",
        token="dapi-test-token",
    )
    mock_provider.enrich.assert_awaited_once_with(graph)


async def test_catalog_enrichment_provider_failure_is_swallowed(no_sleep):
    """A provider raising during enrich() does not abort the phase."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )
    graph = LineageGraph()
    graph.add_node(_uc_node())

    with patch(
        "lineage_bridge.extractors.phases.catalog_enrichment.DatabricksUCProvider"
    ) as MockUC:
        mock_provider = AsyncMock()
        mock_provider.catalog_type = "UNITY_CATALOG"
        mock_provider.enrich = AsyncMock(side_effect=RuntimeError("API down"))
        MockUC.return_value = mock_provider

        # Should not raise
        count = await run_catalog_enrichment(settings, graph)

    mock_provider.enrich.assert_awaited_once()
    assert count == 1  # provider was attempted


async def test_catalog_enrichment_emits_progress_messages(
    no_sleep, progress_callback, progress_log
):
    """Progress callback receives an Enrichment start + count message."""
    settings = make_settings(
        databricks_workspace_url="https://myworkspace.databricks.com",
        databricks_token="dapi-test-token",
    )
    graph = LineageGraph()
    graph.add_node(_uc_node())

    with patch(
        "lineage_bridge.extractors.phases.catalog_enrichment.DatabricksUCProvider"
    ) as MockUC:
        mock_provider = AsyncMock()
        mock_provider.catalog_type = "UNITY_CATALOG"
        mock_provider.enrich = AsyncMock()
        MockUC.return_value = mock_provider

        await run_catalog_enrichment(settings, graph, on_progress=progress_callback)

    assert any("catalog provider" in m[1] for m in progress_log)
