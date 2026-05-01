# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `services.push_service.run_push`.

Phase 1B: dispatcher now resolves a fresh provider per request via
`_provider_for(req.provider, settings)` and calls `provider.push_lineage()`
directly. We patch the provider classes inside push_service to inject mocks.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lineage_bridge.models.graph import LineageGraph, PushResult
from lineage_bridge.services import PUSH_PROVIDERS, PushRequest, run_push


def _settings() -> MagicMock:
    """Settings stub with all the fields _provider_for() reads."""
    s = MagicMock()
    s.databricks_workspace_url = "https://workspace.databricks.com"
    s.databricks_token = "dapi-test"
    s.aws_region = "us-east-1"
    s.aws_datazone_domain_id = "dzd-test"
    s.aws_datazone_project_id = "prj-test"
    s.gcp_project_id = "gcp-test"
    s.gcp_location = "us"
    return s


@pytest.mark.parametrize(
    "provider,patch_target",
    [
        ("databricks_uc", "lineage_bridge.catalogs.databricks_uc.DatabricksUCProvider"),
        ("aws_glue", "lineage_bridge.catalogs.aws_glue.GlueCatalogProvider"),
        ("google", "lineage_bridge.catalogs.google_lineage.GoogleLineageProvider"),
        ("datazone", "lineage_bridge.catalogs.aws_datazone.AWSDataZoneProvider"),
    ],
)
async def test_run_push_dispatches_to_correct_provider(provider, patch_target):
    """Each PushRequest provider name routes to the matching provider class."""
    graph = LineageGraph()
    req = PushRequest(provider=provider, options={})
    mock_instance = AsyncMock()
    mock_instance.push_lineage = AsyncMock(return_value=PushResult(tables_updated=1))

    with patch(patch_target, return_value=mock_instance) as MockClass:
        result = await run_push(req, _settings(), graph)

    MockClass.assert_called_once()
    mock_instance.push_lineage.assert_awaited_once()
    assert result.tables_updated == 1


async def test_run_push_forwards_options_as_kwargs():
    """`req.options` is splatted into provider.push_lineage()."""
    req = PushRequest(
        provider="databricks_uc",
        options={
            "set_properties": False,
            "set_comments": True,
            "create_bridge_table": True,
            "bridge_table_name": "main.lineage.bridge",
        },
    )
    mock_instance = AsyncMock()
    mock_instance.push_lineage = AsyncMock(return_value=PushResult())

    with patch(
        "lineage_bridge.catalogs.databricks_uc.DatabricksUCProvider",
        return_value=mock_instance,
    ):
        await run_push(req, _settings(), LineageGraph())

    kwargs = mock_instance.push_lineage.call_args.kwargs
    assert kwargs["set_properties"] is False
    assert kwargs["set_comments"] is True
    assert kwargs["create_bridge_table"] is True
    assert kwargs["bridge_table_name"] == "main.lineage.bridge"


async def test_run_push_passes_progress_callback_through():
    req = PushRequest(provider="aws_glue", options={})

    def cb(phase: str, detail: str) -> None: ...

    mock_instance = AsyncMock()
    mock_instance.push_lineage = AsyncMock(return_value=PushResult())

    with patch(
        "lineage_bridge.catalogs.aws_glue.GlueCatalogProvider",
        return_value=mock_instance,
    ):
        await run_push(req, _settings(), LineageGraph(), on_progress=cb)

    assert mock_instance.push_lineage.call_args.kwargs["on_progress"] is cb


async def test_unknown_provider_raises_validation_error():
    """Pydantic's Literal type rejects unknown providers at PushRequest construction."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PushRequest(provider="snowflake", options={})  # type: ignore[arg-type]


def test_push_providers_constant_lists_all_dispatch_keys():
    assert set(PUSH_PROVIDERS) == {"databricks_uc", "aws_glue", "google", "datazone"}
