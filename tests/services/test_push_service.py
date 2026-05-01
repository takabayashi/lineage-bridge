# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `services.push_service.run_push`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lineage_bridge.models.graph import LineageGraph, PushResult
from lineage_bridge.services import PUSH_PROVIDERS, PushRequest, run_push


@pytest.mark.parametrize(
    "provider,patch_target",
    [
        ("databricks_uc", "lineage_bridge.extractors.orchestrator.run_lineage_push"),
        ("aws_glue", "lineage_bridge.extractors.orchestrator.run_glue_push"),
        ("google", "lineage_bridge.extractors.orchestrator.run_google_push"),
        ("datazone", "lineage_bridge.extractors.orchestrator.run_datazone_push"),
    ],
)
async def test_run_push_dispatches_to_correct_provider(provider, patch_target):
    settings = MagicMock()
    graph = LineageGraph()
    req = PushRequest(provider=provider, options={})

    with patch(patch_target, new=AsyncMock(return_value=PushResult(tables_updated=1))) as mock:
        result = await run_push(req, settings, graph)

    mock.assert_awaited_once()
    assert result.tables_updated == 1


async def test_run_push_forwards_options_as_kwargs():
    """`req.options` is splatted into the underlying push function."""
    req = PushRequest(
        provider="databricks_uc",
        options={
            "set_properties": False,
            "set_comments": True,
            "create_bridge_table": True,
            "bridge_table_name": "main.lineage.bridge",
        },
    )

    with patch(
        "lineage_bridge.extractors.orchestrator.run_lineage_push",
        new=AsyncMock(return_value=PushResult()),
    ) as mock:
        await run_push(req, MagicMock(), LineageGraph())

    kwargs = mock.call_args.kwargs
    assert kwargs["set_properties"] is False
    assert kwargs["set_comments"] is True
    assert kwargs["create_bridge_table"] is True
    assert kwargs["bridge_table_name"] == "main.lineage.bridge"


async def test_run_push_passes_progress_callback_through():
    req = PushRequest(provider="aws_glue", options={})

    def cb(phase: str, detail: str) -> None: ...

    with patch(
        "lineage_bridge.extractors.orchestrator.run_glue_push",
        new=AsyncMock(return_value=PushResult()),
    ) as mock:
        await run_push(req, MagicMock(), LineageGraph(), on_progress=cb)

    assert mock.call_args.kwargs["on_progress"] is cb


async def test_unknown_provider_raises_validation_error():
    """Pydantic's Literal type rejects unknown providers at PushRequest construction."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PushRequest(provider="snowflake", options={})  # type: ignore[arg-type]


def test_push_providers_constant_lists_all_dispatch_keys():
    assert set(PUSH_PROVIDERS) == {"databricks_uc", "aws_glue", "google", "datazone"}
