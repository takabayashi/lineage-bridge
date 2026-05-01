# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pydantic v2 request models for the service layer.

These are the wire format between callers (UI, API, watcher, CLI) and the
extraction/enrichment/push services. They are frozen so the same instance can
safely be passed across threads / processes / background tasks.

The credential dicts (`sr_credentials`, `flink_credentials`,
`cluster_credentials`) keep their existing two-key shape (`api_key` /
`api_secret`) for backward compatibility with the UI's session state and the
local cache schema.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ── extraction ──────────────────────────────────────────────────────────


class ExtractionRequest(BaseModel):
    """What to extract and which knobs to flip while doing it.

    The same model is built by `request_builder.build_extraction_request()`
    from a UI session_state dict and parsed from the API's JSON body — that
    parity is enforced by `tests/services/test_request_parity.py`.
    """

    model_config = ConfigDict(frozen=True)

    environment_ids: list[str]
    cluster_ids: list[str] | None = None

    # Phase enable flags
    enable_connect: bool = True
    enable_ksqldb: bool = True
    enable_flink: bool = True
    enable_schema_registry: bool = True
    enable_stream_catalog: bool = True
    enable_tableflow: bool = True

    # Post-extraction enrichment
    enable_enrichment: bool = True
    enable_metrics: bool = False
    metrics_lookback_hours: int = 1

    # Per-env credential overrides (UI populates from session state; API
    # accepts the same shape via JSON body). Empty dict = use Settings.
    sr_endpoints: dict[str, str] = Field(default_factory=dict)
    sr_credentials: dict[str, dict[str, str]] = Field(default_factory=dict)
    flink_credentials: dict[str, dict[str, str]] = Field(default_factory=dict)
    cluster_credentials: dict[str, dict[str, str]] = Field(default_factory=dict)


# ── enrichment ──────────────────────────────────────────────────────────


class EnrichmentRequest(BaseModel):
    """Knobs for `run_enrichment` against an already-extracted graph."""

    model_config = ConfigDict(frozen=True)

    enable_catalog: bool = True
    enable_metrics: bool = False
    metrics_lookback_hours: int = 1


# ── push ────────────────────────────────────────────────────────────────


PushProviderName = Literal["databricks_uc", "aws_glue", "google", "datazone"]


class PushRequest(BaseModel):
    """Push lineage to a single catalog provider.

    `options` is a free-form dict whose accepted keys depend on `provider`.
    Phase 1B (catalog protocol v2) will replace this with a typed
    discriminated union of `PushOptions` subclasses; for now the dict shape
    matches the existing orchestrator `run_*_push` kwargs:

      - databricks_uc: set_properties, set_comments, create_bridge_table,
        bridge_table_name
      - aws_glue:      set_parameters, set_description
      - google:        (none)
      - datazone:      (none)
    """

    model_config = ConfigDict(frozen=True)

    provider: PushProviderName
    options: dict[str, Any] = Field(default_factory=dict)
