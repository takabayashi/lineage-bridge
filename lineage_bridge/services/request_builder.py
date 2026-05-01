# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pure-function builder for `ExtractionRequest`.

UI calls this from `_resolve_extraction_context` after collecting session
state; API calls it from the request body parser. Same input, same output —
that's the contract `tests/services/test_request_parity.py` locks in.

This module deliberately has no Streamlit, FastAPI, or boto3 imports. It only
depends on the request models so the parity test can compare instances
without spinning up either framework.
"""

from __future__ import annotations

from typing import Any

from lineage_bridge.services.requests import ExtractionRequest


def _coalesce(*values: Any, default: Any = None) -> Any:
    """Return the first non-None value, or *default*."""
    for v in values:
        if v is not None:
            return v
    return default


def build_extraction_request(params: dict[str, Any]) -> ExtractionRequest:
    """Translate a raw params dict (UI session state or API body) into an `ExtractionRequest`.

    Accepts both spellings of the environment-ID key (`environment_ids` —
    Pydantic-canonical, used by the API; `env_ids` — historical UI shape) so
    UI sidebar code can keep its existing keys without juggling.
    """
    return ExtractionRequest(
        environment_ids=_coalesce(
            params.get("environment_ids"),
            params.get("env_ids"),
            default=[],
        ),
        cluster_ids=params.get("cluster_ids"),
        enable_connect=params.get("enable_connect", True),
        enable_ksqldb=params.get("enable_ksqldb", True),
        enable_flink=params.get("enable_flink", True),
        enable_schema_registry=params.get("enable_schema_registry", True),
        enable_stream_catalog=params.get("enable_stream_catalog", True),
        enable_tableflow=params.get("enable_tableflow", True),
        enable_enrichment=params.get("enable_enrichment", True),
        enable_metrics=params.get("enable_metrics", False),
        metrics_lookback_hours=params.get("metrics_lookback_hours", 1),
        sr_endpoints=params.get("sr_endpoints") or {},
        sr_credentials=params.get("sr_credentials") or {},
        flink_credentials=params.get("flink_credentials") or {},
        cluster_credentials=params.get("cluster_credentials") or {},
    )
