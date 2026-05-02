# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for `request_builder.build_extraction_request`."""

from __future__ import annotations

from lineage_bridge.services import build_extraction_request
from lineage_bridge.services.requests import ExtractionRequest


def test_minimal_params_uses_defaults():
    req = build_extraction_request({"environment_ids": ["env-1"]})
    assert req.environment_ids == ["env-1"]
    assert req.cluster_ids is None
    assert req.enable_connect is True
    assert req.enable_metrics is False
    assert req.metrics_lookback_hours == 1


def test_legacy_env_ids_key_is_accepted():
    """UI session_state historically used `env_ids`; the API uses `environment_ids`."""
    legacy = build_extraction_request({"env_ids": ["env-1"]})
    canonical = build_extraction_request({"environment_ids": ["env-1"]})
    assert legacy == canonical


def test_environment_ids_takes_precedence_over_env_ids():
    """When both are present the canonical key wins (defensive)."""
    req = build_extraction_request(
        {"environment_ids": ["env-canonical"], "env_ids": ["env-legacy"]}
    )
    assert req.environment_ids == ["env-canonical"]


def test_all_flags_propagate():
    req = build_extraction_request(
        {
            "environment_ids": ["env-1"],
            "enable_connect": False,
            "enable_ksqldb": False,
            "enable_flink": False,
            "enable_schema_registry": False,
            "enable_stream_catalog": False,
            "enable_tableflow": False,
            "enable_enrichment": False,
            "enable_metrics": True,
            "metrics_lookback_hours": 24,
        }
    )
    assert req.enable_connect is False
    assert req.enable_ksqldb is False
    assert req.enable_flink is False
    assert req.enable_schema_registry is False
    assert req.enable_stream_catalog is False
    assert req.enable_tableflow is False
    assert req.enable_enrichment is False
    assert req.enable_metrics is True
    assert req.metrics_lookback_hours == 24


def test_credential_dicts_default_to_empty():
    req = build_extraction_request({"environment_ids": ["env-1"]})
    assert req.sr_endpoints == {}
    assert req.sr_credentials == {}
    assert req.flink_credentials == {}
    assert req.cluster_credentials == {}


def test_credential_dicts_propagate():
    req = build_extraction_request(
        {
            "environment_ids": ["env-1"],
            "sr_endpoints": {"env-1": "https://psrc-test.confluent.cloud"},
            "sr_credentials": {"env-1": {"api_key": "k", "api_secret": "s"}},
            "flink_credentials": {"env-1": {"api_key": "fk", "api_secret": "fs"}},
            "cluster_credentials": {"lkc-a": {"api_key": "ck", "api_secret": "cs"}},
        }
    )
    assert req.sr_endpoints == {"env-1": "https://psrc-test.confluent.cloud"}
    assert req.sr_credentials == {"env-1": {"api_key": "k", "api_secret": "s"}}
    assert req.flink_credentials == {"env-1": {"api_key": "fk", "api_secret": "fs"}}
    assert req.cluster_credentials == {"lkc-a": {"api_key": "ck", "api_secret": "cs"}}


def test_none_credentials_become_empty_dicts():
    """build_extraction_request normalizes explicit None credentials to {}."""
    req = build_extraction_request(
        {
            "environment_ids": ["env-1"],
            "sr_credentials": None,
            "flink_credentials": None,
            "cluster_credentials": None,
        }
    )
    assert req.sr_credentials == {}
    assert req.flink_credentials == {}
    assert req.cluster_credentials == {}


def test_cluster_ids_can_be_explicit_none():
    req = build_extraction_request({"environment_ids": ["env-1"], "cluster_ids": None})
    assert req.cluster_ids is None


def test_returns_frozen_extraction_request():
    """Pydantic frozen=True means the result can't be mutated after construction."""
    req = build_extraction_request({"environment_ids": ["env-1"]})
    assert isinstance(req, ExtractionRequest)
    try:
        req.enable_connect = False
        raised = False
    except Exception:
        raised = True
    assert raised, "ExtractionRequest should be frozen"
