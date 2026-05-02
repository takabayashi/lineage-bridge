# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Parity test — UI's request_builder and the API's body parser produce identical models.

This is the acceptance criterion #4 from docs/plan-refactor.md (Service layer
parity). When this test passes, the API can no longer drift behind the UI by
construction: every field the UI populates is the same field the API parses.
"""

from __future__ import annotations

from lineage_bridge.services import ExtractionRequest, build_extraction_request


def _ui_session_state() -> dict:
    """The exact shape `ui/sidebar.py` puts together when the user clicks Extract."""
    return {
        "env_ids": ["env-test1", "env-test2"],
        "cluster_ids": ["lkc-a", "lkc-b"],
        "cluster_credentials": {"lkc-a": {"api_key": "ck", "api_secret": "cs"}},
        "sr_credentials": {"env-test1": {"api_key": "sk", "api_secret": "ss"}},
        "flink_credentials": {"env-test1": {"api_key": "fk", "api_secret": "fs"}},
        "sr_endpoints": {"env-test1": "https://psrc-test.confluent.cloud"},
        "enable_connect": True,
        "enable_ksqldb": False,
        "enable_flink": True,
        "enable_schema_registry": True,
        "enable_stream_catalog": False,
        "enable_tableflow": True,
        "enable_metrics": True,
        "metrics_lookback_hours": 6,
        "enable_enrichment": True,
    }


def _api_request_body() -> dict:
    """The same scenario phrased as an `ExtractionRequest` JSON body the API would receive."""
    return {
        "environment_ids": ["env-test1", "env-test2"],
        "cluster_ids": ["lkc-a", "lkc-b"],
        "cluster_credentials": {"lkc-a": {"api_key": "ck", "api_secret": "cs"}},
        "sr_credentials": {"env-test1": {"api_key": "sk", "api_secret": "ss"}},
        "flink_credentials": {"env-test1": {"api_key": "fk", "api_secret": "fs"}},
        "sr_endpoints": {"env-test1": "https://psrc-test.confluent.cloud"},
        "enable_connect": True,
        "enable_ksqldb": False,
        "enable_flink": True,
        "enable_schema_registry": True,
        "enable_stream_catalog": False,
        "enable_tableflow": True,
        "enable_metrics": True,
        "metrics_lookback_hours": 6,
        "enable_enrichment": True,
    }


def test_ui_and_api_produce_identical_extraction_requests():
    """The UI's build_extraction_request and the API's ExtractionRequest(**body) parser agree."""
    ui_built = build_extraction_request(_ui_session_state())
    api_parsed = ExtractionRequest(**_api_request_body())

    assert ui_built == api_parsed
    # Defensive — equality compares by value; also assert serialised form matches.
    assert ui_built.model_dump() == api_parsed.model_dump()


def test_parity_holds_for_minimal_request():
    """Defaults align between UI's empty session and the API's `{environment_ids: [...]}` body."""
    ui_built = build_extraction_request({"env_ids": ["env-1"]})
    api_parsed = ExtractionRequest(environment_ids=["env-1"])
    assert ui_built == api_parsed
