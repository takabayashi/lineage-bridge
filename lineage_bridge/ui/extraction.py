# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Extraction orchestration helpers for the UI.

This module is intentionally thin — it translates Streamlit session state
into request models and forwards to `lineage_bridge.services`. The service
layer is what UI, API, and watcher all converge on (see ADR-020).

Phase A redesign: progress lines are routed to either `extraction_log`
(for extract / enrich / refresh) or `push_log` (for push actions) — a
single shared list previously caused the extraction log to be wiped on
every push.
"""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from lineage_bridge.config.cache import update_cache
from lineage_bridge.services import (
    EnrichmentRequest,
    PushRequest,
    build_extraction_request,
    run_enrichment,
    run_extraction,
    run_push,
)
from lineage_bridge.ui.discovery import _run_async


def _save_selections_to_cache(params: dict) -> None:
    """Persist extraction selections + credentials to local cache."""
    cache_data: dict = {
        "selected_envs": st.session_state.get("env_select", ""),
        "selected_clusters": st.session_state.get("cluster_select", []),
        "last_extraction_params": params,
    }
    creds = params.get("cluster_credentials", {})
    if creds:
        cache_data["cluster_credentials"] = creds
    sr_creds = params.get("sr_credentials", {})
    if sr_creds:
        cache_data["sr_credentials"] = sr_creds
    flink_creds = params.get("flink_credentials", {})
    if flink_creds:
        cache_data["flink_credentials"] = flink_creds
    update_cache(**cache_data)


def _build_sr_endpoints(params: dict) -> dict[str, str]:
    """Build a map of env_id -> SR endpoint from UI inputs + discovery cache."""
    sr_endpoints: dict[str, str] = {}
    env_cache = st.session_state.get("env_cache", {})
    for env_id, cached in env_cache.items():
        svc = cached.get("services")
        if svc and svc.schema_registry_endpoint:
            sr_endpoints[env_id] = svc.schema_registry_endpoint
    sr_creds = params.get("sr_credentials", {})
    for env_id, cred in sr_creds.items():
        if cred.get("endpoint"):
            sr_endpoints[env_id] = cred["endpoint"]
    return sr_endpoints


def _ui_progress(state_key: str = "extraction_log"):
    """Append-to-log progress callback. *state_key* is the session-state list to append to.

    Each entry is timestamped (`[HH:MM:SS]`) so users can see how long phases
    took without inferring from line ordering. The `_classify_log_entry`
    helper in `actions.py` strips the prefix before rendering.
    """
    log = st.session_state.get(state_key)
    if log is None:
        log = []
        st.session_state[state_key] = log

    def on_progress(phase: str, detail: str = "") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        log.append(f"[{ts}] **{phase}** {detail}")

    return on_progress


def _run_enrichment_on_graph(settings, graph, params: dict):
    """Run enrichment on an existing graph. Returns the enriched graph."""
    req = EnrichmentRequest(
        enable_catalog=True,
        enable_metrics=params.get("enable_metrics", False),
        metrics_lookback_hours=params.get("metrics_lookback_hours", 1),
    )
    return _run_async(run_enrichment(req, settings, graph, on_progress=_ui_progress()))


def _push_request(provider: str, params: dict, options: dict | None = None) -> PushRequest:
    """Build a `PushRequest` for *provider* using *options* (or {} for option-less providers)."""
    return PushRequest(provider=provider, options=options or {})


def _run_lineage_push(settings, graph, params: dict):
    """Push lineage metadata to Databricks UC tables. Returns PushResult."""
    req = _push_request(
        "databricks_uc",
        params,
        {
            "set_properties": params.get("push_properties", True),
            "set_comments": params.get("push_comments", True),
            "create_bridge_table": params.get("push_bridge_table", False),
            # Native External Lineage API path. Default falls back to the
            # legacy TBLPROPERTIES + bridge writer when the flag is unset.
            "use_native_lineage": params.get(
                "push_native_lineage", settings.databricks_use_native_lineage
            ),
        },
    )
    return _run_async(run_push(req, settings, graph, on_progress=_ui_progress("push_log")))


def _run_glue_push(settings, graph, params: dict):
    """Push lineage metadata to AWS Glue tables. Returns PushResult."""
    req = _push_request(
        "aws_glue",
        params,
        {
            "set_parameters": params.get("push_parameters", True),
            "set_description": params.get("push_description", True),
        },
    )
    return _run_async(run_push(req, settings, graph, on_progress=_ui_progress("push_log")))


def _run_google_push(settings, graph, params: dict):
    """Push lineage as OpenLineage events to Google Data Lineage. Returns PushResult."""
    req = _push_request("google", params)
    return _run_async(run_push(req, settings, graph, on_progress=_ui_progress("push_log")))


def _run_datazone_push(settings, graph, params: dict):
    """Register Kafka assets + push OpenLineage events to AWS DataZone. Returns PushResult."""
    req = _push_request("datazone", params)
    return _run_async(run_push(req, settings, graph, on_progress=_ui_progress("push_log")))


def _run_extraction_with_params(settings, params: dict):
    """Run extraction with a params dict. Returns the graph or raises.

    Builds an `ExtractionRequest` via the shared `build_extraction_request`
    helper so the API and UI hit the request constructor through the same
    code path (parity is locked in by `tests/services/test_request_parity.py`).
    """
    req = build_extraction_request({**params, "sr_endpoints": _build_sr_endpoints(params)})
    return _run_async(run_extraction(req, settings, on_progress=_ui_progress()))
