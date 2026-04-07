# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Extraction orchestration helpers for the UI."""

from __future__ import annotations

import streamlit as st

from lineage_bridge.config.cache import update_cache
from lineage_bridge.config.settings import ClusterCredential
from lineage_bridge.ui.discovery import _auto_provision_keys, _run_async


def _save_selections_to_cache(params: dict) -> None:
    """Persist extraction selections + credentials to local cache."""
    cache_data: dict = {
        "selected_envs": st.session_state.get("env_multi_select", []),
        "selected_clusters": st.session_state.get("cluster_select", []),
        "last_extraction_params": params,
    }
    # Save per-cluster credentials (only non-empty ones)
    creds = params.get("cluster_credentials", {})
    if creds:
        cache_data["cluster_credentials"] = creds
    # Save per-environment SR credentials
    sr_creds = params.get("sr_credentials", {})
    if sr_creds:
        cache_data["sr_credentials"] = sr_creds
    # Save per-environment Flink credentials
    flink_creds = params.get("flink_credentials", {})
    if flink_creds:
        cache_data["flink_credentials"] = flink_creds
    update_cache(**cache_data)


def _build_sr_endpoints(params: dict) -> dict[str, str]:
    """Build a map of env_id -> SR endpoint from UI inputs + discovery cache."""
    sr_endpoints: dict[str, str] = {}
    # First, populate from discovery cache
    env_cache = st.session_state.get("env_cache", {})
    for env_id, cached in env_cache.items():
        svc = cached.get("services")
        if svc and svc.schema_registry_endpoint:
            sr_endpoints[env_id] = svc.schema_registry_endpoint
    # Override with manually-entered endpoints (take priority)
    sr_creds = params.get("sr_credentials", {})
    for env_id, cred in sr_creds.items():
        if cred.get("endpoint"):
            sr_endpoints[env_id] = cred["endpoint"]
    return sr_endpoints


def _run_enrichment_on_graph(settings, graph, params: dict):
    """Run enrichment on an existing graph. Returns the enriched graph."""
    from lineage_bridge.extractors.orchestrator import run_enrichment

    log = st.session_state.extraction_log

    def on_progress(phase: str, detail: str = "") -> None:
        log.append(f"**{phase}** {detail}")

    async def _do_enrich():
        return await run_enrichment(
            settings,
            graph,
            enable_catalog=True,
            enable_metrics=params.get("enable_metrics", False),
            metrics_lookback_hours=params.get("metrics_lookback_hours", 1),
            on_progress=on_progress,
        )

    return _run_async(_do_enrich())


def _run_lineage_push(settings, graph, params: dict):
    """Push lineage metadata to Databricks UC tables. Returns PushResult."""
    from lineage_bridge.extractors.orchestrator import run_lineage_push

    log = st.session_state.extraction_log

    def on_progress(phase: str, detail: str = "") -> None:
        log.append(f"**{phase}** {detail}")

    async def _do_push():
        return await run_lineage_push(
            settings,
            graph,
            set_properties=params.get("push_properties", True),
            set_comments=params.get("push_comments", True),
            create_bridge_table=params.get("push_bridge_table", False),
            on_progress=on_progress,
        )

    return _run_async(_do_push())


def _run_glue_push(settings, graph, params: dict):
    """Push lineage metadata to AWS Glue tables. Returns PushResult."""
    from lineage_bridge.extractors.orchestrator import run_glue_push

    log = st.session_state.extraction_log

    def on_progress(phase: str, detail: str = "") -> None:
        log.append(f"**{phase}** {detail}")

    async def _do_push():
        return await run_glue_push(
            settings,
            graph,
            set_parameters=params.get("push_parameters", True),
            set_description=params.get("push_description", True),
            on_progress=on_progress,
        )

    return _run_async(_do_push())


def _run_extraction_with_params(settings, params: dict):
    """Run extraction with a params dict. Returns the graph or raises."""
    from lineage_bridge.extractors.orchestrator import run_extraction

    # Merge UI-provided per-cluster credentials into settings
    ui_creds = params.get("cluster_credentials", {})
    if ui_creds:
        merged = dict(settings.cluster_credentials)
        for cid, cred_dict in ui_creds.items():
            merged[cid] = ClusterCredential(**cred_dict)
        settings = settings.model_copy(update={"cluster_credentials": merged})

    # Pass SR endpoints from discovery cache
    sr_endpoints = _build_sr_endpoints(params)

    log = st.session_state.extraction_log

    def on_progress(phase: str, detail: str = "") -> None:
        log.append(f"**{phase}** {detail}")

    async def _do_extract():
        nonlocal params, settings

        # Auto-provision missing keys if enabled
        if st.session_state.get("auto_provision", False):
            on_progress("Provisioning", "Checking for missing API keys...")
            params = await _auto_provision_keys(settings, params, sr_endpoints, on_progress)
            # Re-merge cluster credentials after provisioning
            prov_creds = params.get("cluster_credentials", {})
            if prov_creds:
                merged = dict(settings.cluster_credentials)
                for cid, cred_dict in prov_creds.items():
                    merged[cid] = ClusterCredential(**cred_dict)
                settings = settings.model_copy(update={"cluster_credentials": merged})
            on_progress("Provisioning", "Key provisioning complete")

        return await run_extraction(
            settings,
            environment_ids=params["env_ids"],
            cluster_ids=params["cluster_ids"],
            enable_connect=params["enable_connect"],
            enable_ksqldb=params["enable_ksqldb"],
            enable_flink=params["enable_flink"],
            enable_schema_registry=params["enable_schema_registry"],
            enable_stream_catalog=params["enable_stream_catalog"],
            enable_tableflow=params["enable_tableflow"],
            enable_enrichment=params.get("enable_enrichment", True),
            enable_metrics=params.get("enable_metrics", False),
            metrics_lookback_hours=params.get("metrics_lookback_hours", 1),
            sr_endpoints=_build_sr_endpoints(params),
            sr_credentials=params.get("sr_credentials"),
            flink_credentials=params.get("flink_credentials"),
            on_progress=on_progress,
        )

    return _run_async(_do_extract())
