# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Service discovery and key provisioning helpers for the UI."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

from lineage_bridge.config.provisioner import KeyProvisioner


def _try_load_settings():
    """Attempt to load credentials from .env / environment variables."""
    try:
        from lineage_bridge.config.settings import Settings

        return Settings()  # type: ignore[call-arg]
    except Exception:
        return None


def _run_async(coro):
    """Run an async coroutine from sync Streamlit context."""
    ctx = get_script_run_ctx()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if ctx is not None:
        add_script_run_ctx(threading.current_thread(), ctx)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())


def _make_cloud_client(settings):
    """Create a ConfluentClient for the Cloud API."""
    from lineage_bridge.clients.base import ConfluentClient

    return ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )


def _discover_one(settings, env_id):
    """Discover services for a single environment."""
    from lineage_bridge.clients.discovery import discover_services

    async def _do():
        async with _make_cloud_client(settings) as cloud:
            return await discover_services(cloud, env_id)

    services = _run_async(_do())
    return {
        "services": services,
        "fetched_at": datetime.now(UTC).strftime("%H:%M:%S UTC"),
    }


def _services_summary(services) -> str:
    """One-line summary of discovered services."""
    return (
        f"{len(services.clusters)} cluster(s), "
        f"SR={'Yes' if services.has_schema_registry else 'No'}, "
        f"ksqlDB={services.ksqldb_cluster_count}, "
        f"Flink={services.flink_pool_count}"
    )


async def _auto_provision_keys(
    settings,
    params: dict,
    sr_endpoints: dict[str, str],
    on_progress,
) -> dict:
    """Provision missing API keys before extraction.

    Returns an updated copy of params with provisioned credentials merged in.
    """
    from lineage_bridge.clients.base import ConfluentClient

    prefix = st.session_state.get("provision_prefix", "lineage-bridge")
    use_sa = st.session_state.get("provision_sa", False)

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    try:
        provisioner = KeyProvisioner(cloud, prefix=prefix)

        # Optionally create / find a dedicated service account
        owner_id: str | None = None
        if use_sa:
            on_progress("Provisioning", "Ensuring service account...")
            owner_id = await provisioner.ensure_service_account()
            on_progress("Provisioning", f"Service account: {owner_id}")

        updated_params = dict(params)

        # ── Provision per-cluster Kafka keys ──────────────────────
        cluster_creds = dict(params.get("cluster_credentials", {}))
        env_cache = st.session_state.get("env_cache", {})

        for env_id in params["env_ids"]:
            cached_env = env_cache.get(env_id, {})
            svc = cached_env.get("services")
            if not svc:
                continue

            for cluster in svc.clusters:
                cid = cluster.id
                # Skip if already has credentials
                if cid in cluster_creds:
                    continue
                # Skip if settings already has per-cluster or global kafka key
                existing_key, _ = settings.get_cluster_credentials(cid)
                if existing_key != settings.confluent_cloud_api_key:
                    continue

                on_progress("Provisioning", f"Kafka key for {cid}...")
                key = await provisioner.provision_cluster_key(
                    cluster_id=cid,
                    environment_id=env_id,
                    owner_id=owner_id,
                )
                cluster_creds[cid] = {
                    "api_key": key.api_key,
                    "api_secret": key.api_secret,
                }

        if cluster_creds:
            updated_params["cluster_credentials"] = cluster_creds

        # ── Provision per-env SR keys ─────────────────────────────
        sr_creds = dict(params.get("sr_credentials", {}))
        if params.get("enable_schema_registry") or params.get("enable_stream_catalog"):
            for env_id in params["env_ids"]:
                # Skip if already has per-env SR credentials
                if env_id in sr_creds and sr_creds[env_id].get("api_key"):
                    continue
                # Skip if global SR key is set
                if settings.schema_registry_api_key:
                    continue

                # Discover SR cluster ID via API
                sr_cluster_id = None
                try:
                    sr_items = await cloud.paginate(
                        "/srcm/v2/clusters",
                        params={"environment": env_id},
                    )
                    if sr_items:
                        sr_cluster_id = sr_items[0].get("id")
                except Exception:
                    pass

                if sr_cluster_id:
                    on_progress("Provisioning", f"SR key for {env_id}...")
                    key = await provisioner.provision_sr_key(
                        sr_cluster_id=sr_cluster_id,
                        environment_id=env_id,
                        owner_id=owner_id,
                    )
                    existing = sr_creds.get(env_id, {})
                    existing["api_key"] = key.api_key
                    existing["api_secret"] = key.api_secret
                    sr_creds[env_id] = existing

        if sr_creds:
            updated_params["sr_credentials"] = sr_creds

        # ── Provision per-env Flink keys ──────────────────────────
        flink_creds = dict(params.get("flink_credentials", {}))
        if params.get("enable_flink"):
            for env_id in params["env_ids"]:
                if env_id in flink_creds and flink_creds[env_id].get("api_key"):
                    continue
                if settings.flink_api_key:
                    continue

                # Discover Flink compute pool ID via API
                try:
                    flink_items = await cloud.paginate(
                        "/fcpm/v2/compute-pools",
                        params={"environment": env_id},
                    )
                    if flink_items:
                        pool_id = flink_items[0].get("id", "")
                        if pool_id:
                            on_progress("Provisioning", f"Flink key for {env_id}...")
                            key = await provisioner.provision_flink_key(
                                flink_pool_id=pool_id,
                                environment_id=env_id,
                                owner_id=owner_id,
                            )
                            flink_creds[env_id] = {
                                "api_key": key.api_key,
                                "api_secret": key.api_secret,
                            }
                except Exception:
                    pass

        if flink_creds:
            updated_params["flink_credentials"] = flink_creds

        return updated_params
    finally:
        await cloud.close()
