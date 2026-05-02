# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Service discovery helpers for the UI."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx


def _try_load_settings():
    """Attempt to load credentials from .env / environment variables.

    Falls back to the local encrypted cache for fields the demos write there
    (Databricks workspace + token, GCP project + location, AWS region, and
    DataZone domain + project). This keeps the UI's "Push to UC" / "Push to
    Google" / "Push to DataZone" buttons available across multi-demo workflows
    even when ``.env`` has been overwritten by a different demo.
    """
    import os

    try:
        from lineage_bridge.config.settings import Settings

        settings = Settings()  # type: ignore[call-arg]
    except Exception:
        import logging

        logging.getLogger(__name__).warning(
            "Failed to load settings from .env / environment variables",
            exc_info=True,
        )
        return None

    try:
        from lineage_bridge.config.cache import load_cache

        cache = load_cache()
        update: dict[str, str] = {}

        # ── Databricks (UC demo) ─────────────────────────────────────────
        db = cache.get("databricks_settings") or {}
        if not settings.databricks_workspace_url and db.get("workspace_url"):
            update["databricks_workspace_url"] = db["workspace_url"]
        if not settings.databricks_token and db.get("token"):
            update["databricks_token"] = db["token"]
        if not settings.databricks_warehouse_id and db.get("warehouse_id"):
            update["databricks_warehouse_id"] = db["warehouse_id"]

        # ── AWS region (Glue demo). Settings.aws_region defaults to
        # us-east-1, so we can't rely on falsy-check; only override when the
        # user didn't set the env var explicitly. ──────────────────────────
        aws = cache.get("aws_settings") or {}
        if "LINEAGE_BRIDGE_AWS_REGION" not in os.environ and aws.get("region"):
            update["aws_region"] = aws["region"]

        # ── GCP (BigQuery demo) ──────────────────────────────────────────
        gcp = cache.get("gcp_settings") or {}
        if not settings.gcp_project_id and gcp.get("project_id"):
            update["gcp_project_id"] = gcp["project_id"]
        if gcp.get("location") and "LINEAGE_BRIDGE_GCP_LOCATION" not in os.environ:
            update.setdefault("gcp_location", gcp["location"])

        # ── AWS DataZone (Glue + DataZone demo) ──────────────────────────
        dz = cache.get("aws_datazone_settings") or {}
        if not settings.aws_datazone_domain_id and dz.get("domain_id"):
            update["aws_datazone_domain_id"] = dz["domain_id"]
        if not settings.aws_datazone_project_id and dz.get("project_id"):
            update["aws_datazone_project_id"] = dz["project_id"]

        if update:
            settings = settings.model_copy(update=update)
    except Exception:
        import logging

        logging.getLogger(__name__).debug("Cache fallback failed", exc_info=True)

    return settings


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
