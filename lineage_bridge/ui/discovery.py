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
    (``gcp_project_id`` / ``gcp_location`` and the AWS DataZone domain /
    project IDs). This means the UI's "Push to Google" / "Push to DataZone"
    buttons stay available across multi-demo workflows even when ``.env``
    has been overwritten by a different demo.
    """
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

        gcp = cache.get("gcp_settings") or {}
        if not settings.gcp_project_id and gcp.get("project_id"):
            update["gcp_project_id"] = gcp["project_id"]
        if gcp.get("location"):
            update.setdefault("gcp_location", gcp["location"])

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
