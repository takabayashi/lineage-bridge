# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Service discovery helpers for the UI."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx


def _try_load_settings():
    """Attempt to load credentials from .env / environment variables."""
    try:
        from lineage_bridge.config.settings import Settings

        return Settings()  # type: ignore[call-arg]
    except Exception:
        import logging

        logging.getLogger(__name__).warning(
            "Failed to load settings from .env / environment variables",
            exc_info=True,
        )
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
