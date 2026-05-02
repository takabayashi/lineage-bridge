# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Watcher service — pure-logic state machine for one watcher (Phase 2G).

`WatcherService` owns the in-memory state for a single watcher and exposes
async methods the runner calls each tick. Two intentional non-features:

  - **No threading.** The threading wrapper from the old `WatcherEngine`
    is gone; the runner owns its event loop and calls into the service
    directly. Clean separation: service is a pure object, runner is the
    long-running loop.
  - **No persistence.** The service knows nothing about the storage layer.
    The runner persists each tick's status snapshot to a `WatcherRepository`
    so any UI can read it without holding the runner in-process.

Mode selection (audit-log Kafka consumer vs REST polling) is explicit in
`WatcherConfig.mode` — no more `_use_audit_log` private property reading
deeply-nested settings attrs.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lineage_bridge.services.requests import PushRequest
from lineage_bridge.services.watcher_models import (
    ExtractionRecord,
    WatcherConfig,
    WatcherEvent,
    WatcherMode,
    WatcherState,
    WatcherStatus,
)

if TYPE_CHECKING:
    from lineage_bridge.config.settings import Settings

logger = logging.getLogger(__name__)


class WatcherService:
    """One watcher's state machine + extraction-trigger logic.

    Owned by the runner; never shared across runners. The runner calls:

        await service.start()
        while not stopped:
            events = await service.poll_once()
            await service.maybe_extract()
        await service.stop()

    Status snapshots come from `service.snapshot()` between calls.
    """

    def __init__(
        self,
        watcher_id: str,
        config: WatcherConfig,
        settings: Settings,
    ) -> None:
        self.watcher_id = watcher_id
        self.config = config
        self.settings = settings
        self._state = WatcherState.STOPPED
        self._started_at: datetime | None = None
        self._last_poll_at: datetime | None = None
        self._last_extraction_at: datetime | None = None
        self._poll_count = 0
        self._event_count = 0
        self._extraction_count = 0
        self._last_error: str | None = None
        self._cooldown_deadline: float = 0.0
        self._pending_events: list[WatcherEvent] = []

        # Lazy-init on first poll() so construction is cheap and tests
        # aren't forced into an async context just to instantiate the service.
        self._poller = None
        self._consumer = None

    @property
    def state(self) -> WatcherState:
        return self._state

    @property
    def cooldown_remaining(self) -> float:
        if self._state != WatcherState.COOLDOWN:
            return 0.0
        return max(0.0, self._cooldown_deadline - time.monotonic())

    def snapshot(self) -> WatcherStatus:
        """Return the current status as a Pydantic model the runner can persist."""
        return WatcherStatus(
            watcher_id=self.watcher_id,
            state=self._state,
            started_at=self._started_at,
            last_poll_at=self._last_poll_at,
            last_extraction_at=self._last_extraction_at,
            poll_count=self._poll_count,
            event_count=self._event_count,
            extraction_count=self._extraction_count,
            cooldown_remaining_seconds=self.cooldown_remaining,
            last_error=self._last_error,
        )

    async def start(self) -> None:
        """Initialise the underlying poller / consumer for the configured mode."""
        if not self.config.extraction.environment_ids:
            raise ValueError("extraction.environment_ids must be set")

        if self.config.mode == WatcherMode.AUDIT_LOG:
            self._consumer = await self._build_audit_consumer()
        else:
            self._poller = await self._build_rest_poller()

        self._state = WatcherState.WATCHING
        self._started_at = datetime.now(UTC)
        logger.info("Watcher %s started in %s mode", self.watcher_id, self.config.mode.value)

    async def stop(self) -> None:
        """Tear down the poller / consumer. Idempotent."""
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                logger.warning("Audit consumer close failed", exc_info=True)
            self._consumer = None
        # ChangePoller has no close — its httpx clients are short-lived.
        self._poller = None
        self._state = WatcherState.STOPPED
        logger.info("Watcher %s stopped", self.watcher_id)

    async def poll_once(self) -> list[WatcherEvent]:
        """Run one detection cycle. Returns any new events (also recorded internally).

        AUDIT_LOG: blocks for up to ~1s on the Kafka consumer.
        REST_POLLING: makes the configured REST calls in parallel.
        """
        events: list[WatcherEvent] = []
        try:
            if self.config.mode == WatcherMode.AUDIT_LOG and self._consumer is not None:
                event = self._consumer.poll_one(timeout=1.0)
                if event is not None:
                    events.append(event)
            elif self._poller is not None:
                events = await self._poller.poll()
        except Exception as exc:
            self._last_error = f"poll failed: {exc}"
            logger.warning("Poll failed for watcher %s", self.watcher_id, exc_info=True)
            return []

        self._poll_count += 1
        self._last_poll_at = datetime.now(UTC)

        if events:
            self._pending_events.extend(events)
            self._event_count += len(events)
            # Reset cooldown on every event batch — debouncing strategy: more
            # changes within the cooldown window push the deadline forward,
            # so the extraction batches the whole burst into one run.
            self._cooldown_deadline = time.monotonic() + self.config.cooldown_seconds
            self._state = WatcherState.COOLDOWN
            logger.info(
                "Watcher %s detected %d change(s) — cooldown %.0fs",
                self.watcher_id,
                len(events),
                self.config.cooldown_seconds,
            )

        return events

    async def maybe_extract(self) -> ExtractionRecord | None:
        """If the cooldown has elapsed, run extraction + push and return the record.

        Idempotent — returns None when there's nothing to do (no events buffered
        or still in cooldown). The runner calls this every tick; one of every
        N ticks (depending on cooldown vs poll-interval) will do real work.
        """
        if self._state != WatcherState.COOLDOWN:
            return None
        if time.monotonic() < self._cooldown_deadline:
            return None
        return await self._do_extraction()

    async def drain_events(self) -> AsyncIterator[WatcherEvent]:
        """Yield buffered events (for the runner to persist) and clear the buffer."""
        events = list(self._pending_events)
        self._pending_events.clear()
        for event in events:
            yield event

    # ── internals ───────────────────────────────────────────────────────

    async def _build_audit_consumer(self):
        from lineage_bridge.clients.audit_consumer import AuditLogConsumer

        bootstrap = (
            self.config.audit_log_bootstrap_servers
            or self.settings.audit_log_bootstrap_servers
        )
        api_key = self.config.audit_log_api_key or self.settings.audit_log_api_key
        api_secret = self.config.audit_log_api_secret or self.settings.audit_log_api_secret
        if not (bootstrap and api_key and api_secret):
            raise ValueError(
                "Audit-log mode needs bootstrap_servers + api_key + api_secret "
                "(in WatcherConfig or Settings)"
            )
        return AuditLogConsumer(
            bootstrap_servers=bootstrap,
            api_key=api_key,
            api_secret=api_secret,
        )

    async def _build_rest_poller(self):
        from lineage_bridge.clients.audit_consumer import ChangePoller, ClusterEndpoint
        from lineage_bridge.clients.base import ConfluentClient
        from lineage_bridge.clients.discovery import list_clusters

        env_ids = list(self.config.extraction.environment_ids)
        env_id = env_ids[0]

        # Discover Kafka cluster endpoints so the poller can hit the data plane.
        endpoints: list = []
        try:
            client = ConfluentClient(
                "https://api.confluent.cloud",
                self.settings.confluent_cloud_api_key,
                self.settings.confluent_cloud_api_secret,
            )
            async with client:
                clusters = await list_clusters(client, env_id)
            for cluster in clusters:
                if not cluster.rest_endpoint:
                    continue
                api_key, api_secret = self.settings.get_cluster_credentials(cluster.id)
                endpoints.append(
                    ClusterEndpoint(
                        cluster_id=cluster.id,
                        rest_endpoint=cluster.rest_endpoint,
                        api_key=api_key,
                        api_secret=api_secret,
                    )
                )
        except Exception:
            logger.warning("Cluster discovery failed for %s", env_id, exc_info=True)

        return ChangePoller(
            cloud_api_key=self.settings.confluent_cloud_api_key,
            cloud_api_secret=self.settings.confluent_cloud_api_secret,
            environment_id=env_id,
            cluster_endpoints=endpoints,
            ksqldb_api_key=self.settings.ksqldb_api_key,
            ksqldb_api_secret=self.settings.ksqldb_api_secret,
            flink_api_key=self.settings.flink_api_key,
            flink_api_secret=self.settings.flink_api_secret,
        )

    async def _do_extraction(self) -> ExtractionRecord:
        """Run the extraction pipeline + any configured pushes. Always returns a record."""
        from lineage_bridge.services.extraction_service import run_extraction
        from lineage_bridge.services.push_service import run_push

        self._state = WatcherState.EXTRACTING
        triggered_at = datetime.now(UTC)
        trigger_count = len(self._pending_events)
        self._pending_events.clear()
        record = ExtractionRecord(
            triggered_at=triggered_at,
            trigger_event_count=trigger_count,
        )

        try:
            graph = await run_extraction(self.config.extraction, self.settings)
            record.node_count = graph.node_count
            record.edge_count = graph.edge_count

            # Per-provider push — soft-fail (record but don't block extraction).
            push_providers: list[str] = []
            if self.config.push_databricks_uc:
                push_providers.append("databricks_uc")
            if self.config.push_aws_glue:
                push_providers.append("aws_glue")
            if self.config.push_google:
                push_providers.append("google")
            if self.config.push_datazone:
                push_providers.append("datazone")
            for provider in push_providers:
                try:
                    await run_push(PushRequest(provider=provider), self.settings, graph)
                except Exception as exc:
                    logger.warning(
                        "Watcher %s push to %s failed: %s",
                        self.watcher_id,
                        provider,
                        exc,
                    )
        except Exception as exc:
            record.error = str(exc)
            self._last_error = record.error
            logger.exception("Watcher %s extraction failed", self.watcher_id)

        record.completed_at = datetime.now(UTC)
        self._extraction_count += 1
        self._last_extraction_at = record.completed_at
        self._state = WatcherState.WATCHING
        return record
