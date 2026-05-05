# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Watcher engine — background poller with debounced extraction."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from lineage_bridge.config.settings import Settings
from lineage_bridge.models.audit_event import AuditEvent

logger = logging.getLogger(__name__)


class WatcherState(StrEnum):
    STOPPED = "stopped"
    WATCHING = "watching"
    COOLDOWN = "cooldown"
    EXTRACTING = "extracting"


@dataclass
class ExtractionRecord:
    """Record of a watcher-triggered extraction run."""

    triggered_at: datetime
    completed_at: datetime | None = None
    trigger_events: list[AuditEvent] = field(default_factory=list)
    node_count: int = 0
    edge_count: int = 0
    error: str | None = None


class WatcherEngine:
    """Background REST API poller with debounced extraction triggering.

    Polls Confluent Cloud REST APIs to detect lineage-relevant changes
    (topic creation/deletion, connector changes, etc.) and triggers
    extraction after a cooldown period of inactivity.
    """

    def __init__(
        self,
        settings: Settings,
        extraction_params: dict[str, Any],
        *,
        cooldown_seconds: float = 30.0,
        poll_interval: float = 10.0,
    ) -> None:
        self.settings = settings
        self.extraction_params = extraction_params
        self.cooldown_seconds = cooldown_seconds
        self.poll_interval = poll_interval

        self.state: WatcherState = WatcherState.STOPPED
        self.event_feed: deque[AuditEvent] = deque(maxlen=200)
        self.extraction_history: list[ExtractionRecord] = []
        self.poll_count: int = 0
        self.last_poll_time: datetime | None = None
        self.last_graph: Any = None

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._cooldown_deadline: float = 0.0
        self._pending_events: list[AuditEvent] = []

    @property
    def cooldown_remaining(self) -> float:
        """Seconds remaining in the current cooldown, or 0."""
        if self.state != WatcherState.COOLDOWN:
            return 0.0
        return max(0.0, self._cooldown_deadline - time.monotonic())

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Start the watcher in a background thread."""
        if self.is_running:
            logger.warning("Watcher is already running")
            return

        env_ids = self.extraction_params.get("env_ids", [])
        if not env_ids:
            raise ValueError("env_ids must be set in extraction_params")

        self._stop_event.clear()
        self.state = WatcherState.WATCHING
        self._thread = threading.Thread(
            target=self._run_loop,
            name="lineage-bridge-watcher",
            daemon=True,
        )
        self._thread.start()
        logger.info("Watcher started (polling every %.0fs)", self.poll_interval)

    def stop(self) -> None:
        """Signal the watcher to stop and wait for the thread to join."""
        if not self.is_running:
            return

        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)
            self._thread = None
        self.state = WatcherState.STOPPED
        logger.info("Watcher stopped")

    @property
    def _use_audit_log(self) -> bool:
        """Whether audit log Kafka consumer mode is configured."""
        s = self.settings
        return bool(
            getattr(s, "audit_log_bootstrap_servers", None)
            and getattr(s, "audit_log_api_key", None)
            and getattr(s, "audit_log_api_secret", None)
        )

    def _run_loop(self) -> None:
        """Main loop: consume events → debounce → extract.

        Uses the audit log Kafka consumer when credentials are configured,
        otherwise falls back to REST API state-diffing.
        """
        if self._use_audit_log:
            self._run_audit_log_loop()
        else:
            self._run_polling_loop()

    def _run_audit_log_loop(self) -> None:
        """Consume from the Confluent Cloud audit log Kafka topic."""
        from lineage_bridge.clients.audit_consumer import AuditLogConsumer

        consumer = AuditLogConsumer(
            bootstrap_servers=self.settings.audit_log_bootstrap_servers,
            api_key=self.settings.audit_log_api_key,
            api_secret=self.settings.audit_log_api_secret,
        )
        logger.info("Watcher using audit log Kafka consumer")

        try:
            while not self._stop_event.is_set():
                event = consumer.poll_one(timeout=1.0)
                self.poll_count += 1
                self.last_poll_time = datetime.now(UTC)

                if event is not None:
                    self.event_feed.append(event)
                    self._pending_events.append(event)
                    self._cooldown_deadline = time.monotonic() + self.cooldown_seconds
                    self.state = WatcherState.COOLDOWN
                    logger.info(
                        "Audit event: %s on %s (%.0fs cooldown)",
                        event.method_name,
                        event.resource_name,
                        self.cooldown_seconds,
                    )

                # Check if cooldown has expired
                if (
                    self.state == WatcherState.COOLDOWN
                    and time.monotonic() >= self._cooldown_deadline
                ):
                    self._trigger_extraction()

        except Exception:
            logger.exception("Audit log watcher loop error")
            self.state = WatcherState.STOPPED
        finally:
            consumer.close()

    def _run_polling_loop(self) -> None:
        """Fall back to REST API state-diffing."""
        from lineage_bridge.clients.audit_consumer import ChangePoller

        params = self.extraction_params
        env_ids = params.get("env_ids", [])

        # Discover cluster endpoints (REST URL + credentials)
        cluster_endpoints = []
        if env_ids:
            try:
                cluster_endpoints = asyncio.run(self._discover_cluster_endpoints(env_ids[0]))
                logger.info(
                    "Discovered %d cluster(s): %s",
                    len(cluster_endpoints),
                    [ep.cluster_id for ep in cluster_endpoints],
                )
            except Exception:
                logger.warning("Cluster discovery failed", exc_info=True)

        poller = ChangePoller(
            cloud_api_key=self.settings.confluent_cloud_api_key,
            cloud_api_secret=self.settings.confluent_cloud_api_secret,
            environment_id=env_ids[0] if env_ids else "",
            cluster_endpoints=cluster_endpoints,
            ksqldb_api_key=self.settings.ksqldb_api_key,
            ksqldb_api_secret=self.settings.ksqldb_api_secret,
            flink_api_key=self.settings.flink_api_key,
            flink_api_secret=self.settings.flink_api_secret,
        )
        logger.info("Watcher using REST API polling (no audit log configured)")

        try:
            while not self._stop_event.is_set():
                # Poll REST APIs for changes
                try:
                    events = asyncio.run(poller.poll())
                    self.poll_count += 1
                    self.last_poll_time = datetime.now(UTC)
                except Exception:
                    logger.warning("Poll failed", exc_info=True)
                    events = []

                for event in events:
                    self.event_feed.append(event)
                    self._pending_events.append(event)
                    self._cooldown_deadline = time.monotonic() + self.cooldown_seconds
                    self.state = WatcherState.COOLDOWN
                    logger.info(
                        "Change detected: %s (%.0fs cooldown)",
                        event.resource_name,
                        self.cooldown_seconds,
                    )

                # Check if cooldown has expired
                if (
                    self.state == WatcherState.COOLDOWN
                    and time.monotonic() >= self._cooldown_deadline
                ):
                    self._trigger_extraction()

                # Wait for next poll interval (interruptible)
                self._stop_event.wait(timeout=self.poll_interval)

        except Exception:
            logger.exception("Watcher loop error")
            self.state = WatcherState.STOPPED

    def _trigger_extraction(self) -> None:
        """Run extraction pipeline in response to accumulated events.

        Always runs extraction on detected changes. Push to UC/Glue
        only happens if the corresponding checkbox was enabled.
        """
        self.state = WatcherState.EXTRACTING
        trigger_events = list(self._pending_events)
        self._pending_events.clear()

        record = ExtractionRecord(
            triggered_at=datetime.now(UTC),
            trigger_events=trigger_events,
        )

        logger.info(
            "Triggering extraction for %d events",
            len(trigger_events),
        )

        try:
            graph = asyncio.run(self._do_extraction())
            self.last_graph = graph
            record.node_count = graph.node_count
            record.edge_count = graph.edge_count
            record.completed_at = datetime.now(UTC)
            logger.info(
                "Extraction complete: %d nodes, %d edges",
                graph.node_count,
                graph.edge_count,
            )
        except Exception as exc:
            record.error = str(exc)
            record.completed_at = datetime.now(UTC)
            logger.exception("Watcher-triggered extraction failed")

        self.extraction_history.append(record)
        self.state = WatcherState.WATCHING

    async def _discover_cluster_endpoints(self, environment_id: str) -> list:
        """Discover Kafka cluster endpoints for an environment."""
        from lineage_bridge.clients.audit_consumer import ClusterEndpoint
        from lineage_bridge.clients.base import ConfluentClient
        from lineage_bridge.clients.discovery import list_clusters

        client = ConfluentClient(
            "https://api.confluent.cloud",
            self.settings.confluent_cloud_api_key,
            self.settings.confluent_cloud_api_secret,
        )
        async with client:
            clusters = await list_clusters(client, environment_id)

        endpoints = []
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
        return endpoints

    async def _do_extraction(self):
        """Run the extraction pipeline."""
        from lineage_bridge.extractors.orchestrator import run_extraction
        from lineage_bridge.services.push_service import run_push
        from lineage_bridge.services.requests import PushRequest

        params = self.extraction_params
        graph = await run_extraction(
            self.settings,
            environment_ids=params["env_ids"],
            cluster_ids=params.get("cluster_ids"),
            enable_connect=params.get("enable_connect", True),
            enable_ksqldb=params.get("enable_ksqldb", True),
            enable_flink=params.get("enable_flink", True),
            enable_schema_registry=params.get("enable_schema_registry", True),
            enable_stream_catalog=params.get("enable_stream_catalog", False),
            enable_tableflow=params.get("enable_tableflow", True),
            enable_enrichment=params.get("enable_enrichment", True),
        )

        # Push to each configured provider, isolating failures so one bad
        # destination doesn't lose results from the others.
        for provider_name in params.get("push_providers", []):
            try:
                await run_push(PushRequest(provider=provider_name), self.settings, graph)
            except Exception:
                logger.warning(
                    "%s push failed during watcher extraction", provider_name, exc_info=True
                )

        return graph
