# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Change-detection consumers for Confluent Cloud resources.

Two modes:
- AuditLogConsumer: Kafka consumer on the ``confluent-audit-log-events`` topic
  (real-time, event-level granularity, requires audit log cluster access).
- ChangePoller: REST API state-diffing via snapshot hashing (fallback when
  audit log cluster is not reachable).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.audit_event import AuditEvent, is_lineage_relevant

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  Audit Log Kafka Consumer
# ═══════════════════════════════════════════════════════════════════════════


class AuditLogConsumer:
    """Kafka consumer for the Confluent Cloud audit log topic.

    Wraps ``confluent_kafka.Consumer`` with SASL_SSL/PLAIN auth, filters
    messages to lineage-relevant events, and returns parsed ``AuditEvent``
    objects.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        api_key: str,
        api_secret: str,
        *,
        group_id: str = "lineage-bridge-watcher",
        topic: str = "confluent-audit-log-events",
    ) -> None:
        self.topic = topic
        self._consumer = self._create_consumer(bootstrap_servers, api_key, api_secret, group_id)
        self._consumer.subscribe([self.topic])
        logger.info("Audit log consumer subscribed to %s (group=%s)", topic, group_id)

    @staticmethod
    def _create_consumer(
        bootstrap_servers: str,
        api_key: str,
        api_secret: str,
        group_id: str,
    ):
        """Create a confluent_kafka.Consumer with SASL_SSL config."""
        from confluent_kafka import Consumer

        return Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": api_key,
                "sasl.password": api_secret,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

    def poll_one(self, timeout: float = 1.0) -> AuditEvent | None:
        """Poll for a single lineage-relevant audit event.

        Returns an ``AuditEvent`` if a relevant event was consumed, or
        ``None`` on timeout, non-relevant events, or parse errors.
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            logger.debug("Consumer error: %s", msg.error())
            return None

        try:
            payload = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError):
            logger.debug("Failed to decode audit log message")
            return None

        event = AuditEvent.from_cloud_event(payload)
        if event is None:
            return None

        if not is_lineage_relevant(event.method_name):
            return None

        return event

    def close(self) -> None:
        """Close the underlying Kafka consumer."""
        try:
            self._consumer.close()
            logger.info("Audit log consumer closed")
        except Exception:
            logger.debug("Error closing audit log consumer", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════
#  REST API Change Poller (fallback)
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class ClusterEndpoint:
    """REST endpoint + credentials for a single Kafka cluster."""

    cluster_id: str
    rest_endpoint: str  # e.g. https://pkc-xxxxx.region.cloud.confluent.cloud:443
    api_key: str
    api_secret: str


@dataclass
class _Snapshot:
    """Hashable snapshot of resource state."""

    topics: str = ""
    connectors: str = ""
    ksqldb_queries: str = ""
    flink_statements: str = ""

    def diff(self, other: _Snapshot) -> list[str]:
        """Return list of changed resource types."""
        changes: list[str] = []
        if self.topics != other.topics:
            changes.append("topics")
        if self.connectors != other.connectors:
            changes.append("connectors")
        if self.ksqldb_queries != other.ksqldb_queries:
            changes.append("ksqldb_queries")
        if self.flink_statements != other.flink_statements:
            changes.append("flink_statements")
        return changes


def _hash_json(data: Any) -> str:
    """Produce a stable hash of JSON-serializable data."""
    return hashlib.md5(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


@dataclass
class ChangePoller:
    """Polls Confluent Cloud REST APIs to detect lineage-relevant changes.

    Uses Cloud API key for management APIs and cluster-scoped credentials
    for topic listing. Polls ksqlDB data-plane endpoints for query changes
    and Flink regional endpoints for statement changes.
    """

    cloud_api_key: str
    cloud_api_secret: str
    environment_id: str
    cluster_endpoints: list[ClusterEndpoint] = field(default_factory=list)
    # Optional credentials for ksqlDB / Flink data-plane access
    ksqldb_api_key: str | None = None
    ksqldb_api_secret: str | None = None
    flink_api_key: str | None = None
    flink_api_secret: str | None = None
    cloud_base_url: str = "https://api.confluent.cloud"
    timeout: float = 15.0

    _last_snapshot: _Snapshot = field(default_factory=_Snapshot, init=False)
    _initialized: bool = field(default=False, init=False)

    @property
    def cluster_ids(self) -> list[str]:
        return [ep.cluster_id for ep in self.cluster_endpoints]

    async def poll(self) -> list[AuditEvent]:
        """Poll APIs and return synthetic AuditEvents for detected changes.

        Returns an empty list if no changes detected or on first poll
        (baseline snapshot).
        """
        try:
            snapshot = await self._take_snapshot()
        except Exception:
            logger.warning("Change poll failed", exc_info=True)
            return []

        if not self._initialized:
            self._last_snapshot = snapshot
            self._initialized = True
            logger.info("Change poller initialized (baseline snapshot taken)")
            return []

        changes = self._last_snapshot.diff(snapshot)
        self._last_snapshot = snapshot

        if not changes:
            return []

        logger.info("Changes detected: %s", ", ".join(changes))
        now = datetime.now(UTC)
        return [
            AuditEvent(
                id=f"poll-{resource}-{now.timestamp():.0f}",
                time=now,
                method_name=f"poll.{resource}.Changed",
                resource_name=resource,
                principal="lineage-bridge-poller",
                environment_id=self.environment_id,
                cluster_id=(self.cluster_ids[0] if self.cluster_ids else None),
                raw={"changed_resources": changes},
            )
            for resource in changes
        ]

    async def _take_snapshot(self) -> _Snapshot:
        """Fetch current state from all monitored endpoints."""
        cloud = ConfluentClient(
            self.cloud_base_url,
            self.cloud_api_key,
            self.cloud_api_secret,
            timeout=self.timeout,
        )

        async with cloud:
            results = await asyncio.gather(
                self._snapshot_topics(),
                self._snapshot_connectors(cloud),
                self._snapshot_ksqldb(cloud),
                self._snapshot_flink(cloud),
                return_exceptions=True,
            )

        return _Snapshot(
            topics=results[0] if isinstance(results[0], str) else "",
            connectors=results[1] if isinstance(results[1], str) else "",
            ksqldb_queries=(results[2] if isinstance(results[2], str) else ""),
            flink_statements=(results[3] if isinstance(results[3], str) else ""),
        )

    async def _snapshot_topics(self) -> str:
        """Hash of topic names across all clusters.

        Uses cluster-scoped REST endpoints + credentials.
        """
        all_topics: list[str] = []
        for ep in self.cluster_endpoints:
            try:
                client = ConfluentClient(
                    ep.rest_endpoint,
                    ep.api_key,
                    ep.api_secret,
                    timeout=self.timeout,
                )
                async with client:
                    items = await client.paginate(f"/kafka/v3/clusters/{ep.cluster_id}/topics")
                    all_topics.extend(sorted(t.get("topic_name", "") for t in items))
            except Exception:
                logger.debug("Topic poll failed for %s", ep.cluster_id, exc_info=True)
        return _hash_json(all_topics)

    async def _snapshot_connectors(self, cloud: ConfluentClient) -> str:
        """Hash of connector names and states."""
        connectors: list[dict] = []
        for ep in self.cluster_endpoints:
            try:
                items = await cloud.paginate(
                    f"/connect/v1/environments/{self.environment_id}"
                    f"/clusters/{ep.cluster_id}/connectors",
                    params={"expand": "status"},
                )
                for c in items:
                    connectors.append(
                        {
                            "name": c.get("info", {}).get("name", ""),
                            "status": c.get("status", {}).get("connector", {}).get("state", ""),
                        }
                    )
            except Exception:
                logger.debug(
                    "Connector poll failed for %s",
                    ep.cluster_id,
                    exc_info=True,
                )
        return _hash_json(sorted(connectors, key=lambda c: c["name"]))

    async def _snapshot_ksqldb(self, cloud: ConfluentClient) -> str:
        """Hash of ksqlDB persistent queries across all clusters.

        Discovers ksqlDB clusters via Cloud API, then queries each
        data-plane endpoint with SHOW QUERIES to get query IDs + states.
        """
        all_queries: list[dict] = []
        try:
            clusters = await cloud.paginate(
                "/ksqldbcm/v2/clusters",
                params={"environment": self.environment_id},
            )
        except Exception:
            logger.debug("ksqlDB cluster discovery failed", exc_info=True)
            return ""

        dp_key = self.ksqldb_api_key or self.cloud_api_key
        dp_secret = self.ksqldb_api_secret or self.cloud_api_secret

        for cluster in clusters:
            endpoint = cluster.get("status", {}).get("http_endpoint") or cluster.get(
                "spec", {}
            ).get("http_endpoint")
            if not endpoint:
                continue
            try:
                dp = ConfluentClient(endpoint, dp_key, dp_secret, timeout=self.timeout)
                async with dp:
                    resp = await dp.post(
                        "/ksql",
                        json_body={
                            "ksql": "SHOW QUERIES;",
                            "streamsProperties": {},
                        },
                    )
                    queries = []
                    if isinstance(resp, list) and resp:
                        queries = resp[0].get("queries", [])
                    elif isinstance(resp, dict):
                        queries = resp.get("queries", [])
                    for q in queries:
                        all_queries.append(
                            {
                                "id": q.get("id", ""),
                                "state": q.get("state", ""),
                            }
                        )
            except Exception:
                logger.debug(
                    "ksqlDB query poll failed for %s",
                    cluster.get("id"),
                    exc_info=True,
                )
        return _hash_json(sorted(all_queries, key=lambda q: q["id"]))

    async def _snapshot_flink(self, cloud: ConfluentClient) -> str:
        """Hash of Flink statement names and states.

        Discovers compute pools to determine region/cloud, fetches org ID,
        then polls the regional Flink data-plane for statements.
        """
        try:
            pools = await cloud.paginate(
                "/fcpm/v2/compute-pools",
                params={"environment": self.environment_id},
            )
        except Exception:
            logger.debug("Flink pool discovery failed", exc_info=True)
            return ""

        if not pools:
            return ""

        # Determine region/cloud from first pool
        spec = pools[0].get("spec", {})
        region = spec.get("region", "")
        flink_cloud = spec.get("cloud", "").lower()
        if not region or not flink_cloud:
            return ""

        # Get organization ID from pool metadata
        org_id = ""
        resource_name = pools[0].get("metadata", {}).get("resource_name", "")
        if "/organization=" in resource_name:
            org_id = resource_name.split("/organization=")[1].split("/")[0]

        if not org_id:
            # Try from environment metadata
            try:
                env_data = await cloud.get(f"/org/v2/environments/{self.environment_id}")
                rn = env_data.get("metadata", {}).get("resource_name", "")
                if "/organization=" in rn:
                    org_id = rn.split("/organization=")[1].split("/")[0]
            except Exception:
                logger.debug("Org ID lookup failed", exc_info=True)

        if not org_id:
            logger.debug("Cannot determine org ID for Flink polling")
            return ""

        dp_key = self.flink_api_key or self.cloud_api_key
        dp_secret = self.flink_api_secret or self.cloud_api_secret

        try:
            flink_base = f"https://flink.{region}.{flink_cloud}.confluent.cloud"
            dp = ConfluentClient(flink_base, dp_key, dp_secret, timeout=self.timeout)
            async with dp:
                path = (
                    f"/sql/v1/organizations/{org_id}/environments/{self.environment_id}/statements"
                )
                items = await dp.paginate(path, page_size=100)
                statements = [
                    {
                        "name": s.get("name", ""),
                        "status": s.get("status", {}).get("phase", ""),
                    }
                    for s in items
                ]
                return _hash_json(sorted(statements, key=lambda s: s["name"]))
        except Exception:
            logger.debug("Flink statement poll failed", exc_info=True)
            return ""
