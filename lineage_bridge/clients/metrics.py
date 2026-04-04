"""Confluent Cloud Metrics API client for enriching lineage nodes with throughput data.

Queries the Telemetry API to add real-time metrics (bytes in/out, records,
consumer lag) to existing graph nodes. This is an enrichment-only client —
it does not create new nodes or edges.

API reference: POST /v2/metrics/cloud/query
Auth: Cloud API key with MetricsViewer role.
Rate limit: 10 req/min — queries are batched per cluster.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import LineageGraph, NodeType

logger = logging.getLogger(__name__)

_TELEMETRY_BASE = "https://api.telemetry.confluent.cloud"

# Metrics we query, grouped by resource type
_TOPIC_METRICS = [
    "io.confluent.kafka.server/received_bytes",
    "io.confluent.kafka.server/sent_bytes",
    "io.confluent.kafka.server/received_records",
    "io.confluent.kafka.server/sent_records",
]

_CONNECTOR_METRICS = [
    "io.confluent.kafka.connect/received_records",
    "io.confluent.kafka.connect/sent_records",
]

_FLINK_METRICS = [
    "io.confluent.flink/num_records_in",
    "io.confluent.flink/num_records_out",
]


@dataclass
class MetricsSummary:
    """Aggregated metrics for a single resource over the query window."""

    received_bytes: float = 0.0
    sent_bytes: float = 0.0
    received_records: float = 0.0
    sent_records: float = 0.0
    is_active: bool = False


class MetricsClient:
    """Client for the Confluent Cloud Metrics (Telemetry) API.

    Enriches an existing LineageGraph with throughput and activity metrics.
    Uses the Cloud API key — requires MetricsViewer role.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        lookback_hours: int = 1,
    ) -> None:
        self._client = ConfluentClient(
            _TELEMETRY_BASE,
            api_key,
            api_secret,
        )
        self._lookback_hours = lookback_hours

    async def close(self) -> None:
        await self._client.close()

    async def __aenter__(self) -> MetricsClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _time_interval(self) -> str:
        """Build an ISO 8601 interval string for the lookback window."""
        now = datetime.now(UTC)
        start = now - timedelta(hours=self._lookback_hours)
        return f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{now.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    async def _query_metric(
        self,
        metric: str,
        cluster_id: str,
        group_by: str,
        *,
        agg: str = "SUM",
    ) -> list[dict[str, Any]]:
        """Execute a single metrics query and return the data points."""
        body = {
            "aggregations": [{"metric": metric, "agg": agg}],
            "filter": {
                "field": "resource.kafka.id",
                "op": "EQ",
                "value": cluster_id,
            },
            "granularity": "ALL",
            "intervals": [self._time_interval()],
            "group_by": [group_by],
            "limit": 1000,
        }
        try:
            resp = await self._client.post(
                "/v2/metrics/cloud/query", json_body=body
            )
            return resp.get("data", [])
        except Exception as exc:
            logger.debug(
                "Metrics query failed for %s on %s: %s",
                metric,
                cluster_id,
                exc,
            )
            return []

    async def query_topic_metrics(
        self, cluster_id: str
    ) -> dict[str, MetricsSummary]:
        """Query topic-level metrics for a Kafka cluster.

        Returns a dict of topic_name -> MetricsSummary.
        """
        summaries: dict[str, MetricsSummary] = {}

        for metric in _TOPIC_METRICS:
            data = await self._query_metric(
                metric, cluster_id, "metric.topic"
            )
            for point in data:
                topic = point.get("metric.topic", "")
                if not topic:
                    continue
                s = summaries.setdefault(topic, MetricsSummary())
                val = point.get("value", 0.0)
                if "received_bytes" in metric:
                    s.received_bytes += val
                elif "sent_bytes" in metric:
                    s.sent_bytes += val
                elif "received_records" in metric:
                    s.received_records += val
                elif "sent_records" in metric:
                    s.sent_records += val
                if val > 0:
                    s.is_active = True

        return summaries

    async def query_connector_metrics(
        self, cluster_id: str
    ) -> dict[str, MetricsSummary]:
        """Query connector-level metrics for a Kafka cluster."""
        summaries: dict[str, MetricsSummary] = {}

        for metric in _CONNECTOR_METRICS:
            data = await self._query_metric(
                metric, cluster_id, "resource.connector.id"
            )
            for point in data:
                connector = point.get("resource.connector.id", "")
                if not connector:
                    continue
                s = summaries.setdefault(connector, MetricsSummary())
                val = point.get("value", 0.0)
                if "received_records" in metric:
                    s.received_records += val
                elif "sent_records" in metric:
                    s.sent_records += val
                if val > 0:
                    s.is_active = True

        return summaries

    async def enrich(self, graph: LineageGraph, cluster_id: str) -> int:
        """Enrich graph nodes with metrics data for a given cluster.

        Adds metric attributes to topic and connector nodes:
        - metrics_received_bytes, metrics_sent_bytes
        - metrics_received_records, metrics_sent_records
        - metrics_active (bool)
        - metrics_window_hours

        Returns the number of nodes enriched.
        """
        enriched = 0

        # Enrich topics
        topic_metrics = await self.query_topic_metrics(cluster_id)
        for node in graph.nodes:
            if node.node_type != NodeType.KAFKA_TOPIC:
                continue
            if node.cluster_id != cluster_id:
                continue
            # Match by topic name (last segment of qualified_name)
            parts = node.qualified_name.split(":")
            topic_name = parts[-1] if len(parts) > 1 else node.qualified_name
            # Also try display_name
            summary = topic_metrics.get(topic_name) or topic_metrics.get(node.display_name)
            if summary:
                node.attributes["metrics_received_bytes"] = summary.received_bytes
                node.attributes["metrics_sent_bytes"] = summary.sent_bytes
                node.attributes["metrics_received_records"] = summary.received_records
                node.attributes["metrics_sent_records"] = summary.sent_records
                node.attributes["metrics_active"] = summary.is_active
                node.attributes["metrics_window_hours"] = self._lookback_hours
                enriched += 1

        # Enrich connectors
        connector_metrics = await self.query_connector_metrics(cluster_id)
        for node in graph.nodes:
            if node.node_type != NodeType.CONNECTOR:
                continue
            if node.cluster_id != cluster_id:
                continue
            connector_id = node.attributes.get("connector_id", node.display_name)
            summary = connector_metrics.get(connector_id)
            if summary:
                node.attributes["metrics_received_records"] = summary.received_records
                node.attributes["metrics_sent_records"] = summary.sent_records
                node.attributes["metrics_active"] = summary.is_active
                node.attributes["metrics_window_hours"] = self._lookback_hours
                enriched += 1

        logger.info(
            "Enriched %d nodes with metrics for cluster %s",
            enriched,
            cluster_id,
        )
        return enriched
