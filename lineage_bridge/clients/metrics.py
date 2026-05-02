# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
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
from lineage_bridge.models.graph import EdgeType, LineageGraph, LineageNode, NodeType

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


def _coerce_int(val: Any) -> int | None:
    """Best-effort int conversion. Catalog APIs return num_rows/num_bytes as
    strings ("582"), ints, or sometimes nothing — normalize to int|None."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _coerce_epoch_ms(val: Any) -> datetime | None:
    """Coerce a catalog-provided timestamp into a UTC datetime, or None.

    Accepts the three shapes the catalog providers actually return:
    - BigQuery / UC: epoch-millis as int or numeric string ("1777688367152")
    - AWS Glue:      ISO 8601 / Python `str(datetime)` ("2026-04-14 21:42:11+00:00")
    """
    if val is None or val == "":
        return None
    ms = _coerce_int(val)
    if ms is not None and ms > 0:
        try:
            return datetime.fromtimestamp(ms / 1000.0, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    # Try ISO-like strings (with or without timezone).
    if isinstance(val, str):
        for s in (val, val.replace(" ", "T")):
            try:
                dt = datetime.fromisoformat(s)
                return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            except ValueError:
                continue
    return None


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
            resp = await self._client.post("/v2/metrics/cloud/query", json_body=body)
            return resp.get("data", [])
        except Exception as exc:
            logger.debug(
                "Metrics query failed for %s on %s: %s",
                metric,
                cluster_id,
                exc,
            )
            return []

    async def query_topic_metrics(self, cluster_id: str) -> dict[str, MetricsSummary]:
        """Query topic-level metrics for a Kafka cluster.

        Returns a dict of topic_name -> MetricsSummary.
        """
        summaries: dict[str, MetricsSummary] = {}

        for metric in _TOPIC_METRICS:
            data = await self._query_metric(metric, cluster_id, "metric.topic")
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

    async def query_connector_metrics(self, cluster_id: str) -> dict[str, MetricsSummary]:
        """Query connector-level metrics for a Kafka cluster."""
        summaries: dict[str, MetricsSummary] = {}

        for metric in _CONNECTOR_METRICS:
            data = await self._query_metric(metric, cluster_id, "resource.connector.id")
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

    async def query_flink_metrics(self, cluster_id: str) -> dict[str, MetricsSummary]:
        """Query Flink statement-level metrics for a Kafka cluster.

        Returns a dict of statement_id -> MetricsSummary (received/sent records).
        Telemetry doesn't expose Flink statement bytes, so byte fields stay 0.
        """
        summaries: dict[str, MetricsSummary] = {}
        for metric in _FLINK_METRICS:
            data = await self._query_metric(metric, cluster_id, "resource.flink_statement.id")
            for point in data:
                statement = point.get("resource.flink_statement.id", "")
                if not statement:
                    continue
                s = summaries.setdefault(statement, MetricsSummary())
                val = point.get("value", 0.0)
                if "num_records_in" in metric:
                    s.received_records += val
                elif "num_records_out" in metric:
                    s.sent_records += val
                if val > 0:
                    s.is_active = True
        return summaries

    @staticmethod
    def _apply_summary(node: LineageNode, summary: MetricsSummary, window_hours: int) -> None:
        """Mutate `node.attributes` with the standard `metrics_*` shape."""
        node.attributes["metrics_received_bytes"] = summary.received_bytes
        node.attributes["metrics_sent_bytes"] = summary.sent_bytes
        node.attributes["metrics_received_records"] = summary.received_records
        node.attributes["metrics_sent_records"] = summary.sent_records
        node.attributes["metrics_active"] = summary.is_active
        node.attributes["metrics_window_hours"] = window_hours

    async def enrich(self, graph: LineageGraph, cluster_id: str) -> int:
        """Enrich graph nodes with metrics for a given cluster.

        Populates the standard `metrics_*` attribute shape across every
        applicable node type:

        - KAFKA_TOPIC, CONNECTOR, FLINK_JOB → live counters from Telemetry API
        - CONSUMER_GROUP                    → derived from MEMBER_OF.max_lag edges
        - TABLEFLOW_TABLE                   → inherited from upstream KAFKA_TOPIC
        - CATALOG_TABLE                     → promoted from catalog enrichment
                                             (num_rows / num_bytes / last_modified)
        - KSQLDB_QUERY                      → metrics_active from query state
        - SCHEMA, EXTERNAL_DATASET          → skipped (no real metric source)

        Returns the number of nodes enriched.
        """
        enriched = 0

        # ── Telemetry-backed metrics (Topics, Connectors, Flink) ───────
        topic_metrics = await self.query_topic_metrics(cluster_id)
        for node in graph.nodes:
            if node.node_type != NodeType.KAFKA_TOPIC or node.cluster_id != cluster_id:
                continue
            parts = node.qualified_name.split(":")
            topic_name = parts[-1] if len(parts) > 1 else node.qualified_name
            summary = topic_metrics.get(topic_name) or topic_metrics.get(node.display_name)
            if summary:
                self._apply_summary(node, summary, self._lookback_hours)
                enriched += 1

        connector_metrics = await self.query_connector_metrics(cluster_id)
        for node in graph.nodes:
            if node.node_type != NodeType.CONNECTOR or node.cluster_id != cluster_id:
                continue
            # `confluent_id` (the lcc-XXXXX resource ID) is what Telemetry
            # groups by; fall back to display_name for self-managed connectors.
            keys = (node.attributes.get("confluent_id"), node.display_name)
            summary = next(
                (connector_metrics[k] for k in keys if k and k in connector_metrics),
                None,
            )
            if summary:
                node.attributes["metrics_received_records"] = summary.received_records
                node.attributes["metrics_sent_records"] = summary.sent_records
                node.attributes["metrics_active"] = summary.is_active
                node.attributes["metrics_window_hours"] = self._lookback_hours
                enriched += 1

        flink_metrics = await self.query_flink_metrics(cluster_id)
        for node in graph.nodes:
            if node.node_type != NodeType.FLINK_JOB:
                continue
            # Flink statement IDs match the node's qualified_name; fall back
            # to display_name for older extracts that used the friendlier name.
            keys = (node.qualified_name, node.display_name)
            summary = next((flink_metrics[k] for k in keys if k and k in flink_metrics), None)
            if summary:
                node.attributes["metrics_received_records"] = summary.received_records
                node.attributes["metrics_sent_records"] = summary.sent_records
                node.attributes["metrics_active"] = summary.is_active
                node.attributes["metrics_window_hours"] = self._lookback_hours
                enriched += 1

        # ── Graph-derived metrics (no extra API calls) ─────────────────
        enriched += self._enrich_consumer_groups(graph)
        enriched += self._enrich_catalog_tables(graph)
        enriched += self._enrich_ksqldb_queries(graph)
        # Inherit-from-topic must run AFTER topic enrichment above so we
        # have something to copy.
        enriched += self._enrich_tableflow_tables(graph)

        logger.info(
            "Enriched %d nodes with metrics for cluster %s",
            enriched,
            cluster_id,
        )
        return enriched

    def _enrich_consumer_groups(self, graph: LineageGraph) -> int:
        """Aggregate per-topic `max_lag` (already on MEMBER_OF edges) into a
        per-group `metrics_total_lag` + `metrics_active` on the CONSUMER_GROUP node.
        """
        lag_by_group: dict[str, int] = {}
        for edge in graph.edges:
            if edge.edge_type != EdgeType.MEMBER_OF:
                continue
            lag = int(edge.attributes.get("max_lag", 0) or 0)
            lag_by_group[edge.src_id] = lag_by_group.get(edge.src_id, 0) + lag

        enriched = 0
        for node in graph.nodes:
            if node.node_type != NodeType.CONSUMER_GROUP:
                continue
            total_lag = lag_by_group.get(node.node_id, 0)
            node.attributes["metrics_total_lag"] = total_lag
            node.attributes["metrics_active"] = total_lag > 0 or node.attributes.get("state") in {
                "STABLE",
                "Stable",
            }
            node.attributes["metrics_window_hours"] = self._lookback_hours
            enriched += 1
        return enriched

    def _enrich_tableflow_tables(self, graph: LineageGraph) -> int:
        """Inherit topic metrics onto TABLEFLOW_TABLE via upstream MATERIALIZES edges."""
        topics_by_id = {n.node_id: n for n in graph.nodes if n.node_type == NodeType.KAFKA_TOPIC}
        upstream_topic: dict[str, str] = {}
        for edge in graph.edges:
            if edge.edge_type != EdgeType.MATERIALIZES:
                continue
            if edge.src_id in topics_by_id:
                upstream_topic[edge.dst_id] = edge.src_id

        enriched = 0
        for node in graph.nodes:
            if node.node_type != NodeType.TABLEFLOW_TABLE:
                continue
            tid = upstream_topic.get(node.node_id)
            if not tid:
                continue
            tn = topics_by_id[tid]
            if tn.attributes.get("metrics_active") is None:
                continue
            for k in (
                "metrics_received_bytes",
                "metrics_sent_bytes",
                "metrics_received_records",
                "metrics_sent_records",
                "metrics_active",
                "metrics_window_hours",
            ):
                if k in tn.attributes:
                    node.attributes[k] = tn.attributes[k]
            enriched += 1
        return enriched

    def _enrich_catalog_tables(self, graph: LineageGraph) -> int:
        """Promote catalog-provided row/byte counts (and a recency signal) to
        the standard metrics_* shape. Each catalog supplies a different subset:

        - BigQuery (Google): num_rows, num_bytes, last_modified_time
        - AWS Glue:          create_time, update_time (no row/byte counts)
        - Unity Catalog:     updated_at (epoch ms; no row/byte counts)

        We populate whichever of `metrics_received_records` / `_bytes` we can,
        and derive `metrics_active` from a recency timestamp when present so
        every CATALOG_TABLE has at least the active/window pair.
        """
        now = datetime.now(UTC)
        # Per-catalog timestamp aliases — picked from whatever the provider
        # actually fills in. First non-None wins.
        timestamp_keys = ("last_modified_time", "updated_at", "update_time", "create_time")
        enriched = 0
        for node in graph.nodes:
            if node.node_type != NodeType.CATALOG_TABLE:
                continue
            num_rows = _coerce_int(node.attributes.get("num_rows"))
            num_bytes = _coerce_int(node.attributes.get("num_bytes"))
            last_mod = next(
                (
                    parsed
                    for k in timestamp_keys
                    if (parsed := _coerce_epoch_ms(node.attributes.get(k))) is not None
                ),
                None,
            )
            if num_rows is None and num_bytes is None and last_mod is None:
                continue
            if num_rows is not None:
                node.attributes["metrics_received_records"] = float(num_rows)
            if num_bytes is not None:
                node.attributes["metrics_received_bytes"] = float(num_bytes)
            if last_mod is not None:
                age_hours = (now - last_mod).total_seconds() / 3600.0
                active = age_hours <= self._lookback_hours
            else:
                active = (num_rows or 0) > 0
            node.attributes["metrics_active"] = active
            node.attributes["metrics_window_hours"] = self._lookback_hours
            enriched += 1
        return enriched

    def _enrich_ksqldb_queries(self, graph: LineageGraph) -> int:
        """KSQLDB_QUERY has no Telemetry counter — surface RUNNING state as a metric."""
        enriched = 0
        for node in graph.nodes:
            if node.node_type != NodeType.KSQLDB_QUERY:
                continue
            state = (node.attributes.get("state") or "").upper()
            node.attributes["metrics_active"] = state in {"RUNNING", "ACTIVE"}
            node.attributes["metrics_window_hours"] = self._lookback_hours
            enriched += 1
        return enriched
