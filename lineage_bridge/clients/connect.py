# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Confluent Cloud Connect API client — extracts connector lineage."""

from __future__ import annotations

import logging
import re
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)

# ── connector class classification ──────────────────────────────────────

# Maps regex patterns (matched against connector.class) to direction.
_SOURCE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?i)debezium"),
    re.compile(r"(?i)PostgresSource"),
    re.compile(r"(?i)MySqlSource"),
    re.compile(r"(?i)SqlServerSource"),
    re.compile(r"(?i)MongoDbSource"),
    re.compile(r"(?i)OracleSource"),
    re.compile(r"(?i)JdbcSource"),
    re.compile(r"(?i)Source$"),
    re.compile(r"(?i)\.source\."),
]

_SINK_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?i)S3Sink"),
    re.compile(r"(?i)GcsSink"),
    re.compile(r"(?i)BigQuery(?:Storage)?Sink"),
    re.compile(r"(?i)ElasticsearchSink"),
    re.compile(r"(?i)JdbcSink"),
    re.compile(r"(?i)SnowflakeSink"),
    re.compile(r"(?i)Sink$"),
    re.compile(r"(?i)\.sink\."),
]


def _classify_connector(connector_class: str, explicit_type: str | None = None) -> str:
    """Return ``'source'``, ``'sink'``, or ``'unknown'``."""
    if explicit_type in ("source", "sink"):
        return explicit_type
    for pat in _SOURCE_PATTERNS:
        if pat.search(connector_class):
            return "source"
    for pat in _SINK_PATTERNS:
        if pat.search(connector_class):
            return "sink"
    return "unknown"


def _extract_topics(config: dict[str, str]) -> list[str]:
    """Pull topic names from well-known connector config keys."""
    for key in ("topics", "kafka.topic", "topic"):
        val = config.get(key, "")
        if val:
            return [t.strip() for t in val.split(",") if t.strip()]
    return []


def _infer_external_dataset(config: dict[str, str], connector_class: str) -> str:
    """Best-effort inference of the external dataset name."""
    # S3
    bucket = config.get("s3.bucket.name")
    if bucket:
        return f"s3://{bucket}/"

    # GCS
    bucket = config.get("gcs.bucket.name")
    if bucket:
        return f"gs://{bucket}/"

    # JDBC / Debezium database
    host = config.get("connection.host", config.get("database.hostname", ""))
    db = config.get("db.name", config.get("database.dbname", config.get("database", "")))
    if host and db:
        return f"{host}/{db}"
    if db:
        return db

    # BigQuery
    project = config.get("project")
    dataset = config.get("defaultDataset", config.get("dataset", config.get("datasets")))
    if project and dataset:
        return f"{project}.{dataset}"

    # Snowflake
    sf_db = config.get("snowflake.database.name")
    sf_schema = config.get("snowflake.schema.name")
    if sf_db:
        return f"snowflake://{sf_db}" + (f"/{sf_schema}" if sf_schema else "")

    # Elasticsearch
    es_url = config.get("connection.url")
    if es_url:
        return es_url

    # Fallback: use the connector class itself
    return connector_class


def _bigquery_dataset_ref(config: dict[str, str]) -> tuple[str, str] | None:
    """Return ``(project, dataset)`` for a BigQuery sink connector, or None."""
    project = config.get("project", "").strip()
    dataset = (
        config.get("defaultDataset") or config.get("dataset") or config.get("datasets", "")
    ).strip()
    if not project or not dataset:
        return None
    # ``datasets`` may be a comma-separated list — take the first.
    dataset = dataset.split(",", 1)[0].strip()
    if not dataset:
        return None
    return project, dataset


class ConnectClient(ConfluentClient):
    """Extracts connector lineage from the Confluent Cloud Connect v1 API."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment_id: str,
        kafka_cluster_id: str,
        *,
        base_url: str = "https://api.confluent.cloud",
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.environment_id = environment_id
        self.kafka_cluster_id = kafka_cluster_id

    # ── helpers ─────────────────────────────────────────────────────────

    def _connector_node_id(self, name: str) -> str:
        return f"confluent:connector:{self.environment_id}:{name}"

    def _topic_node_id(self, topic: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic}"

    def _ext_node_id(self, dataset: str) -> str:
        return f"confluent:external_dataset:{self.environment_id}:{dataset}"

    def _build_google_tables(
        self,
        *,
        topics: list[str],
        project: str,
        dataset: str,
        connector_id: str,
        cluster_id: str,
    ) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Build one GOOGLE_TABLE node + PRODUCES edge per topic for a BigQuery sink."""
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []
        for topic in topics:
            table_name = topic.replace(".", "_").replace("-", "_")
            qualified = f"{project}.{dataset}.{table_name}"
            google_id = f"google:google_table:{self.environment_id}:{qualified}"
            nodes.append(
                LineageNode(
                    node_id=google_id,
                    system=SystemType.GOOGLE,
                    node_type=NodeType.GOOGLE_TABLE,
                    qualified_name=qualified,
                    display_name=qualified,
                    environment_id=self.environment_id,
                    cluster_id=cluster_id,
                    attributes={
                        "project_id": project,
                        "dataset_id": dataset,
                        "table_name": table_name,
                        "source_topic": topic,
                    },
                )
            )
            edges.append(
                LineageEdge(
                    src_id=connector_id,
                    dst_id=google_id,
                    edge_type=EdgeType.PRODUCES,
                )
            )
        return nodes, edges

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        connectors = await self._list_connectors()
        for detail in connectors:
            cname = detail.get("name", "")
            if not cname:
                continue

            config: dict[str, str] = detail.get("config", {})
            connector_class = config.get("connector.class", "")
            explicit_type = detail.get("type")
            direction = _classify_connector(connector_class, explicit_type)
            # Extract connector state from status block
            conn_state = detail.get("status", {}).get("connector", {}).get("state", "")

            # Connector node
            nodes.append(
                LineageNode(
                    node_id=self._connector_node_id(cname),
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.CONNECTOR,
                    qualified_name=cname,
                    display_name=cname,
                    environment_id=self.environment_id,
                    cluster_id=self.kafka_cluster_id,
                    attributes={
                        "connector_class": connector_class,
                        "direction": direction,
                        "state": conn_state,
                        "tasks_max": config.get("tasks.max"),
                        "output_data_format": config.get("output.data.format"),
                    },
                )
            )

            topics = _extract_topics(config)
            conn_id = self._connector_node_id(cname)

            # BigQuery sinks get per-topic GOOGLE_TABLE nodes instead of an
            # EXTERNAL_DATASET hub — the dataset is already encoded in each
            # google_table's qualified name.
            bq_ref = (
                _bigquery_dataset_ref(config) if "bigquery" in connector_class.lower() else None
            )
            ext_id: str | None = None
            if not bq_ref:
                ext_name = _infer_external_dataset(config, connector_class)
                ext_id = self._ext_node_id(ext_name)
                nodes.append(
                    LineageNode(
                        node_id=ext_id,
                        system=SystemType.EXTERNAL,
                        node_type=NodeType.EXTERNAL_DATASET,
                        qualified_name=ext_name,
                        display_name=ext_name,
                        environment_id=self.environment_id,
                        attributes={
                            "inferred_from": cname,
                            "connector_class": connector_class,
                        },
                    )
                )

            if direction == "source":
                # external_dataset → connector (PRODUCES)
                if ext_id is not None:
                    edges.append(
                        LineageEdge(
                            src_id=ext_id,
                            dst_id=conn_id,
                            edge_type=EdgeType.PRODUCES,
                        )
                    )
                # connector → kafka_topic (PRODUCES) for each topic
                for topic in topics:
                    tid = self._topic_node_id(topic)
                    # Create a placeholder topic node (will be merged with real one)
                    nodes.append(
                        LineageNode(
                            node_id=tid,
                            system=SystemType.CONFLUENT,
                            node_type=NodeType.KAFKA_TOPIC,
                            qualified_name=topic,
                            display_name=topic,
                            environment_id=self.environment_id,
                            cluster_id=self.kafka_cluster_id,
                        )
                    )
                    edges.append(
                        LineageEdge(
                            src_id=conn_id,
                            dst_id=tid,
                            edge_type=EdgeType.PRODUCES,
                        )
                    )
            elif direction == "sink":
                # kafka_topic → connector (CONSUMES)
                for topic in topics:
                    tid = self._topic_node_id(topic)
                    nodes.append(
                        LineageNode(
                            node_id=tid,
                            system=SystemType.CONFLUENT,
                            node_type=NodeType.KAFKA_TOPIC,
                            qualified_name=topic,
                            display_name=topic,
                            environment_id=self.environment_id,
                            cluster_id=self.kafka_cluster_id,
                        )
                    )
                    edges.append(
                        LineageEdge(
                            src_id=tid,
                            dst_id=conn_id,
                            edge_type=EdgeType.CONSUMES,
                        )
                    )
                # connector → external_dataset (PRODUCES) — skipped for BigQuery
                # sinks, which use per-topic GOOGLE_TABLE nodes instead.
                if ext_id is not None:
                    edges.append(
                        LineageEdge(
                            src_id=conn_id,
                            dst_id=ext_id,
                            edge_type=EdgeType.PRODUCES,
                        )
                    )

                # BigQuery sinks: synthesize one GOOGLE_TABLE per topic so the
                # publish UI can push lineage to Google Data Lineage. UC and Glue
                # get the same treatment via Tableflow; BigQuery isn't a
                # Tableflow target, so we infer it from the connector config.
                if bq_ref:
                    project, dataset = bq_ref
                    g_nodes, g_edges = self._build_google_tables(
                        topics=topics,
                        project=project,
                        dataset=dataset,
                        connector_id=conn_id,
                        cluster_id=self.kafka_cluster_id,
                    )
                    nodes.extend(g_nodes)
                    edges.extend(g_edges)
            else:
                logger.warning(
                    "Could not classify connector %s (class=%s) — skipping edges",
                    cname,
                    connector_class,
                )

        logger.info(
            "Connect extracted %d nodes, %d edges from cluster %s",
            len(nodes),
            len(edges),
            self.kafka_cluster_id,
        )
        return nodes, edges

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_connectors(self) -> list[dict[str, Any]]:
        """Return list of connector objects with config and status.

        The ``expand=info,status`` query returns a dict keyed by connector
        name where each value has ``info`` and ``status`` sub-keys::

            {"my-conn": {"info": {"name": ..., "config": ..., "type": ...},
                         "status": {"connector": {"state": "RUNNING"}, ...}}}

        We flatten each entry so the caller sees top-level ``name``,
        ``config``, ``type``, and ``status`` keys.
        """
        path = (
            f"/connect/v1/environments/{self.environment_id}"
            f"/clusters/{self.kafka_cluster_id}/connectors"
        )
        data = await self.get(path, params={"expand": "info,status"})
        if isinstance(data, list):
            # Plain list of names — fetch each individually
            result = []
            for name in data:
                try:
                    detail = await self.get(f"{path}/{name}")
                    result.append(detail)
                except Exception:
                    logger.warning("Failed to fetch connector %s", name, exc_info=True)
            return result
        # Expanded response: dict of name -> {info: {...}, status: {...}}
        result = []
        for name, wrapper in data.items():
            if not isinstance(wrapper, dict):
                continue
            info = wrapper.get("info", {})
            status = wrapper.get("status", {})
            # Flatten: merge info fields at top level, keep status nested
            entry = {**info, "status": status}
            # Ensure name is set even if info didn't have it
            entry.setdefault("name", name)
            result.append(entry)
        return result
