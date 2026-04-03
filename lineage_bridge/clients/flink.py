"""Flink on Confluent Cloud client — extracts Flink SQL job lineage."""

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

# ── Flink SQL regex parsers ─────────────────────────────────────────────

# INSERT INTO <table>
_INSERT_INTO_RE = re.compile(r"\bINSERT\s+INTO\s+(?:`([^`]+)`|\"([^\"]+)\"|(\S+))", re.IGNORECASE)

# FROM <table>  /  JOIN <table>
_FROM_RE = re.compile(r"\bFROM\s+(?:`([^`]+)`|\"([^\"]+)\"|(\S+))", re.IGNORECASE)
_JOIN_RE = re.compile(r"\bJOIN\s+(?:`([^`]+)`|\"([^\"]+)\"|(\S+))", re.IGNORECASE)

# CREATE TABLE ... WITH ( 'kafka.topic' = '<topic>' )
_KAFKA_TOPIC_PROP_RE = re.compile(r"'kafka\.topic'\s*=\s*'([^']+)'", re.IGNORECASE)


def _extract_name(match: re.Match[str]) -> str:
    """Return the first non-None group from a table-name match."""
    for g in match.groups():
        if g is not None:
            return g
    return ""


def _last_segment(name: str) -> str:
    """Return the last dot-separated segment (i.e. the table/topic name)."""
    return name.rsplit(".", 1)[-1]


class FlinkClient:
    """Extracts Flink SQL statement lineage from Confluent Cloud.

    Uses two API surfaces:
    1. Cloud API — compute pool discovery.
    2. Flink data-plane — statement listing.
    """

    def __init__(
        self,
        cloud_api_key: str,
        cloud_api_secret: str,
        environment_id: str,
        organization_id: str,
        *,
        flink_api_key: str | None = None,
        flink_api_secret: str | None = None,
        flink_region: str | None = None,
        flink_cloud: str | None = None,
        cloud_base_url: str = "https://api.confluent.cloud",
        timeout: float = 30.0,
    ) -> None:
        self.environment_id = environment_id
        self.organization_id = organization_id
        self._flink_api_key = flink_api_key or cloud_api_key
        self._flink_api_secret = flink_api_secret or cloud_api_secret
        self._flink_region = flink_region
        self._flink_cloud = flink_cloud
        self._timeout = timeout
        self._cloud = ConfluentClient(
            cloud_base_url, cloud_api_key, cloud_api_secret, timeout=timeout
        )

    async def close(self) -> None:
        await self._cloud.close()

    async def __aenter__(self) -> FlinkClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── helpers ─────────────────────────────────────────────────────────

    def _job_node_id(self, name: str) -> str:
        return f"confluent:flink_job:{self.environment_id}:{name}"

    def _topic_node_id(self, topic: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic}"

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        # Discover compute pools to get region/cloud if not supplied.
        pools = await self._list_compute_pools()
        if not pools:
            logger.info("No Flink compute pools in environment %s", self.environment_id)
            return nodes, edges

        # Determine region/cloud from the first pool if not configured.
        region = self._flink_region
        cloud = self._flink_cloud
        if not region or not cloud:
            spec = pools[0].get("spec", {})
            region = region or spec.get("region", "")
            cloud = cloud or (spec.get("cloud", "").lower())
        if not region or not cloud:
            logger.warning("Cannot determine Flink region/cloud — skipping statements")
            return nodes, edges

        flink_base = f"https://flink.{region}.{cloud}.confluent.cloud"
        dp = ConfluentClient(
            flink_base, self._flink_api_key, self._flink_api_secret, timeout=self._timeout
        )

        try:
            async with dp:
                statements = await self._list_statements(dp, region, cloud)
                for stmt in statements:
                    s_nodes, s_edges = self._process_statement(stmt)
                    nodes.extend(s_nodes)
                    edges.extend(s_edges)
        except Exception:
            logger.warning("Failed to list Flink statements", exc_info=True)

        logger.info(
            "Flink extracted %d nodes, %d edges from environment %s",
            len(nodes),
            len(edges),
            self.environment_id,
        )
        return nodes, edges

    def _process_statement(
        self, stmt: dict[str, Any]
    ) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        name = stmt.get("name", "")
        spec = stmt.get("spec", {})
        status = stmt.get("status", {})
        sql = spec.get("statement", "")
        phase = status.get("phase", "")

        if not name or not sql:
            return nodes, edges

        compute_pool_id = spec.get("compute_pool", {}).get("id")

        nodes.append(
            LineageNode(
                node_id=self._job_node_id(name),
                system=SystemType.CONFLUENT,
                node_type=NodeType.FLINK_JOB,
                qualified_name=name,
                display_name=name,
                environment_id=self.environment_id,
                attributes={
                    "sql": sql,
                    "phase": phase,
                    "compute_pool_id": compute_pool_id,
                    "principal": spec.get("principal"),
                },
            )
        )

        source_topics, sink_topics = self._parse_flink_sql(sql)

        for topic in source_topics:
            tid = self._topic_node_id(topic)
            nodes.append(
                LineageNode(
                    node_id=tid,
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.KAFKA_TOPIC,
                    qualified_name=topic,
                    display_name=topic,
                    environment_id=self.environment_id,
                )
            )
            edges.append(
                LineageEdge(
                    src_id=tid,
                    dst_id=self._job_node_id(name),
                    edge_type=EdgeType.CONSUMES,
                    confidence=0.8,
                )
            )

        for topic in sink_topics:
            tid = self._topic_node_id(topic)
            nodes.append(
                LineageNode(
                    node_id=tid,
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.KAFKA_TOPIC,
                    qualified_name=topic,
                    display_name=topic,
                    environment_id=self.environment_id,
                )
            )
            edges.append(
                LineageEdge(
                    src_id=self._job_node_id(name),
                    dst_id=tid,
                    edge_type=EdgeType.PRODUCES,
                )
            )

        return nodes, edges

    # ── SQL parsing ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_flink_sql(sql: str) -> tuple[set[str], set[str]]:
        """Parse a Flink SQL statement and return ``(source_topics, sink_topics)``.

        Uses regex — intentionally simple for the POC.
        """
        sources: set[str] = set()
        sinks: set[str] = set()

        # Explicit kafka.topic property in CREATE TABLE DDL.
        for _m in _KAFKA_TOPIC_PROP_RE.finditer(sql):
            # These could be either source or sink depending on usage;
            # handled below by INSERT INTO / FROM context.
            pass

        # INSERT INTO → sink.
        for m in _INSERT_INTO_RE.finditer(sql):
            sinks.add(_last_segment(_extract_name(m)))

        # FROM / JOIN → source.
        for m in _FROM_RE.finditer(sql):
            sources.add(_last_segment(_extract_name(m)))
        for m in _JOIN_RE.finditer(sql):
            sources.add(_last_segment(_extract_name(m)))

        # Remove sinks from sources (a table appearing in both is primarily a sink).
        sources -= sinks

        # Filter out SQL noise keywords that might be captured.
        noise = {"select", "where", "group", "having", "order", "limit", "values", "set"}
        sources = {s for s in sources if s.lower() not in noise}
        sinks = {s for s in sinks if s.lower() not in noise}

        return sources, sinks

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_compute_pools(self) -> list[dict[str, Any]]:
        return await self._cloud.paginate(
            "/fcpm/v2/compute-pools",
            params={"environment": self.environment_id},
        )

    async def _list_statements(
        self, dp: ConfluentClient, region: str, cloud: str
    ) -> list[dict[str, Any]]:
        path = (
            f"/sql/v1/organizations/{self.organization_id}"
            f"/environments/{self.environment_id}/statements"
        )
        return await dp.paginate(path, page_size=100)
