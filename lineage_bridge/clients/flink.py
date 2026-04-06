# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
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

# Match a possibly catalog-qualified identifier:
#   `cat`.`db`.`table`  or  "cat"."db"."table"  or  cat.db.table
_IDENT = r"(?:`[^`]+`(?:\.`[^`]+`)*|\"[^\"]+\"(?:\.\"[^\"]+\")*|\S+)"

# INSERT INTO <table>
_INSERT_INTO_RE = re.compile(rf"\bINSERT\s+INTO\s+({_IDENT})", re.IGNORECASE)

# CREATE TABLE <name> [WITH (...)] AS SELECT ... (CTAS — the table is the sink)
_CTAS_RE = re.compile(
    rf"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})"
    r"(?:\s+WITH\s*\([^)]*\))?"  # optional WITH (…) block
    r"\s+AS\b",
    re.IGNORECASE | re.DOTALL,
)

# FROM <table>  /  JOIN <table>
# Negative lookahead excludes windowing functions (handled separately).
_FROM_RE = re.compile(rf"\bFROM\s+(?!(?:TUMBLE|HOP|CUMULATE|SESSION)\b)({_IDENT})", re.IGNORECASE)
_JOIN_RE = re.compile(rf"\bJOIN\s+({_IDENT})", re.IGNORECASE)

# Flink windowing: TUMBLE(TABLE <name>, …), HOP(TABLE <name>, …), etc.
# Use a tighter ident pattern that stops at comma/paren.
_WINDOW_IDENT = r"(?:`[^`]+`(?:\.`[^`]+`)*|\"[^\"]+\"(?:\.\"[^\"]+\")*|[^\s,()]+)"
_WINDOW_TABLE_RE = re.compile(
    rf"\b(?:TUMBLE|HOP|CUMULATE|SESSION)\s*\(\s*TABLE\s+({_WINDOW_IDENT})",
    re.IGNORECASE,
)

# CREATE TABLE <name> ... WITH ( 'kafka.topic' = '<topic>' ) — pure DDL (no AS)
_CREATE_TABLE_RE = re.compile(
    rf"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})",
    re.IGNORECASE,
)
_KAFKA_TOPIC_PROP_RE = re.compile(r"'kafka\.topic'\s*=\s*'([^']+)'", re.IGNORECASE)


def _extract_name(match: re.Match[str]) -> str:
    """Return the first non-None group from a table-name match."""
    for g in match.groups():
        if g is not None:
            return g
    return ""


def _last_segment(name: str) -> str:
    """Return the last segment of a possibly catalog-qualified name.

    Handles:  `cat`.`db`.`table`     → table
              `cat`.`db`.`my.table`  → my.table   (dot inside backticks is literal)
              "cat"."db"."table"     → table
              cat.db.table           → table
              table                  → table
              `my.topic`             → my.topic   (single quoted ident with dot)
    """
    name = name.strip()

    # Multi-part qualified name: split on `.` between quoted identifiers.
    # e.g. `cat`.`db`.`tbl` → ['`cat`', '`db`', '`tbl`']
    # Only split on dots that sit *between* closing and opening quotes.
    parts = re.split(r"(?<=`)\s*\.\s*(?=`)|(?<=\")\s*\.\s*(?=\")", name)

    # If no inter-quote splits happened, try plain dot split only for
    # unquoted identifiers (no backticks/quotes at all).
    if len(parts) == 1 and not name.startswith("`") and not name.startswith('"'):
        parts = name.split(".")

    last = parts[-1].strip()
    # Strip surrounding backticks or double quotes
    if last.startswith("`") and last.endswith("`"):
        last = last[1:-1]
    elif last.startswith('"') and last.endswith('"'):
        last = last[1:-1]
    return last


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

                # First pass: build table-name → kafka-topic map from CREATE TABLE DDLs
                table_topic_map: dict[str, str] = {}
                for stmt in statements:
                    sql = stmt.get("spec", {}).get("statement", "")
                    # Strip leading SET commands
                    sql = re.sub(
                        r"^(\s*SET\s+'[^']*'\s+'[^']*'\s*;\s*)+",
                        "",
                        sql,
                        flags=re.IGNORECASE,
                    ).strip()
                    ct_match = _CREATE_TABLE_RE.search(sql)
                    if ct_match:
                        tbl_name = _last_segment(_extract_name(ct_match))
                        topic_match = _KAFKA_TOPIC_PROP_RE.search(sql)
                        if topic_match:
                            table_topic_map[tbl_name] = topic_match.group(1)
                        else:
                            # Confluent Cloud Flink defaults topic name = table name
                            table_topic_map[tbl_name] = tbl_name

                logger.debug("Flink table→topic map: %s", table_topic_map)

                logger.info(
                    "Flink listed %d statements in environment %s",
                    len(statements),
                    self.environment_id,
                )

                # Second pass: process DML and CTAS statements
                for stmt in statements:
                    stmt_name = stmt.get("name", "")
                    phase = stmt.get("status", {}).get("phase", "")
                    if phase.upper() not in (
                        "RUNNING",
                        "COMPLETED",
                        "STOPPED",
                        "SUSPENDED",
                    ):
                        logger.debug(
                            "Flink skip %s: phase=%s",
                            stmt_name,
                            phase,
                        )
                        continue
                    sql = stmt.get("spec", {}).get("statement", "").strip()
                    # Strip leading SET commands (workspace UI prepends them)
                    sql = re.sub(
                        r"^(\s*SET\s+'[^']*'\s+'[^']*'\s*;\s*)+",
                        "",
                        sql,
                        flags=re.IGNORECASE,
                    ).strip()
                    # Skip pure DDL (CREATE TABLE without AS SELECT)
                    is_create = _CREATE_TABLE_RE.match(sql)
                    is_ctas = _CTAS_RE.search(sql)
                    if is_create and not is_ctas:
                        logger.debug("Flink skip %s: pure DDL", stmt_name)
                        continue
                    # Skip non-lineage statements (SELECT, SHOW, DESC, etc.)
                    sql_upper = sql.upper().lstrip()
                    if not (sql_upper.startswith("INSERT") or sql_upper.startswith("CREATE TABLE")):
                        logger.debug(
                            "Flink skip %s: not INSERT/CREATE (%s…)",
                            stmt_name,
                            sql_upper[:30],
                        )
                        continue
                    s_nodes, s_edges = self._process_statement(stmt, table_topic_map)
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
        self,
        stmt: dict[str, Any],
        table_topic_map: dict[str, str] | None = None,
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

        source_tables, sink_tables = self._parse_flink_sql(sql)
        topic_map = table_topic_map or {}

        # Resolve table names to Kafka topic names
        source_topics = {topic_map.get(t, t) for t in source_tables}
        sink_topics = {topic_map.get(t, t) for t in sink_tables}

        logger.debug(
            "Flink statement %s: sources=%s sinks=%s (table→topic resolved)",
            name,
            source_topics,
            sink_topics,
        )

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
        """Parse a Flink SQL statement and return ``(source_tables, sink_tables)``.

        Handles:
        - ``INSERT INTO <sink> SELECT ... FROM <source>``
        - ``CREATE TABLE <sink> AS SELECT ... FROM <source>`` (CTAS)

        Returns Flink table names (not Kafka topic names). The caller is
        responsible for resolving table names to topic names via the
        table→topic map built from CREATE TABLE DDLs.
        """
        sources: set[str] = set()
        sinks: set[str] = set()

        # INSERT INTO → sink.
        for m in _INSERT_INTO_RE.finditer(sql):
            sinks.add(_last_segment(_extract_name(m)))

        # CREATE TABLE ... AS SELECT → sink (CTAS).
        for m in _CTAS_RE.finditer(sql):
            sinks.add(_last_segment(_extract_name(m)))

        # FROM / JOIN → source.
        for m in _FROM_RE.finditer(sql):
            sources.add(_last_segment(_extract_name(m)))
        for m in _JOIN_RE.finditer(sql):
            sources.add(_last_segment(_extract_name(m)))

        # Flink windowing: TUMBLE(TABLE <name>, ...) etc.
        for m in _WINDOW_TABLE_RE.finditer(sql):
            sources.add(_last_segment(_extract_name(m)))

        # Remove sinks from sources (a table appearing in both is primarily a sink).
        sources -= sinks

        # Filter out SQL noise keywords and windowing function names.
        noise = {
            "select",
            "where",
            "group",
            "having",
            "order",
            "limit",
            "values",
            "set",
            "tumble",
            "hop",
            "cumulate",
            "session",
        }
        sources = {s for s in sources if s.lower() not in noise and "(" not in s}
        sinks = {s for s in sinks if s.lower() not in noise and "(" not in s}

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
