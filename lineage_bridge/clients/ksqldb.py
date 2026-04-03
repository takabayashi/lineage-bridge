"""ksqlDB client — extracts persistent query lineage."""

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

# Regex helpers for parsing ksqlDB SQL source/sink topics from statements.
_FROM_RE = re.compile(r"\bFROM\s+([`\"]?[\w.]+[`\"]?)", re.IGNORECASE)
_JOIN_RE = re.compile(r"\bJOIN\s+([`\"]?[\w.]+[`\"]?)", re.IGNORECASE)
_INTO_RE = re.compile(r"\bINTO\s+([`\"]?[\w.]+[`\"]?)", re.IGNORECASE)


def _strip_quotes(name: str) -> str:
    return name.strip('`"')


class KsqlDBClient:
    """Extracts ksqlDB persistent-query lineage.

    Uses two API surfaces:
    1. Cloud API (``api.confluent.cloud``) to discover ksqlDB clusters.
    2. Data-plane REST API (per-cluster endpoint) for query metadata.
    """

    def __init__(
        self,
        cloud_api_key: str,
        cloud_api_secret: str,
        environment_id: str,
        *,
        ksqldb_api_key: str | None = None,
        ksqldb_api_secret: str | None = None,
        cloud_base_url: str = "https://api.confluent.cloud",
        timeout: float = 30.0,
    ) -> None:
        self.environment_id = environment_id
        self._cloud = ConfluentClient(
            cloud_base_url, cloud_api_key, cloud_api_secret, timeout=timeout
        )
        # Data-plane credentials (may differ from cloud creds).
        self._dp_key = ksqldb_api_key or cloud_api_key
        self._dp_secret = ksqldb_api_secret or cloud_api_secret
        self._timeout = timeout

    async def close(self) -> None:
        await self._cloud.close()

    async def __aenter__(self) -> KsqlDBClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ── helpers ─────────────────────────────────────────────────────────

    def _query_node_id(self, query_id: str) -> str:
        return f"confluent:ksqldb_query:{self.environment_id}:{query_id}"

    def _topic_node_id(self, topic: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic}"

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        clusters = await self._discover_clusters()
        if not clusters:
            logger.info("No ksqlDB clusters found in environment %s", self.environment_id)
            return nodes, edges

        for cluster in clusters:
            endpoint = cluster.get("status", {}).get("http_endpoint") or cluster.get(
                "spec", {}
            ).get("http_endpoint")
            if not endpoint:
                logger.warning("No HTTP endpoint for ksqlDB cluster %s", cluster.get("id"))
                continue

            kafka_cluster_id = cluster.get("spec", {}).get("kafka_cluster", {}).get("id")

            try:
                dp_client = ConfluentClient(
                    endpoint, self._dp_key, self._dp_secret, timeout=self._timeout
                )
                async with dp_client:
                    n, e = await self._extract_from_cluster(
                        dp_client, cluster.get("id", ""), kafka_cluster_id
                    )
                    nodes.extend(n)
                    edges.extend(e)
            except Exception:
                logger.warning(
                    "Failed to extract from ksqlDB cluster %s",
                    cluster.get("id"),
                    exc_info=True,
                )

        logger.info(
            "ksqlDB extracted %d nodes, %d edges from environment %s",
            len(nodes),
            len(edges),
            self.environment_id,
        )
        return nodes, edges

    async def _extract_from_cluster(
        self,
        dp: ConfluentClient,
        ksqldb_cluster_id: str,
        kafka_cluster_id: str | None,
    ) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        queries = await self._show_queries(dp)
        for q in queries:
            qid = q.get("id", "")
            sql = q.get("queryString", "")
            state = q.get("state", "")
            sink_topics: list[str] = q.get("sinkKafkaTopics", [])

            nodes.append(
                LineageNode(
                    node_id=self._query_node_id(qid),
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.KSQLDB_QUERY,
                    qualified_name=qid,
                    display_name=qid,
                    environment_id=self.environment_id,
                    cluster_id=kafka_cluster_id,
                    attributes={
                        "ksqldb_cluster_id": ksqldb_cluster_id,
                        "state": state,
                        "sql": sql,
                    },
                )
            )

            # Parse SQL for source topics.
            source_names = self._parse_source_names(sql)
            for sname in source_names:
                # Streams/tables map 1:1 to Kafka topics (lowercase).
                topic = sname.lower()
                tid = self._topic_node_id(topic)
                # Placeholder topic node.
                nodes.append(
                    LineageNode(
                        node_id=tid,
                        system=SystemType.CONFLUENT,
                        node_type=NodeType.KAFKA_TOPIC,
                        qualified_name=topic,
                        display_name=topic,
                        environment_id=self.environment_id,
                        cluster_id=kafka_cluster_id,
                    )
                )
                edges.append(
                    LineageEdge(
                        src_id=tid,
                        dst_id=self._query_node_id(qid),
                        edge_type=EdgeType.CONSUMES,
                        confidence=0.8,
                    )
                )

            # Sink topics are explicit in SHOW QUERIES response.
            for stopic in sink_topics:
                tid = self._topic_node_id(stopic)
                nodes.append(
                    LineageNode(
                        node_id=tid,
                        system=SystemType.CONFLUENT,
                        node_type=NodeType.KAFKA_TOPIC,
                        qualified_name=stopic,
                        display_name=stopic,
                        environment_id=self.environment_id,
                        cluster_id=kafka_cluster_id,
                    )
                )
                edges.append(
                    LineageEdge(
                        src_id=self._query_node_id(qid),
                        dst_id=tid,
                        edge_type=EdgeType.PRODUCES,
                    )
                )

        return nodes, edges

    # ── SQL parsing ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_source_names(sql: str) -> list[str]:
        """Extract source stream/table names from a ksqlDB SQL statement."""
        names: list[str] = []
        for match in _FROM_RE.finditer(sql):
            names.append(_strip_quotes(match.group(1)))
        for match in _JOIN_RE.finditer(sql):
            names.append(_strip_quotes(match.group(1)))
        # Deduplicate while preserving order.
        seen: set[str] = set()
        result: list[str] = []
        for n in names:
            if n.lower() not in seen:
                seen.add(n.lower())
                result.append(n)
        return result

    # ── raw API calls ───────────────────────────────────────────────────

    async def _discover_clusters(self) -> list[dict[str, Any]]:
        return await self._cloud.paginate(
            "/ksqldbcm/v2/clusters",
            params={"environment": self.environment_id},
        )

    async def _show_queries(self, dp: ConfluentClient) -> list[dict[str, Any]]:
        resp = await dp.post("/ksql", json_body={"ksql": "SHOW QUERIES;", "streamsProperties": {}})
        # Response is a list; first element contains the queries.
        if isinstance(resp, list) and resp:
            return resp[0].get("queries", [])
        if isinstance(resp, dict):
            return resp.get("queries", [])
        return []
