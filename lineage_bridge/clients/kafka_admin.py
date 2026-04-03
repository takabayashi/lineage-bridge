"""Kafka Admin REST v3 client — extracts topics and consumer groups."""

from __future__ import annotations

import logging
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

# Prefixes that mark internal/system topics to skip.
_INTERNAL_PREFIXES = ("_", "confluent")


class KafkaAdminClient(ConfluentClient):
    """Extracts topic and consumer-group lineage from the Kafka REST v3 API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        cluster_id: str,
        environment_id: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.cluster_id = cluster_id
        self.environment_id = environment_id

    # ── helpers ─────────────────────────────────────────────────────────

    def _topic_node_id(self, topic_name: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic_name}"

    def _group_node_id(self, group_id: str) -> str:
        return f"confluent:consumer_group:{self.environment_id}:{group_id}"

    @staticmethod
    def _is_internal(topic_name: str) -> bool:
        return any(topic_name.startswith(p) for p in _INTERNAL_PREFIXES)

    def _topic_url(self, topic_name: str) -> str:
        return (
            f"https://confluent.cloud/environments/{self.environment_id}"
            f"/clusters/{self.cluster_id}/topics/{topic_name}"
        )

    # ── extraction ──────────────────────────────────────────────────────

    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Implement :class:`LineageExtractor` protocol."""
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []

        # 1. List topics ------------------------------------------------
        topics = await self._list_topics()
        topic_names: set[str] = set()
        for t in topics:
            name: str = t["topic_name"]
            if self._is_internal(name):
                logger.debug("Skipping internal topic %s", name)
                continue
            topic_names.add(name)
            nodes.append(
                LineageNode(
                    node_id=self._topic_node_id(name),
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.KAFKA_TOPIC,
                    qualified_name=name,
                    display_name=name,
                    environment_id=self.environment_id,
                    cluster_id=self.cluster_id,
                    url=self._topic_url(name),
                    attributes={
                        "partitions_count": t.get("partitions_count"),
                        "replication_factor": t.get("replication_factor"),
                        "is_internal": t.get("is_internal", False),
                    },
                )
            )

        # 2. List consumer groups & lag ---------------------------------
        groups = await self._list_consumer_groups()
        for g in groups:
            gid: str = g["consumer_group_id"]
            nodes.append(
                LineageNode(
                    node_id=self._group_node_id(gid),
                    system=SystemType.CONFLUENT,
                    node_type=NodeType.CONSUMER_GROUP,
                    qualified_name=gid,
                    display_name=gid,
                    environment_id=self.environment_id,
                    cluster_id=self.cluster_id,
                    attributes={
                        "state": g.get("state"),
                        "is_simple": g.get("is_simple"),
                    },
                )
            )

            # Fetch lag to discover group→topic edges
            try:
                lag_items = await self._get_consumer_lag(gid)
            except Exception:
                logger.warning("Failed to fetch lag for group %s", gid, exc_info=True)
                lag_items = []

            seen_topics: set[str] = set()
            for lag in lag_items:
                tname = lag.get("topic_name", "")
                if tname in seen_topics or tname not in topic_names:
                    continue
                seen_topics.add(tname)
                edges.append(
                    LineageEdge(
                        src_id=self._group_node_id(gid),
                        dst_id=self._topic_node_id(tname),
                        edge_type=EdgeType.MEMBER_OF,
                        attributes={
                            "max_lag": max(
                                (
                                    item.get("lag", 0)
                                    for item in lag_items
                                    if item.get("topic_name") == tname
                                ),
                                default=0,
                            ),
                        },
                    )
                )

        logger.info(
            "KafkaAdmin extracted %d nodes, %d edges from cluster %s",
            len(nodes),
            len(edges),
            self.cluster_id,
        )
        return nodes, edges

    # ── raw API calls ───────────────────────────────────────────────────

    async def _list_topics(self) -> list[dict[str, Any]]:
        path = f"/kafka/v3/clusters/{self.cluster_id}/topics"
        return await self.paginate(path)

    async def _list_consumer_groups(self) -> list[dict[str, Any]]:
        path = f"/kafka/v3/clusters/{self.cluster_id}/consumer-groups"
        return await self.paginate(path)

    async def _get_consumer_lag(self, group_id: str) -> list[dict[str, Any]]:
        path = f"/kafka/v3/clusters/{self.cluster_id}/consumer-groups/{group_id}/lags"
        data = await self.get(path)
        return data.get("data", [])
