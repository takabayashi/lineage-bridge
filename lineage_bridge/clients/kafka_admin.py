"""Kafka Admin REST v3 client — extracts topics and consumer groups."""

from __future__ import annotations

import asyncio
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


def _list_offsets_via_protocol(
    bootstrap_servers: str,
    api_key: str,
    api_secret: str,
    group_id: str,
) -> set[str]:
    """Use the Kafka protocol to discover which topics a consumer group has offsets for.

    Returns a set of topic names. Runs synchronously (Kafka protocol is blocking).
    """
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        })
        futures = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)]
        )
        for gid, fut in futures.items():
            result = fut.result(timeout=10)
            return {tp.topic for tp in result.topic_partitions if tp.topic}
    except Exception:
        logger.debug(
            "Kafka protocol offset lookup failed for group %s",
            group_id, exc_info=True,
        )
    return set()


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
        bootstrap_servers: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.cluster_id = cluster_id
        self.environment_id = environment_id
        self._bootstrap_servers = bootstrap_servers
        self._api_key = api_key
        self._api_secret = api_secret

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
                logger.info(
                    "Lag endpoint returned %d items for group %s",
                    len(lag_items), gid,
                )
            except Exception as exc:
                logger.info(
                    "Lag endpoint failed for group %s: %s", gid, exc,
                )
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

            # Fallback: if lag returned nothing, use Kafka protocol to
            # discover committed offsets (works when lag endpoint returns 404).
            if not seen_topics and self._bootstrap_servers:
                logger.info(
                    "Falling back to Kafka protocol for group %s "
                    "(bootstrap=%s)", gid, self._bootstrap_servers,
                )
                offset_topics = await asyncio.to_thread(
                    _list_offsets_via_protocol,
                    self._bootstrap_servers,
                    self._api_key,
                    self._api_secret,
                    gid,
                )
                logger.info(
                    "Protocol fallback for group %s returned: %s",
                    gid, offset_topics,
                )
                for tname in offset_topics:
                    if tname not in topic_names:
                        logger.info(
                            "Skipping offset topic %s (not in topic_names)",
                            tname,
                        )
                        continue
                    edges.append(
                        LineageEdge(
                            src_id=self._group_node_id(gid),
                            dst_id=self._topic_node_id(tname),
                            edge_type=EdgeType.MEMBER_OF,
                        )
                    )
                    logger.info(
                        "Added MEMBER_OF edge: %s -> %s", gid, tname,
                    )
            elif not seen_topics:
                logger.info(
                    "No lag data and no bootstrap_servers for group %s "
                    "— cannot discover topic membership", gid,
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
