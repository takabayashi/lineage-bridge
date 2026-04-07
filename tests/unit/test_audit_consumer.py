# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.audit_consumer."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lineage_bridge.clients.audit_consumer import (
    AuditLogConsumer,
    ChangePoller,
    ClusterEndpoint,
    _hash_json,
    _Snapshot,
)


def _ep(cluster_id: str = "lkc-test") -> ClusterEndpoint:
    return ClusterEndpoint(
        cluster_id=cluster_id,
        rest_endpoint="https://pkc-test.us-west-2.aws.confluent.cloud:443",
        api_key="cluster-key",
        api_secret="cluster-secret",
    )


class TestHashJson:
    def test_stable_hash(self):
        """Same data always produces the same hash."""
        assert _hash_json(["a", "b"]) == _hash_json(["a", "b"])

    def test_different_data_different_hash(self):
        assert _hash_json(["a"]) != _hash_json(["b"])

    def test_order_independent_for_dicts(self):
        """Dict key order doesn't matter due to sort_keys."""
        assert _hash_json({"b": 1, "a": 2}) == _hash_json({"a": 2, "b": 1})


class TestSnapshot:
    def test_diff_no_changes(self):
        s1 = _Snapshot(topics="abc", connectors="def")
        s2 = _Snapshot(topics="abc", connectors="def")
        assert s1.diff(s2) == []

    def test_diff_topics_changed(self):
        s1 = _Snapshot(topics="abc")
        s2 = _Snapshot(topics="xyz")
        assert s1.diff(s2) == ["topics"]

    def test_diff_multiple_changes(self):
        s1 = _Snapshot(topics="a", connectors="b", flink_statements="c")
        s2 = _Snapshot(topics="x", connectors="b", flink_statements="y")
        changes = s1.diff(s2)
        assert "topics" in changes
        assert "flink_statements" in changes
        assert "connectors" not in changes

    def test_diff_all_changed(self):
        s1 = _Snapshot(topics="a", connectors="b", ksqldb_queries="c", flink_statements="d")
        s2 = _Snapshot(topics="w", connectors="x", ksqldb_queries="y", flink_statements="z")
        assert len(s1.diff(s2)) == 4


class TestChangePollerPoll:
    @pytest.mark.asyncio
    async def test_first_poll_returns_empty(self):
        """First poll establishes baseline, returns no events."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=_Snapshot(topics="abc"),
        ):
            events = await poller.poll()

        assert events == []
        assert poller._initialized

    @pytest.mark.asyncio
    async def test_no_changes_returns_empty(self):
        """Second poll with same snapshot returns no events."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(topics="abc")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=_Snapshot(topics="abc"),
        ):
            events = await poller.poll()

        assert events == []

    @pytest.mark.asyncio
    async def test_changes_return_events(self):
        """When snapshot differs, synthetic AuditEvents are returned."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(topics="abc", connectors="def")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=_Snapshot(topics="xyz", connectors="def"),
        ):
            events = await poller.poll()

        assert len(events) == 1
        assert events[0].method_name == "poll.topics.Changed"
        assert events[0].resource_name == "topics"
        assert events[0].environment_id == "env-test"
        assert events[0].cluster_id == "lkc-test"

    @pytest.mark.asyncio
    async def test_multiple_changes_return_multiple_events(self):
        """Multiple resource changes produce multiple events."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(topics="a", connectors="b")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=_Snapshot(topics="x", connectors="y"),
        ):
            events = await poller.poll()

        assert len(events) == 2
        methods = {e.method_name for e in events}
        assert "poll.topics.Changed" in methods
        assert "poll.connectors.Changed" in methods

    @pytest.mark.asyncio
    async def test_poll_failure_returns_empty(self):
        """If REST API call fails, returns empty list."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(topics="abc")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            side_effect=RuntimeError("network error"),
        ):
            events = await poller.poll()

        assert events == []

    @pytest.mark.asyncio
    async def test_snapshot_updates_after_changes(self):
        """Last snapshot is updated after detecting changes."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[_ep()],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(topics="old")
        new_snapshot = _Snapshot(topics="new")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=new_snapshot,
        ):
            await poller.poll()

        assert poller._last_snapshot.topics == "new"

    @pytest.mark.asyncio
    async def test_no_cluster_endpoints_sets_cluster_id_none(self):
        """Without cluster_endpoints, events have cluster_id=None."""
        poller = ChangePoller(
            cloud_api_key="key",
            cloud_api_secret="secret",
            environment_id="env-test",
            cluster_endpoints=[],
        )
        poller._initialized = True
        poller._last_snapshot = _Snapshot(ksqldb_queries="old")

        with patch.object(
            poller, "_take_snapshot", new_callable=AsyncMock,
            return_value=_Snapshot(ksqldb_queries="new"),
        ):
            events = await poller.poll()

        assert len(events) == 1
        assert events[0].cluster_id is None


# ═══════════════════════════════════════════════════════════════════════════
#  AuditLogConsumer tests
# ═══════════════════════════════════════════════════════════════════════════

_CREATE_TOPIC_EVENT = {
    "id": "ae-001",
    "source": "crn://confluent.cloud/organization=org-abc/environment=env-2yzd0o"
    "/kafka=lkc-jkn588",
    "type": "io.confluent.kafka.server/authorization",
    "time": "2026-04-06T10:30:00Z",
    "data": {
        "serviceName": "crn://confluent.cloud",
        "methodName": "kafka.CreateTopics",
        "resourceName": "crn://confluent.cloud/organization=org-abc"
        "/environment=env-2yzd0o/kafka=lkc-jkn588/topic=test_topic",
        "authenticationInfo": {"principal": "User:sa-demo"},
    },
}


class TestAuditLogConsumer:
    def test_poll_one_returns_event_for_relevant_message(self):
        """A lineage-relevant audit log message is parsed and returned."""
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps(_CREATE_TOPIC_EVENT).encode()

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_msg

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            event = consumer.poll_one()

        assert event is not None
        assert event.method_name == "kafka.CreateTopics"
        assert event.environment_id == "env-2yzd0o"
        assert event.cluster_id == "lkc-jkn588"

    def test_poll_one_returns_none_on_timeout(self):
        """Returns None when no message is available."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            assert consumer.poll_one() is None

    def test_poll_one_skips_non_relevant_events(self):
        """Non-lineage events (e.g., kafka.Fetch) are filtered out."""
        fetch_event = dict(_CREATE_TOPIC_EVENT)
        fetch_event = {**_CREATE_TOPIC_EVENT, "data": {
            **_CREATE_TOPIC_EVENT["data"],
            "methodName": "kafka.Fetch",
        }}

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = json.dumps(fetch_event).encode()

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_msg

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            assert consumer.poll_one() is None

    def test_poll_one_handles_consumer_error(self):
        """Returns None when the consumer message has an error."""
        mock_msg = MagicMock()
        mock_msg.error.return_value = MagicMock()  # truthy = error

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_msg

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            assert consumer.poll_one() is None

    def test_poll_one_handles_malformed_json(self):
        """Returns None for messages that aren't valid JSON."""
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"not json"

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = mock_msg

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            assert consumer.poll_one() is None

    def test_close_delegates_to_consumer(self):
        """close() calls the underlying consumer's close()."""
        mock_consumer = MagicMock()

        with patch.object(
            AuditLogConsumer, "_create_consumer", return_value=mock_consumer
        ):
            consumer = AuditLogConsumer("bs:9092", "key", "secret")
            consumer.close()

        mock_consumer.close.assert_called_once()
