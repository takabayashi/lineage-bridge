# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.watcher.engine."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lineage_bridge.models.audit_event import AuditEvent
from lineage_bridge.watcher.engine import WatcherEngine, WatcherState


def _make_settings(**overrides):
    """Create a mock Settings object."""
    defaults = {
        "confluent_cloud_api_key": "cloud-key",
        "confluent_cloud_api_secret": "cloud-secret",
        "audit_log_bootstrap_servers": None,
        "audit_log_api_key": None,
        "audit_log_api_secret": None,
    }
    defaults.update(overrides)
    settings = MagicMock()
    for k, v in defaults.items():
        setattr(settings, k, v)
    return settings


def _make_params(**overrides):
    """Create minimal extraction params."""
    defaults = {
        "env_ids": ["env-test"],
        "cluster_ids": ["lkc-test"],
        "enable_connect": True,
        "enable_ksqldb": True,
        "enable_flink": True,
        "enable_schema_registry": True,
        "enable_stream_catalog": False,
        "enable_tableflow": True,
        "enable_enrichment": True,
        "push_uc": False,
        "push_glue": False,
    }
    defaults.update(overrides)
    return defaults


def _make_event(method: str = "poll.topics.Changed", **kwargs) -> AuditEvent:
    """Create a test AuditEvent."""
    defaults = {
        "id": "ae-test",
        "time": datetime.now(UTC),
        "method_name": method,
        "resource_name": "topics",
        "principal": "lineage-bridge-poller",
        "environment_id": "env-test",
        "cluster_id": "lkc-test",
        "raw": {},
    }
    defaults.update(kwargs)
    return AuditEvent(**defaults)


class TestWatcherEngineInit:
    def test_initial_state_is_stopped(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )
        assert engine.state == WatcherState.STOPPED
        assert len(engine.event_feed) == 0
        assert len(engine.extraction_history) == 0

    def test_cooldown_remaining_when_stopped(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )
        assert engine.cooldown_remaining == 0.0

    def test_is_running_when_stopped(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )
        assert engine.is_running is False


class TestWatcherEngineStartStop:
    def test_start_requires_env_ids(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(env_ids=[]),
        )
        with pytest.raises(ValueError, match="env_ids"):
            engine.start()

    def test_start_sets_watching_state(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )

        def _block_until_stop():
            engine._stop_event.wait()

        with patch.object(engine, "_run_loop", side_effect=_block_until_stop):
            engine.start()
            assert engine.state == WatcherState.WATCHING
            assert engine.is_running
            engine.stop()

    def test_stop_sets_stopped_state(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )

        def _block_until_stop():
            engine._stop_event.wait()

        with patch.object(engine, "_run_loop", side_effect=_block_until_stop):
            engine.start()
            engine.stop()
            assert engine.state == WatcherState.STOPPED
            assert engine.is_running is False

    def test_start_twice_is_noop(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )

        def _block_until_stop():
            engine._stop_event.wait()

        with patch.object(engine, "_run_loop", side_effect=_block_until_stop):
            engine.start()
            engine.start()  # should not raise
            engine.stop()


class TestWatcherEngineLoop:
    def test_event_triggers_cooldown(self):
        """When changes are detected, state transitions to COOLDOWN."""
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
            cooldown_seconds=10.0,
            poll_interval=0.01,
        )
        event = _make_event()

        mock_poller = MagicMock()
        call_count = 0

        async def poll_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [event]
            # Stop after observing the state change
            engine._stop_event.set()
            return []

        mock_poller.poll = AsyncMock(side_effect=poll_side_effect)

        with patch(
            "lineage_bridge.clients.audit_consumer.ChangePoller",
            return_value=mock_poller,
        ):
            engine.state = WatcherState.WATCHING
            engine._run_loop()

        assert len(engine.event_feed) == 1
        assert engine.event_feed[0].method_name == "poll.topics.Changed"

    def test_cooldown_expires_triggers_extraction(self):
        """After cooldown expires, extraction is triggered."""
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
            cooldown_seconds=0.05,
            poll_interval=0.01,
        )

        mock_poller = MagicMock()
        call_count = 0

        async def poll_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [_make_event()]
            if call_count == 2:
                # Wait for cooldown to expire
                time.sleep(0.1)
                return []
            # Stop after extraction
            engine._stop_event.set()
            return []

        mock_poller.poll = AsyncMock(side_effect=poll_side_effect)

        mock_graph = MagicMock()
        mock_graph.node_count = 5
        mock_graph.edge_count = 3

        with (
            patch(
                "lineage_bridge.clients.audit_consumer.ChangePoller",
                return_value=mock_poller,
            ),
            patch.object(
                engine,
                "_do_extraction",
                new_callable=AsyncMock,
                return_value=mock_graph,
            ),
        ):
            engine.state = WatcherState.WATCHING
            engine._run_loop()

        assert len(engine.extraction_history) == 1
        record = engine.extraction_history[0]
        assert record.node_count == 5
        assert record.edge_count == 3
        assert record.error is None
        assert len(record.trigger_events) == 1

    def test_multiple_events_batch_into_one_extraction(self):
        """Multiple change events during cooldown result in a single extraction."""
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
            cooldown_seconds=0.05,
            poll_interval=0.01,
        )

        mock_poller = MagicMock()
        call_count = 0

        async def poll_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    _make_event(id="ae-1", method="poll.topics.Changed"),
                    _make_event(id="ae-2", method="poll.connectors.Changed"),
                ]
            if call_count == 2:
                return [_make_event(id="ae-3", method="poll.flink_statements.Changed")]
            if call_count == 3:
                time.sleep(0.1)
                return []
            engine._stop_event.set()
            return []

        mock_poller.poll = AsyncMock(side_effect=poll_side_effect)

        mock_graph = MagicMock()
        mock_graph.node_count = 10
        mock_graph.edge_count = 8

        with (
            patch(
                "lineage_bridge.clients.audit_consumer.ChangePoller",
                return_value=mock_poller,
            ),
            patch.object(
                engine,
                "_do_extraction",
                new_callable=AsyncMock,
                return_value=mock_graph,
            ),
        ):
            engine.state = WatcherState.WATCHING
            engine._run_loop()

        assert len(engine.event_feed) == 3
        assert len(engine.extraction_history) == 1
        assert len(engine.extraction_history[0].trigger_events) == 3

    def test_extraction_error_recorded(self):
        """If extraction fails, the error is recorded."""
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
            cooldown_seconds=0.05,
            poll_interval=0.01,
        )

        mock_poller = MagicMock()
        call_count = 0

        async def poll_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [_make_event()]
            if call_count == 2:
                time.sleep(0.1)
                return []
            engine._stop_event.set()
            return []

        mock_poller.poll = AsyncMock(side_effect=poll_side_effect)

        with (
            patch(
                "lineage_bridge.clients.audit_consumer.ChangePoller",
                return_value=mock_poller,
            ),
            patch.object(
                engine,
                "_do_extraction",
                new_callable=AsyncMock,
                side_effect=RuntimeError("API unreachable"),
            ),
        ):
            engine.state = WatcherState.WATCHING
            engine._run_loop()

        assert len(engine.extraction_history) == 1
        assert engine.extraction_history[0].error == "API unreachable"


class TestWatcherEngineAuditLogMode:
    def test_use_audit_log_when_configured(self):
        """Engine uses audit log when all three settings are provided."""
        settings = _make_settings(
            audit_log_bootstrap_servers="bs:9092",
            audit_log_api_key="key",
            audit_log_api_secret="secret",
        )
        engine = WatcherEngine(
            settings=settings,
            extraction_params=_make_params(),
        )
        assert engine._use_audit_log is True

    def test_use_polling_when_audit_log_not_configured(self):
        """Engine falls back to polling when audit log settings are missing."""
        settings = _make_settings(
            audit_log_bootstrap_servers=None,
            audit_log_api_key=None,
            audit_log_api_secret=None,
        )
        engine = WatcherEngine(
            settings=settings,
            extraction_params=_make_params(),
        )
        assert engine._use_audit_log is False

    def test_audit_log_loop_processes_events(self):
        """Audit log loop processes events and enters cooldown."""
        settings = _make_settings(
            audit_log_bootstrap_servers="bs:9092",
            audit_log_api_key="key",
            audit_log_api_secret="secret",
        )
        engine = WatcherEngine(
            settings=settings,
            extraction_params=_make_params(),
            cooldown_seconds=10.0,
        )

        event = _make_event(method="kafka.CreateTopics")
        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return event
            engine._stop_event.set()
            return None

        mock_consumer = MagicMock()
        mock_consumer.poll_one = MagicMock(side_effect=poll_side_effect)

        with patch(
            "lineage_bridge.clients.audit_consumer.AuditLogConsumer",
            return_value=mock_consumer,
        ):
            engine.state = WatcherState.WATCHING
            engine._run_audit_log_loop()

        assert len(engine.event_feed) == 1
        assert engine.event_feed[0].method_name == "kafka.CreateTopics"
        mock_consumer.close.assert_called_once()


class TestCooldownRemaining:
    def test_cooldown_remaining_during_cooldown(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
            cooldown_seconds=30.0,
        )
        engine.state = WatcherState.COOLDOWN
        engine._cooldown_deadline = time.monotonic() + 15.0
        remaining = engine.cooldown_remaining
        assert 14.0 < remaining <= 15.0

    def test_cooldown_remaining_after_expired(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(),
        )
        engine.state = WatcherState.COOLDOWN
        engine._cooldown_deadline = time.monotonic() - 1.0
        assert engine.cooldown_remaining == 0.0
