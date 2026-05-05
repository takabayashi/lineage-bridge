# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.watcher.engine."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lineage_bridge.models.audit_event import AuditEvent
from lineage_bridge.watcher.engine import WatcherEngine, WatcherMode, WatcherState


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
        "push_providers": [],
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

    def test_explicit_polling_mode_overrides_audit_creds(self):
        """mode=POLLING wins over audit-log creds present in Settings.

        Regression: a stale LINEAGE_BRIDGE_AUDIT_LOG_* in .env used to
        silently force audit-log mode even when the UI toggle was off.
        The user could not actually choose REST polling unless they also
        unset the env vars.
        """
        settings = _make_settings(
            audit_log_bootstrap_servers="bs:9092",
            audit_log_api_key="key",
            audit_log_api_secret="secret",
        )
        engine = WatcherEngine(
            settings=settings,
            extraction_params=_make_params(),
            mode=WatcherMode.POLLING,
        )
        assert engine._use_audit_log is False

    def test_explicit_audit_mode_without_creds_raises_on_start(self):
        """mode=AUDIT without creds is a configuration error, not a silent
        downgrade — silently switching to polling would mask user mistakes.
        """
        settings = _make_settings(
            audit_log_bootstrap_servers=None,
            audit_log_api_key=None,
            audit_log_api_secret=None,
        )
        engine = WatcherEngine(
            settings=settings,
            extraction_params=_make_params(),
            mode=WatcherMode.AUDIT,
        )
        with pytest.raises(ValueError, match="Audit log mode requires"):
            engine.start()

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


class TestDoExtractionPushDispatch:
    """Verify the watcher's _do_extraction routes pushes through push_service.

    Phase A regression guard: previously the engine imported deleted
    `run_glue_push` / `run_lineage_push` functions and crashed at the first
    triggered extraction whenever the user enabled a push toggle. This class
    locks in that the new dispatch loop calls run_push once per provider in
    `params["push_providers"]` and that one provider's failure doesn't drop
    the others.
    """

    @pytest.mark.asyncio
    async def test_no_push_when_providers_list_empty(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(push_providers=[]),
        )
        with (
            patch(
                "lineage_bridge.extractors.orchestrator.run_extraction",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "lineage_bridge.services.push_service.run_push",
                new_callable=AsyncMock,
            ) as mock_run_push,
        ):
            await engine._do_extraction()
        mock_run_push.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_dispatches_once_per_provider(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(
                push_providers=["databricks_uc", "aws_glue"],
            ),
        )
        with (
            patch(
                "lineage_bridge.extractors.orchestrator.run_extraction",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "lineage_bridge.services.push_service.run_push",
                new_callable=AsyncMock,
            ) as mock_run_push,
        ):
            await engine._do_extraction()

        assert mock_run_push.call_count == 2
        called_providers = [call.args[0].provider for call in mock_run_push.call_args_list]
        assert called_providers == ["databricks_uc", "aws_glue"]

    @pytest.mark.asyncio
    async def test_one_provider_failure_does_not_block_others(self):
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(
                push_providers=["databricks_uc", "aws_glue"],
            ),
        )

        async def push_side_effect(req, *_args, **_kwargs):
            if req.provider == "databricks_uc":
                raise RuntimeError("UC permission denied")
            return MagicMock()

        with (
            patch(
                "lineage_bridge.extractors.orchestrator.run_extraction",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ),
            patch(
                "lineage_bridge.services.push_service.run_push",
                new_callable=AsyncMock,
                side_effect=push_side_effect,
            ) as mock_run_push,
        ):
            await engine._do_extraction()

        # Both providers must have been attempted even though UC raised.
        assert mock_run_push.call_count == 2

    @pytest.mark.asyncio
    async def test_extraction_params_pass_through_sr_and_flink_credentials(self):
        """Per-env SR / Flink credentials reach run_extraction.

        Without forwarding, the watcher would only see whatever Settings
        was constructed from .env — the sidebar's Manage Credentials
        dialog stores per-env overrides in last_extraction_params, and
        the watcher must thread them through to match the foreground
        Extract behaviour.
        """
        engine = WatcherEngine(
            settings=_make_settings(),
            extraction_params=_make_params(
                sr_credentials={"env-test": {"endpoint": "https://psrc-x", "api_key": "K"}},
                flink_credentials={"env-test": {"api_key": "FK", "api_secret": "FS"}},
                sr_endpoints={"env-test": "https://psrc-x"},
            ),
        )
        with (
            patch(
                "lineage_bridge.extractors.orchestrator.run_extraction",
                new_callable=AsyncMock,
                return_value=MagicMock(),
            ) as mock_run_extraction,
            patch(
                "lineage_bridge.services.push_service.run_push",
                new_callable=AsyncMock,
            ),
        ):
            await engine._do_extraction()
        kwargs = mock_run_extraction.call_args.kwargs
        assert kwargs["sr_credentials"] == {
            "env-test": {"endpoint": "https://psrc-x", "api_key": "K"}
        }
        assert kwargs["flink_credentials"] == {"env-test": {"api_key": "FK", "api_secret": "FS"}}
        assert kwargs["sr_endpoints"] == {"env-test": "https://psrc-x"}

    @pytest.mark.asyncio
    async def test_orchestrator_no_longer_imports_deleted_push_wrappers(self):
        """Static guard: the deleted run_glue_push/run_lineage_push must NOT
        be imported by _do_extraction. If a future refactor reintroduces
        either name, this test fails with AttributeError on the patch lookup
        and points at the regression — far cheaper than a runtime ImportError
        triggered only when a user flips a push toggle.
        """
        from lineage_bridge.extractors import orchestrator

        assert not hasattr(orchestrator, "run_glue_push")
        assert not hasattr(orchestrator, "run_lineage_push")
