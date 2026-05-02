# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pure-logic tests for WatcherService — no threading, no I/O.

The service's `start` / `poll_once` / `maybe_extract` / `stop` lifecycle is
tested with a fake poller injected on `service._poller`, which lets us
assert state-machine transitions without standing up real Confluent clients.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from lineage_bridge.services.requests import ExtractionRequest
from lineage_bridge.services.watcher_models import (
    WatcherConfig,
    WatcherEvent,
    WatcherMode,
    WatcherState,
)
from lineage_bridge.services.watcher_service import WatcherService


def _fake_event(name: str = "kafka.CreateTopics") -> WatcherEvent:
    from datetime import UTC, datetime

    return WatcherEvent(
        id=f"e-{name}",
        time=datetime.now(UTC),
        method_name=name,
        resource_name="topic-x",
        principal="u:test",
        raw={},
    )


def _service(
    *,
    cooldown: float = 30.0,
    poll_interval: float = 5.0,
    push_uc: bool = False,
) -> WatcherService:
    config = WatcherConfig(
        mode=WatcherMode.REST_POLLING,
        poll_interval_seconds=poll_interval,
        cooldown_seconds=cooldown,
        extraction=ExtractionRequest(environment_ids=["env-1"]),
        push_databricks_uc=push_uc,
    )
    settings = MagicMock()
    return WatcherService("w-1", config, settings)


# ── snapshot ────────────────────────────────────────────────────────────


def test_snapshot_on_fresh_service_is_stopped():
    svc = _service()
    snap = svc.snapshot()
    assert snap.state == WatcherState.STOPPED
    assert snap.poll_count == 0
    assert snap.event_count == 0


# ── start / stop ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_without_env_ids_raises():
    config = WatcherConfig(extraction=ExtractionRequest(environment_ids=[]))
    svc = WatcherService("w-2", config, MagicMock())
    with pytest.raises(ValueError, match="environment_ids"):
        await svc.start()


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    """Stop on an unstarted service must not raise — runner relies on it."""
    svc = _service()
    await svc.stop()
    await svc.stop()


# ── poll_once → cooldown transition ─────────────────────────────────────


@pytest.mark.asyncio
async def test_poll_with_no_events_stays_in_state(monkeypatch):
    svc = _service()
    fake_poller = MagicMock()
    fake_poller.poll = AsyncMock(return_value=[])
    svc._poller = fake_poller
    svc._state = WatcherState.WATCHING

    events = await svc.poll_once()

    assert events == []
    assert svc.snapshot().poll_count == 1
    assert svc.state == WatcherState.WATCHING


@pytest.mark.asyncio
async def test_poll_with_events_enters_cooldown_and_buffers():
    svc = _service(cooldown=60)
    fake_poller = MagicMock()
    fake_poller.poll = AsyncMock(return_value=[_fake_event(), _fake_event()])
    svc._poller = fake_poller
    svc._state = WatcherState.WATCHING

    events = await svc.poll_once()

    assert len(events) == 2
    snap = svc.snapshot()
    assert snap.event_count == 2
    assert svc.state == WatcherState.COOLDOWN
    assert snap.cooldown_remaining_seconds > 50  # within ~10s of 60


@pytest.mark.asyncio
async def test_poll_failure_records_error_and_returns_empty():
    svc = _service()
    fake_poller = MagicMock()
    fake_poller.poll = AsyncMock(side_effect=RuntimeError("network down"))
    svc._poller = fake_poller
    svc._state = WatcherState.WATCHING

    events = await svc.poll_once()

    assert events == []
    snap = svc.snapshot()
    assert snap.last_error is not None
    assert "network down" in snap.last_error
    # Failed polls don't advance poll_count (the service treats them as a no-op).
    assert snap.poll_count == 0


# ── maybe_extract ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_maybe_extract_does_nothing_when_not_in_cooldown():
    svc = _service()
    svc._state = WatcherState.WATCHING
    record = await svc.maybe_extract()
    assert record is None


@pytest.mark.asyncio
async def test_maybe_extract_does_nothing_when_cooldown_pending():
    svc = _service(cooldown=60)
    svc._state = WatcherState.COOLDOWN
    svc._cooldown_deadline = time.monotonic() + 60
    assert await svc.maybe_extract() is None


@pytest.mark.asyncio
async def test_maybe_extract_runs_when_cooldown_elapsed(monkeypatch):
    """Cooldown deadline in the past + COOLDOWN state → extraction runs."""
    svc = _service(cooldown=0.001)
    svc._state = WatcherState.COOLDOWN
    svc._cooldown_deadline = time.monotonic() - 1
    svc._pending_events = [_fake_event(), _fake_event(), _fake_event()]

    fake_graph = MagicMock(node_count=42, edge_count=13)
    monkeypatch.setattr(
        "lineage_bridge.services.extraction_service.run_extraction",
        AsyncMock(return_value=fake_graph),
    )
    record = await svc.maybe_extract()

    assert record is not None
    assert record.trigger_event_count == 3
    assert record.node_count == 42
    assert record.edge_count == 13
    assert record.error is None
    assert svc.state == WatcherState.WATCHING
    assert svc.snapshot().extraction_count == 1


@pytest.mark.asyncio
async def test_maybe_extract_records_extraction_failure(monkeypatch):
    svc = _service(cooldown=0.001)
    svc._state = WatcherState.COOLDOWN
    svc._cooldown_deadline = time.monotonic() - 1
    monkeypatch.setattr(
        "lineage_bridge.services.extraction_service.run_extraction",
        AsyncMock(side_effect=RuntimeError("api down")),
    )

    record = await svc.maybe_extract()

    assert record is not None
    assert record.error is not None
    assert "api down" in record.error
    assert svc.state == WatcherState.WATCHING  # back to watching after failure


@pytest.mark.asyncio
async def test_maybe_extract_invokes_configured_pushes(monkeypatch):
    """Each push flag in the config triggers one run_push call after extraction."""
    svc = _service(cooldown=0.001, push_uc=True)
    svc._state = WatcherState.COOLDOWN
    svc._cooldown_deadline = time.monotonic() - 1

    fake_graph = MagicMock(node_count=1, edge_count=0)
    monkeypatch.setattr(
        "lineage_bridge.services.extraction_service.run_extraction",
        AsyncMock(return_value=fake_graph),
    )
    push_mock = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr("lineage_bridge.services.push_service.run_push", push_mock)

    await svc.maybe_extract()

    assert push_mock.call_count == 1
    request = push_mock.call_args.args[0]
    assert request.provider == "databricks_uc"


@pytest.mark.asyncio
async def test_push_failure_does_not_propagate_to_record(monkeypatch):
    """Push failures are warnings — extraction record stays clean."""
    svc = _service(cooldown=0.001, push_uc=True)
    svc._state = WatcherState.COOLDOWN
    svc._cooldown_deadline = time.monotonic() - 1

    monkeypatch.setattr(
        "lineage_bridge.services.extraction_service.run_extraction",
        AsyncMock(return_value=MagicMock(node_count=1, edge_count=0)),
    )
    monkeypatch.setattr(
        "lineage_bridge.services.push_service.run_push",
        AsyncMock(side_effect=RuntimeError("uc denied")),
    )

    record = await svc.maybe_extract()

    assert record is not None
    assert record.error is None  # Extraction succeeded; push failure is warned-only.
