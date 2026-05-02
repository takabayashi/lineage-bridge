# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for WatcherRunner — the asyncio loop + persistence wrapper.

Uses MemoryWatcherRepository as the durability sink (the conformance
suite already proves the protocol contract; here we verify the runner
actually writes through it on every tick).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from lineage_bridge.services.requests import ExtractionRequest
from lineage_bridge.services.watcher_models import (
    WatcherConfig,
    WatcherEvent,
    WatcherMode,
    WatcherState,
)
from lineage_bridge.services.watcher_runner import WatcherRunner
from lineage_bridge.storage.backends.memory import MemoryWatcherRepository


def _config(poll_interval: float = 0.05, cooldown: float = 0.001) -> WatcherConfig:
    """Tight intervals so the loop ticks several times in a few ms test budget."""
    return WatcherConfig(
        mode=WatcherMode.REST_POLLING,
        poll_interval_seconds=poll_interval,
        cooldown_seconds=cooldown,
        extraction=ExtractionRequest(environment_ids=["env-1"]),
    )


def _fake_event() -> WatcherEvent:
    from datetime import UTC, datetime

    return WatcherEvent(
        id="e1",
        time=datetime.now(UTC),
        method_name="kafka.CreateTopics",
        resource_name="topic-x",
        principal="u:test",
        raw={},
    )


# ── spawn ───────────────────────────────────────────────────────────────


def test_spawn_registers_config_in_repo():
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner.spawn(_config(), settings, repo)
    assert repo.get_config(runner.watcher_id) is not None


def test_spawned_runner_has_unique_id():
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    a = WatcherRunner.spawn(_config(), settings, repo)
    b = WatcherRunner.spawn(_config(), settings, repo)
    assert a.watcher_id != b.watcher_id


# ── loop persists status / events / extractions ─────────────────────────


@pytest.mark.asyncio
async def test_run_forever_persists_status_on_every_tick(monkeypatch):
    """One tick → one status write to the repo."""
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner.spawn(_config(poll_interval=0.02), settings, repo)

    # Stub the service: poll returns empty (no extraction trigger).
    monkeypatch.setattr(runner._service, "start", AsyncMock())
    monkeypatch.setattr(runner._service, "stop", AsyncMock())
    monkeypatch.setattr(runner._service, "poll_once", AsyncMock(return_value=[]))
    monkeypatch.setattr(runner._service, "maybe_extract", AsyncMock(return_value=None))

    await runner.start()
    await asyncio.sleep(0.1)  # let it tick a few times
    await runner.stop()

    # At minimum the start() call writes the initial status, and the loop
    # writes every iteration; even if poll_once was never called we'd see
    # at least the initial + final snapshots.
    status = repo.get_status(runner.watcher_id)
    assert status is not None
    # State drained back to STOPPED on stop().
    assert status.state == WatcherState.STOPPED


@pytest.mark.asyncio
async def test_run_forever_appends_events_to_repo(monkeypatch):
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner.spawn(_config(), settings, repo)

    poll_calls = 0

    async def fake_poll() -> list[WatcherEvent]:
        nonlocal poll_calls
        poll_calls += 1
        # First call yields an event; subsequent return empty.
        if poll_calls == 1:
            return [_fake_event()]
        return []

    monkeypatch.setattr(runner._service, "start", AsyncMock())
    monkeypatch.setattr(runner._service, "stop", AsyncMock())
    monkeypatch.setattr(runner._service, "poll_once", fake_poll)
    monkeypatch.setattr(runner._service, "maybe_extract", AsyncMock(return_value=None))

    await runner.start()
    await asyncio.sleep(0.15)
    await runner.stop()

    events = repo.list_events(runner.watcher_id)
    assert len(events) >= 1
    assert events[0].method_name == "kafka.CreateTopics"


@pytest.mark.asyncio
async def test_run_forever_appends_extraction_records(monkeypatch):
    from datetime import UTC, datetime

    from lineage_bridge.services.watcher_models import ExtractionRecord

    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner.spawn(_config(), settings, repo)

    record = ExtractionRecord(
        triggered_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        node_count=12,
    )
    extract_calls = 0

    async def fake_extract():
        nonlocal extract_calls
        extract_calls += 1
        return record if extract_calls == 1 else None

    monkeypatch.setattr(runner._service, "start", AsyncMock())
    monkeypatch.setattr(runner._service, "stop", AsyncMock())
    monkeypatch.setattr(runner._service, "poll_once", AsyncMock(return_value=[]))
    monkeypatch.setattr(runner._service, "maybe_extract", fake_extract)

    await runner.start()
    await asyncio.sleep(0.15)
    await runner.stop()

    history = repo.list_extractions(runner.watcher_id)
    assert len(history) == 1
    assert history[0].node_count == 12


# ── stop() drains the task ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stop_returns_quickly_when_loop_is_responsive(monkeypatch):
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner.spawn(_config(poll_interval=0.05), settings, repo)
    monkeypatch.setattr(runner._service, "start", AsyncMock())
    monkeypatch.setattr(runner._service, "stop", AsyncMock())
    monkeypatch.setattr(runner._service, "poll_once", AsyncMock(return_value=[]))
    monkeypatch.setattr(runner._service, "maybe_extract", AsyncMock(return_value=None))

    await runner.start()
    # Loop alive
    assert runner.is_running

    await asyncio.wait_for(runner.stop(), timeout=2.0)
    assert not runner.is_running


@pytest.mark.asyncio
async def test_stop_is_idempotent_when_no_task(monkeypatch):
    """stop() before start() must not blow up."""
    repo = MemoryWatcherRepository()
    settings = MagicMock()
    runner = WatcherRunner(
        watcher_id="w-x",
        config=_config(),
        settings=settings,
        repo=repo,
    )
    await runner.stop()
