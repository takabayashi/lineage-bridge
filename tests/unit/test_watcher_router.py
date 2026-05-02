# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for `/api/v1/watcher/*` endpoints (Phase 2G)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from lineage_bridge.api.app import create_app


@pytest.fixture()
def client():
    return TestClient(create_app())


def _config_body(env_id: str = "env-1") -> dict:
    return {
        "mode": "rest_polling",
        "extraction": {"environment_ids": [env_id]},
    }


# ── start / list / status / stop ────────────────────────────────────────


def test_start_creates_watcher_returns_id(client):
    """POST /watcher → 201 with a UUID; the runner is registered in the repo."""
    with patch(
        "lineage_bridge.services.watcher_runner.WatcherRunner.start",
        new=AsyncMock(),
    ):
        resp = client.post("/api/v1/watcher", json=_config_body())
    assert resp.status_code == 201
    body = resp.json()
    assert "watcher_id" in body
    assert len(body["watcher_id"]) >= 8


def test_list_watchers_includes_started_one(client):
    with patch(
        "lineage_bridge.services.watcher_runner.WatcherRunner.start",
        new=AsyncMock(),
    ):
        wid = client.post("/api/v1/watcher", json=_config_body("env-x")).json()["watcher_id"]
    resp = client.get("/api/v1/watcher")
    assert resp.status_code == 200
    ids = [w["watcher_id"] for w in resp.json()["watchers"]]
    assert wid in ids


def test_get_status_returns_404_for_unknown(client):
    resp = client.get("/api/v1/watcher/nope/status")
    assert resp.status_code == 404


def test_get_status_returns_snapshot_after_start(client):
    """The runner persists a status snapshot on start()."""

    async def fake_start(self):
        # Mimic runner.start writing the initial status snapshot via the repo.
        from lineage_bridge.services.watcher_models import (
            WatcherState,
            WatcherStatus,
        )

        self.repo.update_status(
            self.watcher_id,
            WatcherStatus(watcher_id=self.watcher_id, state=WatcherState.WATCHING),
        )

    with patch(
        "lineage_bridge.services.watcher_runner.WatcherRunner.start",
        new=fake_start,
    ):
        wid = client.post("/api/v1/watcher", json=_config_body()).json()["watcher_id"]

    resp = client.get(f"/api/v1/watcher/{wid}/status")
    assert resp.status_code == 200
    assert resp.json()["state"] == "watching"


def test_stop_returns_404_when_runner_not_in_this_process(client):
    """Cross-process stop is not supported; 404 makes that explicit."""
    resp = client.post("/api/v1/watcher/nonexistent/stop")
    assert resp.status_code == 404


def test_stop_succeeds_for_runner_in_this_process(client):
    started = AsyncMock()
    stopped = AsyncMock()
    with (
        patch(
            "lineage_bridge.services.watcher_runner.WatcherRunner.start",
            new=started,
        ),
        patch(
            "lineage_bridge.services.watcher_runner.WatcherRunner.stop",
            new=stopped,
        ),
    ):
        wid = client.post("/api/v1/watcher", json=_config_body()).json()["watcher_id"]
        resp = client.post(f"/api/v1/watcher/{wid}/stop")
    assert resp.status_code == 204
    stopped.assert_awaited()


# ── events + history ────────────────────────────────────────────────────


def test_list_events_returns_404_for_unknown(client):
    resp = client.get("/api/v1/watcher/nope/events")
    assert resp.status_code == 404


def test_list_events_returns_recorded_events(client):
    """The repo's events surface through the endpoint."""
    from datetime import UTC, datetime

    from lineage_bridge.services.watcher_models import WatcherEvent

    with patch(
        "lineage_bridge.services.watcher_runner.WatcherRunner.start",
        new=AsyncMock(),
    ):
        wid = client.post("/api/v1/watcher", json=_config_body()).json()["watcher_id"]

    repo = client.app.state.watcher_repo
    repo.append_event(
        wid,
        WatcherEvent(
            id="e1",
            time=datetime.now(UTC),
            method_name="kafka.CreateTopics",
            resource_name="topic-x",
            principal="u:test",
            raw={},
        ),
    )

    resp = client.get(f"/api/v1/watcher/{wid}/events")
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert len(events) == 1
    assert events[0]["method_name"] == "kafka.CreateTopics"


def test_list_history_returns_recorded_extractions(client):
    from datetime import UTC, datetime

    from lineage_bridge.services.watcher_models import ExtractionRecord

    with patch(
        "lineage_bridge.services.watcher_runner.WatcherRunner.start",
        new=AsyncMock(),
    ):
        wid = client.post("/api/v1/watcher", json=_config_body()).json()["watcher_id"]

    repo = client.app.state.watcher_repo
    repo.append_extraction(
        wid,
        ExtractionRecord(
            triggered_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            node_count=7,
        ),
    )

    resp = client.get(f"/api/v1/watcher/{wid}/history")
    assert resp.status_code == 200
    history = resp.json()["extractions"]
    assert len(history) == 1
    assert history[0]["node_count"] == 7


# ── deregister ──────────────────────────────────────────────────────────


def test_deregister_drops_watcher_from_repo(client):
    with (
        patch(
            "lineage_bridge.services.watcher_runner.WatcherRunner.start",
            new=AsyncMock(),
        ),
        patch(
            "lineage_bridge.services.watcher_runner.WatcherRunner.stop",
            new=AsyncMock(),
        ),
    ):
        wid = client.post("/api/v1/watcher", json=_config_body()).json()["watcher_id"]
        resp = client.delete(f"/api/v1/watcher/{wid}")
    assert resp.status_code == 204
    assert client.app.state.watcher_repo.get_status(wid) is None


def test_deregister_returns_404_when_unknown(client):
    resp = client.delete("/api/v1/watcher/never-existed")
    assert resp.status_code == 404
