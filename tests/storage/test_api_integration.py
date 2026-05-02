# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""End-to-end: the API survives an `app` recreation when backed by file storage.

This is the durability claim Phase 1C is selling. The conformance suite proves
each repository round-trips in isolation; this test proves that the API +
adapter chain (route -> Store -> Repository -> file) actually persists across
fresh `create_app` calls pointing at the same storage root.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from lineage_bridge.api.app import create_app
from lineage_bridge.storage import Repositories
from lineage_bridge.storage.backends.file import (
    FileEventRepository,
    FileGraphRepository,
    FileTaskRepository,
)
from lineage_bridge.storage.backends.sqlite import (
    SqliteEventRepository,
    SqliteGraphRepository,
    SqliteTaskRepository,
)


def _file_repos(root: Path) -> Repositories:
    from lineage_bridge.storage.backends.memory import MemoryWatcherRepository

    return Repositories(
        graphs=FileGraphRepository(root / "graphs"),
        tasks=FileTaskRepository(root / "tasks"),
        events=FileEventRepository(root / "events.jsonl"),
        # No file backend for watchers — sqlite is the durable path.
        watchers=MemoryWatcherRepository(),
    )


def test_file_backend_graph_persists_across_app_recreation(tmp_path: Path):
    """POST a graph through one TestClient; GET it through a fresh one."""
    root = tmp_path / "storage"

    client1 = TestClient(create_app(repositories=_file_repos(root)))
    create_resp = client1.post("/api/v1/graphs")
    assert create_resp.status_code == 201
    graph_id = create_resp.json()["graph_id"]

    # New TestClient, new repository instances, same storage root —
    # simulates a process restart.
    client2 = TestClient(create_app(repositories=_file_repos(root)))
    list_resp = client2.get("/api/v1/graphs")
    assert list_resp.status_code == 200
    ids = [g["graph_id"] for g in list_resp.json()]
    assert graph_id in ids


def test_file_backend_task_persists_across_app_recreation(tmp_path: Path):
    """A task created through the API is visible after `create_app` is called again."""
    root = tmp_path / "storage"

    client1 = TestClient(create_app(repositories=_file_repos(root)))
    extract_resp = client1.post("/api/v1/tasks/extract", json={"environment_ids": []})
    assert extract_resp.status_code == 202
    task_id = extract_resp.json()["task_id"]

    client2 = TestClient(create_app(repositories=_file_repos(root)))
    get_resp = client2.get(f"/api/v1/tasks/{task_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["task_id"] == task_id


# ── sqlite backend (Phase 2F) — same durability claim ───────────────────


def _sqlite_repos(root: Path) -> Repositories:
    from lineage_bridge.storage.backends.sqlite import SqliteWatcherRepository

    db_path = root / "storage.db"
    return Repositories(
        graphs=SqliteGraphRepository(db_path),
        tasks=SqliteTaskRepository(db_path),
        events=SqliteEventRepository(db_path),
        watchers=SqliteWatcherRepository(db_path),
    )


def test_sqlite_backend_graph_persists_across_app_recreation(tmp_path: Path):
    """Same durability claim as the file backend, but against `storage.db`."""
    root = tmp_path / "storage"

    client1 = TestClient(create_app(repositories=_sqlite_repos(root)))
    create_resp = client1.post("/api/v1/graphs")
    assert create_resp.status_code == 201
    graph_id = create_resp.json()["graph_id"]

    client2 = TestClient(create_app(repositories=_sqlite_repos(root)))
    list_resp = client2.get("/api/v1/graphs")
    assert list_resp.status_code == 200
    ids = [g["graph_id"] for g in list_resp.json()]
    assert graph_id in ids


def test_sqlite_backend_task_persists_across_app_recreation(tmp_path: Path):
    root = tmp_path / "storage"

    client1 = TestClient(create_app(repositories=_sqlite_repos(root)))
    extract_resp = client1.post("/api/v1/tasks/extract", json={"environment_ids": []})
    assert extract_resp.status_code == 202
    task_id = extract_resp.json()["task_id"]

    client2 = TestClient(create_app(repositories=_sqlite_repos(root)))
    get_resp = client2.get(f"/api/v1/tasks/{task_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["task_id"] == task_id
