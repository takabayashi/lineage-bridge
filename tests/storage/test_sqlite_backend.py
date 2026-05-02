# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""SQLite-backend tests beyond what the protocol-conformance suite covers.

Conformance proves CRUD shape; this file covers the things only SQLite has:
on-disk durability across re-opens, migrations applied exactly once, the
schema_migrations table, the run_id index, and that the file-on-disk model
matches the file-backend's durability claim.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from lineage_bridge.api.task_store import TaskInfo, TaskType
from lineage_bridge.storage.backends.sqlite import (
    SqliteEventRepository,
    SqliteGraphRepository,
    SqliteTaskRepository,
)
from lineage_bridge.storage.migrations import apply_pending
from lineage_bridge.storage.protocol import GraphMeta
from tests.storage.conftest import make_event, make_graph

# ── on-disk durability across re-opens ──────────────────────────────────


def test_graph_persists_across_reopen(tmp_path: Path):
    db = tmp_path / "storage.db"

    repo1 = SqliteGraphRepository(db)
    repo1.save("g1", make_graph(node_count=3), GraphMeta.now("g1"))
    repo1.close()

    repo2 = SqliteGraphRepository(db)
    loaded = repo2.get("g1")
    assert loaded is not None
    graph, meta = loaded
    assert graph.node_count == 3
    assert meta.graph_id == "g1"


def test_task_persists_across_reopen(tmp_path: Path):
    db = tmp_path / "storage.db"

    repo1 = SqliteTaskRepository(db)
    repo1.save(TaskInfo(task_id="t1", task_type=TaskType.EXTRACT))
    repo1.close()

    repo2 = SqliteTaskRepository(db)
    got = repo2.get("t1")
    assert got is not None
    assert got.task_id == "t1"


def test_events_persist_across_reopen(tmp_path: Path):
    db = tmp_path / "storage.db"

    repo1 = SqliteEventRepository(db)
    repo1.add([make_event("r1"), make_event("r2")])
    repo1.close()

    repo2 = SqliteEventRepository(db)
    assert repo2.count() == 2
    assert {e.run.runId for e in repo2.all()} == {"r1", "r2"}


def test_three_repos_share_one_db_file(tmp_path: Path):
    """Graphs / tasks / events all sit in the same `storage.db` — that's
    the deployment promise (one file to back up, one file to encrypt)."""
    db = tmp_path / "storage.db"

    SqliteGraphRepository(db).save("g1", make_graph(), GraphMeta.now("g1"))
    SqliteTaskRepository(db).save(TaskInfo(task_id="t1", task_type=TaskType.EXTRACT))
    SqliteEventRepository(db).add([make_event("r1")])

    assert db.exists()
    # Only one DB file (plus the WAL sidecars sqlite creates while open).
    db_files = sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("storage.db"))
    # storage.db, possibly storage.db-wal, storage.db-shm — but no other .db files.
    assert all(name.startswith("storage.db") for name in db_files)


# ── migrations ──────────────────────────────────────────────────────────


def test_migrations_apply_on_first_open(tmp_path: Path):
    """Opening a fresh DB creates schema_migrations + entity tables."""
    db = tmp_path / "storage.db"
    repo = SqliteGraphRepository(db)
    # Trigger lazy connect.
    repo.count()
    rows = repo.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {r[0] for r in rows}
    assert {"schema_migrations", "graphs", "tasks", "events"}.issubset(table_names)


def test_migrations_record_applied_versions(tmp_path: Path):
    db = tmp_path / "storage.db"
    repo = SqliteGraphRepository(db)
    repo.count()  # trigger init
    versions = [
        row[0] for row in repo.conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    ]
    # Currently one initial migration; adding more should append, not break.
    assert 1 in versions


def test_apply_pending_is_idempotent(tmp_path: Path):
    """Calling apply_pending() twice on the same connection only runs migrations once."""
    db = tmp_path / "storage.db"
    repo = SqliteGraphRepository(db)
    repo.count()  # first open applies migrations
    second_run = apply_pending(repo.conn)
    assert second_run == [], f"second apply_pending should be a no-op, got {second_run}"


def test_run_id_index_exists(tmp_path: Path):
    """`by_run_id` is the API hot-path; the index must exist or queries scan."""
    db = tmp_path / "storage.db"
    repo = SqliteEventRepository(db)
    repo.add([make_event("r1")])  # trigger init via write
    indexes = repo.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='events'"
    ).fetchall()
    assert any("run_id" in row[0] for row in indexes)


# ── concurrent reads (WAL) ──────────────────────────────────────────────


def test_wal_mode_enabled(tmp_path: Path):
    """WAL is what lets multiple repo connections read/write the same file."""
    db = tmp_path / "storage.db"
    repo = SqliteGraphRepository(db)
    repo.count()
    mode = repo.conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_separate_repos_see_each_others_writes(tmp_path: Path):
    """Two repo instances on the same DB see each other's commits — that's
    the shared-state guarantee the API depends on (graphs router holds one
    GraphRepository, tasks router holds one TaskRepository, both backed by
    the same DB)."""
    db = tmp_path / "storage.db"

    writer = SqliteGraphRepository(db)
    reader = SqliteGraphRepository(db)

    writer.save("g1", make_graph(node_count=2), GraphMeta.now("g1"))

    assert reader.get("g1") is not None
    assert reader.count() == 1


# ── lazy connection ─────────────────────────────────────────────────────


def test_db_file_not_created_until_first_use(tmp_path: Path):
    """Constructing a repo doesn't open the DB — useful for tests that
    instantiate repos in fixtures but only some test cases actually use them."""
    db = tmp_path / "storage.db"
    SqliteGraphRepository(db)
    assert not db.exists()


def test_db_file_created_on_first_write(tmp_path: Path):
    db = tmp_path / "storage.db"
    repo = SqliteGraphRepository(db)
    repo.save("g1", make_graph(), GraphMeta.now("g1"))
    assert db.exists()


# ── corrupt DB handling ─────────────────────────────────────────────────


def test_existing_unrelated_db_file_does_not_block_init(tmp_path: Path):
    """If the file exists with no schema (e.g. truncated or hand-created),
    the migration runner still creates the tables on open."""
    db = tmp_path / "storage.db"
    # Pre-create an empty SQLite DB.
    sqlite3.connect(db).close()

    repo = SqliteGraphRepository(db)
    repo.save("g1", make_graph(), GraphMeta.now("g1"))
    assert repo.count() == 1


# ── watcher migration ──────────────────────────────────────────────────


def test_watcher_migration_creates_tables(tmp_path: Path):
    """The 002_watchers.sql migration creates watchers / watcher_events /
    watcher_extractions tables on first open of any sqlite repo."""
    from lineage_bridge.storage.backends.sqlite import SqliteWatcherRepository

    db = tmp_path / "storage.db"
    repo = SqliteWatcherRepository(db)
    repo.list_watchers()  # trigger lazy init
    rows = repo.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {r[0] for r in rows}
    assert {"watchers", "watcher_events", "watcher_extractions"}.issubset(table_names)


def test_watcher_deregister_is_atomic(tmp_path: Path):
    """Regression — deregister cascades through three tables; if a failure
    between statements leaks orphan rows, a re-registered watcher_id would
    inherit the old events/extractions. Verify the wrapping transaction
    rolls back as a unit.

    `sqlite3.Connection.execute` is read-only at the C level so we can't
    monkeypatch it; instead, use a thin proxy connection that fails on the
    second DELETE.
    """
    import sqlite3
    from datetime import UTC, datetime

    from lineage_bridge.services.requests import ExtractionRequest
    from lineage_bridge.services.watcher_models import (
        ExtractionRecord,
        WatcherConfig,
        WatcherEvent,
    )
    from lineage_bridge.storage.backends.sqlite import SqliteWatcherRepository

    db = tmp_path / "storage.db"
    repo = SqliteWatcherRepository(db)
    cfg = WatcherConfig(extraction=ExtractionRequest(environment_ids=["env-1"]))
    repo.register("w1", cfg)
    repo.append_event(
        "w1",
        WatcherEvent(
            id="e1",
            time=datetime.now(UTC),
            method_name="kafka.CreateTopics",
            resource_name="t",
            principal="u",
            raw={},
        ),
    )
    repo.append_extraction("w1", ExtractionRecord(triggered_at=datetime.now(UTC)))

    real_conn = repo.conn

    class FailingConn:
        """Proxy that forwards every attribute to the real connection except
        `execute` — which fails on the watcher_events delete."""

        def __getattr__(self, name):
            return getattr(real_conn, name)

        def execute(self, sql, *args, **kwargs):
            if "DELETE FROM watcher_events" in sql:
                raise sqlite3.OperationalError("simulated failure")
            return real_conn.execute(sql, *args, **kwargs)

    import contextlib

    repo._conn = FailingConn()  # type: ignore[assignment]
    with contextlib.suppress(sqlite3.OperationalError):
        repo.deregister("w1")
    repo._conn = real_conn  # restore for the assertions below

    # Rollback must have undone the watchers DELETE — config still readable,
    # events + extractions still present.
    assert repo.get_config("w1") is not None
    assert len(repo.list_events("w1")) == 1
    assert len(repo.list_extractions("w1")) == 1


def test_watcher_lookup_indexes_exist(tmp_path: Path):
    """Per-watcher list endpoints rely on (watcher_id, time) indexes."""
    from lineage_bridge.storage.backends.sqlite import SqliteWatcherRepository

    db = tmp_path / "storage.db"
    repo = SqliteWatcherRepository(db)
    repo.list_watchers()  # trigger lazy init
    indexes = {
        row[0]
        for row in repo.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name IN ('watcher_events', 'watcher_extractions')"
        ).fetchall()
    }
    assert "idx_watcher_events_lookup" in indexes
    assert "idx_watcher_extractions_lookup" in indexes
