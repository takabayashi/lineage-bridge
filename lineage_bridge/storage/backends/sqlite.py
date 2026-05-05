# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""SQLite-backed storage — a single ``storage.db`` file under the storage root.

Three repositories sharing one DB file: graphs / tasks / events tables.
Each repo owns its own connection so callers don't share state across
instances; SQLite WAL mode (enabled by the migrations runner) lets the
connections concurrently read while one writes.

We use the stdlib ``sqlite3`` module instead of ``aiosqlite`` because the
repository protocol is synchronous (see protocol.py docstring) — adding an
async client would just force every method through ``asyncio.run`` for no
benefit. If the protocol ever becomes async (Postgres-async, ADR-022 future
work) the swap is local to this file.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

from lineage_bridge.api.task_store import TaskInfo
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.openlineage.models import RunEvent
from lineage_bridge.storage.migrations import apply_pending
from lineage_bridge.storage.protocol import GraphMeta


def _connect(db_path: Path) -> sqlite3.Connection:
    """Open + migrate a connection. Inlined here so each repository can call
    it without dragging in the migrations module's `initialise` directly."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
        isolation_level=None,  # autocommit; explicit BEGIN/COMMIT below
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    apply_pending(conn)
    return conn


class _SqliteRepo:
    """Common scaffolding: lazy-init the connection + serialize writes.

    The protocol is sync but Streamlit + uvicorn both schedule callers across
    threads. SQLite handles concurrent reads on its own (WAL mode), but
    serialising writes here avoids the occasional "database is locked"
    blip on rapid back-to-back saves.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._write_lock = threading.Lock()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = _connect(self._db_path)
        return self._conn

    def close(self) -> None:
        """Drop the connection. Tests use this; production never calls it."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


# ── graphs ──────────────────────────────────────────────────────────────


class SqliteGraphRepository(_SqliteRepo):
    """SQLite-backed `GraphRepository`."""

    def save(self, graph_id: str, graph: LineageGraph, meta: GraphMeta) -> None:
        import json

        payload = json.dumps(graph.to_dict())
        with self._write_lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO graphs(graph_id, payload, created_at, last_modified) "
                "VALUES (?, ?, ?, ?)",
                (
                    graph_id,
                    payload,
                    meta.created_at.isoformat(),
                    meta.last_modified.isoformat(),
                ),
            )

    def get(self, graph_id: str) -> tuple[LineageGraph, GraphMeta] | None:
        row = self.conn.execute(
            "SELECT payload, created_at, last_modified FROM graphs WHERE graph_id = ?",
            (graph_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_pair(graph_id, row)

    def list_meta(self) -> list[GraphMeta]:
        rows = self.conn.execute(
            "SELECT graph_id, created_at, last_modified FROM graphs"
        ).fetchall()
        return [
            GraphMeta(
                graph_id=gid,
                created_at=datetime.fromisoformat(created),
                last_modified=datetime.fromisoformat(modified),
            )
            for gid, created, modified in rows
        ]

    def list_with_graphs(self) -> list[tuple[LineageGraph, GraphMeta]]:
        rows = self.conn.execute(
            "SELECT graph_id, payload, created_at, last_modified FROM graphs"
        ).fetchall()
        out: list[tuple[LineageGraph, GraphMeta]] = []
        for gid, payload, created, modified in rows:
            out.append(self._row_to_pair(gid, (payload, created, modified)))
        return out

    def delete(self, graph_id: str) -> bool:
        with self._write_lock:
            cur = self.conn.execute("DELETE FROM graphs WHERE graph_id = ?", (graph_id,))
            return cur.rowcount > 0

    def touch(self, graph_id: str) -> None:
        with self._write_lock:
            self.conn.execute(
                "UPDATE graphs SET last_modified = ? WHERE graph_id = ?",
                (datetime.now(UTC).isoformat(), graph_id),
            )

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM graphs").fetchone()[0]

    @staticmethod
    def _row_to_pair(graph_id: str, row: tuple) -> tuple[LineageGraph, GraphMeta]:
        import json

        payload, created, modified = row
        graph = LineageGraph.from_dict(json.loads(payload))
        meta = GraphMeta(
            graph_id=graph_id,
            created_at=datetime.fromisoformat(created),
            last_modified=datetime.fromisoformat(modified),
        )
        return graph, meta


# ── tasks ───────────────────────────────────────────────────────────────


class SqliteTaskRepository(_SqliteRepo):
    """SQLite-backed `TaskRepository`."""

    def save(self, task: TaskInfo) -> None:
        with self._write_lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO tasks(task_id, payload) VALUES (?, ?)",
                (task.task_id, task.model_dump_json()),
            )

    def get(self, task_id: str) -> TaskInfo | None:
        row = self.conn.execute(
            "SELECT payload FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        return TaskInfo.model_validate_json(row[0])

    def list(self) -> list[TaskInfo]:
        rows = self.conn.execute("SELECT payload FROM tasks").fetchall()
        return [TaskInfo.model_validate_json(payload) for (payload,) in rows]

    def delete(self, task_id: str) -> bool:
        with self._write_lock:
            cur = self.conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            return cur.rowcount > 0

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]


# ── events ──────────────────────────────────────────────────────────────


class SqliteEventRepository(_SqliteRepo):
    """SQLite-backed append-only `EventRepository`.

    `id` is the autoincrement PK so `all()` returns events in insertion order
    without an explicit ordering column. `idx_events_run_id` covers the
    `by_run_id` lookup that the API hot-paths.
    """

    def add(self, events: list[RunEvent]) -> int:
        if not events:
            return 0
        rows = [(event.run.runId, event.model_dump_json(by_alias=True)) for event in events]
        with self._write_lock:
            self.conn.executemany("INSERT INTO events(run_id, payload) VALUES (?, ?)", rows)
        return len(events)

    def all(self) -> list[RunEvent]:
        rows = self.conn.execute("SELECT payload FROM events ORDER BY id").fetchall()
        return [RunEvent.model_validate_json(payload) for (payload,) in rows]

    def by_run_id(self, run_id: str) -> list[RunEvent]:
        rows = self.conn.execute(
            "SELECT payload FROM events WHERE run_id = ? ORDER BY id", (run_id,)
        ).fetchall()
        return [RunEvent.model_validate_json(payload) for (payload,) in rows]

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def clear(self) -> None:
        with self._write_lock:
            self.conn.execute("DELETE FROM events")
