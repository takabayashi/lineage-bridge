# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Apply pending SQLite migrations on first connect.

`apply_pending(conn)` is idempotent — it inspects `schema_migrations` (which
the very first migration creates) and runs only the `.sql` files whose
version isn't recorded yet. Migrations live as `NNN_<description>.sql` files
in this package; ordering is the integer prefix.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path

logger = logging.getLogger(__name__)

_VERSION_PREFIX = re.compile(r"^(\d+)_")


def _migration_files() -> list[tuple[int, str, str]]:
    """Return `(version, name, sql)` for every migration in version order."""
    pkg = "lineage_bridge.storage.migrations"
    out: list[tuple[int, str, str]] = []
    for entry in resources.files(pkg).iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _VERSION_PREFIX.match(entry.name)
        if not match:
            continue
        out.append((int(match.group(1)), entry.name, entry.read_text(encoding="utf-8")))
    out.sort(key=lambda t: t[0])
    return out


def _applied_versions(conn: sqlite3.Connection) -> set[int]:
    """Return versions already recorded in `schema_migrations`, or `set()` if
    the table doesn't exist yet (first run)."""
    try:
        rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {row[0] for row in rows}


def apply_pending(conn: sqlite3.Connection) -> list[int]:
    """Apply every migration whose version isn't yet in `schema_migrations`.

    Returns the list of versions newly applied (empty when up-to-date). Each
    migration runs in its own transaction; a failure aborts that one
    migration without committing — subsequent opens will retry it.
    """
    applied = _applied_versions(conn)
    newly_applied: list[int] = []
    for version, name, sql in _migration_files():
        if version in applied:
            continue
        try:
            # `executescript` runs many statements but resets autocommit
            # between them, dropping any open transaction. Prefixing with
            # `BEGIN;` keeps the whole migration + the version-row insert
            # below in one atomic transaction so a partial schema can't
            # land if the process dies mid-migration.
            conn.executescript("BEGIN; " + sql)
            conn.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (version, datetime.now(UTC).isoformat()),
            )
            conn.commit()
            newly_applied.append(version)
            logger.info("Applied SQLite migration %s", name)
        except sqlite3.Error:
            conn.rollback()
            logger.exception("Migration %s failed; aborting", name)
            raise
    return newly_applied


def initialise(db_path: Path) -> sqlite3.Connection:
    """Open a connection to *db_path*, run pending migrations, return the connection.

    Convenience helper for backend constructors. Enables WAL mode so concurrent
    readers don't block writers (matches what we'd get from a file backend
    holding flocks). Foreign keys aren't used yet but enabling the pragma is
    cheap and protects future schema changes.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,  # Streamlit + uvicorn both call across threads
        isolation_level=None,  # autocommit; we manage transactions explicitly
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")  # WAL+NORMAL is the SQLite-recommended pair
    apply_pending(conn)
    return conn
