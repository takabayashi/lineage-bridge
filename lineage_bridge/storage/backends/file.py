# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""File-backed storage — JSON files under `~/.lineage_bridge/storage/`.

Layout:

    {root}/graphs/{graph_id}.json   {"graph": <LineageGraph dict>, "meta": {...}}
    {root}/tasks/{task_id}.json     <TaskInfo dict>
    {root}/events.jsonl             one RunEvent per line, append-only

We use the stdlib `fcntl.flock` (LOCK_EX) on POSIX for cross-process safety
during writes, since `portalocker` would add a third-party dep just for one
line. Reads are unsynchronised — the JSON files are written atomically
(temp file + os.replace), so a concurrent read either sees the old file or
the new one, never a half-written one.

Failure mode: a write that crashes between marshalling and rename leaves
no partial file (atomic rename). A write that crashes after rename leaves
the new file as the source of truth. Worst case is the lock file becoming
stale on a hard process kill — `flock` releases automatically when the file
descriptor closes, so a crashed process can't hold the lock forever.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lineage_bridge.api.task_store import TaskInfo
from lineage_bridge.models.graph import LineageGraph
from lineage_bridge.openlineage.models import RunEvent
from lineage_bridge.storage.protocol import GraphMeta

logger = logging.getLogger(__name__)


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Write *payload* to *path* atomically (tempfile + rename).

    Callers must hand in a fully JSON-safe payload; we deliberately don't pass
    a `default=` to `json.dump` so a stray non-serialisable object surfaces as
    a TypeError rather than silently coercing to its `str()` form.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp_path, path)
    except Exception:
        # Best-effort cleanup of the tempfile if rename never happened
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


def _read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("Could not read %s", path, exc_info=True)
        return None


_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class _DirRepo:
    """Helper: per-entity directory of `{key}.json` files with flock-guarded writes."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._lock_path = root / ".lock"

    def _path(self, key: str) -> Path:
        # Reject anything that isn't a plain alphanumeric / dash / underscore
        # token. Without this, an authenticated request to (e.g.)
        # `DELETE /api/v1/graphs/..%2F..%2Fevil` would have the `_root`-
        # joined path escape the storage root once Starlette URL-decodes
        # the path segment.
        if not _SAFE_KEY_RE.fullmatch(key):
            raise ValueError(
                f"Storage key {key!r} contains invalid characters; "
                "expected [A-Za-z0-9_-] only."
            )
        return self._root / f"{key}.json"

    def _with_write_lock(self, fn):
        """Run *fn* while holding an exclusive flock on the directory's .lock file."""
        # Open in 'a' so the file is created if missing and we get a writable fd.
        with open(self._lock_path, "a") as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            try:
                return fn()
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)


class FileGraphRepository:
    """File-backed `GraphRepository`."""

    def __init__(self, root: Path) -> None:
        self._dir = _DirRepo(root)
        self._root = root

    def save(self, graph_id: str, graph: LineageGraph, meta: GraphMeta) -> None:
        payload = {
            "graph": graph.to_dict(),
            "meta": {
                "graph_id": meta.graph_id,
                "created_at": meta.created_at.isoformat(),
                "last_modified": meta.last_modified.isoformat(),
            },
        }
        self._dir._with_write_lock(lambda: _atomic_write_json(self._dir._path(graph_id), payload))

    def get(self, graph_id: str) -> tuple[LineageGraph, GraphMeta] | None:
        data = _read_json(self._dir._path(graph_id))
        if data is None:
            return None
        graph = LineageGraph.from_dict(data["graph"])
        m = data["meta"]
        meta = GraphMeta(
            graph_id=m["graph_id"],
            created_at=datetime.fromisoformat(m["created_at"]),
            last_modified=datetime.fromisoformat(m["last_modified"]),
        )
        return graph, meta

    def list_meta(self) -> list[GraphMeta]:
        return [meta for _, meta in self.list_with_graphs()]

    def list_with_graphs(self) -> list[tuple[LineageGraph, GraphMeta]]:
        out: list[tuple[LineageGraph, GraphMeta]] = []
        for path in self._root.glob("*.json"):
            data = _read_json(path)
            if data is None:
                continue
            m = data["meta"]
            meta = GraphMeta(
                graph_id=m["graph_id"],
                created_at=datetime.fromisoformat(m["created_at"]),
                last_modified=datetime.fromisoformat(m["last_modified"]),
            )
            graph = LineageGraph.from_dict(data["graph"])
            out.append((graph, meta))
        return out

    def delete(self, graph_id: str) -> bool:
        path = self._dir._path(graph_id)
        if not path.exists():
            return False

        def _delete() -> bool:
            try:
                path.unlink()
                return True
            except FileNotFoundError:
                return False

        return self._dir._with_write_lock(_delete)

    def touch(self, graph_id: str) -> None:
        existing = self.get(graph_id)
        if existing is None:
            return
        graph, meta = existing
        meta.last_modified = datetime.now(UTC)
        self.save(graph_id, graph, meta)

    def count(self) -> int:
        return sum(1 for _ in self._root.glob("*.json"))


class FileTaskRepository:
    """File-backed `TaskRepository`."""

    def __init__(self, root: Path) -> None:
        self._dir = _DirRepo(root)
        self._root = root

    def save(self, task: TaskInfo) -> None:
        payload = task.model_dump(mode="json")
        self._dir._with_write_lock(
            lambda: _atomic_write_json(self._dir._path(task.task_id), payload)
        )

    def get(self, task_id: str) -> TaskInfo | None:
        data = _read_json(self._dir._path(task_id))
        if data is None:
            return None
        return TaskInfo.model_validate(data)

    def list(self) -> list[TaskInfo]:
        out: list[TaskInfo] = []
        for path in self._root.glob("*.json"):
            data = _read_json(path)
            if data is None:
                continue
            out.append(TaskInfo.model_validate(data))
        return out

    def delete(self, task_id: str) -> bool:
        path = self._dir._path(task_id)
        if not path.exists():
            return False

        def _delete() -> bool:
            try:
                path.unlink()
                return True
            except FileNotFoundError:
                return False

        return self._dir._with_write_lock(_delete)

    def count(self) -> int:
        return sum(1 for _ in self._root.glob("*.json"))


class FileEventRepository:
    """File-backed append-only `EventRepository` (one event per line, JSONL).

    Reads scan the whole file. That's fine for the tens-of-thousands-of-events
    range; if we grow past that, this becomes the SQLite-backend's job.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = path.parent / ".events.lock"

    def _with_write_lock(self, fn):
        with open(self._lock_path, "a") as lock_fd:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            try:
                return fn()
            finally:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

    def add(self, events: list[RunEvent]) -> int:
        if not events:
            return 0

        def _append() -> int:
            with self._path.open("a", encoding="utf-8") as f:
                for event in events:
                    f.write(event.model_dump_json(by_alias=True))
                    f.write("\n")
            return len(events)

        return self._with_write_lock(_append)

    def _read_all(self) -> list[RunEvent]:
        if not self._path.exists():
            return []
        out: list[RunEvent] = []
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(RunEvent.model_validate_json(line))
                except Exception:
                    logger.warning("Skipping malformed event line: %s", line[:200])
        return out

    def all(self) -> list[RunEvent]:
        return self._read_all()

    def by_run_id(self, run_id: str) -> list[RunEvent]:
        return [e for e in self._read_all() if e.run.runId == run_id]

    def count(self) -> int:
        if not self._path.exists():
            return 0
        with self._path.open("r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())

    def clear(self) -> None:
        def _clear() -> None:
            if self._path.exists():
                self._path.unlink()

        self._with_write_lock(_clear)
