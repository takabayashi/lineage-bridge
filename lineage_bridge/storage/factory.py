# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Repository factory — chooses a backend based on Settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from lineage_bridge.storage.backends.file import (
    FileEventRepository,
    FileGraphRepository,
    FileTaskRepository,
)
from lineage_bridge.storage.backends.memory import (
    MemoryEventRepository,
    MemoryGraphRepository,
    MemoryTaskRepository,
    MemoryWatcherRepository,
)
from lineage_bridge.storage.backends.sqlite import (
    SqliteEventRepository,
    SqliteGraphRepository,
    SqliteTaskRepository,
    SqliteWatcherRepository,
)
from lineage_bridge.storage.protocol import (
    EventRepository,
    GraphRepository,
    TaskRepository,
    WatcherRepository,
)

if TYPE_CHECKING:
    from lineage_bridge.config.settings import Settings


@dataclass
class Repositories:
    """Bundle of the four repositories the API + watcher consume."""

    graphs: GraphRepository
    tasks: TaskRepository
    events: EventRepository
    watchers: WatcherRepository


def make_repositories(settings: Settings) -> Repositories:
    """Build a `Repositories` bundle from `settings.storage.{backend, path}`.

    Raises `ValueError` (unknown backend) or `NotImplementedError` (sqlite,
    pre-Phase-2F) on misconfiguration — fail-loud is the right default for
    storage. `create_app()` decides whether to surface the failure or fall
    back to memory.
    """
    storage = settings.storage
    backend = storage.backend.lower()

    if backend == "memory":
        return Repositories(
            graphs=MemoryGraphRepository(),
            tasks=MemoryTaskRepository(),
            events=MemoryEventRepository(),
            watchers=MemoryWatcherRepository(),
        )

    if backend == "file":
        root = Path(storage.path).expanduser()
        return Repositories(
            graphs=FileGraphRepository(root / "graphs"),
            tasks=FileTaskRepository(root / "tasks"),
            events=FileEventRepository(root / "events.jsonl"),
            # No file-backend watcher implementation by design: sqlite is
            # the watcher's durable path (one DB file, transactional appends,
            # cheap by-watcher_id indexes). The memory fallback keeps watcher
            # state alive within the API process; operators who want
            # cross-restart watchers should set storage.backend=sqlite.
            watchers=MemoryWatcherRepository(),
        )

    if backend == "sqlite":
        # All four repos point at the same `storage.db` file under the
        # configured root. Each owns its own connection (WAL handles
        # concurrent reads); migrations run on first open.
        db_path = Path(storage.path).expanduser() / "storage.db"
        return Repositories(
            graphs=SqliteGraphRepository(db_path),
            tasks=SqliteTaskRepository(db_path),
            events=SqliteEventRepository(db_path),
            watchers=SqliteWatcherRepository(db_path),
        )

    raise ValueError(
        f"Unknown storage backend: {storage.backend!r}. "
        "Set LINEAGE_BRIDGE_STORAGE__BACKEND to one of: memory, file, sqlite."
    )
