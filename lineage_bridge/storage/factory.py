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
)
from lineage_bridge.storage.protocol import (
    EventRepository,
    GraphRepository,
    TaskRepository,
)

if TYPE_CHECKING:
    from lineage_bridge.config.settings import Settings


@dataclass
class Repositories:
    """Bundle of the three repositories the API + watcher consume."""

    graphs: GraphRepository
    tasks: TaskRepository
    events: EventRepository


def make_repositories(settings: Settings) -> Repositories:
    """Build a `Repositories` bundle from `settings.storage.{backend, path}`.

    Falls back to memory if the backend is unrecognised — defensive choice
    because storage misconfiguration shouldn't take the API down on startup
    (the API would be unusable anyway, but at least it boots so its
    `/api/v1/health` could be hit for debugging).
    """
    storage = settings.storage
    backend = storage.backend.lower()

    if backend == "memory":
        return Repositories(
            graphs=MemoryGraphRepository(),
            tasks=MemoryTaskRepository(),
            events=MemoryEventRepository(),
        )

    if backend == "file":
        root = Path(storage.path).expanduser()
        return Repositories(
            graphs=FileGraphRepository(root / "graphs"),
            tasks=FileTaskRepository(root / "tasks"),
            events=FileEventRepository(root / "events.jsonl"),
        )

    if backend == "sqlite":
        # Phase 2F.
        raise NotImplementedError(
            "sqlite backend lands in Phase 2F. Use 'memory' or 'file' for now."
        )

    raise ValueError(
        f"Unknown storage backend: {storage.backend!r}. "
        "Set LINEAGE_BRIDGE_STORAGE__BACKEND to one of: memory, file."
    )
