# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for `storage.factory.make_repositories`."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lineage_bridge.storage import make_repositories
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


def _settings(backend: str, path: Path | None = None):
    s = MagicMock()
    s.storage = MagicMock()
    s.storage.backend = backend
    s.storage.path = path or Path("/tmp/lineage-bridge-test")
    return s


def test_memory_backend_returns_memory_repos():
    repos = make_repositories(_settings("memory"))
    assert isinstance(repos.graphs, MemoryGraphRepository)
    assert isinstance(repos.tasks, MemoryTaskRepository)
    assert isinstance(repos.events, MemoryEventRepository)


def test_file_backend_returns_file_repos(tmp_path: Path):
    repos = make_repositories(_settings("file", tmp_path))
    assert isinstance(repos.graphs, FileGraphRepository)
    assert isinstance(repos.tasks, FileTaskRepository)
    assert isinstance(repos.events, FileEventRepository)


def test_backend_is_case_insensitive():
    repos = make_repositories(_settings("MEMORY"))
    assert isinstance(repos.graphs, MemoryGraphRepository)


def test_sqlite_backend_raises_not_implemented():
    """SQLite backend lands in Phase 2F; until then it's a clear NotImplementedError."""
    with pytest.raises(NotImplementedError, match="Phase 2F"):
        make_repositories(_settings("sqlite"))


def test_unknown_backend_raises_value_error():
    with pytest.raises(ValueError, match="Unknown storage backend"):
        make_repositories(_settings("redis"))
