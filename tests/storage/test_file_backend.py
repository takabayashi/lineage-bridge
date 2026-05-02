# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""File-backend-specific tests: persistence across instances, atomic writes."""

from __future__ import annotations

from pathlib import Path

import pytest

from lineage_bridge.api.task_store import TaskInfo, TaskType
from lineage_bridge.openlineage.models import Job, Run, RunEvent, RunEventType
from lineage_bridge.storage.backends.file import (
    FileEventRepository,
    FileGraphRepository,
    FileTaskRepository,
)
from lineage_bridge.storage.protocol import GraphMeta
from tests.storage.conftest import make_event, make_graph, make_task


def test_file_graph_persists_across_instances(tmp_path: Path):
    """A FileGraphRepository written and then re-opened sees the same data."""
    root = tmp_path / "graphs"
    repo1 = FileGraphRepository(root)
    repo1.save("g1", make_graph(node_count=4), GraphMeta.now("g1"))

    repo2 = FileGraphRepository(root)
    loaded = repo2.get("g1")
    assert loaded is not None
    g, _ = loaded
    assert g.node_count == 4


def test_file_task_persists_across_instances(tmp_path: Path):
    root = tmp_path / "tasks"
    repo1 = FileTaskRepository(root)
    t = make_task("t1")
    t.progress.append("hello")
    repo1.save(t)

    repo2 = FileTaskRepository(root)
    got = repo2.get("t1")
    assert got is not None
    assert got.progress == ["hello"]


def test_file_event_persists_across_instances(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    repo1 = FileEventRepository(path)
    repo1.add([make_event("r1", "j1"), make_event("r2", "j2")])

    repo2 = FileEventRepository(path)
    assert repo2.count() == 2
    assert {e.run.runId for e in repo2.all()} == {"r1", "r2"}


def test_file_graph_atomic_write_no_partial_file_on_serialisation_failure(
    tmp_path: Path, monkeypatch
):
    """If serialisation fails mid-write, the destination file isn't created."""
    root = tmp_path / "graphs"
    repo = FileGraphRepository(root)

    # First, write a valid graph so the directory is set up.
    repo.save("g1", make_graph(), GraphMeta.now("g1"))

    # Now break json.dump to simulate mid-write failure.

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr("lineage_bridge.storage.backends.file.json.dump", boom)

    with pytest.raises(RuntimeError):
        repo.save("g2", make_graph(), GraphMeta.now("g2"))

    # g1 should still be readable; g2 should not exist
    assert repo.get("g1") is not None
    assert repo.get("g2") is None
    # And no leftover .tmp- file in the directory
    tmp_files = list(root.glob(".tmp-*"))
    assert tmp_files == [], f"tempfile not cleaned up: {tmp_files}"


def test_file_event_skips_malformed_lines(tmp_path: Path):
    """Garbage lines in events.jsonl are logged + skipped, not fatal."""
    path = tmp_path / "events.jsonl"
    repo = FileEventRepository(path)
    repo.add([make_event("r1")])

    # Manually append a junk line
    with path.open("a", encoding="utf-8") as f:
        f.write("not-valid-json\n")

    repo.add([make_event("r2")])

    # The two real events should round-trip; the junk line is ignored
    all_events = repo.all()
    assert len(all_events) == 2
    assert {e.run.runId for e in all_events} == {"r1", "r2"}


def test_file_task_round_trips_complex_state(tmp_path: Path):
    """Tasks with progress lists, results, errors all serialise + round-trip cleanly."""
    repo = FileTaskRepository(tmp_path / "tasks")
    t = TaskInfo(task_id="t1", task_type=TaskType.EXTRACT)
    t.progress = ["step 1", "step 2"]
    t.result = {"graph_id": "g-abc", "node_count": 42}
    t.error = None
    repo.save(t)

    got = repo.get("t1")
    assert got is not None
    assert got.progress == ["step 1", "step 2"]
    assert got.result == {"graph_id": "g-abc", "node_count": 42}


def test_file_graph_count_only_counts_json_files(tmp_path: Path):
    """count() ignores tempfiles and the lock file."""
    root = tmp_path / "graphs"
    repo = FileGraphRepository(root)
    for i in range(3):
        repo.save(f"g{i}", make_graph(), GraphMeta.now(f"g{i}"))

    # Drop a stray tempfile + lock file to simulate concurrent writers
    (root / ".tmp-zzz").write_text("partial")
    (root / ".lock").write_text("")

    assert repo.count() == 3


def test_file_event_clear_removes_the_file(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    repo = FileEventRepository(path)
    repo.add([make_event("r1")])
    assert path.exists()

    repo.clear()

    assert not path.exists()
    assert repo.count() == 0


def test_file_event_run_event_json_round_trips_with_aliases(tmp_path: Path):
    """`schema` is a Pydantic alias for `schema_`; persistence must preserve it."""
    path = tmp_path / "events.jsonl"
    repo = FileEventRepository(path)
    event = RunEvent(
        eventType=RunEventType.START,
        eventTime=make_event().eventTime,
        run=Run(runId="r1"),
        job=Job(namespace="ns", name="j1"),
        producer="https://example.com",
        schemaURL="https://openlineage.io/spec/2-0-2/OpenLineage.json",
    )
    repo.add([event])

    repo2 = FileEventRepository(path)
    loaded = repo2.all()[0]
    assert loaded.eventType == RunEventType.START
    assert loaded.job.namespace == "ns"


# ── path-traversal protection (security review fix) ────────────────────


def test_file_graph_repository_rejects_path_traversal(tmp_path: Path):
    """Regression — graph_id with `..` or `/` in it must be rejected, not
    allowed to escape the storage root. Without this, an authenticated
    request with a crafted path-param could read/write arbitrary `.json`
    files on the filesystem."""
    repo = FileGraphRepository(tmp_path / "graphs")
    for malicious in ("../../etc/passwd", "..%2Fevil", "g/../escape", "../../"):
        with pytest.raises(ValueError, match="invalid characters"):
            repo.get(malicious)


def test_file_task_repository_rejects_path_traversal(tmp_path: Path):
    repo = FileTaskRepository(tmp_path / "tasks")
    with pytest.raises(ValueError, match="invalid characters"):
        repo.get("../../escape")


def test_file_repository_accepts_uuid_keys(tmp_path: Path):
    """UUID4 (hyphenated hex) is the canonical key shape and must pass."""
    import uuid
    repo = FileGraphRepository(tmp_path / "graphs")
    # Just exercising _path through a no-op .get() — no exception means OK.
    assert repo.get(str(uuid.uuid4())) is None

