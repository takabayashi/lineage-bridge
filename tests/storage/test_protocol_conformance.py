# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Repository protocol conformance suite.

Every backend (memory, file, soon sqlite) must pass these tests. Failures
here mean the backend doesn't honor the contract every Store + adapter is
written against.
"""

from __future__ import annotations

import time

from lineage_bridge.storage.protocol import GraphMeta

# ── GraphRepository ─────────────────────────────────────────────────────


def test_graph_save_get_roundtrip(graph_repo, graph_factory):
    g = graph_factory(node_count=3)
    meta = GraphMeta.now("g1")

    graph_repo.save("g1", g, meta)
    loaded = graph_repo.get("g1")

    assert loaded is not None
    got_graph, got_meta = loaded
    assert got_graph.node_count == 3
    assert got_meta.graph_id == "g1"
    assert got_meta.created_at == meta.created_at


def test_graph_get_unknown_returns_none(graph_repo):
    assert graph_repo.get("missing") is None


def test_graph_list_meta_empty(graph_repo):
    assert graph_repo.list_meta() == []


def test_graph_list_meta_returns_all(graph_repo, graph_factory):
    for i in range(3):
        graph_repo.save(f"g{i}", graph_factory(), GraphMeta.now(f"g{i}"))

    metas = graph_repo.list_meta()
    assert {m.graph_id for m in metas} == {"g0", "g1", "g2"}


def test_graph_delete_returns_true_when_present(graph_repo, graph_factory):
    graph_repo.save("g1", graph_factory(), GraphMeta.now("g1"))
    assert graph_repo.delete("g1") is True
    assert graph_repo.get("g1") is None


def test_graph_delete_returns_false_when_missing(graph_repo):
    assert graph_repo.delete("never-existed") is False


def test_graph_touch_updates_last_modified(graph_repo, graph_factory):
    meta = GraphMeta.now("g1")
    graph_repo.save("g1", graph_factory(), meta)
    initial_modified = meta.last_modified

    time.sleep(0.01)  # ensure datetime tick
    graph_repo.touch("g1")

    loaded = graph_repo.get("g1")
    assert loaded is not None
    _, new_meta = loaded
    assert new_meta.last_modified > initial_modified


def test_graph_touch_unknown_is_a_noop(graph_repo):
    graph_repo.touch("never-existed")  # should not raise


def test_graph_count(graph_repo, graph_factory):
    assert graph_repo.count() == 0
    for i in range(2):
        graph_repo.save(f"g{i}", graph_factory(), GraphMeta.now(f"g{i}"))
    assert graph_repo.count() == 2


def test_graph_save_overwrites_existing(graph_repo, graph_factory):
    """save() with an existing key replaces the prior graph."""
    graph_repo.save("g1", graph_factory(node_count=1), GraphMeta.now("g1"))
    graph_repo.save("g1", graph_factory(node_count=5), GraphMeta.now("g1"))

    loaded = graph_repo.get("g1")
    assert loaded is not None
    g, _ = loaded
    assert g.node_count == 5


# ── TaskRepository ──────────────────────────────────────────────────────


def test_task_save_get_roundtrip(task_repo, task_factory):
    t = task_factory("t1")
    task_repo.save(t)
    got = task_repo.get("t1")
    assert got is not None
    assert got.task_id == "t1"


def test_task_get_unknown_returns_none(task_repo):
    assert task_repo.get("missing") is None


def test_task_list_returns_all(task_repo, task_factory):
    for i in range(3):
        task_repo.save(task_factory(f"t{i}"))
    assert {t.task_id for t in task_repo.list()} == {"t0", "t1", "t2"}


def test_task_save_overwrites_existing(task_repo, task_factory):
    """Re-saving the same task_id reflects the latest state (used by start/complete/fail)."""
    t = task_factory("t1")
    task_repo.save(t)
    t.progress.append("step 1")
    task_repo.save(t)
    got = task_repo.get("t1")
    assert got is not None
    assert got.progress == ["step 1"]


def test_task_delete(task_repo, task_factory):
    task_repo.save(task_factory("t1"))
    assert task_repo.delete("t1") is True
    assert task_repo.get("t1") is None
    assert task_repo.delete("t1") is False


def test_task_count(task_repo, task_factory):
    assert task_repo.count() == 0
    for i in range(4):
        task_repo.save(task_factory(f"t{i}"))
    assert task_repo.count() == 4


# ── EventRepository ─────────────────────────────────────────────────────


def test_event_add_returns_count(event_repo, event_factory):
    n = event_repo.add([event_factory("r1"), event_factory("r2")])
    assert n == 2


def test_event_add_empty_is_zero(event_repo):
    assert event_repo.add([]) == 0


def test_event_all_returns_in_insertion_order(event_repo, event_factory):
    event_repo.add([event_factory("r1", "j1")])
    event_repo.add([event_factory("r2", "j2")])
    all_events = event_repo.all()
    assert [e.run.runId for e in all_events] == ["r1", "r2"]


def test_event_by_run_id(event_repo, event_factory):
    event_repo.add(
        [event_factory("r1", "j1"), event_factory("r1", "j1"), event_factory("r2", "j2")]
    )
    assert len(event_repo.by_run_id("r1")) == 2
    assert len(event_repo.by_run_id("r2")) == 1
    assert event_repo.by_run_id("missing") == []


def test_event_count(event_repo, event_factory):
    assert event_repo.count() == 0
    event_repo.add([event_factory("r1"), event_factory("r2")])
    assert event_repo.count() == 2


def test_event_clear(event_repo, event_factory):
    event_repo.add([event_factory("r1")])
    assert event_repo.count() == 1
    event_repo.clear()
    assert event_repo.count() == 0
    assert event_repo.all() == []
