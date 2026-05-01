# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Shared fixtures for the storage conformance suite.

Each backend gets a parametrized fixture so the same conformance tests run
against memory + file (and, after Phase 2F, sqlite). Adding a backend = one
fixture entry, no new tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lineage_bridge.api.task_store import TaskInfo, TaskType
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.openlineage.models import (
    Job,
    Run,
    RunEvent,
    RunEventType,
)
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

# ── factories: one per (backend, repo type) ─────────────────────────────


@pytest.fixture(params=["memory", "file"])
def graph_repo(request, tmp_path: Path):
    """Conformance-test fixture — yields a fresh GraphRepository per backend."""
    if request.param == "memory":
        return MemoryGraphRepository()
    if request.param == "file":
        return FileGraphRepository(tmp_path / "graphs")
    raise NotImplementedError(request.param)


@pytest.fixture(params=["memory", "file"])
def task_repo(request, tmp_path: Path):
    if request.param == "memory":
        return MemoryTaskRepository()
    if request.param == "file":
        return FileTaskRepository(tmp_path / "tasks")
    raise NotImplementedError(request.param)


@pytest.fixture(params=["memory", "file"])
def event_repo(request, tmp_path: Path):
    if request.param == "memory":
        return MemoryEventRepository()
    if request.param == "file":
        return FileEventRepository(tmp_path / "events.jsonl")
    raise NotImplementedError(request.param)


# ── object factories ────────────────────────────────────────────────────


def make_graph(node_count: int = 1) -> LineageGraph:
    g = LineageGraph()
    for i in range(node_count):
        g.add_node(
            LineageNode(
                node_id=f"confluent:kafka_topic:env-test:topic-{i}",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name=f"topic-{i}",
                display_name=f"topic-{i}",
                environment_id="env-test",
                cluster_id="lkc-test",
            )
        )
    if node_count >= 2:
        g.add_edge(
            LineageEdge(
                src_id="confluent:kafka_topic:env-test:topic-0",
                dst_id="confluent:kafka_topic:env-test:topic-1",
                edge_type=EdgeType.PRODUCES,
            )
        )
    return g


def make_task(task_id: str = "task-1", task_type: TaskType = TaskType.EXTRACT) -> TaskInfo:
    return TaskInfo(task_id=task_id, task_type=task_type)


def make_event(run_id: str = "run-1", job_name: str = "job-1") -> RunEvent:
    return RunEvent(
        eventType=RunEventType.COMPLETE,
        eventTime=datetime.now(UTC),
        run=Run(runId=run_id),
        job=Job(namespace="ns", name=job_name),
        producer="https://example.com/lineage-bridge",
        schemaURL="https://openlineage.io/spec/2-0-2/OpenLineage.json",
    )


@pytest.fixture()
def graph_factory():
    return make_graph


@pytest.fixture()
def task_factory():
    return make_task


@pytest.fixture()
def event_factory():
    return make_event
