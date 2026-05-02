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
from lineage_bridge.services.requests import ExtractionRequest
from lineage_bridge.services.watcher_models import (
    ExtractionRecord,
    WatcherConfig,
    WatcherEvent,
    WatcherStatus,
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
    MemoryWatcherRepository,
)
from lineage_bridge.storage.backends.sqlite import (
    SqliteEventRepository,
    SqliteGraphRepository,
    SqliteTaskRepository,
    SqliteWatcherRepository,
)

# ── factories: one per (backend, repo type) ─────────────────────────────


@pytest.fixture(params=["memory", "file", "sqlite"])
def graph_repo(request, tmp_path: Path):
    """Conformance-test fixture — yields a fresh GraphRepository per backend."""
    if request.param == "memory":
        return MemoryGraphRepository()
    if request.param == "file":
        return FileGraphRepository(tmp_path / "graphs")
    if request.param == "sqlite":
        return SqliteGraphRepository(tmp_path / "storage.db")
    raise NotImplementedError(request.param)


@pytest.fixture(params=["memory", "file", "sqlite"])
def task_repo(request, tmp_path: Path):
    if request.param == "memory":
        return MemoryTaskRepository()
    if request.param == "file":
        return FileTaskRepository(tmp_path / "tasks")
    if request.param == "sqlite":
        return SqliteTaskRepository(tmp_path / "storage.db")
    raise NotImplementedError(request.param)


@pytest.fixture(params=["memory", "file", "sqlite"])
def event_repo(request, tmp_path: Path):
    if request.param == "memory":
        return MemoryEventRepository()
    if request.param == "file":
        return FileEventRepository(tmp_path / "events.jsonl")
    if request.param == "sqlite":
        return SqliteEventRepository(tmp_path / "storage.db")
    raise NotImplementedError(request.param)


# Watcher repo: only memory + sqlite (no file backend implementation —
# sqlite is the durable path for watchers per Phase 2G).
@pytest.fixture(params=["memory", "sqlite"])
def watcher_repo(request, tmp_path: Path):
    if request.param == "memory":
        return MemoryWatcherRepository()
    if request.param == "sqlite":
        return SqliteWatcherRepository(tmp_path / "storage.db")
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


def make_watcher_config(env_id: str = "env-test") -> WatcherConfig:
    return WatcherConfig(
        extraction=ExtractionRequest(environment_ids=[env_id]),
    )


def make_watcher_status(watcher_id: str = "w-1") -> WatcherStatus:
    return WatcherStatus(watcher_id=watcher_id)


def make_watcher_event(watcher_id: str = "w-1") -> WatcherEvent:
    """Reuses the AuditEvent shape — the watcher's event feed IS audit events."""
    return WatcherEvent(
        id=f"e-{watcher_id}-{datetime.now(UTC).timestamp()}",
        time=datetime.now(UTC),
        method_name="kafka.CreateTopics",
        resource_name="topic-x",
        principal="u:test",
        raw={},
    )


def make_extraction_record() -> ExtractionRecord:
    return ExtractionRecord(triggered_at=datetime.now(UTC))


@pytest.fixture()
def graph_factory():
    return make_graph


@pytest.fixture()
def task_factory():
    return make_task


@pytest.fixture()
def watcher_config_factory():
    return make_watcher_config


@pytest.fixture()
def watcher_status_factory():
    return make_watcher_status


@pytest.fixture()
def watcher_event_factory():
    return make_watcher_event


@pytest.fixture()
def extraction_record_factory():
    return make_extraction_record


@pytest.fixture()
def event_factory():
    return make_event
