# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the task store."""

from __future__ import annotations

from lineage_bridge.api.task_store import TaskStatus, TaskStore, TaskType


class TestTaskCreation:
    def test_create_returns_pending_task(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        assert task.status == TaskStatus.PENDING
        assert task.task_type == TaskType.EXTRACT
        assert task.task_id

    def test_create_with_params(self):
        store = TaskStore()
        task = store.create(TaskType.ENRICH, {"graph_id": "g1"})
        assert task.params == {"graph_id": "g1"}

    def test_unique_ids(self):
        store = TaskStore()
        t1 = store.create(TaskType.EXTRACT)
        t2 = store.create(TaskType.EXTRACT)
        assert t1.task_id != t2.task_id


class TestTaskLifecycle:
    def test_start_sets_running(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        store.start(task.task_id)
        assert store.get(task.task_id).status == TaskStatus.RUNNING
        assert store.get(task.task_id).started_at is not None

    def test_complete_sets_completed(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        store.start(task.task_id)
        store.complete(task.task_id, {"nodes": 10})
        t = store.get(task.task_id)
        assert t.status == TaskStatus.COMPLETED
        assert t.result == {"nodes": 10}
        assert t.completed_at is not None

    def test_fail_sets_failed(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        store.start(task.task_id)
        store.fail(task.task_id, "connection refused")
        t = store.get(task.task_id)
        assert t.status == TaskStatus.FAILED
        assert t.error == "connection refused"
        assert t.completed_at is not None


class TestProgress:
    def test_add_progress_messages(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        store.start(task.task_id)
        store.add_progress(task.task_id, "Step 1")
        store.add_progress(task.task_id, "Step 2")
        t = store.get(task.task_id)
        assert t.progress == ["Step 1", "Step 2"]


class TestListTasks:
    def test_list_all(self):
        store = TaskStore()
        store.create(TaskType.EXTRACT)
        store.create(TaskType.ENRICH)
        assert len(store.list_tasks()) == 2

    def test_filter_by_type(self):
        store = TaskStore()
        store.create(TaskType.EXTRACT)
        store.create(TaskType.ENRICH)
        result = store.list_tasks(task_type=TaskType.EXTRACT)
        assert len(result) == 1
        assert result[0].task_type == TaskType.EXTRACT

    def test_filter_by_status(self):
        store = TaskStore()
        t1 = store.create(TaskType.EXTRACT)
        store.create(TaskType.EXTRACT)
        store.start(t1.task_id)
        result = store.list_tasks(status=TaskStatus.RUNNING)
        assert len(result) == 1

    def test_limit(self):
        store = TaskStore()
        for _ in range(10):
            store.create(TaskType.EXTRACT)
        result = store.list_tasks(limit=3)
        assert len(result) == 3

    def test_most_recent_first(self):
        store = TaskStore()
        t1 = store.create(TaskType.EXTRACT)
        t2 = store.create(TaskType.EXTRACT)
        result = store.list_tasks()
        assert result[0].task_id == t2.task_id
        assert result[1].task_id == t1.task_id


class TestGetTask:
    def test_get_existing(self):
        store = TaskStore()
        task = store.create(TaskType.EXTRACT)
        assert store.get(task.task_id) is not None

    def test_get_nonexistent(self):
        store = TaskStore()
        assert store.get("nonexistent") is None


class TestTaskCount:
    def test_empty(self):
        store = TaskStore()
        assert store.task_count == 0

    def test_after_creation(self):
        store = TaskStore()
        store.create(TaskType.EXTRACT)
        store.create(TaskType.ENRICH)
        assert store.task_count == 2
