# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""TaskStore — thin adapter over a `TaskRepository`.

Public API unchanged from the pre-Phase-1C in-memory implementation so the
API routers (`api/routers/tasks.py`) need no changes. The repository
persists; the store does the lifecycle calls (`start` / `complete` / `fail`)
and filtering (`list_tasks`) as before.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from lineage_bridge.storage.protocol import TaskRepository


class TaskStatus(StrEnum):
    """Lifecycle states for an async task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskType(StrEnum):
    """Types of async operations."""

    EXTRACT = "extract"
    ENRICH = "enrich"


class TaskInfo(BaseModel):
    """Metadata and state for a single async task."""

    task_id: str
    task_type: TaskType
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    progress: list[str] = Field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class TaskStore:
    """Async task tracking, persisted via a `TaskRepository`.

    Tasks are created PENDING, transition to RUNNING, and end COMPLETED or
    FAILED. Each lifecycle method re-saves the task so the repository sees
    every state transition (important for the file backend, where state
    survives process restart).
    """

    def __init__(self, repo: TaskRepository | None = None) -> None:
        # Late import so `TaskInfo` etc. can be imported without dragging in
        # the storage backends transitively.
        from lineage_bridge.storage.backends.memory import MemoryTaskRepository

        self._repo: TaskRepository = repo or MemoryTaskRepository()

    def create(self, task_type: TaskType, params: dict[str, Any] | None = None) -> TaskInfo:
        task = TaskInfo(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            params=params or {},
        )
        self._repo.save(task)
        return task

    def get(self, task_id: str) -> TaskInfo | None:
        return self._repo.get(task_id)

    def start(self, task_id: str) -> None:
        task = self._repo.get(task_id)
        if task:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(UTC)
            self._repo.save(task)

    def add_progress(self, task_id: str, message: str) -> None:
        task = self._repo.get(task_id)
        if task:
            task.progress.append(message)
            self._repo.save(task)

    def complete(self, task_id: str, result: dict[str, Any] | None = None) -> None:
        task = self._repo.get(task_id)
        if task:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(UTC)
            task.result = result
            self._repo.save(task)

    def fail(self, task_id: str, error: str) -> None:
        task = self._repo.get(task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = error
            self._repo.save(task)

    def list_tasks(
        self,
        *,
        task_type: TaskType | None = None,
        status: TaskStatus | None = None,
        limit: int = 20,
    ) -> list[TaskInfo]:
        """List tasks with optional filters, most recent first."""
        tasks = self._repo.list()
        if task_type:
            tasks = [t for t in tasks if t.task_type == task_type]
        if status:
            tasks = [t for t in tasks if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    @property
    def task_count(self) -> int:
        return self._repo.count()
