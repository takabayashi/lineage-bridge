# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""In-memory store for async task tracking."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


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
    """In-memory store for async task tracking.

    Tasks are created as PENDING, transition to RUNNING when work begins,
    and end as COMPLETED or FAILED.
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskInfo] = {}

    def create(self, task_type: TaskType, params: dict[str, Any] | None = None) -> TaskInfo:
        """Create a new pending task."""
        task = TaskInfo(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            params=params or {},
        )
        self._tasks[task.task_id] = task
        return task

    def get(self, task_id: str) -> TaskInfo | None:
        return self._tasks.get(task_id)

    def start(self, task_id: str) -> None:
        """Mark a task as running."""
        task = self._tasks.get(task_id)
        if task:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(UTC)

    def add_progress(self, task_id: str, message: str) -> None:
        """Append a progress message to a running task."""
        task = self._tasks.get(task_id)
        if task:
            task.progress.append(message)

    def complete(self, task_id: str, result: dict[str, Any] | None = None) -> None:
        """Mark a task as completed."""
        task = self._tasks.get(task_id)
        if task:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(UTC)
            task.result = result

    def fail(self, task_id: str, error: str) -> None:
        """Mark a task as failed."""
        task = self._tasks.get(task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = error

    def list_tasks(
        self,
        *,
        task_type: TaskType | None = None,
        status: TaskStatus | None = None,
        limit: int = 20,
    ) -> list[TaskInfo]:
        """List tasks with optional filters, most recent first."""
        tasks = list(self._tasks.values())
        if task_type:
            tasks = [t for t in tasks if t.task_type == task_type]
        if status:
            tasks = [t for t in tasks if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    @property
    def task_count(self) -> int:
        return len(self._tasks)
