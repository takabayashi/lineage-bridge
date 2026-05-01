# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pluggable storage layer for graphs, tasks, and OpenLineage events.

See ADR-022 for the design rationale and the deferred-future-work note on
postgres / S3 / redis backends.
"""

from __future__ import annotations

from lineage_bridge.storage.factory import Repositories, make_repositories
from lineage_bridge.storage.protocol import (
    EventRepository,
    GraphMeta,
    GraphRepository,
    TaskRepository,
)

__all__ = [
    "EventRepository",
    "GraphMeta",
    "GraphRepository",
    "Repositories",
    "TaskRepository",
    "make_repositories",
]
