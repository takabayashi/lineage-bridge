# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pydantic models for the watcher service (Phase 2G).

Five models, each Pydantic so they round-trip cleanly through the storage
layer and the REST API:

- `WatcherConfig`     — what to poll + how often + what to push on extraction
- `WatcherStatus`     — current state machine snapshot (poll counts, cooldown)
- `WatcherSummary`    — id-level summary for `GET /api/v1/watcher` listings
- `WatcherEvent`      — alias for `models.AuditEvent` (the change events the
                        poller / audit-consumer produces)
- `ExtractionRecord`  — one watcher-triggered extraction's outcome

Reusing `AuditEvent` rather than introducing a new schema keeps the existing
poller / audit-consumer wiring intact — they already produce that shape.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from lineage_bridge.models.audit_event import AuditEvent
from lineage_bridge.services.requests import ExtractionRequest

# Reuse the existing audit-event shape — every WatcherEvent IS an AuditEvent.
WatcherEvent = AuditEvent


class WatcherState(StrEnum):
    STOPPED = "stopped"
    WATCHING = "watching"
    COOLDOWN = "cooldown"
    EXTRACTING = "extracting"


class WatcherMode(StrEnum):
    """Detection mode — audit log Kafka consumer or REST API state-diffing."""

    AUDIT_LOG = "audit_log"
    REST_POLLING = "rest_polling"


class WatcherConfig(BaseModel):
    """What to watch, how, and what to push on each triggered extraction.

    Frozen so a config snapshot can safely be passed across threads / persisted
    without callers mutating it underneath the runner. The push flags are
    explicit booleans (not a list) so the JSON wire format is stable when new
    catalogs land.
    """

    model_config = ConfigDict(frozen=True)

    # Detection wiring
    mode: WatcherMode = WatcherMode.REST_POLLING
    poll_interval_seconds: float = Field(default=10.0, gt=0)
    cooldown_seconds: float = Field(default=30.0, ge=0)

    # Audit-log mode credentials (only used when mode == AUDIT_LOG)
    audit_log_bootstrap_servers: str | None = None
    audit_log_api_key: str | None = None
    audit_log_api_secret: str | None = None

    # Extraction request — fed to services.run_extraction on each trigger.
    extraction: ExtractionRequest

    # Per-provider push flags (any True → push runs after extraction).
    push_databricks_uc: bool = False
    push_aws_glue: bool = False
    push_google: bool = False
    push_datazone: bool = False


class WatcherStatus(BaseModel):
    """Snapshot of one watcher's runtime state.

    Persisted on every tick so any UI can read it without holding the runner
    in-process. `cooldown_remaining_seconds` is computed at write time so
    the UI doesn't need a clock — it just shows the most recent value.
    """

    watcher_id: str
    state: WatcherState = WatcherState.STOPPED
    started_at: datetime | None = None
    last_poll_at: datetime | None = None
    last_extraction_at: datetime | None = None
    poll_count: int = 0
    event_count: int = 0
    extraction_count: int = 0
    cooldown_remaining_seconds: float = 0.0
    last_error: str | None = None


class WatcherSummary(BaseModel):
    """Id-level summary for `GET /api/v1/watcher` (list endpoint).

    Subset of WatcherStatus plus the WatcherConfig.environment IDs so the
    listing UI can show "Watcher abc123 — env-abc, watching, 47 polls" without
    a second round-trip per row.
    """

    watcher_id: str
    state: WatcherState
    started_at: datetime | None = None
    environment_ids: list[str] = Field(default_factory=list)
    poll_count: int = 0
    event_count: int = 0


class ExtractionRecord(BaseModel):
    """One watcher-triggered extraction's outcome.

    Stored append-only per watcher so the UI can show "last 50 extractions"
    without retaining the full graph each time. `node_count` / `edge_count`
    are summary stats; the actual graph (if needed) lives in GraphRepository
    separately.
    """

    triggered_at: datetime
    completed_at: datetime | None = None
    trigger_event_count: int = 0
    node_count: int = 0
    edge_count: int = 0
    error: str | None = None
    graph_id: str | None = None  # Reference to GraphRepository if persisted

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.triggered_at).total_seconds()


__all__ = [
    "ExtractionRecord",
    "WatcherConfig",
    "WatcherEvent",
    "WatcherMode",
    "WatcherState",
    "WatcherStatus",
    "WatcherSummary",
]
