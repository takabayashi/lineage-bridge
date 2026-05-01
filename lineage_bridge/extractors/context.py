# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""ExtractionContext — shared state passed across phases for one environment.

The context is built by the orchestrator's discovery step (cluster list +
Schema Registry endpoint + credential resolution) and then handed to each
phase's `execute(ctx)` call. Phases read from it and mutate `ctx.graph`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lineage_bridge.clients.base import ConfluentClient
    from lineage_bridge.config.settings import Settings
    from lineage_bridge.models.graph import LineageGraph


# Callable that receives (phase_label, detail_message)
ProgressCallback = Any  # typing: Callable[[str, str], None] | None


@dataclass
class ExtractionContext:
    """Shared state for one environment's extraction run."""

    settings: Settings
    cloud: ConfluentClient
    env_id: str
    graph: LineageGraph

    # ── discovery output (populated before phases run) ─────────────────
    clusters: list[dict[str, Any]] = field(default_factory=list)
    sr_endpoint: str | None = None
    sr_key: str | None = None
    sr_secret: str | None = None

    # ── per-env credential overrides (passed in by caller) ─────────────
    sr_credentials: dict[str, dict[str, str]] | None = None
    flink_credentials: dict[str, dict[str, str]] | None = None

    # ── phase enable flags ─────────────────────────────────────────────
    enable_connect: bool = True
    enable_ksqldb: bool = True
    enable_flink: bool = True
    enable_schema_registry: bool = True
    enable_stream_catalog: bool = True
    enable_tableflow: bool = True

    # ── progress reporting ─────────────────────────────────────────────
    on_progress: ProgressCallback = None

    def progress(self, phase: str, detail: str = "") -> None:
        """Forward a progress event to the configured callback (no-op if absent)."""
        if self.on_progress:
            self.on_progress(phase, detail)
