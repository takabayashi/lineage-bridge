# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Per-phase extraction modules.

Two groups live here:

  - **Pipeline phases** implement `ExtractionPhase` and run inside
    `PhaseRunner` per environment. The orchestrator composes them in order.
  - **Post-extraction enrichers** are plain async functions called directly
    by `run_enrichment()`. They run once across the merged graph (after every
    environment has been extracted), so they take `(settings, graph)` rather
    than an `ExtractionContext`. They live here because the plan groups them
    as "Phase 4b" (catalog) and "Phase 5" (metrics), but they are not phases
    in the runner sense.
"""

from __future__ import annotations

from lineage_bridge.extractors.phases.catalog_enrichment import run_catalog_enrichment
from lineage_bridge.extractors.phases.enrichment import SchemaEnrichmentPhase
from lineage_bridge.extractors.phases.kafka_admin import KafkaAdminPhase
from lineage_bridge.extractors.phases.metrics import run_metrics_enrichment
from lineage_bridge.extractors.phases.processing import ProcessingPhase
from lineage_bridge.extractors.phases.tableflow import TableflowPhase

# Pipeline phases (used by PhaseRunner)
__all__ = [
    "KafkaAdminPhase",
    "ProcessingPhase",
    "SchemaEnrichmentPhase",
    "TableflowPhase",
    # Post-extraction enrichers (called directly)
    "run_catalog_enrichment",
    "run_metrics_enrichment",
]
