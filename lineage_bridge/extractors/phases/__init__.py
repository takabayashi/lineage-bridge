# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Per-phase extraction modules.

Each module exports a single phase class implementing `ExtractionPhase`.
The orchestrator composes them into a `PhaseRunner` and runs them in order.
"""

from __future__ import annotations

from lineage_bridge.extractors.phases.catalog_enrichment import CatalogEnrichmentPhase
from lineage_bridge.extractors.phases.enrichment import SchemaEnrichmentPhase
from lineage_bridge.extractors.phases.kafka_admin import KafkaAdminPhase
from lineage_bridge.extractors.phases.metrics import MetricsPhase
from lineage_bridge.extractors.phases.processing import ProcessingPhase
from lineage_bridge.extractors.phases.tableflow import TableflowPhase

__all__ = [
    "CatalogEnrichmentPhase",
    "KafkaAdminPhase",
    "MetricsPhase",
    "ProcessingPhase",
    "SchemaEnrichmentPhase",
    "TableflowPhase",
]
