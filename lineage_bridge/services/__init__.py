# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Service layer — single entry point for extraction, enrichment, and push.

The UI and the API both call into this layer with the same `ExtractionRequest`
/ `EnrichmentRequest` / `PushRequest` Pydantic models, eliminating the
divergent signatures that previously lived in `ui/extraction.py` and
`api/routers/tasks.py`.

See ADR-020 for the design rationale.
"""

from __future__ import annotations

from lineage_bridge.services.enrichment_service import run_enrichment
from lineage_bridge.services.extraction_service import run_extraction
from lineage_bridge.services.push_service import PUSH_PROVIDERS, run_push
from lineage_bridge.services.request_builder import build_extraction_request
from lineage_bridge.services.requests import (
    EnrichmentRequest,
    ExtractionRequest,
    PushRequest,
)

__all__ = [
    "PUSH_PROVIDERS",
    "EnrichmentRequest",
    "ExtractionRequest",
    "PushRequest",
    "build_extraction_request",
    "run_enrichment",
    "run_extraction",
    "run_push",
]
