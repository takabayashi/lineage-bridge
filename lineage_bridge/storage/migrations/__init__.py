# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""SQLite schema migrations.

Versioned `.sql` files in this package, applied in order on first connect.
Each migration's `version` is its file-name prefix (`001_*.sql` -> 1). The
runner records applied versions in `schema_migrations`, so subsequent opens
skip already-applied files.
"""

from __future__ import annotations

from lineage_bridge.storage.migrations.runner import apply_pending

__all__ = ["apply_pending"]
