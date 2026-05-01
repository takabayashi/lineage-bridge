# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sample lineage graph used by the empty-state "Load Demo Graph" button.

Phase 2E: replaced the original 907-LOC imperative builder with a JSON
snapshot bundled at `ui/static/sample_graph.json`. The loader is trivial.
Regenerate with::

    uv run python -c "
    import json
    from lineage_bridge.models.graph import LineageGraph
    # ... build the LineageGraph however you want ...
    with open('lineage_bridge/ui/static/sample_graph.json', 'w') as f:
        json.dump(graph.to_dict(), f, indent=2, default=str)
    "
"""

from __future__ import annotations

from pathlib import Path

from lineage_bridge.models.graph import LineageGraph

_SAMPLE_PATH = Path(__file__).parent / "static" / "sample_graph.json"


def generate_sample_graph() -> LineageGraph:
    """Load the bundled sample lineage graph (small demo extraction snapshot)."""
    return LineageGraph.from_json_file(_SAMPLE_PATH)
