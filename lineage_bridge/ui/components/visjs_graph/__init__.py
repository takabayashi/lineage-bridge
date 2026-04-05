# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Custom vis.js graph component with region-select zoom."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import streamlit.components.v1 as components

_COMPONENT_DIR = Path(__file__).parent

_component = components.declare_component(
    "visjs_graph", path=str(_COMPONENT_DIR)
)


def visjs_graph(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    height: int = 650,
    key: str | None = None,
) -> str | None:
    """Render a vis.js network graph with region-select zoom.

    Returns the clicked node ID (str) or None.
    """
    data_json = json.dumps({"nodes": nodes, "edges": edges})
    config_json = json.dumps(config)

    return _component(
        data=data_json,
        config=config_json,
        graph_height=height,
        key=key,
        default=None,
    )
