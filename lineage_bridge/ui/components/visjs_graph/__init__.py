# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Custom vis.js graph component with region-select zoom."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import streamlit.components.v1 as components

_COMPONENT_DIR = Path(__file__).parent

_component = components.declare_component("visjs_graph", path=str(_COMPONENT_DIR))


def visjs_graph(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    config: dict[str, Any],
    height: int = 650,
    key: str | None = None,
) -> dict[str, Any] | str | None:
    """Render a vis.js network graph with region-select zoom.

    Returns one of:
      - ``{"action": "select", "id": "...", "seq": N}`` on a single click
      - ``{"action": "focus",  "id": "...", "seq": N}`` on a double click
      - ``None`` when nothing has been clicked yet
      - A bare string ``"<node_id>"`` for back-compat with older renders that
        haven't yet upgraded their payload (callers should treat as "select").
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
