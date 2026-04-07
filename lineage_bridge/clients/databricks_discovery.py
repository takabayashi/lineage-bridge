# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks workspace discovery utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass
class WarehouseInfo:
    """Summary of a Databricks SQL Warehouse."""

    id: str
    name: str
    state: str  # RUNNING, STARTING, STOPPED, STOPPING, DELETED, DELETING
    cluster_size: str | None = None
    creator_name: str | None = None


async def list_warehouses(
    workspace_url: str,
    token: str,
) -> list[WarehouseInfo]:
    """List all SQL Warehouses in the workspace.

    Returns warehouses sorted with RUNNING ones first.
    """
    url = f"{workspace_url.rstrip('/')}/api/2.0/sql/warehouses"
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    warehouses = []
    for wh in data.get("warehouses", []):
        warehouses.append(
            WarehouseInfo(
                id=wh.get("id", ""),
                name=wh.get("name", ""),
                state=wh.get("state", "UNKNOWN"),
                cluster_size=wh.get("cluster_size"),
                creator_name=wh.get("creator_name"),
            )
        )

    # Sort: RUNNING first, then by name
    state_order = {"RUNNING": 0, "STARTING": 1, "STOPPED": 2}
    warehouses.sort(key=lambda w: (state_order.get(w.state, 9), w.name))
    return warehouses


def pick_running_warehouse(warehouses: list[WarehouseInfo]) -> WarehouseInfo | None:
    """Select the first RUNNING warehouse, or the first one available."""
    if not warehouses:
        return None
    for wh in warehouses:
        if wh.state == "RUNNING":
            return wh
    return warehouses[0]
