# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for Databricks workspace discovery."""

from __future__ import annotations

import respx
from httpx import Response

from lineage_bridge.clients.databricks_discovery import (
    WarehouseInfo,
    list_warehouses,
    pick_running_warehouse,
)

WORKSPACE_URL = "https://acme-prod.cloud.databricks.com"
TOKEN = "dapi-test-token-123"
WAREHOUSES_URL = f"{WORKSPACE_URL}/api/2.0/sql/warehouses"


@respx.mock
async def test_list_warehouses_returns_sorted():
    """list_warehouses returns warehouses with RUNNING first."""
    respx.get(WAREHOUSES_URL).mock(
        return_value=Response(
            200,
            json={
                "warehouses": [
                    {"id": "wh-stopped", "name": "Stopped WH", "state": "STOPPED"},
                    {"id": "wh-running", "name": "Active WH", "state": "RUNNING"},
                    {"id": "wh-starting", "name": "Starting WH", "state": "STARTING"},
                ]
            },
        )
    )

    result = await list_warehouses(WORKSPACE_URL, TOKEN)
    assert len(result) == 3
    assert result[0].id == "wh-running"
    assert result[0].state == "RUNNING"
    assert result[1].state == "STARTING"
    assert result[2].state == "STOPPED"


@respx.mock
async def test_list_warehouses_empty():
    """list_warehouses returns empty list when no warehouses exist."""
    respx.get(WAREHOUSES_URL).mock(
        return_value=Response(200, json={"warehouses": []})
    )

    result = await list_warehouses(WORKSPACE_URL, TOKEN)
    assert result == []


def test_pick_running_warehouse_selects_running():
    """pick_running_warehouse prefers RUNNING warehouse."""
    warehouses = [
        WarehouseInfo(id="wh-1", name="Stopped", state="STOPPED"),
        WarehouseInfo(id="wh-2", name="Running", state="RUNNING"),
    ]
    selected = pick_running_warehouse(warehouses)
    assert selected is not None
    assert selected.id == "wh-2"


def test_pick_running_warehouse_falls_back():
    """pick_running_warehouse falls back to first warehouse when none running."""
    warehouses = [
        WarehouseInfo(id="wh-1", name="Stopped", state="STOPPED"),
    ]
    selected = pick_running_warehouse(warehouses)
    assert selected is not None
    assert selected.id == "wh-1"


def test_pick_running_warehouse_empty():
    """pick_running_warehouse returns None when list is empty."""
    assert pick_running_warehouse([]) is None
