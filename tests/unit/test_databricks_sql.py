# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for DatabricksSQLClient."""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from lineage_bridge.clients.databricks_sql import DatabricksSQLClient

WORKSPACE_URL = "https://acme-prod.cloud.databricks.com"
TOKEN = "dapi-test-token-123"
WAREHOUSE_ID = "abc123def456"
STATEMENTS_URL = f"{WORKSPACE_URL}/api/2.0/sql/statements"


@pytest.fixture()
def sql_client():
    return DatabricksSQLClient(
        workspace_url=WORKSPACE_URL,
        token=TOKEN,
        warehouse_id=WAREHOUSE_ID,
    )


@respx.mock
async def test_execute_success(sql_client):
    """Successful SQL execution returns result."""
    respx.post(STATEMENTS_URL).mock(
        return_value=Response(
            200,
            json={
                "statement_id": "stmt-1",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["42"]]},
            },
        )
    )

    result = await sql_client.execute("SELECT 42")
    assert result["status"]["state"] == "SUCCEEDED"
    assert result["result"]["data_array"] == [["42"]]


@respx.mock
async def test_execute_pending_then_success(sql_client, no_sleep):
    """Pending statement is polled until succeeded."""
    stmt_url = f"{STATEMENTS_URL}/stmt-1"

    respx.post(STATEMENTS_URL).mock(
        return_value=Response(
            200,
            json={
                "statement_id": "stmt-1",
                "status": {"state": "PENDING"},
            },
        )
    )
    respx.get(stmt_url).mock(
        return_value=Response(
            200,
            json={
                "statement_id": "stmt-1",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["done"]]},
            },
        )
    )

    result = await sql_client.execute("SELECT 1")
    assert result["status"]["state"] == "SUCCEEDED"


@respx.mock
async def test_execute_failed(sql_client):
    """Failed SQL execution returns error status."""
    respx.post(STATEMENTS_URL).mock(
        return_value=Response(
            200,
            json={
                "statement_id": "stmt-1",
                "status": {
                    "state": "FAILED",
                    "error": {"message": "Table not found"},
                },
            },
        )
    )

    result = await sql_client.execute("SELECT * FROM nonexistent")
    assert result["status"]["state"] == "FAILED"
    assert "Table not found" in result["status"]["error"]["message"]


@respx.mock
async def test_execute_batch(sql_client):
    """Batch execution runs statements sequentially."""
    respx.post(STATEMENTS_URL).mock(
        return_value=Response(
            200,
            json={
                "statement_id": "stmt-1",
                "status": {"state": "SUCCEEDED"},
            },
        )
    )

    results = await sql_client.execute_batch(["SELECT 1", "SELECT 2", "SELECT 3"])
    assert len(results) == 3
    assert all(r["status"]["state"] == "SUCCEEDED" for r in results)
