# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Async client for the Databricks Statement Execution API."""

from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 2.0
_MAX_POLLS = 30


class DatabricksSQLClient:
    """Execute SQL statements on a Databricks SQL Warehouse.

    Uses the Statement Execution API:
    https://docs.databricks.com/api/workspace/statementexecution
    """

    def __init__(self, workspace_url: str, token: str, warehouse_id: str) -> None:
        self._workspace_url = workspace_url.rstrip("/")
        self._token = token
        self._warehouse_id = warehouse_id

    async def execute(self, sql: str, *, wait: bool = True) -> dict:
        """Execute a SQL statement and return the result.

        Args:
            sql: The SQL statement to execute.
            wait: If True, poll until the statement completes.

        Returns:
            The full statement result dict from the API.
        """
        async with httpx.AsyncClient(
            base_url=self._workspace_url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=60.0,
        ) as client:
            body = {
                "warehouse_id": self._warehouse_id,
                "statement": sql,
                "wait_timeout": "30s" if wait else "0s",
            }
            resp = await client.post("/api/2.0/sql/statements", json=body)
            resp.raise_for_status()
            result = resp.json()

            status = result.get("status", {}).get("state", "")

            if wait and status == "PENDING":
                stmt_id = result["statement_id"]
                for _ in range(_MAX_POLLS):
                    await asyncio.sleep(_POLL_INTERVAL)
                    resp = await client.get(f"/api/2.0/sql/statements/{stmt_id}")
                    resp.raise_for_status()
                    result = resp.json()
                    status = result.get("status", {}).get("state", "")
                    if status in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                        break

            if status == "FAILED":
                error_msg = result.get("status", {}).get("error", {}).get("message", "unknown")
                # Logged at WARNING — the caller (e.g. DatabricksUCProvider) is
                # the only one positioned to decide whether this is a real push
                # error or a benign skip (cross-owner catalog spill, missing
                # bridge schema before CREATE retry, etc.). Logging at ERROR
                # here painted every benign skip as an alarming failure in
                # the UI log even when the push result was clean.
                logger.warning("SQL statement failed: %s — %s", sql[:100], error_msg)

            return result

    async def execute_batch(self, statements: list[str]) -> list[dict]:
        """Execute multiple SQL statements sequentially.

        Returns a list of results, one per statement.
        """
        results = []
        for sql in statements:
            result = await self.execute(sql)
            results.append(result)
        return results
