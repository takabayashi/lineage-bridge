#!/usr/bin/env python3
# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Test Databricks lineage tracking by creating a simple derived table pipeline.

Steps:
  1. List SQL warehouses and pick one
  2. List available tables in the demo catalog
  3. Create a derived table (CTAS) from Tableflow-materialized tables
  4. Query the Databricks Lineage Tracking API to inspect captured lineage

Usage:
  uv run python scripts/test_databricks_lineage.py
"""

from __future__ import annotations

import json
import os
import sys
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

WORKSPACE_URL = os.environ["LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL"].rstrip("/")
TOKEN = os.environ["LINEAGE_BRIDGE_DATABRICKS_TOKEN"]

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def api_get(path: str) -> dict:
    resp = httpx.get(f"{WORKSPACE_URL}{path}", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str, json_body: dict) -> dict:
    resp = httpx.post(f"{WORKSPACE_URL}{path}", headers=HEADERS, json=json_body, timeout=60)
    resp.raise_for_status()
    return resp.json()


def api_delete(path: str) -> None:
    resp = httpx.delete(f"{WORKSPACE_URL}{path}", headers=HEADERS, timeout=30)
    if resp.status_code not in (200, 204, 404):
        resp.raise_for_status()


# ── Step 1: Find a SQL warehouse ─────────────────────────────────────────


def find_warehouse() -> str | None:
    """Find a running SQL warehouse, or return None."""
    data = api_get("/api/2.0/sql/warehouses")
    warehouses = data.get("warehouses", [])
    for wh in warehouses:
        state = wh.get("state", "")
        print(f"  Warehouse: {wh['name']} (id={wh['id']}, state={state})")
        if state == "RUNNING":
            return wh["id"]
    # Try starting the first one if none running
    if warehouses:
        wh = warehouses[0]
        print(f"\n  Starting warehouse '{wh['name']}'...")
        api_post(f"/api/2.0/sql/warehouses/{wh['id']}/start", {})
        # Wait for it to start
        for _ in range(30):
            time.sleep(5)
            info = api_get(f"/api/2.0/sql/warehouses/{wh['id']}")
            if info.get("state") == "RUNNING":
                return wh["id"]
            print(f"  Waiting... state={info.get('state')}")
    return None


# ── Step 2: Execute SQL ──────────────────────────────────────────────────


def execute_sql(warehouse_id: str, sql: str, wait: bool = True) -> dict:
    """Execute SQL via the Statement Execution API."""
    body = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": "30s" if wait else "0s",
    }
    result = api_post("/api/2.0/sql/statements", body)
    status = result.get("status", {}).get("state", "")

    if wait and status == "PENDING":
        stmt_id = result["statement_id"]
        for _ in range(30):
            time.sleep(2)
            result = api_get(f"/api/2.0/sql/statements/{stmt_id}")
            status = result.get("status", {}).get("state", "")
            if status in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                break

    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        print(f"  SQL FAILED: {error.get('message', 'unknown error')}")
    return result


# ── Step 3: Query lineage API ────────────────────────────────────────────


def get_table_lineage(catalog: str, schema: str, table: str) -> dict:
    """Get table-level lineage from Databricks Lineage Tracking API."""
    return api_get(
        f"/api/2.0/lineage-tracking/table-lineage"
        f"?table_name={catalog}.{schema}.{table}"
    )


def get_column_lineage(catalog: str, schema: str, table: str) -> dict:
    """Get column-level lineage from Databricks Lineage Tracking API."""
    return api_get(
        f"/api/2.0/lineage-tracking/column-lineage"
        f"?table_name={catalog}.{schema}.{table}"
    )


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("Databricks Lineage Test Pipeline")
    print("=" * 60)
    print(f"\nWorkspace: {WORKSPACE_URL}")

    # Step 1: Find warehouse
    print("\n── Step 1: Finding SQL warehouse ──")
    warehouse_id = find_warehouse()
    if not warehouse_id:
        print("ERROR: No SQL warehouse available. Create one in Databricks UI.")
        sys.exit(1)
    print(f"  Using warehouse: {warehouse_id}")

    # Step 2: List catalogs and schemas
    print("\n── Step 2: Discovering tables ──")
    result = execute_sql(warehouse_id, "SHOW CATALOGS")
    catalogs = []
    if result.get("status", {}).get("state") == "SUCCEEDED":
        for row in result.get("result", {}).get("data_array", []):
            catalogs.append(row[0])
    print(f"  Catalogs: {catalogs}")

    # Find the demo catalog (name contains 'lineage_bridge' or 'lb_demo')
    demo_catalog = None
    for c in catalogs:
        if "lineage_bridge" in c or "lb_demo" in c:
            demo_catalog = c
            break

    if not demo_catalog:
        print("ERROR: No demo catalog found. Available catalogs:")
        for c in catalogs:
            print(f"    - {c}")
        sys.exit(1)
    print(f"  Demo catalog: {demo_catalog}")

    # List schemas
    result = execute_sql(warehouse_id, f"SHOW SCHEMAS IN {demo_catalog}")
    schemas = []
    if result.get("status", {}).get("state") == "SUCCEEDED":
        for row in result.get("result", {}).get("data_array", []):
            schemas.append(row[0])
    print(f"  Schemas: {schemas}")

    # Find schema with tables (try all non-information_schema)
    cluster_schema = None
    tables = []
    for s in schemas:
        if s == "information_schema":
            continue
        # Schema names with dashes need backtick quoting
        quoted_schema = f"`{s}`" if "-" in s else s
        result = execute_sql(
            warehouse_id, f"SHOW TABLES IN {demo_catalog}.{quoted_schema}"
        )
        schema_tables = []
        if result.get("status", {}).get("state") == "SUCCEEDED":
            for row in result.get("result", {}).get("data_array", []):
                schema_tables.append(row[1])
        print(f"  Schema {s}: {schema_tables}")
        if schema_tables and not tables:
            cluster_schema = s
            tables = schema_tables

    if not cluster_schema:
        print("ERROR: No schema with tables found.")
        sys.exit(1)

    print(f"  Using schema: {cluster_schema}")
    print(f"  Tables: {tables}")

    # Schema quoting for SQL (dashes need backticks)
    quoted_schema = f"`{cluster_schema}`" if "-" in cluster_schema else cluster_schema

    # Step 3: Preview data
    print("\n── Step 3: Previewing source data ──")
    for table in tables[:2]:
        fqn = f"{demo_catalog}.{quoted_schema}.{table}"
        result = execute_sql(warehouse_id, f"SELECT COUNT(*) AS cnt FROM {fqn}")
        if result.get("status", {}).get("state") == "SUCCEEDED":
            rows = result.get("result", {}).get("data_array", [])
            count = rows[0][0] if rows else "?"
            print(f"  {fqn}: {count} rows")

    # Step 4: Discover columns and create derived tables
    print("\n── Step 4: Discovering columns & creating derived tables ──")
    derived_schema = f"{demo_catalog}.{quoted_schema}"

    # Find orders and customers tables
    orders_table = None
    customers_table = None
    for t in tables:
        if "orders" in t and "stats" not in t:
            orders_table = t
        if "customers" in t:
            customers_table = t

    if not orders_table:
        print("ERROR: No orders table found.")
        sys.exit(1)

    # Discover actual columns
    for t in [orders_table, customers_table]:
        if not t:
            continue
        fqn = f"{derived_schema}.{t}"
        result = execute_sql(warehouse_id, f"DESCRIBE TABLE {fqn}")
        if result.get("status", {}).get("state") == "SUCCEEDED":
            cols = [row[0] for row in result.get("result", {}).get("data_array", [])]
            print(f"  {t} columns: {cols}")

    # Create derived table using SELECT *
    derived_table = "order_summary_lineage_test"
    derived_fqn = f"{derived_schema}.{derived_table}"
    execute_sql(warehouse_id, f"DROP TABLE IF EXISTS {derived_fqn}")

    ctas_sql = f"""
    CREATE TABLE {derived_fqn} AS
    SELECT *, CURRENT_TIMESTAMP() as processed_at
    FROM {derived_schema}.{orders_table}
    LIMIT 100
    """
    print(f"  Running CTAS: {derived_fqn}")
    result = execute_sql(warehouse_id, ctas_sql)
    if result.get("status", {}).get("state") == "SUCCEEDED":
        print("  CTAS succeeded!")
    else:
        print(f"  CTAS failed: {result.get('status', {}).get('error', {}).get('message', '')[:200]}")

    # Join-based derived table
    if customers_table:
        join_table = "order_customer_lineage_test"
        join_fqn = f"{derived_schema}.{join_table}"
        execute_sql(warehouse_id, f"DROP TABLE IF EXISTS {join_fqn}")

        join_sql = f"""
        CREATE TABLE {join_fqn} AS
        SELECT
            o.order_id,
            o.customer_id,
            o.product_name,
            o.price,
            c.name as customer_name,
            c.email as customer_email,
            c.country,
            CURRENT_TIMESTAMP() as joined_at
        FROM {derived_schema}.{orders_table} o
        JOIN {derived_schema}.{customers_table} c
            ON o.customer_id = c.customer_id
        LIMIT 100
        """
        print(f"  Running JOIN CTAS: {join_fqn}")
        result = execute_sql(warehouse_id, join_sql)
        if result.get("status", {}).get("state") == "SUCCEEDED":
            print("  JOIN CTAS succeeded!")
        else:
            print(f"  JOIN failed: {result.get('status', {}).get('error', {}).get('message', '')[:200]}")

    # Step 5: Wait for lineage tracking to catch up
    print("\n── Step 5: Waiting for lineage tracking (30s) ──")
    time.sleep(30)

    # Step 6: Query lineage API
    print("\n── Step 6: Querying Lineage Tracking API ──")

    # Lineage for the derived table
    print(f"\n  Table lineage for: {derived_table}")
    try:
        lineage = get_table_lineage(demo_catalog, cluster_schema, derived_table)
        print(f"  Response:\n{json.dumps(lineage, indent=2)}")
    except httpx.HTTPStatusError as e:
        print(f"  Error {e.response.status_code}: {e.response.text[:200]}")

    # Lineage for the source table
    print(f"\n  Table lineage for: {orders_table}")
    try:
        lineage = get_table_lineage(demo_catalog, cluster_schema, orders_table)
        print(f"  Response:\n{json.dumps(lineage, indent=2)}")
    except httpx.HTTPStatusError as e:
        print(f"  Error {e.response.status_code}: {e.response.text[:200]}")

    # Column lineage
    print(f"\n  Column lineage for: {derived_table}")
    try:
        lineage = get_column_lineage(demo_catalog, cluster_schema, derived_table)
        print(f"  Response:\n{json.dumps(lineage, indent=2)}")
    except httpx.HTTPStatusError as e:
        print(f"  Error {e.response.status_code}: {e.response.text[:200]}")

    if customers_table:
        print(f"\n  Table lineage for: {join_table}")
        try:
            lineage = get_table_lineage(demo_catalog, cluster_schema, join_table)
            print(f"  Response:\n{json.dumps(lineage, indent=2)}")
        except httpx.HTTPStatusError as e:
            print(f"  Error {e.response.status_code}: {e.response.text[:200]}")

    # Step 7: Cleanup
    print("\n── Step 7: Cleanup ──")
    print(f"  To clean up test tables, run:")
    print(f"    DROP TABLE IF EXISTS {derived_fqn};")
    if customers_table:
        print(f"    DROP TABLE IF EXISTS {derived_schema}.{join_table};")

    print("\nDone!")


if __name__ == "__main__":
    main()
