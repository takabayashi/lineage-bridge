# Adding New Catalog Providers

This guide walks you through creating a custom catalog provider to integrate LineageBridge with additional data catalogs beyond Databricks UC, AWS Glue, and Google Data Lineage.

## Overview

LineageBridge uses a **provider pattern** for catalog integrations. Each provider implements the `CatalogProvider` protocol, which defines four core methods:

1. **`build_node()`** - Create a catalog table node and edge from a Tableflow integration
2. **`enrich()`** - Backfill metadata from the catalog's API
3. **`build_url()`** - Generate a deep link to the catalog's UI
4. **`push_lineage()`** (optional) - Write lineage metadata back to the catalog

Adding a new catalog requires:
1. Create a provider class in `lineage_bridge/catalogs/`
2. Register it in `lineage_bridge/catalogs/__init__.py`
3. Add corresponding `NodeType` and `SystemType` to `lineage_bridge/models/graph.py` (if needed)
4. Write tests in `tests/catalogs/`

## The CatalogProvider Protocol

Located in `lineage_bridge/catalogs/protocol.py`:

```python
from typing import Any, Protocol
from lineage_bridge.models.graph import (
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

class CatalogProvider(Protocol):
    """Interface for data catalog integrations."""

    catalog_type: str       # e.g. "UNITY_CATALOG", "AWS_GLUE"
    node_type: NodeType     # The NodeType this provider creates
    system_type: SystemType # The SystemType for nodes

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog node + MATERIALIZES edge."""
        ...

    async def enrich(self, graph: LineageGraph) -> None:
        """Enrich existing catalog nodes with metadata from the catalog's API."""
        ...

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link URL to this node in the catalog's UI."""
        ...
```

The `push_lineage()` method is optional and not part of the protocol (to allow flexibility in signatures).

## Step-by-Step Guide

### Step 1: Define Node and System Types

If your catalog requires a new node or system type, add them to `lineage_bridge/models/graph.py`:

```python
# Add to SystemType enum
class SystemType(str, Enum):
    CONFLUENT = "confluent"
    DATABRICKS = "databricks"
    AWS = "aws"
    GOOGLE = "google"
    SNOWFLAKE = "snowflake"  # New!
    EXTERNAL = "external"

# Add to NodeType enum
class NodeType(str, Enum):
    KAFKA_TOPIC = "KAFKA_TOPIC"
    UC_TABLE = "UC_TABLE"
    GLUE_TABLE = "GLUE_TABLE"
    GOOGLE_TABLE = "GOOGLE_TABLE"
    SNOWFLAKE_TABLE = "SNOWFLAKE_TABLE"  # New!
    # ... other types
```

### Step 2: Create the Provider Class

Create `lineage_bridge/catalogs/snowflake.py`:

```python
# Copyright 2026 Your Name
# Licensed under the Apache License, Version 2.0
"""Snowflake Data Catalog provider."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    PushResult,
    SystemType,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0


class SnowflakeCatalogProvider:
    """CatalogProvider implementation for Snowflake."""

    catalog_type: str = "SNOWFLAKE"
    node_type: NodeType = NodeType.SNOWFLAKE_TABLE
    system_type: SystemType = SystemType.SNOWFLAKE

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self._account = account
        self._user = user
        self._password = password

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a SNOWFLAKE_TABLE node and MATERIALIZES edge."""
        # Extract config from Tableflow catalog integration
        snowflake_cfg = ci_config.get("snowflake", ci_config)
        database = snowflake_cfg.get("database", "CONFLUENT")
        schema = snowflake_cfg.get("schema", cluster_id.upper())
        # Normalize table name (Snowflake is case-insensitive, uppercase by default)
        table_name = topic_name.replace(".", "_").replace("-", "_").upper()
        
        qualified = f"{database}.{schema}.{table_name}"
        node_id = f"snowflake:snowflake_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=node_id,
            system=SystemType.SNOWFLAKE,
            node_type=NodeType.SNOWFLAKE_TABLE,
            qualified_name=qualified,
            display_name=qualified,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "account": self._account,
                "database": database,
                "schema": schema,
                "table_name": table_name,
            },
        )
        
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=node_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata from Snowflake INFORMATION_SCHEMA."""
        if not all([self._account, self._user, self._password]):
            logger.debug("Snowflake enrichment skipped — no credentials configured")
            return

        snowflake_nodes = graph.filter_by_type(NodeType.SNOWFLAKE_TABLE)
        if not snowflake_nodes:
            return

        # Option 1: Use Snowflake Python connector (recommended)
        try:
            import snowflake.connector
        except ImportError:
            logger.warning("snowflake-connector-python not installed — skipping enrichment")
            return

        conn = snowflake.connector.connect(
            account=self._account,
            user=self._user,
            password=self._password,
        )

        for node in snowflake_nodes:
            if node.system != SystemType.SNOWFLAKE:
                continue
            await self._enrich_node(conn, graph, node)

        conn.close()

    async def _enrich_node(
        self,
        conn: Any,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch metadata for a single Snowflake table."""
        database = node.attributes.get("database")
        schema = node.attributes.get("schema")
        table_name = node.attributes.get("table_name")
        if not all([database, schema, table_name]):
            return

        query = f"""
        SELECT 
            TABLE_OWNER,
            TABLE_TYPE,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name}'
        """

        try:
            cursor = await asyncio.to_thread(conn.cursor().execute, query)
            row = await asyncio.to_thread(cursor.fetchone)
            if not row:
                logger.warning(
                    "Snowflake table %s.%s.%s not found",
                    database,
                    schema,
                    table_name,
                )
                return

            enriched_attrs = {
                **node.attributes,
                "owner": row[0],
                "table_type": row[1],
                "row_count": row[2],
                "bytes": row[3],
                "created": str(row[4]) if row[4] else None,
                "last_altered": str(row[5]) if row[5] else None,
                "comment": row[6],
            }
            enriched = node.model_copy(update={"attributes": enriched_attrs})
            graph.add_node(enriched)

        except Exception:
            logger.exception(
                "Snowflake enrichment failed for %s.%s.%s",
                database,
                schema,
                table_name,
            )

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the table in Snowflake UI."""
        account = node.attributes.get("account")
        database = node.attributes.get("database")
        schema = node.attributes.get("schema")
        table_name = node.attributes.get("table_name")
        if not all([account, database, schema, table_name]):
            return None
        # Snowflake URL format
        return (
            f"https://app.snowflake.com/{account}/#/data/databases/"
            f"{database}/schemas/{schema}/table/{table_name}"
        )

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        set_comment: bool = True,
        set_tags: bool = True,
    ) -> PushResult:
        """Push Confluent lineage metadata to Snowflake tables via SQL.
        
        Sets table comments and optionally tags.
        """
        if not all([self._account, self._user, self._password]):
            return PushResult(errors=["No Snowflake credentials configured"])

        import snowflake.connector

        result = PushResult()
        snowflake_nodes = [
            n
            for n in graph.filter_by_type(NodeType.SNOWFLAKE_TABLE)
            if n.system == SystemType.SNOWFLAKE
        ]
        if not snowflake_nodes:
            return result

        conn = snowflake.connector.connect(
            account=self._account,
            user=self._user,
            password=self._password,
        )

        for node in snowflake_nodes:
            upstream = graph.get_upstream(node.node_id)
            if not upstream:
                continue

            database = node.attributes.get("database")
            schema = node.attributes.get("schema")
            table_name = node.attributes.get("table_name")
            if not all([database, schema, table_name]):
                continue

            qualified = f"{database}.{schema}.{table_name}"

            if set_comment:
                # Build lineage comment
                source_topics = [
                    up_node.qualified_name
                    for up_node, _, _ in upstream
                    if up_node.node_type == NodeType.KAFKA_TOPIC
                ]
                comment_text = (
                    f"Materialized from Kafka topic {', '.join(source_topics)} "
                    f"via Confluent Tableflow. Managed by LineageBridge."
                )
                sql = f"COMMENT ON TABLE {qualified} IS '{comment_text}'"
                try:
                    await asyncio.to_thread(conn.cursor().execute, sql)
                    result.comments_set += 1
                except Exception as exc:
                    result.errors.append(f"Comment failed for {qualified}: {exc}")

            result.tables_updated += 1

        conn.close()
        return result
```

### Step 3: Register the Provider

Edit `lineage_bridge/catalogs/__init__.py`:

```python
from lineage_bridge.catalogs.snowflake import SnowflakeCatalogProvider

_PROVIDERS: dict[str, CatalogProvider] = {
    "UNITY_CATALOG": DatabricksUCProvider(),
    "AWS_GLUE": GlueCatalogProvider(),
    "GOOGLE_DATA_LINEAGE": GoogleLineageProvider(),
    "SNOWFLAKE": SnowflakeCatalogProvider(),  # New!
}
```

### Step 4: Add Configuration Settings

Edit `lineage_bridge/config/settings.py`:

```python
class Settings(BaseSettings):
    # ... existing fields ...

    # Snowflake
    snowflake_account: str | None = Field(
        default=None,
        description="Snowflake account identifier (e.g. xy12345.us-east-1)",
    )
    snowflake_user: str | None = Field(default=None, description="Snowflake username")
    snowflake_password: str | None = Field(default=None, description="Snowflake password")
```

### Step 5: Initialize the Provider

The Tableflow client will automatically detect catalog integrations. If you need to initialize your provider with credentials from settings, update the orchestrator or UI:

```python
from lineage_bridge.config import Settings
from lineage_bridge.catalogs.snowflake import SnowflakeCatalogProvider

settings = Settings()
provider = SnowflakeCatalogProvider(
    account=settings.snowflake_account,
    user=settings.snowflake_user,
    password=settings.snowflake_password,
)
```

### Step 6: Write Tests

Create `tests/catalogs/test_snowflake.py`:

```python
import pytest
from lineage_bridge.catalogs.snowflake import SnowflakeCatalogProvider
from lineage_bridge.models.graph import LineageGraph, NodeType, SystemType


def test_build_node():
    """Test Snowflake node creation."""
    provider = SnowflakeCatalogProvider(account="xy12345")
    ci_config = {
        "snowflake": {
            "database": "CONFLUENT",
            "schema": "LKC_ABC123",
        }
    }
    node, edge = provider.build_node(
        ci_config=ci_config,
        tableflow_node_id="confluent:tableflow_table:env-123:tf-1",
        topic_name="orders.v1",
        cluster_id="lkc-abc123",
        environment_id="env-123",
    )

    assert node.node_type == NodeType.SNOWFLAKE_TABLE
    assert node.system == SystemType.SNOWFLAKE
    assert node.qualified_name == "CONFLUENT.LKC_ABC123.ORDERS_V1"
    assert edge.edge_type.value == "MATERIALIZES"


@pytest.mark.asyncio
async def test_enrich_skip_no_credentials():
    """Test enrichment skips when credentials are missing."""
    provider = SnowflakeCatalogProvider()  # No credentials
    graph = LineageGraph()
    await provider.enrich(graph)
    # Should not raise, just log and skip


def test_build_url():
    """Test deep link generation."""
    provider = SnowflakeCatalogProvider()
    from lineage_bridge.models.graph import LineageNode

    node = LineageNode(
        node_id="snowflake:snowflake_table:env-123:CONFLUENT.PUBLIC.ORDERS",
        system=SystemType.SNOWFLAKE,
        node_type=NodeType.SNOWFLAKE_TABLE,
        qualified_name="CONFLUENT.PUBLIC.ORDERS",
        display_name="CONFLUENT.PUBLIC.ORDERS",
        attributes={
            "account": "xy12345.us-east-1",
            "database": "CONFLUENT",
            "schema": "PUBLIC",
            "table_name": "ORDERS",
        },
    )
    url = provider.build_url(node)
    assert url == (
        "https://app.snowflake.com/xy12345.us-east-1/#/data/databases/"
        "CONFLUENT/schemas/PUBLIC/table/ORDERS"
    )
```

Run tests:

```bash
uv run pytest tests/catalogs/test_snowflake.py -v
```

### Step 7: Document Your Provider

Create `docs/catalog-integration/snowflake.md` following the structure of existing provider docs (see Databricks UC, AWS Glue, Google Lineage guides).

Include:
- Prerequisites
- Configuration
- Features (build_node, enrich, push_lineage)
- Testing steps
- Troubleshooting
- Best practices

## Best Practices

### 1. Use Async I/O

All enrichment and push operations should be async to avoid blocking the event loop:

```python
async def enrich(self, graph: LineageGraph) -> None:
    async with httpx.AsyncClient(...) as client:
        for node in nodes:
            await self._enrich_node(client, graph, node)
```

For synchronous libraries (boto3, snowflake-connector), use `asyncio.to_thread()`:

```python
response = await asyncio.to_thread(client.get_table, DatabaseName=db, Name=table)
```

### 2. Implement Retry Logic

Network calls should retry on transient errors (429, 500, 502, 503, 504):

```python
for attempt in range(_MAX_RETRIES):
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            delay = _BACKOFF_BASE * (2**attempt)
            await asyncio.sleep(delay)
            continue
        return None
    except httpx.HTTPError:
        if attempt < _MAX_RETRIES - 1:
            await asyncio.sleep(_BACKOFF_BASE * (2**attempt))
```

### 3. Handle Missing Credentials Gracefully

If credentials are not configured, skip enrichment and log a debug message:

```python
if not self._account or not self._token:
    logger.debug("MyProvider enrichment skipped — no credentials configured")
    return
```

### 4. Preserve User Metadata

When pushing lineage, preserve existing metadata (don't overwrite user-defined properties):

```python
# Merge instead of replacing
existing_params = table.get("Parameters", {})
new_params = {**existing_params, **lineage_params}
```

### 5. Add Structured Attributes

Use structured attributes instead of dumping raw API responses:

```python
# Good
enriched_attrs = {
    **node.attributes,
    "owner": data.get("owner"),
    "table_type": data.get("table_type"),
    "columns": [{"name": c["name"], "type": c["type"]} for c in data.get("columns", [])],
}

# Avoid
enriched_attrs = {**node.attributes, "raw_response": data}
```

### 6. Use Descriptive Node IDs

Node IDs should be unique and descriptive:

```
{system}:{node_type}:{environment_id}:{qualified_name}
```

Example:
```
snowflake:snowflake_table:env-abc123:CONFLUENT.PUBLIC.ORDERS
```

## Testing Checklist

- [ ] `build_node()` creates correct node and edge
- [ ] `enrich()` skips when credentials are missing
- [ ] `enrich()` fetches metadata from catalog API
- [ ] `build_url()` generates valid deep link
- [ ] `push_lineage()` writes metadata to catalog
- [ ] Retry logic works on transient errors
- [ ] Error handling logs warnings for non-existent tables
- [ ] Integration test with real credentials (optional, CI skip)

## Example: Minimal Provider

Here's a minimal provider that only implements required methods:

```python
from lineage_bridge.models.graph import *

class MinimalProvider:
    catalog_type: str = "MY_CATALOG"
    node_type: NodeType = NodeType.EXTERNAL_DATASET  # Reuse existing type
    system_type: SystemType = SystemType.EXTERNAL

    def build_node(
        self, ci_config, tableflow_node_id, topic_name, cluster_id, environment_id
    ):
        qualified = f"my-catalog://{topic_name}"
        node_id = f"external:external_dataset:{environment_id}:{qualified}"
        node = LineageNode(
            node_id=node_id,
            system=SystemType.EXTERNAL,
            node_type=NodeType.EXTERNAL_DATASET,
            qualified_name=qualified,
            display_name=qualified,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={"catalog": "MY_CATALOG", "topic": topic_name},
        )
        edge = LineageEdge(
            src_id=tableflow_node_id, dst_id=node_id, edge_type=EdgeType.MATERIALIZES
        )
        return node, edge

    async def enrich(self, graph):
        pass  # No enrichment needed

    def build_url(self, node):
        return None  # No UI link
```

This provider creates nodes but does not enrich or push lineage. Useful for catalogs with limited APIs.

## Common Pitfalls

1. **Forgetting to register the provider** — Add it to `_PROVIDERS` in `__init__.py`
2. **Blocking I/O in async methods** — Use `asyncio.to_thread()` for sync libraries
3. **Not handling 404 errors** — Tables may not exist yet (Tableflow is eventually consistent)
4. **Hardcoding credentials** — Always load from settings or constructor arguments
5. **Not testing with missing credentials** — Enrichment should skip gracefully
6. **Overly complex node IDs** — Keep them human-readable and URL-safe

## Next Steps

- Study existing providers: [Databricks UC](databricks-unity-catalog.md), [AWS Glue](aws-glue.md), [Google Lineage](google-data-lineage.md)
- Review the protocol definition: `lineage_bridge/catalogs/protocol.py`
- Check the graph model: `lineage_bridge/models/graph.py`
- Look at how the Tableflow client calls providers: `lineage_bridge/clients/tableflow.py`

If you build a provider for a popular catalog, consider contributing it back to the project via a pull request!
