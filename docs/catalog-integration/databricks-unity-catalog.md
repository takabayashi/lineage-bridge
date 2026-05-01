# Databricks Unity Catalog Integration

LineageBridge integrates with Databricks Unity Catalog (UC) to enrich table metadata and push Confluent lineage information back into UC as table properties, comments, and lineage records.

## Overview

The `DatabricksUCProvider` offers the most comprehensive catalog integration:

- **Build Nodes**: Creates `UC_TABLE` nodes from Tableflow catalog integrations
- **Enrich Metadata**: Fetches table schema, owner, storage location, and update timestamps via the Unity Catalog REST API
- **Discover Lineage**: Walks downstream UC tables to discover derived tables created via Databricks SQL/Spark
- **Push Lineage**: Writes Confluent lineage metadata as table properties, comments, and optionally a bridge table for querying

## Prerequisites

1. **Databricks Workspace**: Access to a Databricks workspace with Unity Catalog enabled
2. **Personal Access Token**: Generate a token with workspace access permissions
3. **SQL Warehouse** (for lineage push): A running SQL warehouse for executing SQL statements
4. **Tableflow Integration**: Configure Tableflow in Confluent Cloud to sync topics to UC tables

### Generate a Databricks Token

1. Navigate to **User Settings** > **Developer** > **Access Tokens**
2. Click **Generate New Token**
3. Set a descriptive name (e.g., `lineage-bridge`) and optional expiration
4. Copy the token (it will not be shown again)

### Find Your SQL Warehouse ID

1. Navigate to **SQL** > **SQL Warehouses**
2. Click on the warehouse you want to use
3. Copy the **ID** from the URL: `https://<workspace>.cloud.databricks.com/sql/warehouses/<warehouse_id>`

## Configuration

Add the following to your `.env` file:

```env
# Databricks Unity Catalog
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi1234567890abcdef1234567890ab
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456  # Required for lineage push
```

**Note**: The workspace URL should not include a trailing slash.

## Features

### 1. Node Creation (build_node)

When Tableflow reports a Unity Catalog integration, the provider creates a `UC_TABLE` node:

```python
# Node ID format
node_id = f"databricks:uc_table:{environment_id}:{catalog}.{schema}.{table}"

# Qualified name format
qualified_name = f"{catalog_name}.{cluster_id}.{topic_name_normalized}"
```

**Naming Convention**:
- Catalog name: From Tableflow config or defaults to `confluent_tableflow`
- Schema name: Raw cluster ID (e.g., `lkc-abc123`)
- Table name: Topic name with dots normalized to underscores (e.g., `orders.v1` becomes `orders_v1`)

**Example**:
```
Topic: "orders.v1"
Cluster: lkc-abc123
UC Table: confluent_tableflow.lkc-abc123.orders_v1
```

### 2. Metadata Enrichment (enrich)

The provider fetches metadata for each UC table via the REST API:

**Endpoint**: `GET /api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}`

**Enriched Attributes**:
- `owner`: Table owner
- `table_type`: MANAGED, EXTERNAL, VIEW, etc.
- `columns`: Array of column definitions with name, type, and comment
- `storage_location`: S3/ADLS/GCS path
- `updated_at`: Last modification timestamp

**Retry Logic**: Exponential backoff on 429/500/502/503/504 errors (max 3 retries)

### 3. Downstream Lineage Discovery

The provider walks the Unity Catalog lineage API to discover derived tables:

**Endpoint**: `GET /api/2.0/lineage-tracking/table-lineage?table_name={table}`

For each discovered downstream table:
1. Create a new `UC_TABLE` node with `derived: true`
2. Add a `TRANSFORMS` edge from the upstream table
3. Enrich the new node with metadata
4. Continue walking downstream recursively

**Example**:
```
orders.v1 (topic)
  -> confluent_tableflow.lkc-abc123.orders_v1 (UC table)
    -> sales_analytics.gold.orders_daily (derived UC table, discovered via lineage)
```

This allows LineageBridge to trace how Confluent topics flow through Databricks transformations.

### 4. Lineage Push (push_lineage)

Push Confluent lineage metadata back to UC via the Databricks Statement Execution API:

**Options**:
- `set_properties`: Write `lineage_bridge.*` table properties
- `set_comments`: Write a human-readable lineage comment
- `create_bridge_table`: Create a queryable lineage table

**Table Properties** (set via `ALTER TABLE ... SET TBLPROPERTIES`):

```sql
ALTER TABLE confluent_tableflow.lkc-abc123.orders_v1 SET TBLPROPERTIES (
  'lineage_bridge.source_topics' = 'orders.v1',
  'lineage_bridge.source_connectors' = 'MySqlSourceConnector',
  'lineage_bridge.pipeline_type' = 'tableflow',
  'lineage_bridge.last_synced' = '2026-04-30T12:34:56.789Z',
  'lineage_bridge.environment_id' = 'env-abc123',
  'lineage_bridge.cluster_id' = 'lkc-abc123'
)
```

**Table Comment** (set via `COMMENT ON TABLE`):

```sql
COMMENT ON TABLE confluent_tableflow.lkc-abc123.orders_v1 IS 
'Materialized from Kafka topic "orders.v1" via Confluent Tableflow.
Sources: MySqlSourceConnector
Environment: env-abc123
Last synced: 2026-04-30T12:34:56.789Z
Managed by LineageBridge'
```

**Bridge Table** (optional, for querying lineage):

Creates `{catalog}.default.confluent_lineage` with upstream lineage records:

```sql
CREATE TABLE IF NOT EXISTS confluent_tableflow.default.confluent_lineage (
  uc_table STRING,
  source_type STRING,
  source_name STRING,
  source_system STRING,
  edge_type STRING,
  environment_id STRING,
  cluster_id STRING,
  hop_distance INT,
  full_path STRING,
  synced_at TIMESTAMP
)
```

Each UC table gets one row per upstream node (topic, connector, etc.).

**Usage Example** (UI):

1. Extract lineage with Tableflow enabled
2. Click **Push Lineage** in the sidebar
3. Select options:
   - Set table properties
   - Set table comments
   - Create bridge table (optional)
4. Click **Push**

**Usage Example** (API):

```bash
curl -X POST http://localhost:8000/api/v1/lineage/push \
  -H "Content-Type: application/json" \
  -d '{
    "catalog_type": "UNITY_CATALOG",
    "set_properties": true,
    "set_comments": true,
    "create_bridge_table": true
  }'
```

## Testing

### 1. Test Enrichment

Extract lineage with UC credentials configured:

```bash
export LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
export LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi...
uv run lineage-bridge-extract
```

Check the extracted graph for UC table nodes with enriched metadata:

```bash
cat lineage_graph.json | jq '.nodes[] | select(.node_type == "UC_TABLE")'
```

Expected attributes: `owner`, `table_type`, `columns`, `storage_location`, `updated_at`

### 2. Test Lineage Push

In the UI:
1. Extract lineage
2. Click **Push Lineage** > **Unity Catalog**
3. Enable all options
4. Click **Push**
5. Check results panel for success/error counts

Verify in Databricks SQL:

```sql
-- Check table properties
SHOW TBLPROPERTIES confluent_tableflow.lkc_abc123.orders_v1;

-- Check table comment
DESCRIBE TABLE EXTENDED confluent_tableflow.lkc_abc123.orders_v1;

-- Query bridge table
SELECT * FROM confluent_tableflow.default.confluent_lineage
WHERE uc_table = 'confluent_tableflow.lkc_abc123.orders_v1';
```

### 3. Test Downstream Discovery

If you have derived UC tables created via SQL:

```sql
CREATE TABLE sales_analytics.gold.orders_daily AS
SELECT date_trunc('day', order_time) AS day, count(*) AS count
FROM confluent_tableflow.lkc_abc123.orders_v1
GROUP BY day;
```

After extraction, search the graph for `orders_daily` — it should appear as a derived `UC_TABLE` node connected to the source table.

## Troubleshooting

### Error: "Databricks UC API returned 401"

**Cause**: Invalid or expired token

**Fix**: Regenerate the token and update `.env`

### Error: "Databricks UC API returned 404"

**Cause**: Table does not exist in Unity Catalog

**Fix**:
1. Verify Tableflow sync is running: `confluent tableflow connection list`
2. Check table exists: `SHOW TABLES IN <catalog>.<schema>`
3. Confirm catalog/schema/table names in Tableflow config match UC

### Error: "Bridge table creation failed: permission denied"

**Cause**: Token lacks `CREATE TABLE` permission on the default schema

**Fix**: Grant `CREATE` on `{catalog}.default` or use a service principal with broader permissions

### Lineage push succeeds but properties don't appear

**Cause**: SQL warehouse may be serverless with delayed metadata sync

**Fix**: Wait 30-60 seconds and re-query, or use a classic SQL warehouse

## Best Practices

1. **Use Service Principals**: For production deployments, use a service principal token instead of personal access tokens
2. **Limit Warehouse Size**: Lineage push uses lightweight SQL — a Small warehouse is sufficient
3. **Monitor Bridge Table Growth**: If using the bridge table, monitor row counts and set a retention policy
4. **Enable Downstream Discovery**: To capture full lakehouse lineage, ensure UC lineage tracking is enabled in workspace settings
5. **Quote Special Characters**: Tableflow table names are auto-quoted if they contain non-alphanumeric characters

## Deep Links

The provider generates deep links to the Unity Catalog UI:

```
https://<workspace>.cloud.databricks.com/explore/data/<catalog>/<schema>/<table>
```

Click any UC table node in the LineageBridge UI to open it in the Databricks workspace.

## Next Steps

- [AWS Glue Integration](aws-glue.md) - Integrate with AWS Glue Data Catalog
- [Google Data Lineage Integration](google-data-lineage.md) - Integrate with Google Data Lineage
- [Adding New Catalogs](adding-new-catalogs.md) - Build a custom catalog provider
