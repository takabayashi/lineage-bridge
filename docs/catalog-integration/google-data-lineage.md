# Google Data Lineage Integration

LineageBridge integrates with Google Data Lineage (part of Google Cloud Dataplex) to enrich BigQuery table metadata and push Confluent lineage as native OpenLineage events.

## Overview

The `GoogleLineageProvider` offers native OpenLineage integration:

- **Build Nodes**: Creates `GOOGLE_TABLE` nodes from Tableflow catalog integrations
- **Enrich Metadata**: Fetches table schema, size, and metadata via the BigQuery API
- **Push Lineage**: Sends OpenLineage events to the Data Lineage API (no custom metadata format needed)

Google Data Lineage is unique among catalog providers because it natively speaks the OpenLineage standard, making it a natural integration point for stream lineage.

## Prerequisites

1. **Google Cloud Project**: Access to a GCP project with BigQuery and Data Lineage API enabled
2. **Application Default Credentials**: Configured via `gcloud auth application-default login`
3. **IAM Permissions**: Service account or user with BigQuery and Data Lineage permissions
4. **Tableflow Integration**: Configure Tableflow in Confluent Cloud to sync topics to BigQuery tables

### Enable Required APIs

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable datalineage.googleapis.com
```

### Required IAM Permissions

Create a custom role or use predefined roles with the following permissions:

```yaml
# BigQuery permissions (for enrichment)
bigquery.tables.get
bigquery.tables.getData

# Data Lineage permissions (for lineage push)
datalineage.locations.searchLinks
datalineage.operations.get
datalineage.processes.create
datalineage.runs.create
```

**Predefined Roles**:
- `roles/bigquery.dataViewer` - BigQuery metadata read
- `roles/datalineage.admin` - Data Lineage write

### Configure Application Default Credentials

```bash
# For local development
gcloud auth application-default login

# For production (service account)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## Configuration

Add the following to your `.env` file:

```env
# Google Cloud Platform
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us  # or us-central1, europe-west1, etc.
```

**Authentication**: LineageBridge uses the `google-auth` library, which loads credentials from:
1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable (service account key)
2. Application Default Credentials (via `gcloud auth application-default login`)
3. Compute Engine metadata service (when running on GCE, GKE, Cloud Run)

## Features

### 1. Node Creation (build_node)

When Tableflow reports a Google BigQuery integration, the provider creates a `GOOGLE_TABLE` node:

```python
# Node ID format
node_id = f"google:google_table:{environment_id}:{project}.{dataset}.{table}"

# Qualified name format
qualified_name = f"{project_id}.{dataset_id}.{table_name}"
```

**Naming Convention**:
- Project ID: From Tableflow config or `LINEAGE_BRIDGE_GCP_PROJECT_ID`
- Dataset ID: From Tableflow config or defaults to cluster ID
- Table name: Topic name with dots and hyphens normalized to underscores (e.g., `orders.v1` becomes `orders_v1`)

**Example**:
```
Topic: "orders.v1"
Cluster: lkc-abc123
BigQuery Table: my-project.lkc_abc123.orders_v1
```

### 2. Metadata Enrichment (enrich)

The provider fetches metadata for each BigQuery table via the BigQuery API:

**Endpoint**: `GET https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}`

**Enriched Attributes**:
- `table_type`: TABLE, VIEW, EXTERNAL, MATERIALIZED_VIEW
- `columns`: Array of column definitions with name, type, and description
- `num_rows`: Row count (null for views)
- `num_bytes`: Storage size in bytes
- `creation_time`: Table creation timestamp (milliseconds since epoch)
- `last_modified_time`: Last modification timestamp
- `description`: User-provided table description
- `labels`: User-defined key-value labels

**Retry Logic**: Exponential backoff on 429/500/502/503/504 errors (max 3 retries)

**Error Handling**:
- `404 Not Found`: Table does not exist (skipped with warning)
- `401/403 Forbidden`: Insufficient permissions (skipped with warning)

### 3. Lineage Push (push_lineage)

Push Confluent lineage as OpenLineage events to the Data Lineage API:

**Endpoint**: `POST https://datalineage.googleapis.com/v1/projects/{project}/locations/{location}:processOpenLineageRunEvent`

**How It Works**:
1. Convert the LineageBridge graph to OpenLineage events using `graph_to_events()` translator
2. For each event, POST to the Data Lineage API
3. Google indexes the events and makes them queryable via the Data Lineage UI

**OpenLineage Format**:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-04-30T12:34:56.789Z",
  "run": {
    "runId": "abc123-def456-...",
    "facets": {}
  },
  "job": {
    "namespace": "confluent://env-abc123",
    "name": "tableflow-orders.v1",
    "facets": {}
  },
  "inputs": [
    {
      "namespace": "kafka://lkc-abc123",
      "name": "orders.v1",
      "facets": {}
    }
  ],
  "outputs": [
    {
      "namespace": "bigquery://my-project",
      "name": "lkc_abc123.orders_v1",
      "facets": {
        "schema": {...},
        "dataSource": {...}
      }
    }
  ]
}
```

**Usage Example** (UI):

1. Extract lineage with Tableflow enabled
2. Click **Push Lineage** in the sidebar
3. Select **Google Data Lineage**
4. Click **Push**

**Usage Example** (API):

```bash
curl -X POST http://localhost:8000/api/v1/lineage/push \
  -H "Content-Type: application/json" \
  -d '{
    "catalog_type": "GOOGLE_DATA_LINEAGE"
  }'
```

## Testing

### 1. Test Enrichment

Extract lineage with GCP credentials configured:

```bash
export LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
uv run lineage-bridge-extract
```

Check the extracted graph for Google table nodes with enriched metadata:

```bash
cat lineage_graph.json | jq '.nodes[] | select(.node_type == "GOOGLE_TABLE")'
```

Expected attributes: `table_type`, `columns`, `num_rows`, `num_bytes`, `description`, `labels`

### 2. Test Lineage Push

In the UI:
1. Extract lineage
2. Click **Push Lineage** > **Google Data Lineage**
3. Click **Push**
4. Check results panel for success/error counts

Verify in Google Cloud Console:

1. Navigate to **Dataplex** > **Data Lineage**
2. Search for your BigQuery table: `my-project.lkc_abc123.orders_v1`
3. View the lineage graph — you should see upstream Kafka topics and connectors

Or use the `gcloud` CLI:

```bash
# List lineage events
gcloud dataplex data-lineage search-links \
  --location=us \
  --project=my-project \
  --target="bigquery:my-project.lkc_abc123.orders_v1"
```

### 3. Query via BigQuery

BigQuery table metadata is separate from lineage. Query table details:

```sql
SELECT * FROM `my-project.lkc_abc123.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'orders_v1';
```

## Troubleshooting

### Error: "BigQuery API returned 404"

**Cause**: Table does not exist in BigQuery

**Fix**:
1. Verify Tableflow sync is running: `confluent tableflow connection list`
2. Check table exists: `bq show my-project:lkc_abc123.orders_v1`
3. Confirm project/dataset/table names in Tableflow config match BigQuery

### Error: "BigQuery API returned 403"

**Cause**: Service account lacks `bigquery.tables.get` permission

**Fix**:
1. Grant `roles/bigquery.dataViewer` to the service account
2. Or add custom role with `bigquery.tables.get` permission
3. Test: `gcloud auth application-default print-access-token` (should succeed)

### Error: "Data Lineage API returned 403"

**Cause**: Service account lacks `datalineage.runs.create` permission

**Fix**:
1. Grant `roles/datalineage.admin` to the service account
2. Verify Data Lineage API is enabled: `gcloud services list --enabled | grep datalineage`
3. Check quota limits in the GCP Console

### Error: "google-auth not available or no ADC configured"

**Cause**: `google-auth` library not installed or Application Default Credentials not configured

**Fix**:
```bash
# Install google-auth (should be included in dependencies)
uv pip install google-auth

# Configure ADC
gcloud auth application-default login
```

### Lineage events pushed but not visible in UI

**Cause**: Data Lineage indexing can take 5-15 minutes

**Fix**: Wait and refresh the Data Lineage UI. Events are indexed asynchronously.

## Integration with Google Services

Google Data Lineage integrates with:

- **BigQuery**: Query lineage for tables, views, and materialized views
- **Google Data Catalog**: View lineage in the Data Catalog UI
- **Dataplex**: Unified data governance and lineage
- **Cloud Composer (Airflow)**: Airflow DAGs can emit OpenLineage events
- **Dataflow**: Dataflow jobs emit lineage automatically

## Best Practices

1. **Use Service Accounts**: For production, use service accounts instead of user credentials
2. **Choose Location Wisely**: Data Lineage location should match your BigQuery dataset location
3. **Monitor API Quotas**: Data Lineage has API quotas — monitor usage in the GCP Console
4. **Tag Tables**: Use BigQuery labels for cost tracking and governance
5. **Combine with dbt**: If using dbt, combine LineageBridge lineage with dbt's OpenLineage integration for full DAG visibility

## OpenLineage Compatibility

Google Data Lineage implements the [OpenLineage](https://openlineage.io/) standard, which means:

- **Vendor-Neutral**: Lineage events are portable across tools
- **Community Standard**: Growing ecosystem of integrations (Airflow, Spark, dbt, Flink)
- **Extensible**: Custom facets for domain-specific metadata

LineageBridge's OpenLineage translator (`lineage_bridge.api.openlineage.translator`) converts the internal graph model to OpenLineage events, bridging Confluent stream lineage into the broader data lineage ecosystem.

## Deep Links

The provider generates deep links to the BigQuery Console:

```
https://console.cloud.google.com/bigquery?project={project}&p={project}&d={dataset}&t={table}&page=table
```

Click any Google table node in the LineageBridge UI to open it in the BigQuery Console.

## Next Steps

- [Databricks Unity Catalog Integration](databricks-unity-catalog.md) - Integrate with Unity Catalog
- [AWS Glue Integration](aws-glue.md) - Integrate with AWS Glue Data Catalog
- [Adding New Catalogs](adding-new-catalogs.md) - Build a custom catalog provider
- [OpenLineage Mapping Reference](../api-reference/openlineage-mapping.md) - Learn how Confluent concepts map to OpenLineage
