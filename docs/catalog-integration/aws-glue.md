# AWS Glue Data Catalog Integration

LineageBridge integrates with AWS Glue Data Catalog to enrich table metadata and push Confluent lineage information as table parameters and descriptions.

## Overview

The `GlueCatalogProvider` offers native integration with AWS analytics services:

- **Build Nodes**: Creates `GLUE_TABLE` nodes from Tableflow catalog integrations
- **Enrich Metadata**: Fetches table schema, partitions, storage format, and SerDe info via the Glue API
- **Push Lineage**: Writes Confluent lineage metadata as table parameters and description text

## Prerequisites

1. **AWS Account**: Access to an AWS account with Glue Data Catalog enabled
2. **IAM Permissions**: Credentials with `glue:GetTable` and `glue:UpdateTable` permissions
3. **Tableflow Integration**: Configure Tableflow in Confluent Cloud to sync topics to Glue tables

### Required IAM Permissions

Create an IAM policy with the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/*",
        "arn:aws:glue:*:*:table/*"
      ]
    }
  ]
}
```

Attach this policy to an IAM user or role used by LineageBridge.

## Configuration

Add the following to your `.env` file:

```env
# AWS Glue Data Catalog
LINEAGE_BRIDGE_AWS_REGION=us-east-1
```

**AWS Credentials**: LineageBridge uses boto3, which loads credentials from:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (when running on EC2, ECS, or Lambda)

**Example using environment variables:**

```env
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
LINEAGE_BRIDGE_AWS_REGION=us-east-1
```

**Example using credentials file:**

```ini
# ~/.aws/credentials
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Features

### 1. Node Creation (build_node)

When Tableflow reports an AWS Glue integration, the provider creates a `GLUE_TABLE` node:

```python
# Node ID format
node_id = f"aws:glue_table:{environment_id}:glue://{database}/{table}"

# Qualified name format
qualified_name = f"glue://{database}/{table_name}"
```

**Naming Convention**:
- Database name: From Tableflow config or defaults to cluster ID
- Table name: Topic name (dots preserved, unlike UC which normalizes them)

**Example**:
```
Topic: "orders.v1"
Cluster: lkc-abc123
Glue Table: glue://lkc-abc123/orders.v1
```

### 2. Metadata Enrichment (enrich)

The provider fetches metadata for each Glue table via boto3:

**API Call**: `client.get_table(DatabaseName=database, Name=table_name)`

**Enriched Attributes**:
- `aws_region`: AWS region
- `owner`: Table owner
- `table_type`: EXTERNAL_TABLE, MANAGED_TABLE, VIRTUAL_VIEW
- `columns`: Array of column definitions with name, type, and comment
- `partition_keys`: Array of partition key definitions
- `storage_location`: S3 path
- `input_format`: Input format class (e.g., `org.apache.hadoop.mapred.TextInputFormat`)
- `output_format`: Output format class
- `serde_info`: SerDe library (e.g., `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`)
- `parameters`: User-defined key-value parameters
- `create_time`: Table creation timestamp
- `update_time`: Last update timestamp

**Retry Logic**: Exponential backoff on transient errors (max 3 retries)

**Error Handling**:
- `EntityNotFoundException`: Table not found (skipped with warning)
- `AccessDeniedException`: Insufficient permissions (skipped with warning)

### 3. Lineage Push (push_lineage)

Push Confluent lineage metadata back to Glue via the `update_table` API:

**Options**:
- `set_parameters`: Write `lineage_bridge.*` table parameters
- `set_description`: Write a human-readable lineage description

**Table Parameters** (merged into existing parameters):

```python
parameters = {
  "lineage_bridge.source_topics": "orders.v1",
  "lineage_bridge.source_connectors": "MySqlSourceConnector",
  "lineage_bridge.pipeline_type": "tableflow",
  "lineage_bridge.last_synced": "2026-04-30T12:34:56.789Z",
  "lineage_bridge.environment_id": "env-abc123",
  "lineage_bridge.cluster_id": "lkc-abc123"
}
```

**Table Description**:

```
Materialized from Kafka topic "orders.v1" via Confluent Tableflow.
Sources: MySqlSourceConnector
Environment: env-abc123
Last synced: 2026-04-30T12:34:56.789Z
Managed by LineageBridge
```

**Implementation Details**:
- Fetches existing table definition to preserve `StorageDescriptor` and other read-only fields
- Merges new parameters with existing parameters (preserves user-defined parameters)
- Updates table description (overwrites existing description)

**Usage Example** (UI):

1. Extract lineage with Tableflow enabled
2. Click **Push Lineage** in the sidebar
3. Select **AWS Glue**
4. Enable options:
   - Set table parameters
   - Set table description
5. Click **Push**

**Usage Example** (API):

```bash
curl -X POST http://localhost:8000/api/v1/lineage/push \
  -H "Content-Type: application/json" \
  -d '{
    "catalog_type": "AWS_GLUE",
    "set_parameters": true,
    "set_description": true
  }'
```

## Testing

### 1. Test Enrichment

Extract lineage with AWS credentials configured:

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
uv run lineage-bridge-extract
```

Check the extracted graph for Glue table nodes with enriched metadata:

```bash
cat lineage_graph.json | jq '.nodes[] | select(.node_type == "GLUE_TABLE")'
```

Expected attributes: `table_type`, `columns`, `partition_keys`, `storage_location`, `serde_info`

### 2. Test Lineage Push

In the UI:
1. Extract lineage
2. Click **Push Lineage** > **AWS Glue**
3. Enable all options
4. Click **Push**
5. Check results panel for success/error counts

Verify in AWS Glue Console or CLI:

```bash
# View table details
aws glue get-table \
  --database-name lkc-abc123 \
  --name orders.v1 \
  --region us-east-1

# Check parameters
aws glue get-table \
  --database-name lkc-abc123 \
  --name orders.v1 \
  --region us-east-1 \
  --query 'Table.Parameters' \
  --output json
```

Expected parameters:
```json
{
  "lineage_bridge.source_topics": "orders.v1",
  "lineage_bridge.source_connectors": "MySqlSourceConnector",
  "lineage_bridge.pipeline_type": "tableflow",
  "lineage_bridge.last_synced": "2026-04-30T12:34:56.789Z",
  "lineage_bridge.environment_id": "env-abc123",
  "lineage_bridge.cluster_id": "lkc-abc123"
}
```

### 3. Query via Athena

If using Amazon Athena, lineage parameters appear as table properties:

```sql
SHOW TBLPROPERTIES lkc_abc123.`orders.v1`;
```

Note: Athena requires backticks for table names with dots.

## Troubleshooting

### Error: "EntityNotFoundException: Table not found"

**Cause**: Table does not exist in Glue Data Catalog

**Fix**:
1. Verify Tableflow sync is running: `confluent tableflow connection list`
2. Check table exists: `aws glue get-table --database-name <db> --name <table>`
3. Confirm database/table names in Tableflow config match Glue

### Error: "AccessDeniedException: Insufficient permissions"

**Cause**: IAM credentials lack `glue:GetTable` or `glue:UpdateTable` permissions

**Fix**:
1. Verify IAM policy includes required permissions (see Prerequisites)
2. Check IAM user/role has the policy attached
3. Test credentials: `aws glue get-table --database-name <db> --name <table>`

### Error: "InvalidInputException: TableInput is invalid"

**Cause**: `update_table` requires specific fields from the existing table definition

**Fix**: This is handled automatically by `_build_table_input()`. If the error persists, the existing table may be corrupted. Try recreating it.

### Parameters appear but description is empty

**Cause**: `set_description` was disabled during push

**Fix**: Re-run lineage push with `set_description: true`

### Table has dots in the name and queries fail

**Cause**: Athena and some tools require escaping table names with dots

**Fix**: Use backticks: `SELECT * FROM db.\`orders.v1\``

## Integration with AWS Services

Glue lineage metadata is visible in:

- **AWS Glue Console**: View table properties and description
- **Amazon Athena**: Query tables and view properties via `SHOW TBLPROPERTIES`
- **Amazon Redshift Spectrum**: Access Glue tables from Redshift
- **AWS Lake Formation**: Manage permissions and governance
- **Amazon EMR**: Read Glue table metadata in Spark/Hive jobs

## Best Practices

1. **Use IAM Roles**: For production, use IAM roles instead of access keys (eliminates credential rotation)
2. **Tag Tables**: Add AWS tags to Glue tables for cost tracking and governance
3. **Monitor API Calls**: Glue API calls are logged in CloudTrail — monitor for errors
4. **Preserve User Parameters**: LineageBridge merges parameters, so user-defined parameters are preserved
5. **Database Naming**: Use cluster IDs as database names for multi-tenant isolation

## Deep Links

The provider generates deep links to the AWS Glue Console:

```
https://<region>.console.aws.amazon.com/glue/home?region=<region>#/v2/data-catalog/tables/view/<table>?database=<database>
```

Click any Glue table node in the LineageBridge UI to open it in the AWS Console.

## Next Steps

- [Databricks Unity Catalog Integration](databricks-unity-catalog.md) - Integrate with Unity Catalog
- [Google Data Lineage Integration](google-data-lineage.md) - Integrate with Google Data Lineage
- [Adding New Catalogs](adding-new-catalogs.md) - Build a custom catalog provider
