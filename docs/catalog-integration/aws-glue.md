# AWS Glue Data Catalog Integration

**What you'll build**: Kafka lineage visible in AWS Glue, Athena, and Redshift Spectrum with enriched table metadata showing source topics and connectors.

**Why this matters**: Your data platform is AWS-native. Analysts query Glue tables with Athena, compliance needs to know data sources, and your S3 data lake is cataloged in Glue. LineageBridge bridges the gap between Confluent and AWS.

## Data Flow

Here's how Kafka topics become AWS Glue tables:

```mermaid
graph LR
    A[Kafka Topic<br/>orders.v1] --> B[Confluent Tableflow]
    B --> C[S3 Path<br/>s3://bucket/orders/]
    C --> D[Glue Table<br/>lkc-abc123.orders.v1]
    D --> E[Glue API]
    E --> F[Schema & Partitions]
    F --> G[LineageBridge Graph]
    G --> H[Update Table Parameters]
    H --> I[Athena Queries]
    H --> J[Redshift Spectrum]
```

**LineageBridge role**:
1. Discovers the Tableflow-created Glue table
2. Enriches it with schema, SerDe, and storage metadata from Glue API
3. Pushes Kafka source metadata as table parameters (queryable via `SHOW TBLPROPERTIES`)
4. Makes lineage visible in Athena and other AWS tools

## Capabilities

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

=== "Environment Variables"

    ```bash
    # Required: AWS region
    export LINEAGE_BRIDGE_AWS_REGION=us-east-1
    
    # Option 1: Use IAM role (recommended for EC2/ECS/Lambda)
    # No additional config needed — boto3 auto-discovers role
    
    # Option 2: Use access keys
    export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
    export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

=== ".env File"

    ```bash
    # Add to .env in your project root
    
    # Required: AWS region
    LINEAGE_BRIDGE_AWS_REGION=us-east-1
    
    # Option 2: Access keys (not recommended for production)
    AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
    AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

=== "AWS Credentials File"

    ```bash
    # ~/.aws/credentials
    [default]
    aws_access_key_id = AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    
    # Then set region in .env or env var
    export LINEAGE_BRIDGE_AWS_REGION=us-east-1
    ```

=== "IAM Role (Production)"

    ```bash
    # When running on EC2, ECS, or Lambda, just set region
    export LINEAGE_BRIDGE_AWS_REGION=us-east-1
    
    # Attach IAM role with this policy to your instance/task/function:
    # - glue:GetTable
    # - glue:UpdateTable
    ```

**Credential Resolution Order** (boto3 standard):
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (EC2 instance profile, ECS task role, Lambda execution role)

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
  "lineage_bridge.upstream_chain": '[{"hop":1,"kind":"topic","qualified_name":"orders.v1","schema_fields":[{"name":"order_id","type":"long"}]},{"hop":2,"kind":"flink_job","qualified_name":"enrich_orders","sql":"SELECT * FROM ..."},{"hop":3,"kind":"connector","qualified_name":"debezium-mysql","connector_class":"DebeziumMysqlConnector"}]',
  "lineage_bridge.pipeline_type": "tableflow",
  "lineage_bridge.last_synced": "2026-04-30T12:34:56.789Z",
  "lineage_bridge.environment_id": "env-abc123",
  "lineage_bridge.cluster_id": "lkc-abc123"
}
```

`lineage_bridge.upstream_chain` is the **multi-hop chain** as a JSON array, ordered by hop distance from the Glue table. Each hop carries `kind` (topic / flink_job / ksqldb_query / connector / external_dataset / tableflow_table), the qualified name, optional `sql` for Flink/ksqlDB, optional `connector_class`, and `schema_fields` for topics that have a `HAS_SCHEMA` edge. The flat `source_topics` / `source_connectors` keys are kept for backwards compatibility.

Glue Parameter values cap at 512 KB; the chain JSON is capped at 64 KB to keep table descriptions sane. If the chain is truncated, `lineage_bridge.upstream_truncated = "true"` is also set.

Query the chain from Athena:

```sql
SELECT t.parameters['lineage_bridge.upstream_chain'] AS chain
FROM information_schema.tables t
WHERE t.table_schema = 'my_database' AND t.table_name = 'orders_v1';
```

**Table Description**:

```
Upstream lineage:
- connector: debezium-mysql [DebeziumMysqlConnector]
  - topic: orders.v1 [3 columns]
    - flink_job: enrich_orders [SQL: SELECT * FROM ...]
  → orders_v1
Environment: env-abc123
Last synced: 2026-04-30T12:34:56.789Z
Managed by LineageBridge
```

The description renders the chain as an indented tree, walking from the farthest upstream toward the table — visible in the Glue console table detail and in Athena's query catalog browser.

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

**What it means**: The Glue table doesn't exist yet.

**How to fix**:
1. Verify Tableflow is running:
   ```bash
   confluent tableflow connection list
   ```
2. Check the table exists in Glue:
   ```bash
   aws glue get-table --database-name lkc-abc123 --name orders.v1 --region us-east-1
   ```
3. Check Tableflow config matches Glue naming:
   - Database name (default: cluster ID)
   - Table name (raw topic name with dots preserved)

**Common cause**: Tableflow sync hasn't completed. Wait a few minutes after creating the integration.

### Error: "AccessDeniedException: Insufficient permissions"

**What it means**: Your IAM credentials lack Glue permissions.

**How to fix**:
1. Attach this policy to your IAM user/role:
   ```json
   {
     "Effect": "Allow",
     "Action": ["glue:GetTable", "glue:UpdateTable"],
     "Resource": ["arn:aws:glue:*:*:catalog", "arn:aws:glue:*:*:database/*", "arn:aws:glue:*:*:table/*"]
   }
   ```
2. Test permissions:
   ```bash
   aws glue get-table --database-name lkc-abc123 --name orders.v1
   ```
3. If still failing, check `aws sts get-caller-identity` to verify which credentials you're using

**Common cause**: Using credentials from the wrong AWS account or missing policy attachment.

### Error: "InvalidInputException: TableInput is invalid"

**What it means**: The table definition is malformed (rare).

**How to fix**:
This is handled automatically by `_build_table_input()`. If it persists:
1. Check the table wasn't manually edited with invalid fields
2. Try recreating the Glue table via Tableflow
3. Check CloudTrail logs for the exact validation error

**Common cause**: Table was manually modified outside Tableflow.

### Parameters appear but description is empty

**What it means**: You disabled description updates during lineage push.

**How to fix**:
Re-run lineage push in the UI with "Set table description" enabled, or via API:
```bash
curl -X POST http://localhost:8000/api/v1/lineage/push \
  -H "Content-Type: application/json" \
  -d '{"catalog_type": "AWS_GLUE", "set_description": true}'
```

### Athena query fails: "Table orders.v1 not found"

**What it means**: Athena can't parse table names with dots.

**How to fix**:
Wrap the table name in backticks:
```sql
SELECT * FROM lkc_abc123.`orders.v1` LIMIT 10;
```

Or in your catalog tool:
```sql
SHOW TBLPROPERTIES lkc_abc123.`orders.v1`;
```

**Common cause**: Glue preserves topic names with dots (unlike UC which normalizes them).

## Integration with AWS Services

Glue lineage metadata is visible in:

- **AWS Glue Console**: View table properties and description
- **Amazon Athena**: Query tables and view properties via `SHOW TBLPROPERTIES`
- **Amazon Redshift Spectrum**: Access Glue tables from Redshift
- **AWS Lake Formation**: Manage permissions and governance
- **Amazon EMR**: Read Glue table metadata in Spark/Hive jobs

## Common Pitfalls

### Pitfall 1: Wrong AWS Region

**Problem**: Region configured in LineageBridge doesn't match Tableflow/Glue region

**Symptom**: "EntityNotFoundException" even though table exists

**Fix**: Match regions exactly
```bash
# Check Glue region in AWS console URL or Tableflow config
# Update .env to match
LINEAGE_BRIDGE_AWS_REGION=us-west-2  # Must match your Glue catalog region
```

### Pitfall 2: Access Keys in Environment

**Problem**: Credentials from previous project in environment variables

**Symptom**: "AccessDeniedException" or wrong account

**Fix**: Check which credentials boto3 is using
```bash
# Clear unwanted env vars
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY

# Verify credentials
aws sts get-caller-identity

# Use credentials file or IAM role instead
```

### Pitfall 3: Table Names with Dots

**Problem**: Topic `orders.v1` becomes Glue table `orders.v1` (dots preserved)

**Symptom**: Athena queries fail: "Table orders.v1 not found"

**Fix**: Use backticks in Athena
```sql
-- Wrong
SELECT * FROM lkc_abc123.orders.v1;

-- Right
SELECT * FROM lkc_abc123.`orders.v1`;
```

### Pitfall 4: Insufficient IAM Permissions

**Problem**: Policy grants `glue:*` on wrong resources

**Symptom**: "AccessDeniedException" even with broad permissions

**Fix**: Grant on catalog, database, AND table resources
```json
{
  "Resource": [
    "arn:aws:glue:*:*:catalog",
    "arn:aws:glue:*:*:database/*",
    "arn:aws:glue:*:*:table/*"
  ]
}
```

### Pitfall 5: Overwriting User Parameters

**Problem**: Worried lineage push will overwrite existing table parameters

**Symptom**: Hesitation to enable lineage push

**Reality**: LineageBridge merges parameters — user-defined parameters are preserved
```python
# Existing parameters
{"user_param": "value", "another_param": "123"}

# After lineage push
{
  "user_param": "value",  # Preserved
  "another_param": "123",  # Preserved
  "lineage_bridge.source_topics": "orders.v1"  # Added
}
```

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
