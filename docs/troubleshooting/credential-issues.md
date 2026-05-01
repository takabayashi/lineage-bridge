# Credential Issues

Common authentication and authorization problems when using LineageBridge.

## 401 Unauthorized

### Symptoms

```
Extractor 'KafkaAdmin:lkc-xyz789' got 401 Unauthorized.
This likely means a cluster-scoped API key is needed.
Set LINEAGE_BRIDGE_KAFKA_API_KEY in .env.
```

### Causes

1. **Missing API key** - No credentials configured
2. **Invalid API key** - Typo, expired, or deleted key
3. **Wrong API key type** - Cloud API key used where cluster-scoped key is required

### Solutions

#### For Confluent Cloud API

Verify your Cloud API key is correctly configured:

```bash
# .env
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=ABCDEF1234567890
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=XYZABC...
```

Test the key with the Confluent CLI:

```bash
confluent login --save
confluent environment list
```

#### For Cluster-Scoped Operations

Some operations (Kafka REST API) require cluster-scoped API keys:

```bash
# .env
LINEAGE_BRIDGE_KAFKA_API_KEY=CLUSTER_KEY
LINEAGE_BRIDGE_KAFKA_API_SECRET=CLUSTER_SECRET
```

Create a cluster API key:

```bash
confluent api-key create --resource lkc-xyz789
```

#### For Schema Registry

Schema Registry has its own API keys:

```bash
# .env
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=SR_KEY
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=SR_SECRET
```

If not set, LineageBridge will try the Cloud API key, which may not work.

Create a Schema Registry API key:

```bash
confluent api-key create --resource lsrc-abc123
```

#### For ksqlDB

ksqlDB clusters can have their own API keys:

```bash
# .env
LINEAGE_BRIDGE_KSQLDB_API_KEY=KSQL_KEY
LINEAGE_BRIDGE_KSQLDB_API_SECRET=KSQL_SECRET
```

Create a ksqlDB API key:

```bash
confluent api-key create --resource lksqlc-xyz789
```

#### For Flink

Flink SQL has its own API keys:

```bash
# .env
LINEAGE_BRIDGE_FLINK_API_KEY=FLINK_KEY
LINEAGE_BRIDGE_FLINK_API_SECRET=FLINK_SECRET
```

Create a Flink API key in the Confluent Cloud UI.

#### For Databricks

Databricks uses personal access tokens:

```bash
# .env
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi1234567890abcdef
```

Create a token in Databricks:
1. User Settings → Developer → Access Tokens → Generate New Token

#### For AWS Glue

AWS Glue uses boto3 credential chain (environment variables, `~/.aws/credentials`, IAM roles):

```bash
# .env or environment
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
```

Or configure AWS CLI:

```bash
aws configure
```

#### For Google BigQuery

Google uses Application Default Credentials:

```bash
# .env
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us-central1
```

Authenticate with gcloud:

```bash
gcloud auth application-default login
```

Or set service account key:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

## 403 Forbidden

### Symptoms

```
Extractor 'Connect' got 403 Forbidden. The API key lacks required permissions.
```

### Causes

1. **Insufficient permissions** - API key doesn't have required RBAC roles
2. **Wrong resource scope** - Key scoped to wrong cluster/environment
3. **Organization restrictions** - API disabled at organization level

### Solutions

#### Verify Cloud API Key Permissions

Required permissions for LineageBridge:

- **CloudClusterAdmin** or **CloudClusterRead** - Read cluster metadata
- **ResourceOwner** or **ResourceRead** - Read connectors, ksqlDB, Flink
- **OrganizationAdmin** (optional) - For organization-level discovery

Grant permissions in Confluent Cloud UI:
1. Accounts & Access → Service Accounts
2. Select the service account
3. Add role bindings

#### Verify Cluster API Key Permissions

Cluster API keys need:
- **Read** permission on the cluster

#### Verify Schema Registry Permissions

Schema Registry keys need:
- **Read** permission on Schema Registry cluster

#### Verify Databricks Permissions

Databricks tokens need:
- **CAN_USE** on SQL Warehouse (for Tableflow)
- **SELECT** on catalog.schema.table (for enrichment)
- **MODIFY** on catalog.schema.table (for lineage push)

Grant permissions in Databricks:

```sql
GRANT USE SCHEMA ON CATALOG main TO `user@example.com`;
GRANT SELECT ON TABLE main.sales.orders TO `user@example.com`;
```

#### Verify AWS Glue Permissions

AWS IAM user/role needs:
- `glue:GetTable` - Read table metadata
- `glue:UpdateTable` - Write lineage metadata

Example IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:UpdateTable",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTables"
      ],
      "Resource": "*"
    }
  ]
}
```

#### Verify Google BigQuery Permissions

Service account or user needs:
- `bigquery.tables.get` - Read table metadata
- `datalineage.events.create` - Push lineage events

Grant permissions in Google Cloud Console:
1. IAM & Admin → IAM
2. Add role: **BigQuery Data Viewer** + **Data Lineage Producer**

## 400 Bad Request

### Symptoms

```
Extractor 'KafkaAdmin' got 400 Bad Request.
The API key may not have access to this environment,
or the API parameters are invalid.
```

### Causes

1. **Invalid environment ID** - Typo or environment doesn't exist
2. **Invalid cluster ID** - Cluster doesn't exist or wrong environment
3. **Malformed request** - Bug in client code (rare)

### Solutions

#### Verify Environment ID

List environments with the Confluent CLI:

```bash
confluent environment list
```

Use the correct ID format:

```bash
uv run lineage-bridge-extract --env env-abc123  # Correct
uv run lineage-bridge-extract --env abc123      # Wrong
```

#### Verify Cluster ID

List clusters in the environment:

```bash
confluent kafka cluster list --environment env-abc123
```

Filter by specific cluster:

```bash
uv run lineage-bridge-extract --env env-abc123 --cluster lkc-xyz789
```

#### Verify Resource Scope

Ensure the API key is scoped to the correct environment:

```bash
confluent api-key list
```

Output should show the environment association.

## Environment Variable Reference

### Confluent Cloud

```bash
# Required
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your_cloud_api_key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your_cloud_api_secret

# Optional (fall back to Cloud API key)
LINEAGE_BRIDGE_KAFKA_API_KEY=cluster_api_key
LINEAGE_BRIDGE_KAFKA_API_SECRET=cluster_api_secret
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=sr_api_key
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=sr_api_secret
LINEAGE_BRIDGE_KSQLDB_API_KEY=ksql_api_key
LINEAGE_BRIDGE_KSQLDB_API_SECRET=ksql_api_secret
LINEAGE_BRIDGE_FLINK_API_KEY=flink_api_key
LINEAGE_BRIDGE_FLINK_API_SECRET=flink_api_secret
LINEAGE_BRIDGE_TABLEFLOW_API_KEY=tableflow_api_key
LINEAGE_BRIDGE_TABLEFLOW_API_SECRET=tableflow_api_secret
```

### Databricks

```bash
# Required for UC enrichment and lineage push
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi1234567890abcdef

# Optional (auto-discovered)
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456
```

### AWS

```bash
# AWS region (required for Glue)
LINEAGE_BRIDGE_AWS_REGION=us-east-1

# AWS credentials (use environment or ~/.aws/credentials)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Google Cloud

```bash
# GCP project and location (required for BigQuery/Data Lineage)
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us-central1

# Credentials (use gcloud or service account key)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

## Testing Credentials

### Quick Test

```bash
uv run lineage-bridge-extract --env env-abc123
```

Look for these log messages:
- `✓ Discovered N clusters in env-abc123` - Cloud API key works
- `✓ Schema Registry found: https://...` - SR discovery works
- `✓ Phase 1/4: Extracting Kafka topics` - Cluster access works

### Per-Service Tests

**Confluent Cloud**:
```bash
confluent environment list
confluent kafka cluster list --environment env-abc123
```

**Schema Registry**:
```bash
curl -u "$LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY:$LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET" \
  https://psrc-abc123.us-east-1.aws.confluent.cloud/subjects
```

**Databricks**:
```bash
curl -H "Authorization: Bearer $LINEAGE_BRIDGE_DATABRICKS_TOKEN" \
  "$LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL/api/2.0/sql/warehouses"
```

**AWS Glue**:
```bash
aws glue get-databases --region us-east-1
```

**Google BigQuery**:
```bash
bq ls $LINEAGE_BRIDGE_GCP_PROJECT_ID:
```

## Next Steps

- [API Errors](api-errors.md) - Handling API error codes
- [Extraction Failures](extraction-failures.md) - Debugging missing data
- [Getting Started: Configuration](../getting-started/configuration.md)
