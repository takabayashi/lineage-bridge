# Configuration

LineageBridge uses environment variables for credential management. This guide covers all configuration options and best practices.

## Overview

Configuration follows these principles:

- **Credentials only** - Only API keys and secrets go in config (no runtime scope)
- **Environment variables** - All settings use the `LINEAGE_BRIDGE_` prefix
- **.env file support** - Store credentials in a `.env` file (never commit this!)
- **Credential hierarchy** - Falls back from specific to general credentials

## Quick Setup

### Step 1: Create .env File

Copy the example configuration:

```bash
cp .env.example .env
```

### Step 2: Edit .env File

Open `.env` in your editor and add your Confluent Cloud credentials:

```bash
# Minimum required configuration
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

That's it! LineageBridge will use these cloud-level credentials for all services.

!!! warning "Security"
    Never commit `.env` files to version control. The `.gitignore` file already excludes them.

## Confluent Cloud Credentials

### Cloud-Level API Key (Required)

The cloud-level API key is used to discover environments, clusters, and services. It requires **OrgAdmin** or **EnvironmentAdmin** permissions.

```env
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=ABC123DEF456
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=xyz789secretABC...
```

**How to create:**

1. Log in to [Confluent Cloud](https://confluent.cloud)
2. Navigate to **Administration → API Keys**
3. Click **+ Add key**
4. Select **Cloud resource** as the scope
5. Choose **My account** or create a service account
6. Save the key and secret immediately (they won't be shown again)

### Service-Scoped Credentials (Optional)

You can provide dedicated API keys for each Confluent service. If not set, the cloud API key is used as a fallback.

#### Kafka Cluster Access

Global Kafka credentials (used for all clusters unless overridden):

```env
LINEAGE_BRIDGE_KAFKA_API_KEY=kafka-key
LINEAGE_BRIDGE_KAFKA_API_SECRET=kafka-secret
```

#### Schema Registry

```env
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=sr-key
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=sr-secret
```

!!! tip "Auto-Discovery"
    The Schema Registry endpoint is auto-discovered if not provided. Only set this if you have multiple Schema Registry clusters.

#### ksqlDB

```env
LINEAGE_BRIDGE_KSQLDB_API_KEY=ksql-key
LINEAGE_BRIDGE_KSQLDB_API_SECRET=ksql-secret
```

The ksqlDB endpoint is auto-discovered from your clusters.

#### Flink SQL

```env
LINEAGE_BRIDGE_FLINK_API_KEY=flink-key
LINEAGE_BRIDGE_FLINK_API_SECRET=flink-secret
```

#### Tableflow

```env
LINEAGE_BRIDGE_TABLEFLOW_API_KEY=tableflow-key
LINEAGE_BRIDGE_TABLEFLOW_API_SECRET=tableflow-secret
```

### Per-Cluster Credentials (Advanced)

If each Kafka cluster has its own API key, provide them as a JSON map:

```env
LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-abc123":{"api_key":"key1","api_secret":"secret1"},"lkc-def456":{"api_key":"key2","api_secret":"secret2"}}
```

**Example formatted for readability:**

```json
{
  "lkc-abc123": {
    "api_key": "cluster1-key",
    "api_secret": "cluster1-secret"
  },
  "lkc-def456": {
    "api_key": "cluster2-key",
    "api_secret": "cluster2-secret"
  }
}
```

!!! note "UI Alternative"
    You can also add per-cluster credentials in the Streamlit UI sidebar instead of using this environment variable.

### Credential Resolution Order

LineageBridge resolves Kafka credentials in this order:

1. **Per-cluster credential** - from `CLUSTER_CREDENTIALS` map
2. **Global Kafka credential** - from `KAFKA_API_KEY` / `KAFKA_API_SECRET`
3. **Cloud-level credential** - from `CONFLUENT_CLOUD_API_KEY` (fallback)

## Data Catalog Credentials

### Databricks Unity Catalog

To enable Databricks UC integration:

```env
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi123456789abcdef
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456
```

**How to get these:**

1. **Workspace URL**: Your Databricks workspace URL (e.g., `https://dbc-12345678-abcd.cloud.databricks.com`)
2. **Token**: Create a personal access token in **User Settings → Developer → Access Tokens**
3. **Warehouse ID**: Find it in **SQL Warehouses** → select a warehouse → **Connection Details**

!!! tip "Service Principal"
    For production, use a service principal token instead of a personal access token.

### AWS Glue

For AWS Glue integration, LineageBridge uses the standard AWS credential chain (environment variables, AWS CLI config, IAM roles):

```env
LINEAGE_BRIDGE_AWS_REGION=us-east-1
```

**Required AWS permissions:**

- `glue:GetDatabase`
- `glue:GetTable`
- `glue:UpdateTable`

**Setting AWS credentials:**

=== "Environment Variables"
    ```env
    AWS_ACCESS_KEY_ID=your-access-key
    AWS_SECRET_ACCESS_KEY=your-secret-key
    AWS_REGION=us-east-1
    ```

=== "AWS CLI"
    ```bash
    aws configure
    ```

=== "IAM Role"
    Use an IAM instance role or ECS task role (no credentials needed).

### Google Data Lineage

For Google Cloud integration:

```env
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-gcp-project
LINEAGE_BRIDGE_GCP_LOCATION=us-central1
```

**Authentication:**

Google Cloud uses Application Default Credentials (ADC):

```bash
# Authenticate with your user account
gcloud auth application-default login

# Or set a service account key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

**Required APIs:**

- Data Lineage API
- BigQuery API (if using BigQuery connectors)

## REST API Configuration

If you plan to run the LineageBridge REST API:

```env
LINEAGE_BRIDGE_API_KEY=your-api-key-here
LINEAGE_BRIDGE_API_HOST=0.0.0.0
LINEAGE_BRIDGE_API_PORT=8000
```

- **API_KEY**: Optional authentication key. If unset, the API runs without authentication.
- **API_HOST**: Bind address (default: `0.0.0.0` for all interfaces)
- **API_PORT**: Port number (default: `8000`)

!!! warning "Production Deployment"
    Always set `API_KEY` in production to protect your lineage data.

## Logging

Control log verbosity:

```env
LINEAGE_BRIDGE_LOG_LEVEL=INFO
```

**Valid levels:**

- `DEBUG` - Verbose output including API requests and responses
- `INFO` - Standard output (default)
- `WARNING` - Warnings and errors only
- `ERROR` - Errors only

## Complete .env Example

Here's a fully-configured example:

```env
# ============================================================================
# LineageBridge Configuration
# ============================================================================

# ── Confluent Cloud (required) ──────────────────────────────────────────
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=ABC123DEF456
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=verylongsecretstring123

# ── Confluent Services (optional - falls back to cloud key) ─────────────
LINEAGE_BRIDGE_KAFKA_API_KEY=kafka-cluster-key
LINEAGE_BRIDGE_KAFKA_API_SECRET=kafka-cluster-secret

LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=sr-key
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=sr-secret

LINEAGE_BRIDGE_KSQLDB_API_KEY=ksql-key
LINEAGE_BRIDGE_KSQLDB_API_SECRET=ksql-secret

LINEAGE_BRIDGE_FLINK_API_KEY=flink-key
LINEAGE_BRIDGE_FLINK_API_SECRET=flink-secret

LINEAGE_BRIDGE_TABLEFLOW_API_KEY=tableflow-key
LINEAGE_BRIDGE_TABLEFLOW_API_SECRET=tableflow-secret

# ── Per-Cluster Credentials (optional) ──────────────────────────────────
# LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-abc123":{"api_key":"key1","api_secret":"secret1"}}

# ── Databricks Unity Catalog (optional) ─────────────────────────────────
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi123456789abcdef
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123def456

# ── AWS Glue (optional) ──────────────────────────────────────────────────
LINEAGE_BRIDGE_AWS_REGION=us-east-1

# ── Google Cloud (optional) ──────────────────────────────────────────────
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-gcp-project
LINEAGE_BRIDGE_GCP_LOCATION=us-central1

# ── REST API (optional) ──────────────────────────────────────────────────
LINEAGE_BRIDGE_API_KEY=my-secure-api-key
LINEAGE_BRIDGE_API_HOST=0.0.0.0
LINEAGE_BRIDGE_API_PORT=8000

# ── Logging ──────────────────────────────────────────────────────────────
LINEAGE_BRIDGE_LOG_LEVEL=INFO
```

## Environment-Specific Configuration

For multiple environments (dev, staging, prod), use different `.env` files:

```bash
# Development
.env

# Staging
.env.staging

# Production
.env.production
```

Load a specific env file:

```bash
# Copy to .env before running
cp .env.production .env
make ui
```

Or use dotenv directly:

```python
from dotenv import load_dotenv
load_dotenv(".env.production")
```

## Security Best Practices

### 1. Never Commit Credentials

Ensure `.env` is in your `.gitignore`:

```gitignore
.env
.env.*
!.env.example
```

### 2. Use Least Privilege

- Use dedicated API keys with minimal required permissions
- Avoid using admin keys where read-only access is sufficient

### 3. Rotate Credentials

Regularly rotate API keys and tokens, especially:

- After team member departures
- If credentials may have been exposed
- As part of routine security hygiene

### 4. Use Service Accounts

For production deployments:

- Use Confluent service accounts instead of user accounts
- Use Databricks service principals instead of personal access tokens
- Use AWS IAM roles instead of access keys when possible

### 5. Encrypt at Rest

For extra security, use encrypted storage:

- AWS Secrets Manager
- HashiCorp Vault
- Kubernetes Secrets (with encryption enabled)

## Next Steps

Now that your credentials are configured:

- **[Quickstart →](quickstart.md)** - Extract your first lineage graph
- **[Credential Management →](../how-to/credential-management.md)** - Advanced credential management patterns

## Troubleshooting

### Credential Not Found Errors

If you see "Cloud API key not configured":

1. Verify `.env` exists in the project root
2. Check variable names use the `LINEAGE_BRIDGE_` prefix
3. Ensure no extra spaces around `=` in `.env`
4. Try explicitly loading: `export $(cat .env | xargs)`

### Authentication Failures

If you get 401 Unauthorized errors:

1. Verify credentials are valid in Confluent Cloud UI
2. Check the API key has appropriate permissions
3. Ensure you're using the correct credential type (cloud vs. cluster-scoped)

### Schema Registry Issues

If Schema Registry extraction fails:

1. Ensure the endpoint URL is correct
2. Verify the API key has Schema Registry access
3. Try omitting the endpoint to use auto-discovery

For more help, see [Credential Issues](../troubleshooting/credential-issues.md).
