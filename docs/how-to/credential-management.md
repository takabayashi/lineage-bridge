# Credential Management

Best practices for managing Confluent Cloud credentials, per-cluster API keys, and secrets in different environments.

## Overview

LineageBridge needs credentials to access:

- **Confluent Cloud REST API** (environments, clusters, metadata)
- **Kafka clusters** (topics, consumer groups)
- **Schema Registry** (schemas, subjects)
- **ksqlDB, Flink, Tableflow** (queries, statements, mappings)
- **Data catalogs** (Databricks UC, AWS Glue, Google BigQuery)

This guide shows you how to configure, rotate, and secure these credentials.

## Understanding Credential Resolution

LineageBridge uses a **hierarchical fallback** for Kafka cluster credentials:

```
Per-Cluster Key → Global Kafka Key → Cloud API Key
```

For each cluster, LineageBridge tries:

1. **Per-cluster credential** from `CLUSTER_CREDENTIALS` JSON map
2. **Global Kafka credential** (`KAFKA_API_KEY` / `KAFKA_API_SECRET`)
3. **Cloud API key** (`CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`)

**Example:**
```bash
# Cloud API key (always required)
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-key-xyz
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-secret-xyz

# Global Kafka key (used for all clusters except those in CLUSTER_CREDENTIALS)
LINEAGE_BRIDGE_KAFKA_API_KEY=kafka-key-default
LINEAGE_BRIDGE_KAFKA_API_SECRET=kafka-secret-default

# Per-cluster keys (highest priority)
LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-prod":{"api_key":"kafka-key-prod","api_secret":"kafka-secret-prod"}}
```

**What happens:**
- Cluster `lkc-prod` uses `kafka-key-prod` (per-cluster)
- All other clusters use `kafka-key-default` (global)
- If `KAFKA_API_KEY` is not set, all clusters use `cloud-key-xyz` (cloud API fallback)

## .env File Structure

LineageBridge loads credentials from a `.env` file in your project root.

### Minimal Setup

Only the cloud API key is required:

```bash
# Confluent Cloud (required)
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

This works if your cloud API key has permissions to read from Kafka clusters, Schema Registry, ksqlDB, Flink, and Tableflow.

### Recommended Setup

Use service-scoped credentials for better security:

```bash
# ── Confluent Cloud API key (required) ────────────────────────────
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-api-secret

# ── Global Kafka credentials (optional) ───────────────────────────
LINEAGE_BRIDGE_KAFKA_API_KEY=kafka-api-key
LINEAGE_BRIDGE_KAFKA_API_SECRET=kafka-api-secret

# ── Schema Registry (optional, auto-discovered if unset) ──────────
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=https://psrc-xxxxx.region.cloud.confluent.cloud
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=sr-api-key
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=sr-api-secret

# ── ksqlDB (optional, auto-discovered if unset) ───────────────────
LINEAGE_BRIDGE_KSQLDB_API_KEY=ksql-api-key
LINEAGE_BRIDGE_KSQLDB_API_SECRET=ksql-api-secret

# ── Flink (optional) ──────────────────────────────────────────────
LINEAGE_BRIDGE_FLINK_API_KEY=flink-api-key
LINEAGE_BRIDGE_FLINK_API_SECRET=flink-api-secret

# ── Tableflow (optional) ──────────────────────────────────────────
LINEAGE_BRIDGE_TABLEFLOW_API_KEY=tableflow-api-key
LINEAGE_BRIDGE_TABLEFLOW_API_SECRET=tableflow-api-secret

# ── Databricks Unity Catalog (optional) ───────────────────────────
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi...
LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID=abc123

# ── AWS Glue (optional, uses AWS credentials from environment) ────
LINEAGE_BRIDGE_AWS_REGION=us-east-1

# ── Google Cloud (optional) ───────────────────────────────────────
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us
```

### Per-Cluster Credentials

For production setups with multiple clusters, use `CLUSTER_CREDENTIALS`:

```bash
LINEAGE_BRIDGE_CLUSTER_CREDENTIALS='{"lkc-prod-us":{"api_key":"prod-us-key","api_secret":"prod-us-secret"},"lkc-prod-eu":{"api_key":"prod-eu-key","api_secret":"prod-eu-secret"},"lkc-staging":{"api_key":"staging-key","api_secret":"staging-secret"}}'
```

**Formatting tips:**

- Use single quotes around the JSON to avoid shell escaping issues
- Minify the JSON (no extra whitespace)
- Each cluster ID maps to an object with `api_key` and `api_secret` fields

**JSON structure:**
```json
{
  "lkc-prod-us": {
    "api_key": "prod-us-key",
    "api_secret": "prod-us-secret"
  },
  "lkc-prod-eu": {
    "api_key": "prod-eu-key",
    "api_secret": "prod-eu-secret"
  }
}
```

## Auto-Provisioning with Confluent CLI

LineageBridge includes a script to auto-provision a cloud API key using the Confluent CLI.

### Prerequisites

1. Install the Confluent CLI:
   ```bash
   $ brew install confluentinc/tap/cli
   # or: https://docs.confluent.io/confluent-cli/current/install.html
   ```

2. Log in to Confluent Cloud:
   ```bash
   $ confluent login
   ```

### Run the Script

```bash
$ bash scripts/ensure-cloud-key.sh
```

**What it does:**

1. Checks if `.env` contains `CONFLUENT_CLOUD_API_KEY`
2. If missing, creates a cloud API key via `confluent api-key create --resource cloud`
3. Appends the key to `.env` with a timestamp comment
4. Exits silently if a key already exists

**Expected output:**
```
  ┌──────────────────────────────────────────────────────────┐
  │  No Confluent Cloud API key found.                       │
  │  Creating one via the Confluent CLI...                   │
  └──────────────────────────────────────────────────────────┘

  Creating Cloud API key...

  ✓ Cloud API key created: ABCD...WXYZ
  ✓ Saved to .env
```

### Use in Scripts

The `make ui` target runs this script automatically:

```bash
$ make ui
# Auto-provisions cloud key if missing, then starts the UI
```

For headless automation, run the script before extraction:

```bash
#!/bin/bash
set -euo pipefail

bash scripts/ensure-cloud-key.sh
uv run lineage-bridge-extract --env env-abc123
```

## Encrypted Cache

LineageBridge stores UI state (selected environments, per-cluster credentials) in an encrypted local cache.

### Cache Location

```
~/.lineage_bridge/cache.json        # Encrypted cache
~/.lineage_bridge/.cache_key        # Fernet encryption key (owner-only)
```

### What's Cached

- Selected environments and clusters (plaintext)
- Per-cluster API credentials entered via UI (encrypted)
- Schema Registry credentials (encrypted)
- Flink credentials (encrypted)
- Auto-provisioned API keys (encrypted)

### Encryption

LineageBridge uses **Fernet symmetric encryption** (AES-128-CBC with HMAC-SHA256):

1. On first run, generates a Fernet key and stores it in `~/.lineage_bridge/.cache_key`
2. Sets file permissions to `600` (owner-only read/write)
3. Encrypts sensitive fields before writing to `cache.json`
4. Decrypts on load

**Security properties:**

- Key never leaves the local machine
- Cache file is useless without the key file
- Key file has restricted permissions (owner-only)

### Cache Invalidation

To clear the cache:

```bash
$ rm -rf ~/.lineage_bridge/
```

Or delete specific entries by editing `cache.json` (decrypt it first, or just delete the file).

## Credential Rotation

### Cloud API Key Rotation

1. **Create a new cloud API key:**
   ```bash
   $ confluent api-key create --resource cloud --description "LineageBridge (new)"
   ```

2. **Update `.env`:**
   ```bash
   LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=new-cloud-key
   LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=new-cloud-secret
   ```

3. **Verify it works:**
   ```bash
   $ uv run lineage-bridge-extract
   ```

4. **Delete the old key:**
   ```bash
   $ confluent api-key delete old-cloud-key --force
   ```

### Cluster-Scoped Key Rotation

1. **Create a new cluster-scoped API key:**
   ```bash
   $ confluent api-key create --resource lkc-abc123 --description "LineageBridge (new)"
   ```

2. **Update `.env` (per-cluster credentials):**
   ```bash
   # Old:
   LINEAGE_BRIDGE_CLUSTER_CREDENTIALS='{"lkc-abc123":{"api_key":"old-key","api_secret":"old-secret"}}'

   # New:
   LINEAGE_BRIDGE_CLUSTER_CREDENTIALS='{"lkc-abc123":{"api_key":"new-key","api_secret":"new-secret"}}'
   ```

3. **Verify it works:**
   ```bash
   $ uv run lineage-bridge-extract --env env-xyz --cluster lkc-abc123
   ```

4. **Delete the old key:**
   ```bash
   $ confluent api-key delete old-key --force
   ```

### Databricks Token Rotation

1. **Generate a new personal access token** in the Databricks workspace UI (User Settings → Access Tokens)

2. **Update `.env`:**
   ```bash
   LINEAGE_BRIDGE_DATABRICKS_TOKEN=new-token
   ```

3. **Verify it works:**
   ```bash
   $ uv run lineage-bridge-extract --env env-xyz
   # Check that UC enrichment succeeds
   ```

4. **Revoke the old token** in the Databricks UI

### AWS Credentials Rotation

LineageBridge uses the standard AWS credential chain (environment variables, `~/.aws/credentials`, IAM role).

To rotate:

1. **Update AWS credentials** via `aws configure` or environment variables
2. **Verify it works:**
   ```bash
   $ aws glue get-databases --region us-east-1
   $ uv run lineage-bridge-extract --env env-xyz
   ```

No changes to `.env` needed unless you're setting `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` explicitly.

## Secrets in CI/CD

### GitHub Actions

Store credentials as **repository secrets**:

1. Go to your repository → Settings → Secrets and variables → Actions
2. Add secrets:
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`
   - `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL`
   - `LINEAGE_BRIDGE_DATABRICKS_TOKEN`
   - (others as needed)

3. Reference them in your workflow:
   ```yaml
   env:
     LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY }}
     LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET }}
     LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL: ${{ secrets.LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL }}
     LINEAGE_BRIDGE_DATABRICKS_TOKEN: ${{ secrets.LINEAGE_BRIDGE_DATABRICKS_TOKEN }}
   ```

**Do NOT commit credentials to `.env` in CI**. Use secrets or environment variables.

### GitLab CI

Store credentials as **CI/CD variables**:

1. Go to your project → Settings → CI/CD → Variables
2. Add variables (mark as "Masked" and "Protected"):
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
   - `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`

3. Reference them in `.gitlab-ci.yml`:
   ```yaml
   extract-lineage:
     script:
       - export LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY}"
       - export LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET}"
       - uv run lineage-bridge-extract
   ```

### Docker Secrets

For Docker deployments, use **Docker secrets** or environment variables:

```bash
$ docker run \
  -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY="$CONFLUENT_KEY" \
  -e LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET="$CONFLUENT_SECRET" \
  lineage-bridge:latest \
  lineage-bridge-extract
```

Or mount a `.env` file as a volume (be careful with file permissions):

```bash
$ docker run \
  -v "$(pwd)/.env:/app/.env:ro" \
  lineage-bridge:latest \
  lineage-bridge-extract
```

## Security Best Practices

### 1. Use Service-Scoped API Keys

Prefer cluster-scoped, Schema Registry-scoped, and ksqlDB-scoped keys over cloud API keys.

**Why:** Cloud API keys have broad permissions. Service-scoped keys limit the blast radius if compromised.

**How:**
```bash
# Create cluster-scoped key
$ confluent api-key create --resource lkc-abc123 --description "LineageBridge read-only"

# Create Schema Registry key
$ confluent api-key create --resource lsrc-xyz --description "LineageBridge SR read"
```

### 2. Restrict File Permissions

`.env` should be readable only by you:

```bash
$ chmod 600 .env
```

The cache directory is auto-secured by LineageBridge (permissions `700` on the directory, `600` on files).

### 3. Rotate Credentials Regularly

Rotate API keys every 90 days:

1. Create new keys
2. Update `.env` and CI/CD secrets
3. Verify everything works
4. Delete old keys

### 4. Use Read-Only Keys

LineageBridge only reads metadata. It never writes to Kafka, Schema Registry, or ksqlDB (except when pushing lineage to catalogs, which is opt-in).

**Grant minimal permissions:**
- Kafka: `DeveloperRead` or `ResourceOwner` (read-only)
- Schema Registry: `ResourceOwner` (read schemas)
- ksqlDB: read-only access (no `CREATE`, `DROP`, `INSERT`)

### 5. Never Commit `.env` to Git

Add `.env` to `.gitignore`:

```bash
$ echo ".env" >> .gitignore
```

Use `.env.example` as a template (no real credentials).

### 6. Audit Access

Check which API keys exist:

```bash
$ confluent api-key list
```

Delete unused keys:

```bash
$ confluent api-key delete <key-id> --force
```

## Troubleshooting

### "401 Unauthorized"

**Cause:** The API key is invalid or lacks required permissions.

**Solution:**

1. **Verify the key exists and is active:**
   ```bash
   $ confluent api-key list
   ```

2. **Check which credential is being used.** LineageBridge logs the credential resolution order:
   ```
   [INFO] Using per-cluster credential for lkc-abc123
   [INFO] Using global Kafka credential for lkc-def456
   [INFO] Using cloud API key for lkc-ghi789
   ```

3. **Test the key manually:**
   ```bash
   $ confluent kafka topic list --cluster lkc-abc123 \
       --api-key <key> --api-secret <secret>
   ```

4. **If the key is wrong, update `.env` and restart.**

### "403 Forbidden"

**Cause:** The API key is valid but lacks the required permissions.

**Solution:**

1. **Check the key's role:**
   ```bash
   $ confluent api-key describe <key>
   ```

2. **Grant read permissions** via the Confluent Cloud UI (IAM → API Keys).

3. **For cluster-scoped keys, ensure they have ACLs:**
   ```bash
   $ confluent kafka acl list --cluster lkc-abc123 --api-key <key> --api-secret <secret>
   ```

### "Could not find .env file"

**Cause:** `.env` doesn't exist in your project root.

**Solution:**

1. **Copy the example:**
   ```bash
   $ cp .env.example .env
   ```

2. **Fill in your credentials.**

3. **Verify the file exists:**
   ```bash
   $ ls -la .env
   ```

### "Cluster credential JSON is invalid"

**Cause:** Malformed `CLUSTER_CREDENTIALS` JSON.

**Solution:**

1. **Validate the JSON:**
   ```bash
   $ echo "$LINEAGE_BRIDGE_CLUSTER_CREDENTIALS" | jq .
   ```

2. **Check for common issues:**
   - Missing quotes around keys or values
   - Trailing commas
   - Single quotes inside double-quoted strings (use `\'` or switch outer quotes)

3. **Use a JSON formatter:**
   ```bash
   $ echo '{"lkc-abc":{"api_key":"foo","api_secret":"bar"}}' | jq -c .
   ```

## Next Steps

- [Set up multi-environment extraction](multi-environment-setup.md) with per-cluster credentials
- [Deploy with Docker](docker-deployment.md) to isolate secrets in containers
- [Integrate with CI/CD](ci-cd-integration.md) to manage secrets in GitHub Actions or GitLab CI
