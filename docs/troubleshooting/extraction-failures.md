# Extraction Failures

Debugging common issues when extraction succeeds but produces incomplete or unexpected results.

## Timeout Errors

### Symptoms

```
Extractor 'KafkaAdmin:lkc-xyz789' timed out after 120s
```

### Causes

1. **Slow API response** - Large clusters, many topics
2. **Network latency** - Distance from API region, poor connectivity
3. **Overloaded backend** - Confluent Cloud API under load

### Solutions

#### Reduce Extraction Scope

Extract one cluster at a time:

```bash
uv run lineage-bridge-extract --env env-abc123 --cluster lkc-xyz789
```

#### Disable Heavy Extractors

Metrics and Stream Catalog can be slow:

```bash
# In UI: uncheck "Metrics" and "Stream Catalog" in sidebar

# Or disable via orchestrator in code
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_metrics=False,
    enable_stream_catalog=False,
)
```

#### Split Extraction and Enrichment

```bash
# Extract without enrichment
uv run lineage-bridge-extract --env env-abc123 --no-enrich --output graph_raw.json

# Enrich separately
uv run lineage-bridge-extract --enrich-only --output graph_raw.json
```

#### Check Network

Test latency to API endpoints:

```bash
ping api.confluent.cloud
curl -w "@-" -o /dev/null -s https://api.confluent.cloud <<'EOF'
     time_namelookup:  %{time_namelookup}s\n
        time_connect:  %{time_connect}s\n
     time_appconnect:  %{time_appconnect}s\n
       time_redirect:  %{time_redirect}s\n
    time_pretransfer:  %{time_pretransfer}s\n
  time_starttransfer:  %{time_starttransfer}s\n
                     ----------\n
          time_total:  %{time_total}s\n
EOF
```

## Missing Topics

### Symptoms

- Graph shows no topics
- Graph shows some topics but not all
- Specific topics are missing

### Causes

1. **KafkaAdmin extractor disabled** - Extractor not running
2. **Wrong cluster credentials** - Using Cloud API key instead of cluster key
3. **Cluster filter mismatch** - Topic exists in different cluster
4. **Permission issues** - API key lacks read permissions

### Solutions

#### Enable KafkaAdmin Extractor

Check in UI sidebar or CLI:

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_kafka_admin=True,  # Ensure this is True
)
```

#### Verify Cluster Credentials

Some clusters require cluster-scoped API keys:

```bash
# .env
LINEAGE_BRIDGE_KAFKA_API_KEY=CLUSTER_KEY
LINEAGE_BRIDGE_KAFKA_API_SECRET=CLUSTER_SECRET
```

Create a cluster API key:

```bash
confluent api-key create --resource lkc-xyz789
```

#### Remove Cluster Filter

If using `--cluster`, remove it to see all clusters:

```bash
uv run lineage-bridge-extract --env env-abc123  # No --cluster flag
```

#### Check Permissions

Verify API key has read permissions:

```bash
confluent api-key list
```

Test with Confluent CLI:

```bash
confluent kafka topic list --cluster lkc-xyz789
```

## Missing Connectors

### Symptoms

- Graph shows topics but no connectors
- Some connectors are missing

### Causes

1. **Connect extractor disabled**
2. **Wrong environment ID**
3. **Connector in different cluster**

### Solutions

#### Enable Connect Extractor

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_connect=True,
)
```

#### Verify Environment

List connectors in environment:

```bash
confluent connect cluster list --environment env-abc123
confluent connect list --cluster lcc-abc123
```

#### Check Connector Status

Connector may be FAILED or DELETED:

```bash
confluent connect describe <connector-name> --cluster lcc-abc123
```

## Missing ksqlDB Queries

### Symptoms

- Graph shows topics but no ksqlDB queries
- Some queries are missing

### Causes

1. **ksqlDB extractor disabled**
2. **Wrong ksqlDB credentials**
3. **ksqlDB cluster not found**
4. **Query is not persistent** (only persistent queries are extracted)

### Solutions

#### Enable ksqlDB Extractor

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_ksqldb=True,
)
```

#### Verify ksqlDB Cluster

List ksqlDB clusters:

```bash
confluent ksql app list --environment env-abc123
```

#### Check Query Type

Only persistent queries are extracted (CTAS, CSAS):

```sql
-- This is extracted
CREATE STREAM enriched_orders AS
  SELECT * FROM orders WHERE amount > 100;

-- This is NOT extracted (transient query)
SELECT * FROM orders;
```

List persistent queries:

```bash
confluent ksql app describe lksqlc-abc123
```

## Missing Flink Jobs

### Symptoms

- Graph shows topics but no Flink statements
- Some statements are missing

### Causes

1. **Flink extractor disabled**
2. **Wrong organization ID**
3. **Flink credentials missing**
4. **Statement is STOPPED or DELETED**

### Solutions

#### Enable Flink Extractor

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_flink=True,
)
```

#### Verify Organization ID

Flink API requires organization ID:

```bash
# Check logs for organization ID
uv run lineage-bridge-extract --env env-abc123 | grep "organization"
```

If missing:

```
Could not determine organization ID for Flink in env-abc123 — Flink extraction skipped
```

Organization ID is auto-discovered from cluster metadata. If this fails, it's a bug — please report it.

#### Check Statement Status

Only RUNNING statements are extracted:

```bash
confluent flink statement list --environment env-abc123
```

## Missing Schemas

### Symptoms

- Graph shows topics but no schema nodes
- HAS_SCHEMA edges are missing
- Schema enrichment skipped

### Causes

1. **Schema Registry extractor disabled**
2. **Schema Registry endpoint not found**
3. **Schema Registry credentials missing**
4. **Topics don't use schemas** (e.g., JSON, Avro with embedded schema)

### Solutions

#### Enable Schema Registry Extractor

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_schema_registry=True,
)
```

#### Verify Schema Registry Endpoint

Check logs for:

```
Schema Registry found: https://psrc-abc123.us-east-1.aws.confluent.cloud
```

If not found:

```
No Schema Registry endpoint found for env-abc123.
Set LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY in .env
or check Stream Governance is enabled.
```

Manually set endpoint:

```bash
# .env
LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT=https://psrc-abc123.us-east-1.aws.confluent.cloud
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY=SR_KEY
LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET=SR_SECRET
```

#### Check Schema Exists

List schemas:

```bash
confluent schema-registry schema list --environment env-abc123
```

Topics without schemas won't have HAS_SCHEMA edges.

## Missing Catalog Tables

### Symptoms

- Graph shows topics but no UC/Glue/BigQuery tables
- MATERIALIZES edges are missing
- Tableflow extraction skipped

### Causes

1. **Tableflow extractor disabled**
2. **No Tableflow integrations configured**
3. **Catalog credentials missing**
4. **Table not mapped to topic**

### Solutions

#### Enable Tableflow Extractor

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_tableflow=True,
)
```

#### Verify Tableflow Integrations

Check if Tableflow is configured in Confluent Cloud:

1. Confluent Cloud UI → Environment → Tableflow
2. Verify integrations exist (Unity Catalog, AWS Glue, etc.)
3. Verify topic-table mappings exist

#### Verify Catalog Credentials

**Databricks UC**:
```bash
# .env
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://myworkspace.cloud.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=dapi...
```

**AWS Glue**:
```bash
# .env or ~/.aws/credentials
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

**Google BigQuery**:
```bash
# .env
LINEAGE_BRIDGE_GCP_PROJECT_ID=my-project
LINEAGE_BRIDGE_GCP_LOCATION=us-central1
```

Test credentials:

```bash
# Databricks
curl -H "Authorization: Bearer $LINEAGE_BRIDGE_DATABRICKS_TOKEN" \
  "$LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL/api/2.0/sql/warehouses"

# AWS
aws glue get-databases --region us-east-1

# Google
bq ls $LINEAGE_BRIDGE_GCP_PROJECT_ID:
```

## Orphan Nodes

### Symptoms

Graph validation warnings:

```
Orphan node: confluent:kafka_topic:env-abc123:my-topic
```

### Causes

1. **Topic has no producers or consumers**
2. **Producers/consumers in disabled extractors**
3. **Expected behavior** - Not all topics have lineage

### Solutions

#### Enable All Extractors

Orphan topics may have producers/consumers in disabled extractors:

```python
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_connect=True,
    enable_ksqldb=True,
    enable_flink=True,
)
```

#### Check Topic Usage

Topic may genuinely have no producers or consumers:

```bash
# Check consumer groups
confluent kafka topic describe my-topic --cluster lkc-xyz789

# Check connectors
confluent connect list --cluster lcc-abc123
```

#### Ignore Warnings

Orphan nodes are expected in partial extractions. The graph is still valid and usable.

To hide orphans in UI, use the filter:

```
UI → Filters → Hide orphan nodes
```

## Dangling Edges

### Symptoms

Graph validation warnings:

```
Dangling edge src: confluent:connector:env-abc123:lkc-xyz789:mysql-source
Dangling edge dst: confluent:kafka_topic:env-abc123:orders
```

### Causes

1. **Bug in extractor** - Edge references non-existent node
2. **Race condition** - Node deleted between extraction phases
3. **Cluster filter** - Edge references node in filtered-out cluster

### Solutions

#### Enable Full Extraction

Remove cluster filters:

```bash
uv run lineage-bridge-extract --env env-abc123  # No --cluster flag
```

#### Report Bug

Dangling edges should not occur. If you see this consistently, it's a bug:

1. Export graph: `graph.to_json_file("graph.json")`
2. Open GitHub issue with graph JSON and extraction logs

#### Workaround

Graph is still usable. Dangling edges are ignored in rendering.

## Partial Extraction Results

### Symptoms

- Some extractors return empty results
- Graph is smaller than expected
- Multiple warnings in logs

### Causes

1. **Extractor failures** - API errors, timeouts
2. **Missing credentials** - Some extractors skipped
3. **Resource not configured** - No connectors, ksqlDB, etc.

### Solutions

#### Review Logs

Look for warnings:

```bash
uv run lineage-bridge-extract --env env-abc123 | grep "Warning"
```

Common warnings:

```
Warning: No Schema Registry endpoint found
Warning: Extractor 'Connect' failed: 403 Forbidden
Warning: Could not determine organization ID for Flink
```

#### Enable Debug Logging

```bash
# .env
LINEAGE_BRIDGE_LOG_LEVEL=DEBUG
```

#### Extract in Stages

Extract different resource types separately to isolate failures:

```python
# Extract topics only
await run_extraction(
    settings,
    environment_ids=["env-abc123"],
    enable_connect=False,
    enable_ksqldb=False,
    enable_flink=False,
    enable_schema_registry=False,
)
```

## Next Steps

- [API Errors](api-errors.md) - HTTP error codes
- [Credential Issues](credential-issues.md) - Authentication problems
- [Performance](performance.md) - Optimization tips
