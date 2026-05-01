# Multi-Environment Setup

Extract lineage from multiple Confluent Cloud environments simultaneously and visualize the unified topology.

## Overview

LineageBridge can discover and extract from all environments in your Confluent Cloud organization, or you can selectively target specific environments and clusters. This guide shows you how to:

- Extract from all environments automatically
- Filter to specific environments or clusters
- Configure per-cluster API credentials
- Handle cross-environment data flows

## Before You Start

You'll need:

- A Confluent Cloud organization-level API key (cloud API key)
- Read access to the environments and clusters you want to scan
- Per-cluster API keys if using cluster-scoped credentials (optional)

## Auto-Discovery Mode

=== "UI"

    The simplest approach. LineageBridge discovers all available environments automatically:

    1. **Start the UI**:
       ```bash
       $ make ui
       ```

    2. **Wait for environment discovery**:
       The sidebar shows a loading spinner while discovering environments. When complete, you'll see all available environments listed.

    3. **Select environments**:
       - Check "All Environments" to extract from everything
       - Or select individual environments from the list

    4. **Select clusters** (optional):
       - Leave "All Clusters" checked to scan everything
       - Or expand an environment and select specific clusters

    5. **Run extraction**:
       Click "Extract Lineage" and watch the progress panel.

    **Expected output:**
    ```
    ✓ Discovered 3 environments
    ✓ Discovered 7 clusters
    ✓ Extracting from env-abc123, env-def456, env-ghi789...
    ✓ Extraction complete (142 nodes, 218 edges)
    ```

=== "CLI"

    For headless or scripted extractions:

    1. **Extract from all environments**:
       ```bash
       $ uv run lineage-bridge-extract
       ```

       This auto-discovers all environments and clusters. The `--env` flag is now **optional**.

    2. **Filter to specific environments**:
       ```bash
       $ uv run lineage-bridge-extract --env env-abc123 --env env-def456
       ```

    3. **Filter to specific clusters**:
       ```bash
       $ uv run lineage-bridge-extract --env env-abc123 --cluster lkc-111111 --cluster lkc-222222
       ```

    **Expected output:**
    ```
    [INFO] Discovering environments...
    [INFO] Found 3 environments: env-abc123, env-def456, env-ghi789
    [INFO] Discovering clusters...
    [INFO] Found 7 clusters across 3 environments
    [INFO] Extracting from env-abc123 (2 clusters)...
    [INFO] Phase 1: KafkaAdmin (lkc-111111)
    [INFO]   - Found 23 topics, 5 consumer groups
    [INFO] Phase 1: KafkaAdmin (lkc-222222)
    [INFO]   - Found 18 topics, 3 consumer groups
    [INFO] Phase 2: Connect, ksqlDB, Flink (parallel)
    [INFO]   - Connect: 4 connectors
    [INFO]   - ksqlDB: 2 queries
    [INFO]   - Flink: 1 statement
    [INFO] Phase 3: SchemaRegistry, StreamCatalog (parallel)
    [INFO]   - SchemaRegistry: 41 schemas
    [INFO] Phase 4: Tableflow
    [INFO]   - 12 topics mapped to UC tables
    [INFO] Extraction complete: 142 nodes, 218 edges
    [INFO] Graph saved to lineage_graph.json
    ```

## Per-Cluster Credentials

By default, LineageBridge uses your cloud API key for all operations. If you have cluster-scoped API keys, you can configure them per cluster.

### Credential Resolution Order

For each cluster, LineageBridge tries:

1. **Per-cluster credential** (from `CLUSTER_CREDENTIALS` map)
2. **Global Kafka credential** (`KAFKA_API_KEY` / `KAFKA_API_SECRET`)
3. **Cloud API key** (fallback)

### Option 1: Environment Variable

Set a JSON map in your `.env` file:

```bash
LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-111111":{"api_key":"AAAA","api_secret":"xxxx"},"lkc-222222":{"api_key":"BBBB","api_secret":"yyyy"}}
```

**Format:**
```json
{
  "lkc-111111": {
    "api_key": "AAAA...",
    "api_secret": "xxxx..."
  },
  "lkc-222222": {
    "api_key": "BBBB...",
    "api_secret": "yyyy..."
  }
}
```

**Example `.env` snippet:**
```bash
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=cloud-level-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=cloud-level-secret
LINEAGE_BRIDGE_CLUSTER_CREDENTIALS={"lkc-abc123":{"api_key":"cluster-key-1","api_secret":"secret-1"},"lkc-def456":{"api_key":"cluster-key-2","api_secret":"secret-2"}}
```

### Option 2: UI Entry

1. Start the UI and expand an environment in the sidebar
2. Click the gear icon next to a cluster
3. Enter the cluster-scoped API key and secret
4. Click "Save"

The credentials are encrypted and stored in `~/.lineage_bridge/cache.json` using Fernet symmetric encryption.

### Option 3: Global Kafka Credential

If all clusters use the same cluster-scoped key:

```bash
LINEAGE_BRIDGE_KAFKA_API_KEY=common-cluster-key
LINEAGE_BRIDGE_KAFKA_API_SECRET=common-cluster-secret
```

This applies to all clusters that don't have a per-cluster credential.

## Multi-Environment Use Cases

### Use Case 1: Separate Dev/Staging/Prod Environments

Extract from all three environments to see lineage across your SDLC:

```bash
$ uv run lineage-bridge-extract \
    --env env-dev \
    --env env-staging \
    --env env-prod
```

The graph will show nodes from all three environments, with edges connecting topics that share data across environments.

**Node IDs include the environment:**
```
confluent:kafka_topic:env-dev:orders
confluent:kafka_topic:env-staging:orders
confluent:kafka_topic:env-prod:orders
```

Each is a separate node. Cross-environment connectors or Flink jobs create edges between them.

### Use Case 2: Regional Deployments

You have one environment per region (US, EU, APAC):

```bash
$ uv run lineage-bridge-extract \
    --env env-us-east \
    --env env-eu-west \
    --env env-apac-southeast
```

The graph shows regional isolation or cross-region replication (via connectors or MirrorMaker).

### Use Case 3: Per-Team Environments

Extract from all team environments to understand inter-team dependencies:

```bash
$ uv run lineage-bridge-extract
```

Auto-discovery finds all environments, including:
- `env-team-payments`
- `env-team-orders`
- `env-team-notifications`

The graph reveals which teams consume data from other teams.

## Filtering Strategies

### Filter by Environment

Only extract from specific environments:

```bash
$ uv run lineage-bridge-extract --env env-abc123 --env env-def456
```

### Filter by Cluster

Only extract from specific clusters within an environment:

```bash
$ uv run lineage-bridge-extract --env env-abc123 --cluster lkc-111111
```

Useful when an environment has many clusters but you only care about one.

### Combine Filters

Filter by both environment and cluster:

```bash
$ uv run lineage-bridge-extract \
    --env env-abc123 --cluster lkc-111111 \
    --env env-def456 --cluster lkc-222222
```

This extracts only the specified clusters from the specified environments.

## Performance Considerations

### Parallel Extraction

LineageBridge extracts from clusters **sequentially** (Phase 1: KafkaAdmin) but processes **transformation layers in parallel** (Phase 2: Connect, ksqlDB, Flink).

**Extraction time scales with:**
- Number of topics (most significant)
- Number of connectors, ksqlDB queries, Flink statements
- Network latency to Confluent Cloud

**Typical timings:**
- Small environment (10 topics, 2 clusters): ~10 seconds
- Medium environment (100 topics, 5 clusters): ~30 seconds
- Large environment (1000 topics, 10 clusters): ~2 minutes

### Caching

The UI caches environment and cluster discovery results for 5 minutes. To force a refresh, restart the UI.

The CLI always performs fresh discovery.

## Troubleshooting

### "No environments found"

**Cause:** Your cloud API key lacks read access to environments.

**Solution:**
1. Verify your API key has `OrgAdmin` or `EnvironmentAdmin` role:
   ```bash
   $ confluent api-key describe <key> --resource cloud
   ```

2. Try listing environments manually:
   ```bash
   $ confluent environment list
   ```

3. If that fails, your key doesn't have the required permissions. Create a new cloud API key with broader access.

### "Cluster discovery failed for env-xyz"

**Cause:** The cloud API key can see the environment but not its clusters.

**Solution:**
1. Check cluster list access:
   ```bash
   $ confluent kafka cluster list --environment env-xyz
   ```

2. If you see clusters, the issue is with LineageBridge. Check logs for HTTP errors.

3. If you don't see clusters, your key lacks `CloudClusterAdmin` access. Grant it via the Confluent Cloud UI.

### "401 Unauthorized on cluster lkc-xyz"

**Cause:** The API key used for that cluster lacks read access to Kafka topics.

**Solution:**
1. Check which credential is being used. LineageBridge logs the credential resolution order.

2. If using per-cluster credentials, verify the key has read access:
   ```bash
   $ confluent kafka topic list --cluster lkc-xyz --api-key <key> --api-secret <secret>
   ```

3. If using the cloud API key, it may not have cluster-level read access. Create a cluster-scoped API key:
   ```bash
   $ confluent api-key create --resource lkc-xyz --description "LineageBridge read-only"
   ```

   Then add it to `CLUSTER_CREDENTIALS` in your `.env` file.

### "Extraction is slow for large environments"

**Cause:** Processing many topics sequentially.

**Solution:**
1. Use cluster filtering to extract only relevant clusters:
   ```bash
   $ uv run lineage-bridge-extract --env env-abc123 --cluster lkc-primary
   ```

2. Disable optional extractors if you don't need them:
   - In the UI: uncheck Schema Registry, Stream Catalog, or Tableflow in the sidebar
   - In the CLI: not yet supported (all extractors run by default)

3. Run extractions in parallel for different environments (separate processes):
   ```bash
   $ uv run lineage-bridge-extract --env env-abc123 > graph-abc.json &
   $ uv run lineage-bridge-extract --env env-def456 > graph-def.json &
   $ wait
   ```

   Then merge the graphs manually (no built-in merge tool yet).

## Next Steps

- [Configure per-cluster credentials properly](credential-management.md) to avoid permission errors
- [Set up Docker deployment](docker-deployment.md) to run multi-environment extractions in containers
- [Automate extraction with CI/CD](ci-cd-integration.md) to keep lineage up to date
