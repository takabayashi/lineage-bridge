# CLI Tools Reference

LineageBridge provides four command-line tools for lineage extraction, monitoring, UI access, and API serving.

## Overview

| Command | Purpose | Entry Point |
|---------|---------|-------------|
| `lineage-bridge-extract` | Extract lineage and export to JSON | `lineage_bridge.extractors.orchestrator:main` |
| `lineage-bridge-watch` | Monitor for changes and auto-extract | `lineage_bridge.watcher.cli:main` |
| `lineage-bridge-ui` | Launch Streamlit UI | `lineage_bridge.ui.app:run` |
| `lineage-bridge-api` | Start REST API server | `lineage_bridge.api.main:main` |

All commands are installed as scripts via `pyproject.toml` and are available after installation.

---

## lineage-bridge-extract

Extract lineage from Confluent Cloud and export as JSON.

### Synopsis

```bash
lineage-bridge-extract --env ENV_ID [OPTIONS]
```

### Required Arguments

| Flag | Description |
|------|-------------|
| `--env ENV_ID` | Environment ID to scan. Repeatable for multiple environments. |

### Optional Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--cluster CLUSTER_ID` | string | None | Cluster ID filter. Repeatable. If omitted, scans all clusters. |
| `--output PATH` | string | `./lineage_graph.json` | Output JSON file path. |
| `--no-enrich` | flag | false | Skip catalog and metrics enrichment (extraction only). |
| `--enrich-only` | flag | false | Enrich an existing graph file (reads from `--output` path). |
| `--push-lineage` | flag | false | Push lineage metadata to Databricks UC tables after extraction. |

### Examples

#### Basic Extraction

Extract from one environment, save to default path:

```bash
lineage-bridge-extract --env env-abc123
```

Output:

```
Complete: 142 nodes, 238 edges
Output: ./lineage_graph.json
```

#### Multi-Environment Extraction

Extract from multiple environments:

```bash
lineage-bridge-extract \
  --env env-abc123 \
  --env env-def456 \
  --output multi-env-graph.json
```

#### Cluster Filter

Extract only specific clusters:

```bash
lineage-bridge-extract \
  --env env-abc123 \
  --cluster lkc-xyz789 \
  --cluster lkc-pqr456
```

#### Extract Without Enrichment

Skip catalog enrichment and metrics (faster, but less metadata):

```bash
lineage-bridge-extract --env env-abc123 --no-enrich
```

#### Enrich Existing Graph

Backfill catalog metadata and metrics on a previously extracted graph:

```bash
lineage-bridge-extract --enrich-only --output my-graph.json
```

This is useful when:

- You want to add catalog data after extraction
- Catalog credentials were unavailable during initial extraction
- You want to refresh metrics without re-extracting topology

#### Push to Databricks

Extract and push lineage to Unity Catalog:

```bash
lineage-bridge-extract \
  --env env-abc123 \
  --push-lineage
```

Requires:

- `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL`
- `LINEAGE_BRIDGE_DATABRICKS_TOKEN`
- `LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID` (or auto-discovery)

### Environment Variables

See [Configuration](../getting-started/configuration.md) for all settings. Key variables:

- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY` — Cloud API key (required)
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET` — Cloud API secret (required)
- `LINEAGE_BRIDGE_KAFKA_API_KEY` — Cluster-scoped API key (recommended)
- `LINEAGE_BRIDGE_KAFKA_API_SECRET` — Cluster-scoped API secret (recommended)
- `LINEAGE_BRIDGE_ENABLE_METRICS` — Set to `true` to include throughput metrics
- `LINEAGE_BRIDGE_LOG_LEVEL` — `DEBUG`, `INFO`, `WARNING`, `ERROR`

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Interrupted (Ctrl+C) |
| `2` | Extraction failed (exception) |

### Extraction Phases

The extractor runs a 5-phase pipeline:

1. **KafkaAdmin** — Topic inventory and consumer groups (sequential per cluster)
2. **Connect, ksqlDB, Flink** — Transformation edges (parallel)
3. **SchemaRegistry, StreamCatalog** — Schema and metadata enrichment (parallel)
4. **Tableflow** — Topic → table → catalog mapping
5. **Metrics** — Throughput enrichment (optional, if `ENABLE_METRICS=true`)

Each phase logs progress:

```
[INFO] Phase 1/4: Extracting Kafka topics & consumer groups
[INFO] Phase 2/4: Extracting connectors, ksqlDB, Flink
[INFO] Phase 3/4: Enriching with schemas & catalog metadata
[INFO] Phase 4/4: Extracting Tableflow & catalog integrations
[INFO] Done: 142 nodes, 238 edges (CONFLUENT: 120, DATABRICKS: 15, AWS: 7)
```

### Output Format

The JSON output is a serialized `LineageGraph` object with this structure:

```json
{
  "nodes": [
    {
      "id": "confluent:kafka_topic:env-abc123:orders",
      "node_type": "KAFKA_TOPIC",
      "system": "CONFLUENT",
      "qualified_name": "orders",
      "display_name": "orders",
      "environment_id": "env-abc123",
      "cluster_id": "lkc-xyz789",
      "attributes": { ... }
    }
  ],
  "edges": [
    {
      "src_id": "confluent:connector:env-abc123:postgres-source",
      "dst_id": "confluent:kafka_topic:env-abc123:orders",
      "edge_type": "PRODUCES"
    }
  ]
}
```

---

## lineage-bridge-watch

Monitor Confluent Cloud for lineage changes and trigger automatic extraction.

### Synopsis

```bash
lineage-bridge-watch --env ENV_ID [OPTIONS]
```

### Required Arguments

| Flag | Description |
|------|-------------|
| `--env ENV_ID` | Environment ID to monitor. Repeatable for multiple environments. |

### Optional Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--cluster CLUSTER_ID` | string | None | Cluster ID filter. Repeatable. |
| `--cooldown SECONDS` | float | `30.0` | Seconds to wait after last change before triggering extraction. |
| `--poll-interval SECONDS` | float | `10.0` | Seconds between REST API polls. |
| `--push-uc` | flag | false | Push lineage to Databricks UC after each extraction. |
| `--push-glue` | flag | false | Push lineage to AWS Glue after each extraction. |

### Examples

#### Basic Watcher (Polling Mode)

Monitor one environment with default settings:

```bash
lineage-bridge-watch --env env-abc123
```

Output:

```
Polling Confluent Cloud every 10s (cooldown: 30s)
Press Ctrl+C to stop
```

The watcher will:

- Poll every 10 seconds
- Detect changes (topic create/delete, connector updates, etc.)
- Wait 30 seconds after the last change
- Trigger extraction and save to `./lineage_graph.json`

#### Custom Cooldown

Increase cooldown to reduce extraction frequency:

```bash
lineage-bridge-watch --env env-abc123 --cooldown 60
```

Use longer cooldowns (60-120s) when:

- Changes are frequent but extraction is expensive
- You want to batch multiple changes into one extraction

#### Fast Polling

Poll more frequently:

```bash
lineage-bridge-watch --env env-abc123 --poll-interval 5
```

Shorter intervals (5-10s) are useful when:

- You need near-real-time lineage updates
- Changes are infrequent but must be detected quickly

#### Auto-Push to Databricks

Extract and push lineage to Unity Catalog after each change:

```bash
lineage-bridge-watch --env env-abc123 --push-uc
```

Requires Databricks credentials in environment.

#### Multi-Environment Monitoring

Watch multiple environments:

```bash
lineage-bridge-watch \
  --env env-abc123 \
  --env env-def456 \
  --cooldown 45
```

### Watcher States

| State | Description |
|-------|-------------|
| `WATCHING` | Actively polling or consuming events |
| `COOLDOWN` | Change detected, waiting for cooldown period to expire |
| `EXTRACTING` | Running extraction pipeline |
| `STOPPED` | Watcher terminated |

### Event Detection

The watcher monitors these lineage-relevant events:

- Topic creation/deletion
- Connector creation/update/deletion
- ksqlDB cluster creation/deletion
- Flink statement creation/update/deletion
- Schema registration

### Environment Variables

See [Configuration](../getting-started/configuration.md) for all extraction-related settings (credentials, enrichment, etc.).

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success (Ctrl+C) |

The watcher runs indefinitely until interrupted.

---

## lineage-bridge-ui

Launch the interactive Streamlit UI.

### Synopsis

```bash
lineage-bridge-ui
```

No command-line arguments. Configuration is via environment variables.

### What It Does

This command is a wrapper that runs:

```bash
streamlit run lineage_bridge/ui/app.py
```

### Access

After launch, the UI is available at:

```
http://localhost:8501
```

Streamlit will automatically open your browser.

### UI Features

- **Connection panel** — Connect to Confluent Cloud
- **Infrastructure selector** — Choose environments and clusters
- **Extraction controls** — Configure and run extraction
- **Graph visualization** — Interactive DAG with filters
- **Node details** — Inspect nodes with deep links
- **Watcher controls** — Start/stop change detection
- **Export/import** — Save and load graphs as JSON

See [Streamlit UI Guide](streamlit-ui.md) for detailed walkthrough.

### Environment Variables

The UI reads all LineageBridge settings from environment variables. See [Configuration](../getting-started/configuration.md).

Required for connection:

- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`

Optional for full functionality:

- Cluster-scoped API keys
- Schema Registry credentials
- Databricks/AWS/GCP credentials

### Port Configuration

To change the default port:

```bash
streamlit run lineage_bridge/ui/app.py --server.port 8502
```

Or set in `~/.streamlit/config.toml`:

```toml
[server]
port = 8502
```

---

## lineage-bridge-api

Start the REST API server for programmatic access.

### Synopsis

```bash
lineage-bridge-api
```

No command-line arguments. Configuration is via environment variables.

### What It Does

Starts a FastAPI + Uvicorn server with 25 REST endpoints for programmatic lineage access.

**Key endpoints:**

- `GET /api/v1/health` — Health check (no auth)
- `POST /api/v1/lineage/events` — Ingest OpenLineage events
- `GET /api/v1/graphs` — Manage lineage graphs
- `POST /api/v1/tasks/extract` — Trigger async extraction

See [API Reference](../api-reference/index.md) for all 25 endpoints.

### Access

!!! success "Start Here"
    After starting the server, visit **http://localhost:8000/docs** in your browser for interactive API testing!

**API Base URL**: `http://localhost:8000/api/v1/`

**Interactive Documentation**:

- **Swagger UI**: http://localhost:8000/docs (recommended)
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Spec**: http://localhost:8000/openapi.json

### Environment Variables

See [Configuration](../getting-started/configuration.md). Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LINEAGE_BRIDGE_API_HOST` | `0.0.0.0` | Bind address |
| `LINEAGE_BRIDGE_API_PORT` | `8000` | Port |
| `LINEAGE_BRIDGE_API_KEY` | None | Optional API key for authentication |

All extraction and catalog credentials also apply.

### Authentication

If `LINEAGE_BRIDGE_API_KEY` is set, all endpoints require the key in the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-key-here" http://localhost:8000/api/v1/environments
```

If unset, the API is unauthenticated (suitable for internal networks only).

### Example Usage

#### List Environments

```bash
curl http://localhost:8000/api/v1/environments
```

Response:

```json
{
  "environments": [
    {
      "id": "env-abc123",
      "name": "Production",
      "stream_governance": { "package": "ESSENTIALS" }
    }
  ]
}
```

#### Trigger Extraction

```bash
curl -X POST http://localhost:8000/api/v1/extract \
  -H "Content-Type: application/json" \
  -d '{
    "environment_ids": ["env-abc123"],
    "enable_metrics": false
  }'
```

Response:

```json
{
  "graph": { "nodes": [...], "edges": [...] },
  "node_count": 142,
  "edge_count": 238
}
```

#### Retrieve Graph

```bash
curl http://localhost:8000/api/v1/graph
```

Returns the most recently extracted graph.

See [API Reference](../api-reference/index.md) for full endpoint documentation.

---

## Common Patterns

### CI/CD Pipeline

Extract lineage in a GitHub Actions workflow:

```yaml
- name: Extract Lineage
  run: |
    lineage-bridge-extract \
      --env ${{ secrets.CONFLUENT_ENV_ID }} \
      --output lineage.json
  env:
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY: ${{ secrets.API_KEY }}
    LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET: ${{ secrets.API_SECRET }}

- name: Upload Artifact
  uses: actions/upload-artifact@v3
  with:
    name: lineage-graph
    path: lineage.json
```

### Scheduled Extraction

Run extraction daily via cron:

```bash
# crontab -e
0 2 * * * cd /path/to/lineage-bridge && \
  uv run lineage-bridge-extract --env env-abc123 \
  --output /data/lineage-$(date +\%Y\%m\%d).json
```

### Watcher as systemd Service

Create `/etc/systemd/system/lineage-bridge-watcher.service`:

```ini
[Unit]
Description=LineageBridge Watcher
After=network.target

[Service]
Type=simple
User=lineage
WorkingDirectory=/opt/lineage-bridge
EnvironmentFile=/opt/lineage-bridge/.env
ExecStart=/usr/local/bin/lineage-bridge-watch --env env-abc123 --cooldown 60
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable lineage-bridge-watcher
sudo systemctl start lineage-bridge-watcher
```

### Docker Compose

Extract via Docker:

```yaml
services:
  extract:
    image: lineage-bridge:latest
    command: lineage-bridge-extract --env env-abc123
    env_file: .env
    volumes:
      - ./output:/output
```

Run:

```bash
docker compose run extract
```

---

## Next Steps

- **Visual exploration?** See [Streamlit UI Guide](streamlit-ui.md)
- **Graph interaction?** See [Graph Visualization Guide](graph-visualization.md)
- **Auto-update?** See [Change Detection Guide](change-detection.md)
- **API integration?** See [API Reference](../api-reference/index.md)
