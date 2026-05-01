# Change Detection Guide

The LineageBridge Change Watcher monitors Confluent Cloud for lineage-relevant changes and triggers automatic re-extraction. This guide covers setup, usage, and operational patterns.

## Overview

The watcher detects changes in:

- Kafka topics (create, delete)
- Connectors (create, update, delete)
- ksqlDB clusters (create, delete)
- Flink statements (create, update, delete)
- Schemas (register, update) — audit log mode only

When changes are detected, the watcher:

1. Waits for a **cooldown period** (default 30s) after the last change
2. Triggers extraction with your configured settings
3. Saves the graph to disk
4. Optionally pushes lineage to catalogs (Databricks UC, AWS Glue)

---

## Modes

The watcher has two modes:

### 1. REST API Polling (Default)

Polls Confluent Cloud REST APIs every N seconds (default 10s) to detect changes:

- Topic list from Kafka Admin API
- Connector list from Connect API
- ksqlDB cluster list from management API
- Flink statement list from Flink API

**Pros:**

- Simple setup (only requires existing credentials)
- Works with all Confluent Cloud environments
- No additional infrastructure

**Cons:**

- Polling interval (10s) limits detection speed
- Higher API request volume

**Use when:** You don't have access to the Confluent Cloud audit log.

### 2. Audit Log Consumer

Subscribes to the Confluent Cloud audit log Kafka topic for real-time events:

- All resource create/update/delete events
- Schema registration events
- User and API key actions

**Pros:**

- Near-real-time detection (sub-second)
- Event-driven, no polling overhead
- Captures schema changes

**Cons:**

- Requires audit log cluster credentials
- Audit log must be enabled in your organization

**Use when:** You need fast detection and have audit log access.

---

## Setup

### Prerequisites

Required credentials (already configured for extraction):

- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY`
- `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`

Optional for audit log mode:

- `LINEAGE_BRIDGE_AUDIT_LOG_BOOTSTRAP_SERVERS`
- `LINEAGE_BRIDGE_AUDIT_LOG_API_KEY`
- `LINEAGE_BRIDGE_AUDIT_LOG_API_SECRET`

### Configuration

Add to `.env`:

```bash
# Required
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-secret

# Optional: Audit log mode
LINEAGE_BRIDGE_AUDIT_LOG_BOOTSTRAP_SERVERS=pkc-audit.us-east-1.aws.confluent.cloud:9092
LINEAGE_BRIDGE_AUDIT_LOG_API_KEY=your-audit-key
LINEAGE_BRIDGE_AUDIT_LOG_API_SECRET=your-audit-secret

# Optional: Catalog push
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://your-workspace.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=your-token
```

---

## Usage

### CLI

**Basic watcher (polling mode):**

```bash
lineage-bridge-watch --env env-abc123
```

Output:

```
Polling Confluent Cloud every 10s (cooldown: 30s)
Press Ctrl+C to stop
```

**Custom cooldown:**

```bash
lineage-bridge-watch --env env-abc123 --cooldown 60
```

**Fast polling:**

```bash
lineage-bridge-watch --env env-abc123 --poll-interval 5
```

**Audit log mode:**

```bash
lineage-bridge-watch \
  --env env-abc123 \
  --audit-log-bootstrap pkc-audit.us-east-1.aws.confluent.cloud:9092 \
  --audit-log-key YOUR_AUDIT_KEY \
  --audit-log-secret YOUR_AUDIT_SECRET
```

Output:

```
Consuming audit log from pkc-audit.us-east-1.aws.confluent.cloud:9092 (cooldown: 30s)
Press Ctrl+C to stop
```

**Auto-push to Databricks:**

```bash
lineage-bridge-watch --env env-abc123 --push-uc
```

**Auto-push to AWS Glue:**

```bash
lineage-bridge-watch --env env-abc123 --push-glue
```

**Multi-environment:**

```bash
lineage-bridge-watch \
  --env env-abc123 \
  --env env-def456 \
  --cooldown 45
```

### Streamlit UI

**Main Area → Change Watcher Tab**

The UI provides controls for the watcher:

#### Start Watcher

1. Connect and select infrastructure
2. Go to **Change Watcher** tab
3. Click **Start Watcher**

The watcher runs in a background thread.

#### Status Display

```
Status: WATCHING
Last poll: 2026-04-30 14:23:45
Polls: 42
Events detected: 3
```

States:

- **STOPPED** — Watcher not running
- **WATCHING** — Actively polling or consuming events
- **COOLDOWN** — Change detected, waiting for cooldown period
- **EXTRACTING** — Running extraction pipeline

#### Stop Watcher

Click **Stop Watcher** to terminate.

#### Event Log

The UI shows recent events:

```
2026-04-30 14:23:30 — Topic created: orders
2026-04-30 14:23:35 — Connector updated: postgres-source
2026-04-30 14:23:40 — Cooldown started (30s)
2026-04-30 14:24:10 — Extraction started
2026-04-30 14:24:25 — Extraction complete (142 nodes, 238 edges)
```

---

## Watcher States

The watcher transitions through these states:

```
STOPPED → WATCHING → COOLDOWN → EXTRACTING → WATCHING
```

### STOPPED

Watcher is not running.

**Transitions:**

- `start()` → WATCHING

### WATCHING

Actively polling or consuming events.

**Behavior:**

- Polls every N seconds (polling mode)
- Consumes from audit log (audit log mode)
- Detects changes and transitions to COOLDOWN

**Transitions:**

- Change detected → COOLDOWN
- `stop()` → STOPPED

### COOLDOWN

Change detected, waiting for cooldown period.

**Behavior:**

- Accumulates additional changes
- Resets cooldown timer on each new change
- Triggers extraction when cooldown expires

**Transitions:**

- Cooldown expires → EXTRACTING
- New change detected → COOLDOWN (reset timer)
- `stop()` → STOPPED

### EXTRACTING

Running the extraction pipeline.

**Behavior:**

- Runs full extraction with configured settings
- Saves graph to disk
- Optionally pushes to catalogs

**Transitions:**

- Extraction complete → WATCHING
- Extraction failed → WATCHING (logs error)
- `stop()` → STOPPED

---

## Event Detection

### Polling Mode

Detects changes by comparing state snapshots:

| Resource | Detection Method |
|----------|-----------------|
| **Topics** | Compare topic list from Kafka Admin API |
| **Connectors** | Compare connector list and configs from Connect API |
| **ksqlDB Clusters** | Compare cluster list from management API |
| **Flink Statements** | Compare statement list from Flink API |

**Hash-based diffing:** Each resource type is hashed. Change detected when hash changes.

**Limitations:**

- Polling interval (10s default) limits detection speed
- Cannot detect schema changes
- Cannot detect fine-grained config changes (only top-level resources)

### Audit Log Mode

Subscribes to the audit log topic and filters for these event types:

| Resource | Event Type |
|----------|-----------|
| **Topics** | `io.confluent.kafka.topic.create`, `io.confluent.kafka.topic.delete` |
| **Connectors** | `io.confluent.kafka.connector.create`, `io.confluent.kafka.connector.update`, `io.confluent.kafka.connector.delete` |
| **ksqlDB** | `io.confluent.ksql.cluster.create`, `io.confluent.ksql.cluster.delete` |
| **Flink** | `io.confluent.flink.statement.create`, `io.confluent.flink.statement.update`, `io.confluent.flink.statement.delete` |
| **Schemas** | `io.confluent.schema.register`, `io.confluent.schema.update` |

**Event-driven:** No polling overhead, near-real-time detection.

**Limitations:**

- Requires audit log cluster credentials
- Audit log must be enabled in your organization

---

## Cooldown Behavior

The cooldown period prevents excessive extractions when changes are frequent.

### How It Works

1. **Change detected:** Timer starts (e.g., 30s)
2. **Additional changes within cooldown:** Timer resets to 30s
3. **No changes for 30s:** Extraction triggers

### Example Timeline

```
00:00 — Topic created → cooldown starts (30s)
00:05 — Connector updated → cooldown resets (30s from now)
00:10 — Schema registered → cooldown resets (30s from now)
00:40 — No changes for 30s → extraction triggered
```

**Result:** One extraction for three changes.

### Cooldown Settings

| Scenario | Recommended Cooldown |
|----------|---------------------|
| **Frequent changes** | 60-120s (batch multiple changes) |
| **Infrequent changes** | 30s (default, fast response) |
| **Development** | 15-30s (quick feedback) |
| **Production** | 45-60s (balance freshness and load) |

Set via `--cooldown` flag:

```bash
lineage-bridge-watch --env env-abc123 --cooldown 60
```

---

## Extraction Settings

The watcher uses the same extraction settings as the CLI:

### Enable/Disable Extractors

The watcher always runs with:

```python
enable_connect = True
enable_ksqldb = True
enable_flink = True
enable_schema_registry = True
enable_stream_catalog = False  # Off by default (slower)
enable_tableflow = True
enable_enrichment = True
```

To customize, modify `lineage_bridge/watcher/cli.py`:

```python
extraction_params = {
    "enable_connect": True,
    "enable_ksqldb": True,
    "enable_flink": True,
    "enable_schema_registry": True,
    "enable_stream_catalog": True,  # Enable for tags/metadata
    "enable_tableflow": True,
    "enable_enrichment": True,
}
```

### Metrics Enrichment

Metrics are disabled by default (slower). Enable via environment variable:

```bash
LINEAGE_BRIDGE_ENABLE_METRICS=true lineage-bridge-watch --env env-abc123
```

### Catalog Push

Enable via flags:

```bash
# Push to Databricks UC after each extraction
lineage-bridge-watch --env env-abc123 --push-uc

# Push to AWS Glue after each extraction
lineage-bridge-watch --env env-abc123 --push-glue
```

Requires catalog credentials in environment.

---

## Output

The watcher saves graphs to:

```
./lineage_graph.json
```

Each extraction overwrites the file. For versioned storage, use a systemd service or cron with timestamped filenames.

### Timestamped Output (Custom Script)

Create a wrapper script:

```bash
#!/bin/bash
# watcher-with-versioning.sh

export OUTPUT_DIR=/data/lineage

while true; do
  lineage-bridge-watch --env env-abc123 --cooldown 30 &
  WATCHER_PID=$!
  
  # Wait for extraction (check for new file)
  inotifywait -e close_write lineage_graph.json
  
  # Copy to versioned file
  cp lineage_graph.json "$OUTPUT_DIR/lineage-$(date +%Y%m%d-%H%M%S).json"
done
```

---

## Operational Patterns

### Run as systemd Service

Create `/etc/systemd/system/lineage-bridge-watcher.service`:

```ini
[Unit]
Description=LineageBridge Change Watcher
After=network.target

[Service]
Type=simple
User=lineage
WorkingDirectory=/opt/lineage-bridge
EnvironmentFile=/opt/lineage-bridge/.env
ExecStart=/usr/local/bin/lineage-bridge-watch \
  --env env-abc123 \
  --cooldown 60 \
  --push-uc
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable lineage-bridge-watcher
sudo systemctl start lineage-bridge-watcher
sudo systemctl status lineage-bridge-watcher
```

View logs:

```bash
sudo journalctl -u lineage-bridge-watcher -f
```

### Run as Docker Container

Create `docker-compose.yml`:

```yaml
services:
  watcher:
    image: lineage-bridge:latest
    command: >
      lineage-bridge-watch
      --env env-abc123
      --cooldown 60
      --push-uc
    env_file: .env
    volumes:
      - ./output:/output
    restart: unless-stopped
```

Run:

```bash
docker compose up -d watcher
docker compose logs -f watcher
```

### Run in Kubernetes

Create deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lineage-bridge-watcher
spec:
  replicas: 1
  selector:
    matchLabels:
      app: lineage-bridge-watcher
  template:
    metadata:
      labels:
        app: lineage-bridge-watcher
    spec:
      containers:
      - name: watcher
        image: lineage-bridge:latest
        command:
        - lineage-bridge-watch
        - --env
        - env-abc123
        - --cooldown
        - "60"
        - --push-uc
        envFrom:
        - secretRef:
            name: lineage-bridge-creds
        volumeMounts:
        - name: output
          mountPath: /output
      volumes:
      - name: output
        persistentVolumeClaim:
          claimName: lineage-output
```

Apply:

```bash
kubectl apply -f watcher-deployment.yaml
kubectl logs -f deployment/lineage-bridge-watcher
```

### Cron Alternative (Polling Without Watcher)

If you don't need real-time updates, use cron:

```bash
# Run extraction every hour
0 * * * * cd /opt/lineage-bridge && \
  uv run lineage-bridge-extract --env env-abc123 \
  --output /data/lineage-$(date +\%Y\%m\%d-\%H).json
```

**Pros:** Simple, predictable schedule

**Cons:** Fixed interval, not event-driven

---

## Monitoring

### Watcher Health Check

Check if the watcher is running:

```bash
# systemd
systemctl is-active lineage-bridge-watcher

# Docker
docker compose ps watcher

# Kubernetes
kubectl get pods -l app=lineage-bridge-watcher
```

### Extraction Success Rate

Check logs for extraction failures:

```bash
# systemd
journalctl -u lineage-bridge-watcher | grep "Extraction failed"

# Docker
docker compose logs watcher | grep "Extraction failed"
```

### Event Volume

Track event count in the UI or logs:

```
Events detected: 42
Extractions: 12
```

**High event count + low extraction count** = Good (cooldown working)

**High event count + high extraction count** = Increase cooldown

---

## Troubleshooting

### Watcher exits immediately

**Cause:** Missing required environment variables

**Solution:** Check `.env` for:

```bash
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET
```

### No changes detected (polling mode)

**Check:**

1. Are you making changes in Confluent Cloud?
2. Is the poll interval too long? (Try `--poll-interval 5`)
3. Check logs for API errors

### Audit log mode not activating

**Cause:** Missing audit log credentials

**Solution:** Set all three:

```bash
LINEAGE_BRIDGE_AUDIT_LOG_BOOTSTRAP_SERVERS
LINEAGE_BRIDGE_AUDIT_LOG_API_KEY
LINEAGE_BRIDGE_AUDIT_LOG_API_SECRET
```

Or pass via CLI flags.

### Extraction triggered too frequently

**Cause:** Cooldown too short for change frequency

**Solution:** Increase cooldown:

```bash
lineage-bridge-watch --env env-abc123 --cooldown 120
```

### Extraction fails in watcher but works in CLI

**Cause:** Watcher runs with different settings

**Solution:** Check extraction params in `watcher/cli.py` and ensure credentials are available.

### Memory usage grows over time

**Cause:** Event feed accumulates in memory (200 event max)

**Behavior:** Normal, event feed is bounded to 200 events (auto-eviction).

If memory grows unbounded:

- Check for leaked graph references
- Restart the watcher periodically (via systemd `RestartSec`)

---

## Best Practices

### Cooldown Tuning

- **Development:** 15-30s (fast feedback)
- **Staging:** 30-60s (balance)
- **Production:** 60-120s (reduce load)

### Poll Interval Tuning

- **Default:** 10s (good for most use cases)
- **Fast detection:** 5s (higher API load)
- **Low load:** 30-60s (delayed detection)

### Catalog Push

Enable `--push-uc` or `--push-glue` only if:

- You need lineage in the catalog immediately
- Extraction frequency is low (< 10/hour)

For high-frequency changes, run a separate cron job for catalog push.

### Event Retention

The watcher keeps 200 events in memory. For long-term event storage:

- Log to a file with structured logging
- Use audit log Kafka consumer to store events in a database

### High Availability

Run multiple watcher instances only if:

- You use audit log mode (Kafka consumer group handles deduplication)
- Each instance watches a different environment

For polling mode, **do not run multiple instances** (duplicate extractions).

---

## Next Steps

- **Automate extraction:** See [CLI Tools Reference](cli-tools.md)
- **Learn UI features:** See [Streamlit UI Guide](streamlit-ui.md)
- **Explore the graph:** See [Graph Visualization Guide](graph-visualization.md)
- **Push lineage to catalogs:** See [Catalog Integration Guides](../catalog-integration/index.md)
