# Change Detection

LineageBridge includes an in-process change-detection watcher that polls Confluent Cloud for changes and automatically re-extracts lineage when topology changes are detected.

## How It Works

The watcher runs in a background thread (within the CLI or UI process) and:

1. **Polls Confluent APIs** every 10 seconds for:
   - Topics (new topics, deletions)
   - Connectors (state changes, config changes)
   - ksqlDB clusters (new queries, dropped queries)
   - Flink SQL statements (new statements, terminated statements)

2. **Detects changes** by comparing resource hashes against the last known state

3. **Debounces events** with a 30-second cooldown to batch rapid changes

4. **Triggers extraction** automatically via `services.run_extraction`

5. **Optionally pushes** to configured catalogs (UC, Glue, BigQuery, DataZone) after each extraction

## Running the Watcher

### CLI Mode

```bash
uv run lineage-bridge-watch \
  --env env-xxxxx \
  --cooldown 30 \
  --push-uc \
  --push-glue
```

**Options:**
- `--env ENV_ID` - Confluent environment ID to watch (required)
- `--cooldown SECONDS` - Debounce cooldown in seconds (default: 30)
- `--push-uc` - Push to Databricks UC after each extraction
- `--push-glue` - Push to AWS Glue after each extraction  
- `--push-google` - Push to Google Data Lineage after each extraction
- `--push-datazone` - Push to AWS DataZone after each extraction

The watcher runs until stopped with Ctrl+C.

### UI Mode

In the Streamlit UI, navigate to the **Change Watcher** tab:

1. Select environment(s) and cluster(s)
2. Configure watcher options (cooldown, auto-push settings)
3. Click **Start Watcher**
4. View live events in the Event Feed
5. Click **Stop Watcher** when done

The watcher runs in a background thread and survives page refreshes within the same session.

## Architecture

```
WatcherEngine (in-process threading)
  │
  ├─► Poll loop (every 10s)
  │     ├── GET /kafka/v3/clusters/{id}/topics
  │     ├── GET /connect/v1/environments/{env}/clusters/{id}/connectors
  │     ├── GET /ksqldb/v2/clusters (list)
  │     └── GET /sql/v1/organizations/{org}/statements (Flink)
  │
  ├─► Change detector (hash comparison)
  │     └── Triggers cooldown on change
  │
  ├─► Cooldown timer (30s default)
  │     └── Fires extraction when expired
  │
  └─► Extraction + push
        └── services.run_extraction → services.run_push (if configured)
```

## Configuration

Watcher uses the same credentials as extraction:

```bash
# Global credentials (used by watcher)
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=...
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=...

# Optional: per-cluster credentials (watcher will use these if present)
# Managed via UI "Manage Credentials" dialog
```

Catalog push credentials:

```bash
# Databricks UC
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=...
LINEAGE_BRIDGE_DATABRICKS_TOKEN=...

# AWS Glue (uses ambient AWS credentials)
LINEAGE_BRIDGE_AWS_REGION=us-east-1

# Google (uses Application Default Credentials)
LINEAGE_BRIDGE_GCP_PROJECT_ID=...
LINEAGE_BRIDGE_GCP_LOCATION=us
```

## State Management

- **CLI mode**: Watcher state lives in-process; stops when CLI exits
- **UI mode**: Watcher state lives in `st.session_state`; stops when session ends or user clicks Stop

No persistent state across restarts. Each start begins fresh polling.

## Limitations

- **Single environment**: The watcher monitors one environment at a time
- **No audit log support**: v0.5.0 removed Kafka audit log consumer mode; REST polling only
- **Session-scoped**: State does not persist across CLI/UI restarts
- **No multi-cluster filtering**: Watcher polls all clusters in the environment

## Troubleshooting

**Watcher not detecting changes:**
- Verify credentials have read access to all resource types
- Check cooldown hasn't expired yet (wait full cooldown period)
- Inspect watcher logs for API errors

**Extraction triggered too frequently:**
- Increase `--cooldown` value (e.g., 60 seconds)
- Check for flapping connector states or ksqlDB query restarts

**Push failures:**
- Verify catalog credentials are set
- Check catalog-specific troubleshooting guides
- Watcher will log push errors but continue polling
