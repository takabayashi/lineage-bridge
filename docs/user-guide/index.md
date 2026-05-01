# User Guide

Welcome to the LineageBridge User Guide. This section covers all user-facing tools and interfaces for working with LineageBridge.

## Tools Overview

LineageBridge provides four main user-facing tools:

### CLI Tools

| Tool | Purpose | Best For |
|------|---------|----------|
| **lineage-bridge-extract** | Extract lineage from Confluent Cloud | One-time exports, automation, CI/CD |
| **lineage-bridge-watch** | Monitor for changes and auto-extract | Keeping lineage up-to-date in production |
| **lineage-bridge-ui** | Interactive Streamlit application | Visual exploration, discovery, analysis |
| **lineage-bridge-api** | REST API server | Integration with other tools, programmatic access |

### Streamlit UI

The **Streamlit UI** is the primary interactive interface for LineageBridge. It provides:

- Visual graph exploration with drag, zoom, and search
- Environment and cluster discovery
- Configurable extraction with progress tracking
- Change detection watcher controls
- Node inspection with deep links
- Export and load capabilities

See the [Streamlit UI Guide](streamlit-ui.md) for a complete walkthrough.

### Graph Visualization

The **interactive graph** is powered by vis.js and provides:

- DAG-based hierarchical layout
- Color-coded nodes by system (Confluent, Databricks, AWS, External)
- Click-to-inspect detail panels
- Multi-select with Shift+drag
- Search by qualified name
- Export as JSON

See the [Graph Visualization Guide](graph-visualization.md) for interaction details.

### Change Detection

The **watcher** monitors Confluent Cloud for lineage-relevant changes and triggers automatic re-extraction:

- REST API polling mode (default)
- Audit log Kafka consumer mode (optional)
- Configurable cooldown period
- Event history and extraction logs

See the [Change Detection Guide](change-detection.md) for setup and usage.

## Quick Start

### 1. Extract Lineage (CLI)

```bash
# Extract from one environment
lineage-bridge-extract --env env-abc123

# Extract from multiple environments with filters
lineage-bridge-extract \
  --env env-abc123 \
  --env env-def456 \
  --cluster lkc-xyz789 \
  --output my-graph.json

# Enrich an existing graph
lineage-bridge-extract --enrich-only --output my-graph.json
```

### 2. Start the UI

```bash
# Launch Streamlit UI
lineage-bridge-ui
# or
uv run streamlit run lineage_bridge/ui/app.py
```

Open http://localhost:8501, connect, select infrastructure, and extract.

### 3. Watch for Changes

```bash
# Monitor and auto-extract
lineage-bridge-watch --env env-abc123 --cooldown 30

# Push to Databricks after each extraction
lineage-bridge-watch --env env-abc123 --push-uc
```

### 4. Start the API

```bash
# Launch REST API
lineage-bridge-api
# Runs on http://localhost:8000 by default
```

## Navigation

Select a topic from the left sidebar to dive deeper:

- **[CLI Tools](cli-tools.md)** — Command reference for all four CLI commands
- **[Streamlit UI](streamlit-ui.md)** — UI walkthrough and features
- **[Graph Visualization](graph-visualization.md)** — Graph interaction and filtering
- **[Change Detection](change-detection.md)** — Watcher setup and usage

## Environment Variables

All CLI tools and the UI read configuration from environment variables with the `LINEAGE_BRIDGE_` prefix. See [Configuration](../getting-started/configuration.md) for the full reference.

Key settings for user tools:

| Variable | Purpose | Default |
|----------|---------|---------|
| `LINEAGE_BRIDGE_LOG_LEVEL` | Logging verbosity | `INFO` |
| `LINEAGE_BRIDGE_ENABLE_METRICS` | Include throughput metrics | `false` |
| `LINEAGE_BRIDGE_METRICS_LOOKBACK_HOURS` | Metrics time window | `1` |
| `LINEAGE_BRIDGE_API_HOST` | API server bind address | `0.0.0.0` |
| `LINEAGE_BRIDGE_API_PORT` | API server port | `8000` |

## Docker

All user tools are available as Docker profiles:

```bash
# UI
docker compose -f infra/docker/docker-compose.yml --profile ui up

# Extract
docker compose -f infra/docker/docker-compose.yml --profile extract up

# Watcher
docker compose -f infra/docker/docker-compose.yml --profile watch up

# API
docker compose -f infra/docker/docker-compose.yml --profile api up
```

Or use Make shortcuts:

```bash
make docker-ui
make docker-extract
make docker-watch
make docker-api
```

## Next Steps

- **First time?** Start with the [Streamlit UI Guide](streamlit-ui.md) for a visual introduction
- **Automating?** Check the [CLI Tools Reference](cli-tools.md) for all flags and options
- **Monitoring?** Set up the [Change Detection Watcher](change-detection.md)
- **Integrating?** Explore the [API Reference](../api-reference/index.md)
