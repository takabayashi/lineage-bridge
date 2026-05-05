# LineageBridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://takabayashi.github.io/lineage-bridge/)
[![GitHub Release](https://img.shields.io/github/v/release/takabayashi/lineage-bridge)](https://github.com/takabayashi/lineage-bridge/releases)

**Extract stream lineage from Confluent Cloud, bridge it to data catalogs, and visualize it as an interactive graph.**

LineageBridge fills a gap: Confluent Cloud has rich stream processing lineage (connectors, Flink jobs, ksqlDB queries, consumer groups) but no way to export it as a queryable graph or bridge it into external data catalogs. LineageBridge extracts this lineage using only public APIs, connects it to Databricks Unity Catalog, AWS Glue, and Google Data Lineage / BigQuery via Tableflow, and renders everything in an interactive Streamlit UI.

## 🚀 Get Started in 30 Seconds

**One-line install** (no dependencies, no configuration):

```bash
curl -fsSL https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/scripts/quickstart.sh | bash
```

This automatically:
- ✅ Installs all dependencies (uv + LineageBridge)
- ✅ Launches the interactive UI
- ✅ Opens your browser to http://localhost:8501
- ✅ Loads a sample lineage graph for immediate exploration

**No Confluent account needed** — try it in demo mode first, connect later!

**Or use Docker:**

```bash
docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:latest
```

Then open http://localhost:8501 → Click **"Load Demo Graph"**

---

📚 **[Full Documentation →](https://takabayashi.github.io/lineage-bridge/)** — catalog integration, API reference, architecture deep-dive

## Architecture

```
Confluent Cloud APIs ──> Clients ──> Orchestrator ──> LineageGraph ──> Streamlit UI
  (REST v3, Kafka)       (async)      (5 phases)      (networkx)       (vis.js)
                                          │
       Databricks UC / AWS Glue / <──────┘ (catalog enrichment + lineage push)
       Google Data Lineage
```

## What It Extracts

| Client | Data Extracted | Lineage Signals |
|--------|---------------|-----------------|
| **KafkaAdmin** | Topics, consumer groups | Consumer group -> topic membership |
| **Connect** | Source & sink connectors | Connector <-> topic edges, external datasets |
| **Flink** | Flink SQL statements | Input/output topic edges from SQL parsing |
| **ksqlDB** | Persistent queries | Input/output topic edges from SQL parsing |
| **SchemaRegistry** | Avro/Protobuf/JSON schemas | Topic -> schema edges |
| **StreamCatalog** | Tags, business metadata | Enrichment on topic nodes |
| **Tableflow** | Topic -> table mappings | Topic -> Tableflow -> UC / Glue / BigQuery table |
| **Metrics** | Throughput (bytes/records) | Real-time metrics on topics and connectors |
| **Databricks UC** | Table metadata, lineage | UC table enrichment + lineage push |
| **AWS Glue** | Table metadata | Glue table enrichment + lineage push |
| **AWS DataZone** | Data products | DataZone asset enrichment + lineage push |
| **Google Data Lineage** | BigQuery datasets/tables | BigQuery table enrichment + lineage push to Google Data Lineage API |

## Features

### Onboarding & Setup
- **Welcome dialog** — First-time setup guide appears when no credentials are found
- **One-line quickstart** — `curl | bash` installer for instant demo mode
- **Three-layer credential flow** — .env → encrypted cache → session state
- **Auto-provisioning** — Optional Cloud API key creation via Confluent CLI

### Graph Visualization
- Interactive directed graph (vis.js) with drag, zoom, and Shift+drag region select
- Sugiyama-style DAG layout with minimal edge crossings
- Color-coded nodes by system (Confluent, Databricks, AWS, External)
- Click-to-inspect detail panel with attributes, neighbors, and deep links
- Search by qualified name with neighborhood filtering
- Export graph as JSON
- Streamlined sidebar (Setup/Run/Publish/Explore) with credential management dialogs
- Real-time extraction progress with persistent logs

### Data Catalog Integration
- **[Databricks Unity Catalog](https://takabayashi.github.io/lineage-bridge/catalog-integration/databricks-unity-catalog/):** Enrich UC tables with metadata, push native lineage via External Lineage API
- **[AWS Glue](https://takabayashi.github.io/lineage-bridge/catalog-integration/aws-glue/):** Enrich Glue tables with metadata, push lineage as table parameters
- **[AWS DataZone](https://takabayashi.github.io/lineage-bridge/catalog-integration/aws-datazone/):** Enrich DataZone data products, push lineage as OpenLineage events
- **[Google Data Lineage](https://takabayashi.github.io/lineage-bridge/catalog-integration/google-data-lineage/):** Enrich BigQuery tables with metadata, push lineage to Cloud Lineage API via Dataplex
- [Extensible provider pattern](https://takabayashi.github.io/lineage-bridge/catalog-integration/adding-new-catalogs/) — add new catalogs with a single file

### Operations
- Real-time metrics enrichment (throughput, consumer lag)
- Change-detection watcher with REST polling and debounced re-extraction
- Multi-credential support with encrypted cache and auto key provisioning
- Pluggable storage backends (memory, file, SQLite)
- REST + OpenLineage API for programmatic access
- Docker support with extract, UI, and watcher profiles

## Connecting to Your Confluent Cloud

After trying the demo, connect to your real Confluent environment:

**Option 1: Via Welcome Dialog** (easiest)

1. Run the quickstart or launch the UI
2. A welcome dialog appears when no credentials are found
3. Click **"Save & Connect"** and enter your Cloud API Key
4. Credentials are saved to `.env` automatically

**Option 2: Manual Setup** (for development)

If you need to customize or develop on LineageBridge:

```bash
# Clone and install
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
uv pip install -e ".[dev]"

# Get your Cloud API Key from:
# https://confluent.cloud/settings/api-keys

# Launch UI (welcome dialog will prompt for credentials)
make ui
```

The welcome dialog will appear and guide you through credential setup. Or manually create `.env`:

```env
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

**Available commands:**

```bash
make ui          # Launch Streamlit UI
make extract     # Run CLI extraction
make watch       # Start change-detection watcher
make api         # Start REST API server
make test        # Run test suite
```

See the [full documentation](https://takabayashi.github.io/lineage-bridge/getting-started/installation/) for advanced configuration.

## What's Included

LineageBridge provides multiple tools for different workflows:

| Tool | Command | Purpose |
|------|---------|---------|
| **UI** | `make ui` | Interactive graph visualization + extraction |
| **CLI** | `make extract` | Headless extraction to JSON |
| **Watcher** | `make watch` | Continuous monitoring + auto-extraction |
| **API** | `make api` | REST + OpenLineage API server |
| **Docker** | `make docker-ui` | Containerized UI deployment |

Run `make help` to see all available targets.

## Documentation

- **[Getting Started](https://takabayashi.github.io/lineage-bridge/getting-started/)** — Installation, quickstart, configuration
- **[User Guide](https://takabayashi.github.io/lineage-bridge/user-guide/)** — UI walkthrough, CLI reference, graph navigation
- **[Catalog Integration](https://takabayashi.github.io/lineage-bridge/catalog-integration/)** — Unity Catalog, AWS Glue, Google Data Lineage, DataZone
- **[API Reference](https://takabayashi.github.io/lineage-bridge/api-reference/)** — REST API + OpenLineage endpoints
- **[Demos](https://takabayashi.github.io/lineage-bridge/demos/)** — Deploy full-stack demos with Terraform
- **[Architecture](https://takabayashi.github.io/lineage-bridge/architecture/)** — Design decisions, internals, extensibility

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

```bash
make install    # Install with dev dependencies
make test       # Run test suite
make lint       # Check code style
make format     # Auto-format code
```

## Recent Releases

- **[v0.6.1](https://github.com/takabayashi/lineage-bridge/releases/tag/v0.6.1)** — One-line quickstart, welcome dialog, Docker publishing
- **[v0.6.0](https://github.com/takabayashi/lineage-bridge/releases/tag/v0.6.0)** — Complete UX redesign, in-process watcher, enhanced credential flow
- **[v0.5.0](https://github.com/takabayashi/lineage-bridge/releases/tag/v0.5.0)** — Native UC lineage, catalog protocol v2, pluggable storage, service layer
- **[v0.4.0](https://github.com/takabayashi/lineage-bridge/releases/tag/v0.4.0)** — Google Data Lineage, DataZone, OpenLineage API, Terraform demos

**Coming next:** Graph comparison, additional catalog providers, performance optimizations

## License

Licensed under the [Apache License 2.0](LICENSE).
