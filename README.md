# LineageBridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://takabayashi.github.io/lineage-bridge/)
[![GitHub Release](https://img.shields.io/github/v/release/takabayashi/lineage-bridge)](https://github.com/takabayashi/lineage-bridge/releases)

**Extract stream lineage from Confluent Cloud, bridge it to data catalogs, and visualize it as an interactive graph.**

LineageBridge fills a gap: Confluent Cloud has rich stream processing lineage (connectors, Flink jobs, ksqlDB queries, consumer groups) but no way to export it as a queryable graph or bridge it into external data catalogs. LineageBridge extracts this lineage using only public APIs, connects it to Databricks Unity Catalog and AWS Glue via Tableflow, and renders everything in an interactive Streamlit UI.

## Architecture

```
Confluent Cloud APIs ──> Clients ──> Orchestrator ──> LineageGraph ──> Streamlit UI
  (REST v3, Kafka)       (async)      (5 phases)      (networkx)       (vis.js)
                                          │
              Databricks UC / AWS Glue <──┘ (catalog enrichment + lineage push)
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
| **Tableflow** | Topic -> table mappings | Topic -> Tableflow -> UC/Glue table |
| **Metrics** | Throughput (bytes/records) | Real-time metrics on topics and connectors |
| **Databricks UC** | Table metadata, lineage | UC table enrichment + lineage push |
| **AWS Glue** | Table metadata | Glue table enrichment + lineage push |

## Features

### Graph Visualization
- Interactive directed graph (vis.js) with drag, zoom, and Shift+drag region select
- Sugiyama-style DAG layout with minimal edge crossings
- Color-coded nodes by system (Confluent, Databricks, AWS, External)
- Click-to-inspect detail panel with attributes, neighbors, and deep links
- Search by qualified name
- Export graph as JSON

### Data Catalog Integration
- **Databricks Unity Catalog:** Enrich UC tables with metadata, push lineage as table properties and comments
- **AWS Glue:** Enrich Glue tables with metadata, push lineage as table parameters
- Extensible provider pattern — add new catalogs with a single file

### Operations
- Real-time metrics enrichment (throughput, consumer lag)
- Change-detection watcher with REST polling and debounced re-extraction
- Multi-credential support with auto key provisioning
- Docker support with extract, UI, and watcher profiles

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A Confluent Cloud account with at least one environment and Kafka cluster

### Install

```bash
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
uv pip install -e .
```

### Configure

```bash
cp .env.example .env
```

Edit `.env` with your Confluent Cloud API credentials:

```env
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret
```

Only a cloud-level API key is required to start. The UI will guide you through adding cluster-scoped credentials if needed, or can auto-provision them via the Confluent CLI.

Optional catalog credentials:

```env
LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL=https://your-workspace.databricks.com
LINEAGE_BRIDGE_DATABRICKS_TOKEN=your-databricks-token
LINEAGE_BRIDGE_AWS_REGION=us-east-1
```

### Run

```bash
# Streamlit UI
uv run streamlit run lineage_bridge/ui/app.py

# CLI extraction
uv run lineage-bridge-extract

# Change-detection watcher
uv run lineage-bridge-watch
```

Open http://localhost:8501, select an environment and cluster, and click **Extract Lineage**.

### Docker

```bash
# Run the UI
docker compose -f infra/docker/docker-compose.yml --profile ui up

# Run extraction
docker compose -f infra/docker/docker-compose.yml --profile extract up

# Run the watcher
docker compose -f infra/docker/docker-compose.yml --profile watch up

# Or use Make shortcuts
make docker-ui
make docker-extract
make docker-watch
```

## Make Targets

All common operations are available via `make`:

| Target | Description |
|--------|-------------|
| `make install` | Install project with dev dependencies |
| `make ui` | Start the Streamlit UI |
| `make extract` | Run lineage extraction CLI |
| `make watch` | Run change-detection watcher CLI |
| `make test` | Run tests |
| `make lint` | Run linter |
| `make format` | Format code and auto-fix lint issues |
| `make clean` | Remove build artifacts and caches |
| `make docker-build` | Build Docker images |
| `make docker-ui` | Start UI via Docker |
| `make docker-extract` | Run extraction via Docker |
| `make docker-watch` | Run change-detection watcher via Docker |
| `make docker-down` | Stop all Docker services |
| `make demo-up` | Provision demo infrastructure (Confluent + AWS + Databricks) |
| `make demo-down` | Tear down demo infrastructure |

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and code style guidelines.

```bash
# Install dev dependencies
make install

# Run tests
make test

# Lint & format
make lint
make format
```

## Roadmap

- [x] **Phase 1:** Confluent Lineage Extractor + Streamlit UI
- [x] **Phase 2:** Catalog provider framework (Databricks UC + AWS Glue)
- [x] **Phase 3:** UI decomposition + UX improvements
- [x] **Phase 4:** Polish, hardening, v0.2.0 release
- [x] **Post-v0.2.0:** UC integration fixes, Databricks lineage push, change-detection watcher
- [ ] **Next:** Glue enrichment, additional catalog providers, graph comparison

## License

Licensed under the [Apache License 2.0](LICENSE).
