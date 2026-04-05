# LineageBridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Extract stream lineage from Confluent Cloud and visualize it as an interactive graph.**

LineageBridge fills a gap: Confluent Tableflow maps Kafka topics to Delta tables in Unity Catalog, but does not export Confluent stream lineage (connectors, Flink jobs, ksqlDB queries, consumer groups) into a queryable graph. LineageBridge extracts this lineage using only public Confluent Cloud APIs and renders it in an interactive Streamlit UI.

## Architecture

```
Confluent Cloud APIs ──> 7 Clients ──> Orchestrator ──> LineageGraph ──> Streamlit UI
  (REST v3, Kafka)       (async)        (4 phases)      (networkx)       (vis.js)
```

## What It Extracts

| Client | Data Extracted | Lineage Signals |
|--------|---------------|-----------------|
| **KafkaAdmin** | Topics, consumer groups | Consumer group -> topic membership |
| **Connect** | Source & sink connectors | Connector <-> topic edges |
| **Flink** | Flink SQL statements | Input/output topic edges from SQL |
| **ksqlDB** | Persistent queries | Input/output topic edges from SQL |
| **SchemaRegistry** | Avro/Protobuf/JSON schemas | Topic -> schema edges |
| **StreamCatalog** | Tags, business metadata | Enrichment on topic nodes |
| **Tableflow** | Topic -> table mappings | Topic -> Tableflow -> UC table |

## UI Features

- Interactive directed graph (vis.js) with drag, zoom, and region select
- Sugiyama-style DAG layout with minimal edge crossings
- Sidebar filters by node type, environment, and cluster
- Search by qualified name
- Click-to-inspect detail panel with attributes, neighbors, and deep links to Confluent Cloud
- Export graph as JSON
- Real-time metrics enrichment (optional)
- Multi-credential support with auto key provisioning

## Quick Start

### Prerequisites

- Python 3.11+
- A Confluent Cloud account with at least one environment and Kafka cluster

### Install

```bash
git clone https://github.com/takabayashi/lineage-bridge.git
cd lineage-bridge
pip install -e .
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

### Run

```bash
streamlit run lineage_bridge/ui/app.py
```

Open http://localhost:8501, select an environment and cluster, and click **Extract Lineage**.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and code style guidelines.

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check .
```

## Roadmap

- [x] **Step 1:** Confluent Lineage Extractor + Streamlit UI
- [ ] **Step 2:** Databricks Unity Catalog integration
- [ ] **Step 3:** Delta export / BYOL external lineage bridge

## License

Licensed under the [Apache License 2.0](LICENSE).
