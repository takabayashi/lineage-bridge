# LineageBridge — Project Status

**Last updated:** 2026-04-07

## What LineageBridge Does

LineageBridge extracts stream lineage from Confluent Cloud (Kafka topics, connectors, Flink jobs, ksqlDB queries, consumer groups, schemas, Tableflow) using only public APIs, bridges it into external data catalogs (Databricks Unity Catalog, AWS Glue), and renders it as an interactive directed graph in a Streamlit UI.

## Current State: v0.2.0+ (Post-Release)

All core features are implemented. Catalog integration, UI decomposition, watcher, and lineage push are complete.

### Extractors (10 clients)
| Client | What it extracts | Status |
|--------|-----------------|--------|
| **KafkaAdmin** | Topics, consumer groups, consumer→topic edges | Working (incl. Kafka protocol fallback) |
| **Connect** | Source/sink connectors, connector↔topic edges, external datasets | Working (handles expanded API format) |
| **Flink** | Flink SQL statements, I/O topic edges | Working (CTAS, INSERT, windowing, backtick-quoted identifiers) |
| **ksqlDB** | ksqlDB queries, I/O topic edges | Working |
| **SchemaRegistry** | Schemas, topic→schema edges | Working |
| **StreamCatalog** | Tags, business metadata enrichment | Working |
| **Tableflow** | Topic→table→catalog mappings | Working (Unity + Glue format matching) |
| **Metrics** | Throughput bytes/records per topic and connector | Working (rate-limit aware) |
| **Databricks UC** | UC table metadata, lineage discovery, lineage push | Working (flat config, dot-to-underscore) |
| **AWS Glue** | Glue table metadata, lineage push | Working (boto3 via asyncio.to_thread) |

### Catalog Providers
| Provider | Build Node | Enrich | Push Lineage | Status |
|----------|-----------|--------|--------------|--------|
| **Databricks UC** | Yes | Yes (async + retry) | Yes (SQL Statement API) | Complete (99% coverage) |
| **AWS Glue** | Yes | Yes (boto3) | Yes (table parameters) | Complete |

### UI Features
- Interactive vis.js graph with drag, zoom, Shift+drag region select
- Sugiyama-style DAG layout (horizontal, minimized crossings)
- Node positions persist across reruns (browser sessionStorage)
- Sidebar filters (node type, environment, cluster)
- Search by qualified name
- Click-to-inspect detail panel with attributes, neighbors, deep links
- Export as JSON
- Real-time metrics enrichment (optional)
- Multi-credential support + auto key provisioning
- Change-detection watcher with debounced re-extraction
- Dark mode support
- Decomposed into modules: app, sidebar, extraction, discovery, state, watcher

### Test Coverage
- **492 tests**, all passing (11 skipped when boto3 not installed)
- **70% overall coverage**
- High coverage: clients (78-100%), models (94%), databricks_uc (99%)
- Medium coverage: orchestrator, graph_renderer
- Low coverage: UI components (Streamlit is hard to test)

## Architecture

```
Confluent Cloud APIs ──► Clients ──► Orchestrator ──► LineageGraph ──► Streamlit UI
  (REST v3, Kafka)       (async)      (5 phases)      (networkx)       (vis.js)
                                          │
              Databricks UC / AWS Glue <──┘ (catalog enrichment + lineage push)
```

**Extraction phases:**
1. Kafka Admin — topic inventory + consumer groups (sequential per cluster)
2. Connect + ksqlDB + Flink — transformation edges (parallel)
3. Schema Registry + Stream Catalog — enrichment (parallel)
4. Tableflow — bridge to catalog tables (delegates to providers)
4b. Catalog enrichment — providers enrich their nodes (parallel)
5. Metrics — throughput enrichment (optional, parallel per cluster)

## CLI Entry Points
- `lineage-bridge-extract` — batch extraction to JSON
- `lineage-bridge-watch` — change-detection watcher with debounced re-extraction
- `lineage-bridge-ui` — Streamlit UI

## What's Next

- Additional catalog providers (Snowflake, BigQuery)
- Graph comparison (diff between extraction runs)
- Node grouping by cluster/environment
- Performance optimization for large graphs (1000+ nodes)
- OpenLineage integration (if Databricks adds write API)

## Resolved Tech Debt
- ~~UI app.py is ~1000 lines~~ → Decomposed into sidebar, extraction, discovery, state, watcher modules
- ~~Protocol fallback not unit-tested~~ → 100% coverage on Kafka protocol fallback
- ~~Graph renderer has dual layout paths~~ → Both retained intentionally (Python Sugiyama for positioning, vis.js for rendering)
- ~~Info-level logging in kafka_admin.py~~ → Validated and kept for observability
