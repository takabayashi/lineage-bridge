# LineageBridge — Project Status

**Last updated:** 2026-04-04

## What LineageBridge Does

LineageBridge extracts stream lineage from Confluent Cloud (Kafka topics, connectors, Flink jobs, ksqlDB queries, consumer groups, schemas, Tableflow) using only public APIs, and renders it as an interactive directed graph in a Streamlit UI.

## Current State: Step 1 Complete

The Confluent Lineage Extractor + Streamlit UI is functionally complete:

### Extractors (7 clients)
| Client | What it extracts | Status |
|--------|-----------------|--------|
| **KafkaAdmin** | Topics, consumer groups, consumer→topic edges | ✅ Working (incl. Kafka protocol fallback for zero-lag groups) |
| **Connect** | Source/sink connectors, connector↔topic edges | ✅ Working (handles expanded API format) |
| **Flink** | Flink SQL statements, I/O topic edges | ✅ Working (CTAS, INSERT, windowing, SET prefix) |
| **ksqlDB** | ksqlDB queries, I/O topic edges | ✅ Working |
| **SchemaRegistry** | Schemas, topic→schema edges | ✅ Working |
| **StreamCatalog** | Tags, business metadata enrichment | ✅ Working |
| **Tableflow** | Topic→table→UC table mapping | ✅ Working |

### UI Features
- Interactive vis.js graph with drag, zoom, region select
- Sugiyama-style DAG layout (horizontal, minimized crossings)
- Node positions persist across Streamlit reruns (browser sessionStorage)
- Sidebar filters (node type, environment, cluster)
- Search by qualified name
- Click-to-inspect detail panel with attributes, neighbors, deep links
- Export as JSON
- Real-time metrics enrichment (optional)
- Multi-credential support + auto key provisioning

### Test Coverage
- **214 tests**, all passing
- **46% overall coverage**
- High coverage: clients (78-100%), models (94%)
- Low coverage: orchestrator (18%), UI (0%), config (0%)

## Architecture

```
Confluent Cloud APIs ──► 7 Clients ──► Orchestrator ──► LineageGraph ──► Streamlit UI
  (REST v3, Kafka)       (async)        (4 phases)      (networkx)       (vis.js)
```

**Extraction phases:**
1. Kafka Admin — topic inventory + consumer groups
2. Connect + ksqlDB + Flink — transformation edges (parallel)
3. Schema Registry + Stream Catalog — enrichment (parallel)
4. Tableflow — bridge to UC tables

## What's Next: Step 2 — Databricks UC Integration

Bridge the lineage graph into Databricks Unity Catalog:
- Read UC table/catalog metadata
- Connect Tableflow-mapped tables to UC counterparts
- Potentially write lineage relationships back to UC

## Known Issues / Tech Debt
- Info-level logging in kafka_admin.py consumer group fallback — can revert to debug after validation
- `_list_offsets_via_protocol` is not covered by unit tests (requires confluent-kafka mock)
- UI app.py is ~1000 lines — could benefit from splitting into sub-modules
- Graph renderer has dual layout code paths (Python Sugiyama + vis.js built-in)
