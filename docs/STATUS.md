# LineageBridge — Project Status

**Last updated:** 2026-05-02

## What LineageBridge Does

LineageBridge extracts stream lineage from Confluent Cloud (Kafka topics, connectors, Flink jobs, ksqlDB queries, consumer groups, schemas, Tableflow) using only public APIs, bridges it into external data catalogs (Databricks Unity Catalog, AWS Glue, Google BigQuery via Data Lineage, AWS DataZone), renders it as an interactive directed graph in Streamlit, and exposes the same data + control plane through a FastAPI REST API.

## Current State: post-refactor (2026-05-02)

The modularity refactor (`docs/plan-refactor.md` Phases 0-2 + 3I + 3G) is shipped. UI / API / Watcher are three peer services sharing one storage backend.

### Extractors (10 clients)
| Client | What it extracts | Status |
|--------|-----------------|--------|
| **KafkaAdmin** | Topics, consumer groups + lag, consumer→topic edges | Working (incl. Kafka protocol fallback) |
| **Connect** | Source/sink connectors + DLQ wiring via `lcc-XXXXX` resource ID, external datasets | Working (handles expanded API format) |
| **Flink** | Flink SQL statements, I/O topic edges | Working (CTAS, INSERT, windowing, backtick-quoted identifiers) |
| **ksqlDB** | ksqlDB queries, I/O topic edges | Working |
| **SchemaRegistry** | Schemas, topic→schema edges | Working |
| **StreamCatalog** | Tags, business metadata enrichment | Working |
| **Tableflow** | Topic→table→catalog mappings | Working (UC + Glue + BQ format matching) |
| **Metrics** | Telemetry-based metrics on every node type with a real source | Working (rate-limit aware; opt-in via `--metrics`) |
| **Databricks UC** | UC table metadata, lineage discovery, lineage push | Working |
| **AWS Glue** | Glue table metadata, lineage push | Working (boto3 via `asyncio.to_thread`; partition-aware deeplinks) |

### Catalog Providers
| Provider | Build Node | Enrich | Push Lineage | Status |
|----------|-----------|--------|--------------|--------|
| **Databricks UC** | Yes | Yes (async + retry) | Yes (SQL Statement API) | Complete |
| **AWS Glue** | Yes | Yes (boto3) | Yes (table parameters) | Complete |
| **Google BigQuery** | Yes | Yes (BigQuery REST + Dataplex Catalog asset registration) | Yes (Data Lineage API — OpenLineage events) | Complete |
| **AWS DataZone** | Push-only | n/a | Yes (custom asset type + OpenLineage events; graceful IAM degradation) | Complete |

All providers share one `CATALOG_TABLE` node type with a `catalog_type` discriminator (ADR-021). Adding a new catalog = one file in `catalogs/` + one entry in `__init__.py:_PROVIDERS`.

### Service Layer (Phase 1A)
Single entry point for extraction / enrichment / push, called by the UI, the API, and the watcher with the same `ExtractionRequest` / `EnrichmentRequest` / `PushRequest` shape.

### Storage Layer (Phases 1C + 2F)
Pluggable per ADR-022. Selected via `LINEAGE_BRIDGE_STORAGE__BACKEND`:
| Backend | Graphs | Tasks | Events | Watchers | Notes |
|---------|--------|-------|--------|----------|-------|
| `memory` | ✓ | ✓ | ✓ | ✓ | Default; lost on restart |
| `file` | ✓ | ✓ | ✓ | (memory fallback) | JSON files + flock |
| `sqlite` | ✓ | ✓ | ✓ | ✓ | One `storage.db`, WAL mode, durable; recommended for the watcher |

### Watcher (Phase 2G)
Independent peer service. `WatcherService` (pure logic) + `WatcherRunner` (asyncio loop) + `api/routers/watcher.py` (6 REST endpoints). Two run modes:
- **Daemon**: `lineage-bridge-watch` — registers a `watcher_id`, persists state via storage layer, survives UI restarts.
- **In-process**: `POST /api/v1/watcher` spawns a runner task on the API event loop; useful for development.
- The UI (`ui/watcher.py`) holds only the `watcher_id` string and polls `GET /api/v1/watcher/{id}/{status,events,history}` via `httpx`.

### REST API
Routers: `/api/v1/{tasks,push,watcher,graphs,lineage,datasets,jobs,meta}`. Async task system for long extractions; OpenLineage-compatible lineage feed.

### UI Features
- Interactive vis.js graph (drag, zoom, region select)
- Per-catalog brand icons (UC / Glue / BQ / DataZone), DLQ + topic-with-schema badge variants
- Sugiyama-style DAG layout (horizontal, minimised crossings)
- Per-cluster credential cache: switching demos doesn't lose previous demos' creds
- Sidebar legend with per-variant rows (catalog brands, DLQ, topic-with-schema)
- Click-to-inspect detail panel with metrics card, attributes, neighbours, deep links
- Per-catalog "Open in Console" button (region/partition-aware for AWS)
- Export as JSON, sample graph for demo mode
- Watcher controls (polls API)

### Test Coverage
- **892 tests**, 7 skipped (live cloud integration; opt-in via `LINEAGE_BRIDGE_*_INTEGRATION=1`)
- Storage conformance suite runs against memory + file + sqlite (96 cases)
- Lint clean (ruff, line length 100)

## Architecture

```
Confluent Cloud APIs ──► Clients ──► Phases ──► LineageGraph ──► Streamlit UI
  (REST v3, Kafka)       (async)     (4-5)      (networkx)        (vis.js)
                            │           │
                            │           └─► Catalog providers (UC / Glue / BQ / DataZone)
                            │                  ├── enrich
                            │                  └── push_lineage
                            │
                            └─► Storage layer (memory / file / sqlite)
                                  ├── graphs / tasks / events
                                  └── watchers (config + status + events + history)

UI ──► REST API ──► Storage          Watcher daemon ──► Storage
                                          │
                                          └── runs the same WatcherService
                                              the API spawns in-process
```

**Extraction phases (`extractors/phases/`):**
1. `KafkaAdminPhase` — topic inventory + consumer groups + lag (sequential per cluster)
2. `ProcessingPhase` — Connect + ksqlDB + Flink (parallel); auto-wires DLQ topics to sink connectors
3. `SchemaEnrichmentPhase` — Schema Registry + Stream Catalog (parallel)
4. `TableflowPhase` — bridge to catalog tables (delegates to providers)
4b. `CatalogEnrichmentPhase` — providers backfill metadata in parallel
5. `MetricsPhase` — Telemetry enrichment (opt-in via `--metrics`)

## CLI Entry Points
- `lineage-bridge-extract --env env-XXX [--metrics] [--push-lineage]` — batch extraction to JSON, plus optional UC push
- `lineage-bridge-watch --env env-XXX [--cooldown 30] [--push-uc] [--push-glue] [--push-google] [--push-datazone]` — change-detection daemon
- `lineage-bridge-api` — uvicorn-backed REST API

## What's Next
- Phase 3H — Snowflake (Kafka Connect Sink) and Watsonx.data (Iceberg origin) catalog providers
- Cross-process watcher stop control (currently in-process only — see watcher router docstring)
- Postgres / S3 storage backends (deferred per ADR-022)
- Graph comparison (diff between extraction runs)
- Performance optimisation for large graphs (1000+ nodes)

## Resolved Tech Debt
- ~~UI app.py is ~1000 lines~~ → Decomposed in Phase 3 + Phase 2E
- ~~UI sidebar.py is 1,077 LOC~~ → Split into `sidebar/{connection,scope,credentials,actions,filters}.py` (Phase 2E)
- ~~Watcher lives in Streamlit's session state, dies on UI restart~~ → Phase 2G; daemon mode
- ~~UC_TABLE / GLUE_TABLE / GOOGLE_TABLE node-type proliferation~~ → Collapsed into `CATALOG_TABLE` + `catalog_type` discriminator (Phase 1B / ADR-021)
- ~~No persistent storage for graphs / tasks / events~~ → Pluggable storage layer (Phase 1C + 2F)
- ~~Per-cluster credentials wiped when switching .env between demos~~ → Cache deep-merge (this session)
