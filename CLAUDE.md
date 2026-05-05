# LineageBridge

Extracts stream lineage from Confluent Cloud, bridges it to data catalogs (Databricks Unity Catalog, AWS Glue, Google BigQuery via Data Lineage, AWS DataZone), and visualises it as an interactive directed graph in Streamlit. Exposes the same data + control-plane through a FastAPI REST API.

## Build & Run

```bash
# Install (use uv)
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest --cov=lineage_bridge --cov-report=term-missing

# Lint + format
uv run ruff check .
uv run ruff format .

# Run UI
uv run streamlit run lineage_bridge/ui/app.py

# Run extraction CLI (--metrics enables Telemetry-based metrics enrichment)
uv run lineage-bridge-extract --env env-XXX [--metrics]

# Run REST API
uv run lineage-bridge-api    # uvicorn on :8000

# Run change-detection watcher daemon (writes state to the configured storage backend)
uv run lineage-bridge-watch --env env-XXX [--cooldown 30] [--push-uc]

# Docker (files in infra/docker/)
make docker-ui
make docker-extract
make docker-watch
make docker-api
```

## Architecture

```
Confluent Cloud APIs ──► Clients ──► Phases ──► LineageGraph ──► Streamlit UI
  (REST v3, Kafka)       (async)     (4-5)      (networkx)        (vis.js)
                            │           │
                            │           └─► Catalog providers (UC / Glue / BQ / DataZone)
                            │                  ├── enrich (read catalog metadata)
                            │                  └── push_lineage (write OpenLineage events)
                            │
                            └─► Storage layer (memory / file / sqlite)
                                  ├── graphs / tasks / events
                                  └── watchers (config + status + events + history)

UI ──► REST API ──► Storage          Watcher daemon ──► Storage
                                          │
                                          └── runs the same WatcherService
                                              the API spawns in-process
```

Three peer services share one storage backend: **UI** (Streamlit), **API** (FastAPI/uvicorn), **Watcher** (CLI daemon or in-process API task). Multiple UI instances see the same state.

### Module Structure

```
lineage_bridge/
  config/settings.py              # Pydantic Settings (LINEAGE_BRIDGE_ env prefix)
  config/cache.py                 # Encrypted local JSON cache (per-demo creds)
  config/provisioner.py           # Auto-provision API keys via Confluent CLI
  clients/base.py                 # ConfluentClient: async httpx, retry, pagination
  clients/protocol.py             # LineageExtractor protocol
  clients/discovery.py            # Environment + cluster discovery
  clients/kafka_admin.py          # Topics + consumer groups + lag
  clients/connect.py              # Connectors + DLQ wiring + external datasets
  clients/flink.py                # Flink SQL parsing
  clients/ksqldb.py               # ksqlDB queries
  clients/schema_registry.py      # Schema enrichment
  clients/stream_catalog.py       # Tags, business metadata
  clients/tableflow.py            # Topic → table → catalog mapping
  clients/metrics.py              # Confluent Telemetry: topic + connector + Flink metrics
  clients/audit_consumer.py       # Audit-log Kafka consumer + REST poller
  clients/databricks_discovery.py # Databricks workspace + catalog discovery
  clients/databricks_sql.py       # Databricks SQL Statement Execution API
  catalogs/protocol.py            # CatalogProvider protocol (extensible)
  catalogs/databricks_uc.py       # Databricks Unity Catalog provider
  catalogs/aws_glue.py            # AWS Glue provider (region-aware partition routing)
  catalogs/google_lineage.py      # BigQuery via Google Data Lineage API
  catalogs/google_dataplex.py     # Dataplex Catalog asset registration helper
  catalogs/aws_datazone.py        # AWS DataZone push (graceful IAM degradation)
  catalogs/upstream_chain.py      # Multi-hop upstream chain helper
  models/graph.py                 # LineageNode, LineageEdge, LineageGraph
  models/audit_event.py           # Audit-log event parsing (CloudEvent format)
  extractors/orchestrator.py      # 4-phase pipeline + CLI entry (run_extraction)
  extractors/context.py           # ExtractionContext shared across phases
  extractors/phase.py             # Phase protocol + safe_extract helper
  extractors/phases/kafka_admin.py        # Phase 1: topics + consumer groups
  extractors/phases/processing.py         # Phase 2: connect + ksqlDB + Flink (parallel)
  extractors/phases/schema_enrichment.py  # Phase 3: SR + StreamCatalog (parallel)
  extractors/phases/tableflow.py          # Phase 4: Tableflow + catalog node creation
  extractors/phases/catalog_enrichment.py # Phase 4b: provider-specific enrichment
  extractors/phases/metrics.py            # Phase 5: optional Telemetry enrichment
  services/__init__.py            # Public service-layer entry points
  services/requests.py            # ExtractionRequest, EnrichmentRequest, PushRequest
  services/extraction_service.py  # run_extraction(req, settings) — UI + API + watcher entry
  services/enrichment_service.py  # run_enrichment(req, settings, graph)
  services/push_service.py        # run_push(req, settings, graph) — dispatches to providers
  services/request_builder.py     # UI session-state → ExtractionRequest helper
  services/watcher_models.py      # WatcherConfig / Status / Summary / Event / ExtractionRecord
  services/watcher_service.py     # Pure-logic state machine (no threading, no I/O)
  services/watcher_runner.py      # Asyncio loop + repository persistence
  storage/protocol.py             # GraphRepository / TaskRepository / EventRepository / WatcherRepository
  storage/factory.py              # make_repositories(settings) — backend dispatch
  storage/migrations/             # SQLite versioned-SQL migrations + apply_pending runner
  storage/backends/memory.py      # In-memory backend (default)
  storage/backends/file.py        # JSON files + flock (per-graph durability)
  storage/backends/sqlite.py      # Single-file storage.db with WAL + per-instance connection
  api/app.py                      # FastAPI factory (create_app)
  api/main.py                     # uvicorn entry point
  api/routers/{graphs,tasks,push,watcher,lineage,datasets,jobs,meta}.py
  ui/app.py                       # Streamlit app (main entry)
  ui/sidebar/                     # Sidebar package: connection / scope / credentials / actions / filters
  ui/extraction.py                # Extraction progress panel
  ui/discovery.py                 # Environment/cluster discovery + Settings cache fallback
  ui/state.py                     # Session state management
  ui/graph_renderer.py            # vis.js data prep + Sugiyama layout
  ui/node_details.py              # Node detail panel
  ui/styles.py                    # Colors, brand icons, URL builders, badge variants
  ui/watcher.py                   # Watcher controls (polls /api/v1/watcher/* — no in-process state)
  ui/sample_data.py               # Sample graph for demo mode
  ui/empty_state.py               # Welcome / hero card on first load
  ui/static/                      # Inlined CSS + bundled sample_graph.json
  ui/components/visjs_graph/      # Custom Streamlit component (vis.js)
  watcher/cli.py                  # `lineage-bridge-watch` daemon entry (thin wrapper)
  openlineage/                    # OpenLineage event models + translator + store
```

### Extraction Phases

1. **KafkaAdminPhase**: topic inventory + consumer groups + lag (sequential per cluster)
2. **ProcessingPhase**: Connect + ksqlDB + Flink (parallel) — DLQ topics auto-wired to sink connectors via `confluent_id`
3. **SchemaEnrichmentPhase**: Schema Registry + Stream Catalog (parallel)
4. **TableflowPhase**: topic → tableflow_table → catalog_table (delegates to providers)
4b. **CatalogEnrichmentPhase**: providers backfill rows / bytes / columns / timestamps in parallel
5. **MetricsPhase** (optional, `--metrics`): Telemetry-based enrichment for topics, connectors, Flink jobs; consumer-group lag → `metrics_total_lag`; tableflow inherits from upstream topic; catalog tables get `metrics_active` from recency

### Data Model

- **Node ID format:** `{system}:{type}:{env_id}:{qualified_name}`
- **Node types:** `KAFKA_TOPIC, CONNECTOR, KSQLDB_QUERY, FLINK_JOB, TABLEFLOW_TABLE, CATALOG_TABLE, SCHEMA, EXTERNAL_DATASET, CONSUMER_GROUP`
  - `CATALOG_TABLE` carries a `catalog_type` discriminator (`UNITY_CATALOG / AWS_GLUE / GOOGLE_DATA_LINEAGE / AWS_DATAZONE`) — see ADR-021.
- **Edge types:** `PRODUCES, CONSUMES, TRANSFORMS, MATERIALIZES, HAS_SCHEMA, MEMBER_OF`
- **System types:** `CONFLUENT, DATABRICKS, AWS, GOOGLE, EXTERNAL`

### Catalog Provider Pattern

Data-catalog integrations implement `CatalogProvider` protocol in `catalogs/protocol.py`:

- `build_node()` — create a `CATALOG_TABLE` node + `MATERIALIZES` edge from a Tableflow integration
- `enrich()` — backfill metadata from the catalog's own API
- `build_url()` — deep link to the catalog's UI (region/partition aware for AWS)
- `push_lineage()` — write lineage as OpenLineage events to the catalog

Adding a new catalog = one file in `catalogs/`, register in `catalogs/__init__.py:_PROVIDERS`. See ADR-021 for the discriminator design.

### Storage Backends

Pluggable per ADR-022. Selected via `LINEAGE_BRIDGE_STORAGE__BACKEND` (note the double underscore — pydantic-settings nested-config delimiter):

- `memory` (default): process-local; lost on restart
- `file`: JSON files under `LINEAGE_BRIDGE_STORAGE__PATH` with `flock`-guarded writes
- `sqlite`: single `storage.db` under the storage root, WAL mode, durable across restarts; recommended for the watcher

### Watcher

Change-detection runs as a peer service (Phase 2G):

- **Daemon mode** (production): `lineage-bridge-watch` registers a `watcher_id` in the storage backend, runs the asyncio loop, persists status/events/history every tick. Survives UI restarts.
- **In-process mode** (development): the API process spawns a `WatcherRunner` task on `POST /api/v1/watcher`. Stop via `POST /api/v1/watcher/{id}/stop` (in-process only — see router docstring for the cross-process limitation).
- **UI**: `ui/watcher.py` polls `GET /api/v1/watcher/{id}/{status,events,history}` via `httpx`. Streamlit holds only the `watcher_id` string in `session_state`.
- **Modes** (config-driven): `audit_log` (Kafka consumer) or `rest_polling` (default).

### REST API

`POST /api/v1/tasks/extract` / `enrich`, `POST /api/v1/push/{provider}`, `POST /api/v1/watcher`, `GET /api/v1/graphs`, plus the OpenLineage-compatible `lineage`/`datasets`/`jobs` endpoints. See `docs/api-reference/`.

## Planning & Execution

- **Agent crew model:** read `docs/crew.md` for the 8 personas, their specialties, and the peer review protocol. Always use these when planning work.
- **Master plan:** read `docs/plan.md` for the phased roadmap. Update as phases complete.
- **Refactor plan:** `docs/plan-refactor.md` (Phases 0-3 of the modularity refactor). Phases 0/1A/1B/1C/1D/2E/2F/2G/3I are complete.
- **Design decisions:** read `docs/decisions.md` before proposing changes — settled decisions are logged there with rationale. New decisions get an ADR entry (status, context, alternatives, tradeoff).
- **Rule:** no commit without a Sentinel review (lint, format, tests, scope check).

## Conventions

- Python 3.11+, async/await with `httpx`
- Pydantic v2 models, `pydantic-settings` for config
- Tests: `pytest` + `pytest-asyncio`, `respx` for HTTP mocking, fixtures in `tests/fixtures/`
- `asyncio_mode = "auto"` in pytest config
- Ruff for linting and formatting (line length 100)
- All Confluent clients extend `ConfluentClient` base
- Catalog providers use their own HTTP clients (`httpx` for Databricks, `boto3` for AWS, ADC for GCP)
- Storage layer is sync (per ADR-022) — `services/*` is async at the request boundary, sync inside repositories
- Prefer Confluent CLI for ad-hoc operations over writing scripts
- New per-cluster / per-env credentials accumulate in the local cache (per-demo workflows don't wipe each other) — see `config/cache.py`
