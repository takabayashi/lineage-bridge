# LineageBridge

Extracts stream lineage from Confluent Cloud, bridges it to data catalogs (Databricks UC, AWS Glue), and visualizes it as an interactive directed graph in Streamlit.

## Build & Run

```bash
# Install (use uv)
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest --cov=lineage_bridge --cov-report=term-missing

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Run UI
uv run streamlit run lineage_bridge/ui/app.py

# Run extraction CLI
uv run lineage-bridge-extract

# Run change-detection watcher
uv run lineage-bridge-watch

# Docker
docker compose --profile ui up
docker compose --profile extract up
docker compose --profile watch up
```

## Architecture

```
Confluent Cloud APIs --> Clients --> Orchestrator --> LineageGraph --> Streamlit UI
  (REST v3, Kafka)       (async)      (5 phases)      (networkx)       (vis.js)
                                          │
              Databricks UC / AWS Glue <──┘ (catalog enrichment + lineage push)
```

### Module Structure

```
lineage_bridge/
  config/settings.py              # Pydantic Settings (LINEAGE_BRIDGE_ env prefix)
  config/cache.py                 # Encrypted local JSON cache
  config/provisioner.py           # Auto-provision API keys via Confluent CLI
  clients/base.py                 # ConfluentClient: async httpx, retry, pagination
  clients/protocol.py             # LineageExtractor protocol
  clients/discovery.py            # Environment + cluster discovery
  clients/kafka_admin.py          # Topics + consumer groups
  clients/connect.py              # Connectors + external datasets
  clients/flink.py                # Flink SQL parsing
  clients/ksqldb.py               # ksqlDB queries
  clients/schema_registry.py      # Schema enrichment
  clients/stream_catalog.py       # Tags, business metadata
  clients/tableflow.py            # Topic -> table -> catalog mapping
  clients/metrics.py              # Confluent Metrics API
  clients/audit_consumer.py       # Audit log Kafka consumer (retained for future use)
  clients/databricks_discovery.py # Databricks workspace + catalog discovery
  clients/databricks_sql.py       # Databricks SQL Statement Execution API
  catalogs/protocol.py            # CatalogProvider protocol (extensible)
  catalogs/databricks_uc.py       # Databricks Unity Catalog provider
  catalogs/aws_glue.py            # AWS Glue provider
  models/graph.py                 # LineageNode, LineageEdge, LineageGraph
  models/audit_event.py           # Audit log event parsing (CloudEvent format)
  extractors/orchestrator.py      # 5-phase extraction pipeline
  ui/app.py                       # Streamlit app (main entry)
  ui/sidebar.py                   # Sidebar: connection, filters, extractors
  ui/extraction.py                # Extraction progress panel
  ui/discovery.py                 # Environment/cluster discovery UI
  ui/state.py                     # Session state management
  ui/graph_renderer.py            # vis.js data prep + Sugiyama layout
  ui/node_details.py              # Node detail panel
  ui/styles.py                    # Colors, icons, URL builders
  ui/watcher.py                   # Watcher controls in UI
  ui/sample_data.py               # Sample graph for demo mode
  ui/components/visjs_graph/      # Custom Streamlit component (vis.js)
  watcher/engine.py               # Change-detection poller + debounced extraction
  watcher/cli.py                  # Watcher CLI entry point
```

### Extraction Phases

1. KafkaAdmin: topic inventory + consumer groups (sequential per cluster)
2. Connect + ksqlDB + Flink: transformation edges (parallel)
3. SchemaRegistry + StreamCatalog: enrichment (parallel)
4. Tableflow: topic -> table -> catalog mapping (delegates to providers)
4b. Catalog enrichment: providers enrich their own nodes (parallel)
5. Metrics: throughput enrichment (optional, parallel per cluster)

### Data Model

- **Node ID format:** `{system}:{type}:{env_id}:{qualified_name}`
- **Node types:** KAFKA_TOPIC, CONNECTOR, KSQLDB_QUERY, FLINK_JOB, TABLEFLOW_TABLE, UC_TABLE, GLUE_TABLE, SCHEMA, EXTERNAL_DATASET, CONSUMER_GROUP
- **Edge types:** PRODUCES, CONSUMES, TRANSFORMS, MATERIALIZES, HAS_SCHEMA, MEMBER_OF
- **System types:** CONFLUENT, DATABRICKS, AWS, EXTERNAL

### Catalog Provider Pattern

Data catalog integrations implement `CatalogProvider` protocol in `catalogs/protocol.py`:
- `build_node()` — create catalog node + MATERIALIZES edge from Tableflow integration
- `enrich()` — backfill metadata from the catalog's own API
- `build_url()` — deep link to the catalog's UI
- `push_lineage()` — write lineage metadata back to the catalog

Adding a new catalog = one file in `catalogs/`, register in `catalogs/__init__.py`.

### Watcher

Change-detection via REST polling (10s interval) + debounced re-extraction (30s cooldown):
- Polls topics, connectors, ksqlDB clusters, Flink statements
- Compares hashes to detect changes
- Runs as background thread (compatible with Streamlit's sync context)
- CLI: `lineage-bridge-watch`, UI: watcher toggle in sidebar

## Planning & Execution

- **Agent crew model:** Read `docs/crew.md` for the 8 personas, their specialties, and the peer review protocol. Always use these when planning work.
- **Master plan:** Read `docs/plan.md` for the phased roadmap, current status, and workstream assignments. Update it as phases are completed.
- **Design decisions:** Read `docs/decisions.md` before proposing changes — settled decisions are logged there with rationale. When making a new design or implementation decision, add an ADR entry with status, context, alternatives, and tradeoff.
- **Rule:** No commit without Sentinel review (lint, format, tests, scope check).

## Conventions

- Python 3.11+, async/await with httpx
- Pydantic v2 models, pydantic-settings for config
- Tests: pytest + pytest-asyncio, respx for HTTP mocking, fixtures in `tests/fixtures/`
- `asyncio_mode = "auto"` in pytest config
- Ruff for linting and formatting (line length 100)
- All Confluent clients extend ConfluentClient base
- Catalog providers use their own HTTP clients (httpx for Databricks, boto3 for AWS)
- Prefer Confluent CLI for ad-hoc operations over writing scripts
