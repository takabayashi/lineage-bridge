# LineageBridge

Extracts stream lineage from Confluent Cloud and visualizes it as an interactive directed graph in Streamlit.

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
```

## Architecture

```
Confluent Cloud APIs --> 7 Clients --> Orchestrator --> LineageGraph --> Streamlit UI
  (REST v3, Kafka)       (async)       (4 phases)      (networkx)       (vis.js)
```

### Module Structure

```
lineage_bridge/
  config/settings.py         # Pydantic Settings (LINEAGE_BRIDGE_ env prefix)
  config/cache.py            # Encrypted local JSON cache
  config/provisioner.py      # Auto-provision API keys via Confluent CLI
  clients/base.py            # ConfluentClient: async httpx, retry, pagination
  clients/protocol.py        # LineageExtractor protocol
  clients/kafka_admin.py     # Topics + consumer groups
  clients/connect.py         # Connectors
  clients/flink.py           # Flink SQL parsing
  clients/ksqldb.py          # ksqlDB queries
  clients/schema_registry.py # Schema enrichment
  clients/stream_catalog.py  # Tags, business metadata
  clients/tableflow.py       # Topic -> table -> catalog mapping
  clients/metrics.py         # Confluent Metrics API
  catalogs/protocol.py       # CatalogProvider protocol (extensible)
  catalogs/databricks_uc.py  # Databricks Unity Catalog provider
  catalogs/aws_glue.py       # AWS Glue provider
  models/graph.py            # LineageNode, LineageEdge, LineageGraph
  extractors/orchestrator.py # 4-phase extraction pipeline
  ui/app.py                  # Streamlit app
  ui/graph_renderer.py       # vis.js data prep + Sugiyama layout
  ui/node_details.py         # Node detail panel
  ui/styles.py               # Colors, icons, URL builders
```

### Extraction Phases

1. KafkaAdmin: topic inventory + consumer groups (sequential per cluster)
2. Connect + ksqlDB + Flink: transformation edges (parallel)
3. SchemaRegistry + StreamCatalog: enrichment (parallel)
4. Tableflow: topic -> table -> catalog mapping
4b. Catalog enrichment: providers enrich their own nodes (parallel)

### Data Model

- **Node ID format:** `{system}:{type}:{env_id}:{qualified_name}`
- **Node types:** KAFKA_TOPIC, CONNECTOR, KSQLDB_QUERY, FLINK_JOB, TABLEFLOW_TABLE, UC_TABLE, GLUE_TABLE, SCHEMA, CONSUMER_GROUP
- **Edge types:** PRODUCES, CONSUMES, TRANSFORMS, MATERIALIZES, HAS_SCHEMA, MEMBER_OF
- **System types:** CONFLUENT, DATABRICKS, AWS, EXTERNAL

### Catalog Provider Pattern

Data catalog integrations implement `CatalogProvider` protocol in `catalogs/protocol.py`:
- `build_node()` — create catalog node + MATERIALIZES edge from Tableflow integration
- `enrich()` — backfill metadata from the catalog's own API
- `build_url()` — deep link to the catalog's UI

Adding a new catalog = one file in `catalogs/`, register in `catalogs/__init__.py`.

## Planning & Execution

- **Agent crew model:** Read `docs/crew.md` for the 8 personas, their specialties, and the peer review protocol. Always use these when planning work.
- **Master plan:** Read `docs/plan.md` for the phased roadmap, current status, and workstream assignments. Update it as phases are completed.
- **Rule:** No commit without Sentinel review (lint, format, tests, scope check).

## Conventions

- Python 3.11+, async/await with httpx
- Pydantic v2 models, pydantic-settings for config
- Tests: pytest + pytest-asyncio, respx for HTTP mocking, fixtures in `tests/fixtures/`
- `asyncio_mode = "auto"` in pytest config
- Ruff for linting and formatting (line length 100)
- All clients extend ConfluentClient base (except catalog providers which use their own HTTP clients)
- Prefer Confluent CLI for ad-hoc operations over writing scripts
