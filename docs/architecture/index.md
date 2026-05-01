# Architecture Overview

LineageBridge is a stream lineage extraction and visualization platform that bridges Confluent Cloud with enterprise data catalogs.

## System Architecture

```
Confluent Cloud APIs --> Clients --> Orchestrator --> LineageGraph --> Streamlit UI
  (REST v3, Kafka)       (async)      (5 phases)      (networkx)       (vis.js)
                                          │
              Databricks UC / AWS Glue <──┘ (catalog enrichment + lineage push)
```

## Core Components

### 1. Extraction Pipeline

The orchestrator runs a 5-phase extraction pipeline that builds a unified lineage graph:

1. **KafkaAdmin** - Topic inventory and consumer groups (sequential per cluster)
2. **Connect + ksqlDB + Flink** - Transformation edges (parallel)
3. **SchemaRegistry + StreamCatalog** - Schema enrichment (parallel)
4. **Tableflow** - Topic to table to catalog mapping (delegates to providers)
5. **Metrics** - Throughput enrichment (optional, parallel per cluster)

See [Extraction Pipeline](extraction-pipeline.md) for detailed phase breakdowns.

### 2. Graph Model

The lineage graph is an in-memory directed graph backed by NetworkX with:

- **Nodes** - Data assets and processing steps
- **Edges** - Data flow relationships
- **Traversal** - Upstream/downstream lineage queries
- **Serialization** - JSON export/import

See [Graph Model](graph-model.md) for node types, edge types, and ID format.

### 3. Client Architecture

All Confluent clients extend `ConfluentClient` base class with:

- Async HTTP client (httpx)
- Exponential backoff retry (3 attempts, retries on 429/5xx)
- Cursor-based pagination
- Basic auth (API key/secret)

Catalog providers use their own HTTP clients:
- **Databricks UC** - httpx with Bearer token
- **AWS Glue** - boto3 (sync, wrapped with asyncio.to_thread)
- **Google Data Lineage** - httpx with Application Default Credentials

See [Clients](clients.md) for protocols and patterns.

### 4. Catalog Provider Pattern

Data catalog integrations implement the `CatalogProvider` protocol:

```python
class CatalogProvider(Protocol):
    def build_node(...) -> tuple[LineageNode, LineageEdge]
    async def enrich(graph: LineageGraph) -> None
    def build_url(node: LineageNode) -> str
    async def push_lineage(graph: LineageGraph, ...) -> PushResult
```

Adding a new catalog requires:
1. Create a file in `catalogs/` implementing the protocol
2. Add node type to `NodeType` enum (e.g., `UC_TABLE`, `GLUE_TABLE`)
3. Add system type to `SystemType` enum (e.g., `DATABRICKS`, `AWS`)
4. Register provider in `catalogs/__init__.py`

### 5. Watcher (Change Detection)

The watcher polls Confluent Cloud REST APIs every 10 seconds to detect changes:

- Compares snapshots of topics, connectors, ksqlDB clusters, Flink statements
- Emits synthetic audit events on hash mismatches
- Debounces extraction (30s cooldown) to batch rapid changes
- Runs as background thread (compatible with Streamlit sync context)

Available as:
- CLI: `lineage-bridge-watch`
- UI: Toggle in sidebar

## Module Structure

```
lineage_bridge/
  config/
    settings.py              # Pydantic Settings (LINEAGE_BRIDGE_ env prefix)
    cache.py                 # Encrypted local JSON cache
    provisioner.py           # Auto-provision API keys via Confluent CLI
  
  clients/
    base.py                  # ConfluentClient: async httpx, retry, pagination
    protocol.py              # LineageExtractor protocol
    discovery.py             # Environment + cluster discovery
    kafka_admin.py           # Topics + consumer groups
    connect.py               # Connectors + external datasets
    flink.py                 # Flink SQL parsing
    ksqldb.py                # ksqlDB queries
    schema_registry.py       # Schema enrichment
    stream_catalog.py        # Tags, business metadata
    tableflow.py             # Topic -> table -> catalog mapping
    metrics.py               # Confluent Metrics API
    databricks_discovery.py  # Databricks workspace + catalog discovery
    databricks_sql.py        # SQL Statement Execution API
  
  catalogs/
    protocol.py              # CatalogProvider protocol (extensible)
    databricks_uc.py         # Databricks Unity Catalog provider
    aws_glue.py              # AWS Glue provider
    google_lineage.py        # Google Data Lineage provider
  
  models/
    graph.py                 # LineageNode, LineageEdge, LineageGraph
    audit_event.py           # Audit log event parsing (CloudEvent format)
  
  extractors/
    orchestrator.py          # 5-phase extraction pipeline
  
  ui/
    app.py                   # Streamlit app (main entry)
    sidebar.py               # Connection, filters, extractors
    extraction.py            # Extraction progress panel
    discovery.py             # Environment/cluster discovery UI
    state.py                 # Session state management
    graph_renderer.py        # vis.js data prep + Sugiyama layout
    node_details.py          # Node detail panel
    styles.py                # Colors, icons, URL builders
    watcher.py               # Watcher controls in UI
    sample_data.py           # Sample graph for demo mode
    components/visjs_graph/  # Custom Streamlit component (vis.js)
  
  watcher/
    engine.py                # Change-detection poller + debounced extraction
    cli.py                   # Watcher CLI entry point
  
  api/
    app.py                   # FastAPI REST API (OpenLineage-compatible)
    openlineage/
      models.py              # OpenLineage RunEvent models
      translator.py          # LineageGraph <-> OpenLineage conversion
      store.py               # In-memory graph/event stores
    routers/                 # API endpoints
```

## Data Flow

### Extraction Flow

```
1. User triggers extraction via UI/CLI
2. Orchestrator discovers environments + clusters
3. Phase 1: KafkaAdminClient extracts topics per cluster (sequential)
4. Phase 2: Connect/ksqlDB/Flink extract transformations (parallel)
5. Phase 3: SchemaRegistry/StreamCatalog enrich (parallel)
6. Phase 4: Tableflow maps topics to catalog tables
7. Phase 4b: Catalog providers enrich with UC/Glue metadata
8. Phase 5: MetricsClient enriches with throughput data
9. Graph validation (warnings for orphans/dangling edges)
10. Graph rendered in UI with vis.js
```

### Lineage Push Flow

```
1. User triggers push via UI/CLI
2. Orchestrator filters graph for UC/Glue nodes
3. For each catalog table:
   - Generate upstream lineage summary
   - Set table properties (machine-readable metadata)
   - Set table comments (human-readable summary)
   - Optional: Insert rows into bridge table
4. Return PushResult with counts + errors
```

## Design Principles

1. **Async-first** - All I/O is async (httpx, asyncio) for performance
2. **Protocol over ABC** - Structural typing for extensibility
3. **Fail-safe extraction** - Extractor failures return empty lists, don't fail the pipeline
4. **In-place graph mutation** - Enrichment mutates graph directly (simpler than merge)
5. **Separate node types per catalog** - UC_TABLE, GLUE_TABLE for type-safe filtering
6. **Two-phase catalog integration** - build_node() in phase 4, enrich() in phase 4b

See [Design Decisions](design-decisions.md) for ADRs and tradeoff rationale.

## Entry Points

- **UI**: `streamlit run lineage_bridge/ui/app.py`
- **CLI Extraction**: `lineage-bridge-extract --env env-abc123`
- **CLI Watcher**: `lineage-bridge-watch`
- **REST API**: `uvicorn lineage_bridge.api.app:app`

## Next Steps

- [Extraction Pipeline](extraction-pipeline.md) - Detailed phase breakdown
- [Graph Model](graph-model.md) - Node/edge types and ID format
- [Clients](clients.md) - Client protocols and patterns
- [Design Decisions](design-decisions.md) - ADRs and tradeoffs
