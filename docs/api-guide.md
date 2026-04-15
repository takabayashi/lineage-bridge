# LineageBridge API Guide

LineageBridge exposes an **OpenLineage-compatible REST API** that bridges the gap between Confluent Cloud's stream lineage and external data catalogs (Databricks Unity Catalog, AWS Glue, Google Data Lineage, and more).

## Why This Exists

Confluent Cloud has no lineage API. External catalogs that speak OpenLineage (the emerging industry standard) cannot consume Confluent stream lineage natively. LineageBridge fills this gap by:

1. **Extracting** stream lineage from Confluent Cloud (topics, connectors, ksqlDB, Flink, schemas, consumer groups)
2. **Serving** it as standard OpenLineage events via REST API
3. **Accepting** OpenLineage events from external catalogs (bidirectional)
4. **Providing** unified lineage views across platforms

## Quick Start

```bash
# Install
uv pip install -e ".[dev]"

# Start the API server
uv run lineage-bridge-api
# or
make api

# Verify it's running
curl http://localhost:8000/api/v1/health
# {"status": "ok"}

# Open Swagger UI
open http://localhost:8000/docs
```

### Configuration

Set environment variables with the `LINEAGE_BRIDGE_` prefix:

```bash
export LINEAGE_BRIDGE_API_KEY="your-secret-key"  # Optional: enable auth
export LINEAGE_BRIDGE_API_HOST="0.0.0.0"          # Default
export LINEAGE_BRIDGE_API_PORT=8000                # Default
```

When `LINEAGE_BRIDGE_API_KEY` is set, all endpoints (except `/health`) require an `X-API-Key` header:

```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8000/api/v1/lineage/events
```

## OpenLineage Mapping

LineageBridge maps Confluent concepts to OpenLineage types:

| Confluent Concept | OpenLineage Type | Namespace Format |
|---|---|---|
| Kafka Topic | Dataset | `confluent://{env_id}/{cluster_id}` |
| Connector | Job (with input/output datasets) | `confluent://{env_id}/{cluster_id}` |
| ksqlDB Query | Job | `confluent://{env_id}/{cluster_id}` |
| Flink Job | Job | `confluent://{env_id}/{cluster_id}` |
| Consumer Group | Job | `confluent://{env_id}/{cluster_id}` |
| Tableflow Table | Dataset | `confluent://{env_id}/{cluster_id}` |
| Schema | SchemaDatasetFacet (on parent Dataset) | -- |
| UC Table | Dataset | `databricks://{workspace}` |
| Glue Table | Dataset | `aws://{region}/{database}` |
| Google BQ Table | Dataset | `google://{project}/{dataset}` |

Edge types map to OpenLineage input/output relationships:
- **PRODUCES** -> OutputDataset on a Job
- **CONSUMES** -> InputDataset on a Job
- **TRANSFORMS** -> Job with both inputs and outputs
- **MATERIALIZES** -> OutputDataset (Tableflow -> catalog)

## API Reference

### Meta

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/health` | Health check (no auth required) |
| GET | `/api/v1/version` | Version info |
| GET | `/api/v1/catalogs` | List registered catalog providers |
| GET | `/api/v1/openapi.yaml` | Download OpenAPI spec |

### OpenLineage Events

```bash
# Query events
curl http://localhost:8000/api/v1/lineage/events

# Query with filters
curl "http://localhost:8000/api/v1/lineage/events?namespace=confluent://*&job=my-connector"

# Get events for a specific run
curl http://localhost:8000/api/v1/lineage/events/{run_id}

# Ingest events from an external system
curl -X POST http://localhost:8000/api/v1/lineage/events \
  -H "Content-Type: application/json" \
  -d '[{
    "eventTime": "2026-04-14T00:00:00Z",
    "eventType": "COMPLETE",
    "run": {"runId": "my-run-1"},
    "job": {"namespace": "databricks://my-workspace", "name": "etl-pipeline"},
    "inputs": [{"namespace": "confluent://env-1/lkc-1", "name": "orders"}],
    "outputs": [{"namespace": "databricks://my-workspace", "name": "catalog.schema.orders"}]
  }]'
```

### Datasets & Jobs

```bash
# List all datasets
curl http://localhost:8000/api/v1/lineage/datasets

# Get a specific dataset
curl "http://localhost:8000/api/v1/lineage/datasets/detail?namespace=confluent://env-1/lkc-1&name=orders"

# Traverse dataset lineage
curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=confluent://env-1/lkc-1&name=orders&direction=upstream&depth=5"

# List all jobs
curl http://localhost:8000/api/v1/lineage/jobs
```

### Graph Management

```bash
# List graphs
curl http://localhost:8000/api/v1/graphs

# Create an empty graph
curl -X POST http://localhost:8000/api/v1/graphs

# Import a LineageGraph
curl -X POST http://localhost:8000/api/v1/graphs/{graph_id}/import \
  -H "Content-Type: application/json" \
  -d @lineage_graph.json

# Export a graph
curl http://localhost:8000/api/v1/graphs/{graph_id}/export
```

### Graph Views

```bash
# Confluent-only view (pure stream lineage, no catalog nodes)
curl http://localhost:8000/api/v1/graphs/confluent/view

# Enriched view (full cross-platform lineage)
curl http://localhost:8000/api/v1/graphs/enriched/view

# Filter enriched view by system
curl "http://localhost:8000/api/v1/graphs/enriched/view?systems=confluent,databricks"
```

### Async Tasks

```bash
# Trigger extraction from Confluent Cloud
curl -X POST http://localhost:8000/api/v1/tasks/extract

# Trigger catalog enrichment
curl -X POST "http://localhost:8000/api/v1/tasks/enrich?graph_id=my-graph"

# Poll task status
curl http://localhost:8000/api/v1/tasks/{task_id}

# List recent tasks
curl http://localhost:8000/api/v1/tasks
```

## Use Case: See Kafka Topic Lineage in Unity Catalog

1. Start the API and extract Confluent lineage:
   ```bash
   make api
   curl -X POST http://localhost:8000/api/v1/tasks/extract
   ```

2. Query upstream lineage for a UC table:
   ```bash
   curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=databricks://my-workspace&name=catalog.schema.orders&direction=upstream&depth=5"
   ```

3. The response shows the full lineage path: PostgreSQL -> CDC Connector -> Kafka Topic -> Tableflow -> UC Table.

4. View as OpenLineage events:
   ```bash
   curl http://localhost:8000/api/v1/graphs/enriched/view
   ```

## Use Case: Push Lineage to Google Data Lineage

Google Data Lineage natively accepts OpenLineage events:

1. Extract and serve Confluent lineage:
   ```bash
   curl http://localhost:8000/api/v1/graphs/confluent/view
   ```

2. The response is standard OpenLineage -- forward it to Google:
   ```bash
   # Google Data Lineage accepts OpenLineage events directly
   curl -X POST \
     "https://datalineage.googleapis.com/v1/projects/my-project/locations/us:processOpenLineageRunEvent" \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json" \
     -d @confluent-lineage-events.json
   ```

3. Or configure LineageBridge to push automatically via the Google provider.

## Bidirectional Flow

LineageBridge is bidirectional -- it doesn't just serve lineage outward, it also accepts lineage inward:

```
External Catalogs                LineageBridge API                  Confluent Cloud
(UC, Google, Glue)              (OpenLineage-compatible)            (no lineage API)
       |                                |                                 |
       |  POST /events (OL ingest) --> |  <-- extraction (CLI/watcher) --+
       |                                |
       | <-- GET /lineage (OL serve) -- |
       |                                |
       +--------------------------------+
                    |
              Streamlit UI
           (unified lineage view)
```

When you POST OpenLineage events, they are merged into the graph store and become visible in the enriched graph view alongside Confluent-extracted lineage.

## Registered Catalog Providers

| Provider | Catalog Type | Node Type | System |
|---|---|---|---|
| Databricks UC | `UNITY_CATALOG` | `uc_table` | `databricks` |
| AWS Glue | `AWS_GLUE` | `glue_table` | `aws` |
| Google Data Lineage | `GOOGLE_DATA_LINEAGE` | `google_table` | `google` |

Adding a new catalog = one file in `lineage_bridge/catalogs/`, register in `__init__.py`.
