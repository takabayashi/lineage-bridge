# API Reference

LineageBridge exposes an **OpenLineage-compatible REST API** that bridges Confluent Cloud stream lineage with external data catalogs.

## Overview

The LineageBridge API provides a RESTful interface for:

- Querying OpenLineage events from Confluent Cloud
- Ingesting lineage from external systems
- Managing lineage graphs
- Triggering extraction and enrichment tasks
- Traversing dataset and job relationships

## Base URL

The API runs on `http://localhost:8000` by default, with all endpoints prefixed by `/api/v1`:

```
http://localhost:8000/api/v1/
```

Configure the host and port via environment variables:

```bash
export LINEAGE_BRIDGE_API_HOST="0.0.0.0"
export LINEAGE_BRIDGE_API_PORT=8000
```

## Versioning

The API uses URL-based versioning. The current version is `v1`, reflected in all endpoint paths:

```
/api/v1/health
/api/v1/lineage/events
/api/v1/graphs
```

Future versions will be released as `v2`, `v3`, etc., allowing backward compatibility.

## Endpoint Categories

The API is organized into 6 main routers:

### Meta

System-level endpoints for health checks and metadata.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/health` | Health check (no auth required) |
| `GET /api/v1/version` | API version information |
| `GET /api/v1/catalogs` | List registered catalog providers |

### Lineage

OpenLineage event query and ingestion.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/lineage/events` | Query OpenLineage events with filters |
| `POST /api/v1/lineage/events` | Ingest OpenLineage events from external systems |
| `GET /api/v1/lineage/events/{run_id}` | Get events for a specific run |

### Datasets

Dataset discovery and lineage traversal.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/lineage/datasets` | List all datasets with optional filters |
| `GET /api/v1/lineage/datasets/detail` | Get a specific dataset by namespace and name |
| `GET /api/v1/lineage/datasets/lineage` | Traverse upstream/downstream lineage for a dataset |

### Jobs

Job discovery and relationship queries.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/lineage/jobs` | List all jobs with optional filters |
| `GET /api/v1/lineage/jobs/detail` | Get a job with its inputs and outputs |

### Graphs

Graph management and views.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/graphs` | List all in-memory graphs |
| `POST /api/v1/graphs` | Create a new empty graph |
| `GET /api/v1/graphs/{graph_id}` | Get a full graph with nodes and edges |
| `DELETE /api/v1/graphs/{graph_id}` | Delete a graph |
| `POST /api/v1/graphs/{graph_id}/import` | Import nodes and edges from JSON |
| `GET /api/v1/graphs/{graph_id}/export` | Export graph as LineageGraph JSON |
| `GET /api/v1/graphs/confluent/view` | Confluent-only lineage view |
| `GET /api/v1/graphs/enriched/view` | Full enriched cross-platform lineage view |
| `GET /api/v1/graphs/{graph_id}/nodes` | List nodes with filters |
| `POST /api/v1/graphs/{graph_id}/nodes` | Add a node |
| `GET /api/v1/graphs/{graph_id}/nodes/{node_id}` | Get a specific node |
| `POST /api/v1/graphs/{graph_id}/edges` | Add an edge |
| `GET /api/v1/graphs/{graph_id}/edges` | List all edges |
| `GET /api/v1/graphs/{graph_id}/query/upstream/{node_id}` | Query upstream lineage |
| `GET /api/v1/graphs/{graph_id}/query/downstream/{node_id}` | Query downstream lineage |

### Tasks

Async task management.

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/tasks` | List recent tasks with optional filters |
| `GET /api/v1/tasks/{task_id}` | Get task status and result |
| `POST /api/v1/tasks/extract` | Trigger async lineage extraction from Confluent Cloud |
| `POST /api/v1/tasks/enrich` | Trigger async catalog enrichment |

## Quick Start

Start the API server:

```bash
uv run lineage-bridge-api
# or
make api
```

Verify it's running:

```bash
curl http://localhost:8000/api/v1/health
# {"status": "ok"}
```

Open the interactive Swagger UI:

```
http://localhost:8000/docs
```

## OpenAPI Specification

The full OpenAPI 3.1 specification is available at:

- **Download**: `GET /api/v1/openapi.yaml`
- **Repository**: `/docs/openapi.yaml`
- **Interactive Explorer**: See [OpenAPI Explorer](openapi.md)

## Next Steps

- [Authentication Guide](authentication.md) - Set up API keys
- [OpenLineage Mapping](openlineage-mapping.md) - Understand the translation layer
- [Code Examples](examples.md) - cURL and Python examples
- [Interactive Explorer](openapi.md) - Try the API in your browser
