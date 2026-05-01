# API Examples

Comprehensive examples for using the LineageBridge API with cURL and Python.

## Prerequisites

Start the API server:

```bash
uv run lineage-bridge-api
# or
make api
```

## Health Check

### cURL

```bash
curl http://localhost:8000/api/v1/health
```

Response:

```json
{"status": "ok"}
```

### Python (httpx)

```python
import httpx

response = httpx.get("http://localhost:8000/api/v1/health")
print(response.json())
# {'status': 'ok'}
```

### Python (requests)

```python
import requests

response = requests.get("http://localhost:8000/api/v1/health")
print(response.json())
```

## Lineage Events

### Query All Events

**cURL:**

```bash
curl http://localhost:8000/api/v1/lineage/events
```

**Python:**

```python
import httpx

client = httpx.Client(base_url="http://localhost:8000/api/v1")
events = client.get("/lineage/events").json()

for event in events:
    print(f"{event['job']['name']} ({event['eventType']})")
```

### Query with Filters

**cURL:**

```bash
# Filter by namespace (glob patterns supported)
curl "http://localhost:8000/api/v1/lineage/events?namespace=confluent://*"

# Filter by job name
curl "http://localhost:8000/api/v1/lineage/events?job=my-connector"

# Filter by time range
curl "http://localhost:8000/api/v1/lineage/events?since=2026-04-01T00:00:00Z&until=2026-04-30T23:59:59Z"

# Pagination
curl "http://localhost:8000/api/v1/lineage/events?limit=10&offset=0"
```

**Python:**

```python
import httpx
from datetime import datetime, timedelta

client = httpx.Client(base_url="http://localhost:8000/api/v1")

# Filter by time range
since = (datetime.now() - timedelta(days=7)).isoformat() + "Z"
events = client.get("/lineage/events", params={
    "namespace": "confluent://*",
    "since": since,
    "limit": 100
}).json()

print(f"Found {len(events)} events in the last 7 days")
```

### Get Events by Run ID

**cURL:**

```bash
curl http://localhost:8000/api/v1/lineage/events/550e8400-e29b-41d4-a716-446655440000
```

**Python:**

```python
run_id = "550e8400-e29b-41d4-a716-446655440000"
events = client.get(f"/lineage/events/{run_id}").json()

for event in events:
    print(f"{event['eventTime']}: {event['eventType']}")
```

### Ingest External Events

**cURL:**

```bash
curl -X POST http://localhost:8000/api/v1/lineage/events \
  -H "Content-Type: application/json" \
  -d '[
    {
      "eventTime": "2026-04-30T00:00:00Z",
      "eventType": "COMPLETE",
      "run": {
        "runId": "my-run-1"
      },
      "job": {
        "namespace": "databricks://my-workspace",
        "name": "etl-pipeline"
      },
      "inputs": [
        {
          "namespace": "confluent://env-1/lkc-1",
          "name": "orders"
        }
      ],
      "outputs": [
        {
          "namespace": "databricks://my-workspace",
          "name": "catalog.schema.orders"
        }
      ]
    }
  ]'
```

**Python:**

```python
from datetime import datetime
import uuid

event = {
    "eventTime": datetime.utcnow().isoformat() + "Z",
    "eventType": "COMPLETE",
    "run": {
        "runId": str(uuid.uuid4())
    },
    "job": {
        "namespace": "databricks://my-workspace",
        "name": "etl-pipeline",
        "facets": {
            "documentation": {
                "description": "Daily ETL pipeline for orders"
            }
        }
    },
    "inputs": [
        {
            "namespace": "confluent://env-1/lkc-1",
            "name": "orders"
        }
    ],
    "outputs": [
        {
            "namespace": "databricks://my-workspace",
            "name": "catalog.schema.orders"
        }
    ]
}

response = client.post("/lineage/events", json=[event])
print(response.json())
```

## Datasets

### List All Datasets

**cURL:**

```bash
curl http://localhost:8000/api/v1/lineage/datasets
```

**Python:**

```python
datasets = client.get("/lineage/datasets").json()

for ds in datasets:
    print(f"{ds['namespace']} / {ds['name']}")
```

### Filter Datasets

**cURL:**

```bash
# Filter by namespace (glob patterns supported)
curl "http://localhost:8000/api/v1/lineage/datasets?namespace=confluent://*"

# Filter by name
curl "http://localhost:8000/api/v1/lineage/datasets?name=orders"
```

**Python:**

```python
# Find all Kafka topics
topics = client.get("/lineage/datasets", params={
    "namespace": "confluent://*"
}).json()

print(f"Found {len(topics)} Kafka topics")

# Find all UC tables
uc_tables = client.get("/lineage/datasets", params={
    "namespace": "databricks://*"
}).json()

print(f"Found {len(uc_tables)} Unity Catalog tables")
```

### Get Dataset Details

**cURL:**

```bash
curl "http://localhost:8000/api/v1/lineage/datasets/detail?namespace=confluent://env-1/lkc-1&name=orders"
```

**Python:**

```python
dataset = client.get("/lineage/datasets/detail", params={
    "namespace": "confluent://env-1/lkc-1",
    "name": "orders"
}).json()

print(f"Dataset: {dataset['name']}")
if dataset.get("facets", {}).get("schema"):
    schema = dataset["facets"]["schema"]
    print(f"Schema fields: {len(schema['fields'])}")
    for field in schema["fields"]:
        print(f"  - {field['name']}: {field['type']}")
```

### Traverse Dataset Lineage

**cURL:**

```bash
# Upstream lineage (where does this data come from?)
curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=databricks://workspace-1&name=catalog.sales.orders&direction=upstream&depth=5"

# Downstream lineage (where does this data go?)
curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=confluent://env-1/lkc-1&name=orders&direction=downstream&depth=3"

# Both directions
curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=confluent://env-1/lkc-1&name=orders&direction=both&depth=10"
```

**Python:**

```python
# Trace upstream lineage for a UC table
lineage = client.get("/lineage/datasets/lineage", params={
    "namespace": "databricks://workspace-1",
    "name": "catalog.sales.orders",
    "direction": "upstream",
    "depth": 5
}).json()

print("Upstream lineage:")
for node in lineage.get("nodes", []):
    print(f"  - {node['display_name']} ({node['node_type']})")

print("\nEdges:")
for edge in lineage.get("edges", []):
    print(f"  {edge['src_id']} --{edge['edge_type']}--> {edge['dst_id']}")
```

## Jobs

### List All Jobs

**cURL:**

```bash
curl http://localhost:8000/api/v1/lineage/jobs
```

**Python:**

```python
jobs = client.get("/lineage/jobs").json()

for job in jobs:
    print(f"{job['namespace']} / {job['name']}")
```

### Filter Jobs

**cURL:**

```bash
# Filter by namespace
curl "http://localhost:8000/api/v1/lineage/jobs?namespace=confluent://*"

# Filter by name
curl "http://localhost:8000/api/v1/lineage/jobs?name=*-connector"
```

**Python:**

```python
# Find all connectors
connectors = client.get("/lineage/jobs", params={
    "namespace": "confluent://*",
    "name": "*-connector"
}).json()

print(f"Found {len(connectors)} connectors")
```

### Get Job Details

**cURL:**

```bash
curl "http://localhost:8000/api/v1/lineage/jobs/detail?namespace=confluent://env-1/lkc-1&name=postgres-cdc-source"
```

**Python:**

```python
job = client.get("/lineage/jobs/detail", params={
    "namespace": "confluent://env-1/lkc-1",
    "name": "postgres-cdc-source"
}).json()

print(f"Job: {job['name']}")
print(f"Inputs: {len(job.get('inputs', []))}")
print(f"Outputs: {len(job.get('outputs', []))}")

if job.get("facets", {}).get("sql"):
    print(f"SQL: {job['facets']['sql']['query']}")
```

## Graphs

### List Graphs

**cURL:**

```bash
curl http://localhost:8000/api/v1/graphs
```

**Python:**

```python
graphs = client.get("/graphs").json()

for graph in graphs:
    print(f"{graph['graph_id']}: {graph['node_count']} nodes, {graph['edge_count']} edges")
```

### Create Graph

**cURL:**

```bash
curl -X POST http://localhost:8000/api/v1/graphs
```

**Python:**

```python
response = client.post("/graphs")
graph_id = response.json()["graph_id"]
print(f"Created graph: {graph_id}")
```

### Get Graph

**cURL:**

```bash
curl http://localhost:8000/api/v1/graphs/my-graph-id
```

**Python:**

```python
graph = client.get("/graphs/my-graph-id").json()

print(f"Nodes: {len(graph['nodes'])}")
print(f"Edges: {len(graph['edges'])}")
print(f"Stats: {graph['stats']}")
```

### Export Graph

**cURL:**

```bash
curl http://localhost:8000/api/v1/graphs/my-graph-id/export > graph.json
```

**Python:**

```python
import json

graph_data = client.get("/graphs/my-graph-id/export").json()

# Save to file
with open("graph.json", "w") as f:
    json.dump(graph_data, f, indent=2)

print(f"Exported graph with {len(graph_data['nodes'])} nodes")
```

### Import Graph

**cURL:**

```bash
curl -X POST http://localhost:8000/api/v1/graphs/my-graph-id/import \
  -H "Content-Type: application/json" \
  -d @graph.json
```

**Python:**

```python
import json

# Load from file
with open("graph.json") as f:
    graph_data = json.load(f)

# Import
response = client.post("/graphs/my-graph-id/import", json=graph_data)
print(response.json())
```

### Confluent-Only View

**cURL:**

```bash
curl http://localhost:8000/api/v1/graphs/confluent/view > confluent-lineage.json
```

**Python:**

```python
# Get pure Confluent lineage (no catalog nodes)
confluent_view = client.get("/graphs/confluent/view").json()

events = confluent_view.get("events", [])
print(f"Found {len(events)} OpenLineage events from Confluent")

# Save for external consumption
with open("confluent-lineage.json", "w") as f:
    json.dump(confluent_view, f, indent=2)
```

### Enriched View

**cURL:**

```bash
# All systems
curl http://localhost:8000/api/v1/graphs/enriched/view > full-lineage.json

# Filter by systems
curl "http://localhost:8000/api/v1/graphs/enriched/view?systems=confluent,databricks" > confluent-databricks.json
```

**Python:**

```python
# Full cross-platform lineage
enriched_view = client.get("/graphs/enriched/view").json()

events = enriched_view.get("events", [])
print(f"Total events: {len(events)}")

# Filter by systems
databricks_view = client.get("/graphs/enriched/view", params={
    "systems": "confluent,databricks"
}).json()

print(f"Confluent+Databricks events: {len(databricks_view.get('events', []))}")
```

### Query Node Lineage

**cURL:**

```bash
# Upstream lineage
curl "http://localhost:8000/api/v1/graphs/my-graph-id/query/upstream/confluent:kafka_topic:env-1:lkc-1%2Forders?hops=3"

# Downstream lineage
curl "http://localhost:8000/api/v1/graphs/my-graph-id/query/downstream/confluent:kafka_topic:env-1:lkc-1%2Forders?hops=3"
```

**Python:**

```python
import urllib.parse

# URL-encode the node ID
node_id = "confluent:kafka_topic:env-1:lkc-1/orders"
encoded_id = urllib.parse.quote(node_id, safe="")

# Query upstream
upstream = client.get(
    f"/graphs/my-graph-id/query/upstream/{encoded_id}",
    params={"hops": 3}
).json()

print(f"Upstream nodes: {len(upstream.get('nodes', []))}")
```

## Tasks

### Trigger Extraction

**cURL:**

```bash
# Extract all environments
curl -X POST http://localhost:8000/api/v1/tasks/extract

# Extract specific environments
curl -X POST http://localhost:8000/api/v1/tasks/extract \
  -H "Content-Type: application/json" \
  -d '["env-abc", "env-xyz"]'
```

**Python:**

```python
# Extract all environments
response = client.post("/tasks/extract")
task_id = response.json()["task_id"]
print(f"Started extraction: {task_id}")

# Extract specific environments
response = client.post("/tasks/extract", json=["env-abc", "env-xyz"])
task_id = response.json()["task_id"]
```

### Trigger Enrichment

**cURL:**

```bash
# Enrich default graph
curl -X POST http://localhost:8000/api/v1/tasks/enrich

# Enrich specific graph
curl -X POST "http://localhost:8000/api/v1/tasks/enrich?graph_id=my-graph-id"
```

**Python:**

```python
# Enrich default graph
response = client.post("/tasks/enrich")
task_id = response.json()["task_id"]

# Enrich specific graph
response = client.post("/tasks/enrich", params={"graph_id": "my-graph-id"})
task_id = response.json()["task_id"]
```

### Poll Task Status

**cURL:**

```bash
curl http://localhost:8000/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000
```

**Python:**

```python
import time

task_id = "550e8400-e29b-41d4-a716-446655440000"

while True:
    task = client.get(f"/tasks/{task_id}").json()
    status = task["status"]
    
    print(f"Status: {status}")
    
    if status == "completed":
        print("Result:", task["result"])
        break
    elif status == "failed":
        print("Error:", task["error"])
        break
    
    # Show progress
    if task.get("progress"):
        for msg in task["progress"]:
            print(f"  {msg}")
    
    time.sleep(2)
```

### List Tasks

**cURL:**

```bash
# All tasks
curl http://localhost:8000/api/v1/tasks

# Filter by type
curl "http://localhost:8000/api/v1/tasks?task_type=extract"

# Filter by status
curl "http://localhost:8000/api/v1/tasks?status=completed"

# Limit results
curl "http://localhost:8000/api/v1/tasks?limit=5"
```

**Python:**

```python
# List recent extraction tasks
tasks = client.get("/tasks", params={
    "task_type": "extract",
    "limit": 10
}).json()

for task in tasks:
    print(f"{task['task_id']}: {task['status']} ({task['created_at']})")
```

## Complete Workflow Example

Extract lineage from Confluent, traverse it, and export as OpenLineage:

**Python:**

```python
import httpx
import time
import json

client = httpx.Client(
    base_url="http://localhost:8000/api/v1",
    timeout=30.0
)

# 1. Trigger extraction
print("Starting extraction...")
response = client.post("/tasks/extract")
task_id = response.json()["task_id"]

# 2. Poll until complete
while True:
    task = client.get(f"/tasks/{task_id}").json()
    if task["status"] == "completed":
        print("Extraction complete!")
        break
    elif task["status"] == "failed":
        print(f"Extraction failed: {task['error']}")
        exit(1)
    time.sleep(2)

# 3. Find a UC table
uc_tables = client.get("/lineage/datasets", params={
    "namespace": "databricks://*"
}).json()

if not uc_tables:
    print("No UC tables found")
    exit(0)

table = uc_tables[0]
print(f"\nFound UC table: {table['name']}")

# 4. Traverse upstream lineage
lineage = client.get("/lineage/datasets/lineage", params={
    "namespace": table["namespace"],
    "name": table["name"],
    "direction": "upstream",
    "depth": 10
}).json()

print(f"\nUpstream lineage chain:")
for node in lineage.get("nodes", []):
    print(f"  - {node['display_name']} ({node['node_type']})")

# 5. Export as OpenLineage
enriched_view = client.get("/graphs/enriched/view").json()

with open("lineage.json", "w") as f:
    json.dump(enriched_view, f, indent=2)

print(f"\nExported {len(enriched_view.get('events', []))} OpenLineage events to lineage.json")
```

## Error Handling

**Python:**

```python
import httpx

client = httpx.Client(base_url="http://localhost:8000/api/v1")

try:
    response = client.get("/lineage/datasets/detail", params={
        "namespace": "unknown",
        "name": "unknown"
    })
    response.raise_for_status()
    dataset = response.json()
except httpx.HTTPStatusError as e:
    if e.response.status_code == 404:
        print("Dataset not found")
    else:
        print(f"HTTP error: {e}")
except httpx.RequestError as e:
    print(f"Request failed: {e}")
```

## Further Reading

- [OpenLineage Mapping](openlineage-mapping.md) - Understand the translation layer
- [Authentication Guide](authentication.md) - Set up API keys
- [Interactive Explorer](openapi.md) - Try the API in your browser
