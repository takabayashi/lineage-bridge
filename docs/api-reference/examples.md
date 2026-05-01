# API Examples

Comprehensive examples for using the LineageBridge API with cURL and Python.

!!! tip "Interactive API Testing"
    **Prefer interactive testing?** Visit **http://localhost:8000/docs** in your browser after starting the API server. You can test all endpoints directly in Swagger UI without writing code!

## Prerequisites

Start the API server:

```bash
make api
# API available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

## Health Check

Use this endpoint to check if the API is running. It's perfect for monitoring scripts, container health checks, or just making sure everything is working before you run expensive operations.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/health
    ```
    
    Response:
    
    ```json
    {"status": "ok"}
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    response = httpx.get("http://localhost:8000/api/v1/health")
    print(response.json())
    # {'status': 'ok'}
    ```

=== "Python (requests)"
    ```python
    import requests
    
    response = requests.get("http://localhost:8000/api/v1/health")
    print(response.json())
    # {'status': 'ok'}
    ```

**No auth required**: This endpoint works even if you have API keys enabled.

## Lineage Events

### Query All Events

Get all OpenLineage events from the API. Each event represents a job execution (like a connector run or ksqlDB query) with its inputs and outputs.

**When you'd use this:** You're building a custom lineage dashboard and want to show all data flows in your organization.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/lineage/events
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    events = client.get("/lineage/events").json()
    
    for event in events:
        print(f"{event['job']['name']} ({event['eventType']})")
    ```

=== "Python (requests)"
    ```python
    import requests
    
    response = requests.get("http://localhost:8000/api/v1/lineage/events")
    events = response.json()
    
    for event in events:
        print(f"{event['job']['name']} ({event['eventType']})")
    ```

### Query with Filters

Narrow down results to specific namespaces, jobs, or time ranges. Supports glob patterns (`*`) for flexible matching.

**When you'd use this:** You only care about lineage for production environments, or you want to see what changed in the last week.

=== "cURL"
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

=== "Python (httpx)"
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

=== "Python (requests)"
    ```python
    import requests
    from datetime import datetime, timedelta
    
    # Filter by time range
    since = (datetime.now() - timedelta(days=7)).isoformat() + "Z"
    response = requests.get(
        "http://localhost:8000/api/v1/lineage/events",
        params={
            "namespace": "confluent://*",
            "since": since,
            "limit": 100
        }
    )
    events = response.json()
    
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

Send OpenLineage events from external systems (like dbt, Airflow, or custom ETL jobs) to LineageBridge. This creates a unified lineage view across all your data platforms.

**When you'd use this:** Your Databricks notebook reads from a Kafka topic and writes to a Unity Catalog table. You want to see that flow in LineageBridge alongside your Confluent lineage.

=== "cURL"
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

=== "Python (httpx)"
    ```python
    from datetime import datetime
    import uuid
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
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

=== "Python (requests)"
    ```python
    from datetime import datetime
    import uuid
    import requests
    
    event = {
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "eventType": "COMPLETE",
        "run": {
            "runId": str(uuid.uuid4())
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
    
    response = requests.post(
        "http://localhost:8000/api/v1/lineage/events",
        json=[event]
    )
    print(response.json())
    ```

## Datasets

### List All Datasets

Get all datasets (topics, tables, external sources) from the lineage graph.

**When you'd use this:** You want to see all the data assets in your organization — Kafka topics, Unity Catalog tables, Glue tables, etc.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/lineage/datasets
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    datasets = client.get("/lineage/datasets").json()
    
    for ds in datasets:
        print(f"{ds['namespace']} / {ds['name']}")
    ```

=== "Python (requests)"
    ```python
    import requests
    
    response = requests.get("http://localhost:8000/api/v1/lineage/datasets")
    datasets = response.json()
    
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

Follow the data flow upstream (where does this come from?) or downstream (where does it go?). This is the most powerful feature for impact analysis.

**When you'd use this:** 
- **Upstream**: "This table has bad data. Where did it come from?"
- **Downstream**: "I'm changing this Kafka topic schema. What will break?"

=== "cURL"
    ```bash
    # Upstream lineage (where does this data come from?)
    curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=databricks://workspace-1&name=catalog.sales.orders&direction=upstream&depth=5"
    
    # Downstream lineage (where does this data go?)
    curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=confluent://env-1/lkc-1&name=orders&direction=downstream&depth=3"
    
    # Both directions (full blast radius)
    curl "http://localhost:8000/api/v1/lineage/datasets/lineage?namespace=confluent://env-1/lkc-1&name=orders&direction=both&depth=10"
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
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

=== "Python (requests)"
    ```python
    import requests
    
    # Trace upstream lineage for a UC table
    response = requests.get(
        "http://localhost:8000/api/v1/lineage/datasets/lineage",
        params={
            "namespace": "databricks://workspace-1",
            "name": "catalog.sales.orders",
            "direction": "upstream",
            "depth": 5
        }
    )
    lineage = response.json()
    
    print("Upstream lineage:")
    for node in lineage.get("nodes", []):
        print(f"  - {node['display_name']} ({node['node_type']})")
    
    print("\nEdges:")
    for edge in lineage.get("edges", []):
        print(f"  {edge['src_id']} --{edge['edge_type']}--> {edge['dst_id']}")
    ```

**Depth parameter:** How many hops to traverse. Use `depth=1` for immediate parents/children, `depth=10` for the full chain.

**Example output:**

```
Upstream lineage:
  - catalog.sales.orders (uc_table)
  - kafka-delta-sink (connector)
  - enriched_orders (kafka_topic)
  - order-enrichment (ksqldb_query)
  - raw_orders (kafka_topic)
  - postgres-cdc-source (connector)
  - public.orders (external_dataset)

Edges:
  postgres-cdc-source --PRODUCES--> raw_orders
  raw_orders --CONSUMES--> order-enrichment
  order-enrichment --PRODUCES--> enriched_orders
  enriched_orders --CONSUMES--> kafka-delta-sink
  kafka-delta-sink --PRODUCES--> catalog.sales.orders
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

See all in-memory lineage graphs. LineageBridge can manage multiple graphs (like different environments or snapshots).

**When you'd use this:** You're running multiple extractions (dev vs prod) and want to see which graphs are loaded.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/graphs
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    graphs = client.get("/graphs").json()
    
    for graph in graphs:
        print(f"{graph['graph_id']}: {graph['node_count']} nodes, {graph['edge_count']} edges")
    ```

=== "Python (requests)"
    ```python
    import requests
    
    response = requests.get("http://localhost:8000/api/v1/graphs")
    graphs = response.json()
    
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

Get just the Confluent lineage without any catalog enrichment (no UC tables, Glue tables, etc.). This is useful for exporting pure Kafka lineage to external systems.

**When you'd use this:** You want to push Kafka lineage to a data catalog that will do its own enrichment, or you only care about what's happening inside Confluent Cloud.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/graphs/confluent/view > confluent-lineage.json
    ```

=== "Python (httpx)"
    ```python
    import httpx
    import json
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
    # Get pure Confluent lineage (no catalog nodes)
    confluent_view = client.get("/graphs/confluent/view").json()
    
    events = confluent_view.get("events", [])
    print(f"Found {len(events)} OpenLineage events from Confluent")
    
    # Save for external consumption
    with open("confluent-lineage.json", "w") as f:
        json.dump(confluent_view, f, indent=2)
    ```

=== "Python (requests)"
    ```python
    import requests
    import json
    
    # Get pure Confluent lineage (no catalog nodes)
    response = requests.get("http://localhost:8000/api/v1/graphs/confluent/view")
    confluent_view = response.json()
    
    events = confluent_view.get("events", [])
    print(f"Found {len(events)} OpenLineage events from Confluent")
    
    # Save for external consumption
    with open("confluent-lineage.json", "w") as f:
        json.dump(confluent_view, f, indent=2)
    ```

**What's included:**
- Kafka topics
- Connectors (source and sink)
- ksqlDB queries
- Flink jobs
- Schemas
- Consumer groups

**What's excluded:**
- Unity Catalog tables
- AWS Glue tables
- Google BigQuery tables
- External datasets (unless they're connector sources)

### Enriched View

Get the full cross-platform lineage graph with all catalog enrichments. This is the "everything" view.

**When you'd use this:** You want to see data flows across all platforms — from Postgres → Kafka → Databricks → AWS Glue → Google BigQuery.

=== "cURL"
    ```bash
    # All systems
    curl http://localhost:8000/api/v1/graphs/enriched/view > full-lineage.json
    
    # Filter by systems
    curl "http://localhost:8000/api/v1/graphs/enriched/view?systems=confluent,databricks" > confluent-databricks.json
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
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

=== "Python (requests)"
    ```python
    import requests
    
    # Full cross-platform lineage
    response = requests.get("http://localhost:8000/api/v1/graphs/enriched/view")
    enriched_view = response.json()
    
    events = enriched_view.get("events", [])
    print(f"Total events: {len(events)}")
    
    # Filter by systems
    response = requests.get(
        "http://localhost:8000/api/v1/graphs/enriched/view",
        params={"systems": "confluent,databricks"}
    )
    databricks_view = response.json()
    
    print(f"Confluent+Databricks events: {len(databricks_view.get('events', []))}")
    ```

**What's included:**
- Everything from the Confluent-only view
- Unity Catalog tables (if configured)
- AWS Glue tables (if configured)
- Google BigQuery tables (if configured)
- MATERIALIZES edges connecting Kafka topics to catalog tables

**System filter values:** `confluent`, `databricks`, `aws`, `google`

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

Start an async lineage extraction from Confluent Cloud. This pulls topics, connectors, schemas, ksqlDB queries, and Flink jobs from your environments.

**When you'd use this:** You want to refresh the lineage graph on-demand (like after deploying new connectors), or you're building an automated pipeline that runs every hour.

=== "cURL"
    ```bash
    # Extract all environments
    curl -X POST http://localhost:8000/api/v1/tasks/extract
    # Response: {"task_id":"abc-123","status":"pending"}
    
    # Extract specific environments
    curl -X POST http://localhost:8000/api/v1/tasks/extract \
      -H "Content-Type: application/json" \
      -d '["env-abc", "env-xyz"]'
    ```

=== "Python (httpx)"
    ```python
    import httpx
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
    # Extract all environments
    response = client.post("/tasks/extract")
    task_id = response.json()["task_id"]
    print(f"Started extraction: {task_id}")
    
    # Extract specific environments
    response = client.post("/tasks/extract", json=["env-abc", "env-xyz"])
    task_id = response.json()["task_id"]
    ```

=== "Python (requests)"
    ```python
    import requests
    
    # Extract all environments
    response = requests.post("http://localhost:8000/api/v1/tasks/extract")
    task_id = response.json()["task_id"]
    print(f"Started extraction: {task_id}")
    
    # Extract specific environments
    response = requests.post(
        "http://localhost:8000/api/v1/tasks/extract",
        json=["env-abc", "env-xyz"]
    )
    task_id = response.json()["task_id"]
    ```

**Response:** You get a `task_id` immediately. The extraction runs in the background (typically 30-60 seconds). Poll the task endpoint to check status.

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

Check if your extraction or enrichment task is complete. Tasks go through states: `pending` → `running` → `completed` (or `failed`).

**When you'd use this:** You triggered an extraction and want to know when it's done so you can fetch the results.

=== "cURL"
    ```bash
    curl http://localhost:8000/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000
    # Response: {"task_id":"...","status":"completed","result":{...}}
    ```

=== "Python (httpx)"
    ```python
    import httpx
    import time
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
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

=== "Python (requests)"
    ```python
    import requests
    import time
    
    task_id = "550e8400-e29b-41d4-a716-446655440000"
    
    while True:
        response = requests.get(f"http://localhost:8000/api/v1/tasks/{task_id}")
        task = response.json()
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

**Tip:** Poll every 2-5 seconds. Don't hammer the API every 100ms — extractions take time!

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

**Real-world scenario:** You're a data engineer at an e-commerce company. You need to understand where your `main.analytics.order_summary` Unity Catalog table gets its data from. You know it reads from Kafka, but you don't know which topics or what transformations happen in between.

This example shows how to:

1. Extract lineage from Confluent Cloud
2. Wait for the extraction to finish
3. Find your Unity Catalog table
4. Trace it back to the source Kafka topics
5. Export the full lineage as OpenLineage events

=== "Python (httpx)"
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
    print(f"Task ID: {task_id}")
    
    # 2. Poll until complete
    while True:
        task = client.get(f"/tasks/{task_id}").json()
        status = task["status"]
        print(f"Status: {status}")
        
        if status == "completed":
            print("✓ Extraction complete!")
            break
        elif status == "failed":
            print(f"✗ Extraction failed: {task['error']}")
            exit(1)
        
        time.sleep(2)
    
    # 3. Find a UC table
    print("\nSearching for Unity Catalog tables...")
    uc_tables = client.get("/lineage/datasets", params={
        "namespace": "databricks://*"
    }).json()
    
    if not uc_tables:
        print("No UC tables found")
        exit(0)
    
    table = uc_tables[0]
    print(f"Found UC table: {table['name']}")
    
    # 4. Traverse upstream lineage
    print(f"\nTracing upstream lineage for {table['name']}...")
    lineage = client.get("/lineage/datasets/lineage", params={
        "namespace": table["namespace"],
        "name": table["name"],
        "direction": "upstream",
        "depth": 10
    }).json()
    
    print(f"\nUpstream lineage chain ({len(lineage.get('nodes', []))} nodes):")
    for node in lineage.get("nodes", []):
        print(f"  - {node['display_name']} ({node['node_type']})")
    
    # 5. Export as OpenLineage
    print("\nExporting full lineage graph...")
    enriched_view = client.get("/graphs/enriched/view").json()
    
    with open("lineage.json", "w") as f:
        json.dump(enriched_view, f, indent=2)
    
    print(f"✓ Exported {len(enriched_view.get('events', []))} OpenLineage events to lineage.json")
    print("\nYou can now:")
    print("  - Import lineage.json into your data catalog")
    print("  - Visualize it in Marquez or another OpenLineage tool")
    print("  - Share it with your team for impact analysis")
    ```

=== "Python (requests)"
    ```python
    import requests
    import time
    import json
    
    base_url = "http://localhost:8000/api/v1"
    
    # 1. Trigger extraction
    print("Starting extraction...")
    response = requests.post(f"{base_url}/tasks/extract")
    task_id = response.json()["task_id"]
    print(f"Task ID: {task_id}")
    
    # 2. Poll until complete
    while True:
        response = requests.get(f"{base_url}/tasks/{task_id}")
        task = response.json()
        status = task["status"]
        print(f"Status: {status}")
        
        if status == "completed":
            print("✓ Extraction complete!")
            break
        elif status == "failed":
            print(f"✗ Extraction failed: {task['error']}")
            exit(1)
        
        time.sleep(2)
    
    # 3. Find a UC table
    print("\nSearching for Unity Catalog tables...")
    response = requests.get(
        f"{base_url}/lineage/datasets",
        params={"namespace": "databricks://*"}
    )
    uc_tables = response.json()
    
    if not uc_tables:
        print("No UC tables found")
        exit(0)
    
    table = uc_tables[0]
    print(f"Found UC table: {table['name']}")
    
    # 4. Traverse upstream lineage
    print(f"\nTracing upstream lineage for {table['name']}...")
    response = requests.get(
        f"{base_url}/lineage/datasets/lineage",
        params={
            "namespace": table["namespace"],
            "name": table["name"],
            "direction": "upstream",
            "depth": 10
        }
    )
    lineage = response.json()
    
    print(f"\nUpstream lineage chain ({len(lineage.get('nodes', []))} nodes):")
    for node in lineage.get("nodes", []):
        print(f"  - {node['display_name']} ({node['node_type']})")
    
    # 5. Export as OpenLineage
    print("\nExporting full lineage graph...")
    response = requests.get(f"{base_url}/graphs/enriched/view")
    enriched_view = response.json()
    
    with open("lineage.json", "w") as f:
        json.dump(enriched_view, f, indent=2)
    
    print(f"✓ Exported {len(enriched_view.get('events', []))} OpenLineage events to lineage.json")
    ```

**Expected output:**

```
Starting extraction...
Task ID: 550e8400-e29b-41d4-a716-446655440000
Status: pending
Status: running
Status: completed
✓ Extraction complete!

Searching for Unity Catalog tables...
Found UC table: main.analytics.order_summary

Tracing upstream lineage for main.analytics.order_summary...

Upstream lineage chain (5 nodes):
  - main.analytics.order_summary (uc_table)
  - kafka-delta-sink (connector)
  - enriched_orders (kafka_topic)
  - order-enrichment-query (ksqldb_query)
  - raw_orders (kafka_topic)

✓ Exported 12 OpenLineage events to lineage.json
```

**What just happened?** You discovered that your UC table comes from a Kafka Delta Sink connector, which reads from `enriched_orders`. That topic is created by a ksqlDB query that transforms `raw_orders`. Now you know the full data flow!

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
