# OpenLineage Mapping

LineageBridge translates Confluent Cloud stream lineage into standard OpenLineage events, enabling integration with any OpenLineage-compatible data catalog.

## Why OpenLineage?

[OpenLineage](https://openlineage.io) is an open standard for data lineage metadata collection and analysis. It's like a common language that all data tools can speak.

**The problem without OpenLineage:** Every vendor has their own lineage format. Confluent uses one schema, Databricks uses another, AWS Glue uses yet another. If you want to combine lineage from all three, you need custom code for each integration.

**The solution with OpenLineage:** Everyone speaks the same language. LineageBridge translates Confluent lineage to OpenLineage, so any tool that understands OpenLineage can consume it.

Benefits:

- **Interoperability** - Works with any catalog that speaks OpenLineage (Databricks, AWS, Google, Marquez, etc.)
- **Standardization** - Uses industry-standard schemas instead of vendor-specific formats
- **Extensibility** - Easy to add new catalog integrations without changing core logic
- **Bidirectionality** - Both produce and consume lineage from external systems

**Real-world example:** Your team uses Confluent for streaming, Databricks for analytics, and dbt for transformations. With OpenLineage, all three systems can share lineage data, giving you end-to-end visibility from Kafka topics → Databricks tables → dbt models in a single graph.

## Translation Process

Here's how LineageBridge converts Confluent lineage to OpenLineage:

```mermaid
graph LR
    subgraph "Confluent Cloud"
        A[Kafka Topics]
        B[Connectors]
        C[ksqlDB Queries]
        D[Schemas]
    end
    
    subgraph "LineageBridge"
        E[Extract]
        F[Transform]
        G[Map to OpenLineage]
    end
    
    subgraph "OpenLineage Events"
        H[Datasets]
        I[Jobs]
        J[Runs]
        K[Facets]
    end
    
    A --> E
    B --> E
    C --> E
    D --> E
    
    E --> F
    F --> G
    
    G --> H
    G --> I
    G --> J
    G --> K
    
    subgraph "Catalog Consumers"
        L[Databricks UC]
        M[AWS Glue]
        N[Google Data Lineage]
        O[Marquez]
    end
    
    H --> L
    I --> L
    H --> M
    I --> M
    H --> N
    I --> N
    H --> O
    I --> O
```

**Step 1: Extract** - Pull metadata from Confluent Cloud APIs (topics, connectors, schemas, etc.)  
**Step 2: Transform** - Build a lineage graph with nodes and edges  
**Step 3: Map** - Convert nodes/edges to OpenLineage datasets, jobs, and events  
**Step 4: Publish** - Send OpenLineage events to data catalogs or export as JSON

## Concept Mapping

### Confluent to OpenLineage

| Confluent Concept | OpenLineage Type | Namespace Format | Description |
|-------------------|------------------|------------------|-------------|
| Kafka Topic | Dataset | `confluent://{env_id}/{cluster_id}` | A stream of events |
| Connector | Job | `confluent://{env_id}/{cluster_id}` | Imports/exports data |
| ksqlDB Query | Job | `confluent://{env_id}/{cluster_id}` | Stream transformation |
| Flink Job | Job | `confluent://{env_id}/{cluster_id}` | Stream processing |
| Consumer Group | Job | `confluent://{env_id}/{cluster_id}` | Topic consumption |
| Tableflow Table | Dataset | `confluent://{env_id}/{cluster_id}` | Logical table view |
| Schema | SchemaDatasetFacet | - | Attached to parent Dataset |

### Catalog Integration

External catalog assets also map to OpenLineage types:

| Catalog Asset | OpenLineage Type | Namespace Format | Description |
|---------------|------------------|------------------|-------------|
| UC Table | Dataset | `databricks://{workspace}` | Unity Catalog table |
| Glue Table | Dataset | `aws://{region}/{database}` | AWS Glue table |
| Google BQ Table | Dataset | `google://{project}/{dataset}` | BigQuery table |

## Edge Type Mapping

Lineage edges map to OpenLineage input/output relationships:

| LineageBridge Edge | OpenLineage Representation | Example |
|--------------------|---------------------------|---------|
| PRODUCES | OutputDataset on a Job | Connector produces to Kafka topic |
| CONSUMES | InputDataset on a Job | Consumer group consumes from topic |
| TRANSFORMS | Job with inputs and outputs | ksqlDB query transforms topic A to topic B |
| MATERIALIZES | OutputDataset (catalog) | Tableflow materializes topic to UC table |
| HAS_SCHEMA | SchemaDatasetFacet | Topic has Avro schema |
| MEMBER_OF | Logical grouping | Consumer instance in consumer group |

## Node ID Format

LineageBridge uses a consistent node ID format:

```
{system}:{type}:{env_id}:{qualified_name}
```

Examples:

- `confluent:kafka_topic:env-abc:lkc-123/orders`
- `databricks:uc_table:workspace-1:main.sales.orders`
- `aws:glue_table:us-west-2:my-database/orders`

When translating to OpenLineage, the node ID is split into:

- **Namespace**: `{system}://{env_id}/{cluster_id}` (for datasets/jobs)
- **Name**: `{qualified_name}`

## OpenLineage Facets

LineageBridge enriches OpenLineage events with custom facets:

### Dataset Facets

**ConfluentKafkaDatasetFacet** - Kafka-specific metadata

```json
{
  "facets": {
    "confluent_kafka": {
      "cluster_id": "lkc-12345",
      "environment_id": "env-abc",
      "partitions": 6,
      "replication_factor": 3,
      "tags": ["pii", "production"]
    }
  }
}
```

**SchemaDatasetFacet** - Schema information

```json
{
  "facets": {
    "schema": {
      "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "amount", "type": "decimal"}
      ]
    }
  }
}
```

**DataSourceDatasetFacet** - External source metadata

```json
{
  "facets": {
    "dataSource": {
      "name": "PostgreSQL",
      "uri": "jdbc:postgresql://db.example.com:5432/orders"
    }
  }
}
```

### Job Facets

**ConfluentConnectorJobFacet** - Connector-specific metadata

```json
{
  "facets": {
    "confluent_connector": {
      "connector_class": "PostgresCdcSource",
      "connector_type": "source",
      "cluster_id": "lkc-12345",
      "environment_id": "env-abc"
    }
  }
}
```

**SqlJobFacet** - SQL query text

```json
{
  "facets": {
    "sql": {
      "query": "CREATE STREAM enriched_orders AS SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id;"
    }
  }
}
```

## Translation Examples

### Kafka Topic to Dataset

LineageBridge node:

```json
{
  "node_id": "confluent:kafka_topic:env-abc:lkc-123/orders",
  "system": "confluent",
  "node_type": "kafka_topic",
  "qualified_name": "lkc-123/orders",
  "display_name": "orders",
  "attributes": {
    "partitions": 6,
    "replication_factor": 3
  }
}
```

OpenLineage Dataset:

```json
{
  "namespace": "confluent://env-abc/lkc-123",
  "name": "orders",
  "facets": {
    "confluent_kafka": {
      "cluster_id": "lkc-123",
      "environment_id": "env-abc",
      "partitions": 6,
      "replication_factor": 3
    }
  }
}
```

### Connector to Job

LineageBridge node:

```json
{
  "node_id": "confluent:connector:env-abc:lkc-123/postgres-cdc-source",
  "system": "confluent",
  "node_type": "connector",
  "qualified_name": "lkc-123/postgres-cdc-source",
  "display_name": "postgres-cdc-source",
  "attributes": {
    "connector_class": "PostgresCdcSource",
    "connector_type": "source"
  }
}
```

OpenLineage Job with RunEvent:

```json
{
  "eventTime": "2026-04-30T00:00:00Z",
  "eventType": "COMPLETE",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "confluent://env-abc/lkc-123",
    "name": "postgres-cdc-source",
    "facets": {
      "confluent_connector": {
        "connector_class": "PostgresCdcSource",
        "connector_type": "source",
        "cluster_id": "lkc-123",
        "environment_id": "env-abc"
      }
    }
  },
  "inputs": [
    {
      "namespace": "jdbc://db.example.com:5432",
      "name": "orders",
      "facets": {
        "dataSource": {
          "name": "PostgreSQL",
          "uri": "jdbc:postgresql://db.example.com:5432/orders"
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "confluent://env-abc/lkc-123",
      "name": "orders",
      "facets": {
        "confluent_kafka": {
          "cluster_id": "lkc-123",
          "environment_id": "env-abc"
        }
      }
    }
  ]
}
```

### ksqlDB Query to Job

LineageBridge edges:

```
orders (topic) --CONSUMES--> my-query (ksqldb_query)
my-query --PRODUCES--> enriched_orders (topic)
```

OpenLineage RunEvent:

```json
{
  "eventType": "COMPLETE",
  "job": {
    "namespace": "confluent://env-abc/lkc-123",
    "name": "my-query",
    "facets": {
      "sql": {
        "query": "CREATE STREAM enriched_orders AS SELECT * FROM orders WHERE amount > 100;"
      }
    }
  },
  "inputs": [
    {
      "namespace": "confluent://env-abc/lkc-123",
      "name": "orders"
    }
  ],
  "outputs": [
    {
      "namespace": "confluent://env-abc/lkc-123",
      "name": "enriched_orders"
    }
  ]
}
```

## Use Cases

### 1. Export Confluent Lineage to Unity Catalog

**Scenario:** Your data team uses Unity Catalog to track all tables, but they can't see what happens before data lands in Databricks. You want to show them the upstream Kafka topics and connectors.

**Solution:** Extract Confluent lineage and push it to Unity Catalog via OpenLineage.

=== "cURL"
    ```bash
    # Trigger extraction
    curl -X POST http://localhost:8000/api/v1/tasks/extract
    # Response: {"task_id":"abc-123"}
    
    # Wait for completion (or poll /tasks/abc-123)
    sleep 30
    
    # Query OpenLineage events
    curl http://localhost:8000/api/v1/lineage/events > confluent-lineage.json
    
    # UC can consume these events or use LineageBridge's push_lineage() provider method
    ```

=== "Python (httpx)"
    ```python
    import httpx
    import time
    import json
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
    # Trigger extraction
    response = client.post("/tasks/extract")
    task_id = response.json()["task_id"]
    
    # Wait for completion
    while True:
        task = client.get(f"/tasks/{task_id}").json()
        if task["status"] == "completed":
            break
        time.sleep(2)
    
    # Get OpenLineage events
    events = client.get("/lineage/events").json()
    
    # Save to file
    with open("confluent-lineage.json", "w") as f:
        json.dump(events, f, indent=2)
    
    print(f"Exported {len(events)} events")
    ```

=== "Python (requests)"
    ```python
    import requests
    import time
    import json
    
    base_url = "http://localhost:8000/api/v1"
    
    # Trigger extraction
    response = requests.post(f"{base_url}/tasks/extract")
    task_id = response.json()["task_id"]
    
    # Wait for completion
    while True:
        response = requests.get(f"{base_url}/tasks/{task_id}")
        if response.json()["status"] == "completed":
            break
        time.sleep(2)
    
    # Get OpenLineage events
    response = requests.get(f"{base_url}/lineage/events")
    events = response.json()
    
    # Save to file
    with open("confluent-lineage.json", "w") as f:
        json.dump(events, f, indent=2)
    
    print(f"Exported {len(events)} events")
    ```

**What you get:** A JSON file with OpenLineage events that Unity Catalog can ingest. Now your data analysts can trace a UC table back to its source Kafka topic!

### 2. Import External Lineage into LineageBridge

**Scenario:** You have a Databricks notebook that reads from a Kafka topic and writes to a Unity Catalog table. LineageBridge knows about the Kafka topic (from Confluent extraction), but it doesn't know about the notebook job. You want to connect the dots.

**Solution:** Send an OpenLineage event from your Databricks job to the LineageBridge API.

=== "cURL"
    ```bash
    curl -X POST http://localhost:8000/api/v1/lineage/events \
      -H "Content-Type: application/json" \
      -d '[{
        "eventTime": "2026-04-30T00:00:00Z",
        "eventType": "COMPLETE",
        "run": {"runId": "external-run-1"},
        "job": {
          "namespace": "databricks://workspace-1",
          "name": "etl-pipeline"
        },
        "inputs": [
          {
            "namespace": "confluent://env-abc/lkc-123",
            "name": "orders"
          }
        ],
        "outputs": [
          {
            "namespace": "databricks://workspace-1",
            "name": "main.sales.orders"
          }
        ]
      }]'
    ```

=== "Python (httpx)"
    ```python
    import httpx
    from datetime import datetime
    import uuid
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
    event = {
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "eventType": "COMPLETE",
        "run": {"runId": str(uuid.uuid4())},
        "job": {
            "namespace": "databricks://workspace-1",
            "name": "etl-pipeline",
            "facets": {
                "documentation": {
                    "description": "Reads from Kafka and writes to UC"
                }
            }
        },
        "inputs": [
            {
                "namespace": "confluent://env-abc/lkc-123",
                "name": "orders"
            }
        ],
        "outputs": [
            {
                "namespace": "databricks://workspace-1",
                "name": "main.sales.orders"
            }
        ]
    }
    
    response = client.post("/lineage/events", json=[event])
    print(response.json())
    ```

=== "Python (requests)"
    ```python
    import requests
    from datetime import datetime
    import uuid
    
    event = {
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "eventType": "COMPLETE",
        "run": {"runId": str(uuid.uuid4())},
        "job": {
            "namespace": "databricks://workspace-1",
            "name": "etl-pipeline"
        },
        "inputs": [
            {
                "namespace": "confluent://env-abc/lkc-123",
                "name": "orders"
            }
        ],
        "outputs": [
            {
                "namespace": "databricks://workspace-1",
                "name": "main.sales.orders"
            }
        ]
    }
    
    response = requests.post(
        "http://localhost:8000/api/v1/lineage/events",
        json=[event]
    )
    print(response.json())
    ```

**What you get:** LineageBridge now knows that `orders` (Kafka topic) flows through `etl-pipeline` (Databricks job) to `main.sales.orders` (UC table). The lineage graph is complete!

**Integration tip:** Add this code to the end of your Databricks notebook so lineage is sent automatically on every run.

### 3. Unified Lineage View

**Scenario:** Your data stack includes Confluent Cloud, Databricks, and AWS Glue. Data flows between all three platforms, but you can't see the full picture in any single tool.

**Solution:** Use the enriched view endpoint to get a unified lineage graph with all platforms combined.

=== "cURL"
    ```bash
    # Full enriched graph (Confluent + Databricks + AWS)
    curl "http://localhost:8000/api/v1/graphs/enriched/view" > unified-lineage.json
    
    # Filter by systems
    curl "http://localhost:8000/api/v1/graphs/enriched/view?systems=confluent,databricks" > kafka-to-databricks.json
    ```

=== "Python (httpx)"
    ```python
    import httpx
    import json
    
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    
    # Full enriched graph
    all_lineage = client.get("/graphs/enriched/view").json()
    
    print(f"Total events: {len(all_lineage.get('events', []))}")
    
    # Filter by systems (Confluent + Databricks only)
    filtered_lineage = client.get(
        "/graphs/enriched/view",
        params={"systems": "confluent,databricks"}
    ).json()
    
    print(f"Confluent + Databricks events: {len(filtered_lineage.get('events', []))}")
    
    # Save to file
    with open("unified-lineage.json", "w") as f:
        json.dump(all_lineage, f, indent=2)
    ```

=== "Python (requests)"
    ```python
    import requests
    import json
    
    base_url = "http://localhost:8000/api/v1"
    
    # Full enriched graph
    response = requests.get(f"{base_url}/graphs/enriched/view")
    all_lineage = response.json()
    
    print(f"Total events: {len(all_lineage.get('events', []))}")
    
    # Filter by systems
    response = requests.get(
        f"{base_url}/graphs/enriched/view",
        params={"systems": "confluent,databricks"}
    )
    filtered_lineage = response.json()
    
    print(f"Confluent + Databricks events: {len(filtered_lineage.get('events', []))}")
    
    # Save to file
    with open("unified-lineage.json", "w") as f:
        json.dump(all_lineage, f, indent=2)
    ```

**What you get:** A single OpenLineage JSON file with events from all platforms. Import it into Marquez, your data catalog, or a custom visualization tool to see the full end-to-end data flow.

**Example flow you can now trace:**  
Postgres table → Kafka topic (via connector) → ksqlDB transformation → Databricks Delta table → AWS Glue table (via Glue crawler)

### 4. Push to Google Data Lineage

**Scenario:** Your team uses Google Cloud Platform, and you want to see Kafka topic lineage in Google's Data Lineage UI alongside your BigQuery tables.

**Solution:** Export Confluent lineage from LineageBridge and push it to Google Data Lineage, which natively accepts OpenLineage events.

=== "cURL"
    ```bash
    # Export from LineageBridge
    curl http://localhost:8000/api/v1/graphs/confluent/view > lineage.json
    
    # Push to Google Data Lineage
    curl -X POST \
      "https://datalineage.googleapis.com/v1/projects/my-project/locations/us:processOpenLineageRunEvent" \
      -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      -H "Content-Type: application/json" \
      -d @lineage.json
    ```

=== "Python (httpx)"
    ```python
    import httpx
    import subprocess
    
    # Export from LineageBridge
    client = httpx.Client(base_url="http://localhost:8000/api/v1")
    lineage = client.get("/graphs/confluent/view").json()
    
    # Get Google Cloud access token
    token = subprocess.check_output(
        ["gcloud", "auth", "print-access-token"]
    ).decode().strip()
    
    # Push to Google Data Lineage
    google_client = httpx.Client()
    response = google_client.post(
        "https://datalineage.googleapis.com/v1/projects/my-project/locations/us:processOpenLineageRunEvent",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=lineage
    )
    
    print(f"Pushed to Google: {response.status_code}")
    ```

=== "Python (requests)"
    ```python
    import requests
    import subprocess
    
    # Export from LineageBridge
    response = requests.get("http://localhost:8000/api/v1/graphs/confluent/view")
    lineage = response.json()
    
    # Get Google Cloud access token
    token = subprocess.check_output(
        ["gcloud", "auth", "print-access-token"]
    ).decode().strip()
    
    # Push to Google Data Lineage
    response = requests.post(
        "https://datalineage.googleapis.com/v1/projects/my-project/locations/us:processOpenLineageRunEvent",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=lineage
    )
    
    print(f"Pushed to Google: {response.status_code}")
    ```

**What you get:** Your Kafka topics and connectors now appear in Google's Data Lineage UI. Click on a BigQuery table, and you can trace it back to the source Kafka topic.

**Automation tip:** Run this script hourly via Cloud Scheduler to keep Google Data Lineage in sync with your Confluent Cloud environment.

## OpenLineage Spec Compliance

LineageBridge implements **OpenLineage 1.0** with the following:

- **Core Types**: RunEvent, Dataset, Job, Run
- **Standard Facets**: schema, dataSource, documentation, sql
- **Custom Facets**: confluent_kafka, confluent_connector (vendor extensions)
- **Event Types**: START, RUNNING, COMPLETE, FAIL, ABORT

Full spec: https://openlineage.io/spec/2-0-2/OpenLineage.json

## Further Reading

- [OpenLineage Documentation](https://openlineage.io)
- [OpenLineage GitHub](https://github.com/OpenLineage/OpenLineage)
- [Marquez (OpenLineage Reference Implementation)](https://marquezproject.github.io/marquez/)
- [Great Expectations + OpenLineage](https://docs.greatexpectations.io/docs/integrations/lineage/openlineage/)
