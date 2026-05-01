# OpenLineage Mapping

LineageBridge translates Confluent Cloud stream lineage into standard OpenLineage events, enabling integration with any OpenLineage-compatible data catalog.

## Why OpenLineage?

[OpenLineage](https://openlineage.io) is an open standard for data lineage metadata collection and analysis. By mapping Confluent concepts to OpenLineage, LineageBridge enables:

- **Interoperability** - Works with any catalog that speaks OpenLineage (Databricks, AWS, Google, Marquez, etc.)
- **Standardization** - Uses industry-standard schemas instead of vendor-specific formats
- **Extensibility** - Easy to add new catalog integrations without changing core logic
- **Bidirectionality** - Both produce and consume lineage from external systems

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

Extract Confluent lineage and expose it as OpenLineage events:

```bash
# Trigger extraction
curl -X POST http://localhost:8000/api/v1/tasks/extract

# Query OpenLineage events
curl http://localhost:8000/api/v1/lineage/events > confluent-lineage.json

# UC can consume these events or use LineageBridge's push_lineage() provider method
```

### 2. Import External Lineage into LineageBridge

Ingest OpenLineage events from external systems:

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

### 3. Unified Lineage View

Query the enriched view to see cross-platform lineage:

```bash
# Full enriched graph (Confluent + Databricks + AWS)
curl "http://localhost:8000/api/v1/graphs/enriched/view"

# Filter by systems
curl "http://localhost:8000/api/v1/graphs/enriched/view?systems=confluent,databricks"
```

### 4. Push to Google Data Lineage

Google Data Lineage natively accepts OpenLineage events:

```bash
# Export from LineageBridge
curl http://localhost:8000/api/v1/graphs/confluent/view > lineage.json

# Push to Google
curl -X POST \
  "https://datalineage.googleapis.com/v1/projects/my-project/locations/us:processOpenLineageRunEvent" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d @lineage.json
```

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
