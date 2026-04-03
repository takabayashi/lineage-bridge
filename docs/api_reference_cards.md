# Confluent Cloud API Reference Cards for LineageBridge

> Generated for the LineageBridge POC. Covers 8 API surfaces needed to extract
> stream lineage from Confluent Cloud. Each card documents what downstream
> builder agents need to implement extractors.

---

## Table of Contents

1. [Kafka REST Admin v3](#1-kafka-rest-admin-v3)
2. [Stream Catalog (Stream Governance)](#2-stream-catalog-stream-governance)
3. [Schema Registry](#3-schema-registry)
4. [Connect API](#4-connect-api)
5. [ksqlDB](#5-ksqldb)
6. [Flink on Confluent Cloud](#6-flink-on-confluent-cloud)
7. [Tableflow APIs](#7-tableflow-apis)
8. [Metrics API](#8-metrics-api)
9. [Credential Matrix](#9-credential-matrix)

---

## 1. Kafka REST Admin v3

### Base URL

```
https://<cluster-rest-endpoint>/kafka/v3/clusters/<cluster-id>
```

The cluster REST endpoint is region-specific and found in the Confluent Cloud
cluster settings. Format:

```
https://<cluster-id>.<region>.<cloud>.confluent.cloud:443
```

Alternative via the global API (for cluster enumeration):

```
https://api.confluent.cloud/cmk/v2/clusters?environment=<env-id>
```

### Authentication

- **API key type:** Cluster-scoped API key (Kafka API key)
- **Auth method:** HTTP Basic Auth
  - Username = Kafka API key
  - Password = Kafka API secret
- For cluster enumeration via `api.confluent.cloud`: Cloud API key (Basic Auth)

### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/topics` | List all topics in cluster | Topic inventory (nodes) |
| `GET` | `/topics/{topic_name}` | Describe topic | Topic metadata, partition count, replication |
| `GET` | `/topics/{topic_name}/configs` | Get topic configs | Retention, cleanup.policy, etc. |
| `GET` | `/consumer-groups` | List all consumer groups | Consumer group nodes |
| `GET` | `/consumer-groups/{group_id}` | Describe consumer group | Members, assignment state |
| `GET` | `/consumer-groups/{group_id}/lags` | Consumer lag per partition | Runtime edges: group reads from topic |
| `GET` | `/acls` | List ACLs | Access control edges: who reads/writes which topics |

#### List Topics - Request

```
GET /kafka/v3/clusters/{cluster_id}/topics
```

#### List Topics - Key Response Fields

```json
{
  "kind": "KafkaTopicList",
  "metadata": { "self": "...", "next": "..." },
  "data": [
    {
      "kind": "KafkaTopic",
      "metadata": { "self": "...", "resource_name": "..." },
      "cluster_id": "lkc-abc123",
      "topic_name": "orders",
      "is_internal": false,
      "replication_factor": 3,
      "partitions_count": 6,
      "partitions": { "related": "..." },
      "configs": { "related": "..." },
      "partition_reassignments": { "related": "..." },
      "authorized_operations": []
    }
  ]
}
```

#### Consumer Group Lag - Key Response Fields

```json
{
  "kind": "KafkaConsumerLagList",
  "data": [
    {
      "cluster_id": "lkc-abc123",
      "consumer_group_id": "my-consumer-group",
      "topic_name": "orders",
      "partition_id": 0,
      "current_offset": 100,
      "log_end_offset": 150,
      "lag": 50,
      "consumer_id": "consumer-1-...",
      "instance_id": null,
      "client_id": "my-app"
    }
  ]
}
```

#### ACLs - Key Response Fields

```json
{
  "kind": "KafkaAclList",
  "data": [
    {
      "cluster_id": "lkc-abc123",
      "resource_type": "TOPIC",
      "resource_name": "orders",
      "pattern_type": "LITERAL",
      "principal": "User:sa-abc123",
      "host": "*",
      "operation": "READ",
      "permission": "ALLOW"
    }
  ]
}
```

### Pagination

- **Pattern:** Link-based. Response `metadata` contains `next` URL.
- For topics: typically no pagination needed (all returned at once).
- For consumer groups/ACLs: follow `metadata.next` if present.

### Rate Limits

- Confluent Cloud applies per-cluster rate limits (not publicly documented as
  exact numbers). Kafka REST v3 shares the cluster's request budget.
- Recommended: 1-2 requests/second per endpoint for bulk extraction.
- HTTP 429 indicates rate limiting; honor `Retry-After` header.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid or expired Kafka API key | No - fix credentials |
| 403 | Insufficient ACL permissions | No - fix permissions |
| 404 | Cluster or resource not found | No |
| 429 | Rate limited | Yes - exponential backoff, honor Retry-After |
| 5xx | Server error | Yes - exponential backoff |

### Lineage Signals

- **Nodes:** `kafka_topic`, `consumer_group`
- **Edges:**
  - `consumer_group --reads--> kafka_topic` (from consumer group lag)
  - `principal --acl(READ|WRITE)--> kafka_topic` (from ACLs)
- **Enrichment:** Topic configs (retention, cleanup.policy) as node attributes

---

## 2. Stream Catalog (Stream Governance)

### Base URL

```
https://<schema-registry-endpoint>/catalog/v1
```

The Schema Registry endpoint doubles as the Stream Catalog endpoint. Found in
the environment's Stream Governance cluster settings. Format:

```
https://psrc-<id>.<region>.<cloud>.confluent.cloud
```

### Authentication

- **API key type:** Schema Registry API key (cluster-scoped to the Schema Registry cluster)
- **Auth method:** HTTP Basic Auth
  - Username = Schema Registry API key
  - Password = Schema Registry API secret

### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/catalog/v1/search/basic` | Search entities by query | Find all topics, connectors, schemas |
| `GET` | `/catalog/v1/entity` | Get entity by qualified name | Detailed entity metadata |
| `GET` | `/catalog/v1/entity/type/{typeName}` | List entities by type | Enumerate all entities of a given type |
| `GET` | `/catalog/v1/types/tagdefs` | List tag definitions | Available governance tags |
| `GET` | `/catalog/v1/entity/tags` | Get tags on entity | PII, data-product classification |
| `GET` | `/catalog/v1/entity/businessmetadata` | Get business metadata | Ownership, domain info |

#### Entity Types (typeName values)

- `kafka_topic` - Kafka topics
- `sr_schema` - Schemas in Schema Registry
- `kafka_connector` - Connectors (managed/custom)
- `kafka_cluster` - Kafka clusters
- `kafka_environment` - Environments
- `flink_compute_pool` - Flink compute pools
- `kafka_connect_pipeline` - Streaming pipelines

#### Search Entities - Request

```
GET /catalog/v1/search/basic?type=kafka_topic&attrs=owner,description
```

Query parameters:
- `type` (optional): Entity type to filter
- `query` (optional): Search text (Atlas DSL)
- `limit` (optional): Max results per page (default 100)
- `offset` (optional): Pagination offset
- `attrs` (optional): Additional attributes to return

#### Search Entities - Key Response Fields

```json
{
  "searchResults": [
    {
      "typeName": "kafka_topic",
      "attributes": {
        "qualifiedName": "lsrc-abc123:lkc-xyz789:orders",
        "name": "orders",
        "owner": "data-team",
        "description": "Customer orders topic",
        "replicationFactor": 3,
        "partitionCount": 6
      },
      "classificationNames": ["PII", "Sensitive"],
      "labels": [],
      "status": "ACTIVE"
    }
  ]
}
```

#### Entity by Qualified Name - Request

```
GET /catalog/v1/entity?type=kafka_topic&qualifiedName=lsrc-abc123:lkc-xyz789:orders
```

#### Entity by Qualified Name - Key Response Fields

```json
{
  "entity": {
    "typeName": "kafka_topic",
    "attributes": {
      "qualifiedName": "lsrc-abc123:lkc-xyz789:orders",
      "name": "orders",
      "owner": "data-team",
      "description": "...",
      "replicationFactor": 3,
      "partitionCount": 6
    },
    "classifications": [
      { "typeName": "PII", "propagate": true }
    ],
    "relationshipAttributes": {
      "schema": [
        { "typeName": "sr_schema", "qualifiedName": "lsrc-abc123:orders-value:1" }
      ]
    }
  }
}
```

### Qualified Name Format

The qualified name is the primary key for entity resolution. Format:

| Entity Type | Qualified Name Format |
|------------|----------------------|
| `kafka_topic` | `<sr-cluster-id>:<kafka-cluster-id>:<topic-name>` |
| `sr_schema` | `<sr-cluster-id>:<subject-name>:<version>` |
| `kafka_connector` | `<sr-cluster-id>:<kafka-cluster-id>:<connector-name>` |
| `kafka_cluster` | `<sr-cluster-id>:<kafka-cluster-id>` |

### Pagination

- **Pattern:** Offset-based
- Parameters: `limit` (default 100, max 1000) and `offset`
- Loop: increment `offset` by `limit` until fewer than `limit` results returned

### Rate Limits

- Shares the Schema Registry cluster's rate limit budget.
- Recommended: 5-10 requests/second for catalog queries.
- HTTP 429 with `Retry-After` header.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid SR API key | No |
| 403 | Insufficient permissions | No |
| 404 | Entity or type not found | No |
| 422 | Invalid query syntax | No - fix query |
| 429 | Rate limited | Yes - backoff |
| 5xx | Server error | Yes - backoff |

### Lineage Signals

- **Nodes:** `kafka_topic`, `sr_schema`, `kafka_connector`, `kafka_cluster`, `flink_compute_pool`
- **Edges:**
  - `kafka_topic <--schema--> sr_schema` (via `relationshipAttributes.schema`)
  - `kafka_connector <--produces/consumes--> kafka_topic` (via connector entity relationships)
  - `kafka_topic --tagged--> tag` (via classifications for data products, PII)
- **Enrichment:** Business metadata (owner, description, tags, classifications) as node attributes.
  This is the **semantic graph** -- the richest source of topology metadata.

---

## 3. Schema Registry

### Base URL

```
https://<schema-registry-endpoint>
```

Same endpoint as Stream Catalog:

```
https://psrc-<id>.<region>.<cloud>.confluent.cloud
```

### Authentication

- **API key type:** Schema Registry API key (cluster-scoped)
- **Auth method:** HTTP Basic Auth
  - Username = Schema Registry API key
  - Password = Schema Registry API secret

### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/subjects` | List all subjects | Schema inventory |
| `GET` | `/subjects/{subject}/versions` | List versions of a subject | Schema evolution history |
| `GET` | `/subjects/{subject}/versions/{version}` | Get specific schema version | Schema content, references |
| `GET` | `/subjects/{subject}/versions/{version}/referencedby` | Schemas referencing this one | Schema dependency edges |
| `GET` | `/schemas/ids/{id}` | Get schema by global ID | Resolve schema references |
| `GET` | `/schemas/types` | List supported schema types | AVRO, PROTOBUF, JSON |
| `GET` | `/config/{subject}` | Get compatibility config | Compatibility mode per subject |
| `GET` | `/mode` | Get SR mode | READWRITE, READONLY, IMPORT |

#### Subject Naming Convention

Subjects follow the `TopicNameStrategy` by default:

- `<topic-name>-key` -- key schema
- `<topic-name>-value` -- value schema

This provides a deterministic mapping: `subject_name -> topic_name`.

#### List Subjects - Response

```json
["orders-value", "orders-key", "users-value", "payments-value"]
```

#### Get Schema Version - Key Response Fields

```json
{
  "subject": "orders-value",
  "version": 3,
  "id": 100042,
  "schemaType": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[...]}",
  "references": [
    {
      "name": "com.example.Customer",
      "subject": "customer-value",
      "version": 1
    }
  ]
}
```

#### Referenced By - Response

```json
[100043, 100044]
```

These are global schema IDs that reference this schema. Resolve each with
`/schemas/ids/{id}` to find the referencing subjects/topics.

### Pagination

- **Pattern:** None for most endpoints; all results returned at once.
- `/subjects` returns all subjects in a single array.
- For very large registries (10k+ subjects), use `subjectPrefix` query param
  to filter: `GET /subjects?subjectPrefix=orders`

### Rate Limits

- Schema Registry has per-cluster rate limits (shared with Stream Catalog).
- Recommended: 10-20 requests/second.
- HTTP 429 with `Retry-After`.

### Error Handling

| Code | Meaning | Error Code | Retry? |
|------|---------|-----------|--------|
| 401 | Invalid credentials | 401xx | No |
| 404 | Subject or version not found | 40401, 40402 | No |
| 409 | Incompatible schema | 409 | No |
| 422 | Invalid schema | 42201 | No |
| 429 | Rate limited | - | Yes |
| 5xx | Server error | 50001-50003 | Yes |

SR returns structured error responses:

```json
{
  "error_code": 40401,
  "message": "Subject 'orders-value' not found."
}
```

### Lineage Signals

- **Nodes:** `schema` (per subject-version)
- **Edges:**
  - `schema --version_of--> schema` (version chain within a subject)
  - `schema --references--> schema` (cross-subject references via `references` field)
  - `schema --used_by--> kafka_topic` (via subject naming convention)
- **Enrichment:** Schema content for column-level lineage, `schemaType`, compatibility mode

---

## 4. Connect API

There are two API surfaces for connectors:

### 4a. Confluent Cloud Connect v1 API (Control Plane)

#### Base URL

```
https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}
```

#### Authentication

- **API key type:** Cloud API key
- **Auth method:** HTTP Basic Auth
  - Username = Cloud API key
  - Password = Cloud API secret

#### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/connectors` | List connector names | Connector inventory |
| `GET` | `/connectors/{name}` | Get connector details | Config, status, type |
| `GET` | `/connectors/{name}/config` | Get connector config | Source/sink topics, external systems |
| `GET` | `/connectors/{name}/status` | Get connector status | Running state |
| `GET` | `/connector-plugins` | List available plugins | Available connector types |

### 4b. Connect REST API (Data Plane)

#### Base URL

```
https://<connect-rest-endpoint>/connectors
```

The Connect REST endpoint is available per Kafka cluster. This is essentially
the same endpoint set but accessed via the cluster's own REST proxy.

#### Authentication

- **API key type:** Cluster-scoped Kafka API key
- **Auth method:** HTTP Basic Auth

### Connector Config -- Key Fields for Lineage

#### Source Connector

```json
{
  "name": "postgres-source-orders",
  "config": {
    "connector.class": "PostgresSource",
    "kafka.api.key": "...",
    "kafka.api.secret": "...",
    "connection.host": "db.example.com",
    "connection.port": "5432",
    "connection.user": "...",
    "db.name": "ecommerce",
    "table.include.list": "public.orders,public.customers",
    "topic.prefix": "pg-",
    "output.data.format": "AVRO",
    "tasks.max": "1"
  },
  "type": "source"
}
```

**Lineage extraction:** A source connector creates edges from an external system
to Kafka topics.
- External node: `db.example.com/ecommerce/public.orders`
- Kafka topic: `pg-public.orders` (derived from `topic.prefix` + table name)
- Edge: `external_dataset --source_connector--> kafka_topic`

#### Sink Connector

```json
{
  "name": "s3-sink-orders",
  "config": {
    "connector.class": "S3_SINK",
    "kafka.api.key": "...",
    "topics": "orders,payments",
    "s3.bucket.name": "my-data-lake",
    "s3.region": "us-east-1",
    "output.data.format": "PARQUET",
    "store.kafka.keys": "true",
    "tasks.max": "2"
  },
  "type": "sink"
}
```

**Lineage extraction:** A sink connector creates edges from Kafka topics to an
external system.
- Kafka topics: `orders`, `payments` (from `topics` config)
- External node: `s3://my-data-lake/`
- Edge: `kafka_topic --sink_connector--> external_dataset`

### Common Config Fields by Connector Class

| Field | Connector Classes | Lineage Meaning |
|-------|------------------|-----------------|
| `topics` | All sinks | Input topics |
| `topic.prefix` | Most sources | Output topic prefix |
| `table.include.list` | JDBC, Debezium | Source tables |
| `connection.host` | JDBC, Debezium | External system |
| `db.name` / `database` | JDBC, Debezium | External database |
| `s3.bucket.name` | S3 sink | Target S3 bucket |
| `connector.class` | All | Connector type identifier |
| `output.data.format` | All | AVRO/JSON/PROTOBUF |

### Pagination

- **Pattern:** None. `/connectors` returns all connector names as a JSON array.
- Individual connector details must be fetched one-by-one.
- For `expand=status` or `expand=info`: `GET /connectors?expand=status&expand=info`
  returns all connectors with full details in one call.

### Rate Limits

- Cloud API (`api.confluent.cloud`): Shared global rate limits.
- Recommended: 5 requests/second.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid Cloud API key | No |
| 403 | Insufficient permissions | No |
| 404 | Connector or cluster not found | No |
| 409 | Connector already exists (on create) | No |
| 429 | Rate limited | Yes |
| 5xx | Server error | Yes |

### Lineage Signals

- **Nodes:** `connector` (with type: source or sink), `external_dataset`
- **Edges:**
  - Source: `external_dataset --source_connector--> kafka_topic`
  - Sink: `kafka_topic --sink_connector--> external_dataset`
- **Key extraction logic:** Parse `connector.class` to determine direction,
  then extract external system details and topic names from config fields.
  Config field names vary by connector class -- a plugin-aware parser is needed.

---

## 5. ksqlDB

### Two API Surfaces

#### 5a. ksqlDB Cluster API (Control Plane)

##### Base URL

```
https://api.confluent.cloud/ksqldbcm/v2/clusters
```

##### Authentication

- **API key type:** Cloud API key
- **Auth method:** HTTP Basic Auth

##### Key Endpoints

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/ksqldbcm/v2/clusters?environment={env_id}` | List ksqlDB clusters | Discover ksqlDB instances |
| `GET` | `/ksqldbcm/v2/clusters/{id}` | Get ksqlDB cluster | Get HTTP endpoint for data plane |

##### Cluster Detail - Key Response Fields

```json
{
  "api_version": "ksqldbcm/v2",
  "id": "lksqlc-abc123",
  "spec": {
    "display_name": "my-ksqldb",
    "use_detailed_processing_log": true,
    "kafka_cluster": { "id": "lkc-xyz789" },
    "environment": { "id": "env-abc123" },
    "http_endpoint": "https://pksqlc-abc123.us-east-1.aws.confluent.cloud"
  },
  "status": {
    "phase": "PROVISIONED",
    "http_endpoint": "https://pksqlc-abc123.us-east-1.aws.confluent.cloud"
  }
}
```

#### 5b. ksqlDB Data-Plane REST API

##### Base URL

```
https://<ksqldb-http-endpoint>
```

Obtained from the cluster API's `status.http_endpoint` field.

##### Authentication

- **API key type:** ksqlDB-scoped API key (cluster-scoped to the ksqlDB cluster)
- **Auth method:** HTTP Basic Auth
  - Username = ksqlDB API key
  - Password = ksqlDB API secret

##### Key Endpoints for Lineage

All queries are submitted via POST to `/ksql`:

| Method | Path | ksql Statement | Lineage Use |
|--------|------|---------------|-------------|
| `POST` | `/ksql` | `SHOW QUERIES;` | List persistent queries |
| `POST` | `/ksql` | `SHOW STREAMS;` | List streams |
| `POST` | `/ksql` | `SHOW TABLES;` | List tables |
| `POST` | `/ksql` | `DESCRIBE <stream\|table> EXTENDED;` | Input/output topics, query text |
| `POST` | `/ksql` | `EXPLAIN <query_id>;` | Query execution plan |

##### Request Format

```json
{
  "ksql": "SHOW QUERIES;",
  "streamsProperties": {}
}
```

##### DESCRIBE EXTENDED - Key Response Fields

```json
[
  {
    "@type": "sourceDescription",
    "statementText": "DESCRIBE orders_enriched EXTENDED;",
    "sourceDescription": {
      "name": "ORDERS_ENRICHED",
      "type": "STREAM",
      "topic": "orders_enriched",
      "keyFormat": "KAFKA",
      "valueFormat": "AVRO",
      "readQueries": [
        { "id": "CSAS_ORDERS_ENRICHED_0", "sinks": ["orders_enriched"], "sinkTopic": "orders_enriched" }
      ],
      "writeQueries": [
        { "id": "CTAS_DOWNSTREAM_0", "sinks": ["downstream_output"], "sinkTopic": "downstream_output" }
      ],
      "fields": [
        { "name": "ORDER_ID", "schema": { "type": "INTEGER" } },
        { "name": "CUSTOMER_NAME", "schema": { "type": "STRING" } }
      ],
      "sourceConstraints": [],
      "statement": "CREATE STREAM orders_enriched AS SELECT o.order_id, c.name AS customer_name FROM orders o JOIN customers c ON o.customer_id = c.id EMIT CHANGES;"
    }
  }
]
```

##### SHOW QUERIES - Key Response Fields

```json
[
  {
    "@type": "queries",
    "queries": [
      {
        "id": "CSAS_ORDERS_ENRICHED_0",
        "queryString": "CREATE STREAM orders_enriched AS SELECT ... FROM orders JOIN customers ...",
        "sinks": ["ORDERS_ENRICHED"],
        "sinkKafkaTopics": ["orders_enriched"],
        "state": "RUNNING"
      }
    ]
  }
]
```

### Pagination

- **Pattern:** None. All results returned in a single response.
- Queries, streams, and tables are typically small in number.

### Rate Limits

- ksqlDB has per-cluster rate limits.
- Data-plane REST is rate limited by the ksqlDB cluster's compute capacity.
- Recommended: 1-2 requests/second for metadata queries.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid ksqlDB API key | No |
| 403 | Insufficient permissions | No |
| 400 | Invalid ksql statement | No |
| 429 | Rate limited | Yes |
| 5xx | Server/cluster error | Yes |

ksqlDB returns structured errors:

```json
{
  "@type": "currentStatus",
  "errorMessage": { "message": "..." }
}
```

### Lineage Signals

- **Nodes:** `ksqldb_query` (persistent query), ksqlDB streams/tables
- **Edges:**
  - `input_topic(s) --ksqldb_query--> output_topic(s)`
  - Derived from `DESCRIBE EXTENDED` and `SHOW QUERIES`:
    - `readQueries` shows which queries read from this stream/table
    - `writeQueries` shows which queries write to this stream/table
    - `sinkKafkaTopics` shows output Kafka topics
  - Parse the SQL `statement` for `FROM`, `JOIN` (inputs) and `INTO`,
    `INSERT INTO` (outputs)

---

## 6. Flink on Confluent Cloud

### Two API Surfaces

#### 6a. Flink Compute Pool API (Control Plane)

##### Base URL

```
https://api.confluent.cloud/fcpm/v2/compute-pools
```

##### Authentication

- **API key type:** Cloud API key
- **Auth method:** HTTP Basic Auth

##### Key Endpoints

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/fcpm/v2/compute-pools?environment={env_id}` | List compute pools | Discover Flink resources |
| `GET` | `/fcpm/v2/compute-pools/{id}` | Get compute pool | Pool details, region |

##### Compute Pool - Key Response Fields

```json
{
  "api_version": "fcpm/v2",
  "id": "lfcp-abc123",
  "spec": {
    "display_name": "my-flink-pool",
    "cloud": "AWS",
    "region": "us-east-1",
    "max_cfu": 10,
    "environment": { "id": "env-abc123" }
  },
  "status": {
    "phase": "PROVISIONED",
    "current_cfu": 5
  }
}
```

#### 6b. Flink Statement / Table API (Data Plane)

##### Base URL

```
https://flink.{region}.{cloud}.confluent.cloud
```

Example: `https://flink.us-east-1.aws.confluent.cloud`

##### Authentication

- **API key type:** Flink API key (organization-scoped or environment-scoped)
- **Auth method:** HTTP Basic Auth
  - Username = Flink API key
  - Password = Flink API secret

##### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/sql/v1/organizations/{org_id}/environments/{env_id}/statements` | List statements | Discover Flink SQL jobs |
| `GET` | `/sql/v1/organizations/{org_id}/environments/{env_id}/statements/{statement_name}` | Get statement detail | SQL text, status |
| `POST` | `/sql/v1/organizations/{org_id}/environments/{env_id}/statements` | Submit statement | Can submit metadata queries |

##### List Statements - Key Response Fields

```json
{
  "api_version": "sql/v1",
  "kind": "StatementList",
  "metadata": { "next": "..." },
  "data": [
    {
      "name": "my-flink-job",
      "spec": {
        "statement": "INSERT INTO enriched_orders SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        "compute_pool": { "id": "lfcp-abc123" },
        "principal": "sa-abc123",
        "properties": {
          "sql.current-catalog": "my-environment",
          "sql.current-database": "my-kafka-cluster"
        }
      },
      "status": {
        "phase": "RUNNING",
        "detail": "..."
      }
    }
  ]
}
```

##### Extracting Lineage from Flink SQL

Parse the `spec.statement` SQL text for:

- **Source tables:** `FROM <table>`, `JOIN <table>` -- maps to Kafka topics
- **Sink tables:** `INSERT INTO <table>` -- maps to Kafka topics
- **Catalog/database context:** `sql.current-catalog` and `sql.current-database`
  in properties establish the namespace

Flink tables map to Kafka topics via the `kafka` connector:
- Catalog = environment name
- Database = Kafka cluster name
- Table = topic name

### Pagination

- **Statements API:** Link-based. Follow `metadata.next`.
- Default page size: 10. Use `page_size` query parameter (max 100).

### Rate Limits

- Flink API has per-organization rate limits.
- Recommended: 2-5 requests/second.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid Flink API key | No |
| 403 | Insufficient permissions | No |
| 404 | Statement or pool not found | No |
| 409 | Statement name conflict | No |
| 429 | Rate limited | Yes |
| 5xx | Server error | Yes |

### Lineage Signals

- **Nodes:** `flink_job` (statement), `flink_compute_pool`
- **Edges:**
  - `source_topic/table --flink_job--> sink_topic/table`
  - Derived from parsing Flink SQL in `spec.statement`
- **Key extraction logic:** Parse SQL for FROM/JOIN (sources) and INSERT INTO
  (sinks). Map table references to Kafka topics using the catalog/database context.

---

## 7. Tableflow APIs

### Base URL

```
https://api.confluent.cloud/tableflow/v1
```

### Authentication

- **API key type:** Cloud API key
- **Auth method:** HTTP Basic Auth

### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `GET` | `/tableflow/v1/tableflow-topics?environment={env_id}` | List TableflowTopics | Discover topic-to-table mappings |
| `GET` | `/tableflow/v1/tableflow-topics/{id}` | Get TableflowTopic | Detailed mapping config |
| `GET` | `/tableflow/v1/catalog-integrations?environment={env_id}` | List CatalogIntegrations | Discover catalog sync targets |
| `GET` | `/tableflow/v1/catalog-integrations/{id}` | Get CatalogIntegration | UC/Glue config details |

#### TableflowTopic - Key Response Fields

```json
{
  "api_version": "tableflow/v1",
  "id": "tft-abc123",
  "spec": {
    "display_name": "orders-tableflow",
    "kafka_cluster": { "id": "lkc-xyz789" },
    "topic_name": "orders",
    "environment": { "id": "env-abc123" },
    "table_formats": ["DELTA", "ICEBERG"],
    "storage": {
      "kind": "ByobStorage",
      "bucket_name": "my-tableflow-bucket",
      "provider": "AWS"
    },
    "record_failure_strategy": "SUSPEND"
  },
  "status": {
    "phase": "ACTIVE",
    "storage_location": "s3://my-tableflow-bucket/lkc-xyz789/orders/",
    "detail": "..."
  }
}
```

#### CatalogIntegration - Key Response Fields

```json
{
  "api_version": "tableflow/v1",
  "id": "tci-abc123",
  "spec": {
    "display_name": "unity-catalog-integration",
    "kafka_cluster": { "id": "lkc-xyz789" },
    "environment": { "id": "env-abc123" },
    "catalog_type": "UNITY_CATALOG",
    "catalog_config": {
      "unity_catalog": {
        "workspace_url": "https://my-workspace.cloud.databricks.com",
        "catalog_name": "confluent_tableflow",
        "credentials": { "kind": "..." }
      }
    },
    "suspended": false
  },
  "status": {
    "phase": "ACTIVE"
  }
}
```

### Structural Mappings (Deterministic)

Tableflow provides deterministic mappings:

```
env + cluster_id + topic_name --> Iceberg/Delta table + storage path
                              --> UC catalog.schema.table
```

For Unity Catalog:
- **Schema name** = Kafka cluster ID (e.g., `lkc-xyz789`)
- **Table name** = topic name (e.g., `orders`)
- **Full path** = `<catalog_name>.<cluster_id>.<topic_name>`

For AWS Glue:
- **Database name** = cluster ID
- **Table name** = topic name

### Pagination

- **Pattern:** Page-token-based.
- Response `metadata` includes `next` link.
- Default page size: 10. Use `page_size` query parameter (max 100).

### Rate Limits

- Shared Cloud API rate limits.
- Recommended: 5 requests/second.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 401 | Invalid Cloud API key | No |
| 403 | Insufficient permissions | No |
| 404 | Resource not found | No |
| 429 | Rate limited | Yes |
| 5xx | Server error | Yes |

### Lineage Signals

- **Nodes:** `tableflow_table`, `uc_table` (derived), `external_dataset` (storage)
- **Edges (deterministic):**
  - `kafka_topic --tableflow--> tableflow_table`
  - `tableflow_table --catalog_integration--> uc_table`
- **Enrichment:** Table formats (Delta/Iceberg), storage location, catalog config
- **This is the authoritative mapping** between Kafka topics and UC tables.

---

## 8. Metrics API

### Base URL

```
https://api.telemetry.confluent.cloud/v2/metrics/{dataset}/query
```

Datasets:
- `cloud` -- general Confluent Cloud metrics

### Authentication

- **API key type:** Cloud API key (with MetricsViewer role or higher)
- **Auth method:** HTTP Basic Auth

### Key Endpoints for Lineage

| Method | Path | Description | Lineage Use |
|--------|------|-------------|-------------|
| `POST` | `/v2/metrics/cloud/query` | Query metrics | Throughput, lag, activity |
| `GET` | `/v2/metrics/cloud/descriptors/metrics` | List available metrics | Discover metric names |
| `GET` | `/v2/metrics/cloud/descriptors/resources` | List resource types | Available resource dimensions |

### Query Request Format

```json
{
  "aggregations": [
    { "metric": "io.confluent.kafka.server/received_bytes", "agg": "SUM" }
  ],
  "filter": {
    "op": "AND",
    "filters": [
      { "field": "resource.kafka.id", "op": "EQ", "value": "lkc-xyz789" },
      { "field": "metric.topic", "op": "EQ", "value": "orders" }
    ]
  },
  "granularity": "PT1H",
  "intervals": ["2024-01-01T00:00:00Z/2024-01-02T00:00:00Z"],
  "group_by": ["metric.topic"],
  "limit": 1000
}
```

### Key Metrics for Lineage

| Metric Name | Description | Lineage Use |
|-------------|-------------|-------------|
| `io.confluent.kafka.server/received_bytes` | Bytes produced to topic | Topic activity (is it alive?) |
| `io.confluent.kafka.server/sent_bytes` | Bytes consumed from topic | Consumer activity |
| `io.confluent.kafka.server/received_records` | Records produced | Production rate |
| `io.confluent.kafka.server/sent_records` | Records consumed | Consumption rate |
| `io.confluent.kafka.server/consumer_lag_offsets` | Consumer group lag | Lag monitoring |
| `io.confluent.kafka.connect/received_records` | Records received by connector | Connector throughput |
| `io.confluent.kafka.connect/sent_records` | Records sent by connector | Connector throughput |
| `io.confluent.flink/num_records_in` | Flink records read | Flink job activity |
| `io.confluent.flink/num_records_out` | Flink records written | Flink job activity |

### Query Response - Key Fields

```json
{
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "value": 1048576.0,
      "metric.topic": "orders"
    }
  ]
}
```

### Available Group-By Dimensions

| Dimension | Description |
|-----------|-------------|
| `resource.kafka.id` | Kafka cluster ID |
| `metric.topic` | Topic name |
| `metric.consumer_group_id` | Consumer group |
| `resource.connector.id` | Connector ID |
| `resource.flink.compute_pool.id` | Flink pool ID |
| `resource.ksql.id` | ksqlDB cluster ID |
| `resource.schema_registry.id` | Schema Registry cluster ID |

### Pagination

- **Pattern:** Result-count-based. Use `limit` in the query body.
- Maximum 1000 data points per query.
- Use `intervals` and `granularity` to control time range.
- For large datasets, issue multiple queries with different time intervals.

### Rate Limits

- **Documented:** 10 requests per minute per API key (stricter than other APIs).
- Queries returning large result sets count more against the limit.
- HTTP 429 with `Retry-After` header.

### Error Handling

| Code | Meaning | Retry? |
|------|---------|--------|
| 400 | Invalid query (bad metric name, filter syntax) | No - fix query |
| 401 | Invalid Cloud API key | No |
| 403 | Insufficient permissions (need MetricsViewer) | No |
| 422 | Unprocessable query | No |
| 429 | Rate limited (10 req/min) | Yes - long backoff |
| 5xx | Server error | Yes |

### Lineage Signals

- **Nodes:** None directly (metrics enrich existing nodes)
- **Edges:** None directly (metrics confirm existing edges are active)
- **Enrichment:**
  - Topic throughput validates that edges are active (not stale)
  - Consumer lag shows consumer-group-to-topic relationships at runtime
  - Connector/Flink metrics confirm processing activity
  - Useful for confidence scoring: edges with recent activity are high-confidence

---

## 9. Credential Matrix

### API Key Types Required

| API Surface | API Key Type | Auth Method | Minimum Role/Permission |
|-------------|-------------|-------------|------------------------|
| **Kafka REST Admin v3** (cluster data plane) | Cluster-scoped Kafka API key | Basic Auth | DeveloperRead on topics, consumer groups, ACLs |
| **Kafka cluster enumeration** (`api.confluent.cloud/cmk/v2`) | Cloud API key | Basic Auth | EnvironmentAdmin or OrganizationAdmin |
| **Stream Catalog** | Schema Registry API key | Basic Auth | DeveloperRead on SR cluster |
| **Schema Registry** | Schema Registry API key | Basic Auth | DeveloperRead on SR cluster |
| **Connect API** (Cloud v1) | Cloud API key | Basic Auth | EnvironmentAdmin or specific connector RBAC |
| **Connect API** (data plane) | Cluster-scoped Kafka API key | Basic Auth | DeveloperRead |
| **ksqlDB** (cluster API) | Cloud API key | Basic Auth | EnvironmentAdmin |
| **ksqlDB** (data plane) | ksqlDB-scoped API key | Basic Auth | DeveloperRead on ksqlDB cluster |
| **Flink** (compute pool API) | Cloud API key | Basic Auth | EnvironmentAdmin |
| **Flink** (Statement/Table API) | Flink API key | Basic Auth | FlinkDeveloper or FlinkAdmin |
| **Tableflow** | Cloud API key | Basic Auth | EnvironmentAdmin |
| **Metrics API** | Cloud API key | Basic Auth | MetricsViewer role |

### Summary of Distinct Keys Needed

For a complete lineage extraction, the following distinct API keys are required:

| # | Key Type | Used For | Count |
|---|----------|----------|-------|
| 1 | **Cloud API key** | Cluster/env enumeration, Connect v1, ksqlDB cluster API, Flink compute pools, Tableflow, Metrics | 1 per organization |
| 2 | **Kafka API key** (cluster-scoped) | Kafka REST v3 (topics, consumer groups, ACLs) | 1 per Kafka cluster |
| 3 | **Schema Registry API key** | Schema Registry + Stream Catalog | 1 per SR cluster (typically 1 per environment) |
| 4 | **ksqlDB API key** | ksqlDB data-plane queries | 1 per ksqlDB cluster |
| 5 | **Flink API key** | Flink Statement/Table API | 1 per environment or org |

### Recommended Service Account Setup

Create a dedicated service account (e.g., `sa-lineage-bridge`) with:

1. **Organization-level:** MetricsViewer
2. **Environment-level:** EnvironmentAdmin (or narrower roles for each service)
3. **Cluster-level:** DeveloperRead on all Kafka clusters
4. **Schema Registry:** DeveloperRead
5. **ksqlDB:** DeveloperRead per ksqlDB cluster
6. **Flink:** FlinkDeveloper (read-only operations)

### Endpoint Discovery Flow

```
1. Cloud API key --> GET /org/v2/environments           --> list environments
2. Cloud API key --> GET /cmk/v2/clusters?env=<env_id>  --> list Kafka clusters
                                                            (gives cluster REST endpoint)
3. Cloud API key --> GET /srcm/v2/clusters?env=<env_id> --> list SR clusters
                                                            (gives SR endpoint)
4. Cloud API key --> GET /ksqldbcm/v2/clusters?env=<env_id> --> list ksqlDB clusters
                                                                  (gives ksqlDB HTTP endpoint)
5. Cloud API key --> GET /fcpm/v2/compute-pools?env=<env_id> --> list Flink pools
6. Kafka API key --> GET /kafka/v3/clusters/<id>/topics  --> list topics
7. SR API key    --> GET /subjects                       --> list schemas
8. SR API key    --> GET /catalog/v1/search/basic        --> search catalog entities
9. Cloud API key --> GET /connect/v1/environments/<env>/clusters/<cluster>/connectors
10. ksqlDB key   --> POST /ksql (SHOW QUERIES, etc.)
11. Flink key    --> GET /sql/v1/organizations/<org>/environments/<env>/statements
12. Cloud API key --> GET /tableflow/v1/tableflow-topics?env=<env_id>
13. Cloud API key --> POST /v2/metrics/cloud/query
```

---

## Appendix: Cross-API Entity Resolution

To build a unified lineage graph, entities from different APIs must be
correlated. The join keys are:

| Entity | API Source | Join Key |
|--------|-----------|----------|
| Kafka topic | Kafka REST v3 | `cluster_id` + `topic_name` |
| Kafka topic | Stream Catalog | `qualifiedName` = `<sr_id>:<cluster_id>:<topic>` |
| Kafka topic | Schema Registry | Subject = `<topic_name>-value` or `<topic_name>-key` |
| Kafka topic | Connect config | `topics` (sink) or `topic.prefix` + table (source) |
| Kafka topic | ksqlDB DESCRIBE | `topic` field in sourceDescription |
| Kafka topic | Flink SQL | Table name in SQL (= topic name) |
| Kafka topic | Tableflow | `spec.topic_name` |
| Kafka topic | Metrics | `metric.topic` dimension |
| Connector | Connect API | `name` (connector name) |
| Connector | Stream Catalog | `qualifiedName` includes connector name |
| Schema | Schema Registry | `subject` + `version` |
| Schema | Stream Catalog | `qualifiedName` = `<sr_id>:<subject>:<version>` |
| ksqlDB query | ksqlDB REST | `id` (query ID) |
| Flink job | Flink API | `name` (statement name) |
| Tableflow table | Tableflow API | `spec.topic_name` + `spec.kafka_cluster.id` |
| UC table | Tableflow + CatalogIntegration | `<catalog>.<cluster_id>.<topic_name>` |
