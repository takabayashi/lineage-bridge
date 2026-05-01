# Glossary

A comprehensive reference of terms used in LineageBridge, Confluent Cloud, and data catalog integrations.

---

## A

### API Key
Credential pair (key + secret) used to authenticate with Confluent Cloud APIs. LineageBridge supports both cloud-level and cluster-scoped API keys.

### Async
Asynchronous programming pattern used throughout LineageBridge clients. All Confluent API calls use `httpx` with `async`/`await` for non-blocking I/O.

### Audit Log
Kafka topic that records Confluent Cloud API activity. LineageBridge can consume this for change detection (retained for future use).

### AWS Glue
Amazon Web Services data catalog service. LineageBridge integrates with Glue via the `GlueProvider` to enrich tables and push lineage metadata.

---

## B

### BYOB (Bring Your Own Bucket)
Tableflow integration mode where data is staged in a customer-managed S3 bucket before loading into a data catalog.

---

## C

### Catalog Provider
Implementation of the `CatalogProvider` protocol that integrates a data catalog (e.g., Databricks UC, AWS Glue, Google Data Lineage). Providers implement `build_node()`, `enrich()`, `build_url()`, and `push_lineage()`.

### Client
Module that interacts with a specific Confluent Cloud API. All clients extend `ConfluentClient` base class and implement retry, pagination, and error handling.

### Cluster
Kafka cluster within a Confluent Cloud environment. LineageBridge supports extracting lineage from multiple clusters in parallel.

### CloudEvent
Standard format for event data. LineageBridge parses audit log events using the CloudEvent specification.

### Connector
Kafka Connect connector (source or sink) that moves data between Kafka topics and external systems. LineageBridge extracts connector metadata and creates edges to external datasets.

### Consumer Group
Group of Kafka consumers that coordinate to consume messages from topics. LineageBridge tracks consumer group membership via the KafkaAdmin client.

---

## D

### DAG (Directed Acyclic Graph)
Graph structure where edges have direction and no cycles exist. LineageBridge renders lineage as a DAG using Sugiyama layout.

### Databricks
Unified analytics platform. LineageBridge integrates with Databricks via Unity Catalog and the SQL Statement Execution API.

### Dataset
External data source or sink referenced by a connector (e.g., S3 bucket, RDS table, REST API).

---

## E

### Edge
Connection between two nodes in the lineage graph. Edge types: `PRODUCES`, `CONSUMES`, `TRANSFORMS`, `MATERIALIZES`, `HAS_SCHEMA`, `MEMBER_OF`.

### Edge Type
Semantic label for the relationship between nodes. See [Edge Types](#edge-types) section.

### Enrichment
Process of backfilling node metadata from external APIs (e.g., Schema Registry, Stream Catalog, catalog providers).

### Environment
Top-level organizational unit in Confluent Cloud. Contains clusters, connectors, Flink pools, and ksqlDB clusters.

### Extractor
Client module that extracts lineage from a specific Confluent Cloud service. Examples: `KafkaAdmin`, `Connect`, `Flink`, `ksqlDB`.

---

## F

### Flink
Stream processing framework. LineageBridge extracts Flink SQL statements and parses them to identify input/output topics.

### Flink SQL
SQL dialect for stream processing. LineageBridge parses CREATE TABLE, INSERT INTO, and CREATE TABLE AS SELECT statements.

---

## G

### Glue Data Catalog
See [AWS Glue](#aws-glue).

### Google Data Lineage
Google Cloud service for tracking data lineage. LineageBridge provides a provider for publishing lineage to Google Data Lineage API.

### Graph
NetworkX `DiGraph` instance that stores nodes and edges. See `LineageGraph` in `models/graph.py`.

---

## K

### Kafka
Distributed event streaming platform. Confluent Cloud is a fully managed Kafka service.

### KafkaAdmin
LineageBridge client that extracts topic metadata and consumer group membership using the Kafka Admin API.

### ksqlDB
Stream processing engine built on Kafka Streams. LineageBridge extracts persistent queries and parses SQL to identify lineage edges.

---

## L

### Lineage
Relationship between data assets showing how data flows and transforms across systems.

### Lineage Edge
See [Edge](#edge).

### Lineage Graph
Complete directed graph of all extracted lineage. Stored as NetworkX `DiGraph` and serialized to JSON for UI rendering.

### Lineage Node
See [Node](#node).

---

## M

### MATERIALIZES
Edge type connecting a Tableflow integration or topic to a catalog table (e.g., `KAFKA_TOPIC -> MATERIALIZES -> UC_TABLE`).

### Metrics API
Confluent Cloud API for querying telemetry data (throughput, consumer lag). LineageBridge uses this to enrich nodes with real-time metrics.

---

## N

### NetworkX
Python library for graph analysis. LineageBridge uses `networkx.DiGraph` as the core data structure.

### Node
Entity in the lineage graph representing a data asset (topic, connector, table, schema, etc.). See [Node Types](#node-types) section.

### Node ID
Unique identifier for a node with format: `{system}:{type}:{env_id}:{qualified_name}`. Example: `CONFLUENT:KAFKA_TOPIC:env-abc123:my-topic`.

### Node Type
Category of data asset. Examples: `KAFKA_TOPIC`, `CONNECTOR`, `UC_TABLE`, `FLINK_JOB`, `KSQLDB_QUERY`.

---

## O

### OpenLineage
Open standard for data lineage metadata. LineageBridge exposes an OpenLineage-compatible API at `/api/v1/lineage`.

### Orchestrator
Core extraction pipeline (`extractors/orchestrator.py`) that coordinates all clients in 5 phases.

---

## P

### Protocol
Python typing concept for structural subtyping. LineageBridge uses protocols for `LineageExtractor` and `CatalogProvider` to enable extensibility.

### Provider
See [Catalog Provider](#catalog-provider).

### Pydantic
Python library for data validation. LineageBridge uses Pydantic v2 for all model definitions and settings.

### Pydantic Settings
Pydantic module for loading configuration from environment variables. LineageBridge uses `LINEAGE_BRIDGE_` prefix for all settings.

---

## Q

### Qualified Name
Fully qualified identifier for a resource (e.g., `catalog.schema.table` for Unity Catalog, `my-topic` for Kafka topic).

---

## R

### RBAC (Role-Based Access Control)
Confluent Cloud authorization model. LineageBridge requires appropriate RBAC permissions to read metadata.

### REST v3
Confluent Cloud REST API version 3. Primary API used by LineageBridge for extraction.

---

## S

### SASL
Simple Authentication and Security Layer. Protocol used for Kafka authentication. LineageBridge uses SASL/PLAIN for audit log consumer.

### Schema
Data structure definition (Avro, Protobuf, JSON Schema) stored in Schema Registry.

### Schema Registry
Confluent service for managing schemas. LineageBridge extracts schemas and creates `HAS_SCHEMA` edges to topics.

### SSL
Secure Sockets Layer. LineageBridge uses SSL/TLS for all Kafka connections.

### Stream Catalog
Confluent Cloud service for managing tags and business metadata on topics. LineageBridge extracts this metadata to enrich topic nodes.

### Streamlit
Python framework for building data apps. LineageBridge UI is built with Streamlit.

### Sugiyama Layout
Hierarchical graph layout algorithm that minimizes edge crossings. LineageBridge uses this to render DAGs top-to-bottom.

### System
High-level categorization of nodes. Values: `CONFLUENT`, `DATABRICKS`, `AWS`, `GOOGLE`, `EXTERNAL`.

---

## T

### Tableflow
Confluent Cloud service that streams Kafka topics to external data catalogs (Unity Catalog, BigQuery, Snowflake). LineageBridge extracts Tableflow mappings to create `MATERIALIZES` edges.

### Topic
Kafka topic where events are published and consumed. Central entity in stream lineage.

---

## U

### UC (Unity Catalog)
Databricks unified governance solution for data and AI assets. LineageBridge integrates with UC via the `DatabricksUCProvider`.

### Unity Catalog
See [UC](#uc-unity-catalog).

---

## V

### vis.js
JavaScript library for network visualization. LineageBridge uses a custom Streamlit component wrapping vis.js for graph rendering.

---

## W

### Watcher
Change-detection service (`watcher/engine.py`) that polls Confluent APIs for changes and triggers re-extraction. Runs as a background thread or standalone CLI.

---

## Node Types

| Type | Description | System |
|------|-------------|--------|
| `KAFKA_TOPIC` | Kafka topic | `CONFLUENT` |
| `CONNECTOR` | Kafka Connect connector | `CONFLUENT` |
| `KSQLDB_QUERY` | ksqlDB persistent query | `CONFLUENT` |
| `FLINK_JOB` | Flink SQL statement | `CONFLUENT` |
| `TABLEFLOW_TABLE` | Tableflow integration | `CONFLUENT` |
| `UC_TABLE` | Unity Catalog table | `DATABRICKS` |
| `GLUE_TABLE` | AWS Glue table | `AWS` |
| `GOOGLE_DATASET` | Google BigQuery dataset | `GOOGLE` |
| `SCHEMA` | Schema Registry schema | `CONFLUENT` |
| `EXTERNAL_DATASET` | External data source/sink | `EXTERNAL` |
| `CONSUMER_GROUP` | Kafka consumer group | `CONFLUENT` |

---

## Edge Types

| Type | Description | Example |
|------|-------------|---------|
| `PRODUCES` | Source produces data to destination | `CONNECTOR -> PRODUCES -> KAFKA_TOPIC` |
| `CONSUMES` | Source consumes data from destination | `CONNECTOR -> CONSUMES -> KAFKA_TOPIC` |
| `TRANSFORMS` | Source transforms destination data | `KSQLDB_QUERY -> TRANSFORMS -> KAFKA_TOPIC` |
| `MATERIALIZES` | Source materializes into destination catalog | `KAFKA_TOPIC -> MATERIALIZES -> UC_TABLE` |
| `HAS_SCHEMA` | Topic has schema definition | `KAFKA_TOPIC -> HAS_SCHEMA -> SCHEMA` |
| `MEMBER_OF` | Consumer is member of group | `CONSUMER -> MEMBER_OF -> CONSUMER_GROUP` |

---

## Acronyms

| Acronym | Full Term |
|---------|-----------|
| ADR | Architecture Decision Record |
| API | Application Programming Interface |
| AWS | Amazon Web Services |
| BYOB | Bring Your Own Bucket |
| CI/CD | Continuous Integration / Continuous Delivery |
| CLI | Command Line Interface |
| DAG | Directed Acyclic Graph |
| GCP | Google Cloud Platform |
| HTTP | Hypertext Transfer Protocol |
| IAM | Identity and Access Management |
| JSON | JavaScript Object Notation |
| OL | OpenLineage |
| RBAC | Role-Based Access Control |
| RDS | Relational Database Service |
| REST | Representational State Transfer |
| SASL | Simple Authentication and Security Layer |
| SCIM | System for Cross-domain Identity Management |
| SQL | Structured Query Language |
| SR | Schema Registry |
| SSL | Secure Sockets Layer |
| TLS | Transport Layer Security |
| UC | Unity Catalog |
| UI | User Interface |
| URL | Uniform Resource Locator |

---

## See Also

- [Architecture](../architecture.md) - System design and component overview
- [Catalog Integration](../catalog-integration.md) - Data catalog provider details
- [API Reference](../api-reference.md) - REST API and client documentation
