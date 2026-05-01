# Graph Model

LineageBridge represents data lineage as a directed graph where nodes are data assets and edges are data flow relationships.

## Data Structures

The graph model is built on three core Pydantic models:

### LineageNode

Represents a data asset or processing step in the lineage graph.

```python
class LineageNode(BaseModel):
    node_id: str                    # Unique ID: {system}:{type}:{env_id}:{qualified_name}
    system: SystemType              # CONFLUENT, DATABRICKS, AWS, GOOGLE, EXTERNAL
    node_type: NodeType             # See NodeType enum below
    qualified_name: str             # Fully qualified name (e.g., catalog.schema.table)
    display_name: str               # Human-readable name
    environment_id: str | None      # Confluent environment ID
    environment_name: str | None    # Environment display name
    cluster_id: str | None          # Kafka cluster ID
    cluster_name: str | None        # Cluster display name
    attributes: dict[str, Any]      # Extensible metadata
    tags: list[str]                 # Business tags
    url: str | None                 # Deep link to UI
    first_seen: datetime            # First observed timestamp
    last_seen: datetime             # Last observed timestamp
```

### LineageEdge

Represents a directed data flow relationship between two nodes.

```python
class LineageEdge(BaseModel):
    src_id: str                     # Source node ID
    dst_id: str                     # Destination node ID
    edge_type: EdgeType             # PRODUCES, CONSUMES, TRANSFORMS, etc.
    confidence: float               # 1.0 = deterministic, <1.0 = inferred
    attributes: dict[str, Any]      # Extensible metadata
    first_seen: datetime            # First observed timestamp
    last_seen: datetime             # Last observed timestamp
```

### LineageGraph

In-memory graph container backed by NetworkX DiGraph.

```python
class LineageGraph:
    def add_node(node: LineageNode) -> None
    def add_edge(edge: LineageEdge) -> None
    def get_node(node_id: str) -> LineageNode | None
    def get_edge(src_id: str, dst_id: str, edge_type: EdgeType) -> LineageEdge | None
    def get_neighbors(node_id: str, direction: str) -> list[LineageNode]
    def upstream(node_id: str, hops: int) -> list[LineageNode]
    def downstream(node_id: str, hops: int) -> list[LineageNode]
    def filter_by_type(node_type: NodeType) -> list[LineageNode]
    def filter_by_env(environment_id: str) -> list[LineageNode]
    def search_nodes(query: str) -> list[LineageNode]
    def validate() -> list[str]
    def to_dict() -> dict[str, Any]
    def to_json_file(path: str) -> None
```

## Node ID Format

All node IDs follow a consistent format:

```
{system}:{type}:{env_id}:{qualified_name}
```

**Examples**:

- Kafka topic: `confluent:kafka_topic:env-abc123:my-topic`
- Connector: `confluent:connector:env-abc123:lkc-xyz789:mysql-source`
- ksqlDB query: `confluent:ksqldb_query:env-abc123:lksqlc-abc:QUERY_123`
- Flink job: `confluent:flink_job:env-abc123:lfcp-xyz:my-statement`
- UC table: `databricks:uc_table:env-abc123:main.sales.orders` *(see "Catalog node IDs" note below)*
- Glue table: `aws:glue_table:env-abc123:my_database.my_table`
- Google table: `google:google_table:env-abc123:project.dataset.table`

> **Catalog node IDs (v0.5.0):** ADR-021 collapsed the per-catalog NodeType
> values into a single `CATALOG_TABLE`, but the **node-ID segments above
> were left intentionally unchanged** (`uc_table` / `glue_table` /
> `google_table`) for ID stability — graphs serialised before the rename
> still resolve to the right keys. The runtime discriminator is the
> `catalog_type` field on the node (`UNITY_CATALOG` / `AWS_GLUE` / etc.),
> not the ID-segment string.
- Schema: `confluent:schema:env-abc123:my-topic-value:v1`
- External dataset: `external:external_dataset:env-abc123:s3://bucket/path`
- Consumer group: `confluent:consumer_group:env-abc123:lkc-xyz789:my-group`

## NodeType Enum

```python
class NodeType(StrEnum):
    KAFKA_TOPIC = "kafka_topic"
    CONNECTOR = "connector"
    KSQLDB_QUERY = "ksqldb_query"
    FLINK_JOB = "flink_job"
    TABLEFLOW_TABLE = "tableflow_table"
    CATALOG_TABLE = "catalog_table"   # v0.5.0: collapses UC/Glue/Google
    SCHEMA = "schema"
    EXTERNAL_DATASET = "external_dataset"
    CONSUMER_GROUP = "consumer_group"
```

The companion `LineageNode.catalog_type: str | None` discriminates among
catalogs (`UNITY_CATALOG`, `AWS_GLUE`, `GOOGLE_DATA_LINEAGE`,
`AWS_DATAZONE`, future `SNOWFLAKE` / `WATSONX`). It's `None` for non-catalog
node types.

### Node Type Descriptions

| Type | Description | Example |
|------|-------------|---------|
| `KAFKA_TOPIC` | Kafka topic in a cluster | `orders`, `user-events` |
| `CONNECTOR` | Kafka Connect source or sink | `mysql-source`, `s3-sink` |
| `KSQLDB_QUERY` | ksqlDB persistent query, stream, or table | `CSAS_ORDERS_0`, `USER_STREAM` |
| `FLINK_JOB` | Flink SQL statement | `process-clickstream` |
| `TABLEFLOW_TABLE` | Tableflow integration table | Intermediate mapping node |
| `CATALOG_TABLE` | Catalog table — UC / Glue / BigQuery / DataZone (discriminated by `catalog_type`) | `main.sales.orders` (UC), `my_database.my_table` (Glue), `project.dataset.table` (BigQuery) |
| `SCHEMA` | Schema Registry schema version | `orders-value`, `users-key` |
| `EXTERNAL_DATASET` | External system (S3, database, etc.) | `s3://bucket/path`, `mysql://host/db.table` |
| `CONSUMER_GROUP` | Kafka consumer group | `payment-processor`, `analytics` |

## EdgeType Enum

```python
class EdgeType(StrEnum):
    PRODUCES = "produces"
    CONSUMES = "consumes"
    TRANSFORMS = "transforms"
    MATERIALIZES = "materializes"
    HAS_SCHEMA = "has_schema"
    MEMBER_OF = "member_of"
```

### Edge Type Descriptions

| Type | Direction | Description | Example |
|------|-----------|-------------|---------|
| `PRODUCES` | Source → Topic | Data source writes to topic | `mysql-source` → `orders` |
| `CONSUMES` | Topic → Sink | Data sink reads from topic | `orders` → `s3-sink` |
| `TRANSFORMS` | Source → Destination | Processing step | `orders` → `CSAS_ENRICHED_ORDERS` |
| `MATERIALIZES` | Topic → Table | Topic materialized to catalog table | `orders` → `main.sales.orders` |
| `HAS_SCHEMA` | Topic → Schema | Topic uses schema | `orders` → `orders-value:v1` |
| `MEMBER_OF` | Consumer Group → Topic | Consumer group subscribes to topic | `analytics` → `orders` |

## SystemType Enum

```python
class SystemType(StrEnum):
    CONFLUENT = "confluent"
    DATABRICKS = "databricks"
    AWS = "aws"
    GOOGLE = "google"
    EXTERNAL = "external"
```

### System Type Descriptions

| System | Description | Node Types |
|--------|-------------|-----------|
| `CONFLUENT` | Confluent Cloud | `KAFKA_TOPIC`, `CONNECTOR`, `KSQLDB_QUERY`, `FLINK_JOB`, `SCHEMA`, `CONSUMER_GROUP`, `TABLEFLOW_TABLE` |
| `DATABRICKS` | Databricks Unity Catalog | `CATALOG_TABLE` (catalog_type=`UNITY_CATALOG`) |
| `AWS` | AWS Glue Data Catalog / DataZone | `CATALOG_TABLE` (catalog_type=`AWS_GLUE` or `AWS_DATAZONE`) |
| `GOOGLE` | Google BigQuery / Data Lineage | `CATALOG_TABLE` (catalog_type=`GOOGLE_DATA_LINEAGE`) |
| `EXTERNAL` | External systems | `EXTERNAL_DATASET` |

## Graph Operations

### Adding Nodes and Edges

```python
graph = LineageGraph()

# Add a Kafka topic node
topic = LineageNode(
    node_id="confluent:kafka_topic:env-abc123:orders",
    system=SystemType.CONFLUENT,
    node_type=NodeType.KAFKA_TOPIC,
    qualified_name="orders",
    display_name="orders",
    environment_id="env-abc123",
    cluster_id="lkc-xyz789",
)
graph.add_node(topic)

# Add a UC table node — note CATALOG_TABLE + catalog_type discriminator (v0.5.0).
# The "uc_table" segment in node_id is intentional ID-stability legacy; the
# runtime type is CATALOG_TABLE.
table = LineageNode(
    node_id="databricks:uc_table:env-abc123:main.sales.orders",
    system=SystemType.DATABRICKS,
    node_type=NodeType.CATALOG_TABLE,
    catalog_type="UNITY_CATALOG",
    qualified_name="main.sales.orders",
    display_name="main.sales.orders",
)
graph.add_node(table)

# Add a MATERIALIZES edge
edge = LineageEdge(
    src_id="confluent:kafka_topic:env-abc123:orders",
    dst_id="databricks:uc_table:env-abc123:main.sales.orders",
    edge_type=EdgeType.MATERIALIZES,
)
graph.add_edge(edge)
```

### Querying Lineage

```python
# Get upstream nodes (predecessors)
upstream = graph.upstream(node_id="databricks:uc_table:env-abc123:main.sales.orders", hops=2)

# Get downstream nodes (successors)
downstream = graph.downstream(node_id="confluent:kafka_topic:env-abc123:orders", hops=1)

# Get all Kafka topics
topics = graph.filter_by_type(NodeType.KAFKA_TOPIC)

# Search by name
results = graph.search_nodes("orders")

# Get neighbors
neighbors = graph.get_neighbors(
    node_id="confluent:kafka_topic:env-abc123:orders",
    direction="both"  # "upstream", "downstream", or "both"
)
```

### Graph Merging

When a node is added multiple times:
- `first_seen` is preserved from the first occurrence
- `last_seen` is updated to the most recent occurrence
- `attributes` are merged (new values overwrite old ones)
- `tags` are combined (deduplicated)

When an edge is added multiple times:
- `first_seen` is preserved
- `last_seen` is updated

### Validation

```python
warnings = graph.validate()
# Returns list of warnings:
# - "Orphan node: {node_id}" (nodes with no edges, excluding schemas)
# - "Dangling edge src: {src_id}" (edge references missing source)
# - "Dangling edge dst: {dst_id}" (edge references missing destination)
```

### Serialization

```python
# Export to JSON file
graph.to_json_file("lineage_graph.json")

# Load from JSON file
graph = LineageGraph.from_json_file("lineage_graph.json")

# Convert to dict
data = graph.to_dict()
# Returns: {"nodes": [...], "edges": [...]}
```

## Example Graph

### Simple Pipeline

```
mysql-source (CONNECTOR)
    |
    | PRODUCES
    |
    v
orders (KAFKA_TOPIC) ----HAS_SCHEMA----> orders-value:v1 (SCHEMA)
    |
    | CONSUMES
    |
    v
CSAS_ENRICHED_ORDERS (KSQLDB_QUERY)
    |
    | PRODUCES
    |
    v
enriched-orders (KAFKA_TOPIC)
    |
    | MATERIALIZES
    |
    v
main.sales.orders (UC_TABLE)
```

### Node Attributes

Nodes can carry arbitrary metadata in the `attributes` dictionary:

**Kafka Topic**:
```python
{
    "partition_count": 6,
    "replication_factor": 3,
    "retention_ms": 604800000,
    "bytes_in_rate": 12345.67,  # Added by MetricsClient
    "record_in_rate": 123.45
}
```

**Connector**:
```python
{
    "connector_class": "MySqlSource",
    "connector_type": "source",
    "tasks_max": 1,
    "status": "RUNNING"
}
```

**UC Table**:
```python
{
    "catalog": "main",
    "schema": "sales",
    "table": "orders",
    "table_type": "MANAGED",
    "storage_location": "s3://bucket/path/",
    "data_source_format": "DELTA"
}
```

## Graph Statistics

```python
# Node and edge counts
print(f"Nodes: {graph.node_count}")
print(f"Edges: {graph.edge_count}")

# Pipeline count (connected components with ≥1 edge, excluding HAS_SCHEMA)
print(f"Pipelines: {graph.pipeline_count}")

# Breakdown by system
from collections import Counter
system_counts = Counter(n.system.value for n in graph.nodes)
# Example: {'confluent': 45, 'databricks': 10, 'aws': 5}
```

## Next Steps

- [Extraction Pipeline](extraction-pipeline.md) - How the graph is built
- [Clients](clients.md) - Client protocols and patterns
- [Catalog Integration](../catalog-integration/index.md) - UC, Glue, Google providers
