# Client Architecture

LineageBridge uses a layered client architecture with protocols for extensibility and a base client for common HTTP operations.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│           Orchestrator (extractors/)            │
└──────────────┬──────────────────────────────────┘
               │
       ┌───────┴───────┐
       │               │
       v               v
┌─────────────┐  ┌──────────────┐
│  Confluent  │  │   Catalog    │
│   Clients   │  │  Providers   │
│             │  │              │
│ (extends    │  │ (implements  │
│  Confluent  │  │  Catalog     │
│  Client)    │  │  Provider)   │
└─────────────┘  └──────────────┘
       │               │
       v               v
┌─────────────┐  ┌──────────────┐
│  httpx      │  │ httpx/boto3/ │
│  AsyncClient│  │ google-auth  │
└─────────────┘  └──────────────┘
```

## ConfluentClient Base

All Confluent Cloud API clients extend `ConfluentClient`, which provides:

- **Async HTTP client** (httpx)
- **Basic authentication** (API key/secret)
- **Exponential backoff retry** (3 attempts, configurable)
- **Cursor-based pagination** (follows `metadata.next`)
- **Timeout handling** (30s default)

### Retry Policy

```python
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds
```

Retries on:
- `429 Too Many Requests`
- `500 Internal Server Error`
- `502 Bad Gateway`
- `503 Service Unavailable`
- `504 Gateway Timeout`

Backoff delay: `1.0 * (2 ** attempt)` seconds (1s, 2s, 4s)

Respects `Retry-After` header when present.

### Usage Example

```python
from lineage_bridge.clients.base import ConfluentClient

client = ConfluentClient(
    base_url="https://api.confluent.cloud",
    api_key="YOUR_API_KEY",
    api_secret="YOUR_API_SECRET",
)

async with client:
    # GET request
    data = await client.get("/org/v2/environments")
    
    # POST request
    result = await client.post("/path", json_body={"key": "value"})
    
    # Paginated request
    all_items = await client.paginate("/cmk/v2/clusters", params={"environment": "env-abc123"})
```

### Error Handling

All requests raise `httpx.HTTPStatusError` on non-retryable failures (e.g., 401, 403, 404).

```python
try:
    data = await client.get("/path")
except httpx.HTTPStatusError as e:
    if e.response.status_code == 401:
        print("Unauthorized - check API key")
    elif e.response.status_code == 403:
        print("Forbidden - check permissions")
    elif e.response.status_code == 404:
        print("Not found")
```

## LineageExtractor Protocol

Extraction clients implement the `LineageExtractor` protocol:

```python
from typing import Protocol

class LineageExtractor(Protocol):
    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Extract lineage data and return nodes and edges."""
        ...
```

This protocol enables:
- **Duck typing** - No inheritance required
- **Easy mocking** - Simple to create test doubles
- **Extensibility** - Add new extractors without modifying existing code

### Implementing Extractors

```python
from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.clients.protocol import LineageExtractor
from lineage_bridge.models.graph import LineageNode, LineageEdge

class MyExtractor(ConfluentClient):
    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        nodes = []
        edges = []
        
        # Fetch data from API
        data = await self.get("/my/endpoint")
        
        # Build nodes and edges
        for item in data:
            node = LineageNode(...)
            nodes.append(node)
            
        return nodes, edges
```

## Confluent Clients

### KafkaAdminClient

**Purpose**: Extract Kafka topics and consumer groups.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/kafka/v3/clusters/{cluster_id}/topics`
- `/kafka/v3/clusters/{cluster_id}/consumer-groups`
- `/kafka/v3/clusters/{cluster_id}/consumer-groups/{group_id}/consumers`

**Credentials**: Cluster-scoped API key (or Cloud API key with Kafka REST permissions)

**Example**:
```python
from lineage_bridge.clients.kafka_admin import KafkaAdminClient

client = KafkaAdminClient(
    base_url="https://pkc-abc123.us-east-1.aws.confluent.cloud:443",
    api_key="KAFKA_KEY",
    api_secret="KAFKA_SECRET",
    cluster_id="lkc-xyz789",
    environment_id="env-abc123",
)

async with client:
    nodes, edges = await client.extract()
```

### ConnectClient

**Purpose**: Extract Kafka Connect connectors and external datasets.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/connect/v1/environments/{env_id}/clusters`
- `/connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors`

**Credentials**: Cloud API key

**Example**:
```python
from lineage_bridge.clients.connect import ConnectClient

client = ConnectClient(
    api_key="CLOUD_API_KEY",
    api_secret="CLOUD_API_SECRET",
    environment_id="env-abc123",
    kafka_cluster_id="lkc-xyz789",
)

async with client:
    nodes, edges = await client.extract()
```

### KsqlDBClient

**Purpose**: Extract ksqlDB queries, streams, and tables.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/ksqldb/v1/organizations/{org_id}/environments/{env_id}/clusters`
- `/ksqldb/v2/clusters/{cluster_id}/ksql` (query introspection)

**Credentials**: Cloud API key + ksqlDB API key (optional)

**Example**:
```python
from lineage_bridge.clients.ksqldb import KsqlDBClient

client = KsqlDBClient(
    cloud_api_key="CLOUD_API_KEY",
    cloud_api_secret="CLOUD_API_SECRET",
    environment_id="env-abc123",
    ksqldb_api_key="KSQLDB_KEY",  # Optional
    ksqldb_api_secret="KSQLDB_SECRET",
)

async with client:
    nodes, edges = await client.extract()
```

### FlinkClient

**Purpose**: Extract Flink SQL statements and parse SQL for lineage.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/sql/v1/organizations/{org_id}/environments/{env_id}/statements`

**Credentials**: Cloud API key + Flink API key (optional)

**SQL Parsing**: Uses `sqlglot` to parse Flink SQL and extract table references.

**Example**:
```python
from lineage_bridge.clients.flink import FlinkClient

client = FlinkClient(
    cloud_api_key="CLOUD_API_KEY",
    cloud_api_secret="CLOUD_API_SECRET",
    environment_id="env-abc123",
    organization_id="org-xyz",
    flink_api_key="FLINK_KEY",  # Optional
    flink_api_secret="FLINK_SECRET",
)

async with client:
    nodes, edges = await client.extract()
```

### SchemaRegistryClient

**Purpose**: Extract schema metadata and link to topics.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/subjects`
- `/subjects/{subject}/versions/latest`

**Credentials**: Schema Registry API key (or Cloud API key)

**Example**:
```python
from lineage_bridge.clients.schema_registry import SchemaRegistryClient

client = SchemaRegistryClient(
    base_url="https://psrc-abc123.us-east-1.aws.confluent.cloud",
    api_key="SR_KEY",
    api_secret="SR_SECRET",
    environment_id="env-abc123",
)

async with client:
    nodes, edges = await client.extract()
```

### StreamCatalogClient

**Purpose**: Enrich topic nodes with business metadata and tags.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/catalog/v1/entity/type/kafka_topic`
- `/catalog/v1/entity/type/sr_schema`

**Credentials**: Schema Registry API key (Stream Catalog is part of Schema Registry)

**Behavior**: Mutates graph in-place, returns empty lists.

**Example**:
```python
from lineage_bridge.clients.stream_catalog import StreamCatalogClient

client = StreamCatalogClient(
    base_url="https://psrc-abc123.us-east-1.aws.confluent.cloud",
    api_key="SR_KEY",
    api_secret="SR_SECRET",
    environment_id="env-abc123",
)

async with client:
    await client.enrich(graph)  # Mutates graph
```

### TableflowClient

**Purpose**: Map Kafka topics to data catalog tables.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/tableflow/v1/environments/{env_id}/integrations`
- `/tableflow/v1/environments/{env_id}/topic-table-mappings`

**Credentials**: Cloud API key (or Tableflow API key)

**Provider Delegation**: Calls `CatalogProvider.build_node()` to create catalog-specific nodes.

**Example**:
```python
from lineage_bridge.clients.tableflow import TableflowClient

client = TableflowClient(
    api_key="CLOUD_API_KEY",
    api_secret="CLOUD_API_SECRET",
    environment_id="env-abc123",
    cluster_ids=["lkc-xyz789"],
)

async with client:
    nodes, edges = await client.extract()
```

### MetricsClient

**Purpose**: Enrich topic nodes with real-time throughput metrics.

**Extends**: `ConfluentClient`

**API Endpoints**:
- `/v2/metrics/cloud/query` (Confluent Metrics API)

**Credentials**: Cloud API key

**Behavior**: Mutates graph in-place, returns count of enriched nodes.

**Example**:
```python
from lineage_bridge.clients.metrics import MetricsClient

client = MetricsClient(
    api_key="CLOUD_API_KEY",
    api_secret="CLOUD_API_SECRET",
    lookback_hours=1,
)

async with client:
    enriched = await client.enrich(graph, cluster_id="lkc-xyz789")
```

## CatalogProvider Protocol

Catalog integrations implement the `CatalogProvider` protocol:

```python
from typing import Protocol

class CatalogProvider(Protocol):
    catalog_type: str  # "UNITY_CATALOG", "AWS_GLUE", "GOOGLE_DATA_LINEAGE"
    
    def build_node(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog table node and MATERIALIZES edge."""
        ...
    
    async def enrich(self, graph: LineageGraph) -> None:
        """Backfill metadata from the catalog API."""
        ...
    
    def build_url(self, node: LineageNode) -> str:
        """Generate a deep link to the catalog UI."""
        ...
    
    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Write lineage metadata back to the catalog."""
        ...
```

### Databricks UC Provider

**HTTP Client**: `httpx.AsyncClient` with Bearer token

**Authentication**: `workspace_url` + `token`

**API Endpoints**:
- `/api/2.1/unity-catalog/tables/{full_name}` (metadata)
- `/api/2.0/sql/statements` (lineage push via SQL)

**Example**:
```python
from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider

provider = DatabricksUCProvider(
    workspace_url="https://myworkspace.cloud.databricks.com",
    token="dapi...",
)

await provider.enrich(graph)
```

### AWS Glue Provider

**HTTP Client**: `boto3.client("glue")` (sync, wrapped with `asyncio.to_thread()`)

**Authentication**: boto3 auto-discovers credentials (env vars, `~/.aws/credentials`, IAM roles)

**API Endpoints**:
- `glue:GetTable` (metadata)
- `glue:UpdateTable` (lineage push)

**Example**:
```python
from lineage_bridge.catalogs.aws_glue import GlueCatalogProvider

provider = GlueCatalogProvider(region="us-east-1")

await provider.enrich(graph)
```

### Google Data Lineage Provider

**HTTP Client**: `httpx.AsyncClient` with Application Default Credentials

**Authentication**: `project_id` + `location` (ADC via `google-auth`)

**API Endpoints**:
- BigQuery: `/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}` (metadata)
- Data Lineage: `/v1/projects/{project}/locations/{location}:processOpenLineageRunEvent` (lineage push)

**Example**:
```python
from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider

provider = GoogleLineageProvider(
    project_id="my-project",
    location="us-central1",
)

await provider.enrich(graph)
```

## Credential Resolution

### Confluent Cloud

- **Cloud API key**: `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY` / `LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET`
- **Cluster API key**: `LINEAGE_BRIDGE_KAFKA_API_KEY` / `LINEAGE_BRIDGE_KAFKA_API_SECRET` (optional)
- **Schema Registry**: `LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY` / `LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET` (optional)
- **ksqlDB**: `LINEAGE_BRIDGE_KSQLDB_API_KEY` / `LINEAGE_BRIDGE_KSQLDB_API_SECRET` (optional)
- **Flink**: `LINEAGE_BRIDGE_FLINK_API_KEY` / `LINEAGE_BRIDGE_FLINK_API_SECRET` (optional)

### Databricks

- **UC**: `LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL` / `LINEAGE_BRIDGE_DATABRICKS_TOKEN`
- **Warehouse ID**: `LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID` (optional, auto-discovered)

### AWS

- **Glue**: `AWS_REGION` (boto3 auto-discovers credentials from environment)

### Google

- **BigQuery/Data Lineage**: `LINEAGE_BRIDGE_GCP_PROJECT_ID` / `LINEAGE_BRIDGE_GCP_LOCATION` (ADC auto-discovers credentials)

## Timeout and Retry Configuration

### Per-Extractor Timeout

Orchestrator wraps all extractors in `asyncio.wait_for(coro, timeout=120)`.

```python
_EXTRACTOR_TIMEOUT = 120  # seconds
```

### HTTP Request Timeout

`ConfluentClient` uses a 30s default timeout per request:

```python
_DEFAULT_TIMEOUT = 30.0  # seconds
```

### Retry Configuration

```python
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds (exponential: 1s, 2s, 4s)
```

## Next Steps

- [Extraction Pipeline](extraction-pipeline.md) - How clients are orchestrated
- [Graph Model](graph-model.md) - Node/edge types
- [Troubleshooting: API Errors](../troubleshooting/api-errors.md)
