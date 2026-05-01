# Adding Extractors

LineageBridge is designed to be extensible. This guide shows you how to add new extractors for Confluent Cloud services or new catalog providers for data warehouses.

## Overview

LineageBridge has two main extension points:

1. **LineageExtractor** - For extracting lineage from Confluent Cloud services (Kafka, ksqlDB, Flink, etc.)
2. **CatalogProvider** - For integrating with data catalogs (Unity Catalog, AWS Glue, BigQuery, etc.)

## Adding a Confluent Service Extractor

### The LineageExtractor Protocol

All extractors implement the `LineageExtractor` protocol defined in `lineage_bridge/clients/protocol.py`:

```python
from typing import Protocol
from lineage_bridge.models.graph import LineageNode, LineageEdge

class LineageExtractor(Protocol):
    """Interface for API-specific lineage extractors."""
    
    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Extract nodes and edges from this API surface.
        
        Returns:
            A tuple of (nodes, edges) discovered by this extractor.
        """
        ...
```

That's it! Implement one async method that returns nodes and edges.

### Step 1: Create the Client Module

Create a new file in `lineage_bridge/clients/` following the naming pattern `<service>_client.py`:

```python
# lineage_bridge/clients/my_service.py
"""MyService client - extracts lineage from Confluent MyService API."""

from __future__ import annotations

import logging
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)


class MyServiceClient(ConfluentClient):
    """Extracts lineage from Confluent MyService API."""
    
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        cluster_id: str,
        environment_id: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(base_url, api_key, api_secret, timeout=timeout)
        self.cluster_id = cluster_id
        self.environment_id = environment_id
    
    async def extract(self) -> tuple[list[LineageNode], list[LineageEdge]]:
        """Extract nodes and edges from MyService API."""
        nodes: list[LineageNode] = []
        edges: list[LineageEdge] = []
        
        # 1. Fetch resources from API
        resources = await self._list_resources()
        
        # 2. Build nodes
        for resource in resources:
            node = LineageNode(
                node_id=self._build_node_id(resource["id"]),
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,  # Or your custom type
                qualified_name=resource["name"],
                display_name=resource["name"],
                environment_id=self.environment_id,
                cluster_id=self.cluster_id,
                url=self._build_url(resource["id"]),
                attributes={
                    "status": resource.get("status"),
                    "created_at": resource.get("created_at"),
                },
            )
            nodes.append(node)
        
        # 3. Build edges (e.g., from source/sink relationships)
        for resource in resources:
            if source_id := resource.get("source_topic"):
                edge = LineageEdge(
                    src_id=self._build_topic_id(source_id),
                    dst_id=self._build_node_id(resource["id"]),
                    edge_type=EdgeType.CONSUMES,
                )
                edges.append(edge)
        
        logger.info(
            "MyService extracted %d nodes, %d edges from cluster %s",
            len(nodes),
            len(edges),
            self.cluster_id,
        )
        return nodes, edges
    
    # Helper methods
    
    def _build_node_id(self, resource_id: str) -> str:
        return f"confluent:my_service:{self.environment_id}:{resource_id}"
    
    def _build_topic_id(self, topic_name: str) -> str:
        return f"confluent:kafka_topic:{self.environment_id}:{topic_name}"
    
    def _build_url(self, resource_id: str) -> str:
        return (
            f"https://confluent.cloud/environments/{self.environment_id}"
            f"/clusters/{self.cluster_id}/my-service/{resource_id}"
        )
    
    # API methods
    
    async def _list_resources(self) -> list[dict[str, Any]]:
        """Fetch resources from MyService API."""
        path = f"/my-service/v1/clusters/{self.cluster_id}/resources"
        return await self.paginate(path)
```

### Step 2: Follow the KafkaAdminClient Pattern

Use `lineage_bridge/clients/kafka_admin.py` as a reference. It demonstrates:

- Extending `ConfluentClient` for automatic retry and pagination
- Building node IDs in the format `{system}:{type}:{env_id}:{qualified_name}`
- Using helper methods for ID and URL construction
- Proper logging and error handling
- Handling pagination with `self.paginate(path)`

Key patterns from `KafkaAdminClient`:

```python
# Node ID format
def _topic_node_id(self, topic_name: str) -> str:
    return f"confluent:kafka_topic:{self.environment_id}:{topic_name}"

# URL construction
def _topic_url(self, topic_name: str) -> str:
    return (
        f"https://confluent.cloud/environments/{self.environment_id}"
        f"/clusters/{self.cluster_id}/topics/{topic_name}"
    )

# Pagination
topics = await self._list_topics()

async def _list_topics(self) -> list[dict[str, Any]]:
    path = f"/kafka/v3/clusters/{self.cluster_id}/topics"
    return await self.paginate(path)
```

### Step 3: Register in the Orchestrator

Add your extractor to `lineage_bridge/extractors/orchestrator.py`:

```python
from lineage_bridge.clients.my_service import MyServiceClient

# In the extraction phase where it belongs
async def _extract_my_service(self) -> None:
    """Extract lineage from MyService."""
    client = MyServiceClient(
        base_url=self.settings.cloud_base_url,
        api_key=self.settings.cloud_api_key,
        api_secret=self.settings.cloud_api_secret.get_secret_value(),
        cluster_id=self.cluster_id,
        environment_id=self.environment_id,
    )
    nodes, edges = await client.extract()
    for node in nodes:
        self.graph.add_node(node)
    for edge in edges:
        self.graph.add_edge(edge)
```

### Step 4: Write Tests

Create `tests/unit/test_my_service.py`:

```python
import httpx
import pytest
import respx
from lineage_bridge.clients.my_service import MyServiceClient
from lineage_bridge.models.graph import NodeType

@pytest.fixture()
def my_service_client():
    return MyServiceClient(
        base_url="https://api.confluent.cloud",
        api_key="test-key",
        api_secret="test-secret",
        cluster_id="lkc-123",
        environment_id="env-abc",
    )

@respx.mock
async def test_extract_resources(my_service_client):
    """Test MyServiceClient extracts resources."""
    
    # Mock API response
    respx.get("https://api.confluent.cloud/my-service/v1/clusters/lkc-123/resources").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "res-1",
                        "name": "my-resource",
                        "status": "ACTIVE",
                        "source_topic": "input-topic",
                    }
                ],
                "metadata": {"next": None}
            }
        )
    )
    
    nodes, edges = await my_service_client.extract()
    
    # Assertions
    assert len(nodes) == 1
    assert nodes[0].display_name == "my-resource"
    assert nodes[0].node_type == NodeType.MY_SERVICE_RESOURCE
    
    assert len(edges) == 1
    assert edges[0].edge_type == EdgeType.CONSUMES
```

See [Testing Guide](testing.md) for more details on writing tests.

## Adding a Catalog Provider

### The CatalogProvider Protocol

Catalog providers implement the `CatalogProvider` protocol defined in `lineage_bridge/catalogs/protocol.py`:

```python
from typing import Protocol, Any
from lineage_bridge.models.graph import (
    LineageNode,
    LineageEdge,
    LineageGraph,
    NodeType,
    SystemType,
)

class CatalogProvider(Protocol):
    """Interface for data catalog integrations."""
    
    catalog_type: str           # e.g., "UNITY_CATALOG", "AWS_GLUE"
    node_type: NodeType         # The NodeType this provider creates
    system_type: SystemType     # The SystemType for nodes
    
    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog node + MATERIALIZES edge."""
        ...
    
    async def enrich(self, graph: LineageGraph) -> None:
        """Enrich catalog nodes with metadata from the catalog's API."""
        ...
    
    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link URL to this node in the catalog's UI."""
        ...
```

### Step 1: Create the Provider Module

Create `lineage_bridge/catalogs/my_catalog.py`:

```python
# lineage_bridge/catalogs/my_catalog.py
"""MyCatalog provider - integrates with MyCatalog API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

logger = logging.getLogger(__name__)


class MyCatalogProvider:
    """CatalogProvider implementation for MyCatalog."""
    
    catalog_type: str = "MY_CATALOG"
    node_type: NodeType = NodeType.MY_CATALOG_TABLE
    system_type: SystemType = SystemType.EXTERNAL  # Or create a new one
    
    def __init__(
        self,
        api_url: str | None = None,
        api_token: str | None = None,
    ) -> None:
        self._api_url = api_url.rstrip("/") if api_url else None
        self._api_token = api_token
    
    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a catalog table node + MATERIALIZES edge."""
        
        # Extract catalog config
        catalog_config = ci_config.get("my_catalog", {})
        database = catalog_config.get("database", "default")
        schema = catalog_config.get("schema", cluster_id)
        
        # Build qualified name (catalog-specific format)
        qualified_name = f"{database}.{schema}.{topic_name}"
        node_id = f"my_catalog:table:{environment_id}:{qualified_name}"
        
        # Create node
        node = LineageNode(
            node_id=node_id,
            system=self.system_type,
            node_type=self.node_type,
            qualified_name=qualified_name,
            display_name=qualified_name,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "database": database,
                "schema": schema,
                "table": topic_name,
            },
        )
        
        # Create MATERIALIZES edge from tableflow to catalog
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=node_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        
        return node, edge
    
    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata from MyCatalog API."""
        if not self._api_url or not self._api_token:
            logger.debug("MyCatalog enrichment skipped - no credentials")
            return
        
        catalog_nodes = graph.filter_by_type(self.node_type)
        if not catalog_nodes:
            return
        
        async with httpx.AsyncClient(
            base_url=self._api_url,
            headers={"Authorization": f"Bearer {self._api_token}"},
            timeout=30.0,
        ) as client:
            for node in catalog_nodes:
                await self._enrich_node(client, graph, node)
    
    async def _enrich_node(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch and merge metadata for a single table."""
        qualified_name = node.qualified_name
        url = f"/api/v1/tables/{qualified_name}"
        
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                enriched_attrs = {
                    **node.attributes,
                    "owner": data.get("owner"),
                    "description": data.get("description"),
                    "columns": data.get("columns"),
                    "row_count": data.get("row_count"),
                }
                enriched = node.model_copy(update={"attributes": enriched_attrs})
                graph.add_node(enriched)
            else:
                logger.debug(
                    "MyCatalog API returned %d for %s",
                    resp.status_code,
                    qualified_name,
                )
        except httpx.HTTPError as exc:
            logger.debug("HTTP error enriching %s: %s", qualified_name, exc)
    
    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the table in MyCatalog UI."""
        if not self._api_url:
            return None
        
        database = node.attributes.get("database")
        schema = node.attributes.get("schema")
        table = node.attributes.get("table")
        
        if not all([database, schema, table]):
            return None
        
        return f"{self._api_url}/databases/{database}/schemas/{schema}/tables/{table}"
```

### Step 2: Follow the DatabricksUCProvider Pattern

Use `lineage_bridge/catalogs/databricks_uc.py` as a reference. It demonstrates:

- Implementing all protocol methods
- Handling credentials (optional, graceful degradation)
- Async enrichment with httpx
- Building deep links to catalog UI
- Error handling and retry logic

Key patterns from `DatabricksUCProvider`:

```python
# build_node creates the initial node from Tableflow config
def build_node(self, ci_config, tableflow_node_id, topic_name, cluster_id, environment_id):
    uc_cfg = ci_config.get("unity_catalog", {})
    catalog_name = uc_cfg.get("catalog_name", "default")
    qualified = f"{catalog_name}.{cluster_id}.{topic_name}"
    # ... create node and edge
    return node, edge

# enrich fetches additional metadata from the catalog's API
async def enrich(self, graph):
    if not self._workspace_url or not self._token:
        return  # Gracefully skip if no credentials
    
    uc_nodes = graph.filter_by_type(NodeType.UC_TABLE)
    async with httpx.AsyncClient(...) as client:
        for node in uc_nodes:
            await self._enrich_node(client, graph, node)

# build_url creates deep links
def build_url(self, node):
    workspace_url = node.attributes.get("workspace_url")
    catalog, schema, table = node.qualified_name.split(".")
    return f"{workspace_url}/explore/data/{catalog}/{schema}/{table}"
```

### Step 3: Register the Provider

Add your provider to `lineage_bridge/catalogs/__init__.py`:

```python
from lineage_bridge.catalogs.my_catalog import MyCatalogProvider

__all__ = [
    "DatabricksUCProvider",
    "AWSGlueProvider",
    "MyCatalogProvider",  # Add here
]
```

Register it in the Tableflow extractor (`lineage_bridge/clients/tableflow.py`):

```python
from lineage_bridge.catalogs.my_catalog import MyCatalogProvider

# In _initialize_providers method
if self.settings.my_catalog_api_url:
    self._providers["MY_CATALOG"] = MyCatalogProvider(
        api_url=self.settings.my_catalog_api_url,
        api_token=self.settings.my_catalog_api_token,
    )
```

### Step 4: Add Settings

Add configuration to `lineage_bridge/config/settings.py`:

```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    # MyCatalog
    my_catalog_api_url: str | None = Field(None, env="LINEAGE_BRIDGE_MY_CATALOG_API_URL")
    my_catalog_api_token: SecretStr | None = Field(
        None, env="LINEAGE_BRIDGE_MY_CATALOG_API_TOKEN"
    )
```

### Step 5: Write Tests

Create `tests/unit/test_my_catalog.py`:

```python
import httpx
import pytest
import respx
from lineage_bridge.catalogs.my_catalog import MyCatalogProvider
from lineage_bridge.models.graph import LineageGraph, NodeType

@pytest.fixture()
def provider():
    return MyCatalogProvider(
        api_url="https://catalog.example.com",
        api_token="test-token",
    )

def test_build_node(provider):
    """Test building a catalog node from Tableflow config."""
    ci_config = {
        "my_catalog": {
            "database": "my_db",
            "schema": "my_schema",
        }
    }
    
    node, edge = provider.build_node(
        ci_config=ci_config,
        tableflow_node_id="confluent:tableflow_table:env-1:orders",
        topic_name="orders",
        cluster_id="lkc-123",
        environment_id="env-1",
    )
    
    assert node.qualified_name == "my_db.my_schema.orders"
    assert node.node_type == NodeType.MY_CATALOG_TABLE
    assert edge.edge_type == EdgeType.MATERIALIZES

@respx.mock
async def test_enrich(provider, sample_node):
    """Test enriching catalog nodes with API metadata."""
    graph = LineageGraph()
    node = sample_node(
        "my_db.my_schema.orders",
        NodeType.MY_CATALOG_TABLE,
        attributes={"database": "my_db", "schema": "my_schema", "table": "orders"}
    )
    graph.add_node(node)
    
    # Mock API response
    respx.get(
        "https://catalog.example.com/api/v1/tables/my_db.my_schema.orders"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "owner": "data-team",
                "description": "Customer orders",
                "row_count": 1000000,
            }
        )
    )
    
    await provider.enrich(graph)
    
    enriched = graph.get_node(node.node_id)
    assert enriched.attributes["owner"] == "data-team"
    assert enriched.attributes["row_count"] == 1000000
```

## Documentation Requirements

When adding a new extractor or catalog provider:

1. **Docstrings**: Add comprehensive docstrings to all public methods
2. **Type hints**: Use type hints for all function signatures
3. **README update**: Add your provider to the main README.md
4. **Architecture docs**: Update CLAUDE.md if adding new modules
5. **Example usage**: Provide example configuration in docs/

## Common Patterns

### Error Handling

```python
try:
    data = await self._fetch_resource(resource_id)
except httpx.HTTPError as exc:
    logger.warning("Failed to fetch resource %s: %s", resource_id, exc)
    return nodes, edges  # Return partial results
```

### Retry with Backoff

```python
for attempt in range(MAX_RETRIES):
    try:
        resp = await client.get(url)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            delay = BACKOFF_BASE * (2 ** attempt)
            await asyncio.sleep(delay)
            continue
    except httpx.HTTPError:
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(BACKOFF_BASE * (2 ** attempt))
```

### Pagination

```python
# ConfluentClient provides paginate() helper
async def _list_resources(self) -> list[dict[str, Any]]:
    path = "/api/v1/resources"
    return await self.paginate(path)

# Manual pagination for non-Confluent APIs
async def _paginate_custom(self, url: str) -> list[dict]:
    items = []
    next_token = None
    while True:
        params = {"next_token": next_token} if next_token else {}
        resp = await client.get(url, params=params)
        data = resp.json()
        items.extend(data.get("items", []))
        next_token = data.get("next_token")
        if not next_token:
            break
    return items
```

## Checklist

Before submitting your new extractor or catalog provider:

- [ ] Implements the protocol fully
- [ ] Follows existing patterns (KafkaAdminClient or DatabricksUCProvider)
- [ ] Includes comprehensive unit tests (>80% coverage)
- [ ] Uses proper logging (logger.info, logger.debug, logger.warning)
- [ ] Handles errors gracefully
- [ ] Updates documentation
- [ ] Registered in orchestrator or tableflow client
- [ ] Added settings to config/settings.py
- [ ] Runs `make lint` and `make format` successfully
- [ ] All tests pass: `make test`

---

**Questions?** Check the [Testing Guide](testing.md) or open a GitHub Discussion.
