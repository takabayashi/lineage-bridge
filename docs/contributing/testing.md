# Testing

LineageBridge uses pytest for all testing. This guide covers how to run tests, write new tests, and understand the testing infrastructure.

## Running Tests

### Run All Tests

```bash
# Using make
make test

# Using uv directly
uv run pytest tests/ -v

# Using pytest directly (if venv is activated)
pytest tests/ -v
```

### Run Specific Test Suites

```bash
# Unit tests only
uv run pytest tests/unit/ -v

# Integration tests only (requires live Confluent Cloud credentials)
uv run pytest tests/integration/ -v

# Specific test file
uv run pytest tests/unit/test_flink_client.py -v

# Specific test function
uv run pytest tests/unit/test_flink_client.py::TestFlinkExtract::test_extract_with_statements -v
```

### Run with Coverage

```bash
# Full coverage report
uv run pytest --cov=lineage_bridge --cov-report=term-missing

# Coverage for specific module
uv run pytest --cov=lineage_bridge.clients.flink --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=lineage_bridge --cov-report=html
open htmlcov/index.html  # View in browser
```

### Run Tests in Parallel

Speed up test runs with pytest-xdist (install separately):

```bash
uv pip install pytest-xdist
uv run pytest tests/ -n auto
```

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── fixtures/                # JSON fixture data for mocking API responses
│   ├── flink_statements.json
│   ├── kafka_topics.json
│   └── ...
├── unit/                    # Unit tests (mocked HTTP, no external deps)
│   ├── test_flink_client.py
│   ├── test_kafka_admin.py
│   ├── test_databricks_uc.py
│   └── ...
└── integration/             # Integration tests (live APIs, manual trigger)
    ├── test_full_extraction.py
    └── ...
```

### Unit Tests

Fast, isolated tests using mocked HTTP responses:

- No external dependencies
- Use `respx` to mock HTTP calls
- Run on every commit
- Located in `tests/unit/`

### Integration Tests

End-to-end tests against real APIs:

- Require live Confluent Cloud credentials
- Slower, for manual validation
- Skip in CI by default
- Located in `tests/integration/`

## pytest Configuration

Configuration is in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

Key settings:

- **asyncio_mode = "auto"**: Automatically detect and run async tests without `@pytest.mark.asyncio` decorator
- **testpaths**: Only search for tests in the `tests/` directory

## Fixtures

### Shared Fixtures (conftest.py)

Located in `tests/conftest.py`, available to all tests:

#### `fixtures_dir`

Path to the `tests/fixtures/` directory:

```python
def test_loads_fixture_data(fixtures_dir):
    path = fixtures_dir / "flink_statements.json"
    data = json.loads(path.read_text())
    assert "data" in data
```

#### `load_fixture(name)`

Helper function to load JSON fixtures:

```python
def test_with_fixture():
    from tests.conftest import load_fixture
    data = load_fixture("kafka_topics.json")
    assert isinstance(data, dict)
```

#### `sample_node(name, node_type, **kwargs)`

Factory for creating test nodes:

```python
def test_creates_node(sample_node):
    node = sample_node("orders", NodeType.KAFKA_TOPIC)
    assert node.display_name == "orders"
    assert node.node_type == NodeType.KAFKA_TOPIC
    
    # With custom attributes
    node = sample_node(
        "my-connector",
        NodeType.CONNECTOR,
        environment_id="env-xyz",
        attributes={"status": "RUNNING"}
    )
```

#### `sample_edge(src_id, dst_id, edge_type, **kwargs)`

Factory for creating test edges:

```python
def test_creates_edge(sample_edge):
    edge = sample_edge(
        "confluent:kafka_topic:env-1:orders",
        "confluent:kafka_topic:env-1:enriched_orders",
        EdgeType.TRANSFORMS
    )
    assert edge.edge_type == EdgeType.TRANSFORMS
```

#### `sample_graph`

Pre-built realistic lineage graph for testing:

```python
def test_graph_operations(sample_graph):
    topics = sample_graph.filter_by_type(NodeType.KAFKA_TOPIC)
    assert len(topics) > 0
    
    upstream = sample_graph.get_upstream("confluent:kafka_topic:env-abc123:enriched_orders")
    assert len(upstream) > 0
```

#### `no_sleep`

Patches `asyncio.sleep` to return immediately:

```python
async def test_retry_logic(no_sleep):
    # No delays during retries
    result = await some_function_with_retries()
    assert result is not None
```

## Writing Tests

### Unit Test Example

Using `respx` to mock HTTP calls:

```python
import httpx
import pytest
import respx
from lineage_bridge.clients.kafka_admin import KafkaAdminClient
from lineage_bridge.models.graph import NodeType

@respx.mock
async def test_kafka_admin_extract():
    """Test KafkaAdminClient extracts topics and consumer groups."""
    
    # Mock the topics endpoint
    respx.get("https://api.confluent.cloud/kafka/v3/clusters/lkc-123/topics").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"topic_name": "orders", "partitions_count": 6},
                    {"topic_name": "customers", "partitions_count": 3},
                ],
                "metadata": {"next": None}
            }
        )
    )
    
    # Mock the consumer groups endpoint
    respx.get("https://api.confluent.cloud/kafka/v3/clusters/lkc-123/consumer-groups").mock(
        return_value=httpx.Response(
            200,
            json={"data": [], "metadata": {"next": None}}
        )
    )
    
    # Create client and extract
    client = KafkaAdminClient(
        base_url="https://api.confluent.cloud",
        api_key="test-key",
        api_secret="test-secret",
        cluster_id="lkc-123",
        environment_id="env-abc",
    )
    
    nodes, edges = await client.extract()
    
    # Assertions
    assert len(nodes) == 2
    topic_nodes = [n for n in nodes if n.node_type == NodeType.KAFKA_TOPIC]
    assert len(topic_nodes) == 2
    assert {n.display_name for n in topic_nodes} == {"orders", "customers"}
```

### Testing Async Code

With `asyncio_mode = "auto"`, async tests work automatically:

```python
async def test_async_function():
    result = await some_async_function()
    assert result is not None
```

No `@pytest.mark.asyncio` decorator needed!

### Using Fixtures

Combine fixtures to build complex test scenarios:

```python
def test_graph_with_custom_nodes(sample_node, sample_edge):
    from lineage_bridge.models.graph import LineageGraph
    
    graph = LineageGraph()
    
    # Add nodes
    topic = sample_node("events", NodeType.KAFKA_TOPIC)
    connector = sample_node("s3-sink", NodeType.CONNECTOR)
    graph.add_node(topic)
    graph.add_node(connector)
    
    # Add edge
    edge = sample_edge(topic.node_id, connector.node_id, EdgeType.CONSUMES)
    graph.add_edge(edge)
    
    # Test
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
```

### Parameterized Tests

Test multiple cases with `pytest.mark.parametrize`:

```python
import pytest
from lineage_bridge.clients.flink import FlinkClient

@pytest.mark.parametrize(
    "sql,expected_sources,expected_sinks",
    [
        (
            "INSERT INTO output SELECT * FROM input",
            {"input"},
            {"output"}
        ),
        (
            "INSERT INTO enriched SELECT * FROM orders JOIN customers",
            {"orders", "customers"},
            {"enriched"}
        ),
        (
            "CREATE TABLE result AS SELECT * FROM source",
            {"source"},
            {"result"}
        ),
    ]
)
def test_flink_sql_parser(sql, expected_sources, expected_sinks):
    sources, sinks = FlinkClient._parse_flink_sql(sql)
    assert sources == expected_sources
    assert sinks == expected_sinks
```

## HTTP Mocking with respx

### Basic Mocking

```python
import httpx
import respx

@respx.mock
async def test_http_client():
    respx.get("https://api.example.com/data").mock(
        return_value=httpx.Response(200, json={"status": "ok"})
    )
    
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://api.example.com/data")
        data = resp.json()
        assert data["status"] == "ok"
```

### Pattern Matching

Match URLs with patterns:

```python
@respx.mock
async def test_pattern_matching():
    respx.get(url__regex=r"https://api.example.com/items/\d+").mock(
        return_value=httpx.Response(200, json={"id": 123})
    )
    
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://api.example.com/items/456")
        assert resp.status_code == 200
```

### Multiple Responses (Pagination)

```python
@respx.mock
async def test_pagination():
    respx.get("https://api.example.com/items", params={"page": 1}).mock(
        return_value=httpx.Response(200, json={"data": [1, 2], "next": 2})
    )
    respx.get("https://api.example.com/items", params={"page": 2}).mock(
        return_value=httpx.Response(200, json={"data": [3, 4], "next": None})
    )
    
    # Test paginated client
    items = await fetch_all_items()
    assert items == [1, 2, 3, 4]
```

## Coverage Requirements

- **Minimum**: 80% coverage for new code
- **Target**: 90% coverage for core modules
- **Critical paths**: 100% coverage for extractors and catalog providers

Check coverage before submitting a PR:

```bash
uv run pytest --cov=lineage_bridge --cov-report=term-missing
```

Look for uncovered lines in the report:

```
Name                                    Stmts   Miss  Cover   Missing
---------------------------------------------------------------------
lineage_bridge/clients/flink.py           145      8    94%   67-70, 122-125
```

## Integration Tests

Integration tests run against live APIs and are skipped by default.

### Running Integration Tests

Requires live Confluent Cloud credentials in `.env`:

```bash
# Run all integration tests
uv run pytest tests/integration/ -v

# Run specific integration test
uv run pytest tests/integration/test_full_extraction.py -v
```

### Writing Integration Tests

Mark tests as integration tests:

```python
import pytest

@pytest.mark.integration
async def test_live_kafka_admin():
    """Test KafkaAdminClient against real Confluent Cloud cluster."""
    from lineage_bridge.config.settings import Settings
    from lineage_bridge.clients.kafka_admin import KafkaAdminClient
    
    settings = Settings()
    client = KafkaAdminClient(
        base_url="https://api.confluent.cloud",
        api_key=settings.cloud_api_key,
        api_secret=settings.cloud_api_secret.get_secret_value(),
        cluster_id="lkc-real-cluster",
        environment_id="env-real-env",
    )
    
    nodes, edges = await client.extract()
    assert len(nodes) > 0
```

## Troubleshooting

### Tests hang or timeout

Check for:
- Missing `@respx.mock` decorator
- Unmocked HTTP calls
- Infinite loops in retry logic

Use `--timeout=10` to fail fast:

```bash
uv pip install pytest-timeout
uv run pytest tests/ --timeout=10
```

### Fixture not found

Ensure fixtures are defined in `conftest.py` or imported properly:

```python
# In conftest.py
@pytest.fixture()
def my_fixture():
    return "value"

# In test file (no import needed, pytest auto-discovers)
def test_with_fixture(my_fixture):
    assert my_fixture == "value"
```

### Import errors in tests

Reinstall in editable mode:

```bash
uv pip install -e ".[dev]"
```

### respx not matching requests

Enable debug logging to see what's being mocked:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Best Practices

1. **One assertion concept per test** - test one thing at a time
2. **Use descriptive test names** - `test_extract_with_tumble_windowing` not `test_1`
3. **Arrange-Act-Assert** - clearly separate setup, execution, and validation
4. **Mock external dependencies** - unit tests should never hit real APIs
5. **Test edge cases** - empty lists, None values, error responses
6. **Keep tests fast** - mock sleeps, use minimal fixture data
7. **Avoid test interdependencies** - each test should run independently

---

**Next**: Learn how to [add new extractors](adding-extractors.md) with proper test coverage.
