"""Unit tests for lineage_bridge.clients.tableflow.TableflowClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from tests.conftest import load_fixture

from lineage_bridge.clients.tableflow import TableflowClient
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType

# ── shared constants ──────────────────────────────────────────────────────

API_KEY = "test-key"
API_SECRET = "test-secret"
ENV_ID = "env-abc123"
BASE_URL = "https://api.confluent.cloud"
TF_TOPICS_PATH = "/tableflow/v1/tableflow-topics"
CATALOG_INT_PATH = "/tableflow/v1/catalog-integrations"


CLUSTER_ID = "lkc-abc123"


@pytest.fixture()
def tf_client():
    return TableflowClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        environment_id=ENV_ID,
        cluster_ids=[CLUSTER_ID],
        base_url=BASE_URL,
        timeout=5.0,
    )


def _mock_paginated(path: str, fixture_name: str | None = None, data: list | None = None):
    """Helper to mock a paginated Confluent endpoint."""
    if fixture_name:
        fixture = load_fixture(fixture_name)
        resp_json = fixture
    else:
        resp_json = {"data": data or [], "metadata": {"next": None}}
    respx.get(f"{BASE_URL}{path}").mock(
        return_value=httpx.Response(200, json=resp_json)
    )


# ── extract() tests ──────────────────────────────────────────────────────


class TestTableflowExtract:

    @respx.mock
    async def test_extract_creates_tableflow_nodes(self, tf_client):
        """Tableflow topics produce TABLEFLOW_TABLE nodes."""
        _mock_paginated(TF_TOPICS_PATH, "tableflow_topics.json")
        _mock_paginated(CATALOG_INT_PATH, data=[])

        nodes, edges = await tf_client.extract()

        tf_nodes = [n for n in nodes if n.node_type == NodeType.TABLEFLOW_TABLE]
        assert len(tf_nodes) == 2
        assert all(n.system == SystemType.CONFLUENT for n in tf_nodes)

        display_names = {n.display_name for n in tf_nodes}
        assert display_names == {"orders-tableflow (tableflow)", "customers-tableflow (tableflow)"}

        # Verify attributes on first node
        orders_tf = next(n for n in tf_nodes if "orders" in n.display_name)
        assert orders_tf.attributes["table_formats"] == ["DELTA", "ICEBERG"]
        assert orders_tf.cluster_id == "lkc-abc123"

    @respx.mock
    async def test_materializes_edges(self, tf_client):
        """Each tableflow topic gets a MATERIALIZES edge from its kafka topic."""
        _mock_paginated(TF_TOPICS_PATH, "tableflow_topics.json")
        _mock_paginated(CATALOG_INT_PATH, data=[])

        nodes, edges = await tf_client.extract()

        mat_edges = [e for e in edges if e.edge_type == EdgeType.MATERIALIZES]
        assert len(mat_edges) == 2

        for edge in mat_edges:
            assert "kafka_topic" in edge.src_id
            assert "tableflow_table" in edge.dst_id

    @respx.mock
    async def test_unity_catalog_integration(self, tf_client):
        """When a UC catalog integration exists, a UC_TABLE node and edge are created."""
        _mock_paginated(TF_TOPICS_PATH, "tableflow_topics.json")
        _mock_paginated(CATALOG_INT_PATH, "catalog_integrations.json")

        nodes, edges = await tf_client.extract()

        uc_nodes = [n for n in nodes if n.node_type == NodeType.UC_TABLE]
        assert len(uc_nodes) == 2  # one per tableflow topic on that cluster
        assert all(n.system == SystemType.DATABRICKS for n in uc_nodes)

        # Check qualified name format: catalog.schema(cluster).table
        for n in uc_nodes:
            assert n.qualified_name.startswith("confluent_tableflow.lkc-abc123.")

        # UC edges: tableflow -> UC table
        uc_edges = [
            e for e in edges
            if e.edge_type == EdgeType.MATERIALIZES and "uc_table" in e.dst_id
        ]
        assert len(uc_edges) == 2

    @respx.mock
    async def test_aws_glue_integration(self, tf_client):
        """An AWS_GLUE catalog integration produces EXTERNAL UC_TABLE nodes."""
        glue_ci = {
            "data": [
                {
                    "id": "tci-glue-001",
                    "spec": {
                        "kafka_cluster": {"id": "lkc-abc123"},
                        "catalog_type": "AWS_GLUE",
                        "catalog_config": {
                            "aws_glue": {
                                "database_name": "my_glue_db",
                            }
                        },
                    },
                }
            ],
            "metadata": {"next": None},
        }
        _mock_paginated(TF_TOPICS_PATH, "tableflow_topics.json")
        respx.get(f"{BASE_URL}{CATALOG_INT_PATH}").mock(
            return_value=httpx.Response(200, json=glue_ci)
        )

        nodes, edges = await tf_client.extract()

        glue_nodes = [n for n in nodes if n.system == SystemType.EXTERNAL]
        assert len(glue_nodes) == 2
        for n in glue_nodes:
            assert n.node_type == NodeType.UC_TABLE
            assert "glue://" in n.qualified_name
            assert n.attributes["catalog_type"] == "AWS_GLUE"
            assert n.attributes["database"] == "my_glue_db"

    @respx.mock
    async def test_unknown_catalog_type_ignored(self, tf_client):
        """An unknown catalog type should not create extra nodes."""
        unknown_ci = {
            "data": [
                {
                    "id": "tci-x-001",
                    "spec": {
                        "kafka_cluster": {"id": "lkc-abc123"},
                        "catalog_type": "SNOWFLAKE_ICECAT",
                        "catalog_config": {},
                    },
                }
            ],
            "metadata": {"next": None},
        }
        _mock_paginated(TF_TOPICS_PATH, "tableflow_topics.json")
        respx.get(f"{BASE_URL}{CATALOG_INT_PATH}").mock(
            return_value=httpx.Response(200, json=unknown_ci)
        )

        nodes, edges = await tf_client.extract()

        # Only tableflow_table nodes, no UC/Glue nodes
        node_types = {n.node_type for n in nodes}
        assert NodeType.UC_TABLE not in node_types

    @respx.mock
    async def test_empty_topics_returns_empty(self, tf_client):
        """No tableflow topics means empty results."""
        _mock_paginated(TF_TOPICS_PATH, data=[])

        nodes, edges = await tf_client.extract()
        assert nodes == []
        assert edges == []

    @respx.mock
    async def test_api_error_returns_empty(self, tf_client, monkeypatch):
        """If the Tableflow API errors, extract returns empty gracefully."""
        monkeypatch.setattr("asyncio.sleep", _no_sleep)

        respx.get(f"{BASE_URL}{TF_TOPICS_PATH}").mock(
            return_value=httpx.Response(400, json={"error": "not enabled"})
        )

        nodes, edges = await tf_client.extract()
        assert nodes == []
        assert edges == []


# ── helpers ───────────────────────────────────────────────────────────────


async def _no_sleep(seconds: float) -> None:
    pass
