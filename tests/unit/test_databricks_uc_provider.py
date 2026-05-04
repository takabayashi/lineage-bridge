# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for DatabricksUCProvider."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.clients.databricks_sql import DatabricksSQLClient
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from tests.conftest import load_fixture

WORKSPACE_URL = "https://acme-prod.cloud.databricks.com"
TOKEN = "dapi-test-token-123"
WAREHOUSE_ID = "abc123def456"
LINEAGE_URL = f"{WORKSPACE_URL}/api/2.0/lineage-tracking/table-lineage"
STATEMENTS_URL = f"{WORKSPACE_URL}/api/2.0/sql/statements"


@pytest.fixture()
def provider():
    return DatabricksUCProvider(workspace_url=WORKSPACE_URL, token=TOKEN)


@pytest.fixture()
def provider_no_creds():
    return DatabricksUCProvider()


@pytest.fixture()
def sample_ci_config():
    return {
        "unity_catalog": {
            "catalog_name": "confluent_tableflow",
            "workspace_url": WORKSPACE_URL,
        }
    }


@pytest.fixture()
def flat_ci_config():
    """Config as returned by the real Confluent Tableflow API."""
    return {
        "kind": "Unity",
        "workspace_endpoint": WORKSPACE_URL,
        "catalog_name": "confluent_tableflow",
        "client_id": "sp-abc123",
        "client_secret": "********",
    }


@pytest.fixture()
def uc_graph():
    """Graph with a single UC_TABLE node."""
    graph = LineageGraph()
    graph.add_node(
        LineageNode(
            node_id="databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders_tableflow",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="confluent_tableflow.lkc-abc123.orders_tableflow",
            display_name="confluent_tableflow.lkc-abc123.orders_tableflow",
            environment_id="env-abc",
            cluster_id="lkc-abc123",
            attributes={
                "catalog_name": "confluent_tableflow",
                "schema_name": "lkc-abc123",
                "table_name": "orders_tableflow",
                "workspace_url": WORKSPACE_URL,
            },
        )
    )
    return graph


class TestBuildNode:
    def test_creates_correct_node_id(self, provider, sample_ci_config):
        node, _edge = provider.build_node(
            sample_ci_config,
            "confluent:tableflow_table:env-abc:lkc-abc123.orders",
            "orders",
            "lkc-abc123",
            "env-abc",
        )
        assert node.node_id == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders"

    def test_node_attributes(self, provider, sample_ci_config):
        node, _ = provider.build_node(sample_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert node.system == SystemType.DATABRICKS
        assert node.node_type == NodeType.CATALOG_TABLE
        assert node.attributes["catalog_name"] == "confluent_tableflow"
        assert node.attributes["schema_name"] == "lkc-abc123"
        assert node.attributes["table_name"] == "orders"
        assert node.attributes["workspace_url"] == WORKSPACE_URL

    def test_edge_type_materializes(self, provider, sample_ci_config):
        _, edge = provider.build_node(sample_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert edge.edge_type == EdgeType.MATERIALIZES
        assert edge.src_id == "tf-id"
        assert "uc_table" in edge.dst_id

    def test_default_catalog_name(self, provider):
        """When catalog_name is missing, defaults to confluent_tableflow."""
        node, _ = provider.build_node(
            {"unity_catalog": {}}, "tf-id", "orders", "lkc-abc123", "env-abc"
        )
        assert node.attributes["catalog_name"] == "confluent_tableflow"

    def test_flat_api_config_format(self, provider, flat_ci_config):
        """Flat config from real Confluent API (no 'unity_catalog' nesting)."""
        node, edge = provider.build_node(flat_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert node.node_id == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders"
        assert node.attributes["catalog_name"] == "confluent_tableflow"
        assert node.attributes["workspace_url"] == WORKSPACE_URL
        assert edge.edge_type == EdgeType.MATERIALIZES

    def test_dot_to_underscore_in_topic_name(self, provider, flat_ci_config):
        """Confluent replaces dots with underscores in UC table names."""
        node, _ = provider.build_node(
            flat_ci_config, "tf-id", "lineage_bridge.orders_v2", "lkc-abc123", "env-abc"
        )
        assert node.attributes["table_name"] == "lineage_bridge_orders_v2"
        assert node.qualified_name == "confluent_tableflow.lkc-abc123.lineage_bridge_orders_v2"
        assert "lineage_bridge_orders_v2" in node.node_id

    def test_schema_name_preserves_cluster_id(self, provider, flat_ci_config):
        """Tableflow uses the raw cluster ID (with hyphens) as the schema name."""
        node, _ = provider.build_node(flat_ci_config, "tf-id", "orders", "lkc-abc123", "env-abc")
        assert node.attributes["schema_name"] == "lkc-abc123"
        assert "lkc-abc123" in node.qualified_name


class TestBuildUrl:
    def test_with_workspace_url(self, provider):
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={"workspace_url": WORKSPACE_URL},
        )
        url = provider.build_url(node)
        assert url == f"{WORKSPACE_URL}/explore/data/catalog/schema/table"

    def test_no_workspace_url(self, provider_no_creds):
        """No URL on the provider AND none on the node → None."""
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={},
        )
        assert provider_no_creds.build_url(node) is None

    def test_invalid_qualified_name(self, provider):
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="no_dots_here",
            display_name="no_dots_here",
            attributes={"workspace_url": WORKSPACE_URL},
        )
        assert provider.build_url(node) is None

    def test_provider_url_overrides_stale_node_attribute(self, provider):
        """If Confluent stored a stale workspace URL on the node, the
        provider's settings-configured URL must win."""
        stale = "https://stale-workspace.cloud.databricks.com"
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={"workspace_url": stale},
        )
        url = provider.build_url(node)
        assert url == f"{WORKSPACE_URL}/explore/data/catalog/schema/table"
        assert stale not in url

    def test_notebook_url_falls_back_to_id_when_path_unknown(self, provider):
        """Without notebook_path we still produce an id-based URL — same as before."""
        node = LineageNode(
            node_id="databricks:notebook:env-abc:42",
            system=SystemType.DATABRICKS,
            node_type=NodeType.NOTEBOOK,
            qualified_name="42",
            display_name="Notebook 42",
            attributes={"notebook_id": 42},
        )
        assert provider.build_url(node) == f"{WORKSPACE_URL}/#notebook/42"

    def test_notebook_url_prefers_path_when_known(self, provider):
        """notebook_path wins over notebook_id — Databricks UI's path-based
        URL is more durable than the numeric form."""
        node = LineageNode(
            node_id="databricks:notebook:env-abc:42",
            system=SystemType.DATABRICKS,
            node_type=NodeType.NOTEBOOK,
            qualified_name="42",
            display_name="customer_order_summary",
            attributes={
                "notebook_id": 42,
                "notebook_path": "/Shared/lb-uc/customer_order_summary",
            },
        )
        assert provider.build_url(node) == (
            f"{WORKSPACE_URL}/#workspace/Shared/lb-uc/customer_order_summary"
        )

    def test_notebook_url_missing_id_and_path(self, provider):
        """Notebook node with neither id nor path → no URL."""
        node = LineageNode(
            node_id="databricks:notebook:env-abc:unknown",
            system=SystemType.DATABRICKS,
            node_type=NodeType.NOTEBOOK,
            qualified_name="unknown",
            display_name="Notebook",
        )
        assert provider.build_url(node) is None

    def test_strips_trailing_slash_on_provider_url(self):
        """Provider URLs with trailing slashes should not produce double slashes."""
        provider = DatabricksUCProvider(workspace_url=f"{WORKSPACE_URL}/", token=TOKEN)
        node = LineageNode(
            node_id="test",
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.table",
            display_name="catalog.schema.table",
            attributes={},
        )
        url = provider.build_url(node)
        assert url == f"{WORKSPACE_URL}/explore/data/catalog/schema/table"

    def test_build_node_prefers_provider_workspace_url(self, provider):
        """build_node should bake the provider's URL into the node, not Confluent's."""
        ci_config_with_stale = {
            "kind": "Unity",
            "workspace_endpoint": "https://stale-workspace.cloud.databricks.com",
            "catalog_name": "confluent_tableflow",
        }
        node, _ = provider.build_node(
            ci_config_with_stale, "tf-id", "orders", "lkc-abc123", "env-abc"
        )
        assert node.attributes["workspace_url"] == WORKSPACE_URL


class TestEnrich:
    def _mock_lineage_empty(self):
        """Mock the lineage API to return no downstreams."""
        respx.get(LINEAGE_URL).mock(return_value=httpx.Response(200, json={}))

    @respx.mock
    async def test_enrich_merges_attributes(self, provider, uc_graph, no_sleep):
        fixture = load_fixture("databricks_table.json")
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json=fixture))
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)

        node = uc_graph.nodes[0]
        assert node.attributes["owner"] == "confluent-tableflow-sp"
        assert node.attributes["table_type"] == "EXTERNAL"
        assert len(node.attributes["columns"]) == 4
        assert "storage_location" in node.attributes

    async def test_enrich_skips_without_credentials(self, provider_no_creds, uc_graph):
        """Enrichment should be a no-op when no credentials are configured."""
        original_attrs = dict(uc_graph.nodes[0].attributes)
        await provider_no_creds.enrich(uc_graph)
        assert uc_graph.nodes[0].attributes == original_attrs

    @respx.mock
    async def test_enrich_handles_401(self, provider, uc_graph, no_sleep):
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(401, json={"error": "unauthorized"}))
        self._mock_lineage_empty()

        # Should not raise
        await provider.enrich(uc_graph)
        # Attributes should be unchanged
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_404(self, provider, uc_graph, no_sleep):
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(404, json={"error": "not found"}))
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_retries_on_429(self, provider, uc_graph, no_sleep):
        """429 triggers retry; succeeds on second attempt."""
        fixture = load_fixture("databricks_table.json")
        route = respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        )
        route.side_effect = [
            httpx.Response(429, json={"error": "rate limited"}),
            httpx.Response(200, json=fixture),
        ]
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)

        node = uc_graph.nodes[0]
        assert node.attributes["owner"] == "confluent-tableflow-sp"
        assert route.call_count == 2

    @respx.mock
    async def test_enrich_exhausts_retries_on_503(self, provider, uc_graph, no_sleep):
        """503 three times exhausts retries without raising."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(503, json={"error": "unavailable"}))
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_http_error(self, provider, uc_graph, no_sleep):
        """Network-level errors are handled gracefully."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(side_effect=httpx.ConnectError("connection refused"))
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    @respx.mock
    async def test_enrich_handles_unexpected_status(self, provider, uc_graph, no_sleep):
        """Unexpected status codes (e.g. 418) are logged and skipped."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(418, json={"error": "teapot"}))
        self._mock_lineage_empty()

        await provider.enrich(uc_graph)
        assert "owner" not in uc_graph.nodes[0].attributes

    async def test_enrich_empty_graph(self, provider):
        """Enriching an empty graph is a no-op."""
        graph = LineageGraph()
        await provider.enrich(graph)
        assert graph.node_count == 0

    @respx.mock
    async def test_lineage_discovers_downstream_tables(self, provider, uc_graph, no_sleep):
        """Lineage API discovers derived downstream tables."""
        fixture = load_fixture("databricks_table.json")
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json=fixture))

        # Lineage for seed node returns a downstream table
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "order_summary",
                                "table_type": "TABLE",
                            }
                        }
                    ]
                },
            )
        )

        # Metadata enrichment for the discovered table
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.order_summary"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "owner": "test-user",
                    "table_type": "MANAGED",
                    "columns": [],
                    "storage_location": "s3://bucket/path",
                },
            )
        )

        # No further downstream from the discovered table
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.order_summary"},
        ).mock(return_value=httpx.Response(200, json={}))

        await provider.enrich(uc_graph)

        assert uc_graph.node_count == 2
        new_node = uc_graph.get_node(
            "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.order_summary"
        )
        assert new_node is not None
        assert new_node.attributes["derived"] is True
        assert new_node.attributes["owner"] == "test-user"

        # Check TRANSFORMS edge was created
        assert any(
            e.edge_type == EdgeType.TRANSFORMS
            and e.src_id
            == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders_tableflow"
            and e.dst_id
            == "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.order_summary"
            for e in uc_graph.edges
        )

    @respx.mock
    async def test_lineage_inserts_notebook_node_between_tables(self, provider, uc_graph, no_sleep):
        """notebookInfos becomes a NOTEBOOK node with CONSUMES + PRODUCES edges.

        When the lineage API attributes a downstream table to a notebook, we
        skip the table-to-table TRANSFORMS edge (the notebook hops are more
        precise) and wire ``source -CONSUMES→ notebook -PRODUCES→ derived``.
        """
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))

        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "order_summary",
                            },
                            "notebookInfos": [
                                {"workspace_id": 4242, "notebook_id": 99001},
                            ],
                        }
                    ]
                },
            )
        )
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.order_summary"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.order_summary"},
        ).mock(return_value=httpx.Response(200, json={}))

        await provider.enrich(uc_graph)

        notebook_id = "databricks:notebook:env-abc:99001"
        nb = uc_graph.get_node(notebook_id)
        assert nb is not None, "notebook node was not created"
        assert nb.node_type == NodeType.NOTEBOOK
        assert nb.system == SystemType.DATABRICKS
        assert nb.attributes["notebook_id"] == 99001
        assert nb.attributes["workspace_id"] == 4242

        edges = list(uc_graph.edges)
        seed_id = "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders_tableflow"
        derived_id = "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.order_summary"

        consumes = [
            e
            for e in edges
            if e.src_id == seed_id and e.dst_id == notebook_id and e.edge_type == EdgeType.CONSUMES
        ]
        produces = [
            e
            for e in edges
            if e.src_id == notebook_id
            and e.dst_id == derived_id
            and e.edge_type == EdgeType.PRODUCES
        ]
        transforms = [
            e
            for e in edges
            if e.src_id == seed_id and e.dst_id == derived_id and e.edge_type == EdgeType.TRANSFORMS
        ]
        assert len(consumes) == 1, "missing source -> notebook CONSUMES edge"
        assert len(produces) == 1, "missing notebook -> derived PRODUCES edge"
        assert not transforms, "TRANSFORMS edge should be skipped when a notebook hop exists"

    @respx.mock
    async def test_lineage_dedupes_notebooks_across_entries(self, provider, uc_graph, no_sleep):
        """A notebook referenced by two downstream entries is deduped to one node."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))

        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary_a",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 555}],
                        },
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary_b",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 555}],
                        },
                    ]
                },
            )
        )
        for tbl in ("summary_a", "summary_b"):
            respx.get(
                f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/confluent_tableflow.lkc-abc123.{tbl}"
            ).mock(return_value=httpx.Response(200, json={}))
            respx.get(
                LINEAGE_URL,
                params__contains={"table_name": f"confluent_tableflow.lkc-abc123.{tbl}"},
            ).mock(return_value=httpx.Response(200, json={}))

        await provider.enrich(uc_graph)

        notebook_nodes = [n for n in uc_graph.nodes if n.node_type == NodeType.NOTEBOOK]
        assert len(notebook_nodes) == 1, (
            f"notebook should be deduped across entries; got {[n.node_id for n in notebook_nodes]}"
        )
        # And it should have produced both downstream tables (one CONSUMES + two PRODUCES).
        produces = [
            e
            for e in uc_graph.edges
            if e.src_id == notebook_nodes[0].node_id and e.edge_type == EdgeType.PRODUCES
        ]
        assert len(produces) == 2

    @respx.mock
    async def test_notebook_attribution_via_upstreams_pass(self, provider, uc_graph, no_sleep):
        """Real Databricks API puts notebookInfos on the *upstream* side of the
        derived table, not the downstream side of the source. Our walker has to
        process upstreams of every visited node to catch it.
        """
        # Lineage of the seed: bare downstream (no notebookInfos), pointing at
        # the derived table — exactly what the live API returned in the
        # production smoke test that uncovered this bug.
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary",
                            }
                        }
                    ]
                },
            )
        )
        # Lineage of the discovered derived table: notebook + job attribution
        # appears here, on the upstreams side.
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/confluent_tableflow.lkc-abc123.summary"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.summary"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "upstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "orders_tableflow",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 77777}],
                            "jobInfos": [{"workspace_id": 1, "job_id": 8888}],
                        }
                    ]
                },
            )
        )
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/get").mock(
            return_value=httpx.Response(200, json={})
        )
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(200, json={})
        )

        await provider.enrich(uc_graph)

        nb = uc_graph.get_node("databricks:notebook:env-abc:77777")
        assert nb is not None, "notebook should be created from upstream attribution"
        assert nb.attributes["job_id"] == 8888

        seed_id = "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders_tableflow"
        derived_id = "databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.summary"

        # Source -> notebook (CONSUMES) and notebook -> derived (PRODUCES)
        assert any(
            e.src_id == seed_id and e.dst_id == nb.node_id and e.edge_type == EdgeType.CONSUMES
            for e in uc_graph.edges
        )
        assert any(
            e.src_id == nb.node_id and e.dst_id == derived_id and e.edge_type == EdgeType.PRODUCES
            for e in uc_graph.edges
        )

        # The bare-downstream pass laid down a TRANSFORMS edge initially;
        # the upstream pass should have removed it once the notebook hops
        # took over so the graph doesn't carry both paths.
        assert not any(
            e.src_id == seed_id and e.dst_id == derived_id and e.edge_type == EdgeType.TRANSFORMS
            for e in uc_graph.edges
        ), "TRANSFORMS edge should be replaced by the notebook hop after upstream pass"

    @respx.mock
    async def test_notebook_picks_up_job_id_from_job_infos(self, provider, uc_graph, no_sleep):
        """jobInfos.job_id in the same entry → attached to notebook attrs."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 99001}],
                            "jobInfos": [{"workspace_id": 1, "job_id": 4242}],
                        }
                    ]
                },
            )
        )
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/confluent_tableflow.lkc-abc123.summary"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.summary"},
        ).mock(return_value=httpx.Response(200, json={}))
        # Provider's _enrich_notebook_jobs will fan out to /jobs/get and
        # /jobs/runs/list — the merged metadata path is covered by its own
        # test below; here we just need the calls to succeed so attrs land.
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/get").mock(
            return_value=httpx.Response(
                200,
                json={
                    "job_id": 4242,
                    "settings": {
                        "name": "customer_order_summary_job",
                        "schedule": {
                            "quartz_cron_expression": "0 0/5 * ? * * *",
                            "timezone_id": "UTC",
                            "pause_status": "UNPAUSED",
                        },
                        "tasks": [
                            {
                                "task_key": "summarize",
                                "notebook_task": {
                                    "notebook_path": "/Shared/lb-uc/customer_order_summary"
                                },
                            }
                        ],
                    },
                },
            )
        )
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(
                200,
                json={
                    "runs": [
                        {
                            "run_id": 7,
                            "state": {
                                "life_cycle_state": "TERMINATED",
                                "result_state": "SUCCESS",
                            },
                            "start_time": 1700000000000,
                        }
                    ]
                },
            )
        )

        await provider.enrich(uc_graph)

        nb = uc_graph.get_node("databricks:notebook:env-abc:99001")
        assert nb is not None
        assert nb.attributes["job_id"] == 4242
        assert nb.attributes["job_name"] == "customer_order_summary_job"
        assert nb.attributes["notebook_path"] == "/Shared/lb-uc/customer_order_summary"
        assert nb.attributes["notebook_name"] == "customer_order_summary"
        assert nb.attributes["schedule_cron"] == "0 0/5 * ? * * *"
        assert nb.attributes["last_run_state"] == "TERMINATED"
        assert nb.attributes["last_run_result"] == "SUCCESS"
        assert nb.attributes["last_run_started_at_ms"] == 1700000000000
        # Display name is promoted to the notebook NAME (basename of its
        # path), not the job name.
        assert nb.display_name == "customer_order_summary"

    @respx.mock
    async def test_notebook_without_job_info_skips_job_enrichment(
        self, provider, uc_graph, no_sleep
    ):
        """Notebook without jobInfos → no job_id, no Jobs API calls."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 12345}],
                            # No jobInfos.
                        }
                    ]
                },
            )
        )
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/confluent_tableflow.lkc-abc123.summary"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.summary"},
        ).mock(return_value=httpx.Response(200, json={}))
        jobs_route = respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/get").mock(
            return_value=httpx.Response(200, json={})
        )
        runs_route = respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(200, json={})
        )

        await provider.enrich(uc_graph)

        nb = uc_graph.get_node("databricks:notebook:env-abc:12345")
        assert nb is not None
        assert "job_id" not in nb.attributes
        assert "job_name" not in nb.attributes
        assert nb.display_name == "Notebook 12345"
        assert not jobs_route.called, "should not hit /jobs/get without a job_id"
        assert not runs_route.called, "should not hit /jobs/runs/list without a job_id"

    @respx.mock
    async def test_notebook_job_404_keeps_notebook_alive(self, provider, uc_graph, no_sleep):
        """Job deleted between lineage walk and enrichment → notebook survives, no metadata."""
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/"
            "confluent_tableflow.lkc-abc123.orders_tableflow"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.orders_tableflow"},
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "downstreams": [
                        {
                            "tableInfo": {
                                "catalog_name": "confluent_tableflow",
                                "schema_name": "lkc-abc123",
                                "name": "summary",
                            },
                            "notebookInfos": [{"workspace_id": 1, "notebook_id": 7}],
                            "jobInfos": [{"workspace_id": 1, "job_id": 999}],
                        }
                    ]
                },
            )
        )
        respx.get(
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/tables/confluent_tableflow.lkc-abc123.summary"
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(
            LINEAGE_URL,
            params__contains={"table_name": "confluent_tableflow.lkc-abc123.summary"},
        ).mock(return_value=httpx.Response(200, json={}))
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/get").mock(
            return_value=httpx.Response(404, json={})
        )
        respx.get(f"{WORKSPACE_URL}/api/2.1/jobs/runs/list").mock(
            return_value=httpx.Response(404, json={})
        )

        await provider.enrich(uc_graph)

        nb = uc_graph.get_node("databricks:notebook:env-abc:7")
        assert nb is not None
        # job_id stays from the lineage walk; nothing else gets attached.
        assert nb.attributes["job_id"] == 999
        assert "job_name" not in nb.attributes
        assert "schedule_cron" not in nb.attributes


# ── Push Lineage Tests ───────────────────────────────────────────────


@pytest.fixture()
def push_graph():
    """Graph with upstream topic -> tableflow -> UC table chain."""
    graph = LineageGraph()

    topic = LineageNode(
        node_id="confluent:kafka_topic:env-abc:orders",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name="orders",
        display_name="orders",
        environment_id="env-abc",
        cluster_id="lkc-abc123",
    )
    tf_node = LineageNode(
        node_id="confluent:tableflow_table:env-abc:lkc-abc123.orders",
        system=SystemType.CONFLUENT,
        node_type=NodeType.TABLEFLOW_TABLE,
        qualified_name="lkc-abc123.orders",
        display_name="lkc-abc123.orders",
        environment_id="env-abc",
        cluster_id="lkc-abc123",
    )
    uc_node = LineageNode(
        node_id="databricks:uc_table:env-abc:confluent_tableflow.lkc-abc123.orders",
        system=SystemType.DATABRICKS,
        node_type=NodeType.CATALOG_TABLE,
        catalog_type="UNITY_CATALOG",
        qualified_name="confluent_tableflow.lkc-abc123.orders",
        display_name="confluent_tableflow.lkc-abc123.orders",
        environment_id="env-abc",
        cluster_id="lkc-abc123",
        attributes={"workspace_url": WORKSPACE_URL},
    )

    graph.add_node(topic)
    graph.add_node(tf_node)
    graph.add_node(uc_node)
    graph.add_edge(
        LineageEdge(
            src_id=topic.node_id,
            dst_id=tf_node.node_id,
            edge_type=EdgeType.MATERIALIZES,
        )
    )
    graph.add_edge(
        LineageEdge(
            src_id=tf_node.node_id,
            dst_id=uc_node.node_id,
            edge_type=EdgeType.MATERIALIZES,
        )
    )
    return graph


@pytest.fixture()
def sql_client():
    return DatabricksSQLClient(
        workspace_url=WORKSPACE_URL,
        token=TOKEN,
        warehouse_id=WAREHOUSE_ID,
    )


class TestPushLineage:
    @respx.mock
    async def test_push_lineage_sets_properties(self, provider, push_graph, sql_client):
        """push_lineage sets TBLPROPERTIES on UC tables."""
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": "stmt-1",
                    "status": {"state": "SUCCEEDED"},
                },
            )
        )

        result = await provider.push_lineage(
            push_graph, sql_client=sql_client, set_properties=True, set_comments=False
        )

        assert result.tables_updated == 1
        assert result.properties_set == 1
        assert result.comments_set == 0
        assert not result.errors

    @respx.mock
    async def test_push_lineage_sets_comments(self, provider, push_graph, sql_client):
        """push_lineage sets COMMENT ON TABLE."""
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": "stmt-1",
                    "status": {"state": "SUCCEEDED"},
                },
            )
        )

        result = await provider.push_lineage(
            push_graph, sql_client=sql_client, set_properties=False, set_comments=True
        )

        assert result.tables_updated == 1
        assert result.comments_set == 1
        assert result.properties_set == 0

    @respx.mock
    async def test_push_lineage_creates_bridge_table(self, provider, push_graph, sql_client):
        """push_lineage creates and populates bridge table when requested."""
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": "stmt-1",
                    "status": {"state": "SUCCEEDED"},
                },
            )
        )

        result = await provider.push_lineage(
            push_graph,
            sql_client=sql_client,
            set_properties=False,
            set_comments=False,
            create_bridge_table=True,
        )

        assert result.tables_updated == 1
        assert result.bridge_rows_inserted > 0

    @respx.mock
    async def test_push_lineage_skips_non_uc_nodes(self, provider, sql_client):
        """push_lineage returns empty result when no UC nodes exist."""
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="confluent:kafka_topic:env-abc:orders",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name="orders",
                display_name="orders",
            )
        )

        result = await provider.push_lineage(graph, sql_client=sql_client)
        assert result.tables_updated == 0

    @respx.mock
    async def test_push_lineage_routes_permission_denied_to_skipped(
        self, provider, push_graph, sql_client
    ):
        """PERMISSION_DENIED on a UC table goes into result.skipped, not errors.

        Multi-demo workflows commonly leave UC tables the current principal
        doesn't own in the graph; a hard error there would block the entire
        push. Skips let the caller surface a warning and move on.
        """
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": "stmt-1",
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "PERMISSION_DENIED"},
                    },
                },
            )
        )

        result = await provider.push_lineage(push_graph, sql_client=sql_client)
        assert result.tables_updated == 1
        assert not result.errors
        assert len(result.skipped) > 0
        assert any("PERMISSION_DENIED" in s for s in result.skipped)

    @respx.mock
    async def test_push_lineage_routes_unexpected_failures_to_errors(
        self, provider, push_graph, sql_client
    ):
        """Non-benign SQL failures (e.g., syntax errors) still go to errors."""
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": "stmt-1",
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "PARSE_SYNTAX_ERROR near 'TBLPROPERTIES'"},
                    },
                },
            )
        )

        result = await provider.push_lineage(push_graph, sql_client=sql_client)
        assert len(result.errors) > 0
        assert not result.skipped
