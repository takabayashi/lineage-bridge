# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Google Data Lineage catalog provider."""

from __future__ import annotations

import pytest

from lineage_bridge.catalogs.google_lineage import GoogleLineageProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


@pytest.fixture()
def provider():
    return GoogleLineageProvider(project_id="my-project", location="us")


class TestBuildNode:
    def test_creates_google_table_node_and_edge(self, provider):
        ci_config = {
            "google_bigquery": {
                "project_id": "my-project",
                "dataset_id": "my_dataset",
            }
        }
        node, edge = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="confluent:tableflow_table:env:lkc.orders",
            topic_name="orders",
            cluster_id="lkc-123",
            environment_id="env-abc",
        )

        assert node.system == SystemType.GOOGLE
        assert node.node_type == NodeType.CATALOG_TABLE
        assert node.qualified_name == "my-project.my_dataset.orders"
        assert node.attributes["project_id"] == "my-project"
        assert node.attributes["dataset_id"] == "my_dataset"
        assert node.attributes["table_name"] == "orders"
        assert node.environment_id == "env-abc"

        assert edge.edge_type == EdgeType.MATERIALIZES
        assert edge.src_id == "confluent:tableflow_table:env:lkc.orders"
        assert edge.dst_id == node.node_id

    def test_replaces_dots_and_dashes_in_table_name(self, provider):
        ci_config = {"google_bigquery": {"project_id": "p", "dataset_id": "d"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="my.topic-name",
            cluster_id="lkc",
            environment_id="env",
        )
        assert node.attributes["table_name"] == "my_topic_name"
        assert "my_topic_name" in node.qualified_name

    def test_defaults_project_from_provider(self, provider):
        ci_config = {"google_bigquery": {"dataset_id": "ds"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="t",
            cluster_id="lkc",
            environment_id="env",
        )
        assert node.attributes["project_id"] == "my-project"

    def test_defaults_dataset_from_cluster_id(self, provider):
        ci_config = {"google_bigquery": {"project_id": "p"}}
        node, _ = provider.build_node(
            ci_config=ci_config,
            tableflow_node_id="tf:id",
            topic_name="t",
            cluster_id="lkc-99",
            environment_id="env",
        )
        assert node.attributes["dataset_id"] == "lkc-99"


class TestBuildUrl:
    def test_builds_bigquery_console_url(self, provider):
        node = LineageNode(
            node_id="google:google_table:env:p.d.t",
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name="p.d.t",
            display_name="p.d.t",
            attributes={
                "project_id": "my-project",
                "dataset_id": "my_dataset",
                "table_name": "orders",
            },
        )
        url = provider.build_url(node)
        assert url is not None
        assert "console.cloud.google.com/bigquery" in url
        assert "project=my-project" in url
        assert "d=my_dataset" in url
        assert "t=orders" in url

    def test_returns_none_when_missing_attrs(self, provider):
        node = LineageNode(
            node_id="google:google_table:env:x",
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name="x",
            display_name="x",
            attributes={},
        )
        assert provider.build_url(node) is None


class TestProviderMetadata:
    def test_catalog_type(self, provider):
        assert provider.catalog_type == "GOOGLE_DATA_LINEAGE"

    # node_type / system_type were dropped from the protocol in Phase 1B
    # (ADR-021). All providers create CATALOG_TABLE nodes; the discriminator
    # is `catalog_type` (asserted above).


class TestEnrich:
    async def test_skips_without_project_id(self):
        provider = GoogleLineageProvider()
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="google:google_table:env:p.d.t",
                system=SystemType.GOOGLE,
                node_type=NodeType.CATALOG_TABLE,
                catalog_type="GOOGLE_DATA_LINEAGE",
                qualified_name="p.d.t",
                display_name="p.d.t",
            )
        )
        # Should not raise
        await provider.enrich(graph)

    async def test_skips_empty_graph(self, provider):
        graph = LineageGraph()
        await provider.enrich(graph)


class TestPushLineage:
    async def test_skips_without_project_id(self):
        provider = GoogleLineageProvider()
        graph = LineageGraph()
        result = await provider.push_lineage(graph)
        assert len(result.errors) == 1
        assert "project_id" in result.errors[0]

    async def test_skips_empty_google_nodes(self, provider):
        graph = LineageGraph()
        graph.add_node(
            LineageNode(
                node_id="confluent:kafka_topic:env:t",
                system=SystemType.CONFLUENT,
                node_type=NodeType.KAFKA_TOPIC,
                qualified_name="t",
                display_name="t",
            )
        )
        # Will fail on auth, but that's expected since no credentials
        result = await provider.push_lineage(graph)
        # No Google nodes → no errors about tables, but might have auth error
        assert result.tables_updated == 0


class TestNormalizeEventForGoogle:
    """Locks in the namespace mapping that lets Google's processor accept events."""

    @staticmethod
    def _make_event(inputs, outputs):
        # Minimal duck-typed event matching what graph_to_events produces.
        from types import SimpleNamespace

        def ds(ns, name):
            return SimpleNamespace(namespace=ns, name=name)

        return SimpleNamespace(
            inputs=[ds(ns, name) for ns, name in inputs],
            outputs=[ds(ns, name) for ns, name in outputs],
        )

    def test_confluent_input_rewritten_to_kafka(self):
        event = self._make_event(
            inputs=[("confluent://env-1/lkc-abc", "my_topic")],
            outputs=[("google://proj/ds", "proj.ds.tbl")],
        )
        GoogleLineageProvider._normalize_event_for_google(event)
        assert event.inputs[0].namespace == "kafka://lkc-abc"
        assert event.inputs[0].name == "my_topic"
        assert event.outputs[0].namespace == "bigquery"
        assert event.outputs[0].name == "proj.ds.tbl"

    def test_kafka_to_kafka_event_preserved(self):
        """Flink/ksqlDB-style events (kafka in, kafka out) survive normalization."""
        event = self._make_event(
            inputs=[("confluent://env-1/lkc-abc", "src_topic")],
            outputs=[("confluent://env-1/lkc-abc", "dst_topic")],
        )
        GoogleLineageProvider._normalize_event_for_google(event)
        assert len(event.inputs) == 1 and len(event.outputs) == 1
        assert event.inputs[0].namespace == "kafka://lkc-abc"
        assert event.outputs[0].namespace == "kafka://lkc-abc"

    def test_unrecognized_datasets_dropped(self):
        """UC/Glue/EXTERNAL datasets get dropped — Google can't link them."""
        event = self._make_event(
            inputs=[
                ("confluent://env-1/lkc-abc", "topic"),
                ("aws://us-east-1/db", "glue_table"),
            ],
            outputs=[
                ("databricks://workspace", "uc.tbl"),
                ("google://proj/ds", "proj.ds.tbl"),
            ],
        )
        GoogleLineageProvider._normalize_event_for_google(event)
        assert [d.namespace for d in event.inputs] == ["kafka://lkc-abc"]
        assert [d.namespace for d in event.outputs] == ["bigquery"]


# ── _walk_downstream_lineage ─────────────────────────────────────────────


class TestWalkDownstreamLineage:
    """Walk forward via Google Data Lineage API to surface BQ-side processes."""

    @staticmethod
    def _bq_table(table: str, project: str = "my-project", dataset: str = "ds") -> LineageNode:
        qualified = f"{project}.{dataset}.{table}"
        return LineageNode(
            node_id=f"google:google_table:my-project:{qualified}",
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name=qualified,
            display_name=qualified,
            attributes={"project_id": project, "dataset_id": dataset, "table_name": table},
        )

    @pytest.fixture()
    def fake_client(self):
        """Mock httpx.AsyncClient.post/get returning canned Lineage API responses.

        Each test sets `client.fake_state["search"|"batch"|"process"]` before
        calling `_walk_downstream_lineage`.
        """
        from unittest.mock import AsyncMock, MagicMock

        client = MagicMock()
        client.fake_state: dict = {}

        async def _post(url: str, json: dict):  # noqa: A002
            resp = MagicMock()
            resp.status_code = 200
            if url.endswith(":searchLinks"):
                src_fqn = json["source"]["fullyQualifiedName"]
                resp.json = MagicMock(
                    return_value={"links": client.fake_state["search"].get(src_fqn, [])}
                )
            elif url.endswith(":batchSearchLinkProcesses"):
                resp.json = MagicMock(return_value={"processLinks": client.fake_state["batch"]})
            else:
                resp.status_code = 404
                resp.json = MagicMock(return_value={})
            return resp

        async def _get(url: str):
            resp = MagicMock()
            proc_id = url.rsplit("/", 1)[-1]
            data = client.fake_state["process"].get(proc_id)
            if data is None:
                resp.status_code = 404
            else:
                resp.status_code = 200
                resp.json = MagicMock(return_value=data)
            return resp

        client.post = AsyncMock(side_effect=_post)
        client.get = AsyncMock(side_effect=_get)
        return client

    async def test_emits_query_node_and_two_edges_for_known_target(
        self, provider, fake_client
    ):
        """Single process linking a known source to a known target → query node + 2 TRANSFORMS edges."""
        graph = LineageGraph()
        src = self._bq_table("enriched_orders")
        tgt = self._bq_table("joined_orders")
        graph.add_node(src)
        graph.add_node(tgt)

        fake_client.fake_state = {
            "search": {
                "bigquery:my-project.ds.enriched_orders": [
                    {
                        "name": "projects/p/locations/us/links/L1",
                        "source": {"fullyQualifiedName": "bigquery:my-project.ds.enriched_orders"},
                        "target": {"fullyQualifiedName": "bigquery:my-project.ds.joined_orders"},
                    }
                ],
                "bigquery:my-project.ds.joined_orders": [],
            },
            "batch": [
                {
                    "process": "projects/p/locations/us/processes/P1",
                    "links": [{"link": "projects/p/locations/us/links/L1"}],
                }
            ],
            "process": {
                "P1": {
                    "displayName": "BigQuery scheduled query: joined_orders CTAS",
                    "attributes": {"sql": "SELECT 1", "schedule": "every 1 hours"},
                    "origin": {
                        "sourceType": "CUSTOM",
                        "name": "custom:bigquery-query:proj.joined_orders_ctas",
                    },
                }
            },
        }

        await provider._walk_downstream_lineage(fake_client, graph)

        query_nodes = [n for n in graph.nodes if n.node_type == NodeType.CATALOG_QUERY]
        assert len(query_nodes) == 1
        q = query_nodes[0]
        assert q.catalog_type == "GOOGLE_DATA_LINEAGE"
        assert q.attributes["sql"] == "SELECT 1"
        assert q.attributes["catalog_fqn"] == "custom:bigquery-query:proj.joined_orders_ctas"

        edges = list(graph.edges)
        assert any(
            e.src_id == src.node_id and e.dst_id == q.node_id and e.edge_type == EdgeType.TRANSFORMS
            for e in edges
        )
        assert any(
            e.src_id == q.node_id and e.dst_id == tgt.node_id and e.edge_type == EdgeType.TRANSFORMS
            for e in edges
        )

    async def test_unknown_target_table_added_as_catalog_table(self, provider, fake_client):
        """Walk discovers a downstream BQ table not yet in the graph → adds CATALOG_TABLE."""
        graph = LineageGraph()
        src = self._bq_table("enriched_orders")
        graph.add_node(src)

        fake_client.fake_state = {
            "search": {
                "bigquery:my-project.ds.enriched_orders": [
                    {
                        "name": "projects/p/locations/us/links/L1",
                        "source": {"fullyQualifiedName": "bigquery:my-project.ds.enriched_orders"},
                        "target": {"fullyQualifiedName": "bigquery:my-project.ds.brand_new_table"},
                    }
                ]
            },
            "batch": [
                {
                    "process": "projects/p/locations/us/processes/P1",
                    "links": [{"link": "projects/p/locations/us/links/L1"}],
                }
            ],
            "process": {"P1": {"displayName": "Q1", "attributes": {}, "origin": {}}},
        }

        await provider._walk_downstream_lineage(fake_client, graph)

        catalog_tables = [n for n in graph.nodes if n.node_type == NodeType.CATALOG_TABLE]
        new_table = next(
            (n for n in catalog_tables if n.qualified_name == "my-project.ds.brand_new_table"),
            None,
        )
        assert new_table is not None
        assert new_table.catalog_type == "GOOGLE_DATA_LINEAGE"
        assert new_table.attributes["table_name"] == "brand_new_table"

    async def test_idempotent_on_re_run(self, provider, fake_client):
        """Calling the walk twice with the same data must not duplicate nodes/edges."""
        graph = LineageGraph()
        src = self._bq_table("enriched_orders")
        tgt = self._bq_table("joined_orders")
        graph.add_node(src)
        graph.add_node(tgt)

        fake_client.fake_state = {
            "search": {
                "bigquery:my-project.ds.enriched_orders": [
                    {
                        "name": "projects/p/locations/us/links/L1",
                        "source": {"fullyQualifiedName": "bigquery:my-project.ds.enriched_orders"},
                        "target": {"fullyQualifiedName": "bigquery:my-project.ds.joined_orders"},
                    }
                ],
                "bigquery:my-project.ds.joined_orders": [],
            },
            "batch": [
                {
                    "process": "projects/p/locations/us/processes/P1",
                    "links": [{"link": "projects/p/locations/us/links/L1"}],
                }
            ],
            "process": {"P1": {"displayName": "Q1", "attributes": {}, "origin": {}}},
        }

        await provider._walk_downstream_lineage(fake_client, graph)
        nodes_after_first = list(graph.nodes)
        edges_after_first = list(graph.edges)

        await provider._walk_downstream_lineage(fake_client, graph)
        nodes_after_second = list(graph.nodes)
        edges_after_second = list(graph.edges)

        assert len(nodes_after_first) == len(nodes_after_second)
        assert len(edges_after_first) == len(edges_after_second)
