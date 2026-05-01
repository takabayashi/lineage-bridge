# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for the LineageBridge REST API."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from lineage_bridge.api.app import create_app
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


@pytest.fixture()
def auth_client():
    app = create_app(api_key="test-secret")
    return TestClient(app)


def _make_graph() -> LineageGraph:
    """Build a small test graph."""
    g = LineageGraph()
    g.add_node(
        LineageNode(
            node_id="confluent:kafka_topic:env:orders",
            system=SystemType.CONFLUENT,
            node_type=NodeType.KAFKA_TOPIC,
            qualified_name="orders",
            display_name="orders",
            environment_id="env",
            cluster_id="lkc",
            attributes={"partitions": 6},
        )
    )
    g.add_node(
        LineageNode(
            node_id="confluent:connector:env:pg-source",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="pg-source",
            display_name="pg-source",
            environment_id="env",
            cluster_id="lkc",
        )
    )
    g.add_edge(
        LineageEdge(
            src_id="confluent:connector:env:pg-source",
            dst_id="confluent:kafka_topic:env:orders",
            edge_type=EdgeType.PRODUCES,
        )
    )
    return g


def _make_event(job_name="test-connector", input_name="input-t", output_name="output-t"):
    return {
        "eventTime": datetime.now(UTC).isoformat(),
        "eventType": "COMPLETE",
        "run": {"runId": f"run-{job_name}"},
        "job": {"namespace": "confluent://env/lkc", "name": job_name},
        "inputs": [{"namespace": "confluent://env/lkc", "name": input_name}],
        "outputs": [{"namespace": "confluent://env/lkc", "name": output_name}],
    }


# ── Meta ───────────────────────────────────────────────────────────────────


class TestMeta:
    def test_health(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}

    def test_version(self, client):
        r = client.get("/api/v1/version")
        assert r.status_code == 200
        assert "version" in r.json()

    def test_catalogs(self, client):
        r = client.get("/api/v1/catalogs")
        assert r.status_code == 200
        types = [c["catalog_type"] for c in r.json()]
        assert "UNITY_CATALOG" in types
        assert "AWS_GLUE" in types

    def test_openapi_yaml(self, client):
        r = client.get("/api/v1/openapi.yaml")
        assert r.status_code == 200
        assert "openapi: 3.1" in r.text


# ── Auth ───────────────────────────────────────────────────────────────────


class TestAuth:
    def test_no_auth_required_in_dev_mode(self, client):
        r = client.get("/api/v1/graphs")
        assert r.status_code == 200

    def test_auth_rejects_missing_key(self, auth_client):
        r = auth_client.get("/api/v1/graphs")
        assert r.status_code == 401

    def test_auth_rejects_wrong_key(self, auth_client):
        r = auth_client.get("/api/v1/graphs", headers={"X-API-Key": "wrong"})
        assert r.status_code == 401

    def test_auth_accepts_correct_key(self, auth_client):
        r = auth_client.get("/api/v1/graphs", headers={"X-API-Key": "test-secret"})
        assert r.status_code == 200

    def test_health_is_public(self, auth_client):
        r = auth_client.get("/api/v1/health")
        assert r.status_code == 200


# ── Graphs ─────────────────────────────────────────────────────────────────


class TestGraphs:
    def test_list_empty(self, client):
        r = client.get("/api/v1/graphs")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_and_get(self, client):
        r = client.post("/api/v1/graphs")
        assert r.status_code == 201
        graph_id = r.json()["graph_id"]

        r = client.get(f"/api/v1/graphs/{graph_id}")
        assert r.status_code == 200
        assert r.json()["stats"]["node_count"] == 0

    def test_delete(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]

        r = client.delete(f"/api/v1/graphs/{graph_id}")
        assert r.status_code == 200

        r = client.get(f"/api/v1/graphs/{graph_id}")
        assert r.status_code == 404

    def test_import_export(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]

        g = _make_graph()
        r = client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())
        assert r.status_code == 200
        assert r.json()["nodes_imported"] == 2

        r = client.get(f"/api/v1/graphs/{graph_id}/export")
        assert r.status_code == 200
        assert len(r.json()["nodes"]) == 2

    def test_not_found(self, client):
        r = client.get("/api/v1/graphs/nonexistent")
        assert r.status_code == 404


# ── Nodes & Edges ──────────────────────────────────────────────────────────


class TestNodesEdges:
    def test_add_node(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]

        node = {
            "node_id": "confluent:kafka_topic:env:test",
            "system": "confluent",
            "node_type": "kafka_topic",
            "qualified_name": "test",
            "display_name": "test",
        }
        r = client.post(f"/api/v1/graphs/{graph_id}/nodes", json=node)
        assert r.status_code == 201

        r = client.get(f"/api/v1/graphs/{graph_id}/nodes")
        assert r.json()["total"] == 1

    def test_filter_nodes_by_type(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get(f"/api/v1/graphs/{graph_id}/nodes?type=kafka_topic")
        assert r.json()["total"] == 1
        assert r.json()["nodes"][0]["qualified_name"] == "orders"

    def test_add_edge(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get(f"/api/v1/graphs/{graph_id}/edges")
        assert r.json()["total"] == 1

    def test_add_edge_missing_node(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]

        edge = {
            "src_id": "nonexistent",
            "dst_id": "also-nonexistent",
            "edge_type": "produces",
        }
        r = client.post(f"/api/v1/graphs/{graph_id}/edges", json=edge)
        assert r.status_code == 400


# ── Traversal ──────────────────────────────────────────────────────────────


class TestTraversal:
    def test_upstream(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get(f"/api/v1/graphs/{graph_id}/query/upstream/confluent:kafka_topic:env:orders")
        assert r.status_code == 200
        assert len(r.json()["nodes"]) == 1  # pg-source connector

    def test_downstream(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get(
            f"/api/v1/graphs/{graph_id}/query/downstream/confluent:connector:env:pg-source"
        )
        assert r.status_code == 200
        assert len(r.json()["nodes"]) == 1  # orders topic

    def test_node_not_found(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]

        r = client.get(f"/api/v1/graphs/{graph_id}/query/upstream/nonexistent")
        assert r.status_code == 404


# ── OpenLineage Events ────────────────────────────────────────────────────


class TestLineageEvents:
    def test_empty_events(self, client):
        r = client.get("/api/v1/lineage/events")
        assert r.status_code == 200
        assert r.json() == []

    def test_ingest_and_query(self, client):
        evt = _make_event()
        r = client.post("/api/v1/lineage/events", json=[evt])
        assert r.status_code == 201
        assert r.json()["events_stored"] == 1

        r = client.get("/api/v1/lineage/events")
        assert len(r.json()) == 1

    def test_query_by_run_id(self, client):
        evt = _make_event()
        client.post("/api/v1/lineage/events", json=[evt])

        r = client.get("/api/v1/lineage/events/run-test-connector")
        assert r.status_code == 200
        assert len(r.json()) == 1

    def test_run_id_not_found(self, client):
        r = client.get("/api/v1/lineage/events/nonexistent")
        assert r.status_code == 404

    def test_ingest_merges_into_graph(self, client):
        """Events ingested via POST are merged into the graph store."""
        evt = _make_event()
        r = client.post("/api/v1/lineage/events", json=[evt])
        assert r.status_code == 201
        graph_id = r.json()["graph_id"]
        assert graph_id is not None

        # The graph should have nodes from the ingested event
        r = client.get(f"/api/v1/graphs/{graph_id}")
        assert r.status_code == 200
        assert r.json()["stats"]["node_count"] > 0

    def test_ingest_multiple_merges_into_same_graph(self, client):
        """Multiple ingestions merge into the same graph."""
        r1 = client.post("/api/v1/lineage/events", json=[_make_event(job_name="j1")])
        gid1 = r1.json()["graph_id"]

        r2 = client.post("/api/v1/lineage/events", json=[_make_event(job_name="j2")])
        gid2 = r2.json()["graph_id"]

        assert gid1 == gid2  # merged into same graph


class TestDatasets:
    def test_empty(self, client):
        r = client.get("/api/v1/lineage/datasets")
        assert r.json() == []

    def test_datasets_from_events(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])

        r = client.get("/api/v1/lineage/datasets")
        names = {d["name"] for d in r.json()}
        assert "input-t" in names
        assert "output-t" in names


class TestJobs:
    def test_empty(self, client):
        r = client.get("/api/v1/lineage/jobs")
        assert r.json() == []

    def test_jobs_from_events(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])

        r = client.get("/api/v1/lineage/jobs")
        assert len(r.json()) == 1
        assert r.json()[0]["name"] == "test-connector"


# ── Graph Views ────────────────────────────────────────────────────────────


class TestGraphViews:
    def test_confluent_view_empty(self, client):
        r = client.get("/api/v1/graphs/confluent/view")
        assert r.status_code == 200
        assert r.json()["events"] == []

    def test_confluent_view_with_data(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get("/api/v1/graphs/confluent/view")
        assert r.status_code == 200
        assert len(r.json()["events"]) > 0

    def test_enriched_view(self, client):
        r = client.post("/api/v1/graphs")
        graph_id = r.json()["graph_id"]
        g = _make_graph()
        client.post(f"/api/v1/graphs/{graph_id}/import", json=g.to_dict())

        r = client.get("/api/v1/graphs/enriched/view")
        assert r.status_code == 200
        assert len(r.json()["events"]) > 0


# ── Tasks ──────────────────────────────────────────────────────────────────


class TestTasks:
    def test_list_empty(self, client):
        r = client.get("/api/v1/tasks")
        assert r.status_code == 200
        assert r.json() == []

    def test_task_not_found(self, client):
        r = client.get("/api/v1/tasks/nonexistent")
        assert r.status_code == 404

    def test_catalogs_includes_google(self, client):
        r = client.get("/api/v1/catalogs")
        assert r.status_code == 200
        types = [c["catalog_type"] for c in r.json()]
        assert "GOOGLE_DATA_LINEAGE" in types


# ── Datasets & Jobs detail endpoints ──────────────────────────────────────


class TestDatasetDetails:
    def test_get_dataset_by_namespace_name(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])
        r = client.get(
            "/api/v1/lineage/datasets/detail",
            params={"namespace": "confluent://env/lkc", "name": "input-t"},
        )
        assert r.status_code == 200
        assert r.json()["name"] == "input-t"

    def test_get_dataset_not_found(self, client):
        r = client.get(
            "/api/v1/lineage/datasets/detail",
            params={"namespace": "no-ns", "name": "no-name"},
        )
        assert r.status_code == 404

    def test_dataset_lineage_upstream(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])
        r = client.get(
            "/api/v1/lineage/datasets/lineage",
            params={
                "namespace": "confluent://env/lkc",
                "name": "output-t",
                "direction": "upstream",
                "depth": 3,
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["direction"] == "upstream"
        assert data["datasets_visited"] >= 1

    def test_dataset_lineage_downstream(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])
        r = client.get(
            "/api/v1/lineage/datasets/lineage",
            params={
                "namespace": "confluent://env/lkc",
                "name": "input-t",
                "direction": "downstream",
            },
        )
        assert r.status_code == 200
        assert r.json()["direction"] == "downstream"

    def test_dataset_lineage_both(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])
        r = client.get(
            "/api/v1/lineage/datasets/lineage",
            params={
                "namespace": "confluent://env/lkc",
                "name": "input-t",
                "direction": "both",
            },
        )
        assert r.status_code == 200
        assert r.json()["direction"] == "both"

    def test_dataset_lineage_empty(self, client):
        r = client.get(
            "/api/v1/lineage/datasets/lineage",
            params={"namespace": "no-ns", "name": "no-name"},
        )
        assert r.status_code == 200
        assert r.json()["events"] == []


class TestJobDetails:
    def test_get_job_by_namespace_name(self, client):
        client.post("/api/v1/lineage/events", json=[_make_event()])
        r = client.get(
            "/api/v1/lineage/jobs/detail",
            params={"namespace": "confluent://env/lkc", "name": "test-connector"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["job"]["name"] == "test-connector"
        assert len(data["latest_inputs"]) == 1
        assert len(data["latest_outputs"]) == 1

    def test_get_job_not_found(self, client):
        r = client.get(
            "/api/v1/lineage/jobs/detail",
            params={"namespace": "no-ns", "name": "no-name"},
        )
        assert r.status_code == 404
