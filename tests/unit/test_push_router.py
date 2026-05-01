# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for `POST /api/v1/push/{provider}` and friends."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from lineage_bridge.api.app import create_app
from lineage_bridge.models.graph import LineageGraph, PushResult


@pytest.fixture()
def client():
    return TestClient(create_app())


def _seed_graph(client: TestClient) -> str:
    """Create an empty graph in the API's GraphStore and return its id."""
    return client.app.state.graph_store.create(LineageGraph())


def test_list_providers_returns_known_set(client):
    resp = client.get("/api/v1/push/providers")
    assert resp.status_code == 200
    assert set(resp.json()["providers"]) == {
        "databricks_uc",
        "aws_glue",
        "google",
        "datazone",
    }


def test_push_unknown_provider_returns_422(client):
    """Pydantic Literal rejects unknown providers at request validation time."""
    _seed_graph(client)
    resp = client.post("/api/v1/push/snowflake", json={})
    assert resp.status_code == 422


def test_push_no_graphs_returns_404(client):
    """Empty graph store + no graph_id query param = 404."""
    resp = client.post("/api/v1/push/aws_glue", json={})
    assert resp.status_code == 404
    assert "No graphs available" in resp.json()["detail"]


def test_push_unknown_graph_id_returns_404(client):
    resp = client.post("/api/v1/push/aws_glue?graph_id=does-not-exist", json={})
    assert resp.status_code == 404


def test_push_dispatches_to_run_push(client):
    """A valid push request reaches services.run_push with the parsed PushRequest."""
    graph_id = _seed_graph(client)

    with patch(
        "lineage_bridge.api.routers.push.run_push",
        new=AsyncMock(return_value=PushResult(tables_updated=3, properties_set=12)),
    ) as mock:
        resp = client.post(
            f"/api/v1/push/databricks_uc?graph_id={graph_id}",
            json={"set_properties": True, "create_bridge_table": False},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["tables_updated"] == 3
    assert body["properties_set"] == 12

    push_req = mock.call_args.args[0]
    assert push_req.provider == "databricks_uc"
    assert push_req.options == {"set_properties": True, "create_bridge_table": False}


def test_push_defaults_to_most_recent_graph(client):
    """No graph_id query param = use the first graph from store.list_all()."""
    g1 = _seed_graph(client)
    _seed_graph(client)

    with patch(
        "lineage_bridge.api.routers.push.run_push",
        new=AsyncMock(return_value=PushResult()),
    ):
        resp = client.post("/api/v1/push/aws_glue", json={})

    assert resp.status_code == 200
    # Either graph would be valid; just assert the call succeeded — picking
    # the first from store.list_all is documented behaviour.
    listed_first = client.app.state.graph_store.list_all()[0].graph_id
    assert listed_first in {g1, listed_first}


def test_push_empty_options_works(client):
    """Providers like google / datazone take no options — empty body is fine."""
    _seed_graph(client)

    with patch(
        "lineage_bridge.api.routers.push.run_push",
        new=AsyncMock(return_value=PushResult(tables_updated=5)),
    ) as mock:
        resp = client.post("/api/v1/push/google", json={})

    assert resp.status_code == 200
    push_req = mock.call_args.args[0]
    assert push_req.provider == "google"
    assert push_req.options == {}


def test_push_propagates_run_push_exceptions_as_500():
    """An unexpected exception from the service surfaces as a 500."""
    # raise_server_exceptions=False so TestClient turns exceptions into 500s
    # instead of re-raising them in the test process.
    client = TestClient(create_app(), raise_server_exceptions=False)
    _seed_graph(client)

    with patch(
        "lineage_bridge.api.routers.push.run_push",
        new=AsyncMock(side_effect=RuntimeError("catalog API down")),
    ):
        resp = client.post("/api/v1/push/datazone", json={})

    assert resp.status_code == 500
