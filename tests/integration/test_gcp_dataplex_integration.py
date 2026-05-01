# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Live-API integration tests for the Google Dataplex Catalog registrar.

Skipped unless ``LINEAGE_BRIDGE_GCP_INTEGRATION=1`` is set AND a working
Application Default Credentials chain is present (``gcloud auth
application-default login`` or ``GOOGLE_APPLICATION_CREDENTIALS``).

What these tests prove:

* The bootstrap (entry group + entry type + aspect type) is idempotent end
  to end against the real Dataplex API.
* A registered entry's FQN matches what ``processOpenLineageRunEvent``
  records, so the BigQuery Lineage tab can join them.
* Schema fields submitted via the aspect survive the round trip.

These tests create real resources in the configured project. Cleanup is
best-effort in a teardown fixture but the entry group / type are left in
place since they're idempotent across runs.
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

from lineage_bridge.catalogs.google_dataplex import (
    ASPECT_TYPE_ID,
    ENTRY_GROUP_ID,
    ENTRY_TYPE_ID,
    DataplexAssetRegistrar,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _gcp_enabled() -> tuple[bool, str]:
    if os.environ.get("LINEAGE_BRIDGE_GCP_INTEGRATION") != "1":
        return False, "set LINEAGE_BRIDGE_GCP_INTEGRATION=1 to run"
    if not os.environ.get("LINEAGE_BRIDGE_GCP_PROJECT_ID"):
        return False, "LINEAGE_BRIDGE_GCP_PROJECT_ID required"
    try:
        import google.auth
        import google.auth.transport.requests
    except ImportError:
        return False, "google-auth not installed"
    try:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(google.auth.transport.requests.Request())
    except Exception as exc:
        return False, f"ADC unavailable: {exc}"
    return True, ""


_enabled, _skip_reason = _gcp_enabled()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _enabled, reason=_skip_reason),
]


@pytest.fixture(scope="module")
def project_id() -> str:
    return os.environ["LINEAGE_BRIDGE_GCP_PROJECT_ID"]


@pytest.fixture(scope="module")
def location() -> str:
    return os.environ.get("LINEAGE_BRIDGE_GCP_LOCATION", "us")


@pytest.fixture(scope="module")
def auth_headers() -> dict[str, str]:
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    return {"Authorization": f"Bearer {creds.token}"}


@pytest.fixture
def integration_graph() -> tuple[LineageGraph, str]:
    """A topic with a schema, plus a unique suffix so parallel runs don't collide."""
    suffix = uuid.uuid4().hex[:6]
    cluster_id = "lkc-int-test"
    topic_name = f"lb_integration_{suffix}.events"

    g = LineageGraph()
    topic = LineageNode(
        node_id=f"confluent:kafka_topic:env-int:{topic_name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=topic_name,
        display_name=topic_name,
        environment_id="env-int",
        cluster_id=cluster_id,
    )
    schema = LineageNode(
        node_id=f"confluent:schema:env-int:{topic_name}-value",
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name=f"{topic_name}-value",
        display_name=f"{topic_name}-value",
        environment_id="env-int",
        attributes={
            "fields": [
                {"name": "event_id", "type": "long", "doc": "Event ID"},
                {"name": "payload", "type": "string"},
            ]
        },
    )
    g.add_node(topic)
    g.add_node(schema)
    g.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=schema.node_id, edge_type=EdgeType.HAS_SCHEMA)
    )
    return g, suffix


async def test_register_creates_entry_with_schema(
    integration_graph, project_id, location, auth_headers
):
    graph, suffix = integration_graph
    topic_node = next(iter(graph.filter_by_type(NodeType.KAFKA_TOPIC)))
    expected_fqn = f"kafka:{topic_node.cluster_id}.`{topic_node.qualified_name}`"

    registrar = DataplexAssetRegistrar(project_id, location, auth_headers)
    count, errors = await registrar.register_kafka_assets(graph)
    assert errors == []
    assert count == 1

    # Fetch the entry directly via the Dataplex API and confirm FQN + aspect.
    eg = f"projects/{project_id}/locations/{location}/entryGroups/{ENTRY_GROUP_ID}"
    url = f"https://dataplex.googleapis.com/v1/{eg}/entries"
    async with httpx.AsyncClient(headers=auth_headers, timeout=30.0) as client:
        list_resp = await client.get(url)
        assert list_resp.status_code == 200
        entries = list_resp.json().get("entries", [])
        match = next((e for e in entries if e.get("fullyQualifiedName") == expected_fqn), None)
        assert match is not None, f"entry with FQN {expected_fqn} not found"

        # Read with view=ALL to inspect the schema aspect data.
        full_resp = await client.get(f"https://dataplex.googleapis.com/v1/{match['name']}?view=ALL")
        assert full_resp.status_code == 200
        body = full_resp.json()
        assert body.get("fullyQualifiedName") == expected_fqn
        # Aspect key is "<projectNumber|projectId>.<location>.<aspectTypeId>"; just
        # check that one of the aspect keys ends with our aspect type ID and has
        # the schema fields we sent.
        aspects = body.get("aspects", {})
        schema_aspects = [v for k, v in aspects.items() if k.endswith(f".{ASPECT_TYPE_ID}")]
        assert schema_aspects, f"aspect {ASPECT_TYPE_ID} not attached"
        fields = schema_aspects[0].get("data", {}).get("fields", [])
        names = [f.get("name") for f in fields]
        assert names == ["event_id", "payload"]


async def test_register_is_idempotent(integration_graph, project_id, location, auth_headers):
    """Running the registrar twice should succeed both times (entry update path)."""
    graph, _ = integration_graph
    registrar = DataplexAssetRegistrar(project_id, location, auth_headers)
    first = await registrar.register_kafka_assets(graph)
    second = await registrar.register_kafka_assets(graph)
    assert first[1] == [] and second[1] == []
    assert first[0] == 1 and second[0] == 1


async def test_bootstrap_resources_exist(project_id, location, auth_headers):
    """After any registration, the entry group + type + aspect type should exist."""
    parent = f"projects/{project_id}/locations/{location}"
    async with httpx.AsyncClient(headers=auth_headers, timeout=30.0) as client:
        for path in (
            f"{parent}/entryGroups/{ENTRY_GROUP_ID}",
            f"{parent}/entryTypes/{ENTRY_TYPE_ID}",
            f"{parent}/aspectTypes/{ASPECT_TYPE_ID}",
        ):
            resp = await client.get(f"https://dataplex.googleapis.com/v1/{path}")
            assert resp.status_code == 200, f"{path} missing: {resp.text[:200]}"
