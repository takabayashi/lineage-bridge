# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Schema Registry client."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from lineage_bridge.clients.schema_registry import SchemaRegistryClient, _topic_from_subject
from lineage_bridge.models.graph import EdgeType, NodeType, SystemType
from tests.conftest import load_fixture


# ── Fixtures ───────────────────────────────────────────────────────────

ENV_ID = "env-abc123"
BASE_URL = "https://sr.example.com"
API_KEY = "test-key"
API_SECRET = "test-secret"


@pytest.fixture()
def sr_client():
    return SchemaRegistryClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        environment_id=ENV_ID,
    )


@pytest.fixture()
def subjects_payload() -> list[str]:
    return load_fixture("schema_subjects.json")


@pytest.fixture()
def schema_version_payload() -> dict:
    return load_fixture("schema_version.json")


# ── _topic_from_subject parsing ────────────────────────────────────────


class TestTopicFromSubject:
    def test_value_suffix(self):
        result = _topic_from_subject("orders-value")
        assert result == ("orders", "value")

    def test_key_suffix(self):
        result = _topic_from_subject("orders-key")
        assert result == ("orders", "key")

    def test_no_match(self):
        result = _topic_from_subject("orders")
        assert result is None

    def test_hyphenated_topic_value(self):
        result = _topic_from_subject("my-cool-topic-value")
        assert result == ("my-cool-topic", "value")

    def test_hyphenated_topic_key(self):
        result = _topic_from_subject("my-cool-topic-key")
        assert result == ("my-cool-topic", "key")

    def test_value_only_string(self):
        # Edge case: subject is literally "-value"
        result = _topic_from_subject("-value")
        assert result == ("", "value")


# ── _count_fields ──────────────────────────────────────────────────────


class TestCountFields:
    def test_avro_fields(self, schema_version_payload):
        schema_str = schema_version_payload["schema"]
        count = SchemaRegistryClient._count_fields(schema_str, "AVRO")
        # The fixture has 8 fields
        parsed = json.loads(schema_str)
        expected = len(parsed["fields"])
        assert count == expected
        assert count == 8

    def test_json_schema_properties(self):
        schema_str = json.dumps({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
        })
        count = SchemaRegistryClient._count_fields(schema_str, "JSON")
        assert count == 2

    def test_json_schema_no_properties(self):
        schema_str = json.dumps({"type": "object"})
        count = SchemaRegistryClient._count_fields(schema_str, "JSON")
        assert count is None

    def test_empty_string(self):
        count = SchemaRegistryClient._count_fields("", "AVRO")
        assert count is None

    def test_invalid_json(self):
        count = SchemaRegistryClient._count_fields("{not valid json", "AVRO")
        assert count is None

    def test_unknown_schema_type(self):
        count = SchemaRegistryClient._count_fields('{"fields":[]}', "PROTOBUF")
        assert count is None


# ── extract ────────────────────────────────────────────────────────────


class TestExtract:
    @respx.mock
    async def test_extract_creates_schema_nodes_and_edges(
        self, sr_client, subjects_payload, schema_version_payload
    ):
        respx.get(f"{BASE_URL}/subjects").mock(
            return_value=httpx.Response(200, json=subjects_payload)
        )
        # All subjects return the same schema version payload for simplicity.
        for subject in subjects_payload:
            respx.get(f"{BASE_URL}/subjects/{subject}/versions/latest").mock(
                return_value=httpx.Response(200, json=schema_version_payload)
            )

        nodes, edges = await sr_client.extract()

        # One schema node per subject.
        assert len(nodes) == len(subjects_payload)
        for node in nodes:
            assert node.node_type == NodeType.SCHEMA
            assert node.system == SystemType.CONFLUENT
            assert node.environment_id == ENV_ID
            assert node.attributes["schema_type"] == "AVRO"
            assert node.attributes["version"] == schema_version_payload["version"]
            assert node.attributes["schema_id"] == schema_version_payload["id"]
            assert node.attributes["field_count"] == 8

        # All three subjects end with "-value", so all produce HAS_SCHEMA edges.
        assert len(edges) == len(subjects_payload)
        for edge in edges:
            assert edge.edge_type == EdgeType.HAS_SCHEMA
            assert edge.attributes["role"] == "value"

    @respx.mock
    async def test_extract_edge_src_dst_ids(
        self, sr_client, schema_version_payload
    ):
        respx.get(f"{BASE_URL}/subjects").mock(
            return_value=httpx.Response(200, json=["orders-value"])
        )
        respx.get(f"{BASE_URL}/subjects/orders-value/versions/latest").mock(
            return_value=httpx.Response(200, json=schema_version_payload)
        )

        nodes, edges = await sr_client.extract()

        assert len(edges) == 1
        edge = edges[0]
        assert edge.src_id == f"confluent:kafka_topic:{ENV_ID}:orders"
        assert edge.dst_id == f"confluent:schema:{ENV_ID}:orders-value"

    @respx.mock
    async def test_extract_no_edge_for_non_standard_subject(
        self, sr_client, schema_version_payload
    ):
        respx.get(f"{BASE_URL}/subjects").mock(
            return_value=httpx.Response(200, json=["weird_subject_no_suffix"])
        )
        respx.get(f"{BASE_URL}/subjects/weird_subject_no_suffix/versions/latest").mock(
            return_value=httpx.Response(200, json=schema_version_payload)
        )

        nodes, edges = await sr_client.extract()

        assert len(nodes) == 1
        assert len(edges) == 0

    @respx.mock
    async def test_handles_schema_fetch_failure(self, sr_client, no_sleep):
        """When fetching a schema version fails, the subject is skipped."""
        respx.get(f"{BASE_URL}/subjects").mock(
            return_value=httpx.Response(200, json=["orders-value", "customers-value"])
        )
        # First subject fails (server error triggers retries, then raises).
        respx.get(f"{BASE_URL}/subjects/orders-value/versions/latest").mock(
            return_value=httpx.Response(500, json={"error_code": 50001})
        )
        # Second subject succeeds.
        good_payload = {
            "subject": "customers-value",
            "version": 1,
            "id": 99,
            "schemaType": "AVRO",
            "schema": '{"type":"record","name":"C","fields":[{"name":"id","type":"string"}]}',
        }
        respx.get(f"{BASE_URL}/subjects/customers-value/versions/latest").mock(
            return_value=httpx.Response(200, json=good_payload)
        )

        nodes, edges = await sr_client.extract()

        # Only the successful subject should produce a node and an edge.
        assert len(nodes) == 1
        assert nodes[0].qualified_name == "customers-value"
        assert len(edges) == 1

    @respx.mock
    async def test_extract_empty_subjects(self, sr_client):
        respx.get(f"{BASE_URL}/subjects").mock(
            return_value=httpx.Response(200, json=[])
        )

        nodes, edges = await sr_client.extract()

        assert nodes == []
        assert edges == []
