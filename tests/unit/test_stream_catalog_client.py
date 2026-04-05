# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.stream_catalog.StreamCatalogClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.stream_catalog import StreamCatalogClient
from lineage_bridge.models.graph import (
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

# ── shared constants ──────────────────────────────────────────────────────

API_KEY = "test-key"
API_SECRET = "test-secret"
ENV_ID = "env-abc123"
BASE_URL = "https://schema-registry.us-east-1.aws.confluent.cloud"
SEARCH_PATH = "/catalog/v1/search/basic"


@pytest.fixture()
def sc_client():
    return StreamCatalogClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        environment_id=ENV_ID,
        timeout=5.0,
    )


def _make_node(name: str, node_type: NodeType = NodeType.KAFKA_TOPIC) -> LineageNode:
    """Create a minimal node matching the ID convention used by StreamCatalogClient."""
    node_id = f"confluent:{node_type.value}:{ENV_ID}:{name}"
    return LineageNode(
        node_id=node_id,
        system=SystemType.CONFLUENT,
        node_type=node_type,
        qualified_name=name,
        display_name=name,
        environment_id=ENV_ID,
    )


def _build_graph_with_nodes(*names: str) -> LineageGraph:
    """Build a LineageGraph containing kafka_topic nodes for the given names."""
    graph = LineageGraph()
    for name in names:
        graph.add_node(_make_node(name))
    return graph


def _catalog_entity(
    name: str, owner: str = "", description: str = "", classifications: list[str] | None = None
) -> dict:
    """Build a catalog search result entity."""
    entity: dict = {
        "attributes": {"name": name},
    }
    if owner:
        entity["attributes"]["owner"] = owner
    if description:
        entity["attributes"]["description"] = description
    if classifications:
        entity["classificationNames"] = classifications
    else:
        entity["classificationNames"] = []
    return entity


def _mock_search(entities_by_type: dict[str, list[dict]]):
    """Mock the catalog search endpoint, routing by type query param."""

    def side_effect(request: httpx.Request) -> httpx.Response:
        entity_type = request.url.params.get("type", "")
        entities = entities_by_type.get(entity_type, [])
        return httpx.Response(200, json={"searchResults": entities})

    respx.get(f"{BASE_URL}{SEARCH_PATH}").mock(side_effect=side_effect)


# ── extract() tests ──────────────────────────────────────────────────────


class TestStreamCatalogExtract:
    async def test_extract_returns_empty(self, sc_client):
        """extract() always returns empty lists -- enrichment is separate."""
        nodes, edges = await sc_client.extract()
        assert nodes == []
        assert edges == []


# ── enrich() tests ───────────────────────────────────────────────────────


class TestStreamCatalogEnrich:
    @respx.mock
    async def test_enrich_adds_owner_and_description(self, sc_client):
        """Owner and description from the catalog are merged into matching nodes."""
        graph = _build_graph_with_nodes("orders", "customers")

        _mock_search(
            {
                "kafka_topic": [
                    _catalog_entity("orders", owner="team-data", description="All orders"),
                    _catalog_entity("customers", owner="team-crm", description="Customer records"),
                ],
                "kafka_connector": [],
            }
        )

        await sc_client.enrich(graph)

        orders = graph.get_node(f"confluent:kafka_topic:{ENV_ID}:orders")
        assert orders is not None
        assert orders.attributes["owner"] == "team-data"
        assert orders.attributes["description"] == "All orders"

        customers = graph.get_node(f"confluent:kafka_topic:{ENV_ID}:customers")
        assert customers is not None
        assert customers.attributes["owner"] == "team-crm"

    @respx.mock
    async def test_enrich_adds_tags(self, sc_client):
        """Classification names are merged into node.tags."""
        graph = _build_graph_with_nodes("orders")

        _mock_search(
            {
                "kafka_topic": [
                    _catalog_entity("orders", classifications=["PII", "GDPR"]),
                ],
                "kafka_connector": [],
            }
        )

        await sc_client.enrich(graph)

        orders = graph.get_node(f"confluent:kafka_topic:{ENV_ID}:orders")
        assert orders is not None
        assert "PII" in orders.tags
        assert "GDPR" in orders.tags

    @respx.mock
    async def test_enrich_skips_unknown_nodes(self, sc_client):
        """Entities not matching any graph node are silently skipped."""
        graph = _build_graph_with_nodes("orders")

        _mock_search(
            {
                "kafka_topic": [
                    _catalog_entity("orders", owner="team-data"),
                    _catalog_entity("unknown-topic", owner="nobody"),
                ],
                "kafka_connector": [],
            }
        )

        await sc_client.enrich(graph)

        # "orders" enriched, "unknown-topic" has no node and is ignored
        assert graph.get_node(f"confluent:kafka_topic:{ENV_ID}:orders") is not None
        assert graph.get_node(f"confluent:kafka_topic:{ENV_ID}:unknown-topic") is None

    @respx.mock
    async def test_enrich_handles_api_failure(self, sc_client, monkeypatch):
        """If the catalog API fails, enrich continues without crashing."""
        monkeypatch.setattr("asyncio.sleep", _no_sleep)

        graph = _build_graph_with_nodes("orders")

        # Fail on kafka_topic search, succeed (empty) on kafka_connector
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            entity_type = request.url.params.get("type", "")
            if entity_type == "kafka_topic":
                return httpx.Response(500, json={"error": "boom"})
            return httpx.Response(200, json={"searchResults": []})

        respx.get(f"{BASE_URL}{SEARCH_PATH}").mock(side_effect=side_effect)

        # Should not raise
        await sc_client.enrich(graph)

        # Node should remain unchanged
        orders = graph.get_node(f"confluent:kafka_topic:{ENV_ID}:orders")
        assert orders is not None
        assert "owner" not in orders.attributes


# ── helpers ───────────────────────────────────────────────────────────────


async def _no_sleep(seconds: float) -> None:
    pass
