# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the Dataplex Catalog asset registrar.

End-to-end behaviour is covered separately by manual API probes; these tests
lock in the FQN format, entry-id sanitization, and the bootstrap-then-upsert
sequence using mocked HTTP responses.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.catalogs.google_dataplex import (
    ASPECT_TYPE_ID,
    ENTRY_GROUP_ID,
    ENTRY_TYPE_ID,
    DataplexAssetRegistrar,
    _entry_id,
    _kafka_fqn,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)

PROJECT = "p"
LOC = "us"
BASE = "https://dataplex.googleapis.com/v1"
PARENT = f"projects/{PROJECT}/locations/{LOC}"


def _topic(qname: str, cluster: str = "lkc-abc") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:kafka_topic:env:{qname}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=qname,
        display_name=qname,
        environment_id="env",
        cluster_id=cluster,
    )


def _schema_node(name: str, fields: list[dict]) -> LineageNode:
    return LineageNode(
        node_id=f"confluent:schema:env:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name=name,
        display_name=name,
        environment_id="env",
        attributes={"fields": fields},
    )


class TestFqnAndEntryId:
    """The FQN must match what processOpenLineageRunEvent records."""

    def test_simple_topic_no_backticks(self):
        assert _kafka_fqn("lkc-abc", "orders") == "kafka:lkc-abc.orders"

    def test_dotted_topic_uses_backticks(self):
        # Google escapes topic names containing "." with backticks.
        assert _kafka_fqn("lkc-abc", "lb.orders_v2") == "kafka:lkc-abc.`lb.orders_v2`"

    def test_entry_id_sanitizes_special_chars(self):
        # Dots are allowed in entry IDs; "$" / spaces get replaced with "_".
        assert _entry_id("lkc-abc", "lb.orders") == "lkc-abc-lb.orders"
        assert "_" in _entry_id("lkc-abc", "weird$topic name")


class TestRegisterKafkaAssets:
    @pytest.fixture
    def registrar(self):
        return DataplexAssetRegistrar(PROJECT, LOC, headers={"Authorization": "Bearer test"})

    def _graph_with_topic_and_schema(self):
        g = LineageGraph()
        topic = _topic("lb.orders")
        schema = _schema_node(
            "lb.orders-value",
            [
                {"name": "id", "type": "long", "doc": "Order ID"},
                {"name": "amount", "type": "double"},
            ],
        )
        g.add_node(topic)
        g.add_node(schema)
        g.add_edge(
            LineageEdge(
                src_id=topic.node_id, dst_id=schema.node_id, edge_type=EdgeType.HAS_SCHEMA
            )
        )
        return g

    @respx.mock
    async def test_no_kafka_topics_skips_everything(self, registrar):
        # Empty graph → no calls made at all.
        count, errors = await registrar.register_kafka_assets(LineageGraph())
        assert count == 0 and errors == []

    @respx.mock
    async def test_bootstrap_creates_missing_resources_then_upserts(self, registrar):
        # Bootstrap GETs return 404 → POSTs return 200 + done LRO.
        respx.get(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}").mock(httpx.Response(404))
        respx.get(f"{BASE}/{PARENT}/entryTypes/{ENTRY_TYPE_ID}").mock(httpx.Response(404))
        respx.get(f"{BASE}/{PARENT}/aspectTypes/{ASPECT_TYPE_ID}").mock(httpx.Response(404))
        respx.post(f"{BASE}/{PARENT}/entryGroups").mock(
            httpx.Response(200, json={"name": f"{PARENT}/operations/op1", "done": True})
        )
        respx.post(f"{BASE}/{PARENT}/entryTypes").mock(
            httpx.Response(200, json={"name": f"{PARENT}/operations/op2", "done": True})
        )
        respx.post(f"{BASE}/{PARENT}/aspectTypes").mock(
            httpx.Response(200, json={"name": f"{PARENT}/operations/op3", "done": True})
        )
        respx.get(f"{BASE}/{PARENT}/operations/op1").mock(httpx.Response(200, json={"done": True}))
        respx.get(f"{BASE}/{PARENT}/operations/op2").mock(httpx.Response(200, json={"done": True}))
        respx.get(f"{BASE}/{PARENT}/operations/op3").mock(httpx.Response(200, json={"done": True}))
        # Entry create succeeds.
        respx.post(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries").mock(
            httpx.Response(200, json={"name": "ok"})
        )

        count, errors = await registrar.register_kafka_assets(self._graph_with_topic_and_schema())
        assert count == 1 and errors == []

    @respx.mock
    async def test_409_on_create_falls_back_to_patch(self, registrar):
        # All bootstrap resources already exist.
        respx.get(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/entryTypes/{ENTRY_TYPE_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/aspectTypes/{ASPECT_TYPE_ID}").mock(httpx.Response(200))
        # Entry POST returns 409 → registrar PATCHes instead.
        respx.post(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries").mock(httpx.Response(409))
        patch_route = respx.patch(
            f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries/lkc-abc-lb.orders"
        ).mock(httpx.Response(200, json={"name": "ok"}))

        count, errors = await registrar.register_kafka_assets(self._graph_with_topic_and_schema())
        assert count == 1 and errors == []
        assert patch_route.called

    @respx.mock
    async def test_per_topic_failures_collected_not_raised(self, registrar):
        respx.get(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/entryTypes/{ENTRY_TYPE_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/aspectTypes/{ASPECT_TYPE_ID}").mock(httpx.Response(200))
        # Entry create fails with 500.
        respx.post(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries").mock(
            httpx.Response(500, json={"error": "boom"})
        )

        g = LineageGraph()
        g.add_node(_topic("lb.orders"))

        count, errors = await registrar.register_kafka_assets(g)
        assert count == 0
        assert len(errors) == 1
        assert "lb.orders" in errors[0]
