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
    _REGISTRY,
    ASPECT_TYPE_ID,
    ENTRY_GROUP_ID,
    ENTRY_TYPE_ID,
    DataplexAssetRegistrar,
    _connector_fqn,
    _consumer_group_fqn,
    _entry_id,
    _flink_fqn,
    _kafka_fqn,
    _ksqldb_fqn,
    _process_display_name,
    _scalar_attr_data,
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
        assert _entry_id("lkc-abc-lb.orders") == "lkc-abc-lb.orders"
        assert "_" in _entry_id("lkc-abc-weird$topic name")


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
            LineageEdge(src_id=topic.node_id, dst_id=schema.node_id, edge_type=EdgeType.HAS_SCHEMA)
        )
        return g

    @respx.mock
    async def test_no_kafka_topics_skips_everything(self, registrar):
        # Empty graph → no calls made at all.
        count, errors = await registrar.register_confluent_assets(LineageGraph())
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

        count, errors = await registrar.register_confluent_assets(
            self._graph_with_topic_and_schema()
        )
        assert count == 1 and errors == []

    @respx.mock
    async def test_409_on_create_falls_back_to_patch(self, registrar):
        # All bootstrap resources already exist.
        respx.get(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/entryTypes/{ENTRY_TYPE_ID}").mock(httpx.Response(200))
        respx.get(f"{BASE}/{PARENT}/aspectTypes/{ASPECT_TYPE_ID}").mock(httpx.Response(200))
        # Entry POST returns 409 → registrar PATCHes instead.
        respx.post(f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries").mock(
            httpx.Response(409)
        )
        patch_route = respx.patch(
            f"{BASE}/{PARENT}/entryGroups/{ENTRY_GROUP_ID}/entries/lkc-abc-lb.orders"
        ).mock(httpx.Response(200, json={"name": "ok"}))

        count, errors = await registrar.register_confluent_assets(
            self._graph_with_topic_and_schema()
        )
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

        count, errors = await registrar.register_confluent_assets(g)
        assert count == 0
        assert len(errors) == 1
        assert "lb.orders" in errors[0]


# ── New: registry coverage for non-Kafka node types ────────────────────


class TestRegistry:
    def test_all_four_extra_node_types_registered(self):
        # Kafka stays + the four new entries are present and distinct.
        present = {nt for nt in _REGISTRY}
        assert NodeType.KAFKA_TOPIC in present
        assert NodeType.FLINK_JOB in present
        assert NodeType.CONNECTOR in present
        assert NodeType.KSQLDB_QUERY in present
        assert NodeType.CONSUMER_GROUP in present
        # Each has its own entry/aspect type IDs.
        eid = {r.entry_type_id for r in _REGISTRY.values()}
        aid = {r.aspect_type_id for r in _REGISTRY.values()}
        assert len(eid) == 5 and len(aid) == 5

    def test_flink_is_env_scoped_others_cluster_scoped(self):
        assert _REGISTRY[NodeType.FLINK_JOB].requires_cluster_id is False
        for nt in (
            NodeType.KAFKA_TOPIC,
            NodeType.CONNECTOR,
            NodeType.KSQLDB_QUERY,
            NodeType.CONSUMER_GROUP,
        ):
            assert _REGISTRY[nt].requires_cluster_id is True


class TestFqnBuilders:
    def test_flink_fqn_uses_env(self):
        # `custom:` prefix required by Dataplex for user-defined systems.
        assert _flink_fqn("env-x", "my-job") == "custom:confluent-flink:env-x.my-job"

    def test_connector_fqn_quotes_dotted_names(self):
        assert _connector_fqn("lkc-1", "bq.sink") == "custom:confluent-connect:lkc-1.`bq.sink`"

    def test_ksqldb_fqn(self):
        assert _ksqldb_fqn("lkc-1", "CTAS_FOO_42") == "custom:confluent-ksqldb:lkc-1.CTAS_FOO_42"

    def test_consumer_group_fqn(self):
        assert (
            _consumer_group_fqn("lkc-1", "connect-grp-1")
            == "custom:confluent-consumer-group:lkc-1.connect-grp-1"
        )

    def test_unknown_cluster_falls_back(self):
        # Defensive: missing cluster id → "unknown" sentinel rather than empty.
        assert _connector_fqn("", "x") == "custom:confluent-connect:unknown.x"


class TestScalarAttrData:
    def test_filters_to_declared_keys_and_drops_empty(self):
        node = LineageNode(
            node_id="x",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="job",
            display_name="job",
            attributes={
                "sql": "SELECT 1",
                "phase": "RUNNING",
                "principal": None,
                "extra": "ignored",
            },
        )
        builder = _scalar_attr_data(("sql", "phase", "compute_pool_id", "principal"))
        out = builder(node, LineageGraph())
        assert out == {"sql": "SELECT 1", "phase": "RUNNING"}

    def test_returns_none_when_nothing_present(self):
        node = LineageNode(
            node_id="x",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="job",
            display_name="job",
            attributes={},
        )
        out = _scalar_attr_data(("sql",))(node, LineageGraph())
        assert out is None


class TestProcessDisplayName:
    def test_matches_translator_format(self):
        # The translator emits namespace=confluent://<env>/<cluster> + name=<qn>;
        # Google joins them as <namespace>:<name> for displayName. We reverse this
        # to find the right Lineage process to PATCH.
        node = LineageNode(
            node_id="x",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="my-job",
            display_name="my-job",
            environment_id="env-1",
            cluster_id="lkc-2",
        )
        assert _process_display_name(node) == "confluent://env-1/lkc-2:my-job"

    def test_falls_back_when_env_or_cluster_missing(self):
        node = LineageNode(
            node_id="x",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="j",
            display_name="j",
        )
        # Mirrors the translator's "unknown"/"default" fallbacks.
        assert _process_display_name(node) == "confluent://unknown/default:j"


class TestLinkProcessesToCatalog:
    @pytest.fixture
    def registrar(self):
        return DataplexAssetRegistrar(PROJECT, LOC, headers={"Authorization": "Bearer test"})

    @respx.mock
    async def test_matches_and_patches_one_process(self, registrar):
        # Graph has a Flink job; Lineage processes.list returns one matching displayName.
        flink = LineageNode(
            node_id="confluent:flink_job:env-1:my-job",
            system=SystemType.CONFLUENT,
            node_type=NodeType.FLINK_JOB,
            qualified_name="my-job",
            display_name="my-job",
            environment_id="env-1",
            cluster_id="lkc-2",
            attributes={"sql": "SELECT 1", "phase": "RUNNING"},
        )
        g = LineageGraph()
        g.add_node(flink)

        list_url = f"https://datalineage.googleapis.com/v1/{PARENT}/processes?pageSize=200"
        respx.get(list_url).mock(
            httpx.Response(
                200,
                json={
                    "processes": [
                        {
                            "name": f"{PARENT}/processes/proc-1",
                            "displayName": "confluent://env-1/lkc-2:my-job",
                        },
                        {
                            "name": f"{PARENT}/processes/proc-other",
                            "displayName": "bigquery:something:else",
                        },
                    ]
                },
            )
        )
        patch_route = respx.patch(
            f"https://datalineage.googleapis.com/v1/{PARENT}/processes/proc-1"
        ).mock(httpx.Response(200, json={"name": "ok"}))

        patched, errors = await registrar.link_processes_to_catalog(g)
        assert patched == 1 and errors == []
        assert patch_route.called
        sent_body = patch_route.calls.last.request.read()
        assert b"custom:confluent-flink:env-1.my-job" in sent_body
        assert b"SELECT 1" in sent_body
        # PATCH must include a *sanitized* displayName — Google clears unset
        # fields even with updateMask, and PATCH validates displayName more
        # strictly than CREATE (no `/` allowed). We rewrite to a safe form.
        assert b"Confluent flink: my-job" in sent_body
        # The original Google-assigned displayName (with `/`) must NOT appear.
        assert b"confluent://env-1/lkc-2:my-job" not in sent_body
        sent_url = str(patch_route.calls.last.request.url)
        assert "displayName" in sent_url

    @respx.mock
    async def test_no_transformation_nodes_short_circuits(self, registrar):
        # Graph with just topics — no Flink/Connector/ksqlDB → no API calls.
        g = LineageGraph()
        g.add_node(_topic("lb.orders"))
        patched, errors = await registrar.link_processes_to_catalog(g)
        assert patched == 0 and errors == []
