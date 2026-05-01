# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the AWS DataZone provider — mocks the boto3 client."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lineage_bridge.catalogs.aws_datazone import (
    ASSET_TYPE_NAME,
    AWSDataZoneProvider,
    DataZoneAssetRegistrar,
    _ext_id,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _topic(name="lb.orders", cluster="lkc-abc") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:kafka_topic:env-1:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id=cluster,
    )


def _schema(topic_name="lb.orders") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:schema:env-1:{topic_name}-value",
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name=f"{topic_name}-value",
        display_name=f"{topic_name}-value",
        environment_id="env-1",
        attributes={"fields": [{"name": "id", "type": "long", "doc": "PK"}]},
    )


def _graph_with_topic_and_schema() -> LineageGraph:
    g = LineageGraph()
    t, s = _topic(), _schema()
    g.add_node(t)
    g.add_node(s)
    g.add_edge(LineageEdge(src_id=t.node_id, dst_id=s.node_id, edge_type=EdgeType.HAS_SCHEMA))
    return g


class TestExtId:
    def test_sanitizes_special_chars(self):
        assert _ext_id("lkc-abc", "lb.orders") == "lkc-abc-lb.orders"
        assert "_" in _ext_id("lkc-abc", "weird$topic name")


class TestRegistrar:
    @pytest.fixture
    def registrar(self):
        r = DataZoneAssetRegistrar(domain_id="dzd_test", project_id="prj_test", region="us-east-1")
        r._client = MagicMock()
        return r

    async def test_skips_when_no_topics(self, registrar):
        count, errors = await registrar.register_kafka_assets(LineageGraph())
        assert count == 0 and errors == []
        assert not registrar._client.method_calls

    async def test_uses_existing_asset_type_when_present(self, registrar):
        # get_asset_type returns successfully → no create call.
        registrar._client.get_asset_type.return_value = {"name": ASSET_TYPE_NAME}
        registrar._client.create_asset.return_value = {"id": "ast_1"}

        count, errors = await registrar.register_kafka_assets(_graph_with_topic_and_schema())
        assert count == 1 and errors == []
        registrar._client.create_asset_type.assert_not_called()
        kwargs = registrar._client.create_asset.call_args.kwargs
        assert kwargs["externalIdentifier"] == "kafka:lkc-abc.`lb.orders`"
        assert kwargs["typeIdentifier"] == ASSET_TYPE_NAME
        # Schema fields make it into the form payload.
        form_content = json.loads(kwargs["formsInput"][0]["content"])
        assert json.loads(form_content["fieldsJson"])[0]["name"] == "id"

    async def test_creates_asset_type_when_missing(self, registrar):
        # get_asset_type → ResourceNotFoundException → registrar should create it.
        not_found = type("ResourceNotFoundException", (Exception,), {})("not found")
        registrar._client.get_asset_type.side_effect = not_found
        registrar._client.create_asset.return_value = {"id": "ast_1"}

        count, errors = await registrar.register_kafka_assets(_graph_with_topic_and_schema())
        assert count == 1 and errors == []
        registrar._client.create_asset_type.assert_called_once()

    async def test_409_on_create_falls_back_to_revision(self, registrar):
        registrar._client.get_asset_type.return_value = {"name": ASSET_TYPE_NAME}
        conflict = type("ConflictException", (Exception,), {})("exists")
        registrar._client.create_asset.side_effect = conflict
        registrar._client.search.return_value = {
            "items": [
                {
                    "assetItem": {
                        "externalIdentifier": "kafka:lkc-abc.`lb.orders`",
                        "identifier": "ast_existing",
                    }
                }
            ]
        }
        registrar._client.create_asset_revision.return_value = {
            "id": "ast_existing",
            "revision": "2",
        }

        count, errors = await registrar.register_kafka_assets(_graph_with_topic_and_schema())
        assert count == 1 and errors == []
        registrar._client.create_asset_revision.assert_called_once()
        assert (
            registrar._client.create_asset_revision.call_args.kwargs["identifier"] == "ast_existing"
        )

    async def test_per_topic_failures_collected_not_raised(self, registrar):
        registrar._client.get_asset_type.return_value = {"name": ASSET_TYPE_NAME}
        registrar._client.create_asset.side_effect = RuntimeError("boom")

        count, errors = await registrar.register_kafka_assets(_graph_with_topic_and_schema())
        assert count == 0
        assert len(errors) == 1
        assert "lb.orders" in errors[0]

    async def test_iam_denied_on_form_type_skips_gracefully(self, registrar):
        """Missing datazone:CreateFormType / CreateAssetType should NOT crash the push.

        Lineage events still post elsewhere; only schema-on-asset display is
        lost. The caller surfaces this as info, so registrar returns (0, []).
        """
        not_found = type("ResourceNotFoundException", (Exception,), {})("not found")
        registrar._client.get_asset_type.side_effect = not_found
        registrar._client.get_form_type.side_effect = not_found
        denied = type("AccessDeniedException", (Exception,), {})("not permitted")
        registrar._client.create_form_type.side_effect = denied

        count, errors = await registrar.register_kafka_assets(_graph_with_topic_and_schema())
        assert count == 0
        # Errors list stays empty — IAM block is informational, not failure.
        assert errors == []
        registrar._client.create_asset.assert_not_called()


class TestProvider:
    async def test_skips_without_config(self):
        provider = AWSDataZoneProvider()
        result = await provider.push_lineage(LineageGraph())
        assert "domain_id" in result.errors[0]

    async def test_post_events_skips_when_no_events(self, monkeypatch):
        provider = AWSDataZoneProvider(domain_id="dzd_x", project_id="prj_x", region="us-east-1")
        # Fake registrar so we don't try real boto3.
        called = {}

        async def fake_register(graph, on_progress=None):
            called["registered"] = True
            return 0, []

        monkeypatch.setattr(
            "lineage_bridge.catalogs.aws_datazone.DataZoneAssetRegistrar.register_kafka_assets",
            fake_register,
        )
        # Mock the boto3 client used in _post_events.
        mock_client = MagicMock()
        monkeypatch.setattr(
            "boto3.client",
            lambda service, region_name=None: mock_client if service == "datazone" else None,
        )

        # Empty graph → translator yields no events → no posts.
        result = await provider.push_lineage(LineageGraph())
        assert result.tables_updated == 0
        mock_client.post_lineage_event.assert_not_called()

    async def test_post_events_posts_each_normalized_event(self, monkeypatch):
        provider = AWSDataZoneProvider(domain_id="dzd_x", project_id="prj_x", region="us-east-1")

        async def fake_register(graph, on_progress=None):
            return 0, []

        monkeypatch.setattr(
            "lineage_bridge.catalogs.aws_datazone.DataZoneAssetRegistrar.register_kafka_assets",
            fake_register,
        )

        # Build a tiny graph with a CONNECTOR job: topic → connector → glue_table.
        g = LineageGraph()
        topic = _topic()
        conn = LineageNode(
            node_id="confluent:connector:env-1:c",
            system=SystemType.CONFLUENT,
            node_type=NodeType.CONNECTOR,
            qualified_name="c",
            display_name="c",
            environment_id="env-1",
            cluster_id="lkc-abc",
            attributes={"connector_class": "S3SinkConnector", "direction": "sink"},
        )
        glue = LineageNode(
            node_id="aws:glue_table:env-1:db.t",
            system=SystemType.AWS,
            node_type=NodeType.GLUE_TABLE,
            qualified_name="db.t",
            display_name="db.t",
            environment_id="env-1",
            cluster_id="lkc-abc",
            attributes={"database": "db", "table_name": "t", "aws_region": "us-east-1"},
        )
        g.add_node(topic)
        g.add_node(conn)
        g.add_node(glue)
        g.add_edge(
            LineageEdge(src_id=topic.node_id, dst_id=conn.node_id, edge_type=EdgeType.CONSUMES)
        )
        g.add_edge(
            LineageEdge(src_id=conn.node_id, dst_id=glue.node_id, edge_type=EdgeType.PRODUCES)
        )

        mock_client = MagicMock()
        mock_client.post_lineage_event.return_value = {"id": "evt_1"}
        monkeypatch.setattr(
            "boto3.client",
            lambda service, region_name=None: (
                mock_client if service == "datazone" else SimpleNamespace()
            ),
        )

        result = await provider.push_lineage(g)
        assert mock_client.post_lineage_event.called
        # At least one call carries an OpenLineage payload referencing both inputs and outputs.
        call_kwargs = mock_client.post_lineage_event.call_args.kwargs
        payload = json.loads(call_kwargs["event"].decode("utf-8"))
        assert payload["job"]["name"] == "c"
        # Input namespace was rewritten kafka://; output kept as aws://.
        ns = {ds["namespace"] for ds in payload["inputs"]}
        out_ns = {ds["namespace"] for ds in payload["outputs"]}
        assert any(n.startswith("kafka://") for n in ns)
        assert any(n.startswith("aws://") for n in out_ns)
        assert result.tables_updated >= 1
