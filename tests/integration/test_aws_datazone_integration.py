# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Live-API integration tests for the AWS DataZone provider.

Skipped unless ALL of these are set:

* ``LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION=1``
* ``LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID``
* ``LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID``
* ``LINEAGE_BRIDGE_AWS_REGION`` (or default boto3 region resolution)
* AWS credentials in the environment (env vars / IAM role / profile)

Behaviour proven end-to-end:

* The custom asset type bootstrap is idempotent against the real DataZone API.
* A registered Kafka asset's ``externalIdentifier`` matches what the
  OpenLineage event references, so the DataZone Catalog UI can link them.
* ``post_lineage_event`` accepts our normalised events.

These tests **create real DataZone assets** in the configured domain. They
do not delete them on teardown — DataZone CRUD is async and the test suite
should not couple to its cleanup latency. Re-runs are idempotent (the second
push hits the create_asset_revision path).
"""

from __future__ import annotations

import os
import uuid

import pytest

from lineage_bridge.catalogs.aws_datazone import (
    ASSET_TYPE_NAME,
    AWSDataZoneProvider,
    DataZoneAssetRegistrar,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _datazone_enabled() -> tuple[bool, str]:
    if os.environ.get("LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION") != "1":
        return False, "set LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION=1 to run"
    for var in (
        "LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID",
        "LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID",
    ):
        if not os.environ.get(var):
            return False, f"{var} required"
    try:
        import boto3

        boto3.client(
            "datazone",
            region_name=os.environ.get("LINEAGE_BRIDGE_AWS_REGION", "us-east-1"),
        )
    except Exception as exc:
        return False, f"boto3 datazone client unavailable: {exc}"
    return True, ""


_enabled, _skip_reason = _datazone_enabled()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _enabled, reason=_skip_reason),
]


@pytest.fixture(scope="module")
def domain_id() -> str:
    return os.environ["LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID"]


@pytest.fixture(scope="module")
def project_id() -> str:
    return os.environ["LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID"]


@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("LINEAGE_BRIDGE_AWS_REGION", "us-east-1")


@pytest.fixture
def integration_graph() -> LineageGraph:
    suffix = uuid.uuid4().hex[:6]
    cluster_id = "lkc-int-test"
    topic_name = f"lb_integration_{suffix}.events"
    glue_table = f"lb_integration_{suffix}_target"

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
        attributes={"fields": [{"name": "id", "type": "long"}, {"name": "v", "type": "string"}]},
    )
    connector = LineageNode(
        node_id=f"confluent:connector:env-int:c-{suffix}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name=f"c-{suffix}",
        display_name=f"c-{suffix}",
        environment_id="env-int",
        cluster_id=cluster_id,
        attributes={"connector_class": "S3SinkConnector", "direction": "sink"},
    )
    glue = LineageNode(
        node_id=f"aws:glue_table:env-int:db.{glue_table}",
        system=SystemType.AWS,
        node_type=NodeType.CATALOG_TABLE,
        catalog_type="AWS_GLUE",
        qualified_name=f"db.{glue_table}",
        display_name=glue_table,
        environment_id="env-int",
        cluster_id=cluster_id,
        attributes={"database": "db", "table_name": glue_table, "aws_region": "us-east-1"},
    )
    for n in (topic, schema, connector, glue):
        g.add_node(n)
    g.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=schema.node_id, edge_type=EdgeType.HAS_SCHEMA)
    )
    g.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=connector.node_id, edge_type=EdgeType.CONSUMES)
    )
    g.add_edge(
        LineageEdge(src_id=connector.node_id, dst_id=glue.node_id, edge_type=EdgeType.PRODUCES)
    )
    return g


async def test_register_creates_asset_with_schema(integration_graph, domain_id, project_id, region):
    registrar = DataZoneAssetRegistrar(domain_id, project_id, region)
    count, errors = await registrar.register_kafka_assets(integration_graph)
    assert errors == []
    assert count == 1

    # Verify via search that the asset is present with the matching externalIdentifier.
    import boto3

    client = boto3.client("datazone", region_name=region)
    topic_node = next(iter(integration_graph.filter_by_type(NodeType.KAFKA_TOPIC)))
    expected_ext = f"kafka:{topic_node.cluster_id}.`{topic_node.qualified_name}`"
    resp = client.search(
        domainIdentifier=domain_id,
        searchScope="ASSET",
        searchText=expected_ext,
        maxResults=10,
    )
    matches = [item.get("assetItem") for item in resp.get("items", []) if item.get("assetItem")]
    assert any(m.get("externalIdentifier") == expected_ext for m in matches), (
        f"asset with externalIdentifier {expected_ext} not found"
    )


async def test_register_idempotent_falls_back_to_revision(
    integration_graph, domain_id, project_id, region
):
    registrar = DataZoneAssetRegistrar(domain_id, project_id, region)
    first = await registrar.register_kafka_assets(integration_graph)
    second = await registrar.register_kafka_assets(integration_graph)
    assert first[1] == [] and second[1] == []
    assert first[0] == 1 and second[0] == 1


async def test_push_lineage_posts_events_and_returns_count(
    integration_graph, domain_id, project_id, region
):
    provider = AWSDataZoneProvider(domain_id=domain_id, project_id=project_id, region=region)
    result = await provider.push_lineage(integration_graph)
    # tables_updated counts the OpenLineage events posted; expect at least one
    # (the connector job: topic → glue_table).
    assert result.tables_updated >= 1
    # Asset registration errors land in result.errors but shouldn't include
    # "domain_id" — that's only when not configured.
    for err in result.errors:
        assert "domain_id" not in err.lower()


async def test_asset_type_exists_after_first_push(domain_id, region):
    """The bootstrap creates ``LineageBridgeKafkaTopic``; subsequent runs just GET it."""
    import boto3

    client = boto3.client("datazone", region_name=region)
    resp = client.get_asset_type(domainIdentifier=domain_id, identifier=ASSET_TYPE_NAME)
    assert resp.get("name") == ASSET_TYPE_NAME
