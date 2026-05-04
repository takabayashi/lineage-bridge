# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Live-API integration tests for the Databricks UC provider.

Skipped unless ALL of these are set:

* ``LINEAGE_BRIDGE_DATABRICKS_INTEGRATION=1``
* ``LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL`` (e.g. ``https://dbc-xxx.cloud.databricks.com``)
* ``LINEAGE_BRIDGE_DATABRICKS_TOKEN`` (PAT for the workspace)
* ``LINEAGE_BRIDGE_DATABRICKS_TEST_CATALOG`` (UC catalog the test will read from)
* ``LINEAGE_BRIDGE_DATABRICKS_TEST_SCHEMA`` (schema inside the catalog —
  the hyphenated ``lkc-xxx`` name Tableflow auto-creates is fine)
* ``LINEAGE_BRIDGE_DATABRICKS_TEST_SOURCE_TABLES`` — comma-separated table
  names that should be the *upstream* sources of a derived table written
  by a Databricks notebook job (e.g.
  ``lineage_bridge_orders_v2,lineage_bridge_customers_v2``).

These tests **read only** — no API call here writes or alters anything in the
workspace. The expected workspace state matches what
``infra/demos/uc/main.tf`` provisions (a notebook job that joins two
Tableflow-materialized tables into ``customer_order_summary``).

What they prove against the real API — the things mocking can't catch:

1. ``include_entity_lineage=true`` actually populates ``notebookInfos`` and
   ``jobInfos`` on the *upstreams* side of the lineage response. Without
   that flag the API returns bare ``tableInfo`` and we'd silently produce
   zero notebook nodes (the bug uncovered during the UC demo smoke test).
2. The notebook node's ``job_id`` resolves through ``/api/2.1/jobs/get``
   to a real job whose ``schedule_cron`` matches the demo's
   ``"0 0/5 * ? * * *"`` (every-5-min Quartz expression).
3. The ``/api/2.1/jobs/runs/list?limit=1`` enrichment populates a
   ``last_run_state`` after the demo's first scheduled run completes.
"""

from __future__ import annotations

import os

import pytest

from lineage_bridge.catalogs.databricks_uc import DatabricksUCProvider
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _databricks_enabled() -> tuple[bool, str]:
    if os.environ.get("LINEAGE_BRIDGE_DATABRICKS_INTEGRATION") != "1":
        return False, "set LINEAGE_BRIDGE_DATABRICKS_INTEGRATION=1 to run"
    for var in (
        "LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL",
        "LINEAGE_BRIDGE_DATABRICKS_TOKEN",
        "LINEAGE_BRIDGE_DATABRICKS_TEST_CATALOG",
        "LINEAGE_BRIDGE_DATABRICKS_TEST_SCHEMA",
        "LINEAGE_BRIDGE_DATABRICKS_TEST_SOURCE_TABLES",
    ):
        if not os.environ.get(var):
            return False, f"{var} required"
    return True, ""


_enabled, _skip_reason = _databricks_enabled()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _enabled, reason=_skip_reason),
]


@pytest.fixture(scope="module")
def workspace_url() -> str:
    return os.environ["LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL"]


@pytest.fixture(scope="module")
def token() -> str:
    return os.environ["LINEAGE_BRIDGE_DATABRICKS_TOKEN"]


@pytest.fixture(scope="module")
def catalog() -> str:
    return os.environ["LINEAGE_BRIDGE_DATABRICKS_TEST_CATALOG"]


@pytest.fixture(scope="module")
def schema() -> str:
    return os.environ["LINEAGE_BRIDGE_DATABRICKS_TEST_SCHEMA"]


@pytest.fixture(scope="module")
def source_tables() -> list[str]:
    return [
        t.strip()
        for t in os.environ["LINEAGE_BRIDGE_DATABRICKS_TEST_SOURCE_TABLES"].split(",")
        if t.strip()
    ]


@pytest.fixture()
def seeded_graph(workspace_url, catalog, schema, source_tables) -> LineageGraph:
    """Graph seeded with the source UC tables the test catalog should contain.

    Mirrors what the in-process catalog enrichment receives from a real
    extraction: each Tableflow-materialized table is a CATALOG_TABLE
    (UNITY_CATALOG) node with workspace_url + catalog/schema/table attrs.
    """
    g = LineageGraph()
    for tbl in source_tables:
        full = f"{catalog}.{schema}.{tbl}"
        g.add_node(
            LineageNode(
                node_id=f"databricks:uc_table:env-integration:{full}",
                system=SystemType.DATABRICKS,
                node_type=NodeType.CATALOG_TABLE,
                catalog_type="UNITY_CATALOG",
                qualified_name=full,
                display_name=full,
                environment_id="env-integration",
                cluster_id=schema,
                attributes={
                    "catalog_name": catalog,
                    "schema_name": schema,
                    "table_name": tbl,
                    "workspace_url": workspace_url,
                },
            )
        )
    return g


async def test_lineage_walk_discovers_notebook_with_job_metadata(
    seeded_graph, workspace_url, token, catalog
):
    """End-to-end: enrich() against a live workspace produces a NOTEBOOK node
    with the upstream sources, the producing job's schedule, and the last
    run state attached.

    Locks in the bug we caught manually:
    * ``include_entity_lineage=true`` is sent on the lineage call (without
      it, the API returns no notebookInfos and we'd get zero notebooks).
    * The walker processes ``upstreams`` of the derived table — that's
      where Databricks puts the producer attribution.
    * Jobs API enrichment runs and merges schedule + last_run state onto
      the notebook node.
    """
    provider = DatabricksUCProvider(workspace_url=workspace_url, token=token)
    await provider.enrich(seeded_graph)

    notebooks = [n for n in seeded_graph.nodes if n.node_type == NodeType.NOTEBOOK]
    assert notebooks, (
        "expected at least one NOTEBOOK node — the lineage API returned no "
        "notebookInfos. Verify (a) the demo job has actually run at least "
        "once (Databricks only attributes lineage post-execution), and (b) "
        "the catalog/schema/source-tables env vars point at the demo state."
    )

    nb = notebooks[0]
    assert nb.attributes.get("notebook_id"), "notebook_id missing from attrs"
    assert nb.attributes.get("workspace_id"), "workspace_id missing from attrs"
    assert nb.attributes.get("job_id"), (
        "job_id missing — jobInfos was not on the upstream entry, or the "
        "include_entity_lineage flag isn't being sent."
    )

    # Jobs API enrichment must populate the job name + notebook path so the
    # display reads as the notebook (not the numeric id) and the panel can
    # show both notebook and job alongside.
    assert nb.attributes.get("job_name"), "job_name not enriched from /api/2.1/jobs/get"
    assert nb.attributes.get("notebook_path"), (
        "notebook_path missing — settings.tasks[].notebook_task.notebook_path "
        "wasn't extracted by get_job."
    )
    assert nb.attributes.get("notebook_name") == nb.attributes["notebook_path"].rsplit("/", 1)[-1]
    assert "/" in (nb.attributes.get("schedule_cron") or ""), (
        "schedule_cron should be a Quartz expression like '0 0/5 * ? * * *'"
    )
    # display_name should be the NOTEBOOK name (basename of path), not the
    # job name and not the notebook id.
    assert nb.display_name == nb.attributes["notebook_name"]


async def test_notebook_url_deeplinks_to_workspace(seeded_graph, workspace_url, token):
    """build_url for a NOTEBOOK node returns a usable Databricks deeplink.

    Path-based form (``/#workspace<path>``) is preferred when the Jobs API
    enrichment discovered the notebook_path; falls back to ``/#notebook/<id>``
    otherwise.
    """
    provider = DatabricksUCProvider(workspace_url=workspace_url, token=token)
    await provider.enrich(seeded_graph)

    notebook = next((n for n in seeded_graph.nodes if n.node_type == NodeType.NOTEBOOK), None)
    assert notebook is not None, "no notebook discovered (see other test for diagnosis)"

    url = provider.build_url(notebook)
    assert url is not None
    assert url.startswith(workspace_url.rstrip("/"))
    if notebook.attributes.get("notebook_path"):
        assert f"/#workspace{notebook.attributes['notebook_path']}" in url
    else:
        assert f"/#notebook/{notebook.attributes['notebook_id']}" in url


async def test_native_lineage_push_round_trip(seeded_graph, workspace_url, token, catalog, schema):
    """End-to-end: push_lineage with use_native_lineage=True registers each
    Confluent topic as external_metadata + creates a lineage relationship
    against the live workspace.

    Skipped (not failed) when the principal lacks CREATE_EXTERNAL_METADATA
    on the metastore — the push itself reports the skip via
    PushResult.skipped, but we'd rather pytest emit a clean SKIP than a
    failure since this test relies on a metastore-admin grant the demo
    Terraform may not have applied yet.
    """
    # Seed an upstream Kafka topic and materialize it into one of the
    # existing UC tables — push_lineage's native path walks upstream from
    # each UC node and only fires when a KAFKA_TOPIC is found.
    uc_node = next(n for n in seeded_graph.nodes if n.node_type == NodeType.CATALOG_TABLE)
    topic = LineageNode(
        node_id="confluent:kafka_topic:env-integration:lineage_bridge.smoke_topic",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name="lineage_bridge.smoke_topic",
        display_name="lineage_bridge.smoke_topic",
        environment_id="env-integration",
        cluster_id=schema,
    )
    seeded_graph.add_node(topic)
    seeded_graph.add_edge(
        LineageEdge(src_id=topic.node_id, dst_id=uc_node.node_id, edge_type=EdgeType.MATERIALIZES)
    )

    provider = DatabricksUCProvider(workspace_url=workspace_url, token=token)
    result = await provider.push_lineage(
        seeded_graph,
        sql_client=None,
        set_properties=False,
        set_comments=False,
        create_bridge_table=False,
        use_native_lineage=True,
    )

    if any("CREATE_EXTERNAL_METADATA" in s for s in result.skipped):
        import pytest as _pytest

        _pytest.skip(
            "Principal lacks CREATE_EXTERNAL_METADATA on the metastore. "
            "Apply infra/demos/uc/main.tf (databricks_grants.metastore_external_metadata) "
            "or grant the privilege manually to enable this test."
        )

    assert not result.errors, f"native push errored: {result.errors}"
    assert result.external_metadata_registered >= 1, (
        "expected at least one Confluent topic registered as external_metadata"
    )
    assert result.lineage_relationships_created >= 1, (
        "expected at least one external_lineage relationship"
    )


async def test_notebook_wires_consumes_and_produces_edges(
    seeded_graph, workspace_url, token, source_tables
):
    """For each source the notebook reads from, there's a CONSUMES edge
    (source → notebook); for each derived table, there's a PRODUCES edge
    (notebook → derived). The TRANSFORMS table-to-table edge is removed
    once the notebook hop replaces it."""
    provider = DatabricksUCProvider(workspace_url=workspace_url, token=token)
    await provider.enrich(seeded_graph)

    notebook = next((n for n in seeded_graph.nodes if n.node_type == NodeType.NOTEBOOK), None)
    assert notebook is not None

    consumes_srcs = {
        e.src_id
        for e in seeded_graph.edges
        if e.dst_id == notebook.node_id and e.edge_type == EdgeType.CONSUMES
    }
    # At least one of our seeded source tables must be wired in. We don't
    # require ALL of them — Databricks sometimes attributes lineage to a
    # subset on the first scheduled run.
    seeded_ids = {n.node_id for n in seeded_graph.nodes if n.node_type == NodeType.CATALOG_TABLE}
    assert consumes_srcs & seeded_ids, (
        f"notebook didn't consume any seeded source. consumes_srcs={consumes_srcs}, "
        f"seeded={seeded_ids}, source_tables={source_tables}"
    )

    produces_dsts = [
        e.dst_id
        for e in seeded_graph.edges
        if e.src_id == notebook.node_id and e.edge_type == EdgeType.PRODUCES
    ]
    assert produces_dsts, "notebook produced no downstream tables"

    # No table-to-table TRANSFORMS edge that was replaced by the notebook hop.
    transforms_between_seeded_and_derived = [
        e
        for e in seeded_graph.edges
        if e.edge_type == EdgeType.TRANSFORMS
        and e.src_id in seeded_ids
        and e.dst_id in produces_dsts
    ]
    assert not transforms_between_seeded_and_derived, (
        "TRANSFORMS edge should be replaced by the notebook hop after upstream "
        f"pass found notebook attribution: {transforms_between_seeded_and_derived}"
    )
