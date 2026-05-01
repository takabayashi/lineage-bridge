# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the shared upstream-chain builder.

Locks in the chain shape that UC/Glue/DataZone all consume — if it changes
those callers need to change with it.
"""

from __future__ import annotations

import json

from lineage_bridge.catalogs.upstream_chain import (
    build_upstream_chain,
    chain_to_json,
    format_chain_summary,
)
from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)


def _topic(name: str, cluster: str = "lkc-1", env: str = "env-1") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:kafka_topic:{env}:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.KAFKA_TOPIC,
        qualified_name=name,
        display_name=name,
        environment_id=env,
        cluster_id=cluster,
    )


def _flink(name: str) -> LineageNode:
    return LineageNode(
        node_id=f"confluent:flink_job:env-1:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.FLINK_JOB,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id="lkc-1",
        attributes={"sql": "SELECT * FROM source"},
    )


def _connector(name: str, cls: str = "S3SourceConnector") -> LineageNode:
    return LineageNode(
        node_id=f"confluent:connector:env-1:{name}",
        system=SystemType.CONFLUENT,
        node_type=NodeType.CONNECTOR,
        qualified_name=name,
        display_name=name,
        environment_id="env-1",
        cluster_id="lkc-1",
        attributes={"connector_class": cls},
    )


def _schema(topic_name: str, fields: list[dict]) -> LineageNode:
    return LineageNode(
        node_id=f"confluent:schema:env-1:{topic_name}-value",
        system=SystemType.CONFLUENT,
        node_type=NodeType.SCHEMA,
        qualified_name=f"{topic_name}-value",
        display_name=f"{topic_name}-value",
        environment_id="env-1",
        attributes={"fields": fields},
    )


def _uc_target() -> LineageNode:
    return LineageNode(
        node_id="databricks:uc_table:env-1:cat.sch.target",
        system=SystemType.DATABRICKS,
        node_type=NodeType.UC_TABLE,
        qualified_name="cat.sch.target",
        display_name="target",
        environment_id="env-1",
    )


def _multi_hop_graph() -> tuple[LineageGraph, str]:
    """Build: source_topic → flink_job → mid_topic → target."""
    g = LineageGraph()
    src_topic = _topic("source_topic")
    mid_topic = _topic("mid_topic")
    flink = _flink("enrich_job")
    target = _uc_target()
    src_schema = _schema(
        "source_topic", [{"name": "id", "type": "long"}, {"name": "v", "type": "string"}]
    )

    for n in (src_topic, mid_topic, flink, target, src_schema):
        g.add_node(n)
    g.add_edge(
        LineageEdge(
            src_id=src_topic.node_id, dst_id=src_schema.node_id, edge_type=EdgeType.HAS_SCHEMA
        )
    )
    g.add_edge(
        LineageEdge(src_id=src_topic.node_id, dst_id=flink.node_id, edge_type=EdgeType.CONSUMES)
    )
    g.add_edge(
        LineageEdge(src_id=flink.node_id, dst_id=mid_topic.node_id, edge_type=EdgeType.PRODUCES)
    )
    g.add_edge(
        LineageEdge(
            src_id=mid_topic.node_id, dst_id=target.node_id, edge_type=EdgeType.MATERIALIZES
        )
    )
    return g, target.node_id


class TestBuildUpstreamChain:
    def test_orders_hops_by_distance(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        # Direct upstream first (mid_topic), then flink, then source_topic.
        kinds = [(h.hop, h.kind, h.qualified_name) for h in chain]
        assert kinds == [
            (1, "topic", "mid_topic"),
            (2, "flink_job", "enrich_job"),
            (3, "topic", "source_topic"),
        ]

    def test_includes_flink_sql(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        flink_hop = next(h for h in chain if h.kind == "flink_job")
        assert flink_hop.sql == "SELECT * FROM source"

    def test_includes_connector_class(self):
        g = LineageGraph()
        topic = _topic("t")
        conn = _connector("c", cls="DebeziumPostgresConnector")
        target = _uc_target()
        for n in (topic, conn, target):
            g.add_node(n)
        g.add_edge(
            LineageEdge(src_id=conn.node_id, dst_id=topic.node_id, edge_type=EdgeType.PRODUCES)
        )
        g.add_edge(
            LineageEdge(
                src_id=topic.node_id, dst_id=target.node_id, edge_type=EdgeType.MATERIALIZES
            )
        )

        chain = build_upstream_chain(g, target.node_id)
        connector_hop = next(h for h in chain if h.kind == "connector")
        assert connector_hop.connector_class == "DebeziumPostgresConnector"

    def test_includes_topic_schema_fields(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        # source_topic carries the schema, mid_topic doesn't.
        src_hop = next(h for h in chain if h.qualified_name == "source_topic")
        mid_hop = next(h for h in chain if h.qualified_name == "mid_topic")
        assert [f["name"] for f in src_hop.schema_fields] == ["id", "v"]
        assert mid_hop.schema_fields == []

    def test_empty_for_isolated_node(self):
        g = LineageGraph()
        g.add_node(_uc_target())
        assert build_upstream_chain(g, "databricks:uc_table:env-1:cat.sch.target") == []


class TestChainToJson:
    def test_round_trip_compact(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        encoded, truncated = chain_to_json(chain)
        assert not truncated
        decoded = json.loads(encoded)
        assert isinstance(decoded, list) and len(decoded) == 3
        assert decoded[0]["kind"] == "topic"

    def test_truncates_when_over_max_bytes(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        encoded, truncated = chain_to_json(chain, max_bytes=80)  # tiny budget
        assert truncated
        decoded = json.loads(encoded)
        assert len(decoded) < 3  # at least one hop got dropped


class TestFormatChainSummary:
    def test_renders_target_arrow(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        summary = format_chain_summary(chain, "target")
        assert "Upstream lineage:" in summary
        assert "→ target" in summary
        assert "topic: source_topic" in summary

    def test_includes_sql_snippet_for_flink(self):
        g, target_id = _multi_hop_graph()
        chain = build_upstream_chain(g, target_id)
        summary = format_chain_summary(chain, "target")
        assert "[SQL: SELECT * FROM source]" in summary
