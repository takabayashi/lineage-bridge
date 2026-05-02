# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for OpenLineage <-> LineageGraph translator."""

from __future__ import annotations

from datetime import UTC, datetime

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.openlineage.models import (
    ConfluentKafkaDatasetFacet,
    DatasetFacets,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunEventType,
)
from lineage_bridge.openlineage.translator import (
    _build_namespace,
    events_to_graph,
    graph_to_events,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _node(
    name,
    ntype=NodeType.KAFKA_TOPIC,
    system=SystemType.CONFLUENT,
    env="env-abc",
    cluster="lkc-123",
    **attrs,
):
    nid = f"{system.value}:{ntype.value}:{env}:{name}"
    return LineageNode(
        node_id=nid,
        system=system,
        node_type=ntype,
        qualified_name=name,
        display_name=name,
        environment_id=env,
        cluster_id=cluster,
        attributes=attrs,
    )


def _edge(src_id, dst_id, etype=EdgeType.PRODUCES):
    return LineageEdge(src_id=src_id, dst_id=dst_id, edge_type=etype)


def _nid(name, ntype, env="env-abc"):
    return f"confluent:{ntype.value}:{env}:{name}"


# ── Namespace Builder ──────────────────────────────────────────────────────


class TestBuildNamespace:
    def test_confluent_namespace(self):
        node = _node("orders", env="env-abc", cluster="lkc-123")
        assert _build_namespace(node) == "confluent://env-abc/lkc-123"

    def test_databricks_namespace(self):
        node = _node(
            "table",
            NodeType.CATALOG_TABLE,
            SystemType.DATABRICKS,
            env="ws",
            cluster="",
            workspace_url="my-ws.cloud.databricks.com",
        )
        assert _build_namespace(node) == "databricks://my-ws.cloud.databricks.com"

    def test_aws_namespace(self):
        node = _node(
            "table",
            NodeType.CATALOG_TABLE,
            SystemType.AWS,
            env="us-east-1",
            cluster="mydb",
            aws_region="us-west-2",
            database="mydb",
        )
        assert _build_namespace(node) == "aws://us-west-2/mydb"

    def test_external_namespace(self):
        node = _node(
            "s3://bucket", NodeType.EXTERNAL_DATASET, SystemType.EXTERNAL, env="ext", cluster=""
        )
        assert _build_namespace(node) == "external://ext/default"


# ── Graph to Events ───────────────────────────────────────────────────────


class TestGraphToEvents:
    def test_empty_graph(self):
        graph = LineageGraph()
        events = graph_to_events(graph)
        assert events == []

    def test_single_connector_job(self):
        """Connector producing to a topic → 1 event with 1 output."""
        graph = LineageGraph()
        conn = _node("pg-source", NodeType.CONNECTOR)
        topic = _node("orders")
        graph.add_node(conn)
        graph.add_node(topic)
        graph.add_edge(
            _edge(
                _nid("pg-source", NodeType.CONNECTOR),
                _nid("orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        assert len(events) == 1
        evt = events[0]
        assert evt.job.name == "pg-source"
        assert len(evt.outputs) == 1
        assert evt.outputs[0].name == "orders"
        assert evt.inputs == []

    def test_ksqldb_query_with_io(self):
        """ksqlDB query consuming 2 topics, producing 1."""
        graph = LineageGraph()
        t1 = _node("orders")
        t2 = _node("customers")
        t3 = _node("enriched_orders")
        q = _node("CSAS_ENRICHED", NodeType.KSQLDB_QUERY)

        for n in [t1, t2, t3, q]:
            graph.add_node(n)

        graph.add_edge(
            _edge(
                _nid("orders", NodeType.KAFKA_TOPIC),
                _nid("CSAS_ENRICHED", NodeType.KSQLDB_QUERY),
                EdgeType.CONSUMES,
            )
        )
        graph.add_edge(
            _edge(
                _nid("customers", NodeType.KAFKA_TOPIC),
                _nid("CSAS_ENRICHED", NodeType.KSQLDB_QUERY),
                EdgeType.CONSUMES,
            )
        )
        graph.add_edge(
            _edge(
                _nid("CSAS_ENRICHED", NodeType.KSQLDB_QUERY),
                _nid("enriched_orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        # 1 event for the ksqlDB job
        job_events = [e for e in events if e.job.name == "CSAS_ENRICHED"]
        assert len(job_events) == 1
        evt = job_events[0]
        assert len(evt.inputs) == 2
        assert len(evt.outputs) == 1
        input_names = {i.name for i in evt.inputs}
        assert input_names == {"orders", "customers"}

    def test_consumer_group(self):
        """Consumer group consuming from a topic."""
        graph = LineageGraph()
        topic = _node("orders")
        cg = _node("my-group", NodeType.CONSUMER_GROUP)
        graph.add_node(topic)
        graph.add_node(cg)
        graph.add_edge(
            _edge(
                _nid("orders", NodeType.KAFKA_TOPIC),
                _nid("my-group", NodeType.CONSUMER_GROUP),
                EdgeType.CONSUMES,
            )
        )

        events = graph_to_events(graph)
        cg_events = [e for e in events if e.job.name == "my-group"]
        assert len(cg_events) == 1
        assert len(cg_events[0].inputs) == 1
        assert cg_events[0].inputs[0].name == "orders"

    def test_schema_becomes_facet(self):
        """Schema nodes should become SchemaDatasetFacet, not standalone events."""
        graph = LineageGraph()
        topic = _node("orders")
        schema = _node("orders-value", NodeType.SCHEMA)
        conn = _node("pg-source", NodeType.CONNECTOR)
        graph.add_node(topic)
        graph.add_node(schema)
        graph.add_node(conn)
        # Schema edge
        graph.add_edge(
            _edge(
                _nid("orders", NodeType.KAFKA_TOPIC),
                _nid("orders-value", NodeType.SCHEMA),
                EdgeType.HAS_SCHEMA,
            )
        )
        # Connector produces to topic
        graph.add_edge(
            _edge(
                _nid("pg-source", NodeType.CONNECTOR),
                _nid("orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        # No event should have "orders-value" as a job name or standalone dataset
        all_job_names = [e.job.name for e in events]
        assert "orders-value" not in all_job_names

    def test_standalone_topic_gets_identity_job(self):
        """Topics not connected to any job get a synthetic identity event."""
        graph = LineageGraph()
        topic = _node("orphan-topic")
        graph.add_node(topic)

        events = graph_to_events(graph)
        assert len(events) == 1
        assert events[0].job.name == "_identity_orphan-topic"
        assert len(events[0].outputs) == 1
        assert events[0].outputs[0].name == "orphan-topic"

    def test_confluent_only_filter(self):
        """confluent_only=True should exclude non-Confluent nodes."""
        graph = LineageGraph()
        conn = _node("s3-sink", NodeType.CONNECTOR)
        topic = _node("orders")
        # Build the UC node with the legacy "uc_table" ID segment that
        # DatabricksUCProvider.build_node actually emits (see databricks_uc.py:79
        # for the ID-stability rationale). The default _node() helper would
        # produce "databricks:catalog_table:..." which doesn't match real data.
        uc_node_id = "databricks:uc_table:ws:catalog.schema.orders"
        uc_table = LineageNode(
            node_id=uc_node_id,
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name="catalog.schema.orders",
            display_name="catalog.schema.orders",
            environment_id="ws",
        )
        graph.add_node(conn)
        graph.add_node(topic)
        graph.add_node(uc_table)
        graph.add_edge(
            _edge(
                _nid("orders", NodeType.KAFKA_TOPIC),
                _nid("s3-sink", NodeType.CONNECTOR),
                EdgeType.CONSUMES,
            )
        )
        graph.add_edge(_edge(_nid("s3-sink", NodeType.CONNECTOR), uc_node_id, EdgeType.PRODUCES))

        events_all = graph_to_events(graph, confluent_only=False)
        events_co = graph_to_events(graph, confluent_only=True)

        # All events should include the UC output
        all_outputs = [o.name for e in events_all for o in e.outputs]
        assert "catalog.schema.orders" in all_outputs

        # Confluent-only should exclude the UC output
        co_outputs = [o.name for e in events_co for o in e.outputs]
        assert "catalog.schema.orders" not in co_outputs

    def test_event_time_override(self):
        graph = LineageGraph()
        graph.add_node(_node("t1"))
        fixed_time = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
        events = graph_to_events(graph, event_time=fixed_time)
        assert events[0].eventTime == fixed_time

    def test_deterministic_run_id(self):
        """Same node should produce the same runId (uuid5-based)."""
        graph = LineageGraph()
        graph.add_node(_node("stable-topic"))
        events1 = graph_to_events(graph)
        events2 = graph_to_events(graph)
        assert events1[0].run.runId == events2[0].run.runId

    def test_connector_facets(self):
        """Connector attributes should appear in ConfluentConnectorJobFacet."""
        graph = LineageGraph()
        conn = _node("pg-src", NodeType.CONNECTOR, **{"connector.class": "PostgresSource"})
        topic = _node("orders")
        graph.add_node(conn)
        graph.add_node(topic)
        graph.add_edge(
            _edge(
                _nid("pg-src", NodeType.CONNECTOR),
                _nid("orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        facets = events[0].job.facets
        assert facets.confluent_connector.connector_class == "PostgresSource"

    def test_kafka_facets_on_output(self):
        """Topic attributes should appear as ConfluentKafkaDatasetFacet."""
        graph = LineageGraph()
        conn = _node("src", NodeType.CONNECTOR)
        topic = _node("orders", partitions=6, replication_factor=3)
        graph.add_node(conn)
        graph.add_node(topic)
        graph.add_edge(
            _edge(
                _nid("src", NodeType.CONNECTOR),
                _nid("orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        output_facets = events[0].outputs[0].facets
        assert output_facets.confluent_kafka.partitions == 6
        assert output_facets.confluent_kafka.replication_factor == 3


class TestGraphToEventsWithSampleGraph:
    """Integration-style tests using the sample_graph fixture."""

    def test_sample_graph_events(self, sample_graph):
        events = graph_to_events(sample_graph)
        assert len(events) > 0
        job_names = {e.job.name for e in events}
        # Should have events for connector, ksqldb query, consumer group
        assert "postgres-source-orders" in job_names
        assert "CSAS_ENRICHED_ORDERS_0" in job_names
        assert "order-processing-group" in job_names

    def test_sample_graph_ksqldb_io(self, sample_graph):
        events = graph_to_events(sample_graph)
        ksql_evt = next(e for e in events if e.job.name == "CSAS_ENRICHED_ORDERS_0")
        input_names = {i.name for i in ksql_evt.inputs}
        output_names = {o.name for o in ksql_evt.outputs}
        assert input_names == {"orders", "customers"}
        assert output_names == {"enriched_orders"}

    def test_sample_graph_all_datasets_covered(self, sample_graph):
        """All non-schema dataset nodes should appear in at least one event."""
        events = graph_to_events(sample_graph)
        all_datasets = set()
        for e in events:
            for i in e.inputs:
                all_datasets.add(i.name)
            for o in e.outputs:
                all_datasets.add(o.name)

        # Topics and external datasets should appear
        assert "orders" in all_datasets
        assert "enriched_orders" in all_datasets


# ── Events to Graph ───────────────────────────────────────────────────────


class TestEventsToGraph:
    def test_empty_events(self):
        graph = events_to_graph([])
        assert graph.node_count == 0
        assert graph.edge_count == 0

    def test_single_event_with_io(self):
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(namespace="confluent://env-abc/lkc-123", name="my-connector"),
            inputs=[InputDataset(namespace="confluent://env-abc/lkc-123", name="input-topic")],
            outputs=[OutputDataset(namespace="confluent://env-abc/lkc-123", name="output-topic")],
        )

        graph = events_to_graph([evt])
        assert graph.node_count == 3  # job + 2 datasets
        assert graph.edge_count == 2  # input->job, job->output

        # Verify node types
        job_node = graph.get_node("confluent:connector:env-abc:my-connector")
        assert job_node is not None
        assert job_node.node_type == NodeType.CONNECTOR

        input_node = graph.get_node("confluent:kafka_topic:env-abc:input-topic")
        assert input_node is not None
        assert input_node.node_type == NodeType.KAFKA_TOPIC

    def test_identity_job_creates_dataset_only(self):
        """Synthetic identity jobs should create dataset nodes but no job node."""
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(namespace="confluent://env/lkc", name="_identity_orphan-topic"),
            inputs=[],
            outputs=[OutputDataset(namespace="confluent://env/lkc", name="orphan-topic")],
        )

        graph = events_to_graph([evt])
        assert graph.node_count == 1  # only the dataset
        node = graph.nodes[0]
        assert node.qualified_name == "orphan-topic"
        assert node.node_type == NodeType.KAFKA_TOPIC

    def test_databricks_event(self):
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(namespace="databricks://ws", name="etl-pipeline"),
            inputs=[
                InputDataset(namespace="databricks://ws", name="raw.orders"),
            ],
            outputs=[
                OutputDataset(namespace="databricks://ws", name="curated.orders"),
            ],
        )

        graph = events_to_graph([evt])
        assert graph.node_count == 3
        # Job should be Databricks system
        for node in graph.nodes:
            assert node.system == SystemType.DATABRICKS

    def test_multiple_events_merge(self):
        """Multiple events referencing the same dataset should merge."""
        evt1 = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(namespace="confluent://env/lkc", name="job-a"),
            outputs=[OutputDataset(namespace="confluent://env/lkc", name="shared-topic")],
        )
        evt2 = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-2"),
            job=Job(namespace="confluent://env/lkc", name="job-b"),
            inputs=[InputDataset(namespace="confluent://env/lkc", name="shared-topic")],
        )

        graph = events_to_graph([evt1, evt2])
        # shared-topic should appear once, not twice
        topic_nodes = graph.filter_by_type(NodeType.KAFKA_TOPIC)
        assert len(topic_nodes) == 1
        assert topic_nodes[0].qualified_name == "shared-topic"

    def test_kafka_facets_preserved(self):
        """ConfluentKafkaDatasetFacet should be preserved in node attributes."""
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(namespace="confluent://env/lkc", name="job"),
            outputs=[
                OutputDataset(
                    namespace="confluent://env/lkc",
                    name="orders",
                    facets=DatasetFacets(
                        confluent_kafka=ConfluentKafkaDatasetFacet(
                            partitions=12,
                            replication_factor=3,
                            tags=["production"],
                        )
                    ),
                ),
            ],
        )

        graph = events_to_graph([evt])
        topic = graph.filter_by_type(NodeType.KAFKA_TOPIC)[0]
        assert topic.attributes["partitions"] == 12
        assert topic.attributes["replication_factor"] == 3
        assert "production" in topic.tags


# ── Roundtrip Tests ────────────────────────────────────────────────────────


class TestRoundtrip:
    def test_basic_roundtrip(self):
        """Graph → events → graph should preserve structure."""
        graph = LineageGraph()
        graph.add_node(_node("orders", partitions=6))
        graph.add_node(_node("enriched", NodeType.KAFKA_TOPIC))
        graph.add_node(_node("transformer", NodeType.KSQLDB_QUERY))
        graph.add_edge(
            _edge(
                _nid("orders", NodeType.KAFKA_TOPIC),
                _nid("transformer", NodeType.KSQLDB_QUERY),
                EdgeType.CONSUMES,
            )
        )
        graph.add_edge(
            _edge(
                _nid("transformer", NodeType.KSQLDB_QUERY),
                _nid("enriched", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        graph2 = events_to_graph(events)

        # Should have same number of non-schema nodes
        assert graph2.node_count == 3  # 2 topics + 1 job
        assert graph2.edge_count == 2

    def test_roundtrip_preserves_kafka_attributes(self):
        """Kafka-specific attributes should survive the roundtrip."""
        graph = LineageGraph()
        graph.add_node(_node("orders", partitions=12, replication_factor=3))
        graph.add_node(_node("src", NodeType.CONNECTOR))
        graph.add_edge(
            _edge(
                _nid("src", NodeType.CONNECTOR),
                _nid("orders", NodeType.KAFKA_TOPIC),
            )
        )

        events = graph_to_events(graph)
        graph2 = events_to_graph(events)

        topic = next(n for n in graph2.nodes if n.node_type == NodeType.KAFKA_TOPIC)
        assert topic.attributes.get("partitions") == 12
        assert topic.attributes.get("replication_factor") == 3

    def test_roundtrip_with_sample_graph(self, sample_graph):
        """Roundtrip using the sample_graph fixture."""
        events = graph_to_events(sample_graph)
        graph2 = events_to_graph(events)

        # Schemas become facets, so roundtrip graph has fewer nodes
        original_non_schema = [n for n in sample_graph.nodes if n.node_type != NodeType.SCHEMA]
        assert graph2.node_count <= len(original_non_schema)
        assert graph2.edge_count > 0
