# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Bidirectional translation between LineageGraph and OpenLineage events.

Mapping rules:
- Kafka topics, Tableflow tables, catalog tables → OpenLineage Dataset
- Connectors, ksqlDB queries, Flink jobs, consumer groups → OpenLineage Job
- Schemas → SchemaDatasetFacet on the parent Dataset
- External datasets → OpenLineage Dataset
- Edges (PRODUCES/CONSUMES/TRANSFORMS) → InputDataset/OutputDataset on Jobs
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    SystemType,
)
from lineage_bridge.openlineage.models import (
    ConfluentConnectorJobFacet,
    ConfluentKafkaDatasetFacet,
    DatasetFacets,
    DataSourceDatasetFacet,
    DocumentationJobFacet,
    InputDataset,
    Job,
    JobFacets,
    OutputDataset,
    Run,
    RunEvent,
    RunEventType,
    SchemaDatasetFacet,
    SchemaField,
    SqlJobFacet,
)

# Node types that map to OpenLineage Dataset
_DATASET_NODE_TYPES = {
    NodeType.KAFKA_TOPIC,
    NodeType.TABLEFLOW_TABLE,
    NodeType.UC_TABLE,
    NodeType.GLUE_TABLE,
    NodeType.GOOGLE_TABLE,
    NodeType.EXTERNAL_DATASET,
}

# Node types that map to OpenLineage Job
_JOB_NODE_TYPES = {
    NodeType.CONNECTOR,
    NodeType.KSQLDB_QUERY,
    NodeType.FLINK_JOB,
    NodeType.CONSUMER_GROUP,
}

PRODUCER = "https://github.com/takabayashi/lineage-bridge"


def _build_namespace(node: LineageNode) -> str:
    """Build an OpenLineage namespace URI from a LineageNode."""
    env = node.environment_id or "unknown"
    cluster = node.cluster_id or "default"

    if node.system == SystemType.CONFLUENT:
        return f"confluent://{env}/{cluster}"
    elif node.system == SystemType.DATABRICKS:
        workspace = node.attributes.get("workspace_url", "default")
        return f"databricks://{workspace}"
    elif node.system == SystemType.AWS:
        region = node.attributes.get("aws_region", "us-east-1")
        database = node.attributes.get("database", cluster)
        return f"aws://{region}/{database}"
    elif node.system == SystemType.GOOGLE:
        project = node.attributes.get("project_id", env)
        dataset = node.attributes.get("dataset_id", cluster)
        return f"google://{project}/{dataset}"
    else:
        return f"{node.system.value}://{env}/{cluster}"


def _build_dataset_facets(node: LineageNode, graph: LineageGraph) -> DatasetFacets:
    """Build dataset facets from a LineageNode and its schema edges."""
    facets = DatasetFacets()

    # Schema facet: look for HAS_SCHEMA edges pointing from this node
    schema_fields: list[SchemaField] = []
    for edge in graph.edges:
        if edge.src_id == node.node_id and edge.edge_type == EdgeType.HAS_SCHEMA:
            schema_node = graph.get_node(edge.dst_id)
            if schema_node:
                # Extract field info from schema node attributes
                fields = schema_node.attributes.get("fields", [])
                for f in fields:
                    if isinstance(f, dict):
                        schema_fields.append(
                            SchemaField(
                                name=f.get("name", ""),
                                type=f.get("type"),
                                description=f.get("doc"),
                            )
                        )
    if schema_fields:
        facets.schema_ = SchemaDatasetFacet(fields=schema_fields)

    # Data source facet
    if node.url:
        facets.dataSource = DataSourceDatasetFacet(
            name=node.display_name,
            uri=node.url,
        )

    # Confluent Kafka-specific facet for topics
    if node.node_type == NodeType.KAFKA_TOPIC:
        facets.confluent_kafka = ConfluentKafkaDatasetFacet(
            cluster_id=node.cluster_id,
            environment_id=node.environment_id,
            partitions=node.attributes.get("partitions"),
            replication_factor=node.attributes.get("replication_factor"),
            tags=node.tags,
        )

    return facets


def _build_job_facets(node: LineageNode) -> JobFacets:
    """Build job facets from a LineageNode."""
    facets = JobFacets()

    if node.node_type == NodeType.CONNECTOR:
        facets.confluent_connector = ConfluentConnectorJobFacet(
            connector_class=node.attributes.get("connector.class"),
            connector_type=node.attributes.get("connector_type"),
            cluster_id=node.cluster_id,
            environment_id=node.environment_id,
        )

    if node.node_type in (NodeType.KSQLDB_QUERY, NodeType.FLINK_JOB):
        query = node.attributes.get("query") or node.attributes.get("sql")
        if query:
            facets.sql = SqlJobFacet(query=query)
        facets.documentation = DocumentationJobFacet(
            description=f"{node.node_type.value}: {node.display_name}",
        )

    return facets


def _is_confluent_only(node: LineageNode) -> bool:
    """Check if a node belongs to the Confluent system."""
    return node.system == SystemType.CONFLUENT


def graph_to_events(
    graph: LineageGraph,
    *,
    confluent_only: bool = False,
    event_time: datetime | None = None,
) -> list[RunEvent]:
    """Convert a LineageGraph into OpenLineage RunEvents.

    Each Job-type node becomes a RunEvent with its input/output datasets.
    Dataset-only nodes (topics with no connected jobs) are emitted as
    single-dataset events with a synthetic "identity" job.

    Args:
        graph: The lineage graph to convert.
        confluent_only: If True, only include Confluent-system nodes.
        event_time: Override event timestamp (defaults to now).

    Returns:
        List of OpenLineage RunEvents.
    """
    now = event_time or datetime.now(UTC)
    events: list[RunEvent] = []
    covered_datasets: set[str] = set()

    # Pass 1: Create events for each Job node
    for node in graph.nodes:
        if node.node_type not in _JOB_NODE_TYPES:
            continue
        if confluent_only and not _is_confluent_only(node):
            continue
        if node.node_type == NodeType.SCHEMA:
            continue

        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []

        for edge in graph.edges:
            # Edges where this job consumes from a dataset
            if edge.dst_id == node.node_id and edge.edge_type in (
                EdgeType.CONSUMES,
                EdgeType.TRANSFORMS,
            ):
                src_node = graph.get_node(edge.src_id)
                if src_node and src_node.node_type in _DATASET_NODE_TYPES:
                    if confluent_only and not _is_confluent_only(src_node):
                        continue
                    inputs.append(
                        InputDataset(
                            namespace=_build_namespace(src_node),
                            name=src_node.qualified_name,
                            facets=_build_dataset_facets(src_node, graph),
                        )
                    )
                    covered_datasets.add(src_node.node_id)

            # Edges where this job produces to a dataset
            if edge.src_id == node.node_id and edge.edge_type in (
                EdgeType.PRODUCES,
                EdgeType.TRANSFORMS,
                EdgeType.MATERIALIZES,
            ):
                dst_node = graph.get_node(edge.dst_id)
                if dst_node and dst_node.node_type in _DATASET_NODE_TYPES:
                    if confluent_only and not _is_confluent_only(dst_node):
                        continue
                    outputs.append(
                        OutputDataset(
                            namespace=_build_namespace(dst_node),
                            name=dst_node.qualified_name,
                            facets=_build_dataset_facets(dst_node, graph),
                        )
                    )
                    covered_datasets.add(dst_node.node_id)

        event = RunEvent(
            eventTime=now,
            eventType=RunEventType.COMPLETE,
            run=Run(runId=str(uuid.uuid5(uuid.NAMESPACE_DNS, node.node_id))),
            job=Job(
                namespace=_build_namespace(node),
                name=node.qualified_name,
                facets=_build_job_facets(node),
            ),
            inputs=inputs,
            outputs=outputs,
            producer=PRODUCER,
        )
        events.append(event)

    # Pass 2: Create events for dataset nodes not covered by any job
    # These are standalone datasets (e.g., topics with only schema edges)
    for node in graph.nodes:
        if node.node_type not in _DATASET_NODE_TYPES:
            continue
        if node.node_id in covered_datasets:
            continue
        if confluent_only and not _is_confluent_only(node):
            continue

        # Synthetic identity job representing the dataset's existence
        event = RunEvent(
            eventTime=now,
            eventType=RunEventType.COMPLETE,
            run=Run(runId=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"identity:{node.node_id}"))),
            job=Job(
                namespace=_build_namespace(node),
                name=f"_identity_{node.qualified_name}",
            ),
            inputs=[],
            outputs=[
                OutputDataset(
                    namespace=_build_namespace(node),
                    name=node.qualified_name,
                    facets=_build_dataset_facets(node, graph),
                )
            ],
            producer=PRODUCER,
        )
        events.append(event)

    return events


def _parse_namespace(namespace: str) -> dict[str, str]:
    """Parse an OpenLineage namespace URI into components.

    Examples:
        "confluent://env-abc/lkc-123" -> {"system": "confluent", "env": "env-abc", ...}
        "databricks://workspace-url" -> {"system": "databricks", "env": "workspace-url"}
    """
    parts = namespace.split("://", 1)
    system = parts[0] if parts else "external"
    path = parts[1] if len(parts) > 1 else ""
    segments = path.split("/", 1)

    return {
        "system": system,
        "segment1": segments[0] if segments else "",
        "segment2": segments[1] if len(segments) > 1 else "",
    }


def _namespace_to_system(system_str: str) -> SystemType:
    """Map a namespace system prefix to a SystemType."""
    mapping = {
        "confluent": SystemType.CONFLUENT,
        "databricks": SystemType.DATABRICKS,
        "aws": SystemType.AWS,
        "google": SystemType.GOOGLE,
    }
    return mapping.get(system_str, SystemType.EXTERNAL)


def _infer_node_type(system: SystemType, is_job: bool, name: str) -> NodeType:
    """Infer the LineageBridge NodeType from system and context."""
    if is_job:
        if system == SystemType.CONFLUENT:
            return NodeType.CONNECTOR
        return NodeType.CONNECTOR  # default job type

    if system == SystemType.CONFLUENT:
        return NodeType.KAFKA_TOPIC
    elif system == SystemType.DATABRICKS:
        return NodeType.UC_TABLE
    elif system == SystemType.AWS:
        return NodeType.GLUE_TABLE
    elif system == SystemType.GOOGLE:
        return NodeType.GOOGLE_TABLE
    else:
        return NodeType.EXTERNAL_DATASET


def events_to_graph(events: list[RunEvent]) -> LineageGraph:
    """Convert OpenLineage RunEvents into a LineageGraph.

    Maps Jobs to job-type nodes and Datasets to dataset-type nodes,
    then creates edges based on input/output relationships.

    Args:
        events: List of OpenLineage RunEvents to convert.

    Returns:
        A LineageGraph populated from the events.
    """
    graph = LineageGraph()
    now = datetime.now(UTC)

    for event in events:
        job = event.job
        ns = _parse_namespace(job.namespace)
        system = _namespace_to_system(ns["system"])
        env_id = ns["segment1"]
        cluster_id = ns["segment2"]

        # Skip synthetic identity jobs
        if job.name.startswith("_identity_"):
            # Just add the output dataset
            for output in event.outputs:
                _add_dataset_node(graph, output, now)
            continue

        # Create job node
        job_type = _infer_node_type(system, is_job=True, name=job.name)
        job_node_id = f"{system.value}:{job_type.value}:{env_id}:{job.name}"

        job_attrs: dict[str, Any] = {}
        if job.facets:
            if job.facets.sql:
                job_attrs["query"] = job.facets.sql.query
            if job.facets.confluent_connector:
                cc = job.facets.confluent_connector
                if cc.connector_class:
                    job_attrs["connector.class"] = cc.connector_class
                if cc.connector_type:
                    job_attrs["connector_type"] = cc.connector_type

        job_node = LineageNode(
            node_id=job_node_id,
            system=system,
            node_type=job_type,
            qualified_name=job.name,
            display_name=job.name,
            environment_id=env_id or None,
            cluster_id=cluster_id or None,
            attributes=job_attrs,
        )
        graph.add_node(job_node)

        # Process inputs (datasets consumed by this job)
        for inp in event.inputs:
            ds_node = _add_dataset_node(graph, inp, now)
            graph.add_edge(
                LineageEdge(
                    src_id=ds_node.node_id,
                    dst_id=job_node_id,
                    edge_type=EdgeType.CONSUMES,
                )
            )

        # Process outputs (datasets produced by this job)
        for out in event.outputs:
            ds_node = _add_dataset_node(graph, out, now)
            graph.add_edge(
                LineageEdge(
                    src_id=job_node_id,
                    dst_id=ds_node.node_id,
                    edge_type=EdgeType.PRODUCES,
                )
            )

    return graph


def _add_dataset_node(
    graph: LineageGraph,
    dataset: InputDataset | OutputDataset,
    now: datetime,
) -> LineageNode:
    """Add a dataset node to the graph from an OpenLineage dataset, returning the node."""
    ns = _parse_namespace(dataset.namespace)
    system = _namespace_to_system(ns["system"])
    env_id = ns["segment1"]
    cluster_id = ns["segment2"]
    node_type = _infer_node_type(system, is_job=False, name=dataset.name)

    node_id = f"{system.value}:{node_type.value}:{env_id}:{dataset.name}"

    attrs: dict[str, Any] = {}
    tags: list[str] = []

    if dataset.facets:
        if dataset.facets.confluent_kafka:
            ck = dataset.facets.confluent_kafka
            if ck.partitions:
                attrs["partitions"] = ck.partitions
            if ck.replication_factor:
                attrs["replication_factor"] = ck.replication_factor
            tags = list(ck.tags)
        if dataset.facets.dataSource and dataset.facets.dataSource.uri:
            attrs["url"] = dataset.facets.dataSource.uri

    node = LineageNode(
        node_id=node_id,
        system=system,
        node_type=node_type,
        qualified_name=dataset.name,
        display_name=dataset.name,
        environment_id=env_id or None,
        cluster_id=cluster_id or None,
        attributes=attrs,
        tags=tags,
        first_seen=now,
        last_seen=now,
    )
    graph.add_node(node)
    return node
