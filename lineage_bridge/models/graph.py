# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Core data models for stream lineage representation."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import networkx as nx
from pydantic import BaseModel, Field


class NodeType(StrEnum):
    """Types of nodes in the lineage graph.

    Per ADR-021, every catalog table (Unity Catalog, AWS Glue, Google Data
    Lineage, Snowflake, Watsonx, ...) shares one node type — `CATALOG_TABLE`
    — with the `catalog_type` field on the node carrying the discriminator.
    """

    KAFKA_TOPIC = "kafka_topic"
    CONNECTOR = "connector"
    KSQLDB_QUERY = "ksqldb_query"
    FLINK_JOB = "flink_job"
    TABLEFLOW_TABLE = "tableflow_table"
    CATALOG_TABLE = "catalog_table"
    SCHEMA = "schema"
    EXTERNAL_DATASET = "external_dataset"
    CONSUMER_GROUP = "consumer_group"
    # Processing node sitting between two CATALOG_TABLE nodes — discovered
    # via the Databricks lineage-tracking API's notebookInfos field. Sibling
    # of FLINK_JOB / KSQLDB_QUERY (transform-style node, not a data store).
    NOTEBOOK = "notebook"


class EdgeType(StrEnum):
    """Types of edges (relationships) in the lineage graph."""

    PRODUCES = "produces"
    CONSUMES = "consumes"
    TRANSFORMS = "transforms"
    MATERIALIZES = "materializes"
    HAS_SCHEMA = "has_schema"
    MEMBER_OF = "member_of"


class SystemType(StrEnum):
    """Source systems that contribute nodes to the graph."""

    CONFLUENT = "confluent"
    DATABRICKS = "databricks"
    AWS = "aws"
    GOOGLE = "google"
    EXTERNAL = "external"


class LineageNode(BaseModel):
    """A node in the lineage graph representing a data asset or processing step."""

    node_id: str = Field(
        ...,
        description="Unique identifier. Format: {system}:{type}:{env_id}:{qualified_name}",
    )
    system: SystemType
    node_type: NodeType
    qualified_name: str
    display_name: str
    environment_id: str | None = None
    environment_name: str | None = None
    cluster_id: str | None = None
    cluster_name: str | None = None
    catalog_type: str | None = Field(
        default=None,
        description=(
            "For NodeType.CATALOG_TABLE only — discriminator for which catalog "
            "owns this node (e.g. 'UNITY_CATALOG', 'AWS_GLUE', 'GOOGLE_DATA_LINEAGE', "
            "'AWS_DATAZONE', 'SNOWFLAKE', 'WATSONX'). None for non-catalog node types."
        ),
    )
    attributes: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    url: str | None = None
    first_seen: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen: datetime = Field(default_factory=lambda: datetime.now(UTC))


class LineageEdge(BaseModel):
    """A directed edge in the lineage graph representing a data flow relationship."""

    src_id: str
    dst_id: str
    edge_type: EdgeType
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="1.0 = deterministic, <1.0 = inferred",
    )
    attributes: dict[str, Any] = Field(default_factory=dict)
    first_seen: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def edge_key(self) -> tuple[str, str, str]:
        """Unique key for this edge: (src, dst, type)."""
        return (self.src_id, self.dst_id, self.edge_type.value)


@dataclass
class PushResult:
    """Result summary from pushing lineage metadata to a catalog."""

    tables_updated: int = 0
    properties_set: int = 0
    comments_set: int = 0
    bridge_rows_inserted: int = 0
    errors: list[str] = field(default_factory=list)
    # Benign skips (e.g. PERMISSION_DENIED on tables the principal doesn't own,
    # bridge INSERT after a CREATE the principal lacked rights for). The push
    # still completes; these are surfaced as warnings, not errors.
    skipped: list[str] = field(default_factory=list)


class LineageGraph:
    """In-memory lineage graph backed by networkx.DiGraph.

    Stores LineageNode objects as node data and LineageEdge objects as edge data.
    Provides traversal, filtering, and serialization capabilities.
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()
        self._nodes: dict[str, LineageNode] = {}
        self._edges: dict[tuple[str, str, str], LineageEdge] = {}

    # ── Mutation ────────────────────────────────────────────────────────

    def add_node(self, node: LineageNode) -> None:
        """Add or update a node in the graph.

        If the node already exists, attributes/tags are merged and optional
        scalar fields (cluster_id, environment_name, url, ...) prefer a
        non-None value from either side — extractors that don't know a
        topic's cluster (Flink, ksqlDB) shouldn't blow away that field
        when KafkaAdmin set it earlier.
        """
        if node.node_id in self._nodes:
            existing = self._nodes[node.node_id]
            merged_attrs = {**existing.attributes, **node.attributes}
            merged_tags = list(set(existing.tags + node.tags))
            update: dict[str, Any] = {
                "first_seen": existing.first_seen,
                "attributes": merged_attrs,
                "tags": merged_tags,
            }
            for f in (
                "environment_id",
                "environment_name",
                "cluster_id",
                "cluster_name",
                "catalog_type",
                "url",
            ):
                if getattr(node, f) is None and getattr(existing, f) is not None:
                    update[f] = getattr(existing, f)
            self._nodes[node.node_id] = node.model_copy(update=update)
        else:
            self._nodes[node.node_id] = node
        self._graph.add_node(node.node_id)

    def add_edge(self, edge: LineageEdge) -> None:
        """Add or update an edge in the graph.

        Both source and destination nodes must already exist.
        If the edge already exists, its last_seen is updated.
        """
        if edge.src_id not in self._nodes:
            raise ValueError(f"Source node '{edge.src_id}' not found in graph")
        if edge.dst_id not in self._nodes:
            raise ValueError(f"Destination node '{edge.dst_id}' not found in graph")

        key = edge.edge_key
        if key in self._edges:
            existing = self._edges[key]
            self._edges[key] = edge.model_copy(update={"first_seen": existing.first_seen})
        else:
            self._edges[key] = edge
        self._graph.add_edge(edge.src_id, edge.dst_id, edge_type=edge.edge_type.value)

    def remove_edge(self, src_id: str, dst_id: str, edge_type: EdgeType) -> bool:
        """Remove the edge with the given composite key. Returns True if removed.

        No-op (returns False) if the edge doesn't exist. Used by the
        Databricks UC provider to swap a TRANSFORMS table-to-table edge
        for a NOTEBOOK hop when the upstream lineage pass turns up the
        producer attribution that the downstream pass missed.
        """
        key = (src_id, dst_id, edge_type.value)
        if key not in self._edges:
            return False
        del self._edges[key]
        # networkx DiGraph: edges are keyed by (src,dst); remove only when
        # no other edge type still uses that pair (we don't model multi-edge
        # variants on the nx side, so this is fine for the typical case).
        if any(
            (s, d, t) in self._edges
            for (s, d, t) in [(src_id, dst_id, et.value) for et in EdgeType if et != edge_type]
        ):
            return True
        if self._graph.has_edge(src_id, dst_id):
            self._graph.remove_edge(src_id, dst_id)
        return True

    # ── Queries ─────────────────────────────────────────────────────────

    def get_node(self, node_id: str) -> LineageNode | None:
        """Return a node by ID, or None if not found."""
        return self._nodes.get(node_id)

    def get_edge(self, src_id: str, dst_id: str, edge_type: EdgeType) -> LineageEdge | None:
        """Return an edge by its composite key, or None if not found."""
        return self._edges.get((src_id, dst_id, edge_type.value))

    def get_neighbors(self, node_id: str, direction: str = "both") -> list[LineageNode]:
        """Return neighboring nodes.

        Args:
            node_id: The node to query.
            direction: "upstream" (predecessors), "downstream" (successors), or "both".
        """
        if node_id not in self._graph:
            return []
        ids: set[str] = set()
        if direction in ("upstream", "both"):
            ids.update(self._graph.predecessors(node_id))
        if direction in ("downstream", "both"):
            ids.update(self._graph.successors(node_id))
        return [self._nodes[nid] for nid in ids if nid in self._nodes]

    def upstream(self, node_id: str, hops: int = 1) -> list[LineageNode]:
        """Return all nodes up to *hops* steps upstream (predecessors)."""
        return self._traverse(node_id, hops, direction="upstream")

    def downstream(self, node_id: str, hops: int = 1) -> list[LineageNode]:
        """Return all nodes up to *hops* steps downstream (successors)."""
        return self._traverse(node_id, hops, direction="downstream")

    def _traverse(self, node_id: str, hops: int, direction: str) -> list[LineageNode]:
        """BFS traversal up to a given number of hops."""
        if node_id not in self._graph:
            return []

        visited: set[str] = set()
        frontier: set[str] = {node_id}

        for _ in range(hops):
            next_frontier: set[str] = set()
            for nid in frontier:
                if direction == "upstream":
                    neighbors = set(self._graph.predecessors(nid))
                else:
                    neighbors = set(self._graph.successors(nid))
                next_frontier.update(neighbors - visited - {node_id})
            visited.update(next_frontier)
            frontier = next_frontier
            if not frontier:
                break

        return [self._nodes[nid] for nid in visited if nid in self._nodes]

    def get_upstream(
        self, node_id: str, max_hops: int = 10
    ) -> list[tuple[LineageNode, LineageEdge, int]]:
        """Return all upstream nodes with their connecting edges and hop distance.

        Uses BFS traversal following edges in reverse (predecessors).
        Returns: [(upstream_node, edge_to_it, hop_distance), ...]
        """
        if node_id not in self._graph:
            return []

        results: list[tuple[LineageNode, LineageEdge, int]] = []
        visited: set[str] = {node_id}
        queue: deque[tuple[str, int]] = deque([(node_id, 0)])

        while queue:
            current_id, depth = queue.popleft()
            if depth >= max_hops:
                continue
            for pred_id in self._graph.predecessors(current_id):
                if pred_id in visited:
                    continue
                visited.add(pred_id)
                # Find the edge connecting pred -> current
                edge = None
                for key, e in self._edges.items():
                    if key[0] == pred_id and key[1] == current_id:
                        edge = e
                        break
                pred_node = self._nodes.get(pred_id)
                if pred_node and edge:
                    results.append((pred_node, edge, depth + 1))
                queue.append((pred_id, depth + 1))

        return results

    def filter_by_type(self, node_type: NodeType) -> list[LineageNode]:
        """Return all nodes matching the given type."""
        return [n for n in self._nodes.values() if n.node_type == node_type]

    def filter_catalog_nodes(self, catalog_type: str | None = None) -> list[LineageNode]:
        """Return CATALOG_TABLE nodes, optionally filtered by `catalog_type`.

        With `catalog_type=None`, returns every catalog table regardless of which
        catalog owns it. With a string, returns only the matching subset (e.g.
        `filter_catalog_nodes("UNITY_CATALOG")`).
        """
        out = [n for n in self._nodes.values() if n.node_type == NodeType.CATALOG_TABLE]
        if catalog_type is not None:
            out = [n for n in out if n.catalog_type == catalog_type]
        return out

    def filter_by_env(self, environment_id: str) -> list[LineageNode]:
        """Return all nodes belonging to the given environment."""
        return [n for n in self._nodes.values() if n.environment_id == environment_id]

    def search_nodes(self, query: str) -> list[LineageNode]:
        """Case-insensitive substring search across display_name and qualified_name."""
        q = query.lower()
        return [
            n
            for n in self._nodes.values()
            if q in n.display_name.lower() or q in n.qualified_name.lower()
        ]

    @property
    def nodes(self) -> list[LineageNode]:
        """All nodes in the graph."""
        return list(self._nodes.values())

    @property
    def edges(self) -> list[LineageEdge]:
        """All edges in the graph."""
        return list(self._edges.values())

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    @property
    def pipeline_count(self) -> int:
        """Number of data-flow pipelines (connected components with ≥1 edge).

        Builds a subgraph excluding HAS_SCHEMA edges so that schemas
        don't inflate the count, then counts only components that have
        at least one edge (i.e. actual data flow, not isolated nodes).
        """
        if not self._graph:
            return 0
        # Build subgraph without HAS_SCHEMA edges
        flow_edges = [
            (u, v)
            for u, v, d in self._graph.edges(data=True)
            if d.get("edge_type") != EdgeType.HAS_SCHEMA.value
        ]
        if not flow_edges:
            return 0
        subgraph = nx.DiGraph()
        subgraph.add_edges_from(flow_edges)
        return nx.number_weakly_connected_components(subgraph)

    # ── Validation ──────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Return a list of validation warnings (empty = clean graph)."""
        warnings: list[str] = []
        for node_id, node in self._nodes.items():
            if node.node_type == NodeType.SCHEMA:
                continue
            if self._graph.in_degree(node_id) == 0 and self._graph.out_degree(node_id) == 0:
                warnings.append(f"Orphan node: {node_id}")
        for edge in self._edges.values():
            if edge.src_id not in self._nodes:
                warnings.append(f"Dangling edge src: {edge.src_id}")
            if edge.dst_id not in self._nodes:
                warnings.append(f"Dangling edge dst: {edge.dst_id}")
        return warnings

    # ── Serialization ───────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Serialize the graph to a plain dictionary."""
        return {
            "nodes": [n.model_dump(mode="json") for n in self._nodes.values()],
            "edges": [e.model_dump(mode="json") for e in self._edges.values()],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LineageGraph:
        """Deserialize a graph from a plain dictionary."""
        graph = cls()
        for node_data in data.get("nodes", []):
            graph.add_node(LineageNode.model_validate(node_data))
        for edge_data in data.get("edges", []):
            graph.add_edge(LineageEdge.model_validate(edge_data))
        return graph

    def to_json_file(self, path: str | Path) -> None:
        """Persist the graph as a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, default=str))

    @classmethod
    def from_json_file(cls, path: str | Path) -> LineageGraph:
        """Load a graph from a JSON file."""
        data = json.loads(Path(path).read_text())
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return f"LineageGraph(nodes={self.node_count}, edges={self.edge_count})"
