# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Shared utility: walk a graph upstream from a target node and emit the
multi-hop chain (source connectors → topics → Flink/ksqlDB → intermediate
topics → sink) in a JSON-serialisable form.

Used by every catalog writer that wants more than the flat "source topics"
list — UC TBLPROPERTIES/COMMENT/bridge table, Glue Parameters/Description,
DataZone asset descriptions. Centralising the shape keeps catalogs aligned
so a hop added here surfaces everywhere.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from lineage_bridge.models.graph import EdgeType, LineageGraph, LineageNode, NodeType

# Node types we surface in the chain. Excludes SCHEMA (folded into hops as
# schema_fields) and CONSUMER_GROUP (not lineage-relevant for catalog UIs).
_DEFAULT_TYPES: set[NodeType] = {
    NodeType.KAFKA_TOPIC,
    NodeType.CONNECTOR,
    NodeType.FLINK_JOB,
    NodeType.KSQLDB_QUERY,
    NodeType.EXTERNAL_DATASET,
    NodeType.TABLEFLOW_TABLE,
}

# Map NodeType → short kind label used in JSON output. Keeping these stable
# matters: catalog consumers (and `format_chain_summary` below) key off them.
_KIND_LABELS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "topic",
    NodeType.CONNECTOR: "connector",
    NodeType.FLINK_JOB: "flink_job",
    NodeType.KSQLDB_QUERY: "ksqldb_query",
    NodeType.EXTERNAL_DATASET: "external_dataset",
    NodeType.TABLEFLOW_TABLE: "tableflow_table",
}


@dataclass
class ChainHop:
    """One step in an upstream chain. JSON-serialisable via ``to_dict``."""

    hop: int  # distance from target; 1 = direct upstream
    kind: str
    qualified_name: str
    display_name: str
    sql: str | None = None
    connector_class: str | None = None
    schema_fields: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        # Drop None / empty fields so the JSON stays compact in TBLPROPERTIES.
        d = asdict(self)
        return {k: v for k, v in d.items() if v not in (None, [], "")}


def build_upstream_chain(
    graph: LineageGraph,
    target_node_id: str,
    *,
    include_node_types: set[NodeType] | None = None,
    max_hops: int = 10,
) -> list[ChainHop]:
    """Return the upstream chain from ``target_node_id`` ordered by hop distance.

    Each hop carries SQL for Flink/ksqlDB jobs and schema fields for Kafka
    topics (looked up via ``HAS_SCHEMA`` edges, same source the Dataplex
    registrar uses).
    """
    types = include_node_types or _DEFAULT_TYPES
    upstream = graph.get_upstream(target_node_id, max_hops=max_hops)
    upstream.sort(key=lambda x: (x[2], x[0].qualified_name))

    hops: list[ChainHop] = []
    for node, _edge, depth in upstream:
        if node.node_type not in types:
            continue
        hops.append(_node_to_hop(node, depth, graph))
    return hops


def _node_to_hop(node: LineageNode, depth: int, graph: LineageGraph) -> ChainHop:
    kind = _KIND_LABELS.get(node.node_type, node.node_type.value)
    sql = None
    connector_class = None
    if node.node_type in (NodeType.FLINK_JOB, NodeType.KSQLDB_QUERY):
        sql = node.attributes.get("query") or node.attributes.get("sql")
    if node.node_type == NodeType.CONNECTOR:
        connector_class = node.attributes.get("connector.class") or node.attributes.get(
            "connector_class"
        )
    schema_fields = _schema_fields(graph, node) if node.node_type == NodeType.KAFKA_TOPIC else []
    return ChainHop(
        hop=depth,
        kind=kind,
        qualified_name=node.qualified_name,
        display_name=node.display_name,
        sql=sql,
        connector_class=connector_class,
        schema_fields=schema_fields,
    )


def _schema_fields(graph: LineageGraph, node: LineageNode) -> list[dict[str, str]]:
    """Pull schema fields from a HAS_SCHEMA-linked SCHEMA node, if any.

    Mirrors :func:`google_dataplex.DataplexAssetRegistrar._schema_fields` so
    every catalog sees the same column list for a given topic.
    """
    for edge in graph.edges:
        if edge.src_id != node.node_id or edge.edge_type != EdgeType.HAS_SCHEMA:
            continue
        schema_node = graph.get_node(edge.dst_id)
        if not schema_node:
            continue
        fields = schema_node.attributes.get("fields") or []
        out: list[dict[str, str]] = []
        for f in fields:
            if not isinstance(f, dict):
                continue
            entry: dict[str, str] = {}
            if f.get("name"):
                entry["name"] = str(f["name"])
            if f.get("type") is not None:
                entry["type"] = str(f["type"])
            doc = f.get("description") or f.get("doc")
            if doc:
                entry["description"] = str(doc)
            if entry:
                out.append(entry)
        if out:
            return out
    return []


def chain_to_json(chain: list[ChainHop], *, max_bytes: int | None = None) -> tuple[str, bool]:
    """Serialise a chain to compact JSON, optionally truncating to fit ``max_bytes``.

    Returns ``(json_string, truncated)`` so callers can flag truncation in a
    sidecar property. Truncation drops the *farthest* hops first so the
    immediate upstream stays visible.
    """
    import json

    payload = [h.to_dict() for h in chain]
    encoded = json.dumps(payload, separators=(",", ":"))
    if max_bytes is None or len(encoded.encode("utf-8")) <= max_bytes:
        return encoded, False

    truncated = list(payload)
    while truncated:
        size = len(json.dumps(truncated, separators=(",", ":")).encode("utf-8"))
        if size <= max_bytes:
            break
        truncated.pop()
    return json.dumps(truncated, separators=(",", ":")), True


def format_chain_summary(chain: list[ChainHop], target_display: str) -> str:
    """Build a human-readable multi-line description of the chain.

    Used in UC ``COMMENT ON TABLE`` and Glue ``Description``. Walks from the
    farthest upstream toward the target so the reader sees the data flow in
    the natural left-to-right order.
    """
    if not chain:
        return ""
    by_hop: dict[int, list[ChainHop]] = {}
    for hop in chain:
        by_hop.setdefault(hop.hop, []).append(hop)

    lines = ["Upstream lineage:"]
    for depth in sorted(by_hop.keys(), reverse=True):
        prefix = "  " * (max(by_hop) - depth)
        for h in by_hop[depth]:
            qual = h.qualified_name or h.display_name
            extra = ""
            if h.kind in ("flink_job", "ksqldb_query") and h.sql:
                snippet = h.sql.replace("\n", " ").strip()
                if len(snippet) > 80:
                    snippet = snippet[:77] + "..."
                extra = f" [SQL: {snippet}]"
            elif h.kind == "connector" and h.connector_class:
                extra = f" [{h.connector_class}]"
            elif h.kind == "topic" and h.schema_fields:
                extra = f" [{len(h.schema_fields)} columns]"
            lines.append(f"{prefix}- {h.kind}: {qual}{extra}")
    lines.append(f"  → {target_display}")
    return "\n".join(lines)
