"""Visual styling constants for the lineage graph UI."""

from __future__ import annotations

from lineage_bridge.models.graph import EdgeType, NodeType

# ── Color palette per node type ────────────────────────────────────────
NODE_COLORS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "#2196F3",  # blue
    NodeType.CONNECTOR: "#FF9800",  # orange
    NodeType.KSQLDB_QUERY: "#9C27B0",  # purple
    NodeType.FLINK_JOB: "#009688",  # teal
    NodeType.TABLEFLOW_TABLE: "#4CAF50",  # green
    NodeType.UC_TABLE: "#FFC107",  # gold / amber
    NodeType.SCHEMA: "#9E9E9E",  # gray
    NodeType.EXTERNAL_DATASET: "#F44336",  # red
    NodeType.CONSUMER_GROUP: "#03A9F4",  # light blue
}

# ── Node sizes (agraph pixel diameter) ─────────────────────────────────
NODE_SIZES: dict[NodeType, int] = {
    NodeType.KAFKA_TOPIC: 30,
    NodeType.CONNECTOR: 25,
    NodeType.KSQLDB_QUERY: 28,
    NodeType.FLINK_JOB: 28,
    NodeType.TABLEFLOW_TABLE: 25,
    NodeType.UC_TABLE: 25,
    NodeType.SCHEMA: 18,
    NodeType.EXTERNAL_DATASET: 22,
    NodeType.CONSUMER_GROUP: 20,
}

# ── Edge colors per type ───────────────────────────────────────────────
EDGE_COLORS: dict[EdgeType, str] = {
    EdgeType.PRODUCES: "#4CAF50",  # green
    EdgeType.CONSUMES: "#2196F3",  # blue
    EdgeType.TRANSFORMS: "#9C27B0",  # purple
    EdgeType.MATERIALIZES: "#FF9800",  # orange
    EdgeType.HAS_SCHEMA: "#9E9E9E",  # gray
    EdgeType.MEMBER_OF: "#607D8B",  # blue-gray
}

# ── Human-readable labels ─────────────────────────────────────────────
NODE_TYPE_LABELS: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "Kafka Topic",
    NodeType.CONNECTOR: "Connector",
    NodeType.KSQLDB_QUERY: "ksqlDB Query",
    NodeType.FLINK_JOB: "Flink Job",
    NodeType.TABLEFLOW_TABLE: "Tableflow Table",
    NodeType.UC_TABLE: "Unity Catalog Table",
    NodeType.SCHEMA: "Schema",
    NodeType.EXTERNAL_DATASET: "External Dataset",
    NodeType.CONSUMER_GROUP: "Consumer Group",
}

EDGE_TYPE_LABELS: dict[EdgeType, str] = {
    EdgeType.PRODUCES: "produces",
    EdgeType.CONSUMES: "consumes",
    EdgeType.TRANSFORMS: "transforms",
    EdgeType.MATERIALIZES: "materializes",
    EdgeType.HAS_SCHEMA: "has schema",
    EdgeType.MEMBER_OF: "member of",
}

# ── Node shape mapping (vis.js / agraph shapes) ───────────────────────
NODE_SHAPES: dict[NodeType, str] = {
    NodeType.KAFKA_TOPIC: "dot",
    NodeType.CONNECTOR: "diamond",
    NodeType.KSQLDB_QUERY: "star",
    NodeType.FLINK_JOB: "star",
    NodeType.TABLEFLOW_TABLE: "square",
    NodeType.UC_TABLE: "square",
    NodeType.SCHEMA: "triangle",
    NodeType.EXTERNAL_DATASET: "hexagon",
    NodeType.CONSUMER_GROUP: "triangleDown",
}
