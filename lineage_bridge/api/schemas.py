# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Pydantic response models for REST API endpoints.

Replaces raw dict[str, Any] returns so the OpenAPI spec (Swagger UI)
shows realistic examples instead of generic placeholders.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from lineage_bridge.api.openlineage.models import Job, RunEvent
from lineage_bridge.models.graph import LineageEdge, LineageNode

# ── Simple responses ──────────────────────────────────────────────────────


class StatusResponse(BaseModel):
    status: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"status": "ok"}],
        }
    }


class VersionResponse(BaseModel):
    version: str
    name: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"version": "0.4.0", "name": "lineage-bridge"}],
        }
    }


class GraphCreatedResponse(BaseModel):
    graph_id: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"graph_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}],
        }
    }


class GraphDeletedResponse(BaseModel):
    status: str
    graph_id: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"status": "deleted", "graph_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}],
        }
    }


class NodeCreatedResponse(BaseModel):
    status: str
    node_id: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"status": "created", "node_id": "confluent:kafka_topic:env-abc:orders"}],
        }
    }


class EdgeCreatedResponse(BaseModel):
    status: str
    edge: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "status": "created",
                    "edge": "confluent:connector:env-abc:pg-source"
                    " -> confluent:kafka_topic:env-abc:orders",
                }
            ],
        }
    }


class ImportResponse(BaseModel):
    graph_id: str
    nodes_imported: int
    edges_imported: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "graph_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "nodes_imported": 15,
                    "edges_imported": 12,
                }
            ],
        }
    }


class EventsIngestedResponse(BaseModel):
    events_stored: int
    graph_id: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"events_stored": 3, "graph_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}],
        }
    }


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: str

    model_config = {
        "json_schema_extra": {
            "examples": [{"task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "status": "pending"}],
        }
    }


class CatalogInfo(BaseModel):
    catalog_type: str
    node_type: str
    system_type: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "catalog_type": "UNITY_CATALOG",
                    "node_type": "uc_table",
                    "system_type": "databricks",
                }
            ],
        }
    }


# ── Complex responses ─────────────────────────────────────────────────────


class GraphStats(BaseModel):
    node_count: int
    edge_count: int
    pipeline_count: int
    validation_warnings: list[str] = Field(default_factory=list)


class GraphDetailResponse(BaseModel):
    graph_id: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    stats: GraphStats
    created_at: str | None
    last_modified: str | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "graph_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "nodes": [
                        {
                            "node_id": "confluent:kafka_topic:env-abc:orders",
                            "system": "confluent",
                            "node_type": "kafka_topic",
                            "qualified_name": "orders",
                            "display_name": "orders",
                            "environment_id": "env-abc",
                            "cluster_id": "lkc-123",
                            "attributes": {"partitions": 6},
                        }
                    ],
                    "edges": [
                        {
                            "src_id": "confluent:connector:env-abc:pg-source",
                            "dst_id": "confluent:kafka_topic:env-abc:orders",
                            "edge_type": "produces",
                        }
                    ],
                    "stats": {
                        "node_count": 15,
                        "edge_count": 12,
                        "pipeline_count": 3,
                        "validation_warnings": [],
                    },
                    "created_at": "2026-04-14T10:30:00+00:00",
                    "last_modified": "2026-04-14T11:00:00+00:00",
                }
            ],
        }
    }


class GraphExportResponse(BaseModel):
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "nodes": [
                        {
                            "node_id": "confluent:kafka_topic:env-abc:orders",
                            "system": "confluent",
                            "node_type": "kafka_topic",
                            "qualified_name": "orders",
                            "display_name": "orders",
                        }
                    ],
                    "edges": [
                        {
                            "src_id": "confluent:connector:env-abc:pg-source",
                            "dst_id": "confluent:kafka_topic:env-abc:orders",
                            "edge_type": "produces",
                        }
                    ],
                }
            ],
        }
    }


class ViewResponse(BaseModel):
    view: str
    events: list[RunEvent]
    systems: str | None = None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "view": "confluent",
                    "events": [
                        {
                            "eventTime": "2026-04-14T10:30:00+00:00",
                            "eventType": "COMPLETE",
                            "run": {"runId": "run-pg-source-001"},
                            "job": {
                                "namespace": "confluent://env-abc/lkc-123",
                                "name": "pg-source",
                            },
                            "inputs": [
                                {
                                    "namespace": "external://postgres",
                                    "name": "ecommerce/orders",
                                }
                            ],
                            "outputs": [
                                {
                                    "namespace": "confluent://env-abc/lkc-123",
                                    "name": "orders",
                                }
                            ],
                            "producer": "https://github.com/takabayashi/lineage-bridge",
                        }
                    ],
                    "systems": None,
                }
            ],
        }
    }


class NodeListResponse(BaseModel):
    nodes: list[LineageNode]
    total: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "nodes": [
                        {
                            "node_id": "confluent:kafka_topic:env-abc:orders",
                            "system": "confluent",
                            "node_type": "kafka_topic",
                            "qualified_name": "orders",
                            "display_name": "orders",
                            "environment_id": "env-abc",
                            "cluster_id": "lkc-123",
                            "attributes": {"partitions": 6},
                        }
                    ],
                    "total": 1,
                }
            ],
        }
    }


class EdgeListResponse(BaseModel):
    edges: list[LineageEdge]
    total: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "edges": [
                        {
                            "src_id": "confluent:connector:env-abc:pg-source",
                            "dst_id": "confluent:kafka_topic:env-abc:orders",
                            "edge_type": "produces",
                        }
                    ],
                    "total": 1,
                }
            ],
        }
    }


class TraversalResponse(BaseModel):
    origin: str
    direction: str
    hops: int
    nodes: list[LineageNode]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "origin": "confluent:kafka_topic:env-abc:orders",
                    "direction": "upstream",
                    "hops": 3,
                    "nodes": [
                        {
                            "node_id": "confluent:connector:env-abc:pg-source",
                            "system": "confluent",
                            "node_type": "connector",
                            "qualified_name": "pg-source",
                            "display_name": "pg-source",
                        }
                    ],
                }
            ],
        }
    }


class DatasetLineageOrigin(BaseModel):
    namespace: str
    name: str


class DatasetLineageResponse(BaseModel):
    origin: DatasetLineageOrigin
    direction: str
    depth: int
    events: list[RunEvent]
    datasets_visited: int
    jobs_visited: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "origin": {
                        "namespace": "confluent://env-abc/lkc-123",
                        "name": "orders",
                    },
                    "direction": "upstream",
                    "depth": 5,
                    "events": [],
                    "datasets_visited": 1,
                    "jobs_visited": 0,
                }
            ],
        }
    }


class JobDetailResponse(BaseModel):
    job: Job
    latest_inputs: list[dict[str, Any]]
    latest_outputs: list[dict[str, Any]]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "job": {
                        "namespace": "confluent://env-abc/lkc-123",
                        "name": "pg-source",
                    },
                    "latest_inputs": [
                        {
                            "namespace": "external://postgres",
                            "name": "ecommerce/orders",
                        }
                    ],
                    "latest_outputs": [
                        {
                            "namespace": "confluent://env-abc/lkc-123",
                            "name": "orders",
                        }
                    ],
                }
            ],
        }
    }
