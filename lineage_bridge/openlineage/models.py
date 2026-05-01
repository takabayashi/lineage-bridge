# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""OpenLineage data models (Pydantic v2).

Follows the OpenLineage specification:
https://openlineage.io/docs/spec/object-model

These models represent the core OpenLineage types used to expose
Confluent stream lineage in a standard, interoperable format.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field

# ── Enums ──────────────────────────────────────────────────────────────────


class RunEventType(StrEnum):
    """Types of OpenLineage run events."""

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"
    ABORT = "ABORT"


# ── Facets ─────────────────────────────────────────────────────────────────


PRODUCER_URI = "https://github.com/takabayashi/lineage-bridge"


class BaseFacet(BaseModel):
    """Base class for all OpenLineage facets.

    Per the OpenLineage spec, every facet must include ``_producer`` (the URI
    of the producer that emitted it) and ``_schemaURL`` (the URL of the JSON
    schema describing the facet). Without these, spec-conformant consumers
    silently drop the facet's payload.
    """

    producer: str = Field(default=PRODUCER_URI, alias="_producer")
    schema_url: str | None = Field(default=None, alias="_schemaURL")

    model_config = {"extra": "allow", "populate_by_name": True}


class SchemaField(BaseModel):
    """A single field in a dataset schema."""

    name: str
    type: str | None = None
    description: str | None = None


_FACET_BASE = "https://openlineage.io/spec/facets"


class SchemaDatasetFacet(BaseFacet):
    """Schema information for a dataset."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-1/SchemaDatasetFacet.json", alias="_schemaURL"
    )
    fields: list[SchemaField] = Field(default_factory=list)


class DataSourceDatasetFacet(BaseFacet):
    """Data source connection details for a dataset."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/DataSourceDatasetFacet.json", alias="_schemaURL"
    )
    name: str | None = None
    uri: str | None = None


class SqlJobFacet(BaseFacet):
    """SQL query associated with a job."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/SQLJobFacet.json", alias="_schemaURL"
    )
    query: str


class DocumentationDatasetFacet(BaseFacet):
    """Human-readable documentation for a dataset."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/DocumentationDatasetFacet.json", alias="_schemaURL"
    )
    description: str


class DocumentationJobFacet(BaseFacet):
    """Human-readable documentation for a job."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/DocumentationJobFacet.json", alias="_schemaURL"
    )
    description: str


class SourceCodeLocationJobFacet(BaseFacet):
    """Source code location for a job."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/SourceCodeLocationJobFacet.json", alias="_schemaURL"
    )
    type: str | None = None
    url: str | None = None


class DataQualityMetricsInputDatasetFacet(BaseFacet):
    """Data quality metrics for an input dataset."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/DataQualityMetricsInputDatasetFacet.json",
        alias="_schemaURL",
    )
    row_count: int | None = Field(default=None, alias="rowCount")
    bytes: int | None = None


class OutputStatisticsOutputDatasetFacet(BaseFacet):
    """Output statistics for a dataset."""

    schema_url: str | None = Field(
        default=f"{_FACET_BASE}/1-0-0/OutputStatisticsOutputDatasetFacet.json",
        alias="_schemaURL",
    )
    row_count: int | None = Field(default=None, alias="rowCount")
    bytes: int | None = None


# ── Custom Facets (LineageBridge-specific) ─────────────────────────────────


class ConfluentKafkaDatasetFacet(BaseFacet):
    """Confluent Kafka-specific metadata for a dataset (topic)."""

    cluster_id: str | None = None
    environment_id: str | None = None
    partitions: int | None = None
    replication_factor: int | None = None
    tags: list[str] = Field(default_factory=list)


class ConfluentConnectorJobFacet(BaseFacet):
    """Confluent connector-specific metadata for a job."""

    connector_class: str | None = None
    connector_type: str | None = None
    cluster_id: str | None = None
    environment_id: str | None = None


# ── Dataset Facets Container ───────────────────────────────────────────────


class DatasetFacets(BaseModel):
    """Container for dataset-level facets."""

    schema_: SchemaDatasetFacet | None = Field(default=None, alias="schema")
    dataSource: DataSourceDatasetFacet | None = None
    documentation: DocumentationDatasetFacet | None = None
    confluent_kafka: ConfluentKafkaDatasetFacet | None = None

    model_config = {"extra": "allow", "populate_by_name": True}


class InputDatasetFacets(BaseModel):
    """Container for input-specific dataset facets."""

    dataQualityMetrics: DataQualityMetricsInputDatasetFacet | None = None

    model_config = {"extra": "allow", "populate_by_name": True}


class OutputDatasetFacets(BaseModel):
    """Container for output-specific dataset facets."""

    outputStatistics: OutputStatisticsOutputDatasetFacet | None = None

    model_config = {"extra": "allow", "populate_by_name": True}


# ── Job Facets Container ──────────────────────────────────────────────────


class JobFacets(BaseModel):
    """Container for job-level facets."""

    sql: SqlJobFacet | None = None
    documentation: DocumentationJobFacet | None = None
    sourceCodeLocation: SourceCodeLocationJobFacet | None = None
    confluent_connector: ConfluentConnectorJobFacet | None = None

    model_config = {"extra": "allow", "populate_by_name": True}


# ── Run Facets Container ─────────────────────────────────────────────────


class RunFacets(BaseModel):
    """Container for run-level facets."""

    model_config = {"extra": "allow"}


# ── Core Types ─────────────────────────────────────────────────────────────


class Dataset(BaseModel):
    """An OpenLineage dataset — a named data asset in a namespace.

    In LineageBridge, Kafka topics, Tableflow tables, and catalog tables
    (UC, Glue, Google) are represented as datasets.
    """

    namespace: str = Field(..., description="Dataset namespace (e.g. confluent://env-abc/lkc-123)")
    name: str = Field(..., description="Dataset name (e.g. topic name or table name)")
    facets: DatasetFacets | None = None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "namespace": "confluent://env-abc/lkc-123",
                    "name": "lineage_bridge.orders_v2",
                    "facets": {
                        "schema": {
                            "fields": [
                                {"name": "order_id", "type": "string"},
                                {"name": "product", "type": "string"},
                                {"name": "quantity", "type": "int"},
                                {"name": "price", "type": "double"},
                            ]
                        },
                        "dataSource": {
                            "name": "orders_v2",
                            "uri": "confluent://env-abc/lkc-123/lineage_bridge.orders_v2",
                        },
                    },
                }
            ],
        }
    }


class InputDataset(BaseModel):
    """A dataset consumed by a job."""

    namespace: str
    name: str
    facets: DatasetFacets | None = None
    inputFacets: InputDatasetFacets | None = None


class OutputDataset(BaseModel):
    """A dataset produced by a job."""

    namespace: str
    name: str
    facets: DatasetFacets | None = None
    outputFacets: OutputDatasetFacets | None = None


class Job(BaseModel):
    """An OpenLineage job — a process that consumes and produces datasets.

    In LineageBridge, connectors, ksqlDB queries, Flink jobs, and
    consumer groups are represented as jobs.
    """

    namespace: str = Field(..., description="Job namespace (e.g. confluent://env-abc/lkc-123)")
    name: str = Field(..., description="Job name (e.g. connector or query name)")
    facets: JobFacets | None = None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "namespace": "confluent://env-abc/lkc-123",
                    "name": "datagen-orders",
                    "facets": {
                        "confluent_connector": {
                            "connector_class": "DatagenSource",
                            "connector_type": "source",
                            "cluster_id": "lkc-123",
                            "environment_id": "env-abc",
                        }
                    },
                }
            ],
        }
    }


class Run(BaseModel):
    """An OpenLineage run — a single execution instance of a job."""

    runId: str = Field(..., description="Unique run identifier (UUID)")
    facets: RunFacets | None = None

    model_config = {
        "json_schema_extra": {
            "examples": [{"runId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}],
        }
    }


class RunEvent(BaseModel):
    """An OpenLineage run event — the top-level event envelope.

    Each event describes a state transition (START, RUNNING, COMPLETE, FAIL, ABORT)
    of a job run, along with the datasets it consumes and produces.
    """

    eventTime: datetime = Field(..., description="ISO 8601 timestamp of the event")
    eventType: RunEventType = Field(..., description="Event type (START, COMPLETE, etc.)")
    run: Run
    job: Job
    inputs: list[InputDataset] = Field(default_factory=list)
    outputs: list[OutputDataset] = Field(default_factory=list)
    producer: str = Field(
        default="https://github.com/takabayashi/lineage-bridge",
        description="URI identifying the producer of this event",
    )
    schemaURL: str = Field(
        default="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        description="URI of the OpenLineage schema for this event",
    )

    model_config = {
        "json_schema_extra": {
            "title": "OpenLineage RunEvent",
            "examples": [
                {
                    "eventTime": "2026-04-14T10:30:00+00:00",
                    "eventType": "COMPLETE",
                    "run": {"runId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"},
                    "job": {
                        "namespace": "confluent://env-abc/lkc-123",
                        "name": "datagen-orders",
                    },
                    "inputs": [],
                    "outputs": [
                        {
                            "namespace": "confluent://env-abc/lkc-123",
                            "name": "lineage_bridge.orders_v2",
                        }
                    ],
                    "producer": "https://github.com/takabayashi/lineage-bridge",
                    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
                }
            ],
        }
    }
