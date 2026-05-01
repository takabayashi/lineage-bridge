# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Tests for OpenLineage Pydantic models."""

from __future__ import annotations

from datetime import UTC, datetime

from lineage_bridge.api.openlineage.models import (
    ConfluentKafkaDatasetFacet,
    Dataset,
    DatasetFacets,
    InputDataset,
    InputDatasetFacets,
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


class TestRunEvent:
    def test_minimal_event(self):
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="abc-123"),
            job=Job(namespace="confluent://env-abc/lkc-123", name="my-job"),
        )
        assert evt.eventType == RunEventType.COMPLETE
        assert evt.job.name == "my-job"
        assert evt.inputs == []
        assert evt.outputs == []
        assert "lineage-bridge" in evt.producer

    def test_event_with_inputs_outputs(self):
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.START,
            run=Run(runId="run-1"),
            job=Job(namespace="confluent://env/lkc", name="connector-1"),
            inputs=[
                InputDataset(namespace="confluent://env/lkc", name="orders"),
            ],
            outputs=[
                OutputDataset(namespace="databricks://ws", name="catalog.schema.orders"),
            ],
        )
        assert len(evt.inputs) == 1
        assert len(evt.outputs) == 1
        assert evt.inputs[0].name == "orders"
        assert evt.outputs[0].name == "catalog.schema.orders"

    def test_json_roundtrip(self):
        evt = RunEvent(
            eventTime=datetime(2026, 1, 1, tzinfo=UTC),
            eventType=RunEventType.COMPLETE,
            run=Run(runId="run-1"),
            job=Job(
                namespace="confluent://env/lkc",
                name="flink-job",
                facets=JobFacets(sql=SqlJobFacet(query="SELECT * FROM t")),
            ),
            inputs=[
                InputDataset(
                    namespace="confluent://env/lkc",
                    name="input-topic",
                    facets=DatasetFacets(
                        schema_=SchemaDatasetFacet(fields=[SchemaField(name="id", type="INT")])
                    ),
                ),
            ],
        )
        data = evt.model_dump(mode="json")
        restored = RunEvent.model_validate(data)
        assert restored.job.facets.sql.query == "SELECT * FROM t"
        assert restored.inputs[0].facets.schema_.fields[0].name == "id"

    def test_all_event_types(self):
        for etype in RunEventType:
            evt = RunEvent(
                eventTime=datetime(2026, 1, 1, tzinfo=UTC),
                eventType=etype,
                run=Run(runId="run-1"),
                job=Job(namespace="test://ns", name="job"),
            )
            assert evt.eventType == etype


class TestDataset:
    def test_minimal_dataset(self):
        ds = Dataset(namespace="confluent://env/lkc", name="orders")
        assert ds.namespace == "confluent://env/lkc"
        assert ds.name == "orders"
        assert ds.facets is None

    def test_dataset_with_facets(self):
        ds = Dataset(
            namespace="confluent://env/lkc",
            name="orders",
            facets=DatasetFacets(
                schema_=SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="id", type="INT"),
                        SchemaField(name="amount", type="DOUBLE", description="Order total"),
                    ]
                ),
                confluent_kafka=ConfluentKafkaDatasetFacet(
                    cluster_id="lkc-123",
                    environment_id="env-abc",
                    partitions=6,
                    replication_factor=3,
                    tags=["pii", "production"],
                ),
            ),
        )
        assert len(ds.facets.schema_.fields) == 2
        assert ds.facets.confluent_kafka.partitions == 6
        assert "pii" in ds.facets.confluent_kafka.tags


class TestJob:
    def test_minimal_job(self):
        job = Job(namespace="confluent://env/lkc", name="my-connector")
        assert job.name == "my-connector"
        assert job.facets is None

    def test_job_with_sql_facet(self):
        job = Job(
            namespace="confluent://env/flink-env",
            name="flink-join",
            facets=JobFacets(sql=SqlJobFacet(query="INSERT INTO out SELECT * FROM inp")),
        )
        assert job.facets.sql.query.startswith("INSERT")


class TestInputOutputDatasets:
    def test_input_dataset(self):
        inp = InputDataset(
            namespace="confluent://env/lkc",
            name="orders",
            inputFacets=InputDatasetFacets(),
        )
        assert inp.name == "orders"

    def test_output_dataset(self):
        out = OutputDataset(
            namespace="databricks://ws",
            name="catalog.schema.orders",
        )
        assert out.namespace == "databricks://ws"


class TestFacetsExtraFields:
    def test_dataset_facets_allow_extra(self):
        """OpenLineage spec allows custom facets — extra fields must be accepted."""
        facets = DatasetFacets(customFacet={"key": "value"})  # type: ignore[call-arg]
        data = facets.model_dump()
        assert data["customFacet"] == {"key": "value"}
