# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the shared OpenLineage namespace normalizer.

Both the Google and DataZone providers depend on this — if the FQN format
or the allowlist semantics change, those providers stop linking.
"""

from __future__ import annotations

from types import SimpleNamespace

from lineage_bridge.api.openlineage.normalize import kafka_fqn, normalize_event


def _ds(ns, name):
    return SimpleNamespace(namespace=ns, name=name)


def _ev(inputs, outputs):
    return SimpleNamespace(
        inputs=[_ds(*x) for x in inputs],
        outputs=[_ds(*x) for x in outputs],
    )


class TestKafkaFqn:
    def test_simple_topic(self):
        assert kafka_fqn("lkc-1", "orders") == "kafka:lkc-1.orders"

    def test_dotted_topic_uses_backticks(self):
        # Matches what Google's processOpenLineageRunEvent records.
        assert kafka_fqn("lkc-1", "lb.orders_v2") == "kafka:lkc-1.`lb.orders_v2`"


class TestNormalizeEventGoogleAllowlist:
    def test_confluent_input_to_kafka(self):
        ev = _ev(
            inputs=[("confluent://env-1/lkc-abc", "topic")],
            outputs=[("google://proj/ds", "proj.ds.tbl")],
        )
        normalize_event(ev, allow={"bigquery"})
        assert ev.inputs[0].namespace == "kafka://lkc-abc"
        assert ev.outputs[0].namespace == "bigquery"

    def test_confluent_output_to_kafka_for_intermediates(self):
        # Flink/ksqlDB writes back to Kafka — must survive normalization.
        ev = _ev(
            inputs=[("confluent://env-1/lkc-abc", "src_topic")],
            outputs=[("confluent://env-1/lkc-abc", "dst_topic")],
        )
        normalize_event(ev, allow={"bigquery"})
        assert len(ev.outputs) == 1
        assert ev.outputs[0].namespace == "kafka://lkc-abc"

    def test_drops_uc_glue_external(self):
        ev = _ev(
            inputs=[
                ("confluent://env/lkc", "topic"),
                ("aws://us-east-1/db", "glue_table"),  # dropped under bigquery allowlist
            ],
            outputs=[
                ("databricks://workspace", "uc.tbl"),  # dropped
                ("google://p/d", "p.d.t"),
            ],
        )
        normalize_event(ev, allow={"bigquery"})
        assert [d.namespace for d in ev.inputs] == ["kafka://lkc"]
        assert [d.namespace for d in ev.outputs] == ["bigquery"]


class TestNormalizeEventDataZoneAllowlist:
    def test_aws_output_kept(self):
        ev = _ev(
            inputs=[("confluent://env/lkc-1", "topic")],
            outputs=[("aws://us-east-1/db", "glue_table")],
        )
        normalize_event(ev, allow={"aws"})
        assert ev.outputs[0].namespace == "aws://us-east-1/db"

    def test_can_combine_allowlists(self):
        # DataZone use case: accept both BigQuery (cross-cloud) and AWS targets.
        ev = _ev(
            inputs=[("confluent://env/lkc-1", "topic")],
            outputs=[
                ("google://p/d", "p.d.t"),
                ("aws://us-east-1/db", "glue.tbl"),
            ],
        )
        normalize_event(ev, allow={"bigquery", "aws"})
        ns = sorted(d.namespace for d in ev.outputs)
        assert ns == ["aws://us-east-1/db", "bigquery"]
