# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.models.audit_event."""

from __future__ import annotations

from lineage_bridge.models.audit_event import AuditEvent, is_lineage_relevant
from tests.conftest import load_fixture


class TestIsLineageRelevant:
    def test_exact_match_create_topics(self):
        assert is_lineage_relevant("kafka.CreateTopics") is True

    def test_exact_match_delete_connector(self):
        assert is_lineage_relevant("connect.DeleteConnector") is True

    def test_exact_match_alter_configs(self):
        assert is_lineage_relevant("kafka.IncrementalAlterConfigs") is True

    def test_prefix_match_ksql(self):
        assert is_lineage_relevant("ksql.CreateQuery") is True

    def test_prefix_match_flink(self):
        assert is_lineage_relevant("io.confluent.flink.v1.statements.create") is True

    def test_prefix_match_schema_registry(self):
        assert is_lineage_relevant("io.confluent.schema_registry.RegisterSchema") is True

    def test_non_relevant_fetch(self):
        assert is_lineage_relevant("kafka.Fetch") is False

    def test_non_relevant_produce(self):
        assert is_lineage_relevant("kafka.Produce") is False

    def test_non_relevant_empty(self):
        assert is_lineage_relevant("") is False

    def test_non_relevant_describe(self):
        assert is_lineage_relevant("kafka.DescribeTopics") is False


class TestAuditEventFromCloudEvent:
    def test_parse_create_topic(self):
        payload = load_fixture("audit_event_create_topic.json")
        event = AuditEvent.from_cloud_event(payload)

        assert event is not None
        assert event.id == "ae-001"
        assert event.method_name == "kafka.CreateTopics"
        assert event.environment_id == "env-2yzd0o"
        assert event.cluster_id == "lkc-jkn588"
        assert event.principal == "User:sa-demo"
        assert "topic=lineage_bridge.new_topic" in event.resource_name

    def test_parse_delete_connector(self):
        payload = load_fixture("audit_event_delete_connector.json")
        event = AuditEvent.from_cloud_event(payload)

        assert event is not None
        assert event.id == "ae-002"
        assert event.method_name == "connect.DeleteConnector"
        assert event.environment_id == "env-2yzd0o"
        assert event.cluster_id == "lkc-jkn588"
        assert event.principal == "User:sa-demo"

    def test_missing_method_name_returns_none(self):
        payload = {"id": "x", "time": "2026-01-01T00:00:00Z", "data": {}}
        assert AuditEvent.from_cloud_event(payload) is None

    def test_missing_time_returns_none(self):
        payload = {"id": "x", "data": {"methodName": "kafka.CreateTopics"}}
        assert AuditEvent.from_cloud_event(payload) is None

    def test_malformed_payload_returns_none(self):
        assert AuditEvent.from_cloud_event({}) is None

    def test_missing_data_returns_none(self):
        payload = {"id": "x", "time": "2026-01-01T00:00:00Z"}
        assert AuditEvent.from_cloud_event(payload) is None

    def test_crn_without_environment(self):
        payload = {
            "id": "x",
            "source": "crn://confluent.cloud/organization=org-abc",
            "time": "2026-01-01T00:00:00Z",
            "data": {
                "methodName": "kafka.CreateTopics",
                "resourceName": "",
                "authenticationInfo": {"principal": "User:test"},
            },
        }
        event = AuditEvent.from_cloud_event(payload)
        assert event is not None
        assert event.environment_id is None
        assert event.cluster_id is None

    def test_time_parsing_with_offset(self):
        payload = {
            "id": "x",
            "source": "",
            "time": "2026-04-06T10:30:00+00:00",
            "data": {
                "methodName": "kafka.DeleteTopics",
                "resourceName": "",
                "authenticationInfo": {"principal": "User:test"},
            },
        }
        event = AuditEvent.from_cloud_event(payload)
        assert event is not None
        assert event.time.hour == 10
