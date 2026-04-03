"""Unit tests for lineage_bridge.config.settings.Settings."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lineage_bridge.config.settings import Settings

# ── test_settings_from_env ─────────────────────────────────────────────


def test_settings_from_env(monkeypatch):
    """Settings loads correctly from environment variables."""
    monkeypatch.setenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY", "cloud-key-123")
    monkeypatch.setenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET", "cloud-secret-456")
    monkeypatch.setenv("LINEAGE_BRIDGE_ENVIRONMENT_IDS", '["env-abc123", "env-xyz789"]')
    monkeypatch.setenv("LINEAGE_BRIDGE_KAFKA_CLUSTER_IDS", '["lkc-abc123"]')
    monkeypatch.setenv("LINEAGE_BRIDGE_KAFKA_API_KEY", "kafka-key-789")
    monkeypatch.setenv("LINEAGE_BRIDGE_KAFKA_API_SECRET", "kafka-secret-012")
    monkeypatch.setenv("LINEAGE_BRIDGE_LOG_LEVEL", "DEBUG")

    # Avoid loading from .env file on disk
    settings = Settings(
        _env_file=None,
    )

    assert settings.confluent_cloud_api_key == "cloud-key-123"
    assert settings.confluent_cloud_api_secret == "cloud-secret-456"
    assert settings.environment_ids == ["env-abc123", "env-xyz789"]
    assert settings.kafka_cluster_ids == ["lkc-abc123"]
    assert settings.kafka_api_key == "kafka-key-789"
    assert settings.kafka_api_secret == "kafka-secret-012"
    assert settings.log_level == "DEBUG"


# ── test_settings_required_fields ──────────────────────────────────────


def test_settings_required_fields(monkeypatch):
    """ValidationError is raised when required fields are missing."""
    # Clear all relevant env vars to ensure nothing leaks in
    for key in [
        "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY",
        "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET",
        "LINEAGE_BRIDGE_ENVIRONMENT_IDS",
    ]:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    errors = exc_info.value.errors()
    missing_fields = {e["loc"][0] for e in errors}
    assert "confluent_cloud_api_key" in missing_fields
    assert "confluent_cloud_api_secret" in missing_fields
    assert "environment_ids" in missing_fields


# ── test_settings_defaults ─────────────────────────────────────────────


def test_settings_defaults(monkeypatch):
    """Optional fields have sensible default values."""
    monkeypatch.setenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY", "key")
    monkeypatch.setenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET", "secret")
    monkeypatch.setenv("LINEAGE_BRIDGE_ENVIRONMENT_IDS", '["env-001"]')

    settings = Settings(_env_file=None)

    # Defaults for optional fields
    assert settings.kafka_cluster_ids == []
    assert settings.kafka_api_key is None
    assert settings.kafka_api_secret is None
    assert settings.schema_registry_api_key is None
    assert settings.schema_registry_api_secret is None
    assert settings.ksqldb_api_key is None
    assert settings.ksqldb_api_secret is None
    assert settings.flink_api_key is None
    assert settings.flink_api_secret is None
    assert settings.flink_region is None
    assert settings.flink_cloud is None
    assert settings.extract_output_path == "./lineage_graph.json"
    assert settings.log_level == "INFO"
