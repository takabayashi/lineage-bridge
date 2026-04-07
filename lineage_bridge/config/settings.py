# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Application settings loaded from environment variables and .env file.

Only Confluent Cloud credentials live here. Runtime scope (which environments,
clusters, extractors to run) is selected interactively in the UI.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClusterCredential(BaseModel):
    """API key/secret pair for a specific Kafka cluster."""

    api_key: str
    api_secret: str


class Settings(BaseSettings):
    """LineageBridge credentials.

    Loaded from environment variables prefixed with ``LINEAGE_BRIDGE_``
    or from a ``.env`` file. Only credentials belong here — extraction
    scope is chosen at runtime via the UI or CLI arguments.
    """

    model_config = SettingsConfigDict(
        env_prefix="LINEAGE_BRIDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Confluent Cloud (org-level / cloud API key) ─────────────────────
    confluent_cloud_api_key: str = Field(..., description="Cloud-level API key for Confluent Cloud")
    confluent_cloud_api_secret: str = Field(
        ..., description="Cloud-level API secret for Confluent Cloud"
    )

    # ── Optional service-scoped credentials (fall back to cloud key) ────
    kafka_api_key: str | None = Field(default=None, description="Cluster-scoped Kafka API key")
    kafka_api_secret: str | None = Field(
        default=None, description="Cluster-scoped Kafka API secret"
    )
    schema_registry_endpoint: str | None = Field(
        default=None,
        description="Schema Registry endpoint URL (e.g. https://psrc-xxxxx.region.cloud.confluent.cloud)",
    )
    schema_registry_api_key: str | None = Field(default=None, description="Schema Registry API key")
    schema_registry_api_secret: str | None = Field(
        default=None, description="Schema Registry API secret"
    )
    ksqldb_api_key: str | None = Field(default=None, description="ksqlDB API key")
    ksqldb_api_secret: str | None = Field(default=None, description="ksqlDB API secret")
    flink_api_key: str | None = Field(default=None, description="Flink API key")
    flink_api_secret: str | None = Field(default=None, description="Flink API secret")
    tableflow_api_key: str | None = Field(default=None, description="Tableflow API key")
    tableflow_api_secret: str | None = Field(default=None, description="Tableflow API secret")

    # ── Databricks Unity Catalog ─────────────────────────────────────
    databricks_workspace_url: str | None = Field(
        default=None,
        description="Databricks workspace URL (e.g. https://myworkspace.cloud.databricks.com)",
    )
    databricks_token: str | None = Field(
        default=None,
        description="Databricks personal access token or service principal token",
    )
    databricks_warehouse_id: str | None = Field(
        default=None,
        description="Databricks SQL Warehouse ID for statement execution",
    )

    # ── AWS ──────────────────────────────────────────────────────────────
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region for Glue Data Catalog",
    )

    # ── Audit log watcher ─────────────────────────────────────────────────
    audit_log_bootstrap_servers: str | None = Field(
        default=None,
        description="Bootstrap servers for the Confluent Cloud audit log cluster",
    )
    audit_log_api_key: str | None = Field(
        default=None, description="API key for the audit log cluster"
    )
    audit_log_api_secret: str | None = Field(
        default=None, description="API secret for the audit log cluster"
    )

    # ── Per-cluster credentials ─────────────────────────────────────────
    # JSON map: {"lkc-abc123": {"api_key": "...", "api_secret": "..."}, ...}
    # Set via env: LINEAGE_BRIDGE_CLUSTER_CREDENTIALS='{"lkc-abc":{"api_key":".."}}'
    cluster_credentials: dict[str, ClusterCredential] = Field(
        default_factory=dict,
        description="Per-cluster API credentials keyed by cluster ID",
    )

    # ── Logging ─────────────────────────────────────────────────────────
    log_level: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )

    def get_cluster_credentials(self, cluster_id: str) -> tuple[str, str]:
        """Return (api_key, api_secret) for a cluster.

        Resolution order:
        1. Per-cluster credential (cluster_credentials map)
        2. Global kafka_api_key / kafka_api_secret
        3. Cloud-level API key (fallback)
        """
        if cluster_id in self.cluster_credentials:
            cred = self.cluster_credentials[cluster_id]
            return cred.api_key, cred.api_secret
        if self.kafka_api_key and self.kafka_api_secret:
            return self.kafka_api_key, self.kafka_api_secret
        return self.confluent_cloud_api_key, self.confluent_cloud_api_secret
