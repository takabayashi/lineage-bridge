"""Application settings loaded from environment variables and .env file."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """LineageBridge configuration.

    All settings can be provided via environment variables prefixed with
    ``LINEAGE_BRIDGE_`` or through a ``.env`` file in the working directory.
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

    # ── Scope ───────────────────────────────────────────────────────────
    environment_ids: list[str] = Field(..., description="Confluent Cloud environment IDs to scan")
    kafka_cluster_ids: list[str] = Field(
        default_factory=list,
        description="Optional filter: specific Kafka cluster IDs. If empty, scan all.",
    )

    # ── Kafka cluster-scoped credentials (optional) ─────────────────────
    kafka_api_key: str | None = Field(
        default=None, description="Cluster-scoped Kafka API key (falls back to cloud key)"
    )
    kafka_api_secret: str | None = Field(
        default=None, description="Cluster-scoped Kafka API secret"
    )

    # ── Schema Registry credentials (optional) ─────────────────────────
    schema_registry_api_key: str | None = Field(default=None, description="Schema Registry API key")
    schema_registry_api_secret: str | None = Field(
        default=None, description="Schema Registry API secret"
    )

    # ── ksqlDB credentials (optional) ──────────────────────────────────
    ksqldb_api_key: str | None = Field(default=None, description="ksqlDB API key")
    ksqldb_api_secret: str | None = Field(default=None, description="ksqlDB API secret")

    # ── Flink credentials (optional) ───────────────────────────────────
    flink_api_key: str | None = Field(default=None, description="Flink API key")
    flink_api_secret: str | None = Field(default=None, description="Flink API secret")
    flink_region: str | None = Field(default=None, description="Flink region (e.g., us-east-1)")
    flink_cloud: str | None = Field(
        default=None, description="Flink cloud provider (e.g., aws, gcp, azure)"
    )

    # ── Output ──────────────────────────────────────────────────────────
    extract_output_path: str = Field(
        default="./lineage_graph.json",
        description="File path where the extracted lineage graph JSON is saved",
    )

    # ── Logging ─────────────────────────────────────────────────────────
    log_level: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
