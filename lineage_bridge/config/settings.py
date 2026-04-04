"""Application settings loaded from environment variables and .env file.

Only Confluent Cloud credentials live here. Runtime scope (which environments,
clusters, extractors to run) is selected interactively in the UI.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # ── Logging ─────────────────────────────────────────────────────────
    log_level: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
