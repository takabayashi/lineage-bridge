# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Shared fixtures for orchestrator and phase tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from lineage_bridge.extractors.context import ExtractionContext
from lineage_bridge.models.graph import LineageGraph


def make_settings(**overrides: Any) -> MagicMock:
    """Mock Settings with sensible defaults for orchestrator/phase tests."""
    s = MagicMock()
    s.confluent_cloud_api_key = "cloud-key"
    s.confluent_cloud_api_secret = "cloud-secret"
    s.kafka_api_key = None
    s.kafka_api_secret = None
    s.schema_registry_endpoint = None
    s.schema_registry_api_key = None
    s.schema_registry_api_secret = None
    s.ksqldb_api_key = None
    s.ksqldb_api_secret = None
    s.flink_api_key = None
    s.flink_api_secret = None
    s.tableflow_api_key = None
    s.tableflow_api_secret = None
    s.databricks_workspace_url = None
    s.databricks_token = None
    s.databricks_warehouse_id = None
    s.aws_region = "us-east-1"
    s.aws_datazone_domain_id = None
    s.aws_datazone_project_id = None
    s.gcp_project_id = None
    s.gcp_location = "us"
    s.log_level = "INFO"
    s.get_cluster_credentials.return_value = ("kafka-key", "kafka-secret")
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def cluster_payload(
    cluster_id: str = "lkc-test1",
    env_id: str = "env-test1",
    display_name: str = "my-cluster",
    rest_endpoint: str = "https://lkc-test1.us-east1.gcp.confluent.cloud:443",
) -> dict[str, Any]:
    """Build a Confluent CMK cluster payload like the API returns."""
    return {
        "id": cluster_id,
        "spec": {
            "display_name": display_name,
            "http_endpoint": rest_endpoint,
            "kafka_bootstrap_endpoint": "SASL_SSL://lkc-test1.us-east1.gcp:9092",
            "region": "us-east1",
            "cloud": "GCP",
            "environment": {"id": env_id},
        },
        "metadata": {"resource_name": ""},
    }


def make_context(
    *,
    settings: MagicMock | None = None,
    env_id: str = "env-test1",
    clusters: list[dict[str, Any]] | None = None,
    graph: LineageGraph | None = None,
    on_progress: Any = None,
    cloud: AsyncMock | None = None,
    sr_endpoint: str | None = None,
    sr_key: str | None = None,
    sr_secret: str | None = None,
    sr_credentials: dict[str, dict[str, str]] | None = None,
    flink_credentials: dict[str, dict[str, str]] | None = None,
    enable_connect: bool = True,
    enable_ksqldb: bool = True,
    enable_flink: bool = True,
    enable_schema_registry: bool = True,
    enable_stream_catalog: bool = True,
    enable_tableflow: bool = True,
) -> ExtractionContext:
    """Build an ExtractionContext with mocked Settings + cloud client."""
    return ExtractionContext(
        settings=settings or make_settings(),
        cloud=cloud or AsyncMock(),
        env_id=env_id,
        graph=graph or LineageGraph(),
        clusters=clusters or [cluster_payload()],
        sr_endpoint=sr_endpoint,
        sr_key=sr_key,
        sr_secret=sr_secret,
        sr_credentials=sr_credentials,
        flink_credentials=flink_credentials,
        enable_connect=enable_connect,
        enable_ksqldb=enable_ksqldb,
        enable_flink=enable_flink,
        enable_schema_registry=enable_schema_registry,
        enable_stream_catalog=enable_stream_catalog,
        enable_tableflow=enable_tableflow,
        on_progress=on_progress,
    )


def make_async_client_mock() -> AsyncMock:
    """Build an AsyncMock that doubles as an async context manager + has extract()."""
    inst = AsyncMock()
    inst.extract = AsyncMock(return_value=([], []))
    inst.__aenter__ = AsyncMock(return_value=inst)
    inst.__aexit__ = AsyncMock(return_value=False)
    return inst


@pytest.fixture()
def settings_factory():
    """Factory fixture returning the make_settings helper."""
    return make_settings


@pytest.fixture()
def cluster_factory():
    """Factory fixture returning the cluster_payload helper."""
    return cluster_payload


@pytest.fixture()
def context_factory():
    """Factory fixture returning the make_context helper."""
    return make_context


@pytest.fixture()
def client_mock_factory():
    """Factory fixture returning make_async_client_mock."""
    return make_async_client_mock


@pytest.fixture()
def progress_log() -> list[tuple[str, str]]:
    """List that tests append progress events to via on_progress."""
    return []


@pytest.fixture()
def progress_callback(progress_log):
    """on_progress callable that writes into progress_log."""

    def _progress(phase: str, detail: str = "") -> None:
        progress_log.append((phase, detail))

    return _progress
