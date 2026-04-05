# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Discover Confluent Cloud environments, clusters, and services.

Used by the UI to let users interactively select what to extract.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lineage_bridge.clients.base import ConfluentClient

logger = logging.getLogger(__name__)


@dataclass
class EnvironmentInfo:
    """Summary of a Confluent Cloud environment."""

    id: str
    display_name: str
    stream_governance_package: str  # e.g. "ESSENTIALS", "ADVANCED", ""


@dataclass
class ClusterInfo:
    """Summary of a Kafka cluster within an environment."""

    id: str
    display_name: str
    cloud: str  # aws, gcp, azure
    region: str
    availability: str  # SINGLE_ZONE, MULTI_ZONE
    rest_endpoint: str


@dataclass
class EnvironmentServices:
    """What services are available in an environment."""

    clusters: list[ClusterInfo]
    has_schema_registry: bool
    schema_registry_endpoint: str | None
    has_ksqldb: bool
    ksqldb_cluster_count: int
    has_flink: bool
    flink_pool_count: int


async def list_environments(cloud: ConfluentClient) -> list[EnvironmentInfo]:
    """List all Confluent Cloud environments accessible with the current API key."""
    items = await cloud.paginate("/org/v2/environments")
    envs = []
    for item in items:
        envs.append(
            EnvironmentInfo(
                id=item.get("id", ""),
                display_name=item.get("display_name", item.get("id", "")),
                stream_governance_package=item.get("stream_governance_config", {}).get(
                    "package", ""
                ),
            )
        )
    return sorted(envs, key=lambda e: e.display_name)


async def list_clusters(cloud: ConfluentClient, environment_id: str) -> list[ClusterInfo]:
    """List Kafka clusters in an environment."""
    items = await cloud.paginate("/cmk/v2/clusters", params={"environment": environment_id})
    clusters = []
    for item in items:
        spec = item.get("spec", {})
        cluster_id = item.get("id", "")
        region = spec.get("region", "")
        cloud_provider = spec.get("cloud", "").lower()
        rest_endpoint = spec.get("http_endpoint", "")
        if not rest_endpoint and region and cloud_provider:
            rest_endpoint = f"https://{cluster_id}.{region}.{cloud_provider}.confluent.cloud:443"
        clusters.append(
            ClusterInfo(
                id=cluster_id,
                display_name=spec.get("display_name", cluster_id),
                cloud=cloud_provider,
                region=region,
                availability=spec.get("availability", ""),
                rest_endpoint=rest_endpoint,
            )
        )
    return sorted(clusters, key=lambda c: c.display_name)


async def discover_services(cloud: ConfluentClient, environment_id: str) -> EnvironmentServices:
    """Check which services (SR, ksqlDB, Flink) are available in an environment."""
    clusters = await list_clusters(cloud, environment_id)

    # Schema Registry
    has_sr = False
    sr_endpoint = None
    try:
        sr_items = await cloud.paginate("/srcm/v2/clusters", params={"environment": environment_id})
        if sr_items:
            has_sr = True
            sr_cluster = sr_items[0]
            sr_endpoint = sr_cluster.get("spec", {}).get("http_endpoint") or sr_cluster.get(
                "status", {}
            ).get("http_endpoint")
    except Exception:
        logger.debug("SR discovery failed for %s", environment_id, exc_info=True)

    # ksqlDB
    has_ksqldb = False
    ksqldb_count = 0
    try:
        ksql_items = await cloud.paginate(
            "/ksqldbcm/v2/clusters", params={"environment": environment_id}
        )
        ksqldb_count = len(ksql_items)
        has_ksqldb = ksqldb_count > 0
    except Exception:
        logger.debug("ksqlDB discovery failed for %s", environment_id, exc_info=True)

    # Flink
    has_flink = False
    flink_count = 0
    try:
        flink_items = await cloud.paginate(
            "/fcpm/v2/compute-pools", params={"environment": environment_id}
        )
        flink_count = len(flink_items)
        has_flink = flink_count > 0
    except Exception:
        logger.debug("Flink discovery failed for %s", environment_id, exc_info=True)

    return EnvironmentServices(
        clusters=clusters,
        has_schema_registry=has_sr,
        schema_registry_endpoint=sr_endpoint,
        has_ksqldb=has_ksqldb,
        ksqldb_cluster_count=ksqldb_count,
        has_flink=has_flink,
        flink_pool_count=flink_count,
    )
