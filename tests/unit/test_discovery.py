# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.clients.discovery."""

from __future__ import annotations

import httpx
import pytest
import respx

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.clients.discovery import (
    EnvironmentInfo,
    discover_services,
    list_clusters,
    list_environments,
)

API_KEY = "test-key"
API_SECRET = "test-secret"
BASE_URL = "https://api.confluent.cloud"


@pytest.fixture()
def cloud():
    return ConfluentClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        timeout=5.0,
        max_retries=0,
    )


# ── list_environments ────────────────────────────────────────────────────


@respx.mock
async def test_list_environments_paginates(cloud):
    """list_environments follows pagination and returns sorted EnvironmentInfo list."""
    page1 = {
        "data": [
            {
                "id": "env-zzz",
                "display_name": "staging",
                "stream_governance_config": {"package": "ESSENTIALS"},
            },
        ],
        "metadata": {
            "next": f"{BASE_URL}/org/v2/environments?page_token=tok2&page_size=100",
        },
    }
    page2 = {
        "data": [
            {
                "id": "env-aaa",
                "display_name": "production",
                "stream_governance_config": {"package": "ADVANCED"},
            },
        ],
        "metadata": {"next": None},
    }

    route = respx.get(f"{BASE_URL}/org/v2/environments")
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    envs = await list_environments(cloud)

    assert len(envs) == 2
    # Results are sorted by display_name
    assert envs[0] == EnvironmentInfo(
        id="env-aaa",
        display_name="production",
        stream_governance_package="ADVANCED",
    )
    assert envs[1] == EnvironmentInfo(
        id="env-zzz",
        display_name="staging",
        stream_governance_package="ESSENTIALS",
    )


@respx.mock
async def test_list_environments_empty(cloud):
    """list_environments returns an empty list when no environments exist."""
    respx.get(f"{BASE_URL}/org/v2/environments").mock(
        return_value=httpx.Response(200, json={"data": [], "metadata": {}})
    )

    envs = await list_environments(cloud)
    assert envs == []


@respx.mock
async def test_list_environments_missing_fields(cloud):
    """list_environments handles items with missing optional fields."""
    respx.get(f"{BASE_URL}/org/v2/environments").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [{"id": "env-bare"}],
                "metadata": {},
            },
        )
    )

    envs = await list_environments(cloud)
    assert len(envs) == 1
    assert envs[0].id == "env-bare"
    assert envs[0].display_name == "env-bare"  # falls back to id
    assert envs[0].stream_governance_package == ""


# ── list_clusters ────────────────────────────────────────────────────────


@respx.mock
async def test_list_clusters_paginates(cloud):
    """list_clusters follows pagination and builds ClusterInfo with spec fields."""
    page1 = {
        "data": [
            {
                "id": "lkc-bbb",
                "spec": {
                    "display_name": "beta",
                    "cloud": "AWS",
                    "region": "us-east-1",
                    "availability": "SINGLE_ZONE",
                    "http_endpoint": "https://lkc-bbb.us-east-1.aws.confluent.cloud:443",
                },
            },
        ],
        "metadata": {
            "next": f"{BASE_URL}/cmk/v2/clusters?page_token=p2&page_size=100",
        },
    }
    page2 = {
        "data": [
            {
                "id": "lkc-aaa",
                "spec": {
                    "display_name": "alpha",
                    "cloud": "GCP",
                    "region": "us-central1",
                    "availability": "MULTI_ZONE",
                    "http_endpoint": "https://lkc-aaa.us-central1.gcp.confluent.cloud:443",
                },
            },
        ],
        "metadata": {"next": None},
    }

    route = respx.get(f"{BASE_URL}/cmk/v2/clusters")
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    clusters = await list_clusters(cloud, "env-123")

    assert len(clusters) == 2
    # Sorted by display_name
    assert clusters[0].display_name == "alpha"
    assert clusters[0].cloud == "gcp"
    assert clusters[1].display_name == "beta"
    assert clusters[1].cloud == "aws"


@respx.mock
async def test_list_clusters_endpoint_fallback(cloud):
    """When http_endpoint is missing, list_clusters constructs it from region+cloud."""
    respx.get(f"{BASE_URL}/cmk/v2/clusters").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "lkc-xyz",
                        "spec": {
                            "display_name": "fallback-cluster",
                            "cloud": "AZURE",
                            "region": "westus2",
                            "availability": "SINGLE_ZONE",
                            # no http_endpoint
                        },
                    },
                ],
                "metadata": {},
            },
        )
    )

    clusters = await list_clusters(cloud, "env-123")

    assert len(clusters) == 1
    assert clusters[0].rest_endpoint == "https://lkc-xyz.westus2.azure.confluent.cloud:443"


# ── discover_services ────────────────────────────────────────────────────


@respx.mock
async def test_discover_services_full(cloud):
    """discover_services returns full info when all service APIs succeed."""
    # Clusters
    respx.get(f"{BASE_URL}/cmk/v2/clusters").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "lkc-1",
                        "spec": {
                            "display_name": "main",
                            "cloud": "AWS",
                            "region": "us-east-1",
                            "availability": "MULTI_ZONE",
                            "http_endpoint": "https://lkc-1.example.com",
                        },
                    },
                ],
                "metadata": {},
            },
        )
    )

    # Schema Registry
    respx.get(f"{BASE_URL}/srcm/v2/clusters").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "lsrc-1",
                        "spec": {"http_endpoint": "https://sr.example.com"},
                    }
                ],
                "metadata": {},
            },
        )
    )

    # ksqlDB
    respx.get(f"{BASE_URL}/ksqldbcm/v2/clusters").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [{"id": "ksql-1"}, {"id": "ksql-2"}],
                "metadata": {},
            },
        )
    )

    # Flink
    respx.get(f"{BASE_URL}/fcpm/v2/compute-pools").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [{"id": "lfcp-1"}],
                "metadata": {},
            },
        )
    )

    services = await discover_services(cloud, "env-abc")

    assert len(services.clusters) == 1
    assert services.clusters[0].id == "lkc-1"
    assert services.has_schema_registry is True
    assert services.schema_registry_endpoint == "https://sr.example.com"
    assert services.has_ksqldb is True
    assert services.ksqldb_cluster_count == 2
    assert services.has_flink is True
    assert services.flink_pool_count == 1


@respx.mock
async def test_discover_services_partial_failure(cloud):
    """discover_services still returns clusters when optional APIs fail."""
    # Clusters succeed
    respx.get(f"{BASE_URL}/cmk/v2/clusters").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "lkc-1",
                        "spec": {
                            "display_name": "main",
                            "cloud": "AWS",
                            "region": "us-east-1",
                            "availability": "SINGLE_ZONE",
                            "http_endpoint": "https://lkc-1.example.com",
                        },
                    }
                ],
                "metadata": {},
            },
        )
    )

    # SR, ksqlDB, Flink all fail
    respx.get(f"{BASE_URL}/srcm/v2/clusters").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    respx.get(f"{BASE_URL}/ksqldbcm/v2/clusters").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    respx.get(f"{BASE_URL}/fcpm/v2/compute-pools").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )

    services = await discover_services(cloud, "env-abc")

    # Clusters still returned
    assert len(services.clusters) == 1
    # Failed services flagged as unavailable
    assert services.has_schema_registry is False
    assert services.schema_registry_endpoint is None
    assert services.has_ksqldb is False
    assert services.ksqldb_cluster_count == 0
    assert services.has_flink is False
    assert services.flink_pool_count == 0
