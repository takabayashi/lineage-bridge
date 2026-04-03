"""Orchestrator that runs all extractors and builds the unified lineage graph.

Execution order:
  1. Discover clusters via the Cloud API.
  2. KafkaAdmin — establish the topic inventory.
  3. Connect, ksqlDB, Flink — transformation / processing edges (parallel).
  4. SchemaRegistry, StreamCatalog — enrichment (parallel).
  5. Tableflow — bridge to UC/Glue (last, depends on topic nodes).
  6. Merge everything into a single LineageGraph and persist.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.clients.connect import ConnectClient
from lineage_bridge.clients.flink import FlinkClient
from lineage_bridge.clients.kafka_admin import KafkaAdminClient
from lineage_bridge.clients.ksqldb import KsqlDBClient
from lineage_bridge.clients.schema_registry import SchemaRegistryClient
from lineage_bridge.clients.stream_catalog import StreamCatalogClient
from lineage_bridge.clients.tableflow import TableflowClient
from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageEdge, LineageGraph, LineageNode

logger = logging.getLogger(__name__)


# ── cluster / endpoint discovery ────────────────────────────────────────


async def _discover_kafka_clusters(
    cloud: ConfluentClient, environment_id: str, filter_ids: list[str]
) -> list[dict[str, Any]]:
    """Return Kafka cluster metadata for *environment_id*."""
    clusters = await cloud.paginate("/cmk/v2/clusters", params={"environment": environment_id})
    if filter_ids:
        clusters = [c for c in clusters if c.get("id") in filter_ids]
    return clusters


async def _discover_sr_cluster(
    cloud: ConfluentClient, environment_id: str
) -> dict[str, Any] | None:
    """Return the Schema Registry cluster for *environment_id*, or None."""
    try:
        items = await cloud.paginate("/srcm/v2/clusters", params={"environment": environment_id})
        return items[0] if items else None
    except Exception:
        logger.warning("Could not discover SR cluster for %s", environment_id, exc_info=True)
        return None


# ── graph merging ───────────────────────────────────────────────────────


def _merge_into(
    graph: LineageGraph,
    nodes: list[LineageNode],
    edges: list[LineageEdge],
) -> None:
    """Add *nodes* and *edges* into *graph*, tolerating missing endpoints."""
    for node in nodes:
        graph.add_node(node)
    for edge in edges:
        try:
            graph.add_edge(edge)
        except ValueError:
            logger.debug(
                "Skipping edge %s -> %s (%s): endpoint not in graph",
                edge.src_id,
                edge.dst_id,
                edge.edge_type.value,
            )


# ── safe extractor runner ───────────────────────────────────────────────


async def _safe_extract(label: str, coro: Any) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Run an extractor coroutine, returning empty on failure."""
    try:
        return await coro
    except Exception:
        logger.warning("Extractor '%s' failed — continuing", label, exc_info=True)
        return [], []


# ── main orchestration ──────────────────────────────────────────────────


async def run_extraction(settings: Settings) -> LineageGraph:
    """Run the full extraction pipeline and return the merged graph."""
    graph = LineageGraph()

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    try:
        for env_id in settings.environment_ids:
            await _extract_environment(settings, cloud, env_id, graph)
    finally:
        await cloud.close()

    # Persist.
    graph.to_json_file(settings.extract_output_path)
    logger.info(
        "Lineage graph saved to %s  (%d nodes, %d edges)",
        settings.extract_output_path,
        graph.node_count,
        graph.edge_count,
    )
    return graph


async def _extract_environment(
    settings: Settings,
    cloud: ConfluentClient,
    env_id: str,
    graph: LineageGraph,
) -> None:
    """Run all extractors for a single Confluent Cloud environment."""
    logger.info("Starting extraction for environment %s", env_id)

    # ── discover Kafka clusters ────────────────────────────────────────
    kafka_clusters = await _discover_kafka_clusters(cloud, env_id, settings.kafka_cluster_ids)
    if not kafka_clusters:
        logger.warning("No Kafka clusters found in environment %s", env_id)
        return

    # ── discover Schema Registry ───────────────────────────────────────
    sr_cluster = await _discover_sr_cluster(cloud, env_id)
    sr_endpoint: str | None = None
    if sr_cluster:
        sr_endpoint = sr_cluster.get("spec", {}).get("http_endpoint") or sr_cluster.get(
            "status", {}
        ).get("http_endpoint")

    # Credential resolution helpers.
    kafka_key = settings.kafka_api_key or settings.confluent_cloud_api_key
    kafka_secret = settings.kafka_api_secret or settings.confluent_cloud_api_secret
    sr_key = settings.schema_registry_api_key or settings.confluent_cloud_api_key
    sr_secret = settings.schema_registry_api_secret or settings.confluent_cloud_api_secret

    # ── Phase 1: Kafka Admin (per cluster) ─────────────────────────────
    for cluster in kafka_clusters:
        cluster_id = cluster.get("id", "")
        spec = cluster.get("spec", {})
        rest_endpoint = spec.get("http_endpoint", "")
        if not rest_endpoint:
            # Build from cluster metadata.
            region = spec.get("region", "")
            cloud_provider = spec.get("cloud", "").lower()
            if region and cloud_provider:
                rest_endpoint = (
                    f"https://{cluster_id}.{region}.{cloud_provider}.confluent.cloud:443"
                )

        if not rest_endpoint:
            logger.warning("No REST endpoint for cluster %s — skipping", cluster_id)
            continue

        kafka_client = KafkaAdminClient(
            base_url=rest_endpoint,
            api_key=kafka_key,
            api_secret=kafka_secret,
            cluster_id=cluster_id,
            environment_id=env_id,
        )
        async with kafka_client:
            nodes, edges = await _safe_extract(f"KafkaAdmin:{cluster_id}", kafka_client.extract())
            _merge_into(graph, nodes, edges)

    # ── Phase 2: Connect, ksqlDB, Flink (parallel) ────────────────────
    phase2_tasks = []

    # Connect — one per cluster.
    for cluster in kafka_clusters:
        cluster_id = cluster.get("id", "")
        connect_client = ConnectClient(
            api_key=settings.confluent_cloud_api_key,
            api_secret=settings.confluent_cloud_api_secret,
            environment_id=env_id,
            kafka_cluster_id=cluster_id,
        )

        async def _run_connect(
            c: ConnectClient = connect_client,
            cid: str = cluster_id,
        ) -> tuple[list[LineageNode], list[LineageEdge]]:
            async with c:
                return await _safe_extract(f"Connect:{cid}", c.extract())

        phase2_tasks.append(_run_connect())

    # ksqlDB
    ksql_client = KsqlDBClient(
        cloud_api_key=settings.confluent_cloud_api_key,
        cloud_api_secret=settings.confluent_cloud_api_secret,
        environment_id=env_id,
        ksqldb_api_key=settings.ksqldb_api_key,
        ksqldb_api_secret=settings.ksqldb_api_secret,
    )

    async def _run_ksqldb() -> tuple[list[LineageNode], list[LineageEdge]]:
        async with ksql_client:
            return await _safe_extract("ksqlDB", ksql_client.extract())

    phase2_tasks.append(_run_ksqldb())

    # Flink — needs organization_id.  Derive from cloud API if possible.
    flink_client = FlinkClient(
        cloud_api_key=settings.confluent_cloud_api_key,
        cloud_api_secret=settings.confluent_cloud_api_secret,
        environment_id=env_id,
        organization_id="",  # Will be populated below if available.
        flink_api_key=settings.flink_api_key,
        flink_api_secret=settings.flink_api_secret,
        flink_region=settings.flink_region,
        flink_cloud=settings.flink_cloud,
    )

    # Try to discover org ID from first Kafka cluster metadata.
    org_id = ""
    for cluster in kafka_clusters:
        org_id = cluster.get("spec", {}).get("organization", {}).get("id", "")
        if org_id:
            break
    flink_client.organization_id = org_id

    async def _run_flink() -> tuple[list[LineageNode], list[LineageEdge]]:
        async with flink_client:
            return await _safe_extract("Flink", flink_client.extract())

    if org_id:
        phase2_tasks.append(_run_flink())
    else:
        logger.debug("Organization ID not found — skipping Flink extraction")

    phase2_results = await asyncio.gather(*phase2_tasks)
    for nodes, edges in phase2_results:
        _merge_into(graph, nodes, edges)

    # ── Phase 3: SchemaRegistry, StreamCatalog (parallel enrichment) ──
    phase3_tasks = []

    if sr_endpoint:
        sr_client = SchemaRegistryClient(
            base_url=sr_endpoint,
            api_key=sr_key,
            api_secret=sr_secret,
            environment_id=env_id,
        )

        async def _run_sr() -> tuple[list[LineageNode], list[LineageEdge]]:
            async with sr_client:
                return await _safe_extract("SchemaRegistry", sr_client.extract())

        phase3_tasks.append(_run_sr())

        catalog_client = StreamCatalogClient(
            base_url=sr_endpoint,
            api_key=sr_key,
            api_secret=sr_secret,
            environment_id=env_id,
        )

        async def _run_catalog() -> tuple[list[LineageNode], list[LineageEdge]]:
            async with catalog_client:
                # Enrichment modifies the graph in-place.
                try:
                    await catalog_client.enrich(graph)
                except Exception:
                    logger.warning("StreamCatalog enrichment failed", exc_info=True)
                return [], []

        phase3_tasks.append(_run_catalog())
    else:
        logger.debug("No SR endpoint found — skipping SchemaRegistry and StreamCatalog")

    if phase3_tasks:
        phase3_results = await asyncio.gather(*phase3_tasks)
        for nodes, edges in phase3_results:
            _merge_into(graph, nodes, edges)

    # ── Phase 4: Tableflow (last) ─────────────────────────────────────
    tf_client = TableflowClient(
        api_key=settings.confluent_cloud_api_key,
        api_secret=settings.confluent_cloud_api_secret,
        environment_id=env_id,
    )
    async with tf_client:
        nodes, edges = await _safe_extract("Tableflow", tf_client.extract())
        _merge_into(graph, nodes, edges)

    logger.info(
        "Environment %s done: graph has %d nodes, %d edges",
        env_id,
        graph.node_count,
        graph.edge_count,
    )


# ── CLI entry point ─────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for lineage extraction."""
    settings = Settings()  # type: ignore[call-arg]

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        graph = asyncio.run(run_extraction(settings))
    except KeyboardInterrupt:
        logger.info("Extraction interrupted")
        sys.exit(1)
    except Exception:
        logger.exception("Extraction failed")
        sys.exit(2)

    print(f"Extraction complete: {graph.node_count} nodes, {graph.edge_count} edges")
    print(f"Output: {settings.extract_output_path}")
