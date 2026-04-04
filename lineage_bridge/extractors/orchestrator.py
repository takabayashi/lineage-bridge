"""Orchestrator that runs all extractors and builds the unified lineage graph.

Execution order:
  1. KafkaAdmin — establish the topic inventory.
  2. Connect, ksqlDB, Flink — transformation / processing edges (parallel).
  3. SchemaRegistry, StreamCatalog — enrichment (parallel).
  4. Tableflow — bridge to UC/Glue (last, depends on topic nodes).
  5. Merge everything into a single LineageGraph.
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
from lineage_bridge.clients.metrics import MetricsClient
from lineage_bridge.clients.schema_registry import SchemaRegistryClient
from lineage_bridge.clients.stream_catalog import StreamCatalogClient
from lineage_bridge.clients.tableflow import TableflowClient
from lineage_bridge.config.settings import Settings
from lineage_bridge.models.graph import LineageEdge, LineageGraph, LineageNode

logger = logging.getLogger(__name__)


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


async def _safe_extract(
    label: str, coro: Any, on_progress: Any = None
) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Run an extractor coroutine, returning empty on failure."""
    try:
        return await coro
    except Exception as exc:
        # Surface auth errors clearly
        msg = str(exc)
        if "401" in msg or "Unauthorized" in msg:
            detail = (
                f"Extractor '{label}' got 401 Unauthorized. "
                "This likely means a cluster-scoped API key is needed. "
                "Set LINEAGE_BRIDGE_KAFKA_API_KEY in .env."
            )
        elif "403" in msg or "Forbidden" in msg:
            detail = (
                f"Extractor '{label}' got 403 Forbidden. The API key lacks required permissions."
            )
        elif "400" in msg or "Bad Request" in msg:
            detail = (
                f"Extractor '{label}' got 400 Bad Request. "
                "The API key may not have access to this environment, "
                "or the API parameters are invalid."
            )
        else:
            detail = f"Extractor '{label}' failed: {exc}"

        logger.warning(detail, exc_info=True)
        if on_progress:
            on_progress("Warning", detail)
        return [], []


# ── progress callback type ──────────────────────────────────────────────

# Callable that receives (phase_label, detail_message)
ProgressCallback = Any  # typing: Callable[[str, str], None] | None


# ── main orchestration ──────────────────────────────────────────────────


async def run_extraction(
    settings: Settings,
    *,
    environment_ids: list[str],
    cluster_ids: list[str] | None = None,
    enable_connect: bool = True,
    enable_ksqldb: bool = True,
    enable_flink: bool = True,
    enable_schema_registry: bool = True,
    enable_stream_catalog: bool = True,
    enable_tableflow: bool = True,
    enable_metrics: bool = False,
    metrics_lookback_hours: int = 1,
    sr_endpoints: dict[str, str] | None = None,
    sr_credentials: dict[str, dict[str, str]] | None = None,
    flink_credentials: dict[str, dict[str, str]] | None = None,
    on_progress: ProgressCallback = None,
) -> LineageGraph:
    """Run the extraction pipeline and return the merged graph.

    Args:
        settings: Credentials (from .env).
        environment_ids: Which environments to scan.
        cluster_ids: Optional filter — specific cluster IDs. If None, scan all.
        enable_*: Toggle individual extractors on/off.
        on_progress: Optional callback for UI progress updates.
    """
    graph = LineageGraph()

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    def _progress(phase: str, detail: str = "") -> None:
        logger.info("%s %s", phase, detail)
        if on_progress:
            on_progress(phase, detail)

    try:
        for env_id in environment_ids:
            await _extract_environment(
                settings,
                cloud,
                env_id,
                graph,
                cluster_ids=cluster_ids,
                enable_connect=enable_connect,
                enable_ksqldb=enable_ksqldb,
                enable_flink=enable_flink,
                enable_schema_registry=enable_schema_registry,
                enable_stream_catalog=enable_stream_catalog,
                enable_tableflow=enable_tableflow,
                enable_metrics=enable_metrics,
                metrics_lookback_hours=metrics_lookback_hours,
                sr_endpoints=sr_endpoints,
                sr_credentials=sr_credentials,
                flink_credentials=flink_credentials,
                on_progress=_progress,
            )
    finally:
        await cloud.close()

    _progress("Done", f"{graph.node_count} nodes, {graph.edge_count} edges")
    return graph


async def _extract_environment(
    settings: Settings,
    cloud: ConfluentClient,
    env_id: str,
    graph: LineageGraph,
    *,
    cluster_ids: list[str] | None,
    enable_connect: bool,
    enable_ksqldb: bool,
    enable_flink: bool,
    enable_schema_registry: bool,
    enable_stream_catalog: bool,
    enable_tableflow: bool,
    enable_metrics: bool,
    metrics_lookback_hours: int,
    sr_endpoints: dict[str, str] | None,
    sr_credentials: dict[str, dict[str, str]] | None,
    flink_credentials: dict[str, dict[str, str]] | None,
    on_progress: Any,
) -> None:
    """Run all extractors for a single Confluent Cloud environment."""
    on_progress("Discovering", f"clusters in {env_id}")

    # ── discover Kafka clusters ────────────────────────────────────────
    all_clusters = await cloud.paginate("/cmk/v2/clusters", params={"environment": env_id})
    if cluster_ids:
        all_clusters = [c for c in all_clusters if c.get("id") in cluster_ids]
    if not all_clusters:
        on_progress("Warning", f"No Kafka clusters found in {env_id}")
        return

    # ── discover Schema Registry ───────────────────────────────────────
    sr_endpoint: str | None = sr_endpoints.get(env_id) if sr_endpoints else None
    # Fall back to global setting
    if not sr_endpoint and settings.schema_registry_endpoint:
        sr_endpoint = settings.schema_registry_endpoint
    if not sr_endpoint and (enable_schema_registry or enable_stream_catalog):
        try:
            sr_items = await cloud.paginate("/srcm/v2/clusters", params={"environment": env_id})
            if sr_items:
                sr_cluster = sr_items[0]
                sr_endpoint = sr_cluster.get("spec", {}).get("http_endpoint") or sr_cluster.get(
                    "status", {}
                ).get("http_endpoint")
        except Exception:
            logger.debug("SR management API failed for %s", env_id, exc_info=True)

        if sr_endpoint:
            on_progress("Discovery", f"Schema Registry found: {sr_endpoint}")
        else:
            on_progress(
                "Warning",
                f"No Schema Registry endpoint found for {env_id}. "
                "Set LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY in .env "
                "or check Stream Governance is enabled.",
            )
    elif sr_endpoint:
        on_progress("Discovery", f"Schema Registry (cached): {sr_endpoint}")

    # Credential resolution — per-env SR keys take priority
    if sr_credentials and env_id in sr_credentials:
        sr_key = sr_credentials[env_id]["api_key"]
        sr_secret = sr_credentials[env_id]["api_secret"]
    else:
        sr_key = settings.schema_registry_api_key or settings.confluent_cloud_api_key
        sr_secret = settings.schema_registry_api_secret or settings.confluent_cloud_api_secret

    # ── Phase 1: Kafka Admin (per cluster) ─────────────────────────────
    on_progress("Phase 1/4", "Extracting Kafka topics & consumer groups")
    for cluster in all_clusters:
        cluster_id = cluster.get("id", "")
        spec = cluster.get("spec", {})
        rest_endpoint = spec.get("http_endpoint", "")
        if not rest_endpoint:
            region = spec.get("region", "")
            cloud_provider = spec.get("cloud", "").lower()
            if region and cloud_provider:
                rest_endpoint = (
                    f"https://{cluster_id}.{region}.{cloud_provider}.confluent.cloud:443"
                )
        if not rest_endpoint:
            on_progress("Warning", f"No REST endpoint for cluster {cluster_id}")
            continue

        kafka_key, kafka_secret = settings.get_cluster_credentials(cluster_id)
        kafka_client = KafkaAdminClient(
            base_url=rest_endpoint,
            api_key=kafka_key,
            api_secret=kafka_secret,
            cluster_id=cluster_id,
            environment_id=env_id,
        )
        async with kafka_client:
            nodes, edges = await _safe_extract(
                f"KafkaAdmin:{cluster_id}", kafka_client.extract(), on_progress
            )
            _merge_into(graph, nodes, edges)

    # ── Phase 2: Connect, ksqlDB, Flink (parallel) ────────────────────
    on_progress("Phase 2/4", "Extracting connectors, ksqlDB, Flink")
    phase2_tasks: list[Any] = []

    if enable_connect:
        for cluster in all_clusters:
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
                    return await _safe_extract(f"Connect:{cid}", c.extract(), on_progress)

            phase2_tasks.append(_run_connect())

    if enable_ksqldb:
        ksql_client = KsqlDBClient(
            cloud_api_key=settings.confluent_cloud_api_key,
            cloud_api_secret=settings.confluent_cloud_api_secret,
            environment_id=env_id,
            ksqldb_api_key=settings.ksqldb_api_key,
            ksqldb_api_secret=settings.ksqldb_api_secret,
        )

        async def _run_ksqldb() -> tuple[list[LineageNode], list[LineageEdge]]:
            async with ksql_client:
                return await _safe_extract("ksqlDB", ksql_client.extract(), on_progress)

        phase2_tasks.append(_run_ksqldb())

    if enable_flink:
        # Try multiple locations for organization ID
        org_id = ""
        for cluster in all_clusters:
            # Try spec.organization.id (some API versions)
            org_id = cluster.get("spec", {}).get("organization", {}).get("id", "")
            if org_id:
                break
            # Try top-level organization.id
            org_id = cluster.get("organization", {}).get("id", "")
            if org_id:
                break
            # Try metadata.resource_name (contains org ID)
            resource_name = cluster.get("metadata", {}).get("resource_name", "")
            if "/organization=" in resource_name:
                org_id = resource_name.split("/organization=")[1].split("/")[0]
                if org_id:
                    break

        if not org_id:
            # Last resort: fetch from /org/v2/environments
            try:
                env_data = await cloud.get(f"/org/v2/environments/{env_id}")
                org_id = env_data.get("metadata", {}).get("resource_name", "")
                if "/organization=" in org_id:
                    org_id = org_id.split("/organization=")[1].split("/")[0]
                else:
                    org_id = ""
            except Exception:
                logger.debug("Failed to fetch org ID from environment %s", env_id, exc_info=True)

        if org_id:
            # Resolve per-env Flink credentials
            if flink_credentials and env_id in flink_credentials:
                flink_key = flink_credentials[env_id]["api_key"]
                flink_secret = flink_credentials[env_id]["api_secret"]
            else:
                flink_key = settings.flink_api_key
                flink_secret = settings.flink_api_secret

            flink_client = FlinkClient(
                cloud_api_key=settings.confluent_cloud_api_key,
                cloud_api_secret=settings.confluent_cloud_api_secret,
                environment_id=env_id,
                organization_id=org_id,
                flink_api_key=flink_key,
                flink_api_secret=flink_secret,
            )

            async def _run_flink() -> tuple[list[LineageNode], list[LineageEdge]]:
                async with flink_client:
                    return await _safe_extract("Flink", flink_client.extract(), on_progress)

            phase2_tasks.append(_run_flink())
        else:
            on_progress(
                "Warning",
                f"Could not determine organization ID for Flink in {env_id} — Flink extraction skipped",
            )

    if phase2_tasks:
        phase2_results = await asyncio.gather(*phase2_tasks)
        for nodes, edges in phase2_results:
            _merge_into(graph, nodes, edges)

    # ── Phase 3: SchemaRegistry, StreamCatalog (parallel enrichment) ──
    on_progress("Phase 3/4", "Enriching with schemas & catalog metadata")
    phase3_tasks: list[Any] = []

    if enable_schema_registry:
        if sr_endpoint:
            sr_client = SchemaRegistryClient(
                base_url=sr_endpoint,
                api_key=sr_key,
                api_secret=sr_secret,
                environment_id=env_id,
            )

            async def _run_sr() -> tuple[list[LineageNode], list[LineageEdge]]:
                async with sr_client:
                    return await _safe_extract("SchemaRegistry", sr_client.extract(), on_progress)

            phase3_tasks.append(_run_sr())
        else:
            on_progress("Skipped", "Schema Registry — no endpoint discovered")

    if enable_stream_catalog and not sr_endpoint:
        on_progress("Skipped", "Stream Catalog — no SR endpoint discovered")
    elif enable_stream_catalog and sr_endpoint:
        catalog_client = StreamCatalogClient(
            base_url=sr_endpoint,
            api_key=sr_key,
            api_secret=sr_secret,
            environment_id=env_id,
        )

        async def _run_catalog() -> tuple[list[LineageNode], list[LineageEdge]]:
            async with catalog_client:
                try:
                    await catalog_client.enrich(graph)
                except Exception:
                    logger.warning("StreamCatalog enrichment failed", exc_info=True)
                return [], []

        phase3_tasks.append(_run_catalog())

    if phase3_tasks:
        phase3_results = await asyncio.gather(*phase3_tasks)
        for nodes, edges in phase3_results:
            _merge_into(graph, nodes, edges)

    # ── Phase 4: Tableflow (last) ─────────────────────────────────────
    if enable_tableflow:
        on_progress("Phase 4/4", "Extracting Tableflow & catalog integrations")
        tf_key = settings.tableflow_api_key or settings.confluent_cloud_api_key
        tf_secret = settings.tableflow_api_secret or settings.confluent_cloud_api_secret
        tf_cluster_ids = [c.get("id", "") for c in all_clusters if c.get("id")]
        tf_client = TableflowClient(
            api_key=tf_key,
            api_secret=tf_secret,
            environment_id=env_id,
            cluster_ids=tf_cluster_ids,
        )
        async with tf_client:
            nodes, edges = await _safe_extract("Tableflow", tf_client.extract(), on_progress)
            _merge_into(graph, nodes, edges)

    # ── Phase 5 (optional): Metrics enrichment ──────────────────────────
    if enable_metrics:
        on_progress("Metrics", "Enriching nodes with real-time metrics")
        metrics_client = MetricsClient(
            api_key=settings.confluent_cloud_api_key,
            api_secret=settings.confluent_cloud_api_secret,
            lookback_hours=metrics_lookback_hours,
        )
        async with metrics_client:
            total_enriched = 0
            for cluster in all_clusters:
                cluster_id = cluster.get("id", "")
                try:
                    enriched = await metrics_client.enrich(graph, cluster_id)
                    total_enriched += enriched
                except Exception as exc:
                    logger.warning(
                        "Metrics enrichment failed for %s: %s",
                        cluster_id,
                        exc,
                    )
            on_progress("Metrics", f"Enriched {total_enriched} nodes with metrics")

    on_progress(
        "Environment done",
        f"{env_id}: {graph.node_count} nodes, {graph.edge_count} edges",
    )


# ── CLI entry point ─────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for lineage extraction.

    Usage: lineage-bridge-extract --env env-abc123 [--env env-def456]
           [--cluster lkc-xxx] [--output graph.json]
    """
    import argparse

    parser = argparse.ArgumentParser(description="Extract Confluent Cloud lineage")
    parser.add_argument(
        "--env",
        dest="envs",
        action="append",
        required=True,
        help="Environment ID to scan (repeatable)",
    )
    parser.add_argument(
        "--cluster",
        dest="clusters",
        action="append",
        default=None,
        help="Cluster ID filter (repeatable, optional)",
    )
    parser.add_argument(
        "--output",
        default="./lineage_graph.json",
        help="Output JSON path (default: ./lineage_graph.json)",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        graph = asyncio.run(
            run_extraction(
                settings,
                environment_ids=args.envs,
                cluster_ids=args.clusters,
            )
        )
    except KeyboardInterrupt:
        logger.info("Extraction interrupted")
        sys.exit(1)
    except Exception:
        logger.exception("Extraction failed")
        sys.exit(2)

    graph.to_json_file(args.output)
    print(f"Extraction complete: {graph.node_count} nodes, {graph.edge_count} edges")
    print(f"Output: {args.output}")
