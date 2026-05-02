# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Orchestrator — composes phases into the extraction pipeline.

Three entry points:
  - run_extraction()  — extract + optionally enrich (full pipeline)
  - run_enrichment()  — enrich an existing graph (catalog + metrics)
  - main()            — CLI with --no-enrich / --enrich-only flags

Per-environment phase ordering (driven by `PhaseRunner`):
  1. KafkaAdmin           — establish the topic inventory.
  2. Processing           — Connect, ksqlDB, Flink (parallel).
  3. SchemaEnrichment     — SchemaRegistry, StreamCatalog (parallel).
  4. Tableflow            — bridge to UC/Glue (depends on topic nodes).

Post-extraction (in `run_enrichment`):
  4b. run_catalog_enrichment — providers enrich their own nodes.
  5.  run_metrics_enrichment — live throughput data (optional).

Push moved to `services.push_service.run_push` in Phase 1B; the per-provider
`run_*_push` wrappers that used to live here are gone.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.config.settings import Settings
from lineage_bridge.extractors.context import ExtractionContext, ProgressCallback
from lineage_bridge.extractors.phase import PhaseRunner, merge_into, safe_extract
from lineage_bridge.extractors.phases import (
    KafkaAdminPhase,
    ProcessingPhase,
    SchemaEnrichmentPhase,
    TableflowPhase,
    run_catalog_enrichment,
    run_metrics_enrichment,
)
from lineage_bridge.models.graph import LineageGraph

logger = logging.getLogger(__name__)


__all__ = [
    "ProgressCallback",
    "main",
    "merge_into",
    "run_enrichment",
    "run_extraction",
    "safe_extract",
]


def _make_progress(on_progress: ProgressCallback = None):
    """Wrap a progress callback so it always logs in addition to forwarding."""

    def _progress(phase: str, detail: str = "") -> None:
        logger.info("%s %s", phase, detail)
        if on_progress:
            on_progress(phase, detail)

    return _progress


# ── enrichment entry point ──────────────────────────────────────────────


async def run_enrichment(
    settings: Settings,
    graph: LineageGraph,
    *,
    enable_catalog: bool = True,
    enable_metrics: bool = False,
    metrics_lookback_hours: int = 1,
    on_progress: ProgressCallback = None,
) -> LineageGraph:
    """Enrich an existing graph with catalog metadata and/or metrics.

    Mutates `graph` in-place and returns it. Catalog enrichment runs Phase 4b
    (per-provider `enrich()`); metrics enrichment runs Phase 5.
    """
    _progress = _make_progress(on_progress)

    if enable_catalog:
        await run_catalog_enrichment(settings, graph, on_progress=_progress)

    if enable_metrics:
        await run_metrics_enrichment(
            settings,
            graph,
            lookback_hours=metrics_lookback_hours,
            on_progress=_progress,
        )

    _progress("Enrichment done", f"{graph.node_count} nodes, {graph.edge_count} edges")
    return graph


# Push wrappers were removed in Phase 1B. Use `services.run_push(req, settings,
# graph, on_progress)` with a `PushRequest` instead — it dispatches via the
# catalog registry to provider.push_lineage().


# ── extraction entry point ─────────────────────────────────────────────


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
    enable_enrichment: bool = True,
    enable_metrics: bool = False,
    metrics_lookback_hours: int = 1,
    sr_endpoints: dict[str, str] | None = None,
    sr_credentials: dict[str, dict[str, str]] | None = None,
    flink_credentials: dict[str, dict[str, str]] | None = None,
    on_progress: ProgressCallback = None,
) -> LineageGraph:
    """Run the extraction pipeline and return the merged graph."""
    from lineage_bridge.catalogs import configure_providers

    # Reseed the catalog-provider singletons with this extraction's settings
    # before any phase runs. The registry lives at module scope so deeplinks
    # generated during render see the right workspace URL — without this
    # call, multiple users in the same UI process would step on each other's
    # configured Databricks workspace.
    configure_providers(
        databricks_workspace_url=settings.databricks_workspace_url,
        databricks_token=settings.databricks_token,
    )

    graph = LineageGraph()

    cloud = ConfluentClient(
        "https://api.confluent.cloud",
        settings.confluent_cloud_api_key,
        settings.confluent_cloud_api_secret,
    )

    _progress = _make_progress(on_progress)

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
                sr_endpoints=sr_endpoints,
                sr_credentials=sr_credentials,
                flink_credentials=flink_credentials,
                on_progress=_progress,
            )
    finally:
        await cloud.close()

    if enable_enrichment:
        await run_enrichment(
            settings,
            graph,
            enable_metrics=enable_metrics,
            metrics_lookback_hours=metrics_lookback_hours,
            on_progress=on_progress,
        )

    warnings = graph.validate()
    for w in warnings:
        logger.warning("Graph validation: %s", w)
    if warnings:
        _progress("Validation", f"{len(warnings)} warning(s) — check logs")

    from collections import Counter

    system_counts = Counter(n.system.value for n in graph.nodes)
    breakdown = ", ".join(f"{system}: {count}" for system, count in sorted(system_counts.items()))
    _progress("Done", f"{graph.node_count} nodes, {graph.edge_count} edges ({breakdown})")
    return graph


async def _discover_environment(
    settings: Settings,
    cloud: ConfluentClient,
    env_id: str,
    *,
    cluster_ids: list[str] | None,
    enable_schema_registry: bool,
    enable_stream_catalog: bool,
    sr_endpoints: dict[str, str] | None,
    sr_credentials: dict[str, dict[str, str]] | None,
    on_progress: Any,
) -> tuple[list[dict[str, Any]], str | None, str | None, str | None]:
    """Discover clusters and Schema Registry endpoint + credentials for one env.

    Returns (clusters, sr_endpoint, sr_key, sr_secret). `clusters` is empty if
    none match the filter; caller should bail in that case.
    """
    on_progress("Discovering", f"clusters in {env_id}")

    all_clusters = await cloud.paginate("/cmk/v2/clusters", params={"environment": env_id})
    if cluster_ids:
        all_clusters = [c for c in all_clusters if c.get("id") in cluster_ids]
    if not all_clusters:
        on_progress("Warning", f"No Kafka clusters found in {env_id}")
        return [], None, None, None

    sr_endpoint: str | None = sr_endpoints.get(env_id) if sr_endpoints else None
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

    if sr_credentials and env_id in sr_credentials:
        sr_key = sr_credentials[env_id]["api_key"]
        sr_secret = sr_credentials[env_id]["api_secret"]
    else:
        sr_key = settings.schema_registry_api_key or settings.confluent_cloud_api_key
        sr_secret = settings.schema_registry_api_secret or settings.confluent_cloud_api_secret

    return all_clusters, sr_endpoint, sr_key, sr_secret


async def _stamp_environment_names(
    cloud: ConfluentClient,
    env_id: str,
    clusters: list[dict[str, Any]],
    graph: LineageGraph,
) -> None:
    """Backfill `environment_name` and `cluster_name` on graph nodes for this env."""
    env_name: str | None = None
    try:
        env_data = await cloud.get(f"/org/v2/environments/{env_id}")
        env_name = env_data.get("display_name")
    except Exception:
        logger.debug("Could not fetch environment name for %s", env_id)

    cluster_names: dict[str, str] = {}
    for c in clusters:
        cid = c.get("id", "")
        cname = c.get("spec", {}).get("display_name", "")
        if cid and cname:
            cluster_names[cid] = cname

    for node in graph.nodes:
        if node.environment_id == env_id and not node.environment_name:
            node.environment_name = env_name
        if node.cluster_id and not node.cluster_name:
            node.cluster_name = cluster_names.get(node.cluster_id)


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
    sr_endpoints: dict[str, str] | None,
    sr_credentials: dict[str, dict[str, str]] | None,
    flink_credentials: dict[str, dict[str, str]] | None,
    on_progress: Any,
) -> None:
    """Run all phases for a single Confluent Cloud environment."""
    clusters, sr_endpoint, sr_key, sr_secret = await _discover_environment(
        settings,
        cloud,
        env_id,
        cluster_ids=cluster_ids,
        enable_schema_registry=enable_schema_registry,
        enable_stream_catalog=enable_stream_catalog,
        sr_endpoints=sr_endpoints,
        sr_credentials=sr_credentials,
        on_progress=on_progress,
    )
    if not clusters:
        return

    ctx = ExtractionContext(
        settings=settings,
        cloud=cloud,
        env_id=env_id,
        graph=graph,
        clusters=clusters,
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

    runner = PhaseRunner(
        [
            KafkaAdminPhase(),
            ProcessingPhase(),
            SchemaEnrichmentPhase(),
            TableflowPhase(),
        ]
    )
    await runner.run(ctx)

    await _stamp_environment_names(cloud, env_id, clusters, graph)

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
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Extract only, skip catalog and metrics enrichment",
    )
    parser.add_argument(
        "--enrich-only",
        action="store_true",
        help="Enrich an existing graph file (reads from --output path)",
    )
    parser.add_argument(
        "--push-lineage",
        action="store_true",
        help="Push lineage metadata to Databricks UC tables after extraction",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Enable Telemetry-based metrics enrichment (Topics/Connectors/Flink) "
        "and graph-derived metrics (Consumer Groups/Tableflow/Catalog Tables/ksqlDB)",
    )
    parser.add_argument(
        "--metrics-lookback-hours",
        type=int,
        default=1,
        help="Lookback window for metrics queries (default: 1)",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Per-env / per-cluster credentials cached by the demo provision scripts.
    # Merge them into settings + the run_extraction kwargs so a CLI run against
    # any provisioned env "just works" without re-exporting the right SR /
    # Flink / Kafka keys for every demo switch.
    from lineage_bridge.config.cache import load_cache
    from lineage_bridge.config.settings import ClusterCredential

    cache = load_cache()
    sr_creds = cache.get("sr_credentials") or {}
    flink_creds = cache.get("flink_credentials") or {}
    cached_cluster_creds = cache.get("cluster_credentials") or {}
    sr_endpoints = {env: c["endpoint"] for env, c in sr_creds.items() if c.get("endpoint")}

    if cached_cluster_creds:
        # Settings.cluster_credentials wins (env-var override); cache fills gaps.
        merged = dict(cached_cluster_creds)
        for cid, cred in settings.cluster_credentials.items():
            merged[cid] = {"api_key": cred.api_key, "api_secret": cred.api_secret}
        settings = settings.model_copy(
            update={
                "cluster_credentials": {
                    cid: ClusterCredential(**c) for cid, c in merged.items()
                }
            }
        )

    try:
        if args.enrich_only:
            graph = LineageGraph.from_json_file(args.output)
            print(f"Loaded graph: {graph.node_count} nodes, {graph.edge_count} edges")
            graph = asyncio.run(run_enrichment(settings, graph))
        else:
            graph = asyncio.run(
                run_extraction(
                    settings,
                    environment_ids=args.envs,
                    cluster_ids=args.clusters,
                    enable_enrichment=not args.no_enrich,
                    enable_metrics=args.metrics,
                    metrics_lookback_hours=args.metrics_lookback_hours,
                    sr_endpoints=sr_endpoints,
                    sr_credentials=sr_creds,
                    flink_credentials=flink_creds,
                )
            )

        if args.push_lineage:
            from lineage_bridge.services import PushRequest, run_push

            result = asyncio.run(run_push(PushRequest(provider="databricks_uc"), settings, graph))
            print(
                f"Push: {result.tables_updated} tables, "
                f"{result.properties_set} properties, {result.comments_set} comments"
            )
            if result.errors:
                for err in result.errors:
                    print(f"  Error: {err}")
    except KeyboardInterrupt:
        logger.info("Extraction interrupted")
        sys.exit(1)
    except Exception:
        logger.exception("Extraction failed")
        sys.exit(2)

    graph.to_json_file(args.output)
    print(f"Complete: {graph.node_count} nodes, {graph.edge_count} edges")
    print(f"Output: {args.output}")
