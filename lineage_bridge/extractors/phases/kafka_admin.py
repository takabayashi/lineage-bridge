# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Phase 1 — KafkaAdmin: topic inventory and consumer groups, per cluster.

Sequential per cluster (each cluster gets its own KafkaAdminClient with
cluster-scoped credentials). Bootstrap servers come from the cluster spec
with the SASL_SSL:// prefix stripped — that's a transport hint that breaks
the Kafka protocol's bootstrap parser.
"""

from __future__ import annotations

import logging

from lineage_bridge.clients.kafka_admin import KafkaAdminClient
from lineage_bridge.extractors.context import ExtractionContext
from lineage_bridge.extractors.phase import PhaseResult, merge_into, safe_extract

logger = logging.getLogger(__name__)


class KafkaAdminPhase:
    """Phase 1 — extract topics and consumer groups for each Kafka cluster."""

    name = "Phase 1/4 — KafkaAdmin"

    async def execute(self, ctx: ExtractionContext) -> PhaseResult:
        ctx.progress("Phase 1/4", "Extracting Kafka topics & consumer groups")

        for cluster in ctx.clusters:
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
                ctx.progress("Warning", f"No REST endpoint for cluster {cluster_id}")
                continue

            kafka_key, kafka_secret = ctx.settings.get_cluster_credentials(cluster_id)
            bootstrap = spec.get("kafka_bootstrap_endpoint", "")
            if bootstrap and bootstrap.startswith("SASL_SSL://"):
                bootstrap = bootstrap[len("SASL_SSL://") :]
            logger.debug(
                "KafkaAdminClient bootstrap_servers=%s for cluster %s",
                bootstrap or None,
                cluster_id,
            )
            kafka_client = KafkaAdminClient(
                base_url=rest_endpoint,
                api_key=kafka_key,
                api_secret=kafka_secret,
                cluster_id=cluster_id,
                environment_id=ctx.env_id,
                bootstrap_servers=bootstrap or None,
            )
            async with kafka_client:
                nodes, edges = await safe_extract(
                    f"KafkaAdmin:{cluster_id}", kafka_client.extract(), ctx.on_progress
                )
                # Merge per-cluster so later clusters can see earlier nodes if needed
                merge_into(ctx.graph, nodes, edges)

        return PhaseResult()
