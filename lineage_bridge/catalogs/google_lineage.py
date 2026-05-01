# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Google Data Lineage catalog provider.

Google Data Lineage natively speaks OpenLineage, making it a natural integration
point. This provider:
  - Creates CATALOG_TABLE nodes (catalog_type="GOOGLE_DATA_LINEAGE")
    from Tableflow catalog integrations
  - Enriches nodes via the Data Lineage REST API
  - Pushes lineage as OpenLineage events to Google Data Lineage API
  - Builds deep links to the Google Cloud console
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from lineage_bridge.models.graph import (
    EdgeType,
    LineageEdge,
    LineageGraph,
    LineageNode,
    NodeType,
    PushResult,
    SystemType,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0

# Google Data Lineage API base
_API_BASE = "https://datalineage.googleapis.com/v1"


class GoogleLineageProvider:
    """CatalogProvider implementation for Google Data Lineage.

    Uses the Google Data Lineage API (part of Dataplex) to read and write
    lineage information. Accepts OpenLineage events natively.
    """

    catalog_type: str = "GOOGLE_DATA_LINEAGE"

    def __init__(
        self,
        project_id: str | None = None,
        location: str = "us",
        credentials_json: str | None = None,
    ) -> None:
        self._project_id = project_id
        self._location = location
        self._credentials_json = credentials_json
        self._token: str | None = None

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a CATALOG_TABLE node + MATERIALIZES edge from the tableflow node."""
        google_cfg = ci_config.get("google_bigquery", ci_config)
        project_id = google_cfg.get("project_id", self._project_id or "unknown")
        dataset_id = google_cfg.get("dataset_id", cluster_id)
        table_name = topic_name.split(".")[-1].replace("-", "_")
        qualified = f"{project_id}.{dataset_id}.{table_name}"
        # Node ID retains "google_table" so existing IDs stay stable; the
        # runtime discriminator is `catalog_type`.
        google_id = f"google:google_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=google_id,
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name=qualified,
            display_name=qualified,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_name": table_name,
                "location": self._location,
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=google_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata from BigQuery and lineage from Data Lineage API."""
        if not self._project_id:
            logger.debug("Google enrichment skipped -- no project_id configured")
            return

        google_nodes = graph.filter_catalog_nodes("GOOGLE_DATA_LINEAGE")
        if not google_nodes:
            return

        headers = await self._get_auth_headers()
        if not headers:
            logger.debug("Google enrichment skipped -- no credentials available")
            return

        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            for node in google_nodes:
                if node.system != SystemType.GOOGLE:
                    continue
                await self._enrich_node(client, graph, node)

    async def _enrich_node(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch metadata for a single Google table."""
        project_id = node.attributes.get("project_id")
        dataset_id = node.attributes.get("dataset_id")
        table_name = node.attributes.get("table_name")
        if not all([project_id, dataset_id, table_name]):
            return

        # Try BigQuery Tables API for metadata
        bq_url = (
            f"https://bigquery.googleapis.com/bigquery/v2"
            f"/projects/{project_id}/datasets/{dataset_id}/tables/{table_name}"
        )

        for attempt in range(_MAX_RETRIES):
            try:
                resp = await client.get(bq_url)
                if resp.status_code == 200:
                    data = resp.json()
                    schema_fields = []
                    for field in data.get("schema", {}).get("fields", []):
                        schema_fields.append(
                            {
                                "name": field.get("name"),
                                "type": field.get("type"),
                                "description": field.get("description"),
                            }
                        )

                    enriched_attrs = {
                        **node.attributes,
                        "table_type": data.get("type"),
                        "columns": schema_fields,
                        "num_rows": data.get("numRows"),
                        "num_bytes": data.get("numBytes"),
                        "creation_time": data.get("creationTime"),
                        "last_modified_time": data.get("lastModifiedTime"),
                        "description": data.get("description"),
                        "labels": data.get("labels", {}),
                    }
                    enriched = node.model_copy(update={"attributes": enriched_attrs})
                    graph.add_node(enriched)
                    return
                if resp.status_code in (401, 403, 404):
                    logger.warning(
                        "BigQuery API returned %d for %s -- skipping",
                        resp.status_code,
                        node.qualified_name,
                    )
                    return
                if resp.status_code in (429, 500, 502, 503, 504):
                    delay = _BACKOFF_BASE * (2**attempt)
                    logger.debug(
                        "BigQuery API %d for %s -- retrying in %.1fs",
                        resp.status_code,
                        node.qualified_name,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.warning(
                    "BigQuery API unexpected %d for %s",
                    resp.status_code,
                    node.qualified_name,
                )
                return
            except httpx.HTTPError:
                logger.debug(
                    "HTTP error enriching %s (attempt %d)",
                    node.qualified_name,
                    attempt + 1,
                )
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(_BACKOFF_BASE * (2**attempt))

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Push lineage as OpenLineage events to Google Data Lineage API.

        Google Data Lineage natively accepts OpenLineage-format events via
        its ProcessOpenLineageRunEvent endpoint.
        """
        if not self._project_id:
            return PushResult(errors=["No project_id configured"])

        headers, auth_error = await self._get_auth_headers_with_reason()
        if not headers:
            return PushResult(errors=[auth_error or "No credentials available"])

        result = PushResult()

        google_nodes = [
            n
            for n in graph.filter_catalog_nodes("GOOGLE_DATA_LINEAGE")
            if n.system == SystemType.GOOGLE
        ]
        if not google_nodes:
            return result

        if on_progress:
            on_progress("Push", f"Found {len(google_nodes)} Google tables to update")

        # Register Kafka topics in Dataplex Catalog so the BQ Lineage tab can
        # show schema for the upstream Confluent nodes — the OpenLineage
        # processor itself drops every facet at storage time, so the Catalog
        # entry is the only path to surface column metadata. Failures are
        # non-fatal: missing entries just mean the schema panel stays empty.
        try:
            from lineage_bridge.catalogs.google_dataplex import DataplexAssetRegistrar

            registrar = DataplexAssetRegistrar(self._project_id, self._location, headers)
            registered, registrar_errors = await registrar.register_kafka_assets(
                graph, on_progress=on_progress
            )
            if registered and on_progress:
                on_progress("Catalog", f"Registered {registered} Kafka entries in Dataplex")
            for err in registrar_errors:
                result.errors.append(err)
        except Exception as exc:
            logger.warning("Dataplex registration skipped: %s", exc, exc_info=True)
            result.errors.append(f"Dataplex registration skipped: {exc}")

        # Convert the graph to OpenLineage events. Push the entire upstream
        # chain (source connectors → topics → Flink/ksqlDB → topics → sink →
        # BigQuery) so Google can walk transitively from the BQ table back to
        # the source topics. Each event's namespaces are rewritten to formats
        # Google's processor recognizes; events that end up with no recognized
        # datasets after normalization are dropped.
        from lineage_bridge.openlineage.translator import graph_to_events

        events = graph_to_events(graph)
        for event in events:
            self._normalize_event_for_google(event)
        events = [e for e in events if e.inputs or e.outputs]
        now = datetime.now(UTC).isoformat()

        parent = f"projects/{self._project_id}/locations/{self._location}"
        url = f"{_API_BASE}/{parent}:processOpenLineageRunEvent"

        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            for event in events:
                event_dict = event.model_dump(mode="json", exclude_none=True, by_alias=True)
                for attempt in range(_MAX_RETRIES):
                    try:
                        resp = await client.post(url, json=event_dict)
                        if resp.status_code in (200, 201):
                            result.tables_updated += 1
                            break
                        if resp.status_code in (429, 500, 502, 503, 504):
                            delay = _BACKOFF_BASE * (2**attempt)
                            await asyncio.sleep(delay)
                            continue
                        error_msg = resp.text[:200]
                        result.errors.append(
                            f"Push failed for event {event.run.runId}: "
                            f"{resp.status_code} {error_msg}"
                        )
                        break
                    except httpx.HTTPError as exc:
                        if attempt < _MAX_RETRIES - 1:
                            await asyncio.sleep(_BACKOFF_BASE * (2**attempt))
                        else:
                            result.errors.append(f"Push error for event {event.run.runId}: {exc}")

        if on_progress:
            on_progress(
                "Push",
                f"Done -- {result.tables_updated} events pushed, last synced: {now}",
            )

        return result

    async def _get_auth_headers(self) -> dict[str, str] | None:
        """Get authentication headers for Google APIs (returns None on any failure)."""
        headers, _ = await self._get_auth_headers_with_reason()
        return headers

    async def _get_auth_headers_with_reason(self) -> tuple[dict[str, str] | None, str | None]:
        """Get auth headers, returning ``(headers, error_reason)`` for diagnostics.

        Tries: (1) cached token, (2) Application Default Credentials via
        ``google-auth``. ``error_reason`` is populated on failure so callers
        can surface it (e.g. ``google-auth not installed``, ``no ADC configured``).
        """
        if self._token:
            return {"Authorization": f"Bearer {self._token}"}, None

        try:
            import google.auth
            import google.auth.transport.requests
        except ImportError as exc:
            reason = f"google-auth not installed ({exc}); add it to dependencies"
            logger.warning(reason)
            return None, reason

        try:
            creds, _project = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            request = google.auth.transport.requests.Request()
            await asyncio.to_thread(creds.refresh, request)
            self._token = creds.token
            return {"Authorization": f"Bearer {self._token}"}, None
        except Exception as exc:
            reason = (
                f"Google ADC unavailable: {exc}. "
                "Run `gcloud auth application-default login` or set "
                "GOOGLE_APPLICATION_CREDENTIALS to a service-account key."
            )
            logger.warning(reason)
            return None, reason

    # ── OpenLineage event normalization for Google Data Lineage ─────────

    @staticmethod
    def _normalize_event_for_google(event: Any) -> None:
        """Rewrite dataset namespaces in-place so Google's processor accepts them.

        Delegates to the shared normalizer with Google's allowlist (BigQuery
        on output; Kafka on both sides). UC/Glue/EXTERNAL datasets are
        dropped — Google can't link them.
        """
        from lineage_bridge.openlineage.normalize import normalize_event

        normalize_event(event, allow={"bigquery"})

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the table in the Google Cloud console."""
        project_id = node.attributes.get("project_id")
        dataset_id = node.attributes.get("dataset_id")
        table_name = node.attributes.get("table_name")
        if not all([project_id, dataset_id, table_name]):
            return None
        return (
            f"https://console.cloud.google.com/bigquery"
            f"?project={project_id}"
            f"&p={project_id}&d={dataset_id}&t={table_name}&page=table"
        )
