# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Google Data Lineage catalog provider.

Google Data Lineage natively speaks OpenLineage, making it a natural integration
point. This provider:
  - Creates GOOGLE_TABLE nodes from Tableflow catalog integrations
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
    node_type: NodeType = NodeType.GOOGLE_TABLE
    system_type: SystemType = SystemType.GOOGLE

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
        """Create a GOOGLE_TABLE node and MATERIALIZES edge from the tableflow node."""
        google_cfg = ci_config.get("google_bigquery", ci_config)
        project_id = google_cfg.get("project_id", self._project_id or "unknown")
        dataset_id = google_cfg.get("dataset_id", cluster_id)
        table_name = topic_name.replace(".", "_").replace("-", "_")
        qualified = f"{project_id}.{dataset_id}.{table_name}"
        google_id = f"google:google_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=google_id,
            system=SystemType.GOOGLE,
            node_type=NodeType.GOOGLE_TABLE,
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

        google_nodes = graph.filter_by_type(NodeType.GOOGLE_TABLE)
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
            n for n in graph.filter_by_type(NodeType.GOOGLE_TABLE) if n.system == SystemType.GOOGLE
        ]
        if not google_nodes:
            return result

        if on_progress:
            on_progress("Push", f"Found {len(google_nodes)} Google tables to update")

        # Convert the graph to OpenLineage events and push each one
        from lineage_bridge.api.openlineage.translator import graph_to_events

        events = graph_to_events(graph)
        # Filter to events that touch a BigQuery output, then rewrite dataset
        # namespaces to formats Google's processor recognizes (the project's
        # internal `confluent://` / `google://` namespaces are rejected).
        events = [e for e in events if any(self._is_bq_output(o) for o in e.outputs)]
        for event in events:
            self._normalize_event_for_google(event)
        now = datetime.now(UTC).isoformat()

        parent = f"projects/{self._project_id}/locations/{self._location}"
        url = f"{_API_BASE}/{parent}:processOpenLineageRunEvent"

        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            for event in events:
                event_dict = event.model_dump(mode="json", exclude_none=True)
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
    def _is_bq_output(dataset: Any) -> bool:
        """True if a dataset looks like a BigQuery target (project.dataset.table)."""
        ns = getattr(dataset, "namespace", "") or ""
        name = getattr(dataset, "name", "") or ""
        return ns.startswith("google://") or ns == "bigquery" or name.count(".") >= 2

    @staticmethod
    def _normalize_event_for_google(event: Any) -> None:
        """Rewrite dataset namespaces in-place so Google's processor accepts them.

        Google requires recognized FQN formats — our internal namespaces
        (``confluent://env/cluster``, ``google://project/dataset``) are rejected.
        Mapping: kafka_topic → ``kafka://<cluster>``; google_table → ``bigquery``.
        Datasets in unrecognized namespaces (UC/Glue/EXTERNAL) are dropped from
        the event since Google can't link them either.
        """
        kept_inputs = []
        for ds in event.inputs:
            ns = ds.namespace or ""
            if ns.startswith("confluent://"):
                # confluent://env-id/cluster-id -> kafka://cluster-id
                cluster = ns.rsplit("/", 1)[-1] or "unknown"
                ds.namespace = f"kafka://{cluster}"
                kept_inputs.append(ds)
            elif ns.startswith("kafka://") or ns == "bigquery":
                kept_inputs.append(ds)
            # else: unrecognized (uc/glue/external) — drop
        event.inputs = kept_inputs

        kept_outputs = []
        for ds in event.outputs:
            ns = ds.namespace or ""
            if ns.startswith("google://") or ds.name.count(".") >= 2:
                ds.namespace = "bigquery"
                kept_outputs.append(ds)
            elif ns == "bigquery":
                kept_outputs.append(ds)
            # else: drop — only BigQuery outputs make sense for Google push
        event.outputs = kept_outputs

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
