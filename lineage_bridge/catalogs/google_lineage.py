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
        # Mirror connect.py:_build_google_tables — keep topic-prefix segments so
        # node IDs match across the Tableflow and Connect-derived code paths.
        table_name = topic_name.replace(".", "_").replace("-", "_")
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

            # Walk forward from each known BQ table via the Data Lineage API
            # to surface BQ-side transformations (scheduled queries, Dataform
            # models, ad-hoc CTAS) as CATALOG_QUERY nodes in the local graph.
            try:
                await self._walk_downstream_lineage(client, graph)
            except Exception as exc:
                logger.warning("Downstream lineage walk failed: %s", exc, exc_info=True)

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

    async def _walk_downstream_lineage(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
    ) -> None:
        """Walk forward from each BQ table via the Data Lineage API.

        For each ``CATALOG_TABLE`` node with ``catalog_type=GOOGLE_DATA_LINEAGE``,
        queries Google's ``searchLinks`` for downstream relationships, resolves
        the linking processes via ``batchSearchLinkProcesses``, and adds:
          - one ``CATALOG_QUERY`` node per unique process (sql/state/origin
            attributes copied off the process resource);
          - a ``CATALOG_TABLE`` node for any target table not already in the
            graph (e.g. a BQ table produced by a scheduled query that isn't
            wired into our Tableflow extraction);
          - ``TRANSFORMS`` edges ``source_table → query → target_table``.

        One hop only — does not recurse from newly-discovered tables. Per-table
        failures (404, 403, transient errors) are logged and swallowed so the
        rest of the walk continues.
        """
        google_tables = graph.filter_catalog_nodes("GOOGLE_DATA_LINEAGE")
        if not google_tables:
            return

        parent = f"projects/{self._project_id}/locations/{self._location}"

        # Index every existing BQ catalog node by its FQN, so we can recognize
        # links whose source/target we already know about and avoid creating
        # duplicate table nodes when the lineage walk loops back on itself.
        fqn_to_node: dict[str, LineageNode] = {}
        for n in google_tables:
            fqn = self._bq_fqn_from_node(n)
            if fqn:
                fqn_to_node[fqn] = n

        seen_processes: set[str] = set()

        for source_node in google_tables:
            source_fqn = self._bq_fqn_from_node(source_node)
            if not source_fqn:
                continue

            try:
                resp = await client.post(
                    f"{_API_BASE}/{parent}:searchLinks",
                    json={"source": {"fullyQualifiedName": source_fqn}},
                )
            except httpx.HTTPError as exc:
                logger.debug("searchLinks transport error for %s: %s", source_fqn, exc)
                continue
            if resp.status_code != 200:
                logger.debug(
                    "searchLinks for %s -> %d (skip)", source_fqn, resp.status_code
                )
                continue
            links = resp.json().get("links", [])
            if not links:
                continue

            try:
                proc_resp = await client.post(
                    f"{_API_BASE}/{parent}:batchSearchLinkProcesses",
                    json={"links": [link["name"] for link in links]},
                )
            except httpx.HTTPError as exc:
                logger.debug("batchSearchLinkProcesses transport error: %s", exc)
                continue
            if proc_resp.status_code != 200:
                continue
            process_links = proc_resp.json().get("processLinks", [])

            # Group links by the process that produced them so we can emit one
            # CATALOG_QUERY node per process with all its source/target edges.
            link_by_name = {link["name"]: link for link in links}
            process_to_links: dict[str, list[dict[str, Any]]] = {}
            for pl in process_links:
                proc_resource = pl.get("process")
                if not proc_resource:
                    continue
                for link_ref in pl.get("links", []):
                    link = link_by_name.get(link_ref.get("link"))
                    if link:
                        process_to_links.setdefault(proc_resource, []).append(link)

            for proc_resource, proc_links in process_to_links.items():
                if proc_resource in seen_processes:
                    # Process already added during a prior source-table iteration;
                    # only need to add any missing edges below.
                    process_node_id = self._catalog_query_node_id(proc_resource)
                else:
                    seen_processes.add(proc_resource)
                    proc_node = await self._build_catalog_query_node(
                        client, proc_resource
                    )
                    if proc_node is None:
                        continue
                    graph.add_node(proc_node)
                    process_node_id = proc_node.node_id

                for link in proc_links:
                    src_fqn = link.get("source", {}).get("fullyQualifiedName")
                    tgt_fqn = link.get("target", {}).get("fullyQualifiedName")

                    src_node = fqn_to_node.get(src_fqn) if src_fqn else None
                    if src_node is not None:
                        try:
                            graph.add_edge(
                                LineageEdge(
                                    src_id=src_node.node_id,
                                    dst_id=process_node_id,
                                    edge_type=EdgeType.TRANSFORMS,
                                )
                            )
                        except ValueError:
                            pass  # process node missing somehow — skip the edge

                    tgt_node = fqn_to_node.get(tgt_fqn) if tgt_fqn else None
                    if tgt_node is None and tgt_fqn:
                        tgt_node = self._build_catalog_table_from_fqn(tgt_fqn)
                        if tgt_node is not None:
                            graph.add_node(tgt_node)
                            fqn_to_node[tgt_fqn] = tgt_node
                    if tgt_node is not None:
                        try:
                            graph.add_edge(
                                LineageEdge(
                                    src_id=process_node_id,
                                    dst_id=tgt_node.node_id,
                                    edge_type=EdgeType.TRANSFORMS,
                                )
                            )
                        except ValueError:
                            pass

    @staticmethod
    def _bq_fqn_from_node(node: LineageNode) -> str | None:
        """Build ``bigquery:<project>.<dataset>.<table>`` from a CATALOG_TABLE node."""
        attrs = node.attributes
        project_id = attrs.get("project_id")
        dataset_id = attrs.get("dataset_id")
        table_name = attrs.get("table_name")
        if not all([project_id, dataset_id, table_name]):
            return None
        return f"bigquery:{project_id}.{dataset_id}.{table_name}"

    @staticmethod
    def _parse_bq_fqn(fqn: str) -> tuple[str, str, str] | None:
        """Reverse of ``_bq_fqn_from_node`` — returns ``(project, dataset, table)``."""
        if not fqn or not fqn.startswith("bigquery:"):
            return None
        parts = fqn[len("bigquery:") :].split(".")
        if len(parts) != 3 or not all(parts):
            return None
        return parts[0], parts[1], parts[2]

    def _catalog_query_node_id(self, proc_resource: str) -> str:
        """Stable node_id derived from the Lineage API process resource name."""
        proc_id = proc_resource.rsplit("/", 1)[-1]
        return f"google:catalog_query:{self._project_id}:{proc_id}"

    async def _build_catalog_query_node(
        self,
        client: httpx.AsyncClient,
        proc_resource: str,
    ) -> LineageNode | None:
        """GET the process detail and turn it into a CATALOG_QUERY node."""
        try:
            resp = await client.get(f"{_API_BASE}/{proc_resource}")
        except httpx.HTTPError as exc:
            logger.debug("Process GET transport error: %s", exc)
            return None
        if resp.status_code != 200:
            return None
        data = resp.json()
        proc_id = proc_resource.rsplit("/", 1)[-1]
        display_name = data.get("displayName") or proc_id
        attrs: dict[str, Any] = dict(data.get("attributes") or {})
        origin = data.get("origin") or {}
        origin_name = origin.get("name")
        if origin_name:
            attrs["catalog_fqn"] = origin_name
            attrs["origin_source_type"] = origin.get("sourceType", "")
        attrs["process_resource"] = proc_resource
        return LineageNode(
            node_id=self._catalog_query_node_id(proc_resource),
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_QUERY,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name=display_name,
            display_name=display_name,
            attributes=attrs,
            url=self._catalog_query_url(attrs, proc_resource),
        )

    def _catalog_query_url(
        self, attrs: dict[str, Any], proc_resource: str
    ) -> str | None:
        """Best-effort GCP console deep link for a catalog query node.

        Preference order:
          1. Scheduled query page if `transfer_config_id` is in attrs (set by
             our DataplexAssetRegistrar.link_processes_to_catalog patch path
             for BQ Data Transfer-backed queries).
          2. Generic Dataplex Lineage process viewer otherwise — useful for
             Google-auto-emitted processes (ad-hoc CTAS, Dataform) where we
             don't know the upstream control-plane resource.
        """
        if not self._project_id:
            return None
        transfer_id = attrs.get("transfer_config_id")
        if transfer_id:
            return (
                "https://console.cloud.google.com/bigquery/scheduled-queries/"
                f"locations/{self._location}/configs/{transfer_id}/runs"
                f"?project={self._project_id}"
            )
        # Fallback: link to the source BQ table list — at least lands the
        # user in the right project's BQ console.
        return (
            "https://console.cloud.google.com/bigquery"
            f"?project={self._project_id}"
        )

    def _build_catalog_table_from_fqn(self, tgt_fqn: str) -> LineageNode | None:
        """Materialize a CATALOG_TABLE node for a BQ FQN we discovered downstream."""
        parsed = self._parse_bq_fqn(tgt_fqn)
        if not parsed:
            return None
        project, dataset, table = parsed
        qualified = f"{project}.{dataset}.{table}"
        return LineageNode(
            node_id=f"google:google_table:{self._project_id}:{qualified}",
            system=SystemType.GOOGLE,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="GOOGLE_DATA_LINEAGE",
            qualified_name=qualified,
            display_name=qualified,
            attributes={
                "project_id": project,
                "dataset_id": dataset,
                "table_name": table,
                "location": self._location,
            },
        )

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

        # Register Confluent assets (topics, Flink jobs, connectors, ksqlDB
        # queries, consumer groups) in Dataplex Catalog so the BQ Lineage tab
        # can surface schema/SQL/config for upstream Confluent nodes — the
        # OpenLineage processor drops every facet at storage time, so the
        # Catalog entry is the only path to surface metadata. Failures are
        # non-fatal: missing entries just mean the side panels stay empty.
        try:
            from lineage_bridge.catalogs.google_dataplex import DataplexAssetRegistrar

            registrar = DataplexAssetRegistrar(self._project_id, self._location, headers)
            registered, registrar_errors = await registrar.register_confluent_assets(
                graph, on_progress=on_progress
            )
            if registered and on_progress:
                on_progress("Catalog", f"Registered {registered} Confluent entries in Dataplex")
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

        # PATCH the Lineage processes Google just created with origin →
        # Catalog FQN + a small attributes dict. Lets the BQ Lineage UI
        # surface SQL/config inline and deep-link to the Catalog entry.
        try:
            from lineage_bridge.catalogs.google_dataplex import DataplexAssetRegistrar

            linker = DataplexAssetRegistrar(self._project_id, self._location, headers)
            linked, link_errors = await linker.link_processes_to_catalog(
                graph, on_progress=on_progress
            )
            if linked and on_progress:
                on_progress("Lineage", f"Linked {linked} process(es) to Catalog entries")
            for err in link_errors:
                result.errors.append(err)
        except Exception as exc:
            logger.warning("Process linkage skipped: %s", exc, exc_info=True)
            result.errors.append(f"Process linkage skipped: {exc}")

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
