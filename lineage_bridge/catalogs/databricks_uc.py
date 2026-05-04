# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks Unity Catalog provider."""

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


def _quote_sql_name(qualified_name: str) -> str:
    """Quote a catalog.schema.table name for SQL, backticking parts with special chars."""
    parts = qualified_name.split(".")
    quoted = []
    for part in parts:
        if not part.replace("_", "").isalnum() or part[0:1].isdigit():
            quoted.append(f"`{part}`")
        else:
            quoted.append(part)
    return ".".join(quoted)


# Substrings that mark a Databricks SQL error as a non-fatal skip rather than
# a real push failure. These show up when the graph spans catalogs the current
# principal doesn't own (e.g., leftover UC tables from a prior demo run): the
# principal lacks MODIFY on the table, or the bridge CREATE silently failed
# upstream so the INSERT can't find the table.
_SKIP_ERROR_MARKERS = (
    "PERMISSION_DENIED",
    "does not have MODIFY",
    "does not have CREATE",
    "does not have USE",
    "does not have OWNERSHIP",
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
)


def _is_benign_sql_error(error_msg: str) -> bool:
    """True when an SQL failure should be reported as a skip, not an error.

    Catalog spill across owners is the common case — the user pushes a graph
    that contains UC tables they didn't create, and Databricks correctly
    rejects ALTER / COMMENT / INSERT against them. Crashing the whole push
    on those would make multi-demo workflows unusable.
    """
    if not error_msg:
        return False
    return any(marker in error_msg for marker in _SKIP_ERROR_MARKERS)


class DatabricksUCProvider:
    """CatalogProvider implementation for Databricks Unity Catalog."""

    catalog_type: str = "UNITY_CATALOG"

    def __init__(
        self,
        workspace_url: str | None = None,
        token: str | None = None,
    ) -> None:
        self._workspace_url = workspace_url.rstrip("/") if workspace_url else None
        self._token = token

    def build_node(
        self,
        ci_config: dict[str, Any],
        tableflow_node_id: str,
        topic_name: str,
        cluster_id: str,
        environment_id: str,
    ) -> tuple[LineageNode, LineageEdge]:
        """Create a CATALOG_TABLE node + MATERIALIZES edge from the tableflow node."""
        # Support both flat config format (API) and nested format (legacy/tests)
        uc_cfg = ci_config.get("unity_catalog", ci_config)
        catalog_name = uc_cfg.get("catalog_name", "confluent_tableflow")
        # Prefer the user-configured workspace URL (settings) over Confluent's
        # stored workspace_endpoint, which may be stale or wrong.
        workspace_url = (
            self._workspace_url or uc_cfg.get("workspace_endpoint") or uc_cfg.get("workspace_url")
        )
        # Tableflow normalizes dots → underscores in table names, but keeps
        # the raw cluster ID (with hyphens) as the schema name.
        uc_table_name = topic_name.replace(".", "_")
        qualified = f"{catalog_name}.{cluster_id}.{uc_table_name}"
        # Node ID keeps the legacy "uc_table" segment so existing graph IDs and
        # external references don't all churn at once. The discriminator that
        # matters at runtime is `catalog_type`.
        uc_id = f"databricks:uc_table:{environment_id}:{qualified}"

        node = LineageNode(
            node_id=uc_id,
            system=SystemType.DATABRICKS,
            node_type=NodeType.CATALOG_TABLE,
            catalog_type="UNITY_CATALOG",
            qualified_name=qualified,
            display_name=qualified,
            environment_id=environment_id,
            cluster_id=cluster_id,
            attributes={
                "catalog_name": catalog_name,
                "schema_name": cluster_id,
                "table_name": uc_table_name,
                "workspace_url": workspace_url,
            },
        )
        edge = LineageEdge(
            src_id=tableflow_node_id,
            dst_id=uc_id,
            edge_type=EdgeType.MATERIALIZES,
        )
        return node, edge

    async def enrich(self, graph: LineageGraph) -> None:
        """Fetch table metadata and lineage from the Databricks UC REST API."""
        if not self._workspace_url or not self._token:
            logger.debug("Databricks UC enrichment skipped — no credentials configured")
            return

        uc_nodes = graph.filter_catalog_nodes("UNITY_CATALOG")
        if not uc_nodes:
            return

        async with httpx.AsyncClient(
            base_url=self._workspace_url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30.0,
        ) as client:
            for node in uc_nodes:
                if node.system != SystemType.DATABRICKS:
                    continue
                await self._enrich_node(client, graph, node)

            # Discover downstream tables via Databricks lineage API
            await self._discover_lineage(client, graph, uc_nodes)

            # After lineage walk, enrich any NOTEBOOK nodes with the metadata
            # of the job that owns them (job_id captured from workflowInfos).
            await self._enrich_notebook_jobs(client, graph)

    async def _enrich_notebook_jobs(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
    ) -> None:
        """Fetch Jobs API metadata for every NOTEBOOK node carrying a job_id.

        One ``/api/2.1/jobs/get`` and one ``/api/2.1/jobs/runs/list?limit=1``
        per unique job_id, fanned out concurrently. Failures (404 / 403) are
        per-job and don't block other notebooks.
        """
        from lineage_bridge.clients.databricks_jobs import get_job, get_last_run

        notebook_nodes = [n for n in graph.nodes if n.node_type == NodeType.NOTEBOOK]
        # Group by job_id so we issue one fetch per job, not per notebook.
        job_to_notebooks: dict[int, list[LineageNode]] = {}
        for n in notebook_nodes:
            jid = n.attributes.get("job_id")
            if jid is None:
                continue
            job_to_notebooks.setdefault(int(jid), []).append(n)

        if not job_to_notebooks:
            return

        async def _fetch(job_id: int) -> tuple[int, Any, Any]:
            return job_id, await get_job(client, job_id), await get_last_run(client, job_id)

        results = await asyncio.gather(
            *(_fetch(jid) for jid in job_to_notebooks),
            return_exceptions=False,
        )

        for job_id, job, last_run in results:
            for nb in job_to_notebooks[job_id]:
                update: dict[str, Any] = {}
                notebook_name: str | None = None
                if job is not None:
                    if job.name:
                        update["job_name"] = job.name
                    if job.schedule_cron:
                        update["schedule_cron"] = job.schedule_cron
                    if job.schedule_timezone:
                        update["schedule_timezone"] = job.schedule_timezone
                    if job.schedule_paused is not None:
                        update["schedule_paused"] = job.schedule_paused
                    if job.creator:
                        update["job_creator"] = job.creator
                    if job.notebook_path:
                        update["notebook_path"] = job.notebook_path
                        # The notebook NAME (basename of its workspace path)
                        # — distinct from the JOB name. The user wants the
                        # chip to read the notebook itself, with the job
                        # surfaced separately in the detail panel.
                        notebook_name = job.notebook_path.rsplit("/", 1)[-1]
                        update["notebook_name"] = notebook_name
                if last_run is not None:
                    if last_run.life_cycle_state:
                        update["last_run_state"] = last_run.life_cycle_state
                    if last_run.result_state:
                        update["last_run_result"] = last_run.result_state
                    if last_run.start_time_ms is not None:
                        update["last_run_started_at_ms"] = last_run.start_time_ms

                if not update:
                    continue
                merged = {**nb.attributes, **update}
                node_update: dict[str, Any] = {"attributes": merged}
                if notebook_name:
                    node_update["display_name"] = notebook_name
                graph.add_node(nb.model_copy(update=node_update))

    async def _enrich_node(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        node: LineageNode,
    ) -> None:
        """Fetch and merge metadata for a single UC table node."""
        full_name = node.qualified_name  # catalog.schema.table
        url = f"/api/2.1/unity-catalog/tables/{full_name}"

        for attempt in range(_MAX_RETRIES):
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    enriched_attrs = {
                        **node.attributes,
                        "owner": data.get("owner"),
                        "table_type": data.get("table_type"),
                        "columns": data.get("columns"),
                        "storage_location": data.get("storage_location"),
                        "updated_at": data.get("updated_at"),
                    }
                    enriched = node.model_copy(update={"attributes": enriched_attrs})
                    graph.add_node(enriched)
                    return
                if resp.status_code in (401, 403, 404):
                    logger.warning(
                        "Databricks UC API returned %d for %s — skipping",
                        resp.status_code,
                        full_name,
                    )
                    return
                if resp.status_code in (429, 500, 502, 503, 504):
                    delay = _BACKOFF_BASE * (2**attempt)
                    logger.debug(
                        "Databricks UC API %d for %s — retrying in %.1fs",
                        resp.status_code,
                        full_name,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                # Other status codes — log and skip
                logger.warning(
                    "Databricks UC API unexpected %d for %s",
                    resp.status_code,
                    full_name,
                )
                return
            except httpx.HTTPError:
                logger.debug("HTTP error enriching %s (attempt %d)", full_name, attempt + 1)
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(_BACKOFF_BASE * (2**attempt))

    async def _discover_lineage(
        self,
        client: httpx.AsyncClient,
        graph: LineageGraph,
        seed_nodes: list[LineageNode],
    ) -> None:
        """Walk lineage from seed UC nodes to discover derived tables and notebooks.

        Two passes per visited table:

        - **downstreams** discover new derived tables. The Databricks API
          rarely populates ``notebookInfos`` on this side (verified empirically
          on UC notebooks-as-job demos), so we only use it for table edges.
        - **upstreams** of the same response carry the producer attribution —
          ``notebookInfos`` and ``jobInfos`` here name the notebook/job that
          wrote the *current* table from each listed source. We use this to
          insert NOTEBOOK nodes between sources and the current table.

        ``jobInfos`` (not ``workflowInfos`` — the API field is misnamed in
        Databricks docs) carries ``job_id`` so the post-walk
        ``_enrich_notebook_jobs`` step can fetch each job's name + schedule
        + last run state.
        """
        seen_qualified: set[str] = {n.qualified_name for n in seed_nodes}
        seen_notebooks: set[str] = set()
        to_visit: list[LineageNode] = list(seed_nodes)

        while to_visit:
            node = to_visit.pop()
            full_name = node.qualified_name
            url = "/api/2.0/lineage-tracking/table-lineage"

            try:
                # `include_entity_lineage=true` is REQUIRED to populate the
                # notebookInfos / jobInfos / dashboardInfos sections. Without
                # it the API returns only tableInfo per entry — which is what
                # bit us in the live smoke test.
                resp = await client.get(
                    url,
                    params={"table_name": full_name, "include_entity_lineage": "true"},
                )
            except httpx.HTTPError:
                logger.debug("Lineage API error for %s", full_name)
                continue

            if resp.status_code != 200:
                logger.debug("Lineage API %d for %s", resp.status_code, full_name)
                continue

            data = resp.json()

            # ── Pass 1: downstreams → discover derived tables ──────────────
            for entry in data.get("downstreams", []):
                ti = entry.get("tableInfo")
                if not ti:
                    continue
                cat = ti.get("catalog_name", "")
                sch = ti.get("schema_name", "")
                tbl = ti.get("name", "")
                if not (cat and sch and tbl):
                    continue
                qualified = f"{cat}.{sch}.{tbl}"
                if qualified in seen_qualified:
                    continue
                seen_qualified.add(qualified)

                env_id = node.environment_id
                cluster_id = node.cluster_id
                new_id = f"databricks:uc_table:{env_id}:{qualified}"

                new_node = LineageNode(
                    node_id=new_id,
                    system=SystemType.DATABRICKS,
                    node_type=NodeType.CATALOG_TABLE,
                    catalog_type="UNITY_CATALOG",
                    qualified_name=qualified,
                    display_name=qualified,
                    environment_id=env_id,
                    cluster_id=cluster_id,
                    attributes={
                        "catalog_name": cat,
                        "schema_name": sch,
                        "table_name": tbl,
                        "workspace_url": self._workspace_url,
                        "derived": True,
                    },
                )
                graph.add_node(new_node)

                # Notebook attribution sometimes shows up on this side too
                # (older Databricks workspaces). Process it when present;
                # otherwise the upstream pass on `new_node` will catch it.
                notebook_nodes = self._build_notebook_nodes(
                    entry.get("notebookInfos") or [],
                    entry.get("jobInfos") or [],
                    env_id,
                    cluster_id,
                )
                if notebook_nodes:
                    self._wire_notebooks(
                        graph,
                        notebook_nodes,
                        seen_notebooks,
                        source_id=node.node_id,
                        target_id=new_id,
                    )
                else:
                    self._add_edge_safe(
                        graph,
                        LineageEdge(
                            src_id=node.node_id,
                            dst_id=new_id,
                            edge_type=EdgeType.TRANSFORMS,
                        ),
                    )

                # Enrich the new node with table metadata
                await self._enrich_node(client, graph, new_node)
                # Continue walking downstream
                to_visit.append(new_node)

                logger.info(
                    "Discovered derived UC table: %s (downstream of %s)",
                    qualified,
                    full_name,
                )

            # ── Pass 2: upstreams → attribute notebooks/jobs to current ────
            # The Databricks API populates notebookInfos/jobInfos on the
            # *upstream* side of the writer relationship: querying lineage
            # of the derived table returns the producer notebook/job in
            # each upstream entry. Use this to add NOTEBOOK hops between
            # the source and the current node when the downstream side
            # didn't carry the attribution.
            for entry in data.get("upstreams", []):
                src_ti = entry.get("tableInfo") or {}
                src_parts = (
                    src_ti.get("catalog_name"),
                    src_ti.get("schema_name"),
                    src_ti.get("name"),
                )
                src_qualified = ".".join(p for p in src_parts if p)
                # Match the source by qualified_name (it should already be in
                # the graph from the seed list or a prior downstream pass).
                src_node = next((n for n in graph.nodes if n.qualified_name == src_qualified), None)
                if src_node is None:
                    continue

                notebook_nodes = self._build_notebook_nodes(
                    entry.get("notebookInfos") or [],
                    entry.get("jobInfos") or [],
                    node.environment_id,
                    node.cluster_id,
                )
                if not notebook_nodes:
                    continue

                # If a TRANSFORMS edge was already laid down (because the
                # downstream pass had no notebook info), drop it now that
                # the notebook hop is the canonical path.
                self._drop_edge_if_present(
                    graph, src_node.node_id, node.node_id, EdgeType.TRANSFORMS
                )
                self._wire_notebooks(
                    graph,
                    notebook_nodes,
                    seen_notebooks,
                    source_id=src_node.node_id,
                    target_id=node.node_id,
                )

    def _wire_notebooks(
        self,
        graph: LineageGraph,
        notebook_nodes: list[LineageNode],
        seen: set[str],
        *,
        source_id: str,
        target_id: str,
    ) -> None:
        """Add notebook nodes (deduped by node_id) and wire CONSUMES + PRODUCES edges.

        Edges always fire — graph.add_edge is idempotent on (src,dst,type),
        so a notebook shared across multiple lineage entries accumulates
        edges to all the sources / targets it actually touches.
        """
        for nb in notebook_nodes:
            if nb.node_id not in seen:
                graph.add_node(nb)
                seen.add(nb.node_id)
            else:
                # Merge any newly-discovered job_id onto the existing node
                # so the post-walk enrichment can fetch its metadata.
                existing = graph.get_node(nb.node_id)
                if (
                    existing is not None
                    and "job_id" in nb.attributes
                    and "job_id" not in existing.attributes
                ):
                    merged = {**existing.attributes, "job_id": nb.attributes["job_id"]}
                    graph.add_node(existing.model_copy(update={"attributes": merged}))
            self._add_edge_safe(
                graph,
                LineageEdge(src_id=source_id, dst_id=nb.node_id, edge_type=EdgeType.CONSUMES),
            )
            self._add_edge_safe(
                graph,
                LineageEdge(src_id=nb.node_id, dst_id=target_id, edge_type=EdgeType.PRODUCES),
            )

    @staticmethod
    def _drop_edge_if_present(
        graph: LineageGraph, src_id: str, dst_id: str, edge_type: EdgeType
    ) -> None:
        """Remove an edge if it exists; no-op otherwise. Used to swap a
        TRANSFORMS table-to-table edge for a notebook hop when the upstream
        pass turns up the missing attribution after the downstream pass."""
        graph.remove_edge(src_id, dst_id, edge_type)

    def _build_notebook_nodes(
        self,
        notebook_infos: list[dict[str, Any]],
        job_infos: list[dict[str, Any]],
        env_id: str | None,
        cluster_id: str | None,
    ) -> list[LineageNode]:
        """Build NOTEBOOK nodes from a lineage entry's notebook + job infos.

        Returns nodes for every notebook in the entry (no dedup here — the
        caller's ``_wire_notebooks`` decides whether to ``add_node`` based
        on its own seen set, but always wires edges so a notebook shared
        across entries accumulates edges to multiple sources/targets).

        ``jobInfos`` from the same entry carries ``job_id`` for the
        Databricks job that ran the notebook. We attach the first job_id
        so the post-walk enrichment can fetch its name + schedule + last
        run state. Multiple jobs per notebook entry are rare; we record
        one for now.
        """
        primary_job_id = None
        for wf in job_infos:
            jid = wf.get("job_id")
            if jid is not None:
                primary_job_id = jid
                break

        out: list[LineageNode] = []
        for info in notebook_infos:
            notebook_id = info.get("notebook_id")
            if notebook_id is None:
                continue
            nb_id = f"databricks:notebook:{env_id or ''}:{notebook_id}"
            workspace_id = info.get("workspace_id")
            display = f"Notebook {notebook_id}"
            attrs: dict[str, Any] = {
                "notebook_id": notebook_id,
                "workspace_id": workspace_id,
                "workspace_url": self._workspace_url,
            }
            if primary_job_id is not None:
                attrs["job_id"] = primary_job_id
            out.append(
                LineageNode(
                    node_id=nb_id,
                    system=SystemType.DATABRICKS,
                    node_type=NodeType.NOTEBOOK,
                    qualified_name=str(notebook_id),
                    display_name=display,
                    environment_id=env_id,
                    cluster_id=cluster_id,
                    attributes=attrs,
                )
            )
        return out

    @staticmethod
    def _add_edge_safe(graph: LineageGraph, edge: LineageEdge) -> None:
        """Add an edge, swallowing the duplicate-edge ValueError as a debug log."""
        try:
            graph.add_edge(edge)
        except ValueError:
            logger.debug(
                "Skipping lineage edge %s -> %s (%s)",
                edge.src_id,
                edge.dst_id,
                edge.edge_type.value,
            )

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        sql_client: Any | None = None,
        warehouse_id: str | None = None,
        set_properties: bool = True,
        set_comments: bool = True,
        create_bridge_table: bool = False,
        bridge_table_name: str | None = None,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Push Confluent lineage metadata to UC tables via the Statement Execution API.

        Provider builds its own `DatabricksSQLClient` if none is supplied,
        discovering a running warehouse if `warehouse_id` is also unset.
        Tests can inject a mock `sql_client` directly.
        """
        from lineage_bridge.clients.databricks_sql import DatabricksSQLClient

        result = PushResult()

        if sql_client is None:
            if not self._workspace_url or not self._token:
                return PushResult(
                    errors=["Databricks workspace URL / token not configured on provider"]
                )
            if not warehouse_id:
                from lineage_bridge.clients.databricks_discovery import (
                    list_warehouses,
                    pick_running_warehouse,
                )

                if on_progress:
                    on_progress("Push", "No warehouse ID configured — discovering...")
                try:
                    warehouses = await list_warehouses(self._workspace_url, self._token)
                    selected = pick_running_warehouse(warehouses)
                    if not selected:
                        return PushResult(errors=["No SQL warehouses found in workspace"])
                    warehouse_id = selected.id
                    if on_progress:
                        on_progress("Push", f"Auto-selected warehouse: {selected.name}")
                except Exception as exc:
                    return PushResult(errors=[f"Warehouse discovery failed: {exc}"])
            sql_client = DatabricksSQLClient(
                workspace_url=self._workspace_url,
                token=self._token,
                warehouse_id=warehouse_id,
            )
        else:
            assert isinstance(sql_client, DatabricksSQLClient)

        uc_nodes = [
            n
            for n in graph.filter_catalog_nodes("UNITY_CATALOG")
            if n.system == SystemType.DATABRICKS
        ]
        if not uc_nodes:
            return result

        if on_progress:
            on_progress("Push", f"Found {len(uc_nodes)} UC tables to update")

        # Derive bridge table name from the first UC node's catalog
        if create_bridge_table:
            if not bridge_table_name:
                catalog_name = uc_nodes[0].attributes.get("catalog_name", "")
                if catalog_name:
                    bridge_table_name = f"{catalog_name}.default.confluent_lineage"
                else:
                    bridge_table_name = "default.confluent_lineage"
            await self._create_bridge_table(sql_client, bridge_table_name, result)

        now = datetime.now(UTC).isoformat()

        for node in uc_nodes:
            upstream = graph.get_upstream(node.node_id)
            if not upstream:
                continue

            if set_properties:
                await self._set_table_properties(sql_client, node, upstream, graph, now, result)

            if set_comments:
                await self._set_table_comment(sql_client, node, upstream, graph, now, result)

            if create_bridge_table:
                await self._insert_bridge_rows(
                    sql_client, bridge_table_name, node, upstream, graph, now, result
                )

            result.tables_updated += 1
            if on_progress:
                on_progress("Push", f"Updated {node.qualified_name}")

        if on_progress:
            on_progress("Push", f"Done — {result.tables_updated} tables updated")

        return result

    async def _set_table_properties(
        self,
        sql_client: Any,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Set lineage_bridge.* table properties on a UC table."""
        from lineage_bridge.catalogs.upstream_chain import build_upstream_chain, chain_to_json

        source_topics = []
        source_connectors = []
        for up_node, _edge, _hop in upstream:
            if up_node.node_type == NodeType.KAFKA_TOPIC:
                source_topics.append(up_node.qualified_name)
            elif up_node.node_type == NodeType.CONNECTOR:
                source_connectors.append(up_node.display_name)

        # Rich multi-hop chain (Flink/ksqlDB/intermediate topics/external sources).
        # Capped at ~3 KB to leave headroom under Databricks' 4 KB per-property limit.
        chain = build_upstream_chain(graph, node.node_id)
        chain_json, truncated = chain_to_json(chain, max_bytes=3 * 1024)

        props = {
            "lineage_bridge.source_topics": ",".join(source_topics) if source_topics else "",
            "lineage_bridge.source_connectors": (
                ",".join(source_connectors) if source_connectors else ""
            ),
            "lineage_bridge.upstream_chain": chain_json if chain else "",
            "lineage_bridge.upstream_truncated": "true" if truncated else "",
            "lineage_bridge.pipeline_type": "tableflow",
            "lineage_bridge.last_synced": now,
            "lineage_bridge.environment_id": node.environment_id or "",
            "lineage_bridge.cluster_id": node.cluster_id or "",
        }

        # Escape single quotes inside SQL string literals.
        prop_pairs = ", ".join(
            f"'{k}' = '{v.replace(chr(39), chr(39) * 2)}'" for k, v in props.items() if v
        )
        if not prop_pairs:
            return

        table_ref = _quote_sql_name(node.qualified_name)
        sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({prop_pairs})"
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.properties_set += 1
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                msg = f"Properties failed for {node.qualified_name}: {error}"
                if _is_benign_sql_error(error):
                    result.skipped.append(msg)
                else:
                    result.errors.append(msg)
        except Exception as exc:
            result.errors.append(f"Properties error for {node.qualified_name}: {exc}")

    async def _set_table_comment(
        self,
        sql_client: Any,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Set a human-readable lineage comment on a UC table."""
        from lineage_bridge.catalogs.upstream_chain import (
            build_upstream_chain,
            format_chain_summary,
        )

        chain = build_upstream_chain(graph, node.node_id)

        lines = []
        summary = format_chain_summary(chain, node.display_name or node.qualified_name)
        if summary:
            lines.append(summary)
        if node.environment_id:
            lines.append(f"Environment: {node.environment_id}")
        lines.append(f"Last synced: {now}")
        lines.append("Managed by LineageBridge")

        comment_text = "\\n".join(lines).replace("'", "\\'")
        table_ref = _quote_sql_name(node.qualified_name)
        sql = f"COMMENT ON TABLE {table_ref} IS '{comment_text}'"

        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.comments_set += 1
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                msg = f"Comment failed for {node.qualified_name}: {error}"
                if _is_benign_sql_error(error):
                    result.skipped.append(msg)
                else:
                    result.errors.append(msg)
        except Exception as exc:
            result.errors.append(f"Comment error for {node.qualified_name}: {exc}")

    async def _create_bridge_table(
        self, sql_client: Any, bridge_table_name: str, result: PushResult
    ) -> None:
        """Create the lineage bridge table if it doesn't exist.

        Confluent Tableflow only auto-creates the ``lkc-{cluster}`` schema in
        the target catalog — the ``default`` schema we'd otherwise rely on
        often doesn't exist on freshly-provisioned UC catalogs. So we
        ``CREATE SCHEMA IF NOT EXISTS`` first, then the bridge table.

        ``chain_json`` carries the full upstream chain (Flink/ksqlDB SQL,
        intermediate topics, connector classes, schema fields per topic) so
        downstream queries can drill into the pipeline without joining back
        to LineageBridge.
        """
        # Pre-create the parent schema. Bridge name is `catalog.schema.table`;
        # CREATE SCHEMA wants `catalog.schema`. Quoting handles names with
        # hyphens (e.g. `lkc-pypmmy`).
        parts = bridge_table_name.split(".")
        if len(parts) == 3:
            schema_ref = _quote_sql_name(".".join(parts[:2]))
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_ref}"
            try:
                schema_resp = await sql_client.execute(schema_sql)
                if schema_resp.get("status", {}).get("state") != "SUCCEEDED":
                    error = schema_resp.get("status", {}).get("error", {}).get("message", "unknown")
                    msg = f"Bridge schema creation failed for {schema_ref}: {error}"
                    if _is_benign_sql_error(error):
                        # Caller lacks CREATE SCHEMA on this catalog — skip
                        # the bridge table too (the subsequent CREATE TABLE
                        # would fail with the same error).
                        result.skipped.append(msg)
                        return
                    result.errors.append(msg)
                    return
            except Exception as exc:
                result.errors.append(f"Bridge schema creation error: {exc}")
                return

        sql = f"""CREATE TABLE IF NOT EXISTS {bridge_table_name} (
    uc_table STRING,
    source_type STRING,
    source_name STRING,
    source_system STRING,
    edge_type STRING,
    environment_id STRING,
    cluster_id STRING,
    hop_distance INT,
    full_path STRING,
    chain_json STRING,
    synced_at TIMESTAMP
)"""
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") != "SUCCEEDED":
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                msg = f"Bridge table creation failed for {bridge_table_name}: {error}"
                if _is_benign_sql_error(error):
                    result.skipped.append(msg)
                else:
                    result.errors.append(msg)
        except Exception as exc:
            result.errors.append(f"Bridge table creation error: {exc}")

    async def _insert_bridge_rows(
        self,
        sql_client: Any,
        bridge_table_name: str,
        node: LineageNode,
        upstream: list,
        graph: LineageGraph,
        now: str,
        result: PushResult,
    ) -> None:
        """Insert lineage rows into the bridge table.

        Each row gets the same ``chain_json`` payload so the full pipeline is
        queryable from any single hop without re-walking the graph.
        """
        from lineage_bridge.catalogs.upstream_chain import build_upstream_chain, chain_to_json

        chain = build_upstream_chain(graph, node.node_id)
        chain_json, _ = chain_to_json(chain)
        chain_json_sql = chain_json.replace("'", "''")

        values = []
        for up_node, edge, hop in upstream:
            # Use raw qualified_name in VALUES (string literal, no quoting needed)
            values.append(
                f"('{node.qualified_name}', '{up_node.node_type.value}', "
                f"'{up_node.qualified_name}', '{up_node.system.value}', "
                f"'{edge.edge_type.value}', '{node.environment_id or ''}', "
                f"'{node.cluster_id or ''}', {hop}, '', '{chain_json_sql}', TIMESTAMP '{now}')"
            )
        if not values:
            return

        values_str = ",\n".join(values)
        bridge_ref = _quote_sql_name(bridge_table_name)
        sql = f"INSERT INTO {bridge_ref} VALUES {values_str}"
        try:
            resp = await sql_client.execute(sql)
            if resp.get("status", {}).get("state") == "SUCCEEDED":
                result.bridge_rows_inserted += len(values)
            else:
                error = resp.get("status", {}).get("error", {}).get("message", "unknown")
                msg = f"Bridge insert failed for {node.qualified_name}: {error}"
                if _is_benign_sql_error(error):
                    result.skipped.append(msg)
                else:
                    result.errors.append(msg)
        except Exception as exc:
            result.errors.append(f"Bridge insert error for {node.qualified_name}: {exc}")

    def build_url(self, node: LineageNode) -> str | None:
        """Build a deep link to the node in the Databricks workspace UI.

        Tables resolve to ``/explore/data/{catalog}/{schema}/{table}``;
        notebooks (discovered via the lineage-tracking API) to
        ``/#notebook/{notebook_id}`` since the API only returns the
        notebook_id, not its workspace path.
        """
        # Prefer the provider's configured workspace URL (from settings) over
        # the per-node attribute, which may carry a stale value baked in by
        # Confluent's Tableflow integration config.
        workspace_url = self._workspace_url or node.attributes.get("workspace_url")
        if not workspace_url:
            return None
        base = workspace_url.rstrip("/")
        if node.node_type == NodeType.NOTEBOOK:
            # Prefer the path-based URL when we know it (more durable than
            # the legacy id form, and matches what the Databricks UI uses):
            #   {workspace}/#workspace<absolute_path>
            path = node.attributes.get("notebook_path")
            if path:
                if not path.startswith("/"):
                    path = f"/{path}"
                return f"{base}/#workspace{path}"
            notebook_id = node.attributes.get("notebook_id")
            if notebook_id is None:
                return None
            return f"{base}/#notebook/{notebook_id}"
        parts = node.qualified_name.split(".")
        if len(parts) != 3:
            return None
        catalog, schema, table = parts
        return f"{base}/explore/data/{catalog}/{schema}/{table}"
