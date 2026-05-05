# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Register Confluent assets in Google Dataplex Catalog and link them to Lineage processes.

Google's ``processOpenLineageRunEvent`` API stores only the lineage link FQNs
— it discards every facet (schema, columnLineage, custom). And the resulting
Lineage ``Process`` resources are bare names, with no link back to where the
SQL or connector config lives.

This module compensates by:

1. Registering one Dataplex Catalog entry per Confluent node type
   (Kafka topic, Flink job, connector, ksqlDB query, consumer group) with a
   typed aspect carrying the relevant metadata. Kafka topics carry their
   schema; Flink/ksqlDB carry their SQL; connectors carry their class +
   direction + state; consumer groups carry their state + member count.

2. After OpenLineage events land, listing the resulting Lineage processes
   and PATCHing each one with ``origin.name`` pointing at the matching
   Catalog entry plus a small ``attributes`` dict — so clicking a process
   node in the BQ Lineage UI surfaces the SQL / config inline and a deep
   link to the Catalog entry with the full record.

Bootstrap (entry group + per-type entry/aspect types) is idempotent and
runs on first use.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from lineage_bridge.models.graph import EdgeType, LineageGraph, LineageNode, NodeType

logger = logging.getLogger(__name__)

_API_BASE = "https://dataplex.googleapis.com/v1"
_LINEAGE_API_BASE = "https://datalineage.googleapis.com/v1"

# Single entry group hosts all of LineageBridge's catalog entries.
ENTRY_GROUP_ID = "lineage-bridge"

# Kept as module constants for backwards-compat with existing tests/imports
# that look these up by name. Per-type config now lives in ``_REGISTRY``.
ENTRY_TYPE_ID = "lineage-bridge-kafka-topic"
ASPECT_TYPE_ID = "lineage-bridge-schema"

_ENTRY_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")


# ── FQN builders ────────────────────────────────────────────────────────


def _kafka_fqn(cluster_id: str, topic: str) -> str:
    """Backwards-compat shim — delegates to the shared FQN helper.

    Kept here so test_google_dataplex.py keeps importing from this module.
    Single source of truth lives in ``openlineage/normalize.py``.
    """
    from lineage_bridge.openlineage.normalize import kafka_fqn

    return kafka_fqn(cluster_id, topic)


def _quote_dotted(name: str) -> str:
    """Backtick-wrap names that contain dots/spaces — mirrors kafka_fqn."""
    if "." in name or " " in name:
        return f"`{name}`"
    return name


# Dataplex only accepts `custom:` as a prefix for user-defined FQNs (verified
# by direct API probing — `kafka-connect:`, `connect:`, etc. are rejected with
# "Unrecognized FQN"). All non-Kafka entries get the `custom:<system>:<id>` form.
def _flink_fqn(env_id: str, job_name: str) -> str:
    return f"custom:confluent-flink:{env_id or 'unknown'}.{_quote_dotted(job_name)}"


def _connector_fqn(cluster_id: str, name: str) -> str:
    return f"custom:confluent-connect:{cluster_id or 'unknown'}.{_quote_dotted(name)}"


def _ksqldb_fqn(cluster_id: str, query_id: str) -> str:
    return f"custom:confluent-ksqldb:{cluster_id or 'unknown'}.{_quote_dotted(query_id)}"


def _consumer_group_fqn(cluster_id: str, group_id: str) -> str:
    return f"custom:confluent-consumer-group:{cluster_id or 'unknown'}.{_quote_dotted(group_id)}"


# ── Entry ID builders ───────────────────────────────────────────────────


def _entry_id(parts: str) -> str:
    """Sanitize an arbitrary string into a Dataplex-legal entry ID (≤200 chars)."""
    return _ENTRY_ID_RE.sub("_", parts).lower()[:200]


# ── Aspect metadata templates ───────────────────────────────────────────

_KAFKA_SCHEMA_TEMPLATE: dict[str, Any] = {
    "name": "schema",
    "type": "record",
    "index": 1,
    "recordFields": [
        {
            "name": "fields",
            "index": 1,
            "type": "array",
            "arrayItems": {
                "name": "field",
                "type": "record",
                "index": 1,
                "recordFields": [
                    {"name": "name", "type": "string", "index": 1},
                    {"name": "type", "type": "string", "index": 2},
                    {"name": "description", "type": "string", "index": 3},
                ],
            },
        }
    ],
}


def _flat_string_template(name: str, fields: list[str]) -> dict[str, Any]:
    """Build a flat record template where every field is a string.

    Most non-Kafka aspects just want to surface a few scalar values
    (sql, state, class, etc.) — coerce everything to string at write time.
    """
    return {
        "name": name,
        "type": "record",
        "index": 1,
        "recordFields": [
            {"name": f, "type": "string", "index": i} for i, f in enumerate(fields, start=1)
        ],
    }


# ── Aspect data builders ────────────────────────────────────────────────


def _kafka_schema_data(node: LineageNode, graph: LineageGraph) -> dict[str, Any] | None:
    """Pull schema fields from a HAS_SCHEMA-linked SCHEMA node, if any."""
    for edge in graph.edges:
        if edge.src_id != node.node_id or edge.edge_type != EdgeType.HAS_SCHEMA:
            continue
        schema_node = graph.get_node(edge.dst_id)
        if not schema_node:
            continue
        fields = schema_node.attributes.get("fields") or []
        out: list[dict[str, str]] = []
        for f in fields:
            if not isinstance(f, dict):
                continue
            entry: dict[str, str] = {}
            if f.get("name"):
                entry["name"] = str(f["name"])
            if f.get("type") is not None:
                entry["type"] = str(f["type"])
            doc = f.get("description") or f.get("doc")
            if doc:
                entry["description"] = str(doc)
            if entry:
                out.append(entry)
        if out:
            return {"fields": out}
    return None


def _scalar_attr_data(
    keys: tuple[str, ...],
) -> Callable[[LineageNode, LineageGraph], dict[str, Any] | None]:
    """Build a closure that extracts the named attributes off a node, stringified.

    Empty/None values are dropped so the persisted aspect only carries the
    fields the extractor actually populated.
    """

    def _builder(node: LineageNode, _graph: LineageGraph) -> dict[str, Any] | None:
        out: dict[str, str] = {}
        for k in keys:
            v = node.attributes.get(k)
            if v is None or v == "":
                continue
            out[k] = str(v)
        return out or None

    return _builder


# ── Per-node-type registration config ──────────────────────────────────


@dataclass(frozen=True)
class NodeRegistration:
    """Per-NodeType config for Dataplex Catalog registration.

    ``aspect_data_builder`` returns the dict that goes inside ``data`` of the
    aspect, or ``None`` to skip the aspect (e.g. Kafka topic with no schema).
    ``fqn_builder`` and ``entry_id_builder`` both take the node and produce
    a string. ``cluster_or_env`` selects which scope the entry sits under
    (Flink is env-scoped; everything else is cluster-scoped).
    """

    node_type: NodeType
    entry_type_id: str
    aspect_type_id: str
    entry_type_display: str
    entry_type_description: str
    platform: str
    aspect_display: str
    aspect_description: str
    aspect_template: dict[str, Any]
    aspect_data_builder: Callable[[LineageNode, LineageGraph], dict[str, Any] | None]
    fqn_builder: Callable[[LineageNode], str]
    entry_id_builder: Callable[[LineageNode], str]
    description_builder: Callable[[LineageNode], str]
    requires_cluster_id: bool


def _kafka_fqn_from_node(node: LineageNode) -> str:
    return _kafka_fqn(node.cluster_id or "", node.qualified_name)


def _kafka_entry_id(node: LineageNode) -> str:
    # Existing format kept verbatim so the 6 already-registered topic entries
    # keep their identity across this refactor.
    return _entry_id(f"{node.cluster_id or ''}-{node.qualified_name}")


def _flink_fqn_from_node(node: LineageNode) -> str:
    return _flink_fqn(node.environment_id or "", node.qualified_name)


def _connector_fqn_from_node(node: LineageNode) -> str:
    return _connector_fqn(node.cluster_id or "", node.qualified_name)


def _ksqldb_fqn_from_node(node: LineageNode) -> str:
    return _ksqldb_fqn(node.cluster_id or "", node.qualified_name)


def _consumer_group_fqn_from_node(node: LineageNode) -> str:
    return _consumer_group_fqn(node.cluster_id or "", node.qualified_name)


_REGISTRY: dict[NodeType, NodeRegistration] = {
    NodeType.KAFKA_TOPIC: NodeRegistration(
        node_type=NodeType.KAFKA_TOPIC,
        entry_type_id=ENTRY_TYPE_ID,
        aspect_type_id=ASPECT_TYPE_ID,
        entry_type_display="Kafka Topic (LineageBridge)",
        entry_type_description=(
            "Kafka topic registered by LineageBridge for cross-system lineage display"
        ),
        platform="kafka",
        aspect_display="Confluent Schema",
        aspect_description="Schema fields from Confluent Schema Registry",
        aspect_template=_KAFKA_SCHEMA_TEMPLATE,
        aspect_data_builder=_kafka_schema_data,
        fqn_builder=_kafka_fqn_from_node,
        entry_id_builder=_kafka_entry_id,
        description_builder=lambda n: (
            n.attributes.get("description") or f"Kafka topic on cluster {n.cluster_id or '?'}"
        ),
        requires_cluster_id=True,
    ),
    NodeType.FLINK_JOB: NodeRegistration(
        node_type=NodeType.FLINK_JOB,
        entry_type_id="lineage-bridge-flink-job",
        aspect_type_id="lineage-bridge-flink",
        entry_type_display="Flink Job (LineageBridge)",
        entry_type_description="Confluent Cloud Flink SQL statement",
        platform="flink",
        aspect_display="Flink Job Metadata",
        aspect_description="SQL, phase, compute pool, principal for a Flink statement",
        aspect_template=_flat_string_template(
            "flink", ["sql", "phase", "compute_pool_id", "principal"]
        ),
        aspect_data_builder=_scalar_attr_data(("sql", "phase", "compute_pool_id", "principal")),
        fqn_builder=_flink_fqn_from_node,
        entry_id_builder=lambda n: _entry_id(f"flink-{n.environment_id or ''}-{n.qualified_name}"),
        description_builder=lambda n: (
            f"Flink SQL statement on environment {n.environment_id or '?'}"
        ),
        requires_cluster_id=False,
    ),
    NodeType.CONNECTOR: NodeRegistration(
        node_type=NodeType.CONNECTOR,
        entry_type_id="lineage-bridge-connector",
        aspect_type_id="lineage-bridge-connector",
        entry_type_display="Kafka Connector (LineageBridge)",
        entry_type_description="Kafka Connect source or sink connector",
        platform="kafka-connect",
        aspect_display="Connector Metadata",
        aspect_description="Connector class, direction, state, task count",
        aspect_template=_flat_string_template(
            "connector",
            ["connector_class", "direction", "state", "tasks_max", "confluent_id"],
        ),
        aspect_data_builder=_scalar_attr_data(
            ("connector_class", "direction", "state", "tasks_max", "confluent_id")
        ),
        fqn_builder=_connector_fqn_from_node,
        entry_id_builder=lambda n: _entry_id(f"connector-{n.cluster_id or ''}-{n.qualified_name}"),
        description_builder=lambda n: (
            f"{n.attributes.get('connector_class', 'Kafka')} "
            f"connector on cluster {n.cluster_id or '?'}"
        ),
        requires_cluster_id=True,
    ),
    NodeType.KSQLDB_QUERY: NodeRegistration(
        node_type=NodeType.KSQLDB_QUERY,
        entry_type_id="lineage-bridge-ksqldb-query",
        aspect_type_id="lineage-bridge-ksqldb",
        entry_type_display="ksqlDB Query (LineageBridge)",
        entry_type_description="ksqlDB persistent query",
        platform="ksqldb",
        aspect_display="ksqlDB Query Metadata",
        aspect_description="Query text, state, target topic for a ksqlDB persistent query",
        aspect_template=_flat_string_template("ksqldb", ["sql", "state", "target_topic"]),
        aspect_data_builder=_scalar_attr_data(("sql", "state", "target_topic")),
        fqn_builder=_ksqldb_fqn_from_node,
        entry_id_builder=lambda n: _entry_id(f"ksqldb-{n.cluster_id or ''}-{n.qualified_name}"),
        description_builder=lambda n: f"ksqlDB query on cluster {n.cluster_id or '?'}",
        requires_cluster_id=True,
    ),
    NodeType.CONSUMER_GROUP: NodeRegistration(
        node_type=NodeType.CONSUMER_GROUP,
        entry_type_id="lineage-bridge-consumer-group",
        aspect_type_id="lineage-bridge-consumer-group",
        entry_type_display="Kafka Consumer Group (LineageBridge)",
        entry_type_description="Kafka consumer group",
        platform="kafka",
        aspect_display="Consumer Group Metadata",
        aspect_description="State, simple flag, member count",
        aspect_template=_flat_string_template(
            "consumer_group", ["state", "is_simple", "member_count"]
        ),
        aspect_data_builder=_scalar_attr_data(("state", "is_simple", "member_count")),
        fqn_builder=_consumer_group_fqn_from_node,
        entry_id_builder=lambda n: _entry_id(
            f"consumer-group-{n.cluster_id or ''}-{n.qualified_name}"
        ),
        description_builder=lambda n: f"Kafka consumer group on cluster {n.cluster_id or '?'}",
        requires_cluster_id=True,
    ),
}


def _process_display_name(node: LineageNode) -> str:
    """Build the displayName Google's processOpenLineageRunEvent assigns to a process.

    The translator emits ``job.namespace = confluent://<env>/<cluster>`` and
    ``job.name = qualified_name``. Google joins them as ``<namespace>:<name>``.
    Used to match LineageProcess resources back to graph nodes for PATCH.
    """
    env = node.environment_id or "unknown"
    cluster = node.cluster_id or "default"
    return f"confluent://{env}/{cluster}:{node.qualified_name}"


class DataplexAssetRegistrar:
    """Upserts Dataplex Catalog entries for Confluent assets and patches Lineage processes."""

    def __init__(
        self,
        project_id: str,
        location: str,
        headers: dict[str, str],
        *,
        timeout: float = 30.0,
    ) -> None:
        self._project_id = project_id
        self._location = location
        self._headers = {**headers, "Content-Type": "application/json"}
        self._timeout = timeout

    @property
    def _parent(self) -> str:
        return f"projects/{self._project_id}/locations/{self._location}"

    @property
    def _entry_group(self) -> str:
        return f"{self._parent}/entryGroups/{ENTRY_GROUP_ID}"

    def _entry_type(self, reg: NodeRegistration) -> str:
        return f"{self._parent}/entryTypes/{reg.entry_type_id}"

    def _aspect_type_key(self, reg: NodeRegistration) -> str:
        return f"{self._project_id}.{self._location}.{reg.aspect_type_id}"

    def _aspect_type_resource(self, reg: NodeRegistration) -> str:
        return f"{self._parent}/aspectTypes/{reg.aspect_type_id}"

    # ── public API ──────────────────────────────────────────────────────

    async def register_confluent_assets(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> tuple[int, list[str]]:
        """Bootstrap catalog primitives, then upsert one entry per Confluent node.

        Returns ``(entries_upserted, errors)``. Failures are non-fatal — they
        only mean the BQ Lineage UI loses some metadata for the affected nodes.
        """
        # Group nodes by NodeType, filtering out anything we can't address
        # (missing cluster_id where required, missing qualified_name).
        per_type: dict[NodeType, list[LineageNode]] = {}
        for nt, reg in _REGISTRY.items():
            nodes = [
                n
                for n in graph.filter_by_type(nt)
                if n.qualified_name
                and (not reg.requires_cluster_id or n.cluster_id)
                and (reg.requires_cluster_id or n.environment_id)
            ]
            if nodes:
                per_type[nt] = nodes
        if not per_type:
            return 0, []

        active = [_REGISTRY[nt] for nt in per_type]

        async with httpx.AsyncClient(headers=self._headers, timeout=self._timeout) as client:
            errors: list[str] = []
            try:
                await self._ensure_bootstrap(client, active)
            except Exception as exc:
                msg = f"Dataplex bootstrap failed: {exc}"
                logger.warning(msg)
                return 0, [msg]

            total = sum(len(v) for v in per_type.values())
            if on_progress:
                on_progress(
                    "Catalog",
                    f"Registering {total} Confluent asset(s) across {len(per_type)} type(s)",
                )

            count = 0
            for nt, nodes in per_type.items():
                reg = _REGISTRY[nt]
                for node in nodes:
                    try:
                        await self._upsert_entry(client, reg, node, graph)
                        count += 1
                    except Exception as exc:
                        err = f"Failed to register {nt.value} {node.qualified_name}: {exc}"
                        logger.warning(err)
                        errors.append(err)
            return count, errors

    async def link_processes_to_catalog(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> tuple[int, list[str]]:
        """List Lineage processes, match by displayName to graph nodes, PATCH origin + attributes.

        For each transformation node (Flink/Connector/ksqlDB) in the graph that
        has a corresponding Lineage process (created by Google when we pushed
        OpenLineage events), set ``process.origin.name`` to the Catalog FQN
        and ``process.attributes`` to a small subset of the node's attributes.
        Result: clicking a process in the BQ Lineage UI shows inline metadata
        plus a navigation link to the Catalog entry with the full record.

        Returns ``(processes_patched, errors)``. Non-fatal.
        """
        # Build displayName → (registration, node) map for nodes that have
        # both a registration AND end up in OpenLineage events as a job
        # (Flink/Connector/ksqlDB — consumer groups don't appear as processes).
        targets: dict[str, tuple[NodeRegistration, LineageNode]] = {}
        for nt in (NodeType.FLINK_JOB, NodeType.CONNECTOR, NodeType.KSQLDB_QUERY):
            reg = _REGISTRY.get(nt)
            if not reg:
                continue
            for n in graph.filter_by_type(nt):
                if not n.qualified_name:
                    continue
                targets[_process_display_name(n)] = (reg, n)
        if not targets:
            return 0, []

        if on_progress:
            on_progress("Lineage", f"Linking {len(targets)} process(es) to Catalog entries")

        errors: list[str] = []
        patched = 0
        async with httpx.AsyncClient(headers=self._headers, timeout=self._timeout) as client:
            page_token = ""
            pages = 0
            while targets and pages < 50:
                pages += 1
                params = "pageSize=200" + (f"&pageToken={page_token}" if page_token else "")
                resp = await client.get(f"{_LINEAGE_API_BASE}/{self._parent}/processes?{params}")
                if resp.status_code != 200:
                    errors.append(f"processes.list failed: {resp.status_code} {resp.text[:200]}")
                    break
                payload = resp.json()
                for proc in payload.get("processes", []):
                    dname = proc.get("displayName", "")
                    match = targets.pop(dname, None)
                    if not match:
                        continue
                    reg, node = match
                    try:
                        await self._patch_process(client, proc["name"], reg, node, dname)
                        patched += 1
                    except Exception as exc:
                        errors.append(f"PATCH process for {node.qualified_name}: {exc}")
                page_token = payload.get("nextPageToken", "")
                if not page_token:
                    break

        if targets and on_progress:
            on_progress(
                "Lineage",
                f"{len(targets)} node(s) had no matching Lineage process — likely not in OL events",
            )
        return patched, errors

    # ── bootstrap (entry group + per-type entry/aspect types) ──────────

    async def _ensure_bootstrap(
        self, client: httpx.AsyncClient, active: list[NodeRegistration]
    ) -> None:
        # Entry group first (everything else is scoped under it), then all
        # entry types + aspect types in parallel — they're independent.
        await self._ensure_entry_group(client)
        await asyncio.gather(
            *[self._ensure_entry_type(client, reg) for reg in active],
            *[self._ensure_aspect_type(client, reg) for reg in active],
        )

    async def _ensure_entry_group(self, client: httpx.AsyncClient) -> None:
        if await self._exists(client, self._entry_group):
            return
        url = f"{_API_BASE}/{self._parent}/entryGroups?entryGroupId={ENTRY_GROUP_ID}"
        body = {
            "displayName": "LineageBridge",
            "description": (
                "Confluent stream assets registered by LineageBridge for Data Lineage display"
            ),
        }
        await self._create_lro(client, url, body, target_name=self._entry_group)

    async def _ensure_entry_type(self, client: httpx.AsyncClient, reg: NodeRegistration) -> None:
        target = self._entry_type(reg)
        if await self._exists(client, target):
            return
        url = f"{_API_BASE}/{self._parent}/entryTypes?entryTypeId={reg.entry_type_id}"
        body = {
            "displayName": reg.entry_type_display,
            "description": reg.entry_type_description,
            "platform": reg.platform,
            "system": "Confluent Cloud",
            "typeAliases": ["TABLE"] if reg.node_type == NodeType.KAFKA_TOPIC else ["RESOURCE"],
        }
        await self._create_lro(client, url, body, target_name=target)

    async def _ensure_aspect_type(self, client: httpx.AsyncClient, reg: NodeRegistration) -> None:
        target = self._aspect_type_resource(reg)
        if await self._exists(client, target):
            return
        url = f"{_API_BASE}/{self._parent}/aspectTypes?aspectTypeId={reg.aspect_type_id}"
        body = {
            "displayName": reg.aspect_display,
            "description": reg.aspect_description,
            "metadataTemplate": reg.aspect_template,
        }
        await self._create_lro(client, url, body, target_name=target)

    async def _exists(self, client: httpx.AsyncClient, resource_name: str) -> bool:
        resp = await client.get(f"{_API_BASE}/{resource_name}")
        return resp.status_code == 200

    async def _create_lro(
        self,
        client: httpx.AsyncClient,
        url: str,
        body: dict[str, Any],
        *,
        target_name: str,
        max_polls: int = 30,
    ) -> None:
        """POST a Dataplex resource and wait for the LRO to settle.

        Treats 409 (ALREADY_EXISTS) as success — covers a race against a
        concurrent push that bootstrapped first.
        """
        resp = await client.post(url, json=body)
        if resp.status_code == 409:
            return
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"create failed: {resp.status_code} {resp.text[:200]}")
        op_name = resp.json().get("name", "")
        if not op_name:
            return
        for _ in range(max_polls):
            await asyncio.sleep(2)
            poll = await client.get(f"{_API_BASE}/{op_name}")
            if poll.status_code != 200:
                continue
            data = poll.json()
            if data.get("done"):
                if "error" in data:
                    raise RuntimeError(f"LRO error: {data['error']}")
                return
        # Polling timed out — verify the resource exists anyway.
        if not await self._exists(client, target_name):
            raise RuntimeError(f"timed out waiting for {target_name}")

    # ── per-entry upsert ───────────────────────────────────────────────

    async def _upsert_entry(
        self,
        client: httpx.AsyncClient,
        reg: NodeRegistration,
        node: LineageNode,
        graph: LineageGraph,
    ) -> None:
        entry_id = reg.entry_id_builder(node)
        fqn = reg.fqn_builder(node)
        aspect_data = reg.aspect_data_builder(node, graph)
        aspect_key = self._aspect_type_key(reg)

        entry_body: dict[str, Any] = {
            "entryType": self._entry_type(reg),
            "fullyQualifiedName": fqn,
            "entrySource": {
                "system": "Confluent Cloud",
                "platform": reg.platform,
                "displayName": node.display_name or node.qualified_name,
                "description": reg.description_builder(node),
            },
        }
        if aspect_data:
            entry_body["aspects"] = {aspect_key: {"data": aspect_data}}

        # POST creates; 409 → fall back to PATCH.
        create_url = f"{_API_BASE}/{self._entry_group}/entries?entryId={entry_id}"
        resp = await client.post(create_url, json=entry_body)
        if resp.status_code == 200:
            return
        if resp.status_code == 409:
            entry_name = f"{self._entry_group}/entries/{entry_id}"
            update_mask = "entrySource"
            if aspect_data:
                update_mask += ",aspects"
                patch_url = (
                    f"{_API_BASE}/{entry_name}?updateMask={update_mask}&aspectKeys={aspect_key}"
                )
            else:
                patch_url = f"{_API_BASE}/{entry_name}?updateMask={update_mask}"
            patch_resp = await client.patch(patch_url, json=entry_body)
            if patch_resp.status_code != 200:
                raise RuntimeError(
                    f"PATCH failed: {patch_resp.status_code} {patch_resp.text[:200]}"
                )
            return
        raise RuntimeError(f"POST failed: {resp.status_code} {resp.text[:200]}")

    # ── lineage process patching ───────────────────────────────────────

    async def _patch_process(
        self,
        client: httpx.AsyncClient,
        process_resource: str,
        reg: NodeRegistration,
        node: LineageNode,
        display_name: str,
    ) -> None:
        """PATCH a single Lineage process with displayName + origin + attributes.

        Google's PATCH on Lineage Process clears unset top-level fields even
        with updateMask, so we always include displayName. PATCH also enforces
        a stricter charset on displayName than CREATE — only letters, digits,
        spaces, ``_-:&.`` are allowed, so the original Google-assigned
        ``confluent://env/cluster:name`` (containing ``/``) gets rejected.
        We rewrite to ``Confluent {node_type}: {qualified_name}`` which is
        safe AND more human-readable in the BQ Lineage UI.

        Side effect: after this PATCH, the linker can't rematch this process
        by displayName on subsequent runs. That's intentional — the process
        is already in the desired state, so re-patching is unnecessary.
        Future NEW processes (from fresh OL events) still get matched and
        patched on first sight.
        """
        attrs_dict = reg.aspect_data_builder(node, LineageGraph()) or {}
        safe_display = f"Confluent {reg.platform}: {node.qualified_name}"
        body: dict[str, Any] = {
            "displayName": safe_display,
            "origin": {
                "sourceType": "CUSTOM",
                "name": reg.fqn_builder(node),
            },
        }
        if attrs_dict:
            body["attributes"] = {k: str(v) for k, v in attrs_dict.items()}
        update_mask = "displayName,origin" + (",attributes" if attrs_dict else "")
        url = f"{_LINEAGE_API_BASE}/{process_resource}?updateMask={update_mask}"
        resp = await client.patch(url, json=body)
        if resp.status_code != 200:
            raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
