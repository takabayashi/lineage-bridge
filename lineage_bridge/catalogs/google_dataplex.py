# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Register Kafka topics in Google Dataplex Catalog so the BigQuery Lineage tab
can show schema metadata on Confluent-side nodes.

Google's ``processOpenLineageRunEvent`` API stores only the lineage link FQNs
— it discards every facet (schema, columnLineage, custom). For a Kafka node
in the Lineage tab to display its column list, that node's FQN must match a
registered Dataplex Catalog entry that carries a schema aspect.

This module owns the bootstrap (entry group + custom entry/aspect types) and
the per-topic upsert. Called from :class:`GoogleLineageProvider.push_lineage`
before the OpenLineage events go out, so by the time Google indexes the
lineage links, the Catalog already has matching entries with schema.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any

import httpx

from lineage_bridge.models.graph import EdgeType, LineageGraph, LineageNode, NodeType

logger = logging.getLogger(__name__)

_API_BASE = "https://dataplex.googleapis.com/v1"

# Names of the catalog primitives we own. Created on first use, reused across
# pushes — they're cheap and idempotent.
ENTRY_GROUP_ID = "lineage-bridge"
ENTRY_TYPE_ID = "lineage-bridge-kafka-topic"
ASPECT_TYPE_ID = "lineage-bridge-schema"

# Aspect type metadata template — defines the shape of the schema payload we
# attach to each Kafka entry. ``index`` is required by Dataplex for ordering.
_ASPECT_TEMPLATE: dict[str, Any] = {
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

# Entry IDs in Dataplex must be ASCII alphanumeric + "-" / "_" / "."; topic
# names with other chars get sanitized.
_ENTRY_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")


def _kafka_fqn(cluster_id: str, topic: str) -> str:
    """Backwards-compat shim — delegates to the shared FQN helper.

    Kept here to preserve the import path that test_google_dataplex.py uses.
    The single source of truth lives in ``api.openlineage.normalize``.
    """
    from lineage_bridge.openlineage.normalize import kafka_fqn

    return kafka_fqn(cluster_id, topic)


def _entry_id(cluster_id: str, topic: str) -> str:
    """Deterministic, Dataplex-legal entry ID derived from cluster + topic."""
    raw = f"{cluster_id}-{topic}"
    sanitized = _ENTRY_ID_RE.sub("_", raw).lower()
    # Dataplex caps entry IDs at 256 chars; keep well under.
    return sanitized[:200]


class DataplexAssetRegistrar:
    """Upserts Dataplex Catalog entries for Kafka topics in a lineage graph."""

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

    @property
    def _entry_type(self) -> str:
        return f"{self._parent}/entryTypes/{ENTRY_TYPE_ID}"

    @property
    def _aspect_type_key(self) -> str:
        return f"{self._project_id}.{self._location}.{ASPECT_TYPE_ID}"

    # ── public API ──────────────────────────────────────────────────────

    async def register_kafka_assets(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> tuple[int, list[str]]:
        """Bootstrap catalog primitives, then upsert one entry per Kafka topic.

        Returns ``(entries_upserted, errors)``. Failures are non-fatal — they
        only mean the BQ Lineage tab won't show schema for the affected nodes.
        """
        topics = self._kafka_topics(graph)
        if not topics:
            return 0, []

        async with httpx.AsyncClient(headers=self._headers, timeout=self._timeout) as client:
            errors: list[str] = []
            try:
                await self._ensure_bootstrap(client)
            except Exception as exc:
                msg = f"Dataplex bootstrap failed: {exc}"
                logger.warning(msg)
                return 0, [msg]

            if on_progress:
                on_progress("Catalog", f"Registering {len(topics)} Kafka topic(s) in Dataplex")

            count = 0
            for node in topics:
                try:
                    await self._upsert_entry(client, node, graph)
                    count += 1
                except Exception as exc:
                    err = f"Failed to register {node.qualified_name}: {exc}"
                    logger.warning(err)
                    errors.append(err)
            return count, errors

    # ── bootstrap (entry group + entry type + aspect type) ──────────────

    async def _ensure_bootstrap(self, client: httpx.AsyncClient) -> None:
        await asyncio.gather(
            self._ensure_entry_group(client),
            self._ensure_entry_type(client),
            self._ensure_aspect_type(client),
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

    async def _ensure_entry_type(self, client: httpx.AsyncClient) -> None:
        if await self._exists(client, self._entry_type):
            return
        url = f"{_API_BASE}/{self._parent}/entryTypes?entryTypeId={ENTRY_TYPE_ID}"
        body = {
            "displayName": "Kafka Topic (LineageBridge)",
            "description": (
                "Kafka topic registered by LineageBridge for cross-system lineage display"
            ),
            "platform": "kafka",
            "system": "Confluent Cloud",
            "typeAliases": ["TABLE"],
        }
        await self._create_lro(client, url, body, target_name=self._entry_type)

    async def _ensure_aspect_type(self, client: httpx.AsyncClient) -> None:
        aspect_type_name = f"{self._parent}/aspectTypes/{ASPECT_TYPE_ID}"
        if await self._exists(client, aspect_type_name):
            return
        url = f"{_API_BASE}/{self._parent}/aspectTypes?aspectTypeId={ASPECT_TYPE_ID}"
        body = {
            "displayName": "Confluent Schema",
            "description": "Schema fields from Confluent Schema Registry",
            "metadataTemplate": _ASPECT_TEMPLATE,
        }
        await self._create_lro(client, url, body, target_name=aspect_type_name)

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
        # Fall through if polling timed out — verify the resource exists anyway.
        if not await self._exists(client, target_name):
            raise RuntimeError(f"timed out waiting for {target_name}")

    # ── per-topic upsert ────────────────────────────────────────────────

    @staticmethod
    def _kafka_topics(graph: LineageGraph) -> list[LineageNode]:
        return [
            n
            for n in graph.filter_by_type(NodeType.KAFKA_TOPIC)
            if n.cluster_id and n.qualified_name
        ]

    @staticmethod
    def _schema_fields(graph: LineageGraph, topic: LineageNode) -> list[dict[str, str]]:
        """Pull schema fields from a HAS_SCHEMA-linked SCHEMA node, if any."""
        for edge in graph.edges:
            if edge.src_id != topic.node_id or edge.edge_type != EdgeType.HAS_SCHEMA:
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
                return out
        return []

    async def _upsert_entry(
        self,
        client: httpx.AsyncClient,
        topic: LineageNode,
        graph: LineageGraph,
    ) -> None:
        cluster_id = topic.cluster_id or ""
        topic_name = topic.qualified_name
        entry_id = _entry_id(cluster_id, topic_name)
        fqn = _kafka_fqn(cluster_id, topic_name)
        fields = self._schema_fields(graph, topic)

        entry_body: dict[str, Any] = {
            "entryType": self._entry_type,
            "fullyQualifiedName": fqn,
            "entrySource": {
                "system": "Confluent Cloud",
                "platform": "kafka",
                "displayName": topic.display_name or topic_name,
                "description": (
                    topic.attributes.get("description") or f"Kafka topic on cluster {cluster_id}"
                ),
            },
        }
        if fields:
            entry_body["aspects"] = {
                self._aspect_type_key: {"data": {"fields": fields}},
            }

        # POST creates; if 409 fall back to PATCH.
        create_url = f"{_API_BASE}/{self._entry_group}/entries?entryId={entry_id}"
        resp = await client.post(create_url, json=entry_body)
        if resp.status_code == 200:
            return
        if resp.status_code == 409:
            entry_name = f"{self._entry_group}/entries/{entry_id}"
            update_mask = "entrySource"
            if fields:
                update_mask += ",aspects"
                patch_url = (
                    f"{_API_BASE}/{entry_name}"
                    f"?updateMask={update_mask}&aspectKeys={self._aspect_type_key}"
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
