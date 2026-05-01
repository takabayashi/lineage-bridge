# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""AWS DataZone catalog provider — registers Kafka topics as DataZone assets
and pushes lineage as OpenLineage events.

Mirrors the Google Data Lineage / Dataplex Catalog pair:

* :class:`DataZoneAssetRegistrar` upserts custom assets in a DataZone domain
  with the same FQN that the lineage events reference, so the DataZone
  Catalog UI can show schema for upstream Kafka nodes.
* :class:`AWSDataZoneProvider.push_lineage` posts OpenLineage RunEvents via
  ``post_lineage_event``. The DataZone processor accepts the standard
  OpenLineage envelope and uses dataset namespaces for FQN linking — same
  contract as Google.

DataZone is a separate AWS service from Glue. We do **not** require Glue or
its provider to be configured: a user with only DataZone gets the lineage
view; a user with only Glue gets the rich chain in Glue Parameters; a user
with both gets both, independently.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Callable
from typing import Any

from lineage_bridge.api.openlineage.normalize import kafka_fqn, normalize_event
from lineage_bridge.models.graph import LineageGraph, LineageNode, NodeType, PushResult, SystemType

logger = logging.getLogger(__name__)

# Custom asset type we own. Created on first use, idempotent.
ASSET_TYPE_NAME = "LineageBridgeKafkaTopic"

# DataZone asset external IDs are alphanumeric/_/-; topic names with dots get sanitized.
_EXT_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")

# DataZone limits asset descriptions to 2048 chars; schema summary stays under that.
_DESC_MAX = 2000


def _ext_id(cluster_id: str, topic: str) -> str:
    """Deterministic DataZone-legal ID derived from cluster + topic."""
    raw = f"{cluster_id}-{topic}"
    return _EXT_ID_RE.sub("_", raw).lower()[:200]


class DataZoneAssetRegistrar:
    """Upserts DataZone assets for Kafka topics so Catalog UI shows schema."""

    def __init__(
        self,
        domain_id: str,
        project_id: str,
        region: str,
    ) -> None:
        self._domain_id = domain_id
        self._project_id = project_id
        self._region = region
        self._client = None  # lazy boto3 init so import-time stays cheap

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3

            self._client = boto3.client("datazone", region_name=self._region)
        return self._client

    async def register_kafka_assets(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> tuple[int, list[str]]:
        """Upsert one DataZone asset per Kafka topic. Returns (count, errors)."""
        topics = [
            n
            for n in graph.filter_by_type(NodeType.KAFKA_TOPIC)
            if n.cluster_id and n.qualified_name
        ]
        if not topics:
            return 0, []

        errors: list[str] = []
        try:
            asset_type_id = await asyncio.to_thread(self._ensure_asset_type)
        except Exception as exc:
            msg = f"DataZone asset-type bootstrap failed: {exc}"
            logger.warning(msg)
            return 0, [msg]

        if on_progress:
            on_progress("Catalog", f"Registering {len(topics)} Kafka topic(s) in DataZone")

        count = 0
        for node in topics:
            try:
                await asyncio.to_thread(self._upsert_asset, asset_type_id, node, graph)
                count += 1
            except Exception as exc:
                err = f"Failed to register {node.qualified_name}: {exc}"
                logger.warning(err)
                errors.append(err)
        return count, errors

    # ── asset type bootstrap ────────────────────────────────────────────

    def _ensure_asset_type(self) -> str:
        """Get-or-create a custom asset type with a minimal schema form.

        Returns the type identifier (name) usable in ``create_asset``.
        """
        try:
            self.client.get_asset_type(domainIdentifier=self._domain_id, identifier=ASSET_TYPE_NAME)
            return ASSET_TYPE_NAME
        except Exception as exc:
            err_name = repr(type(exc).__name__)
            if "ResourceNotFoundException" not in err_name and "not found" not in str(exc).lower():
                # Re-raise unexpected errors (auth, throttling, etc.).
                raise

        # Minimal Smithy form — one string field carries a JSON schema dump.
        smithy = (
            '$version: "2"\n'
            "namespace lineage_bridge\n\n"
            "structure KafkaSchemaForm {\n"
            '    @documentation("Schema fields (JSON), sourced from Confluent Schema Registry")\n'
            "    fieldsJson: String\n"
            '    @documentation("Cluster ID (lkc-XXXXXX)")\n'
            "    clusterId: String\n"
            '    @documentation("Confluent environment ID")\n'
            "    environmentId: String\n"
            "}\n"
        )
        try:
            self.client.create_asset_type(
                domainIdentifier=self._domain_id,
                name=ASSET_TYPE_NAME,
                description="Kafka topics registered by LineageBridge for cross-system lineage",
                owningProjectIdentifier=self._project_id,
                formsInput={"KafkaSchemaForm": {"smithyModel": smithy, "required": False}},
            )
        except Exception as exc:
            # ConflictException → another push won the race; safe to proceed.
            if "ConflictException" not in repr(type(exc).__name__):
                raise
        return ASSET_TYPE_NAME

    # ── per-topic upsert ────────────────────────────────────────────────

    def _upsert_asset(self, asset_type_id: str, topic: LineageNode, graph: LineageGraph) -> None:
        from lineage_bridge.catalogs.upstream_chain import _schema_fields  # reuse same lookup

        cluster_id = topic.cluster_id or ""
        topic_name = topic.qualified_name
        external_id = kafka_fqn(cluster_id, topic_name)
        fields = _schema_fields(graph, topic)
        fields_json = json.dumps(fields) if fields else ""

        description = self._build_description(topic, fields)
        forms_input = [
            {
                "formName": "KafkaSchemaForm",
                "typeIdentifier": "KafkaSchemaForm",
                "content": json.dumps(
                    {
                        "fieldsJson": fields_json,
                        "clusterId": cluster_id,
                        "environmentId": topic.environment_id or "",
                    }
                ),
            }
        ]

        try:
            self.client.create_asset(
                domainIdentifier=self._domain_id,
                name=topic.display_name or topic_name,
                description=description,
                externalIdentifier=external_id,
                typeIdentifier=asset_type_id,
                owningProjectIdentifier=self._project_id,
                formsInput=forms_input,
            )
        except Exception as exc:
            if "ConflictException" not in repr(type(exc).__name__):
                raise
            # Asset exists — DataZone needs the existing identifier to revise.
            existing = self._find_asset(external_id)
            if not existing:
                raise
            self.client.create_asset_revision(
                domainIdentifier=self._domain_id,
                identifier=existing,
                name=topic.display_name or topic_name,
                description=description,
                formsInput=forms_input,
            )

    def _find_asset(self, external_id: str) -> str | None:
        """Search for an existing asset by external identifier."""
        try:
            resp = self.client.search(
                domainIdentifier=self._domain_id,
                searchScope="ASSET",
                searchText=external_id,
                maxResults=10,
            )
        except Exception:
            return None
        for item in resp.get("items", []):
            asset = item.get("assetItem", {})
            if asset.get("externalIdentifier") == external_id:
                return asset.get("identifier")
        return None

    @staticmethod
    def _build_description(topic: LineageNode, fields: list[dict[str, str]]) -> str:
        parts = [
            f"Kafka topic on {topic.cluster_id or 'unknown cluster'}.",
            f"Environment: {topic.environment_id or '-'}.",
        ]
        if fields:
            parts.append("")
            parts.append("Schema fields:")
            for f in fields:
                row = f"  - {f.get('name', '?')}: {f.get('type', '?')}"
                if f.get("description"):
                    row += f" — {f['description']}"
                parts.append(row)
        out = "\n".join(parts)
        return out[:_DESC_MAX]


class AWSDataZoneProvider:
    """CatalogProvider-style facade combining asset registration + lineage push.

    Doesn't implement the full ``CatalogProvider`` protocol (no ``build_node`` /
    ``enrich`` because DataZone doesn't model Confluent stream nodes itself);
    only ``push_lineage`` is wired into the orchestrator.
    """

    catalog_type: str = "AWS_DATAZONE"
    system_type: SystemType = SystemType.AWS

    def __init__(
        self,
        domain_id: str | None = None,
        project_id: str | None = None,
        region: str = "us-east-1",
    ) -> None:
        self._domain_id = domain_id
        self._project_id = project_id
        self._region = region

    async def push_lineage(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> PushResult:
        """Register Kafka assets in DataZone, then post OpenLineage events."""
        if not self._domain_id or not self._project_id:
            return PushResult(errors=["DataZone domain_id / project_id not configured"])

        result = PushResult()

        # 1. Asset registration (non-fatal failures get logged into errors).
        try:
            registrar = DataZoneAssetRegistrar(self._domain_id, self._project_id, self._region)
            registered, reg_errors = await registrar.register_kafka_assets(
                graph, on_progress=on_progress
            )
            if registered and on_progress:
                on_progress("Catalog", f"Registered {registered} DataZone assets")
            result.errors.extend(reg_errors)
        except Exception as exc:
            logger.warning("DataZone asset registration skipped: %s", exc, exc_info=True)
            result.errors.append(f"DataZone asset registration skipped: {exc}")

        # 2. OpenLineage events (the main lineage path).
        try:
            count = await self._post_events(graph, on_progress=on_progress)
            result.tables_updated = count
        except Exception as exc:
            logger.warning("DataZone lineage push failed: %s", exc, exc_info=True)
            result.errors.append(f"Lineage push failed: {exc}")

        return result

    async def _post_events(
        self,
        graph: LineageGraph,
        *,
        on_progress: Callable[[str, str], None] | None = None,
    ) -> int:
        from lineage_bridge.api.openlineage.translator import graph_to_events

        events = graph_to_events(graph)
        # DataZone accepts the same allowlist as Google for Kafka inputs; for
        # outputs we accept BigQuery FQN (cross-cloud lineage works) and AWS
        # namespaces for Glue tables.
        for ev in events:
            normalize_event(ev, allow={"bigquery", "aws"})
        events = [e for e in events if e.inputs or e.outputs]

        if not events:
            return 0

        if on_progress:
            on_progress("Push", f"Posting {len(events)} lineage event(s) to DataZone")

        import boto3

        client = boto3.client("datazone", region_name=self._region)
        count = 0
        for ev in events:
            payload = json.dumps(ev.model_dump(mode="json", exclude_none=True))
            try:
                await asyncio.to_thread(
                    client.post_lineage_event,
                    domainIdentifier=self._domain_id,
                    event=payload.encode("utf-8"),
                )
                count += 1
            except Exception as exc:
                logger.warning("DataZone post_lineage_event failed: %s", exc)
        return count
