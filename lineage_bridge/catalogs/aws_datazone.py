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

from lineage_bridge.models.graph import LineageGraph, LineageNode, NodeType, PushResult
from lineage_bridge.openlineage.normalize import kafka_fqn, normalize_event

logger = logging.getLogger(__name__)

# Custom asset type we own. Created on first use, idempotent.
ASSET_TYPE_NAME = "LineageBridgeKafkaTopic"
FORM_TYPE_NAME = "LineageBridgeKafkaSchemaForm"

# DataZone asset external IDs are alphanumeric/_/-; topic names with dots get sanitized.
_EXT_ID_RE = re.compile(r"[^a-zA-Z0-9._-]")

# DataZone limits asset descriptions to 2048 chars; schema summary stays under that.
_DESC_MAX = 2000


def _ext_id(cluster_id: str, topic: str) -> str:
    """Deterministic DataZone-legal ID derived from cluster + topic."""
    raw = f"{cluster_id}-{topic}"
    return _EXT_ID_RE.sub("_", raw).lower()[:200]


class _AssetBootstrapSkippedError(Exception):
    """Raised when bootstrapping the FormType / AssetType is blocked by IAM.

    This is recoverable: the caller falls back to lineage-events-only mode and
    surfaces an info message so the user knows schema-on-asset display is
    missing but the lineage view still works.
    """


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
        except _AssetBootstrapSkippedError as exc:
            # IAM-level skip: lineage events still post; only schema-on-asset
            # display is missing. Surface as info, not error.
            if on_progress:
                on_progress(
                    "Catalog",
                    f"DataZone asset registration skipped — {exc}. "
                    "Lineage events still post normally.",
                )
            return 0, []
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
        """Idempotently bootstrap the FormType + AssetType pair.

        DataZone V2 needs three things to register a custom asset:
        1. A FormType (Smithy model) describing the schema-payload shape.
        2. An AssetType referencing the form by name + revision.
        3. The asset itself, with a `formsInput` entry matching the form name.

        Returns the asset-type name on success.

        Raises ``_AssetBootstrapSkippedError`` when the caller's IAM lacks
        ``datazone:CreateFormType`` / ``CreateAssetType`` — a recoverable
        condition: lineage events still post; only schema-on-asset display
        is lost. The caller should surface this as info, not error.
        """
        # Fast path: asset type already exists, nothing to bootstrap.
        try:
            self.client.get_asset_type(domainIdentifier=self._domain_id, identifier=ASSET_TYPE_NAME)
            return ASSET_TYPE_NAME
        except Exception as exc:
            err_name = type(exc).__name__
            if "ResourceNotFoundException" not in err_name and "not found" not in str(exc).lower():
                raise

        # Step 1: ensure form type exists (creates if missing).
        form_revision = self._ensure_form_type()

        # Step 2: create the asset type referencing the form by name+revision.
        try:
            self.client.create_asset_type(
                domainIdentifier=self._domain_id,
                name=ASSET_TYPE_NAME,
                description="Kafka topics registered by LineageBridge for cross-system lineage",
                owningProjectIdentifier=self._project_id,
                formsInput={
                    FORM_TYPE_NAME: {
                        "typeIdentifier": FORM_TYPE_NAME,
                        "typeRevision": form_revision,
                        "required": False,
                    },
                },
            )
        except Exception as exc:
            err_name = type(exc).__name__
            if "ConflictException" in err_name:
                # Another push won the race; safe to proceed.
                pass
            elif "AccessDeniedException" in err_name:
                raise _AssetBootstrapSkippedError(
                    "your IAM lacks datazone:CreateAssetType "
                    f"(needed once to register the {ASSET_TYPE_NAME} type)"
                ) from exc
            else:
                raise
        return ASSET_TYPE_NAME

    def _ensure_form_type(self) -> str:
        """Get-or-create the form type. Returns its revision string."""
        try:
            existing = self.client.get_form_type(
                domainIdentifier=self._domain_id, formTypeIdentifier=FORM_TYPE_NAME
            )
            return existing.get("revision", "1")
        except Exception as exc:
            err_name = type(exc).__name__
            if "ResourceNotFoundException" not in err_name and "not found" not in str(exc).lower():
                raise

        # One Smithy structure: JSON schema string + cluster + env IDs (all
        # @amazon.datazone#searchable so the DataZone search picks them up).
        smithy = (
            '$version: "2"\n'
            "namespace lineage_bridge\n\n"
            "structure " + FORM_TYPE_NAME + " {\n"
            "    @amazon.datazone#searchable\n"
            "    fieldsJson: String\n"
            "    @amazon.datazone#searchable\n"
            "    clusterId: String\n"
            "    environmentId: String\n"
            "}\n"
        )
        try:
            r = self.client.create_form_type(
                domainIdentifier=self._domain_id,
                name=FORM_TYPE_NAME,
                model={"smithy": smithy},
                owningProjectIdentifier=self._project_id,
                status="ENABLED",
                description="Schema fields from Confluent Schema Registry",
            )
            return r.get("revision", "1")
        except Exception as exc:
            err_name = type(exc).__name__
            if "AccessDeniedException" in err_name:
                raise _AssetBootstrapSkippedError(
                    "your IAM lacks datazone:CreateFormType "
                    f"(needed once to define the {FORM_TYPE_NAME} schema)"
                ) from exc
            raise

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
                "formName": FORM_TYPE_NAME,
                "typeIdentifier": FORM_TYPE_NAME,
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

    DataZone is push-only — it doesn't model Confluent stream nodes itself, so
    ``build_node`` / ``enrich`` / ``build_url`` are no-op stubs that conform
    to the Protocol structurally without claiming behaviour they don't have.
    Only ``push_lineage`` is wired into the orchestrator.
    """

    catalog_type: str = "AWS_DATAZONE"

    def __init__(
        self,
        domain_id: str | None = None,
        project_id: str | None = None,
        region: str = "us-east-1",
    ) -> None:
        self._domain_id = domain_id
        self._project_id = project_id
        self._region = region

    def build_node(self, *args: Any, **kwargs: Any) -> tuple[LineageNode, Any]:
        """Not supported — DataZone has no materialization origin we can model."""
        raise NotImplementedError(
            "AWSDataZoneProvider is push-only; it does not produce catalog nodes."
        )

    async def enrich(self, graph: LineageGraph) -> None:
        """No-op — DataZone has no nodes in the graph to enrich."""
        return None

    def build_url(self, node: LineageNode) -> str | None:
        """No-op — DataZone has no nodes in the graph to deeplink."""
        return None

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
        from lineage_bridge.openlineage.translator import graph_to_events

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
            # by_alias=True so Pydantic's `producer` / `schema_url` fields serialize
            # as `_producer` / `_schemaURL` per the OpenLineage spec — DataZone
            # rejects events that omit these (Google silently dropped them).
            payload = json.dumps(ev.model_dump(mode="json", exclude_none=True, by_alias=True))
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
