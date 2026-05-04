# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Databricks External Lineage + External Metadata API client.

Wraps the public-preview ``/api/2.0/lineage-tracking/external-metadata`` and
``/api/2.0/lineage-tracking/external-lineage`` endpoints. Used by the UC
provider's native-lineage push to register Confluent topics as
``external_metadata`` objects (system_type=CONFLUENT) and wire them to
their downstream UC tables via ``external_lineage_relationship``.

The native lineage path is an alternative to the legacy TBLPROPERTIES +
bridge-table writer: relationships created here surface in Databricks'
own Lineage tab rather than being buried in table properties.

Permissions
~~~~~~~~~~~
``CREATE_EXTERNAL_METADATA`` on the metastore is required to create
external metadata objects. Without it the API returns 403 with
``User does not have CREATE EXTERNAL METADATA on Metastore '<name>'``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# Confluent system_type for Confluent Cloud / Confluent Platform topics.
# Use "KAFKA" for self-hosted clusters where the source isn't Confluent.
SYSTEM_TYPE_CONFLUENT = "CONFLUENT"
SYSTEM_TYPE_KAFKA = "KAFKA"

# entity_type for a Kafka topic mapped to a UC table. The Databricks API
# accepts free-form entity_type strings; "TABLE" is the convention because
# Tableflow materializes topics as Delta tables and the lineage tab renders
# table-shaped icons for it.
ENTITY_TYPE_TABLE = "TABLE"

_METADATA_PATH = "/api/2.0/lineage-tracking/external-metadata"
_LINEAGE_PATH = "/api/2.0/lineage-tracking/external-lineage"


@dataclass
class ExternalMetadataRef:
    """Reference to an external_metadata object — minimal fields the
    create-relationship API needs to identify the source/target."""

    name: str


@dataclass
class TableRef:
    """Reference to a UC table — full_name in catalog.schema.table form."""

    name: str


async def upsert_external_metadata(
    client: httpx.AsyncClient,
    *,
    name: str,
    system_type: str,
    entity_type: str = ENTITY_TYPE_TABLE,
    description: str | None = None,
    url: str | None = None,
    owner: str | None = None,
) -> dict[str, Any] | None:
    """Create or update an external_metadata object. Idempotent.

    Returns the API response body on success, ``None`` on permission
    denied (the caller can carry on without it). Re-creating with the
    same name is treated as a success — the API responds 409
    ``RESOURCE_ALREADY_EXISTS`` and we PATCH the URL/description so the
    object's metadata stays in sync with what the lineage walk found.
    """
    body = {
        "name": name,
        "system_type": system_type,
        "entity_type": entity_type,
    }
    if description:
        body["description"] = description
    if url:
        body["url"] = url
    if owner:
        body["owner"] = owner

    try:
        resp = await client.post(_METADATA_PATH, json=body)
    except httpx.HTTPError as exc:
        logger.debug("external_metadata create %s failed: %s", name, exc)
        return None

    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 409:
        # Already exists. PATCH the mutable fields so URL / description /
        # owner stay accurate when the upstream Confluent metadata changes.
        return await _patch_external_metadata(
            client, name=name, description=description, url=url, owner=owner
        )
    if resp.status_code in (401, 403):
        logger.warning(
            "external_metadata %s: permission denied (status %d). Grant "
            "CREATE_EXTERNAL_METADATA on the metastore to enable native "
            "lineage push.",
            name,
            resp.status_code,
        )
        return None
    logger.debug(
        "external_metadata create %s returned %d: %s",
        name,
        resp.status_code,
        resp.text[:200],
    )
    return None


async def _patch_external_metadata(
    client: httpx.AsyncClient,
    *,
    name: str,
    description: str | None,
    url: str | None,
    owner: str | None,
) -> dict[str, Any] | None:
    """PATCH the mutable fields. Used when create returned 409."""
    update: dict[str, Any] = {}
    if description is not None:
        update["description"] = description
    if url is not None:
        update["url"] = url
    if owner is not None:
        update["owner"] = owner
    if not update:
        # Nothing to update — fetch and return the existing object.
        return await get_external_metadata(client, name)

    try:
        resp = await client.patch(
            f"{_METADATA_PATH}/{quote(name, safe='')}",
            json=update,
        )
    except httpx.HTTPError as exc:
        logger.debug("external_metadata patch %s failed: %s", name, exc)
        return None
    if resp.status_code == 200:
        return resp.json()
    logger.debug(
        "external_metadata patch %s returned %d: %s",
        name,
        resp.status_code,
        resp.text[:200],
    )
    return None


async def get_external_metadata(client: httpx.AsyncClient, name: str) -> dict[str, Any] | None:
    """Fetch an external_metadata object by name. ``None`` on 404 / error."""
    try:
        resp = await client.get(f"{_METADATA_PATH}/{quote(name, safe='')}")
    except httpx.HTTPError:
        return None
    if resp.status_code == 200:
        return resp.json()
    return None


async def list_external_metadata(
    client: httpx.AsyncClient, *, page_size: int = 100
) -> list[dict[str, Any]]:
    """List external_metadata objects, paginating until exhausted."""
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params: dict[str, Any] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        try:
            resp = await client.get(_METADATA_PATH, params=params)
        except httpx.HTTPError:
            break
        if resp.status_code != 200:
            break
        data = resp.json()
        out.extend(data.get("external_metadata") or [])
        page_token = data.get("next_page_token") or None
        if not page_token:
            break
    return out


async def create_lineage_relationship(
    client: httpx.AsyncClient,
    *,
    source: ExternalMetadataRef | TableRef,
    target: ExternalMetadataRef | TableRef,
    properties: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Create an external_lineage_relationship between source and target.

    Either side can be an ``external_metadata`` reference (Kafka topic
    registered via ``upsert_external_metadata``) or a ``table`` reference
    (catalog.schema.table). The API is idempotent on the (source, target)
    pair — re-creating returns 409 which we treat as success.

    Returns the API response on success, ``None`` on permission denied or
    a 409 (already exists). Other failures are logged and the caller
    can decide whether to escalate.
    """
    body: dict[str, Any] = {
        "source": _ref_to_dict(source),
        "target": _ref_to_dict(target),
    }
    if properties:
        body["properties"] = properties

    try:
        resp = await client.post(_LINEAGE_PATH, json=body)
    except httpx.HTTPError as exc:
        logger.debug("external_lineage create failed: %s", exc)
        return None

    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 409:
        # Relationship already exists — that's a successful no-op for us.
        logger.debug("external_lineage relationship already exists; skipping")
        return None
    if resp.status_code in (401, 403):
        logger.warning(
            "external_lineage relationship: permission denied (status %d).",
            resp.status_code,
        )
        return None
    logger.debug(
        "external_lineage create returned %d: %s",
        resp.status_code,
        resp.text[:200],
    )
    return None


def _ref_to_dict(ref: ExternalMetadataRef | TableRef) -> dict[str, Any]:
    """Serialize a TableRef / ExternalMetadataRef into the oneof shape."""
    if isinstance(ref, ExternalMetadataRef):
        return {"external_metadata": {"name": ref.name}}
    if isinstance(ref, TableRef):
        return {"table": {"name": ref.name}}
    raise TypeError(f"Unsupported lineage reference type: {type(ref).__name__}")
