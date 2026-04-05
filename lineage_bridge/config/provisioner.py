# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Auto-provision Confluent Cloud API keys for LineageBridge.

Creates scoped API keys (and optionally a dedicated service account)
via the Confluent Cloud IAM APIs. Provisioned keys are cached locally
(encrypted) for reuse across sessions.

This module is intentionally decoupled from the extraction pipeline —
it only manages credentials, never touches lineage data.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from lineage_bridge.clients.base import ConfluentClient
from lineage_bridge.config.cache import load_cache, update_cache

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "lineage-bridge"
_SA_DESCRIPTION = "Auto-provisioned service account for LineageBridge extractor"
_KEY_DESCRIPTION = "Auto-provisioned by LineageBridge"

# How long to wait for a new API key to propagate (seconds).
_PROPAGATION_DELAY = 5.0


@dataclass
class ProvisionedKey:
    """A provisioned API key with its metadata."""

    key_id: str
    api_key: str
    api_secret: str
    display_name: str
    resource_id: str
    owner_id: str


class KeyProvisioner:
    """Manages service accounts and scoped API keys via Confluent Cloud APIs.

    Usage::

        provisioner = KeyProvisioner(cloud_client, prefix="lineage-bridge")

        # Optionally create/find a dedicated service account
        sa_id = await provisioner.ensure_service_account()

        # Provision a key scoped to a Schema Registry cluster
        key = await provisioner.provision_key(
            resource_id="lsrc-abc123",
            environment_id="env-xyz",
            scope_label="sr",
            owner_id=sa_id,  # or None to use the current user
        )

        # List all provisioned keys
        keys = await provisioner.list_provisioned_keys()

        # Revoke all provisioned keys
        await provisioner.revoke_all()
    """

    def __init__(
        self,
        cloud: ConfluentClient,
        *,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        self._cloud = cloud
        self._prefix = prefix

    # ── Service Account ──────────────────────────────────────────────

    async def ensure_service_account(self) -> str:
        """Find or create a service account named ``{prefix}-extractor``.

        Returns the service account ID (e.g. ``sa-abc123``).
        """
        sa_name = f"{self._prefix}-extractor"

        # Check if it already exists
        existing = await self._cloud.paginate(
            "/iam/v2/service-accounts",
        )
        for sa in existing:
            if sa.get("display_name") == sa_name:
                sa_id = sa.get("id", "")
                logger.info("Found existing service account: %s (%s)", sa_name, sa_id)
                return sa_id

        # Create it
        resp = await self._cloud.post(
            "/iam/v2/service-accounts",
            json_body={
                "display_name": sa_name,
                "description": _SA_DESCRIPTION,
            },
        )
        sa_id = resp.get("id", "")
        logger.info("Created service account: %s (%s)", sa_name, sa_id)
        return sa_id

    # ── Key Provisioning ─────────────────────────────────────────────

    async def provision_key(
        self,
        resource_id: str,
        environment_id: str,
        scope_label: str,
        *,
        owner_id: str | None = None,
    ) -> ProvisionedKey:
        """Create a scoped API key for a specific resource.

        Args:
            resource_id: The Confluent resource ID (e.g. ``lsrc-abc``, ``lkc-xyz``).
            environment_id: The environment the resource belongs to.
            scope_label: Human label for the scope (e.g. ``sr``, ``kafka``, ``flink``).
            owner_id: Service account ID. If None, the key is owned by the
                      authenticated user (cloud API key owner).

        Returns:
            The provisioned key with api_key and api_secret.
        """
        display_name = f"{self._prefix}-{scope_label}-{environment_id}"

        # Check cache first
        cached = self._get_cached_key(display_name)
        if cached:
            logger.info("Using cached provisioned key: %s", display_name)
            return cached

        # Build the request
        spec: dict[str, Any] = {
            "display_name": display_name,
            "description": _KEY_DESCRIPTION,
            "resource": {
                "id": resource_id,
                "environment": environment_id,
            },
        }
        if owner_id:
            spec["owner"] = {"id": owner_id}

        resp = await self._cloud.post(
            "/iam/v2/api-keys",
            json_body={"spec": spec},
        )

        key = ProvisionedKey(
            key_id=resp.get("id", ""),
            api_key=resp.get("spec", {}).get("secret", resp.get("id", "")),
            api_secret=resp.get("spec", {}).get("secret", ""),
            display_name=display_name,
            resource_id=resource_id,
            owner_id=owner_id or "",
        )

        # The API returns api_key in id and api_secret in spec.secret
        # for newly created keys
        key.api_key = resp.get("id", "")
        key.api_secret = resp.get("spec", {}).get("secret", "")

        # Cache it
        self._cache_key(key)

        logger.info(
            "Provisioned API key %s for %s (resource=%s)",
            key.key_id,
            display_name,
            resource_id,
        )

        # Wait for propagation
        await asyncio.sleep(_PROPAGATION_DELAY)

        return key

    async def provision_cluster_key(
        self,
        cluster_id: str,
        environment_id: str,
        *,
        owner_id: str | None = None,
    ) -> ProvisionedKey:
        """Provision a Kafka cluster-scoped API key."""
        return await self.provision_key(
            resource_id=cluster_id,
            environment_id=environment_id,
            scope_label="kafka",
            owner_id=owner_id,
        )

    async def provision_sr_key(
        self,
        sr_cluster_id: str,
        environment_id: str,
        *,
        owner_id: str | None = None,
    ) -> ProvisionedKey:
        """Provision a Schema Registry-scoped API key."""
        return await self.provision_key(
            resource_id=sr_cluster_id,
            environment_id=environment_id,
            scope_label="sr",
            owner_id=owner_id,
        )

    async def provision_flink_key(
        self,
        flink_pool_id: str,
        environment_id: str,
        *,
        owner_id: str | None = None,
    ) -> ProvisionedKey:
        """Provision a Flink compute pool-scoped API key."""
        return await self.provision_key(
            resource_id=flink_pool_id,
            environment_id=environment_id,
            scope_label="flink",
            owner_id=owner_id,
        )

    # ── Key Listing & Revocation ─────────────────────────────────────

    async def list_provisioned_keys(self) -> list[dict[str, Any]]:
        """List all API keys whose display_name starts with our prefix."""
        all_keys = await self._cloud.paginate("/iam/v2/api-keys")
        return [
            k
            for k in all_keys
            if k.get("spec", {})
            .get("display_name", "")
            .startswith(self._prefix)
        ]

    async def revoke_key(self, key_id: str) -> None:
        """Delete a single API key."""
        await self._cloud.delete(f"/iam/v2/api-keys/{key_id}")
        self._remove_cached_key(key_id)
        logger.info("Revoked API key: %s", key_id)

    async def revoke_all(self) -> None:
        """Revoke all API keys provisioned by LineageBridge."""
        keys = await self.list_provisioned_keys()
        for k in keys:
            key_id = k.get("id", "")
            if key_id:
                try:
                    await self.revoke_key(key_id)
                except Exception:
                    logger.warning("Failed to revoke key %s", key_id, exc_info=True)

        # Clear all provisioned keys from cache
        update_cache(provisioned_keys={})
        logger.info("Revoked %d provisioned API keys", len(keys))

    async def delete_service_account(self) -> None:
        """Delete the LineageBridge service account if it exists."""
        sa_name = f"{self._prefix}-extractor"
        existing = await self._cloud.paginate("/iam/v2/service-accounts")
        for sa in existing:
            if sa.get("display_name") == sa_name:
                sa_id = sa.get("id", "")
                await self._cloud.delete(f"/iam/v2/service-accounts/{sa_id}")
                logger.info("Deleted service account: %s (%s)", sa_name, sa_id)
                return

    # ── Cache helpers ────────────────────────────────────────────────

    @staticmethod
    def _get_cached_key(display_name: str) -> ProvisionedKey | None:
        """Look up a provisioned key by display_name in the local cache."""
        cache = load_cache()
        provisioned = cache.get("provisioned_keys", {})
        entry = provisioned.get(display_name)
        if not entry:
            return None
        return ProvisionedKey(
            key_id=entry["key_id"],
            api_key=entry["api_key"],
            api_secret=entry["api_secret"],
            display_name=display_name,
            resource_id=entry.get("resource_id", ""),
            owner_id=entry.get("owner_id", ""),
        )

    @staticmethod
    def _cache_key(key: ProvisionedKey) -> None:
        """Store a provisioned key in the local cache (encrypted)."""
        cache = load_cache()
        provisioned = cache.get("provisioned_keys", {})
        provisioned[key.display_name] = {
            "key_id": key.key_id,
            "api_key": key.api_key,
            "api_secret": key.api_secret,
            "resource_id": key.resource_id,
            "owner_id": key.owner_id,
        }
        update_cache(provisioned_keys=provisioned)

    @staticmethod
    def _remove_cached_key(key_id: str) -> None:
        """Remove a provisioned key from cache by key_id."""
        cache = load_cache()
        provisioned = cache.get("provisioned_keys", {})
        to_remove = [
            name
            for name, entry in provisioned.items()
            if entry.get("key_id") == key_id
        ]
        for name in to_remove:
            del provisioned[name]
        update_cache(provisioned_keys=provisioned)

    @staticmethod
    def get_all_cached_keys() -> dict[str, dict[str, str]]:
        """Return all cached provisioned keys {display_name -> {api_key, api_secret, ...}}."""
        cache = load_cache()
        return cache.get("provisioned_keys", {})
