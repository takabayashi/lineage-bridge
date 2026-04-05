# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.config.provisioner.

Tests the KeyProvisioner class: service account creation, API key
provisioning (with cache hit/miss), revocation, and error handling.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import lineage_bridge.config.cache as cache_mod
from lineage_bridge.config.provisioner import (
    DEFAULT_PREFIX,
    KeyProvisioner,
)


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """Redirect cache to temp directory so tests don't touch real files."""
    cache_dir = tmp_path / ".lineage_bridge"
    cache_file = cache_dir / "cache.json"
    key_file = cache_dir / ".cache_key"
    monkeypatch.setattr(cache_mod, "_CACHE_DIR", cache_dir)
    monkeypatch.setattr(cache_mod, "_CACHE_FILE", cache_file)
    monkeypatch.setattr(cache_mod, "_KEY_FILE", key_file)


@pytest.fixture()
def mock_cloud():
    """Return a mock ConfluentClient."""
    cloud = AsyncMock()
    cloud.paginate = AsyncMock(return_value=[])
    cloud.post = AsyncMock(return_value={})
    cloud.get = AsyncMock(return_value={})
    cloud.delete = AsyncMock()
    return cloud


@pytest.fixture()
def provisioner(mock_cloud):
    return KeyProvisioner(mock_cloud, prefix="test-lb")


@pytest.fixture()
def no_sleep(monkeypatch):
    """Patch asyncio.sleep to return immediately."""
    monkeypatch.setattr("asyncio.sleep", AsyncMock())


# ── Constructor ─────────────────────────────────────────────────────────


def test_constructor_sets_prefix(mock_cloud):
    """KeyProvisioner stores the prefix."""
    p = KeyProvisioner(mock_cloud, prefix="custom-prefix")
    assert p._prefix == "custom-prefix"


def test_constructor_default_prefix(mock_cloud):
    """KeyProvisioner uses DEFAULT_PREFIX when none specified."""
    p = KeyProvisioner(mock_cloud)
    assert p._prefix == DEFAULT_PREFIX


# ── Service Account ────────────────────────────────────────────────────


async def test_ensure_service_account_creates_new(
    provisioner, mock_cloud
):
    """ensure_service_account creates a new SA when none exists."""
    mock_cloud.paginate.return_value = []
    mock_cloud.post.return_value = {"id": "sa-new123"}

    sa_id = await provisioner.ensure_service_account()

    assert sa_id == "sa-new123"
    mock_cloud.post.assert_awaited_once()
    call_kwargs = mock_cloud.post.call_args
    body = call_kwargs[1]["json_body"]
    assert body["display_name"] == "test-lb-extractor"


async def test_ensure_service_account_finds_existing(
    provisioner, mock_cloud
):
    """ensure_service_account returns existing SA if name matches."""
    mock_cloud.paginate.return_value = [
        {"id": "sa-existing", "display_name": "test-lb-extractor"},
        {"id": "sa-other", "display_name": "something-else"},
    ]

    sa_id = await provisioner.ensure_service_account()

    assert sa_id == "sa-existing"
    mock_cloud.post.assert_not_awaited()


# ── Key Provisioning ───────────────────────────────────────────────────


async def test_provision_key_cache_miss_calls_api(
    provisioner, mock_cloud, no_sleep
):
    """provision_key calls the API when there is no cached key."""
    mock_cloud.post.return_value = {
        "id": "api-key-123",
        "spec": {"secret": "super-secret"},
    }

    key = await provisioner.provision_key(
        resource_id="lsrc-abc",
        environment_id="env-1",
        scope_label="sr",
    )

    assert key.api_key == "api-key-123"
    assert key.api_secret == "super-secret"
    assert key.display_name == "test-lb-sr-env-1"
    mock_cloud.post.assert_awaited_once()


async def test_provision_key_cache_hit_skips_api(
    provisioner, mock_cloud, no_sleep
):
    """provision_key returns cached key without calling API."""
    # Pre-populate cache
    from lineage_bridge.config.cache import update_cache

    update_cache(
        provisioned_keys={
            "test-lb-sr-env-1": {
                "key_id": "cached-key-id",
                "api_key": "cached-key",
                "api_secret": "cached-secret",
                "resource_id": "lsrc-abc",
                "owner_id": "",
            }
        }
    )

    key = await provisioner.provision_key(
        resource_id="lsrc-abc",
        environment_id="env-1",
        scope_label="sr",
    )

    assert key.api_key == "cached-key"
    assert key.api_secret == "cached-secret"
    mock_cloud.post.assert_not_awaited()


async def test_provision_key_with_owner(
    provisioner, mock_cloud, no_sleep
):
    """provision_key includes owner when owner_id is provided."""
    mock_cloud.post.return_value = {
        "id": "key-owned",
        "spec": {"secret": "s"},
    }

    key = await provisioner.provision_key(
        resource_id="lkc-xyz",
        environment_id="env-2",
        scope_label="kafka",
        owner_id="sa-owner",
    )

    call_body = mock_cloud.post.call_args[1]["json_body"]
    assert call_body["spec"]["owner"] == {"id": "sa-owner"}
    assert key.owner_id == "sa-owner"


async def test_provision_key_cached_after_creation(
    provisioner, mock_cloud, no_sleep
):
    """Newly provisioned key is saved to cache for future reuse."""
    mock_cloud.post.return_value = {
        "id": "new-key",
        "spec": {"secret": "new-secret"},
    }

    await provisioner.provision_key(
        resource_id="lsrc-abc",
        environment_id="env-1",
        scope_label="sr",
    )

    # Second call should hit cache
    key2 = await provisioner.provision_key(
        resource_id="lsrc-abc",
        environment_id="env-1",
        scope_label="sr",
    )
    assert key2.api_key == "new-key"
    # API should only have been called once
    assert mock_cloud.post.await_count == 1


async def test_provision_cluster_key(
    provisioner, mock_cloud, no_sleep
):
    """provision_cluster_key delegates to provision_key with scope kafka."""
    mock_cloud.post.return_value = {
        "id": "k1",
        "spec": {"secret": "s1"},
    }

    key = await provisioner.provision_cluster_key(
        cluster_id="lkc-1",
        environment_id="env-1",
    )

    assert key.display_name == "test-lb-kafka-env-1"


async def test_provision_sr_key(
    provisioner, mock_cloud, no_sleep
):
    """provision_sr_key delegates with scope sr."""
    mock_cloud.post.return_value = {
        "id": "k2",
        "spec": {"secret": "s2"},
    }

    key = await provisioner.provision_sr_key(
        sr_cluster_id="lsrc-1",
        environment_id="env-1",
    )

    assert key.display_name == "test-lb-sr-env-1"


async def test_provision_flink_key(
    provisioner, mock_cloud, no_sleep
):
    """provision_flink_key delegates with scope flink."""
    mock_cloud.post.return_value = {
        "id": "k3",
        "spec": {"secret": "s3"},
    }

    key = await provisioner.provision_flink_key(
        flink_pool_id="lfcp-1",
        environment_id="env-1",
    )

    assert key.display_name == "test-lb-flink-env-1"


# ── Key Listing ─────────────────────────────────────────────────────────


async def test_list_provisioned_keys_filters_by_prefix(
    provisioner, mock_cloud
):
    """list_provisioned_keys only returns keys matching prefix."""
    mock_cloud.paginate.return_value = [
        {"id": "k1", "spec": {"display_name": "test-lb-sr-env-1"}},
        {"id": "k2", "spec": {"display_name": "other-prefix-key"}},
        {"id": "k3", "spec": {"display_name": "test-lb-kafka-env-2"}},
    ]

    keys = await provisioner.list_provisioned_keys()

    assert len(keys) == 2
    assert keys[0]["id"] == "k1"
    assert keys[1]["id"] == "k3"


# ── Key Revocation ──────────────────────────────────────────────────────


async def test_revoke_key_calls_delete(
    provisioner, mock_cloud, no_sleep
):
    """revoke_key calls DELETE on the API."""
    # First provision a key to cache it
    mock_cloud.post.return_value = {
        "id": "key-to-revoke",
        "spec": {"secret": "s"},
    }
    await provisioner.provision_key(
        resource_id="lsrc-1",
        environment_id="env-1",
        scope_label="sr",
    )

    await provisioner.revoke_key("key-to-revoke")

    mock_cloud.delete.assert_awaited_with(
        "/iam/v2/api-keys/key-to-revoke"
    )

    # Verify key removed from cache
    cached = KeyProvisioner.get_all_cached_keys()
    matching = [
        v
        for v in cached.values()
        if v.get("key_id") == "key-to-revoke"
    ]
    assert len(matching) == 0


async def test_revoke_all_revokes_matching_keys(
    provisioner, mock_cloud
):
    """revoke_all deletes all keys with our prefix."""
    mock_cloud.paginate.return_value = [
        {"id": "k1", "spec": {"display_name": "test-lb-sr-env-1"}},
        {"id": "k2", "spec": {"display_name": "test-lb-kafka-env-1"}},
    ]

    await provisioner.revoke_all()

    assert mock_cloud.delete.await_count == 2


async def test_revoke_all_handles_individual_failure(
    provisioner, mock_cloud
):
    """revoke_all continues even if one revoke fails."""
    mock_cloud.paginate.return_value = [
        {"id": "k1", "spec": {"display_name": "test-lb-a"}},
        {"id": "k2", "spec": {"display_name": "test-lb-b"}},
    ]
    mock_cloud.delete.side_effect = [
        Exception("network error"),
        None,
    ]

    # Should not raise
    await provisioner.revoke_all()

    assert mock_cloud.delete.await_count == 2


# ── Delete Service Account ─────────────────────────────────────────────


async def test_delete_service_account(provisioner, mock_cloud):
    """delete_service_account deletes the matching SA."""
    mock_cloud.paginate.return_value = [
        {"id": "sa-1", "display_name": "test-lb-extractor"},
    ]

    await provisioner.delete_service_account()

    mock_cloud.delete.assert_awaited_with(
        "/iam/v2/service-accounts/sa-1"
    )


async def test_delete_service_account_no_match(
    provisioner, mock_cloud
):
    """delete_service_account does nothing when no SA matches."""
    mock_cloud.paginate.return_value = [
        {"id": "sa-x", "display_name": "unrelated"},
    ]

    await provisioner.delete_service_account()

    mock_cloud.delete.assert_not_awaited()


# ── Error handling ──────────────────────────────────────────────────────


async def test_provision_key_api_error(
    provisioner, mock_cloud, no_sleep
):
    """provision_key propagates API errors."""
    mock_cloud.post.side_effect = RuntimeError("API unreachable")

    with pytest.raises(RuntimeError, match="API unreachable"):
        await provisioner.provision_key(
            resource_id="lsrc-1",
            environment_id="env-1",
            scope_label="sr",
        )


async def test_ensure_service_account_api_error(
    provisioner, mock_cloud
):
    """ensure_service_account propagates API errors."""
    mock_cloud.paginate.side_effect = RuntimeError("timeout")

    with pytest.raises(RuntimeError, match="timeout"):
        await provisioner.ensure_service_account()


# ── Static cache helpers ────────────────────────────────────────────────


def test_get_all_cached_keys_empty():
    """get_all_cached_keys returns empty dict when no cache."""
    assert KeyProvisioner.get_all_cached_keys() == {}


def test_get_all_cached_keys_returns_stored():
    """get_all_cached_keys returns all stored keys."""
    from lineage_bridge.config.cache import update_cache

    update_cache(
        provisioned_keys={
            "test-lb-sr-env-1": {
                "key_id": "k1",
                "api_key": "ak1",
                "api_secret": "as1",
            }
        }
    )
    keys = KeyProvisioner.get_all_cached_keys()
    assert "test-lb-sr-env-1" in keys
    assert keys["test-lb-sr-env-1"]["api_key"] == "ak1"
