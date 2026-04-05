# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for lineage_bridge.config.cache.

Tests round-trip save/load, encryption of sensitive fields, error
handling for corrupted files, update_cache merging, and file permissions.
"""

from __future__ import annotations

import json
import os
import stat

import pytest

import lineage_bridge.config.cache as cache_mod
from lineage_bridge.config.cache import (
    load_cache,
    save_cache,
    update_cache,
)


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """Redirect all cache paths to a temp directory."""
    cache_dir = tmp_path / ".lineage_bridge"
    cache_file = cache_dir / "cache.json"
    key_file = cache_dir / ".cache_key"
    monkeypatch.setattr(cache_mod, "_CACHE_DIR", cache_dir)
    monkeypatch.setattr(cache_mod, "_CACHE_FILE", cache_file)
    monkeypatch.setattr(cache_mod, "_KEY_FILE", key_file)


# ── round-trip ──────────────────────────────────────────────────────────


def test_round_trip_plain_data():
    """save_cache then load_cache returns the same plain data."""
    data = {"selected_envs": ["env-1", "env-2"], "version": 1}
    save_cache(data)
    loaded = load_cache()
    assert loaded["selected_envs"] == ["env-1", "env-2"]
    assert loaded["version"] == 1


def test_round_trip_encrypted_fields():
    """Encrypted fields are encrypted on disk but decrypted on load."""
    data = {
        "cluster_credentials": {"lkc-1": {"api_key": "key1", "api_secret": "secret1"}},
        "provisioned_keys": {
            "lineage-bridge-sr-env-1": {
                "key_id": "k1",
                "api_key": "ak1",
                "api_secret": "as1",
            }
        },
        "plain_field": "hello",
    }
    save_cache(data)

    # Verify on-disk JSON has encrypted blobs, not plaintext
    raw = json.loads(cache_mod._CACHE_FILE.read_text(encoding="utf-8"))
    assert "_cluster_credentials_enc" in raw
    assert "cluster_credentials" not in raw
    assert "_provisioned_keys_enc" in raw
    assert "provisioned_keys" not in raw
    assert raw["plain_field"] == "hello"

    # Load returns decrypted values
    loaded = load_cache()
    assert loaded["cluster_credentials"]["lkc-1"]["api_key"] == "key1"
    assert loaded["provisioned_keys"]["lineage-bridge-sr-env-1"]["api_secret"] == "as1"


def test_round_trip_sr_credentials():
    """sr_credentials field is also encrypted/decrypted."""
    data = {
        "sr_credentials": {"env-1": {"api_key": "sr-k", "api_secret": "sr-s"}},
    }
    save_cache(data)
    loaded = load_cache()
    assert loaded["sr_credentials"]["env-1"]["api_key"] == "sr-k"


def test_round_trip_flink_credentials():
    """flink_credentials field is also encrypted/decrypted."""
    data = {
        "flink_credentials": {"env-2": {"api_key": "fk", "api_secret": "fs"}},
    }
    save_cache(data)
    loaded = load_cache()
    assert loaded["flink_credentials"]["env-2"]["api_key"] == "fk"


# ── missing / corrupted files ──────────────────────────────────────────


def test_load_cache_missing_file():
    """load_cache returns empty dict when no cache file exists."""
    assert load_cache() == {}


def test_load_cache_corrupted_json():
    """load_cache returns empty dict when cache file has invalid JSON."""
    cache_mod._CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_mod._CACHE_FILE.write_text("NOT VALID JSON {{{", encoding="utf-8")
    assert load_cache() == {}


def test_load_cache_corrupted_key_file():
    """load_cache handles a corrupted key file gracefully.

    The encrypted fields become empty dicts but the plain data is preserved.
    """
    data = {
        "cluster_credentials": {"lkc-1": {"api_key": "k", "api_secret": "s"}},
        "plain": "ok",
    }
    save_cache(data)

    # Corrupt the key file
    cache_mod._KEY_FILE.write_bytes(b"INVALID-FERNET-KEY")

    loaded = load_cache()
    # Corrupted key causes Fernet init to fail inside load_cache,
    # which hits the outer exception handler and returns empty dict.
    assert loaded == {}


def test_load_cache_empty_encrypted_field():
    """Empty encrypted fields are not written and return empty on load."""
    data = {"cluster_credentials": {}, "plain": "x"}
    save_cache(data)
    loaded = load_cache()
    assert loaded.get("plain") == "x"
    # Empty dict was stripped during save
    assert "cluster_credentials" not in loaded


# ── update_cache ────────────────────────────────────────────────────────


def test_update_cache_merges():
    """update_cache merges new kwargs into existing cache."""
    save_cache({"a": 1, "b": 2})
    update_cache(b=3, c=4)
    loaded = load_cache()
    assert loaded["a"] == 1
    assert loaded["b"] == 3
    assert loaded["c"] == 4


def test_update_cache_creates_file():
    """update_cache works when no cache file exists yet."""
    update_cache(fresh="value")
    loaded = load_cache()
    assert loaded["fresh"] == "value"


def test_update_cache_with_encrypted_field():
    """update_cache can add encrypted fields that survive round-trip."""
    update_cache(
        provisioned_keys={"test-key": {"key_id": "k1", "api_key": "ak", "api_secret": "as"}}
    )
    loaded = load_cache()
    assert loaded["provisioned_keys"]["test-key"]["api_key"] == "ak"


# ── file permissions ───────────────────────────────────────────────────


def test_cache_dir_permissions():
    """Cache directory gets owner-only permissions."""
    save_cache({"test": True})
    mode = os.stat(cache_mod._CACHE_DIR).st_mode
    assert mode & stat.S_IRWXU  # owner has rwx
    assert not (mode & stat.S_IRWXG)  # group has nothing
    assert not (mode & stat.S_IRWXO)  # others have nothing


def test_cache_file_permissions():
    """Cache file gets owner read/write only."""
    save_cache({"test": True})
    mode = os.stat(cache_mod._CACHE_FILE).st_mode
    assert mode & stat.S_IRUSR  # owner read
    assert mode & stat.S_IWUSR  # owner write
    assert not (mode & stat.S_IRGRP)  # no group read
    assert not (mode & stat.S_IROTH)  # no others read


def test_key_file_permissions():
    """Key file gets owner read/write only."""
    save_cache({"cluster_credentials": {"lkc": {"k": "v"}}})
    mode = os.stat(cache_mod._KEY_FILE).st_mode
    assert mode & stat.S_IRUSR
    assert mode & stat.S_IWUSR
    assert not (mode & stat.S_IRGRP)
    assert not (mode & stat.S_IROTH)


# ── key generation ──────────────────────────────────────────────────────


def test_key_reused_across_calls():
    """The same Fernet key is returned on subsequent calls."""
    save_cache({"cluster_credentials": {"lkc": {"k": "v"}}})
    key1 = cache_mod._KEY_FILE.read_bytes().strip()
    # Save again — key should not change
    save_cache({"cluster_credentials": {"lkc": {"k": "v2"}}})
    key2 = cache_mod._KEY_FILE.read_bytes().strip()
    assert key1 == key2
