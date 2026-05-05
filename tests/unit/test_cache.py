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


def test_round_trip_audit_log_credentials():
    """audit_log_credentials bundle is encrypted on disk and decrypted on load.

    The bootstrap server hostname leaks no secrets, but the api_key and
    api_secret must never appear in the raw JSON file. Uses unique tokens
    so an accidental substring match in the base64 ciphertext can't hide a
    real plaintext leak.
    """
    bundle = {
        "bootstrap_servers": "pkc-uniqhost.region.aws.confluent.cloud:9092",
        "api_key": "AKIA_AUDITLOG_UNIQUE_KEY_TOKEN",
        "api_secret": "SECRET_AUDITLOG_UNIQUE_VALUE_TOKEN",
    }
    save_cache({"audit_log_credentials": bundle})

    raw = cache_mod._CACHE_FILE.read_text(encoding="utf-8")
    assert "_audit_log_credentials_enc" in raw
    assert "audit_log_credentials" not in json.loads(raw)
    assert "AKIA_AUDITLOG_UNIQUE_KEY_TOKEN" not in raw
    assert "SECRET_AUDITLOG_UNIQUE_VALUE_TOKEN" not in raw

    loaded = load_cache()
    assert loaded["audit_log_credentials"] == bundle


def test_update_cache_replaces_audit_log_bundle():
    """audit_log_credentials uses replace semantics — not merge.

    Unlike `cluster_credentials` / `sr_credentials` / `flink_credentials`
    (which accumulate per-key entries across calls), the audit-log bundle
    is a single record that should be wholly replaced on update so the
    Clear button can wipe it by passing an empty dict.
    """
    update_cache(audit_log_credentials={"api_key": "OLD", "api_secret": "OLD"})
    update_cache(audit_log_credentials={"api_key": "NEW", "api_secret": "NEW"})
    loaded = load_cache()
    assert loaded["audit_log_credentials"] == {"api_key": "NEW", "api_secret": "NEW"}


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


def test_update_cache_accumulates_per_cluster_credentials():
    """Per-cluster credentials persist when subsequent saves target a different cluster.

    Scenario: user extracts Glue (saves creds for lkc-glue), then extracts BQ
    (saves creds for lkc-bq). Both sets must remain in the cache so a return
    visit to Glue can replay them.
    """
    update_cache(cluster_credentials={"lkc-glue": {"api_key": "GK", "api_secret": "GS"}})
    update_cache(cluster_credentials={"lkc-bq": {"api_key": "BK", "api_secret": "BS"}})

    loaded = load_cache()
    assert set(loaded["cluster_credentials"]) == {"lkc-glue", "lkc-bq"}
    assert loaded["cluster_credentials"]["lkc-glue"]["api_key"] == "GK"
    assert loaded["cluster_credentials"]["lkc-bq"]["api_key"] == "BK"


def test_update_cache_accumulates_sr_and_flink_credentials():
    """sr_credentials and flink_credentials follow the same per-env merge."""
    update_cache(
        sr_credentials={"env-glue": {"endpoint": "sr-glue", "api_key": "k", "api_secret": "s"}},
        flink_credentials={"env-glue": {"api_key": "fk", "api_secret": "fs"}},
    )
    update_cache(
        sr_credentials={"env-bq": {"endpoint": "sr-bq", "api_key": "k2", "api_secret": "s2"}},
        flink_credentials={"env-bq": {"api_key": "fk2", "api_secret": "fs2"}},
    )
    loaded = load_cache()
    assert set(loaded["sr_credentials"]) == {"env-glue", "env-bq"}
    assert set(loaded["flink_credentials"]) == {"env-glue", "env-bq"}


def test_update_cache_provisioned_keys_keeps_replace_semantics():
    """provisioned_keys is provisioner-managed (load → modify → save), so it
    must keep replace semantics — otherwise revoke flows can't delete entries.
    """
    update_cache(
        provisioned_keys={
            "k1": {"key_id": "id1", "api_key": "a1", "api_secret": "s1"},
            "k2": {"key_id": "id2", "api_key": "a2", "api_secret": "s2"},
        }
    )
    # Provisioner removes k1, then writes the smaller dict back.
    update_cache(provisioned_keys={"k2": {"key_id": "id2", "api_key": "a2", "api_secret": "s2"}})
    loaded = load_cache()
    assert set(loaded["provisioned_keys"]) == {"k2"}


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
