# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Local file cache for persisting UI state across sessions.

Stores user selections (environments, clusters) in plain JSON and
encrypts sensitive data (per-cluster API credentials) using Fernet
symmetric encryption with a machine-derived key.

Cache location: ``~/.lineage_bridge/cache.json``
Key file:       ``~/.lineage_bridge/.cache_key``
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import stat
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".lineage_bridge"
_CACHE_FILE = _CACHE_DIR / "cache.json"
_KEY_FILE = _CACHE_DIR / ".cache_key"

# Fields that contain secrets and must be encrypted on disk.
_ENCRYPTED_FIELDS = {
    "cluster_credentials",
    "sr_credentials",
    "flink_credentials",
    "provisioned_keys",
}

# Subset of encrypted fields that accumulate across calls — when the caller
# saves credentials for one cluster/env, they shouldn't wipe out credentials
# already cached for other clusters/envs. Provisioned keys are excluded
# because the provisioner manages the full dict itself (load → modify → save)
# and relies on replace semantics for revocation.
_MERGE_FIELDS = {
    "cluster_credentials",
    "sr_credentials",
    "flink_credentials",
}


def _ensure_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Restrict directory to owner only.
    with contextlib.suppress(OSError):
        os.chmod(_CACHE_DIR, stat.S_IRWXU)


def _get_or_create_key() -> bytes:
    """Return the Fernet key, creating one if it doesn't exist."""
    _ensure_dir()
    if _KEY_FILE.exists():
        return _KEY_FILE.read_bytes().strip()
    key = Fernet.generate_key()
    _KEY_FILE.write_bytes(key)
    # Restrict key file to owner read/write only.
    with contextlib.suppress(OSError):
        os.chmod(_KEY_FILE, stat.S_IRUSR | stat.S_IWUSR)
    return key


def _get_fernet() -> Fernet:
    return Fernet(_get_or_create_key())


def _encrypt_value(fernet: Fernet, value: Any) -> str:
    """Serialize and encrypt a value."""
    raw = json.dumps(value).encode("utf-8")
    return fernet.encrypt(raw).decode("ascii")


def _decrypt_value(fernet: Fernet, token: str) -> Any:
    """Decrypt and deserialize a value."""
    raw = fernet.decrypt(token.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


def load_cache() -> dict[str, Any]:
    """Load cached state from disk. Returns empty dict on any error."""
    try:
        if not _CACHE_FILE.exists():
            return {}
        data = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        # Decrypt encrypted fields.
        fernet = _get_fernet()
        for field in _ENCRYPTED_FIELDS:
            enc_key = f"_{field}_enc"
            if enc_key in data:
                try:
                    data[field] = _decrypt_value(fernet, data[enc_key])
                except (InvalidToken, Exception):
                    logger.debug("Failed to decrypt %s — ignoring", field)
                    data[field] = {}
                del data[enc_key]
        return data
    except Exception:
        logger.debug("Failed to load cache from %s", _CACHE_FILE, exc_info=True)
    return {}


def save_cache(data: dict[str, Any]) -> None:
    """Persist state to disk, encrypting sensitive fields."""
    try:
        _ensure_dir()
        # Encrypt sensitive fields before writing.
        to_write = dict(data)
        fernet = _get_fernet()
        for field in _ENCRYPTED_FIELDS:
            if to_write.get(field):
                to_write[f"_{field}_enc"] = _encrypt_value(fernet, to_write[field])
                del to_write[field]
            elif field in to_write:
                del to_write[field]
        _CACHE_FILE.write_text(
            json.dumps(to_write, indent=2, default=str),
            encoding="utf-8",
        )
        # Restrict cache file to owner only.
        with contextlib.suppress(OSError):
            os.chmod(_CACHE_FILE, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        logger.debug("Failed to save cache to %s", _CACHE_FILE, exc_info=True)


def find_provisioned_key(prefix: str, resource_id: str) -> tuple[str | None, str | None]:
    """Look up a cached provisioned API key by name-prefix + resource_id.

    Returns ``(api_key, api_secret)`` for the first matching entry, or
    ``(None, None)`` if no match. Used by extractor phases to recover
    per-env / per-cluster service-account keys (ksqlDB, Tableflow) that
    the demo provision scripts cached but ``.env`` no longer reflects.
    """
    cache = load_cache()
    for name, entry in (cache.get("provisioned_keys") or {}).items():
        if name.startswith(prefix) and entry.get("resource_id") == resource_id:
            return entry.get("api_key"), entry.get("api_secret")
    return None, None


def update_cache(**kwargs: Any) -> None:
    """Merge *kwargs* into the existing cache and save.

    Credential dicts (cluster_credentials, sr_credentials, flink_credentials,
    provisioned_keys) are merged at the second level — i.e. new per-key entries
    are added on top of the existing dict instead of replacing it. This lets
    the user accumulate credentials across multiple clusters/environments in a
    single cache file (extract Glue, then extract BQ → both sets persist).
    Non-credential keys keep the original replace-semantics.
    """
    data = load_cache()
    for key, value in kwargs.items():
        if key in _MERGE_FIELDS and isinstance(value, dict) and isinstance(data.get(key), dict):
            data[key] = {**data[key], **value}
        else:
            data[key] = value
    save_cache(data)
