"""Local file cache for persisting UI state across sessions.

Stores user selections (environments, clusters) in plain JSON and
encrypts sensitive data (per-cluster API credentials) using Fernet
symmetric encryption with a machine-derived key.

Cache location: ``~/.lineage_bridge/cache.json``
Key file:       ``~/.lineage_bridge/.cache_key``
"""

from __future__ import annotations

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
_ENCRYPTED_FIELDS = {"cluster_credentials", "sr_credentials", "flink_credentials", "provisioned_keys"}


def _ensure_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Restrict directory to owner only.
    try:
        os.chmod(_CACHE_DIR, stat.S_IRWXU)
    except OSError:
        pass


def _get_or_create_key() -> bytes:
    """Return the Fernet key, creating one if it doesn't exist."""
    _ensure_dir()
    if _KEY_FILE.exists():
        return _KEY_FILE.read_bytes().strip()
    key = Fernet.generate_key()
    _KEY_FILE.write_bytes(key)
    # Restrict key file to owner read/write only.
    try:
        os.chmod(_KEY_FILE, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
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
            if field in to_write and to_write[field]:
                to_write[f"_{field}_enc"] = _encrypt_value(
                    fernet, to_write[field]
                )
                del to_write[field]
            elif field in to_write:
                del to_write[field]
        _CACHE_FILE.write_text(
            json.dumps(to_write, indent=2, default=str),
            encoding="utf-8",
        )
        # Restrict cache file to owner only.
        try:
            os.chmod(_CACHE_FILE, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
    except Exception:
        logger.debug("Failed to save cache to %s", _CACHE_FILE, exc_info=True)


def update_cache(**kwargs: Any) -> None:
    """Merge *kwargs* into the existing cache and save."""
    data = load_cache()
    data.update(kwargs)
    save_cache(data)
