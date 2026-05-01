# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""API key authentication for the REST API."""

from __future__ import annotations

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Module-level config — set by create_app()
_configured_api_key: str | None = None


def configure_auth(api_key: str | None) -> None:
    """Set the expected API key. If None, authentication is disabled."""
    global _configured_api_key
    _configured_api_key = api_key


async def require_api_key(
    api_key: str | None = Security(_api_key_header),
) -> None:
    """FastAPI dependency that validates the X-API-Key header.

    If no API key is configured (dev mode), all requests are allowed.
    """
    if _configured_api_key is None:
        return
    if api_key != _configured_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
