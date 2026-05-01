# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Uvicorn entry point for the LineageBridge REST API."""

from __future__ import annotations

import uvicorn

from lineage_bridge.api.app import create_app
from lineage_bridge.config.settings import Settings


def main() -> None:
    """Start the API server using settings from environment."""
    try:
        settings = Settings()  # type: ignore[call-arg]
    except Exception:
        # Allow API to start without Confluent credentials for read-only usage
        settings = None

    api_key = settings.api_key if settings else None
    host = settings.api_host if settings else "0.0.0.0"
    port = settings.api_port if settings else 8000

    app = create_app(api_key=api_key)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
