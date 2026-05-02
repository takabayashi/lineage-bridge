# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""CLI entry point for the headless watcher daemon (Phase 2G).

Thin wrapper over `services.watcher_runner.run_forever_blocking`. The CLI
is just config parsing + signal handling — all logic lives in the service
layer so the API and CLI hit identical code paths.

Usage:
  lineage-bridge-watch --env env-abc123 [--cooldown 30] [--push-uc]

The watcher's id is printed on startup so it shows up in `GET /api/v1/watcher`
when an API process is sharing the same storage backend.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch Confluent Cloud for lineage changes")
    parser.add_argument(
        "--env",
        dest="envs",
        action="append",
        required=True,
        help="Environment ID to scan (repeatable)",
    )
    parser.add_argument(
        "--cluster",
        dest="clusters",
        action="append",
        default=None,
        help="Cluster ID filter (repeatable, optional)",
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=30.0,
        help="Seconds to wait after the last change before extracting (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        help="Seconds between REST API polls (default: 10)",
    )
    parser.add_argument(
        "--audit-log-bootstrap",
        default=None,
        help="Audit log Kafka bootstrap servers (forces audit-log mode if all 3 supplied)",
    )
    parser.add_argument("--audit-log-key", default=None, help="Audit log Kafka API key")
    parser.add_argument(
        "--audit-log-secret", default=None, help="Audit log Kafka API secret"
    )
    parser.add_argument(
        "--push-uc", action="store_true", help="Push to Databricks UC after extraction"
    )
    parser.add_argument(
        "--push-glue", action="store_true", help="Push to AWS Glue after extraction"
    )
    parser.add_argument(
        "--push-google", action="store_true", help="Push to Google Data Lineage"
    )
    parser.add_argument(
        "--push-datazone", action="store_true", help="Push to AWS DataZone"
    )
    args = parser.parse_args()

    # Local imports keep the entry point fast for `--help`.
    from lineage_bridge.config.settings import Settings
    from lineage_bridge.services import (
        ExtractionRequest,
        WatcherConfig,
        WatcherMode,
        run_forever_blocking,
    )
    from lineage_bridge.storage import make_repositories

    settings = Settings()  # type: ignore[call-arg]

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Mode is explicit (vs the old `_use_audit_log` private property): if the
    # CLI flags are supplied OR Settings has all 3 audit-log creds, we're in
    # AUDIT_LOG mode; otherwise REST polling.
    audit_bootstrap = args.audit_log_bootstrap or settings.audit_log_bootstrap_servers
    audit_key = args.audit_log_key or settings.audit_log_api_key
    audit_secret = args.audit_log_secret or settings.audit_log_api_secret
    use_audit = bool(audit_bootstrap and audit_key and audit_secret)

    config = WatcherConfig(
        mode=WatcherMode.AUDIT_LOG if use_audit else WatcherMode.REST_POLLING,
        poll_interval_seconds=args.poll_interval,
        cooldown_seconds=args.cooldown,
        audit_log_bootstrap_servers=audit_bootstrap,
        audit_log_api_key=audit_key,
        audit_log_api_secret=audit_secret,
        extraction=ExtractionRequest(
            environment_ids=args.envs,
            cluster_ids=args.clusters,
        ),
        push_databricks_uc=args.push_uc,
        push_aws_glue=args.push_glue,
        push_google=args.push_google,
        push_datazone=args.push_datazone,
    )

    repositories = make_repositories(settings)

    if use_audit:
        print(
            f"Consuming audit log from {audit_bootstrap} (cooldown: {args.cooldown}s)"
        )
    else:
        print(
            f"Polling Confluent Cloud every {args.poll_interval}s "
            f"(cooldown: {args.cooldown}s)"
        )
    print("Press Ctrl+C to stop")

    # Hook SIGTERM (k8s) the same way as SIGINT so containerised runs shut
    # down cleanly without a 10s wait_for fallback.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    main_task = loop.create_task(
        run_forever_blocking(config, settings, repositories.watchers)
    )

    def _on_signal() -> None:
        main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with _SuppressSignalRegistrationError():
            loop.add_signal_handler(sig, _on_signal)

    try:
        loop.run_until_complete(main_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nStopped")
    finally:
        loop.close()
        sys.exit(0)


class _SuppressSignalRegistrationError:
    """add_signal_handler raises OSError on Windows / inside threads — no-op there."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:
        return exc_type is OSError or exc_type is NotImplementedError
