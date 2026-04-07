# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""CLI entry point for headless change-detection watcher."""

from __future__ import annotations

import logging
import sys


def main() -> None:
    """Watch Confluent Cloud for lineage-relevant changes.

    Uses audit log Kafka consumer when --audit-log-bootstrap is provided
    (or LINEAGE_BRIDGE_AUDIT_LOG_BOOTSTRAP_SERVERS is set), otherwise
    falls back to REST API polling.

    Usage: lineage-bridge-watch --env env-abc123 [--cooldown 30]
    """
    import argparse

    from lineage_bridge.config.settings import Settings

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
        help="Seconds to wait after last change before triggering extraction (default: 30)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        help="Seconds between REST API polls when using polling mode (default: 10)",
    )
    parser.add_argument(
        "--audit-log-bootstrap",
        default=None,
        help="Audit log cluster bootstrap servers (overrides env var)",
    )
    parser.add_argument(
        "--audit-log-key",
        default=None,
        help="Audit log cluster API key (overrides env var)",
    )
    parser.add_argument(
        "--audit-log-secret",
        default=None,
        help="Audit log cluster API secret (overrides env var)",
    )
    parser.add_argument(
        "--push-uc",
        action="store_true",
        help="Push lineage to Databricks UC after each extraction",
    )
    parser.add_argument(
        "--push-glue",
        action="store_true",
        help="Push lineage to AWS Glue after each extraction",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]

    # Override audit log settings from CLI args
    if args.audit_log_bootstrap:
        settings.audit_log_bootstrap_servers = args.audit_log_bootstrap
    if args.audit_log_key:
        settings.audit_log_api_key = args.audit_log_key
    if args.audit_log_secret:
        settings.audit_log_api_secret = args.audit_log_secret

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    extraction_params = {
        "env_ids": args.envs,
        "cluster_ids": args.clusters,
        "enable_connect": True,
        "enable_ksqldb": True,
        "enable_flink": True,
        "enable_schema_registry": True,
        "enable_stream_catalog": False,
        "enable_tableflow": True,
        "enable_enrichment": True,
        "push_uc": args.push_uc,
        "push_glue": args.push_glue,
    }

    from lineage_bridge.watcher.engine import WatcherEngine

    engine = WatcherEngine(
        settings=settings,
        extraction_params=extraction_params,
        cooldown_seconds=args.cooldown,
        poll_interval=args.poll_interval,
    )

    use_audit = bool(
        settings.audit_log_bootstrap_servers
        and settings.audit_log_api_key
        and settings.audit_log_api_secret
    )
    if use_audit:
        print(
            f"Consuming audit log from {settings.audit_log_bootstrap_servers} "
            f"(cooldown: {args.cooldown}s)"
        )
    else:
        print(f"Polling Confluent Cloud every {args.poll_interval}s (cooldown: {args.cooldown}s)")
    print("Press Ctrl+C to stop")

    try:
        # Run directly in main thread (no background thread needed for CLI)
        engine.state = engine.state.WATCHING
        engine._run_loop()
    except KeyboardInterrupt:
        print("\nStopped")
        sys.exit(0)
