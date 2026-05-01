# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Merge demo .env credentials into the local encrypted cache.

Invoked by each demo's ``provision-demo.sh`` after Terraform writes the
project-root ``.env``. Without this step, running ``demo-uc-up``,
``demo-glue-up``, and ``demo-bq-up`` in sequence leaves only the latest
demo's credentials in ``.env`` — earlier demos' Kafka/SR/Flink keys end
up buried in ``.env.backup.<timestamp>`` files. The cache already
supports per-cluster and per-environment credential dicts, so merging
into it preserves access to every provisioned demo.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import dotenv_values

from lineage_bridge.config.cache import load_cache, save_cache


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env-file", required=True, type=Path, help="Path to the .env file to read")
    p.add_argument("--env-id", required=True, help="Confluent environment ID (env-xxxxx)")
    p.add_argument("--cluster-id", required=True, help="Confluent Kafka cluster ID (lkc-xxxxx)")
    p.add_argument(
        "--demo-name",
        default="demo",
        help="Demo identifier used in provisioned-key display names (uc|glue|bigquery)",
    )
    return p.parse_args()


def _merge_cluster_creds(cache: dict, env: dict, cluster_id: str) -> bool:
    """Merge Kafka cluster API key into ``cache['cluster_credentials']``."""
    raw = env.get("LINEAGE_BRIDGE_CLUSTER_CREDENTIALS")
    if not raw:
        return False
    try:
        from_env = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(from_env, dict):
        return False
    existing = dict(cache.get("cluster_credentials") or {})
    # The env dict is already keyed by cluster_id; merge by key (preserves other demos).
    existing.update(from_env)
    # Belt-and-suspenders: ensure the cluster_id we were told about is present.
    if cluster_id not in existing:
        return False
    cache["cluster_credentials"] = existing
    return True


def _merge_sr_creds(cache: dict, env: dict, env_id: str) -> bool:
    endpoint = env.get("LINEAGE_BRIDGE_SCHEMA_REGISTRY_ENDPOINT", "").strip()
    api_key = env.get("LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_KEY", "").strip()
    api_secret = env.get("LINEAGE_BRIDGE_SCHEMA_REGISTRY_API_SECRET", "").strip()
    cred: dict[str, str] = {}
    if endpoint:
        cred["endpoint"] = endpoint
    if api_key and api_secret:
        cred["api_key"] = api_key
        cred["api_secret"] = api_secret
    if not cred:
        return False
    existing = dict(cache.get("sr_credentials") or {})
    existing[env_id] = cred
    cache["sr_credentials"] = existing
    return True


def _merge_flink_creds(cache: dict, env: dict, env_id: str) -> bool:
    api_key = env.get("LINEAGE_BRIDGE_FLINK_API_KEY", "").strip()
    api_secret = env.get("LINEAGE_BRIDGE_FLINK_API_SECRET", "").strip()
    if not (api_key and api_secret):
        return False
    existing = dict(cache.get("flink_credentials") or {})
    existing[env_id] = {"api_key": api_key, "api_secret": api_secret}
    cache["flink_credentials"] = existing
    return True


def _merge_provisioned_key(
    cache: dict,
    env: dict,
    *,
    display_name: str,
    key_var: str,
    secret_var: str,
    resource_id: str,
) -> bool:
    api_key = env.get(key_var, "").strip()
    api_secret = env.get(secret_var, "").strip()
    if not (api_key and api_secret):
        return False
    existing = dict(cache.get("provisioned_keys") or {})
    existing[display_name] = {
        "key_id": "",
        "api_key": api_key,
        "api_secret": api_secret,
        "resource_id": resource_id,
        "owner_id": "",
    }
    cache["provisioned_keys"] = existing
    return True


def _merge_gcp_settings(cache: dict, env: dict) -> bool:
    """Mirror GCP project/location into the cache so the UI can fall back when .env is stale."""
    project_id = env.get("LINEAGE_BRIDGE_GCP_PROJECT_ID", "").strip()
    if not project_id:
        return False
    existing = dict(cache.get("gcp_settings") or {})
    existing["project_id"] = project_id
    location = env.get("LINEAGE_BRIDGE_GCP_LOCATION", "").strip()
    if location:
        existing["location"] = location
    cache["gcp_settings"] = existing
    return True


def _merge_aws_datazone_settings(cache: dict, env: dict) -> bool:
    """Mirror AWS DataZone domain/project IDs into the cache.

    Same UX as the GCP fallback — once a demo provisions DataZone wiring, the
    UI's "Push to DataZone" button stays available even when ``.env`` later
    gets overwritten by another demo (multi-demo workflows).
    """
    domain_id = env.get("LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID", "").strip()
    project_id = env.get("LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID", "").strip()
    if not domain_id or not project_id:
        return False
    existing = dict(cache.get("aws_datazone_settings") or {})
    existing["domain_id"] = domain_id
    existing["project_id"] = project_id
    region = env.get("LINEAGE_BRIDGE_AWS_REGION", "").strip()
    if region:
        existing["region"] = region
    cache["aws_datazone_settings"] = existing
    return True


def main() -> int:
    args = _parse_args()

    if not args.env_file.is_file():
        print(f"  Skipped credential cache: {args.env_file} not found", file=sys.stderr)
        return 0

    env = dotenv_values(args.env_file)
    cache = load_cache()

    merged: list[str] = []
    if _merge_cluster_creds(cache, env, args.cluster_id):
        merged.append(f"cluster:{args.cluster_id}")
    if _merge_sr_creds(cache, env, args.env_id):
        merged.append(f"sr:{args.env_id}")
    if _merge_flink_creds(cache, env, args.env_id):
        merged.append(f"flink:{args.env_id}")
    if _merge_provisioned_key(
        cache,
        env,
        display_name=f"lineage-bridge-tableflow-{args.demo_name}-{args.env_id}",
        key_var="LINEAGE_BRIDGE_TABLEFLOW_API_KEY",
        secret_var="LINEAGE_BRIDGE_TABLEFLOW_API_SECRET",
        resource_id=args.env_id,
    ):
        merged.append("tableflow")
    if _merge_provisioned_key(
        cache,
        env,
        display_name=f"lineage-bridge-ksqldb-{args.demo_name}-{args.env_id}",
        key_var="LINEAGE_BRIDGE_KSQLDB_API_KEY",
        secret_var="LINEAGE_BRIDGE_KSQLDB_API_SECRET",
        resource_id=args.env_id,
    ):
        merged.append("ksqldb")
    if _merge_gcp_settings(cache, env):
        merged.append("gcp")
    if _merge_aws_datazone_settings(cache, env):
        merged.append("datazone")

    if not merged:
        print("  No demo credentials found in .env — cache unchanged")
        return 0

    save_cache(cache)
    print(f"  Cached demo credentials: {', '.join(merged)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
