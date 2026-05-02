# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for the per-merger logic in scripts/cache_demo_credentials.py.

Locks in the contract that the demo-up provision scripts depend on:

- GCP project_id is mirrored into ``gcp_settings``
- AWS DataZone domain + project IDs are mirrored into ``aws_datazone_settings``

We test the merger functions directly (pure dict-in / dict-out) — the
side-effect path (load_cache → save_cache) is exercised by the smoke test in
the conversation history and not duplicated here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Load the script as a module since it lives in scripts/, not lineage_bridge/.
_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cache_demo_credentials.py"
spec = importlib.util.spec_from_file_location("cache_demo_credentials", _SCRIPT)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
sys.modules["cache_demo_credentials"] = mod
spec.loader.exec_module(mod)


class TestMergeGcpSettings:
    def test_merges_project_and_location(self):
        cache: dict = {}
        env = {
            "LINEAGE_BRIDGE_GCP_PROJECT_ID": "my-project",
            "LINEAGE_BRIDGE_GCP_LOCATION": "us-central1",
        }
        assert mod._merge_gcp_settings(cache, env) is True
        assert cache["gcp_settings"] == {
            "project_id": "my-project",
            "location": "us-central1",
        }

    def test_skips_when_project_missing(self):
        cache: dict = {}
        assert mod._merge_gcp_settings(cache, {}) is False
        assert "gcp_settings" not in cache

    def test_preserves_existing_keys(self):
        cache = {"gcp_settings": {"location": "us-east1", "extra": "keep"}}
        env = {"LINEAGE_BRIDGE_GCP_PROJECT_ID": "p"}
        mod._merge_gcp_settings(cache, env)
        # extra is not stripped by the merger
        assert cache["gcp_settings"]["extra"] == "keep"


class TestMergeAwsDatazoneSettings:
    def test_merges_when_both_ids_present(self):
        cache: dict = {}
        env = {
            "LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID": "dzd-abc",
            "LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID": "prj-xyz",
            "LINEAGE_BRIDGE_AWS_REGION": "us-west-2",
        }
        assert mod._merge_aws_datazone_settings(cache, env) is True
        assert cache["aws_datazone_settings"] == {
            "domain_id": "dzd-abc",
            "project_id": "prj-xyz",
            "region": "us-west-2",
        }

    @pytest.mark.parametrize(
        "env",
        [
            {},
            {"LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID": "dzd-only"},
            {"LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID": "prj-only"},
            {
                "LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID": "dzd-x",
                "LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID": "  ",
            },
        ],
    )
    def test_skips_when_either_id_missing(self, env):
        # DataZone push needs BOTH IDs; a half-set pair must not pollute the cache.
        cache: dict = {}
        assert mod._merge_aws_datazone_settings(cache, env) is False
        assert "aws_datazone_settings" not in cache

    def test_region_is_optional(self):
        cache: dict = {}
        env = {
            "LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID": "dzd-abc",
            "LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID": "prj-xyz",
        }
        mod._merge_aws_datazone_settings(cache, env)
        assert "region" not in cache["aws_datazone_settings"]

    def test_overwrites_existing_ids(self):
        # Re-running demo-up after an ID change should replace the old IDs.
        cache = {"aws_datazone_settings": {"domain_id": "dzd-old", "project_id": "prj-old"}}
        env = {
            "LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID": "dzd-new",
            "LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID": "prj-new",
        }
        mod._merge_aws_datazone_settings(cache, env)
        assert cache["aws_datazone_settings"]["domain_id"] == "dzd-new"
        assert cache["aws_datazone_settings"]["project_id"] == "prj-new"


class TestMergeDatabricksSettings:
    def test_merges_workspace_token_warehouse(self):
        cache: dict = {}
        env = {
            "LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL": "https://dbc-x.cloud.databricks.com",
            "LINEAGE_BRIDGE_DATABRICKS_TOKEN": "dapi-secret",
            "LINEAGE_BRIDGE_DATABRICKS_WAREHOUSE_ID": "wh-123",
        }
        assert mod._merge_databricks_settings(cache, env) is True
        assert cache["databricks_settings"] == {
            "workspace_url": "https://dbc-x.cloud.databricks.com",
            "token": "dapi-secret",
            "warehouse_id": "wh-123",
        }

    def test_skips_when_all_missing(self):
        cache: dict = {}
        assert mod._merge_databricks_settings(cache, {}) is False
        assert "databricks_settings" not in cache

    def test_partial_fields_are_merged(self):
        cache: dict = {}
        env = {"LINEAGE_BRIDGE_DATABRICKS_WORKSPACE_URL": "https://dbc-y"}
        assert mod._merge_databricks_settings(cache, env) is True
        assert cache["databricks_settings"] == {"workspace_url": "https://dbc-y"}

    def test_preserves_existing_other_fields(self):
        # Re-cache after rotating only the token; the warehouse_id from a prior
        # demo-up call must survive.
        cache = {"databricks_settings": {"warehouse_id": "wh-keep"}}
        env = {"LINEAGE_BRIDGE_DATABRICKS_TOKEN": "new-token"}
        mod._merge_databricks_settings(cache, env)
        assert cache["databricks_settings"]["warehouse_id"] == "wh-keep"
        assert cache["databricks_settings"]["token"] == "new-token"


class TestMergeAwsSettings:
    def test_merges_region(self):
        cache: dict = {}
        env = {"LINEAGE_BRIDGE_AWS_REGION": "us-west-2"}
        assert mod._merge_aws_settings(cache, env) is True
        assert cache["aws_settings"] == {"region": "us-west-2"}

    def test_skips_when_region_missing(self):
        cache: dict = {}
        assert mod._merge_aws_settings(cache, {}) is False
        assert "aws_settings" not in cache

    def test_overwrites_region(self):
        cache = {"aws_settings": {"region": "us-east-1"}}
        env = {"LINEAGE_BRIDGE_AWS_REGION": "eu-west-1"}
        mod._merge_aws_settings(cache, env)
        assert cache["aws_settings"]["region"] == "eu-west-1"
