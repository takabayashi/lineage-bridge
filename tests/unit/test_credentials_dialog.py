# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Unit tests for credential dialog seeding behavior."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCredentialSeeding:
    """Test that cached credentials populate dialog widgets on first open."""

    @patch("lineage_bridge.ui.sidebar.credentials.load_cache")
    @patch("lineage_bridge.ui.sidebar.credentials.st")
    def test_seed_env_state_populates_from_cache(
        self, st_mock: MagicMock, load_cache_mock: MagicMock
    ):
        """_seed_env_state should populate session_state from cached credentials."""
        # Setup: cache has SR credentials for env-123
        load_cache_mock.return_value = {
            "sr_credentials": {
                "env-123": {
                    "endpoint": "https://psrc-test.cloud.confluent.cloud",
                    "api_key": "test-key",
                    "api_secret": "test-secret",
                }
            },
            "flink_credentials": {
                "env-123": {
                    "api_key": "flink-key",
                    "api_secret": "flink-secret",
                }
            },
        }

        # Session state is empty (first dialog open)
        st_mock.session_state = {}

        from lineage_bridge.ui.sidebar.credentials import _seed_env_state

        _seed_env_state("env-123")

        # Assert: all keys should be populated
        assert (
            st_mock.session_state["sr_endpoint_env-123"]
            == "https://psrc-test.cloud.confluent.cloud"
        )
        assert st_mock.session_state["sr_key_env-123"] == "test-key"
        assert st_mock.session_state["sr_secret_env-123"] == "test-secret"
        assert st_mock.session_state["flink_key_env-123"] == "flink-key"
        assert st_mock.session_state["flink_secret_env-123"] == "flink-secret"

    @patch("lineage_bridge.ui.sidebar.credentials.load_cache")
    @patch("lineage_bridge.ui.sidebar.credentials.st")
    def test_seed_env_state_skips_when_already_set(
        self, st_mock: MagicMock, load_cache_mock: MagicMock
    ):
        """_seed_env_state should NOT overwrite existing non-empty values."""
        load_cache_mock.return_value = {
            "sr_credentials": {
                "env-123": {
                    "endpoint": "https://psrc-cached.cloud.confluent.cloud",
                    "api_key": "cached-key",
                    "api_secret": "cached-secret",
                }
            },
        }

        # Session state already has a value (user typed something)
        st_mock.session_state = {
            "sr_endpoint_env-123": "https://psrc-user-typed.cloud.confluent.cloud",
        }

        from lineage_bridge.ui.sidebar.credentials import _seed_env_state

        _seed_env_state("env-123")

        # User's value should be preserved
        assert (
            st_mock.session_state["sr_endpoint_env-123"]
            == "https://psrc-user-typed.cloud.confluent.cloud"
        )
        # But other fields should be seeded from cache
        assert st_mock.session_state["sr_key_env-123"] == "cached-key"

    @patch("lineage_bridge.ui.sidebar.credentials.load_cache")
    @patch("lineage_bridge.ui.sidebar.credentials.st")
    def test_seed_env_state_empty_string_is_reseeded(
        self, st_mock: MagicMock, load_cache_mock: MagicMock
    ):
        """_seed_env_state should populate over empty strings (which st.text_input creates)."""
        load_cache_mock.return_value = {
            "sr_credentials": {
                "env-123": {
                    "endpoint": "https://psrc-cached.cloud.confluent.cloud",
                }
            },
        }

        # Session state has empty string (Streamlit widget default)
        st_mock.session_state = {
            "sr_endpoint_env-123": "",
        }

        from lineage_bridge.ui.sidebar.credentials import _seed_env_state

        _seed_env_state("env-123")

        # Should overwrite empty string with cached value
        assert (
            st_mock.session_state["sr_endpoint_env-123"]
            == "https://psrc-cached.cloud.confluent.cloud"
        )

    @patch("lineage_bridge.ui.sidebar.credentials.load_cache")
    @patch("lineage_bridge.ui.sidebar.credentials.st")
    def test_seed_cluster_state_populates_from_cache(
        self, st_mock: MagicMock, load_cache_mock: MagicMock
    ):
        """_seed_cluster_state should populate session_state from cached credentials."""
        load_cache_mock.return_value = {
            "cluster_credentials": {
                "lkc-123": {
                    "api_key": "cluster-key",
                    "api_secret": "cluster-secret",
                }
            },
        }

        st_mock.session_state = {}

        from lineage_bridge.ui.sidebar.credentials import _seed_cluster_state

        _seed_cluster_state("lkc-123")

        assert st_mock.session_state["cluster_key_lkc-123"] == "cluster-key"
        assert st_mock.session_state["cluster_secret_lkc-123"] == "cluster-secret"
