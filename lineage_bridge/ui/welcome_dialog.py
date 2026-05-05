# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Welcome dialog for first-time setup of cloud credentials.

Appears when no Confluent Cloud credentials are found in .env or environment
variables. Offers to add them interactively or skip for demo mode.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st


def should_show_welcome_dialog() -> bool:
    """Check if we should show the welcome dialog.

    Show when:
    - No cloud credentials in .env or environment
    - User hasn't dismissed the dialog in this session
    - Not already connected

    Returns:
        True if the dialog should be shown
    """
    # Don't show if already dismissed in this session
    if st.session_state.get("_welcome_dismissed"):
        return False

    # Don't show if already connected (credentials available)
    if st.session_state.get("connected"):
        return False

    # Check for credentials in environment
    has_key = bool(os.getenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY"))
    has_secret = bool(os.getenv("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET"))

    # Don't show if credentials are already available
    if has_key and has_secret:
        return False

    # Show the dialog on first load when no credentials
    return True


def _save_credentials_to_env(api_key: str, api_secret: str) -> tuple[bool, str]:
    """Save credentials to .env file.

    Args:
        api_key: Cloud API key
        api_secret: Cloud API secret

    Returns:
        Tuple of (success: bool, message: str)
    """
    env_path = Path(".env")

    try:
        # Read existing .env or create from example
        if env_path.exists():
            content = env_path.read_text(encoding="utf-8")
        elif Path(".env.example").exists():
            content = Path(".env.example").read_text(encoding="utf-8")
        else:
            content = ""

        # Remove existing cloud key lines if present
        lines = content.splitlines()
        filtered_lines = [
            line for line in lines
            if not line.strip().startswith("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY")
            and not line.strip().startswith("LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET")
        ]

        # Append new credentials
        if filtered_lines and filtered_lines[-1].strip():
            filtered_lines.append("")
        filtered_lines.append("# Confluent Cloud credentials (added via UI)")
        filtered_lines.append(f"LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY={api_key}")
        filtered_lines.append(f"LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET={api_secret}")

        # Write back
        env_path.write_text("\n".join(filtered_lines) + "\n", encoding="utf-8")

        # Also set in current environment for immediate use
        os.environ["LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY"] = api_key
        os.environ["LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET"] = api_secret

        return True, f"✓ Credentials saved to {env_path.absolute()}"

    except Exception as exc:
        return False, f"✗ Failed to save credentials: {exc}"


@st.dialog("Welcome to LineageBridge", width="large")
def render_welcome_dialog() -> None:
    """Render the first-time setup dialog for cloud credentials."""

    st.markdown(
        """
        ### 🎉 Get Started with LineageBridge

        To extract lineage from your Confluent Cloud environment, you'll need to
        provide a **Cloud API Key** with read permissions.

        <div style="background: #f0f7ff; border-left: 4px solid #1f77b4; padding: 1rem; margin: 1rem 0; border-radius: 4px;">
        <strong>📚 Need a Cloud API Key?</strong><br/>
        Create one in the <a href="https://confluent.cloud/settings/api-keys" target="_blank">Confluent Cloud Console</a>
        or via the CLI: <code>confluent api-key create --resource cloud</code>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Credential input ──────────────────────────────────────────────

    st.markdown("#### Enter Your Credentials")

    col1, col2 = st.columns(2)

    with col1:
        api_key = st.text_input(
            "Cloud API Key",
            key="_welcome_cloud_key",
            placeholder="e.g., ABCDEFGH12345678",
            help="Your Confluent Cloud API key",
        )

    with col2:
        api_secret = st.text_input(
            "Cloud API Secret",
            type="password",
            key="_welcome_cloud_secret",
            placeholder="Enter your API secret",
            help="Your Confluent Cloud API secret",
        )

    # ── Action buttons ────────────────────────────────────────────────

    st.markdown("---")

    col_save, col_skip, col_demo = st.columns([1, 1, 1])

    with col_save:
        if st.button(
            "💾 Save & Connect",
            use_container_width=True,
            type="primary",
            disabled=not (api_key and api_secret),
        ):
            success, message = _save_credentials_to_env(api_key, api_secret)

            if success:
                st.success(message)
                st.info("🔄 Reloading app to apply credentials...")
                st.session_state["_welcome_dismissed"] = True

                # Wait a moment to show the success message, then rerun
                import time
                time.sleep(1)
                st.rerun()
            else:
                st.error(message)

    with col_skip:
        if st.button("⏭️ Skip for Now", use_container_width=True):
            st.session_state["_welcome_dismissed"] = True
            st.rerun()

    with col_demo:
        if st.button("🎨 Load Demo Graph", use_container_width=True):
            # Load sample graph
            from lineage_bridge.ui.sample_data import generate_sample_graph

            graph = generate_sample_graph()
            st.session_state.graph = graph
            st.session_state.graph_version = 3  # _GRAPH_VERSION from state.py
            st.session_state["_welcome_dismissed"] = True
            st.success("✓ Demo graph loaded!")
            st.rerun()

    # ── Help text ─────────────────────────────────────────────────────

    st.markdown(
        """
        <div style="margin-top: 2rem; padding: 1rem; background: #f8f9fa; border-radius: 4px; font-size: 0.9em;">
        <strong>🔐 Security Note:</strong> Credentials are saved to your local <code>.env</code> file
        and never sent anywhere except to Confluent Cloud APIs during extraction.
        </div>
        """,
        unsafe_allow_html=True,
    )
