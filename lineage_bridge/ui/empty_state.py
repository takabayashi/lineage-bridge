# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Welcome / empty-state hero shown when no graph is loaded.

Extracted from `ui/app.py` in Phase 2E. The HTML/CSS classes (`.hero-card`,
`.feature-grid`, `.arch-flow`, `.step-tracker`) are defined in
`ui/static/styles.css`.
"""

from __future__ import annotations

import streamlit as st

from lineage_bridge.ui.sample_data import generate_sample_graph
from lineage_bridge.ui.state import _GRAPH_VERSION


def render_empty_state() -> None:
    """Welcome/empty state with project overview."""
    # ── Hero ──────────────────────────────────────────────────────
    st.markdown(
        """
        <div class="hero-card">
            <h1>\U0001f310 LineageBridge <span class="poc-badge">POC</span></h1>
            <div class="hero-subtitle">
                Stream Lineage Discovery &amp; Visualization
            </div>
            <p>
                LineageBridge automatically discovers how data flows through
                your Confluent Cloud infrastructure and renders it as an
                interactive directed graph &mdash; giving you end-to-end
                visibility from Kafka topics through transformation layers
                all the way to external data catalogs.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── What it does ──────────────────────────────────────────────
    st.markdown("#### What does LineageBridge do?")
    st.markdown(
        """
        <div class="feature-grid">
            <div class="feature-card">
                <div class="fc-icon">\U0001f50d</div>
                <div class="fc-title">Auto-Discovery</div>
                <div class="fc-desc">
                    Connects to Confluent Cloud APIs to automatically inventory
                    topics, connectors, Flink jobs, ksqlDB queries, and Tableflow
                    tables &mdash; no manual mapping required.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f4ca</div>
                <div class="fc-title">Lineage Visualization</div>
                <div class="fc-desc">
                    Renders a DAG-based interactive graph showing how data
                    flows between resources, with filtering by type,
                    environment, cluster, and search.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f517</div>
                <div class="fc-title">Catalog Integration</div>
                <div class="fc-desc">
                    Extends lineage beyond Confluent Cloud into Databricks
                    Unity Catalog and AWS Glue via Tableflow, bridging
                    streaming and lakehouse worlds.
                </div>
            </div>
            <div class="feature-card">
                <div class="fc-icon">\U0001f4dd</div>
                <div class="fc-title">Metadata Enrichment</div>
                <div class="fc-desc">
                    Enriches nodes with schemas, tags, business metadata,
                    live throughput metrics, and deep links back to the
                    Confluent Cloud and Databricks consoles.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── How it works ─────────────────────────────────────────────
    st.markdown("#### How it works")
    st.markdown(
        """
        <div class="arch-flow" style="flex-direction:column; align-items:stretch; gap:0;">
            <div style="display:flex; align-items:center; justify-content:center;
                        gap:6px; flex-wrap:wrap; padding:8px 0;">
                <span class="arch-node" style="background:rgba(25,118,210,0.12);
                    color:#1976D2;">Confluent Cloud APIs</span>
                <span class="arch-arrow">→</span>
                <span class="arch-node" style="background:rgba(25,118,210,0.08);
                    color:#1565C0;">Async Clients</span>
                <span class="arch-arrow">→</span>
                <span class="arch-node" style="background:rgba(123,31,162,0.1);
                    color:#7B1FA2;">Orchestrator</span>
                <span class="arch-arrow">→</span>
                <span class="arch-node" style="background:rgba(56,142,60,0.1);
                    color:#388E3C;">Lineage Graph</span>
                <span class="arch-arrow">→</span>
                <span class="arch-node" style="background:rgba(249,168,37,0.12);
                    color:#F57F17;">Interactive UI</span>
            </div>
            <hr style="border:none; border-top:1px solid rgba(128,128,128,0.12);
                       margin:4px 0;">
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;
                        padding:10px 4px 6px;">
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 1</span>
                    <span style="color:#999;">&mdash;</span>
                    Kafka topics &amp; consumer groups
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 2</span>
                    <span style="color:#999;">&mdash;</span>
                    Connectors, ksqlDB &amp; Flink edges
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 3</span>
                    <span style="color:#999;">&mdash;</span>
                    Schemas, tags &amp; metadata enrichment
                </div>
                <div style="font-size:0.82rem;">
                    <span style="color:#7B1FA2; font-weight:600;">Phase 4</span>
                    <span style="color:#999;">&mdash;</span>
                    Tableflow &amp; catalog mapping
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "All phases run **asynchronously** for maximum throughput. "
        "Catalog providers (Databricks Unity Catalog, AWS Glue) extend "
        "lineage beyond Confluent Cloud, bridging streaming and lakehouse worlds."
    )

    # ── Step tracker ──────────────────────────────────────────────
    st.markdown("#### Get started")

    connected = st.session_state.connected
    has_envs = bool(st.session_state.get("env_select"))

    step1_cls = "step-done" if connected else "step-current"
    step1_icon = "✓" if connected else "1"
    step2_cls = "step-done" if has_envs else ("step-current" if connected else "step-pending")
    step2_icon = "✓" if has_envs else "2"
    step3_cls = "step-current" if has_envs else "step-pending"

    st.markdown(
        f"""
        <div class="step-tracker">
            <div class="step-item {step1_cls}">
                <strong>{step1_icon}</strong> Connect
            </div>
            <span class="step-arrow">→</span>
            <div class="step-item {step2_cls}">
                <strong>{step2_icon}</strong> Select Infrastructure
            </div>
            <span class="step-arrow">→</span>
            <div class="step-item {step3_cls}">
                <strong>3</strong> Extract &amp; Explore
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "Use the **sidebar** to connect your Confluent Cloud account, "
        "select environments and clusters, then extract lineage. "
        "Or load a demo graph to explore the interface."
    )

    _, center, _ = st.columns([1, 1, 1])
    with center:
        if st.button(
            "⚡ Load Demo Graph",
            key="hero_demo_btn",
            type="primary",
            width="stretch",
        ):
            st.session_state.graph = generate_sample_graph()
            st.session_state.graph_version = _GRAPH_VERSION
            st.session_state.selected_node = None
            st.session_state.focus_node = None
            st.rerun()
