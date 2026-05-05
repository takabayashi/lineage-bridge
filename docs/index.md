# LineageBridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/takabayashi/lineage-bridge/blob/main/LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![GitHub Release](https://img.shields.io/github/v/release/takabayashi/lineage-bridge)](https://github.com/takabayashi/lineage-bridge/releases)

**Extract stream lineage from Confluent Cloud, bridge it to data catalogs, and visualize it as an interactive graph.**

LineageBridge fills a gap: Confluent Cloud has rich stream processing lineage (connectors, Flink jobs, ksqlDB queries, consumer groups) but no way to export it as a queryable graph or bridge it into external data catalogs. LineageBridge extracts this lineage using only public APIs, connects it to Databricks Unity Catalog, AWS Glue, and Google Data Lineage via Tableflow, and renders everything in an interactive UI.

---

## 🚀 Get Started in 30 Seconds

=== "One-Line Quickstart"

    **No installation, no configuration needed:**

    ```bash
    curl -fsSL https://raw.githubusercontent.com/takabayashi/lineage-bridge/main/scripts/quickstart.sh | bash
    ```

    This automatically:
    
    - ✅ Installs dependencies (uv + LineageBridge)
    - ✅ Launches the interactive UI
    - ✅ Opens your browser to http://localhost:8501
    - ✅ Loads a sample graph for immediate exploration

    **Perfect for:** First-time evaluation, demos, trying before committing

=== "Docker"

    **Container-friendly one-liner:**

    ```bash
    docker run -p 8501:8501 ghcr.io/takabayashi/lineage-bridge:latest
    ```

    Then open http://localhost:8501 → Click **"Load Demo Graph"**

    **Perfect for:** Kubernetes, production deployments, isolated environments

=== "Manual Install"

    **For development and customization:**

    ```bash
    git clone https://github.com/takabayashi/lineage-bridge.git
    cd lineage-bridge
    uv pip install -e ".[dev]"
    make ui
    ```

    A welcome dialog appears to guide credential setup.

    **Perfect for:** Contributing, extending, production configuration

---

## Key Features

- **🎯 Zero-Friction Onboarding**: One-line quickstart + welcome dialog for guided setup
- **🔍 Complete Lineage Discovery**: Topics, connectors, Flink, ksqlDB, consumer groups, schemas, catalog tables
- **📊 Interactive Visualization**: Directed graph with drag, zoom, search, neighborhood filtering
- **🔗 Multi-Catalog Integration**: Databricks UC, AWS Glue, Google Data Lineage, AWS DataZone
- **🔄 Change Detection**: Real-time monitoring with auto-extraction on infrastructure changes
- **🚀 Production-Ready**: REST API, OpenLineage endpoints, pluggable storage, Docker support

## What's New

### v0.6.1 — One-Line Quickstart (Latest)

- **🚀 One-line installer**: `curl | bash` quickstart script for instant demo mode
- **🎨 Welcome dialog**: First-time credential setup guide with three options (connect / skip / demo)
- **🐳 Docker publishing**: Pre-built images at `ghcr.io/takabayashi/lineage-bridge`
- **📚 Enhanced docs**: Quickstart-first documentation flow

### v0.6.0 — UX Redesign

- 🎨 **Complete UX Redesign**: Sidebar collapsed from 8 to 4 expanders, credential dialogs, persistent logs
- 🔧 **In-Process Watcher**: Threading-based change detection with REST polling (no separate daemon)
- 🏗️ **Service Layer** (v0.5.0): `run_extraction` / `run_enrichment` / `run_push` - unified API for UI/CLI/watcher
- 💾 **Pluggable Storage** (v0.5.0): Memory/file/SQLite backends via `LINEAGE_BRIDGE_STORAGE__BACKEND`
- 🔗 **Native UC Lineage** (v0.5.0): External Lineage API integration with column-level lineage
- 🏷️ **Catalog Protocol v2** (v0.5.0): `CATALOG_TABLE` + `catalog_type` discriminator (UC/Glue/Google/DataZone)

[View Full Changelog →](reference/changelog.md)

## Documentation

- **[Getting Started](getting-started/index.md)** - Installation, configuration, quickstart
- **[User Guide](user-guide/index.md)** - CLI tools, UI, graph visualization
- **[Catalog Integration](catalog-integration/index.md)** - UC, Glue, Google Data Lineage
- **[API Reference](api-reference/index.md)** - REST API with interactive explorer
- **[Demos](demos/index.md)** - Deploy full-stack demos for each catalog
- **[Architecture](architecture/index.md)** - Deep dive into internals
- **[Troubleshooting](troubleshooting/index.md)** - Common issues and solutions

## Community

- **[GitHub](https://github.com/takabayashi/lineage-bridge)** - Star the project, report issues
- **[Contributing](contributing/index.md)** - Development setup, testing, PR guidelines
- **[License](https://github.com/takabayashi/lineage-bridge/blob/main/LICENSE)** - Apache 2.0
