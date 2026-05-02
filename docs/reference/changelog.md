# Changelog

All notable changes to LineageBridge are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

See the [latest commits](https://github.com/takabayashi/lineage-bridge/commits/main) for work in progress.

## [0.5.0] - 2026-05-02

**Modularity refactor: services layer, pluggable storage, watcher as an independent service. Plus a stack of UX + reliability fixes that fell out during validation.**

### Added

- **Service layer (`lineage_bridge/services/`)** — single entry point for extraction / enrichment / push (`run_extraction`, `run_enrichment`, `run_push`). UI, API, and watcher all call into it with the same `ExtractionRequest` / `EnrichmentRequest` / `PushRequest` shape. (Phase 1A, ADR-020)
- **Pluggable storage layer (`lineage_bridge/storage/`)** with three backends:
  - `memory` (default) — process-local, ephemeral
  - `file` — JSON files + `flock`-guarded writes
  - `sqlite` — single `storage.db`, WAL mode, versioned-SQL migrations, durable across restarts; recommended for the watcher.
  Selected via `LINEAGE_BRIDGE_STORAGE__BACKEND={memory,file,sqlite}`. (Phases 1C + 2F, ADR-022)
- **Catalog protocol v2** — `UC_TABLE` / `GLUE_TABLE` / `GOOGLE_TABLE` collapsed into one `CATALOG_TABLE` node type with a `catalog_type` discriminator (`UNITY_CATALOG / AWS_GLUE / GOOGLE_DATA_LINEAGE / AWS_DATAZONE`). Adding a new catalog = one file in `catalogs/`. Push surface unified through `services.run_push(PushRequest)`. (Phase 1B, ADR-021 — clean break, no migration; old graph JSON raises a `ValidationError` on load)
- **Watcher as an independent peer service** — split into `WatcherService` (pure-logic state machine, no threading), `WatcherRunner` (asyncio loop + persistence), `WatcherRepository` (memory + sqlite backends), and `api/routers/watcher.py` (6 endpoints: start / stop / status / events / history / list / deregister). Two run modes: `lineage-bridge-watch` daemon (production, survives UI restarts) or in-process API task (development). UI now polls `GET /api/v1/watcher/{id}/*` via `httpx` and holds only the `watcher_id` string. (Phase 2G, ADR-023)
- **`POST /api/v1/push/{provider}`** endpoint — the API gains feature parity with the UI's "Push to X" buttons. (Phase 1A)
- **Orchestrator phase abstraction** — each phase implements `Phase.execute(ctx)` and is independently testable. (Phase 1D)
- **Per-demo credential cache accumulation** — switching `.env` between UC / Glue / BQ / DataZone demos no longer wipes the previous demo's credentials. The `~/.lineage_bridge/cache.json` deep-merges per-cluster / per-env credential dicts; the demo provision scripts mirror Databricks workspace + AWS region into the cache too.
- **Metrics enrichment for every node type** (with `--metrics`): Telemetry-based for topics / connectors / Flink jobs; consumer-group lag → `metrics_total_lag`; tableflow inherits from upstream topic; catalog tables get `metrics_active` from the most recent of `last_modified_time` / `updated_at` / `update_time` / `create_time`; ksqlDB queries from state. New `--metrics` and `--metrics-lookback-hours` CLI flags.
- **DLQ topic wiring** — sink connectors now expose their internal `lcc-XXXXX` resource ID via `expand=info,status,id`. The Connect extractor emits a placeholder `dlq-{lcc-id}` topic + `PRODUCES` edge, which merges with the real topic on the kafka_admin pass. DLQ topics no longer appear as orphan nodes; renderer surfaces a red "D" badge variant.
- **Sidebar legend variants** — per-catalog brand rows (UC / Glue / BigQuery / DataZone) plus "DLQ topic" and "Topic with schema" badge rows when present in the graph.
- **`Settings.api_url`** — UI uses this to find the API (default `http://127.0.0.1:8000`); set when API runs in a separate container.
- **AWS partition-aware Glue console deeplinks** — `_console_host(region)` maps GovCloud (`us-gov-*`) and China (`cn-*`) regions to the right console hostname.
- **Storage conformance suite** — same 24 tests run against memory + file + sqlite (72 cases), plus 12 watcher-repository tests × memory + sqlite (24 cases), plus 11 sqlite-specific tests + 2 sqlite API-integration tests.

### Changed

- **UI sidebar decomposed** — the 1,077-LOC `ui/sidebar.py` split into `ui/sidebar/{__init__,connection,scope,credentials,actions,filters}.py`. CSS extracted from `ui/app.py` to `ui/static/styles.css`. Sample graph promoted from imperative builder to bundled `ui/static/sample_graph.json`. (Phase 2E)
- **Per-catalog console deep-link labels** — was hardcoded "Open in BigQuery" on every `CATALOG_TABLE` (visible on UC + Glue tables too); now dispatches per `catalog_type` (`Open in Unity Catalog` / `Open in AWS Glue` / `Open in BigQuery`). Duplicate Glue button removed.
- **Per-catalog brand icons reach the actual graph nodes** — `render_graph_raw` and `render_graph` now wire `icon_for_node(node)` so UC / Glue / BigQuery / DataZone tables render with their brand icon (was: every catalog table got the generic database icon).
- **DataZone live integration tests gate on `iam:SimulatePrincipalPolicy`** — when the caller lacks `datazone:CreateFormType` / `CreateAssetType`, the tests skip cleanly with the IAM diff in the skip reason rather than failing on `AccessDeniedException`. The graceful-degradation path is still exercised by the manual `provider.push_lineage` smoke.
- **Dataplex live integration test** uses deterministic `GET-by-name` instead of `LIST entries → find by FQN` to avoid the eventual-consistency flake on Dataplex Catalog list endpoints.

### Fixed

- **Glue catalog deep-link region** — `build_url` no longer silently falls back to `us-east-1` when the node has no `aws_region`. `build_node` now stamps `aws_region` from the configured provider region (or Tableflow CI region) at build time, and `build_url` returns `None` if no region is available anywhere (no more wrong-region buttons).
- **Google catalog table_name parity** — `GoogleLineageProvider.build_node` was producing different node IDs than `connect.py:_build_google_tables` for the same logical table (split-on-dot vs replace-on-dot). Fixed to mirror the Connect path.
- **ksqlDB extraction 401** — when `Settings.ksqldb_api_key` is unset, the processing phase now falls back to the cached `lineage-bridge-ksqldb-{env_id}` provisioned SA key. Same pattern applied to the Tableflow phase.
- **CSS escape bug** — `ui/static/styles.css` header comment contained the literal text `</style>`, which the browser parsed as closing the wrapping `<style>` opened by `st.markdown`, dumping the rest of the CSS as plain text on the page.
- **Streamlit `cluster_select` widget warning** — the multiselect was passing both `key=` and `default=`. Pre-seed `st.session_state["cluster_select"]` instead, drop the `default` arg, prune stale labels.
- **Watcher push path** — the old `WatcherEngine` imported `run_glue_push` / `run_lineage_push` (both deleted in Phase 1B); the new service uses `services.run_push` via `PushRequest`, the same path the UI + API hit.

### Removed

- **`lineage_bridge/watcher/engine.py:WatcherEngine`** — replaced by `services.WatcherService` + `services.WatcherRunner`. The threading wrapper is gone (the daemon owns its asyncio loop directly). The `_use_audit_log` private property is gone (mode is explicit in `WatcherConfig.mode`). The CLI no longer calls a private `_run_loop()` directly.
- **`run_glue_push` / `run_lineage_push`** wrappers in `extractors/orchestrator.py` — the catalog protocol v2 dispatches via `services.run_push(PushRequest)` instead. (Phase 1B)
- **`UC_TABLE` / `GLUE_TABLE` / `GOOGLE_TABLE` NodeType members** — collapsed to `CATALOG_TABLE` + `catalog_type`. (Phase 1B; clean break per ADR-021)

### Tests
- 813 → **892** baseline pytest, 7 skipped (live cloud integration; opt-in via `LINEAGE_BRIDGE_*_INTEGRATION=1`).
- New conformance suite covers memory + file + sqlite for graphs / tasks / events; memory + sqlite for watchers.
- Lint clean (ruff, line length 100).

## [0.4.1] - 2026-05-01

**New catalog integrations, brand-icon refresh, BigQuery node enhancement, and CI/docs hardening.**

### Added

- **AWS DataZone provider** (`AWSDataZoneProvider`, `DataZoneAssetRegistrar`): registers Kafka topics as DataZone assets with schema and posts OpenLineage events via `post_lineage_event`. Mirrors the Google Dataplex / Data Lineage architecture for AWS.
- **Dataplex Catalog asset registration** (`DataplexAssetRegistrar`): each Kafka topic becomes a Dataplex entry with the same FQN as the lineage event, so the BigQuery Lineage tab shows column metadata on upstream Confluent nodes (events alone don't carry schema — Google strips facets at storage).
- **Rich `lineage_bridge.upstream_chain` payload** in UC TBLPROPERTIES, UC bridge table (`chain_json` column), Glue Parameters, and Glue Description: full multi-hop chain including Flink/ksqlDB SQL, intermediate topics, source connectors, and per-topic schema fields. Capped per catalog's value-size limit.
- **Multi-hop OpenLineage push** for Google: every Job-event (source connectors, Flink, ksqlDB, sinks) is pushed so the Lineage tab can walk transitively from a BQ table back to the source topics.
- **Live integration tests**: `tests/integration/test_gcp_dataplex_integration.py` (gated by `LINEAGE_BRIDGE_GCP_INTEGRATION=1`) and `tests/integration/test_aws_datazone_integration.py` (gated by `LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION=1`). Run via `make test-integration-dataplex` / `make test-integration-datazone`.
- **"Push to DataZone" button** in the Streamlit publish panel, gated on `LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID` + `LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID`.
- **Official brand icons** for Kafka, Flink, Databricks (Unity Catalog), Google BigQuery, and AWS Glue graph nodes. Other node types keep their geometric icons. New `_IconSpec` config supports `logo` and `tile` render modes plus an optional `fill_override` for recolouring monochrome marks.
- **Shell-based integration test harnesses** for all three demo environments: `scripts/integration-test-uc.sh` (7 tests), `scripts/integration-test-glue.sh` (8 tests), `scripts/integration-test-bigquery.sh` (9 tests). Each validates extraction, enrichment, catalog push, API, watcher, and Docker, with `--skip-docker` for CI.
- **Documentation polish**: prominent docs-site link on the README, deep-links from catalog feature bullets to per-catalog guides, GCP environment variables documented, `lineage-bridge-api` entrypoint surfaced.

### Changed

- Shared OpenLineage namespace normalizer (`api/openlineage/normalize.py`) — used by both Google and DataZone providers, parametrised by allowlist (`{bigquery}` for Google, `{kafka, aws}` and `{bigquery, aws}` for DataZone).
- Shared upstream-chain builder (`catalogs/upstream_chain.py`) — single source of truth for chain shape, used by all four catalogs.
- `google-auth` is now a hard dependency (was previously imported lazily and silently failed if missing).
- **BigQuery connector lineage**: BigQuery sink connectors now synthesise per-topic `GOOGLE_TABLE` nodes in `clients/connect.py` so the publish UI surfaces them directly without requiring Tableflow.

### Fixed

- **Deploy Documentation workflow**: 9 broken cross-reference links in `docs/how-to/index.md`, `docs/reference/glossary.md`, and `docs/demos/index.md` retargeted to existing pages so `mkdocs build --strict` passes.
- **CI lint**: import ordering + formatter applied to `lineage_bridge/api/routers/tasks.py` (had been failing `ruff check` on every PR).
- **IAM trust policy** corrections and Interactive API Explorer fix (use built-in Swagger UI).
- **7 bugs** discovered during integration-test validation (full list in `docs/INTEGRATION_TESTS_BUG_FIXES.md`).

## [0.4.0] - 2024-12-15

**Major Features:**

- Multi-demo infrastructure support for parallel Confluent environments
- Google Data Lineage API provider integration
- OpenLineage API server with FastAPI backend
- Comprehensive MkDocs Material documentation site

### Added

- OpenLineage API server at `/api/v1/lineage` with Swagger UI at `/docs`
- Google Data Lineage provider for GCP integration
- Dataset Lineage API documentation and reference
- Interactive API explorer with built-in Swagger UI
- MkDocs Material documentation infrastructure (port 8001)
- Getting Started guide with tab-based examples
- User Guide documentation
- Architecture and Troubleshooting documentation
- Catalog Integration documentation with diagrams
- API Reference documentation with practical examples
- Multi-demo infrastructure for testing multiple catalog providers
- Auto-provisioning of Confluent Cloud API keys via `make ui`
- Interactive demo credential setup with CLI auto-detection
- Auto-generate `.env` after demo provisioning
- Auto-launch UI after demo provisioning
- Demo architecture diagram
- Service principal support for Databricks notebook jobs
- IAM role trust policy fixes for AWS integration

### Changed

- Refactored API Reference with tabs and practical examples
- Refactored Getting Started with human-friendly text
- Refactored Catalog Integration with improved diagrams
- Switched from custom Scalar to built-in Swagger UI
- Changed docs port from 8000 to 8001 to avoid API server conflict
- Improved CLI prerequisite auto-installation during demo setup
- Enhanced post-provisioning messages with available commands

### Fixed

- IAM trust policy for self-assume role in AWS Glue integration
- OAuth secret creation error handling in Databricks setup
- Databricks CLI auth environment variable parsing
- Docker workflow to use correct Dockerfile path
- Broken links in Google Data Lineage documentation
- Import sorting in generate-diagram script

## [0.3.0] - 2024-11-20

**Major Features:**

- Change-detection watcher with REST polling and debounced re-extraction
- AWS Glue catalog provider integration
- Databricks lineage discovery and push-to-UC capabilities
- Interactive demo infrastructure with Terraform

### Added

- Change-detection watcher CLI (`lineage-bridge-watch`)
- REST polling mode for change detection (10s interval, 30s debounce)
- Audit log Kafka consumer mode (retained for future use)
- AWS Glue catalog provider with metadata enrichment
- Format-based catalog matching for Tableflow integration
- Databricks lineage discovery via SQL Statement Execution API
- Push lineage metadata to Unity Catalog as table properties and comments
- PostgreSQL RDS sink connector for enriched orders demo
- Flink compute pool and SQL statements in demo infrastructure
- Self-contained demo infrastructure (Kafka → Tableflow → S3 → Databricks UC)
- Docker multi-stage build with watcher service
- `make watch` and `make docker-watch` targets
- Dark mode support for labels and UI components

### Changed

- Separated extract and enrich into independent operations
- Redesigned node icons with representative shapes, color-coded by system
- Switched Makefile to use `uv` package manager
- Moved Docker files to `infra/docker/` directory
- Moved scripts to `infra/` directory
- Removed key provisioning UI in favor of CLI-based auto-provisioning
- Switched to single-environment selection in UI

### Fixed

- Flink SQL parser to preserve dots inside backtick-quoted identifiers
- Unity Catalog integration to handle real Confluent API format
- Dot-to-underscore mapping for UC table names
- Schema definition display in topic detail panel
- Neighbor display in node details
- Demo `.env` output to match `settings.py` field names

### Removed

- Unused infra scaffolding (kept `infra/demo` only)
- Databricks-to-Kafka publish job and topic

## [0.2.0] - 2024-10-15

**Major Features:**

- Catalog provider framework with extensible protocol
- Databricks Unity Catalog provider
- Custom vis.js graph component with interactive features
- Metrics API integration for real-time throughput

### Added

- Catalog provider framework (`catalogs/protocol.py`)
- Databricks Unity Catalog provider with enrichment and lineage push
- Custom vis.js Streamlit component for graph visualization
- Sugiyama-style DAG layout with minimal edge crossings
- Shift+drag region selection in graph UI
- Confluent Metrics API integration for throughput data
- Rich HTML tooltips with node metadata
- Deep links to Confluent Cloud, Databricks, and AWS consoles
- Comprehensive unit tests (208 tests total)
- Per-extractor timeouts and graph validation
- Kafka protocol fallback for cluster connectivity
- CI coverage gate (80% threshold)
- Docker pipeline with healthchecks

### Changed

- Decomposed `app.py` into state, discovery, extraction, and sidebar modules
- Expanded graph renderer tests to 104 test cases
- Unified URL dispatch for catalog providers
- UX redesign with multi-credential support

### Fixed

- Dark mode: theme-safe colors for CSS, tooltips, and detail panel
- Tableflow extraction and mapping logic
- Flink CTAS parsing for CREATE TABLE AS SELECT statements
- Connector extraction and external dataset handling
- Consumer group membership edges
- Graph layout edge crossing optimization

### Added Documentation

- Architecture decision log (ADR) with Phase 1 tradeoffs
- Agent crew model and master plan in `docs/`
- CLAUDE.md with project conventions and architecture
- Apache 2.0 license and CONTRIBUTING.md

## [0.1.0] - 2024-09-01

**Initial Release**

- Confluent Cloud lineage extraction via REST v3 APIs
- Streamlit UI with interactive graph visualization
- Support for Kafka topics, connectors, Flink jobs, ksqlDB queries
- Schema Registry integration for Avro/Protobuf/JSON schemas
- Stream Catalog integration for tags and business metadata
- Consumer group membership tracking
- Multi-environment and multi-cluster support
- Encrypted local JSON cache
- Auto-provisioning of cluster-scoped API keys via Confluent CLI

### Extraction Clients

- KafkaAdmin: topics and consumer groups
- Connect: source and sink connectors
- Flink: SQL statement parsing
- ksqlDB: persistent query parsing
- SchemaRegistry: schema enrichment
- StreamCatalog: tag and metadata enrichment

### UI Features

- Interactive directed graph with drag and zoom
- Click-to-inspect detail panel
- Search by qualified name
- Export graph as JSON
- Connection settings and credential management

---

## Migration Guides

### Upgrading to v0.4.0

**Breaking Changes:**

- Documentation moved from port 8000 to 8001 to avoid API server conflict
- Docker file paths changed to `infra/docker/` - update any custom compose files

**New Features:**

- OpenLineage API available at `http://localhost:8000/api/v1/lineage`
- Interactive API docs at `http://localhost:8000/docs`
- Google Data Lineage provider enabled via `LINEAGE_BRIDGE_GOOGLE_PROJECT_ID`

### Upgrading to v0.3.0

**Breaking Changes:**

- Environment variable format changed: `CLUSTER_CREDENTIALS` now uses JSON map instead of flat `KAFKA_API_KEY`
- Docker files moved from root to `infra/docker/` - update references in CI/CD
- Scripts moved to `infra/` directory

**New Features:**

- Change-detection watcher: `uv run lineage-bridge-watch`
- AWS Glue support: set `LINEAGE_BRIDGE_AWS_REGION` in `.env`
- Databricks lineage push: automatically writes to UC table properties

**Migration Steps:**

1. Update `.env` file format (see `.env.example`)
2. Update Docker Compose paths: `docker-compose.yml` → `infra/docker/docker-compose.yml`
3. Run `make format` to apply new code style rules

### Upgrading to v0.2.0

**Breaking Changes:**

- `app.py` decomposed into multiple modules - custom imports need updates
- Graph component switched from `streamlit-agraph` to custom vis.js component

**New Features:**

- Catalog providers: add Databricks or Glue credentials to `.env`
- Metrics API: enriches graphs with throughput data (opt-in)

**Migration Steps:**

1. Install updated dependencies: `uv pip install -e ".[dev]"`
2. Update any custom UI code to use new module structure
3. Run `make test` to validate changes

---

## Links

- [GitHub Releases](https://github.com/takabayashi/lineage-bridge/releases)
- [Issue Tracker](https://github.com/takabayashi/lineage-bridge/issues)
- [Contributing Guide](https://github.com/takabayashi/lineage-bridge/blob/main/CONTRIBUTING.md)
