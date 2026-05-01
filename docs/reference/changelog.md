# Changelog

All notable changes to LineageBridge are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

**New catalog integrations and richer push payloads.**

### Added

- **AWS DataZone provider** (`AWSDataZoneProvider`, `DataZoneAssetRegistrar`): registers Kafka topics as DataZone assets with schema and posts OpenLineage events via `post_lineage_event`. Mirrors the Google Dataplex / Data Lineage architecture for AWS.
- **Dataplex Catalog asset registration** (`DataplexAssetRegistrar`): each Kafka topic becomes a Dataplex entry with the same FQN as the lineage event, so the BigQuery Lineage tab shows column metadata on upstream Confluent nodes (events alone don't carry schema — Google strips facets at storage).
- **Rich `lineage_bridge.upstream_chain` payload** in UC TBLPROPERTIES, UC bridge table (`chain_json` column), Glue Parameters, and Glue Description: full multi-hop chain including Flink/ksqlDB SQL, intermediate topics, source connectors, and per-topic schema fields. Capped per catalog's value-size limit.
- **Multi-hop OpenLineage push** for Google: every Job-event (source connectors, Flink, ksqlDB, sinks) is pushed so the Lineage tab can walk transitively from a BQ table back to the source topics.
- **Live integration tests**: `tests/integration/test_gcp_dataplex_integration.py` (gated by `LINEAGE_BRIDGE_GCP_INTEGRATION=1`) and `tests/integration/test_aws_datazone_integration.py` (gated by `LINEAGE_BRIDGE_AWS_DATAZONE_INTEGRATION=1`). Run via `make test-integration-dataplex` / `make test-integration-datazone`.
- **"Push to DataZone" button** in the Streamlit publish panel, gated on `LINEAGE_BRIDGE_AWS_DATAZONE_DOMAIN_ID` + `LINEAGE_BRIDGE_AWS_DATAZONE_PROJECT_ID`.

### Changed

- Shared OpenLineage namespace normalizer (`api/openlineage/normalize.py`) — used by both Google and DataZone providers, parametrised by allowlist (`{bigquery}` for Google, `{kafka, aws}` and `{bigquery, aws}` for DataZone).
- Shared upstream-chain builder (`catalogs/upstream_chain.py`) — single source of truth for chain shape, used by all four catalogs.
- `google-auth` is now a hard dependency (was previously imported lazily and silently failed if missing).
- BigQuery sink connectors synthesise per-topic `GOOGLE_TABLE` nodes in `clients/connect.py` so the publish UI surfaces them and Tableflow isn't required for the BQ demo.

See the [latest commits](https://github.com/takabayashi/lineage-bridge/commits/main) for work in progress.

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
