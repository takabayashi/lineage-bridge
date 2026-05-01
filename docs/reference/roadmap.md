# Roadmap

LineageBridge development roadmap, including completed milestones, current work, and planned features.

---

## Completed Milestones

### Phase 1: Confluent Lineage Extractor + Streamlit UI

**Status:** Completed in v0.1.0 (September 2024)

- [x] Confluent Cloud API client framework with async/retry/pagination
- [x] KafkaAdmin: topics and consumer groups
- [x] Connect: source and sink connectors with external dataset extraction
- [x] Flink: SQL statement parsing for input/output topics
- [x] ksqlDB: persistent query parsing for lineage edges
- [x] SchemaRegistry: schema enrichment with `HAS_SCHEMA` edges
- [x] StreamCatalog: tags and business metadata enrichment
- [x] NetworkX graph model with node/edge types
- [x] Streamlit UI with interactive visualization
- [x] Multi-environment and multi-cluster support
- [x] Encrypted local JSON cache
- [x] Auto-provisioning of cluster-scoped API keys

---

### Phase 2: Catalog Provider Framework

**Status:** Completed in v0.2.0 (October 2024)

- [x] Extensible `CatalogProvider` protocol
- [x] Databricks Unity Catalog provider
  - [x] Table metadata enrichment via Databricks API
  - [x] Deep links to UC table UI
  - [x] Lineage push as table properties and comments
- [x] Custom vis.js Streamlit component
- [x] Sugiyama layout for hierarchical DAG rendering
- [x] Confluent Metrics API integration for throughput data
- [x] Rich HTML tooltips with metadata
- [x] Deep links to Confluent Cloud console
- [x] Comprehensive test suite (208 tests)

---

### Phase 3: UI Decomposition + UX Improvements

**Status:** Completed in v0.2.0 (October 2024)

- [x] Decompose `app.py` into modular components:
  - [x] `state.py` - session state management
  - [x] `discovery.py` - environment/cluster discovery
  - [x] `extraction.py` - extraction progress UI
  - [x] `sidebar.py` - connection settings and filters
- [x] Expand graph renderer test coverage to 104 tests
- [x] Unified URL dispatch for all catalog providers
- [x] Dark mode support with theme-safe colors
- [x] Shift+drag region selection in graph UI

---

### Phase 4: Polish + Hardening

**Status:** Completed in v0.2.0 (October 2024)

- [x] CI/CD pipeline with coverage gate (80% threshold)
- [x] Docker multi-stage build with healthchecks
- [x] Per-extractor timeouts and error handling
- [x] Graph validation (cycle detection, orphan nodes)
- [x] Kafka protocol fallback for cluster connectivity
- [x] Architecture Decision Records (ADR)
- [x] Apache 2.0 license and open source preparation
- [x] CONTRIBUTING.md and code cleanup

---

### Post-v0.2.0: UC Integration + Change Detection

**Status:** Completed in v0.3.0 (November 2024)

- [x] AWS Glue catalog provider
  - [x] Table metadata enrichment via boto3
  - [x] Lineage push as table parameters
  - [x] Deep links to Glue console
- [x] Databricks lineage discovery via SQL Statement Execution API
- [x] Format-based catalog matching for Tableflow
- [x] Change-detection watcher with REST polling (10s interval)
- [x] Debounced re-extraction (30s cooldown)
- [x] Docker watcher service
- [x] `make watch` and `make docker-watch` targets
- [x] Interactive demo infrastructure with Terraform
- [x] Self-contained demo (Kafka → Tableflow → S3 → Databricks UC)
- [x] Flink compute pool and SQL statements in demo
- [x] PostgreSQL RDS sink connector

---

### v0.4.0: Multi-Catalog + API + Documentation

**Status:** Completed in v0.4.0 (December 2024)

- [x] Google Data Lineage API provider
- [x] OpenLineage API server with FastAPI backend
- [x] Interactive API explorer with Swagger UI at `/docs`
- [x] MkDocs Material documentation site
  - [x] Getting Started guide
  - [x] User Guide
  - [x] Architecture documentation
  - [x] Catalog Integration guide
  - [x] API Reference
  - [x] Troubleshooting guide
- [x] Multi-demo infrastructure for parallel catalog testing
- [x] Auto-provisioning of Confluent API keys via `make ui`
- [x] Interactive credential setup with CLI auto-detection
- [x] Demo architecture diagram
- [x] Service principal support for Databricks jobs

---

## Current Work (v0.4.x)

### Documentation Completion

- [x] Reference documentation
  - [x] Changelog
  - [x] Glossary
  - [x] Roadmap
- [ ] Tutorial series
  - [ ] "First Lineage Graph in 5 Minutes"
  - [ ] "Connecting to Unity Catalog"
  - [ ] "Setting Up Change Detection"
- [ ] Example gallery
  - [ ] Common lineage patterns
  - [ ] Integration recipes
  - [ ] Custom provider template

### Quality Improvements

- [ ] Increase test coverage to 90%
- [ ] Add integration tests for all catalog providers
- [ ] Performance benchmarking suite
- [ ] Load testing for large environments (1000+ topics)

---

## Planned Features

### v0.5.0: Enhanced Analytics + Comparison

**Target:** Q2 2025

- [ ] Graph comparison and diff view
  - [ ] Side-by-side graph comparison
  - [ ] Highlight added/removed/modified nodes and edges
  - [ ] Timeline view of graph evolution
- [ ] Impact analysis
  - [ ] Upstream/downstream dependency tree
  - [ ] "What breaks if I delete this topic?"
  - [ ] Consumer lag analysis with Metrics API
- [ ] Advanced filtering
  - [ ] Filter by node type, system, environment
  - [ ] Path finding between two nodes
  - [ ] Subgraph extraction
- [ ] Export formats
  - [ ] GraphML for Gephi/Cytoscape
  - [ ] DOT for Graphviz
  - [ ] CSV edge list
  - [ ] OpenLineage JSON

### v0.5.0: Additional Catalog Providers

- [ ] Snowflake Data Governance
- [ ] Google BigQuery Data Lineage (expand from current GCP provider)
- [ ] Azure Purview
- [ ] Atlan
- [ ] Collibra

### v0.6.0: Schema Evolution Tracking

**Target:** Q3 2025

- [ ] Schema Registry version history
- [ ] Schema compatibility tracking
- [ ] Breaking change detection
- [ ] Schema migration assistant
- [ ] Field-level lineage (experimental)

### v0.7.0: Advanced Stream Processing

**Target:** Q4 2025

- [ ] Kafka Streams topology parsing
- [ ] Confluent Cloud Stream Designer integration
- [ ] Flink DataStream API extraction (via job manager API)
- [ ] ksqlDB user-defined functions (UDF) tracking

### v1.0.0: Production-Ready Platform

**Target:** Q1 2026

- [ ] Multi-user authentication and RBAC
- [ ] Persistent graph storage (PostgreSQL backend)
- [ ] Scheduled extraction jobs
- [ ] Alerting and notifications
  - [ ] Email/Slack on lineage changes
  - [ ] Broken lineage detection
  - [ ] Schema drift alerts
- [ ] API rate limiting and caching
- [ ] Horizontal scaling support
- [ ] Kubernetes Helm chart
- [ ] Prometheus metrics export
- [ ] Grafana dashboard templates

---

## Community Requests

Have a feature request? [Open an issue](https://github.com/takabayashi/lineage-bridge/issues/new) with the `enhancement` label.

### Under Consideration

- [ ] Confluent Cloud Stream Lineage API integration (when available)
- [ ] Confluent Platform (on-prem) support
- [ ] Apache Kafka (vanilla) support
- [ ] Redpanda support
- [ ] Confluent Schema Linking lineage
- [ ] Cluster Linking lineage
- [ ] Data Contracts integration (Confluent, Avro IDL)
- [ ] Data quality metrics (Montecarlo, Great Expectations)
- [ ] Cost analysis (per-topic throughput → cost mapping)

---

## Research Topics

Exploratory work that may become features in future releases.

### Field-Level Lineage

Track column-level transformations through SQL queries and schema mappings. Challenges:
- Parsing complex SQL transformations (joins, aggregations, window functions)
- Mapping fields across different schema formats (Avro ↔ Parquet ↔ Delta)
- Handling schema evolution and breaking changes

### Real-Time Lineage Streaming

Push lineage updates to a Kafka topic for downstream consumption. Use cases:
- Real-time lineage indexing in search engines
- Live lineage monitoring dashboards
- Event-driven lineage validation pipelines

### AI-Powered Lineage Inference

Use LLMs to infer missing lineage from code analysis:
- Parse Kafka Streams applications for producer/consumer patterns
- Analyze microservice code for topic dependencies
- Suggest lineage connections based on naming conventions

### Lineage as Code

Define expected lineage in declarative YAML/JSON files:
- Validate actual lineage matches expected lineage
- Detect drift and breaking changes
- Version control lineage specifications

---

## Contribution Opportunities

Want to contribute? Here are some good first issues and areas where we'd love help:

### Good First Issues

- [ ] Add catalog provider for Snowflake
- [ ] Add catalog provider for Azure Purview
- [ ] Improve error messages with actionable suggestions
- [ ] Add loading spinners to UI extraction phases
- [ ] Write integration test for Glue provider
- [ ] Add Dark Mode toggle to UI sidebar

### Help Wanted

- [ ] Performance optimization for large graphs (1000+ nodes)
- [ ] GraphQL API in addition to REST
- [ ] Terraform module for AWS deployment
- [ ] GitHub Actions workflow for automated extraction
- [ ] Jupyter notebook examples and tutorials
- [ ] Video walkthrough and demo

### Documentation

- [ ] Translate documentation to other languages
- [ ] Add code examples in different programming languages
- [ ] Create architectural diagrams for each extraction phase
- [ ] Write blog posts about use cases and patterns

---

## Release Schedule

LineageBridge follows semantic versioning with roughly monthly minor releases and weekly patch releases as needed.

- **Patch releases (0.4.x):** Bug fixes, documentation updates, minor improvements
- **Minor releases (0.x.0):** New features, catalog providers, non-breaking changes
- **Major releases (x.0.0):** Breaking API changes, architecture overhauls

---

## Deprecation Policy

Features marked for deprecation will be supported for at least one minor version before removal. Deprecation notices are included in:

- Release notes in [Changelog](changelog.md)
- Runtime warnings when deprecated features are used
- Migration guides with recommended alternatives

---

## Feedback

We'd love to hear from you:

- [Open an issue](https://github.com/takabayashi/lineage-bridge/issues/new) for bugs or feature requests
- [Start a discussion](https://github.com/takabayashi/lineage-bridge/discussions) for questions or ideas
- Contribute via [pull request](https://github.com/takabayashi/lineage-bridge/blob/main/CONTRIBUTING.md)

---

## See Also

- [Changelog](changelog.md) - Version history and migration guides
- [Contributing Guide](https://github.com/takabayashi/lineage-bridge/blob/main/CONTRIBUTING.md) - Development setup and guidelines
- [GitHub Issues](https://github.com/takabayashi/lineage-bridge/issues) - Bug reports and feature requests
