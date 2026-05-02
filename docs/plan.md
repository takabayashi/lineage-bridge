# Master Plan — LineageBridge

**Goal:** Extend LineageBridge from Confluent-only lineage into a cross-platform lineage tool bridging Confluent Cloud, Databricks Unity Catalog, and AWS Glue.

**Architectural principle:** Adding a new catalog = one file in `catalogs/`, register in `__init__.py`. Zero changes to Tableflow, orchestrator, or UI.

---

## Phase 1: Catalog Provider Framework (COMPLETED 2026-04-04)

Committed in 3 commits. 304 tests, all passing.

**What was built:**
- `CatalogProvider` protocol + registry (`catalogs/`)
- Databricks UC provider (async enrichment with retry/backoff, deep links)
- AWS Glue provider (node building, URLs, enrich stub)
- Graph model: UC_TABLE, GLUE_TABLE, MATERIALIZES, SystemType.DATABRICKS/AWS
- Settings: databricks_workspace_url, databricks_token
- Tableflow: delegates to providers via `get_provider()`
- Orchestrator Phase 4b: discovers active providers, calls `provider.enrich(graph)`
- UI: styles, icons, colors, sample data for new types
- Tests: catalog registry, UC provider, Glue provider, cache, provisioner, orchestrator (37 tests)
- Infra: Terraform modules (Databricks + Confluent + AWS)
- Scripts: env-from-terraform.sh

**Commits:**
1. `adcdc76` — Catalog framework + providers + tests (Weaver + Lens)
2. `61a194a` — Tableflow/orchestrator wiring + UI + Phase 4b (Forge + Prism + Lens)
3. `3deb972` — Terraform infra + scripts (Anvil)

---

## Phase 2: Integration, UX Review & CI/CD (COMPLETED 2026-04-04)

**Goal:** Unified URL dispatch, UX audit, node_details coverage, harden CI.

| Workstream | Persona | Task | Depends on |
|------------|---------|------|------------|
| E | **Forge** | Add unified URL dispatcher in styles.py: `build_node_url(node)` routes to `provider.build_url()` | Phase 1 |
| E | **Lens** | Integration tests for orchestrator with catalog enrichment enabled | Forge E done |
| F | **Prism** | Full UX audit: usability, layout, interaction, accessibility, info hierarchy | Phase 1 |
| F | **Prism** | Catalog-specific UX: how UC/Glue tables appear in sidebar, node details, graph legend | Phase 1 |
| G | **Lens** | Tests for `ui/node_details.py` — mock Streamlit, test each NodeType (0% → 50%+) | Phase 1 |
| H | **Anvil** | Extend CI: coverage report upload, coverage gate | Phase 1 |
| H | **Anvil** | Add integration test workflow (manual trigger, live envs) | Phase 1 |
| H | **Anvil** | Docker image build + push to ghcr.io on tag | Phase 1 |

**Review:** Sentinel reviews all before PR.

**Commits:**
1. `50d184e` — Unified URL dispatch, GLUE_TABLE details, node_details tests (Forge + Lens)
2. `102af37` — CI coverage gate, integration workflow, Docker pipeline, UX audit (Anvil + Prism)

---

## Phase 3: UI Decomposition + UX Improvements (COMPLETED 2026-04-04)

**Goal:** Break up app.py monolith (1517 lines), apply UX findings from Phase 2.

| Workstream | Persona | Task |
|------------|---------|------|
| I | **Blueprint** | Design decomposition: app.py → app.py (~200), sidebar.py, extraction.py, graph_panel.py, state.py |
| I | **Weaver** | Extract functions into new modules (structural refactor only) |
| I | **Prism** | Guide implementation of top UX improvements from Phase 2 audit |
| I | **Lens** | Add catalog provider UI controls in sidebar |
| J | **Lens** | Test graph_renderer: `_compute_dag_layout`, `render_graph_raw`, tooltips for all types (48% → 80%+) |

**Review:** Sentinel validates no regressions after refactor.

**Commits:**
1. `e3a8a98` — Decompose app.py into state, discovery, extraction, sidebar (Weaver)
2. `d1bd809` — Expand graph renderer tests to 104 (Lens)

---

## Phase 4: Polish & Hardening (COMPLETED 2026-04-04)

**Goal:** Coverage targets, validation, E2E testing. Tag v0.2.0.

**What was built:**
- Per-extractor timeout: `asyncio.wait_for()` with 120s ceiling in `_safe_extract()`
- Graph validation: `LineageGraph.validate()` detects orphan nodes and dangling edges
- Orchestrator calls `graph.validate()` post-extraction, logs warnings
- Kafka admin protocol fallback: 100% coverage (was 78%)
- 408 tests, 70% overall coverage (target was 65%)

**Commits:**
1. `ad417c5` — Per-extractor timeouts, graph validation, Kafka fallback tests (Weaver + Lens)
2. `d68f760` — Dark mode fix: theme-safe colors for CSS, tooltips, and detail panel (Prism)
3. `862f5b8` — CI fix: resolve all lint errors, fix Dockerfile build order (Anvil)
4. `3c03955` — Add pytest-cov to dev dependencies (Anvil)

**Release:** `v0.2.0` tagged and pushed. CI + Docker workflows green.

---

## Post-v0.2.0: UC Integration Fixes (2026-04-06)

**Problem:** Live testing against Confluent Tableflow API revealed format mismatches:
1. API returns `spec.config.kind: "Unity"` (not `spec.catalog_type: "UNITY_CATALOG"`)
2. Config is flat (not nested under `unity_catalog`)
3. Confluent replaces dots with underscores in UC table names

**Fixes:**
- `tableflow.py`: Added `_KIND_TO_CATALOG_TYPE` mapping, read config from `spec.config`
- `databricks_uc.py`: Handle flat config format, `topic_name.replace(".", "_")`
- Test fixtures updated to match real API format

**Test coverage improvements:**
- Added tests for flat API config format, dot-to-underscore mapping
- Added retry/error path tests (429 retry, 503 exhaust, HTTP errors, unexpected status)
- `databricks_uc.py` coverage: 84% → 99%

**Commits:**
1. `1f48e50` — Fix UC integration: handle real API format and dot-to-underscore mapping
2. `0f30364` — Add Flink API key outputs and fix postgres sink connector topic reference

---

## Post-v0.2.0: Feature Additions (2026-04-06 — 2026-04-07)

### Databricks Lineage & SQL Push
- `clients/databricks_discovery.py`: Workspace + catalog discovery
- `clients/databricks_sql.py`: SQL Statement Execution API client
- Lineage push to UC via `ALTER TABLE SET TBLPROPERTIES` + `COMMENT ON TABLE`
- Separate extract and enrich into independent orchestrator operations

### AWS Glue Enrichment
- Full `enrich()` implementation with boto3 (get_table metadata)
- `push_lineage()` via update_table parameters
- Format-based catalog matching (DELTA → UC, ICEBERG → Glue)

### Change-Detection Watcher
- `watcher/engine.py`: REST polling (10s) + debounced re-extraction (30s cooldown)
- `watcher/cli.py`: CLI entry point (`lineage-bridge-watch`)
- `ui/watcher.py`: Watcher toggle in Streamlit sidebar
- `models/audit_event.py`: Audit log event parsing (retained for future Kafka consumer)

### UI & UX Improvements
- Node icon redesign: representative shapes, color-coded by system
- Dark mode support (theme-safe colors for CSS, tooltips, detail panel)
- Schema definition display in topic detail panel
- Short node labels for cleaner graph rendering
- Canvas border and detail panel container improvements
- Removed key provisioning UI, switched to single-env selection

### Infrastructure
- Docker multi-stage build + `.dockerignore` + watcher service in compose
- Makefile: watch and docker-watch targets
- Flink SQL parser fix: preserve dots inside backtick-quoted identifiers

**Key commits:**
- `bf85eae` — Databricks lineage discovery, push-to-UC, short node labels
- `7ebaed6` — AWS Glue integration, format-based catalog matching
- `64dbc98` — Change-detection watcher with audit log and REST polling modes
- `b74aac8` — Remove key provisioning UI, single-env selection
- `f7697ba` — Dark mode, canvas border, detail panel, clusters metric
- `34bc1b1` — Docker multi-stage build, .dockerignore, watcher service
- `080f520` — Provisioner test script, Databricks discovery tests

**Test suite:** 492 tests passing, 70% coverage.

---

## v0.3.0 Housekeeping (2026-04-07)

**What was done:**
- Version sync: `__init__.py` aligned to `0.3.0` (was `0.1.0`)
- ADR-009 updated: Glue `enrich()` no longer a stub — marked as superseded
- Terraform destroy ordering: `time_sleep` delay before provider integration deletion (fixes 409 Conflict)
- Terraform idempotent CTAS: `DROP TABLE IF EXISTS` before Flink CTAS statements
- Makefile: robust `terraform output` handling during partial destroys
- Docker: healthchecks for all compose services (extract, ui, watch)
- Added `py.typed` marker for PEP 561 compliance
- Master plan: added "What's Next" roadmap

---

## OpenLineage API (COMPLETED 2026-04-14)

**Goal:** Expose Confluent stream lineage as an OpenLineage-compatible REST API. Bridge the gap between Confluent's lack of a lineage API and external catalogs that speak OpenLineage.

**What was built:**

### Phase A: OpenLineage Foundation
- OpenLineage Pydantic v2 models (RunEvent, Job, Dataset, Facets)
- Bidirectional translator: `graph_to_events()` / `events_to_graph()`
- Custom Confluent facets (ConfluentKafkaDatasetFacet, ConfluentConnectorJobFacet)

### Phase B: API Core
- FastAPI app factory with optional API key auth
- In-memory stores: GraphStore, EventStore
- 6 routers: meta, lineage, datasets, jobs, graphs, tasks
- 25 endpoints covering CRUD, traversal, views, and OpenLineage events
- Two graph views: Confluent-only and enriched (cross-platform)

### Phase C: Bidirectional Ingestion
- POST /lineage/events accepts OpenLineage events from external catalogs
- Events are merged into graph store via events_to_graph translator
- Supports glob-pattern filtering on namespace and job name

### Phase D: Google Data Lineage Provider
- GoogleLineageProvider implementing CatalogProvider protocol
- BigQuery REST API for metadata enrichment
- Google Data Lineage API for pushing lineage (ProcessOpenLineageRunEvent)
- Auth via Application Default Credentials (google-auth)
- New enums: NodeType.GOOGLE_TABLE, SystemType.GOOGLE
- UI styles, icons, colors, and sample data for Google tables

### Phase E: Async Task System
- TaskStore for tracking extraction/enrichment operations
- POST /tasks/extract and POST /tasks/enrich (return 202 + task_id)
- Background asyncio tasks with progress tracking
- GET /tasks/{task_id} for polling status

### Phase F: Documentation & Polish
- API guide: docs/api-guide.md (use cases, endpoint reference, curl examples)
- OpenAPI spec: docs/openapi.yaml (25 endpoints, 39 schemas)
- ADR-015 (OpenLineage API) and ADR-016 (Google Data Lineage)
- Makefile: `api` and `openapi` targets
- 609 tests, all passing

**Key design decisions:**
- ADR-015: OpenLineage-compatible REST API (see docs/decisions.md)
- ADR-016: Google Data Lineage provider (see docs/decisions.md)

---

## What's Next

Potential improvement areas, roughly ordered by impact:

| Area | Priority | Description |
|------|----------|-------------|
| **Test coverage** | High | Flink/ksqlDB SQL parsers (726 LOC, complex regex, no direct tests) |
| **Proxy-safe tests** | Medium | Strip proxy env vars in conftest.py — tests fail when ALL_PROXY is set |
| **Watcher circuit breaker** | Medium | No backoff when Confluent API is consistently failing |
| **Docker API service** | Medium | Add API service to docker-compose for containerized deployment |
| **Cross-process watcher stop** | Low | API stop endpoint only works for in-process runners (see Phase 2G ADR follow-up) |
| **Postgres / S3 storage backends** | Low | Pluggable per ADR-022; add when there's a real production driver |

---

## Refactor (2026-05-01 — 2026-05-02)

A multi-phase modularity refactor — see `docs/plan-refactor.md` for the original design and ADRs 020-023 in `docs/decisions.md` for the rationale. Phases 0/1A/1B/1C/1D/2E/2F/2G/3I/3G shipped on `refactor/phase-1`.

| Phase | Scope | Outcome |
|-------|-------|---------|
| **0** | ADRs 020-023 (services layer, catalog protocol v2, pluggable storage, watcher service) | Documented |
| **1A** | Services package + UI/API parity | One `ExtractionRequest` / `EnrichmentRequest` / `PushRequest` shape, both UI and API call into `services/` |
| **1B** | Catalog protocol v2 — collapse `UC_TABLE` / `GLUE_TABLE` / `GOOGLE_TABLE` → `CATALOG_TABLE` + `catalog_type` discriminator | Adding a new catalog = one file in `catalogs/`. Old graph JSON breaks (clean break, no migration) |
| **1C** | Pluggable storage with memory + file backends | API survives `create_app` recreation (tests prove it) |
| **1D** | Orchestrator phase abstraction | Each `Phase.execute(ctx)` is independently testable |
| **2E** | UI decomposition — `sidebar.py` (1,077 LOC) split into `sidebar/{connection,scope,credentials,actions,filters}.py` + CSS extracted to `ui/static/styles.css` + sample graph bundled as JSON | Stops being the new monolith |
| **2F** | SQLite storage backend (stdlib `sqlite3`, WAL mode, versioned-SQL migrations) | Conformance suite runs against memory + file + sqlite |
| **2G** | Watcher as independent service — `WatcherService` (pure logic) + `WatcherRunner` (asyncio loop) + `api/routers/watcher.py` (6 endpoints) + `WatcherRepository` (memory + sqlite) | UI restart no longer kills the watcher; multiple UIs see the same state |
| **3I** | Targeted WHY comments on non-obvious post-refactor choices | Not bulk verbosity — 8 specific spots flagged by Sentinel review |
| **3G** | Documentation sweep (this section + `CLAUDE.md` + `docs/STATUS.md` + reference docs) | Post-refactor reality is discoverable |

**Net product changes that landed alongside the refactor (separate themed commits):**
- Per-demo credential cache accumulates across switches (Glue + UC + BQ + DataZone configs all coexist)
- DLQ topics auto-wire to their producing sink connectors via the `lcc-XXXXX` resource ID
- Per-catalog brand icons (UC / Glue / BigQuery / DataZone) reach the actual graph nodes
- Per-catalog console deep-link labels (was hardcoded "Open in BigQuery" for everything)
- AWS Glue console URL respects `aws_region` and AWS partition (commercial / GovCloud / China)
- `MetricsClient` extended to enrich every node type (Flink + consumer-group + tableflow + catalog + ksqlDB)
- Sidebar legend shows per-catalog brand variants + DLQ + topic-with-schema badges
- ksqlDB + Tableflow phases fall back to the cached `lineage-bridge-{ksqldb,tableflow}-<env_id>` SA when `.env` lacks the key
- DataZone live tests gate on an `iam:SimulatePrincipalPolicy` preflight (skip with actionable IAM diff when denied)

**Net refactor totals:** 891 → 892 baseline tests (+ ~80 across the phases), 0 regressions, lint clean throughout.

**Remaining refactor work:**
- Phase 3H — Snowflake + Watsonx providers (deferred to post-merge)

---

## Dependency Graph

```
Phase 1 (DONE) ──> Commits adcdc76, 61a194a, 3deb972
                        │
                        v
Phase 2 (DONE) ──> URL dispatch, UX audit, node_details tests, CI/CD
                        │
                        v
Phase 3 (DONE) ──> UI decomposition, graph renderer tests
                        │
                        v
Phase 4 (DONE) ──> Polish + hardening ──> v0.2.0
                        │
                        v
Post-v0.2.0 (DONE) ──> UC fixes, Databricks push, Glue enrichment, watcher, Docker
                        │
                        v
v0.3.0 Housekeeping ──> Version sync, ADR cleanup, Terraform fixes, Docker healthchecks
                        │
                        v
OpenLineage API (DONE) ──> REST API, Google provider, tasks, docs ──> 609 tests
```
