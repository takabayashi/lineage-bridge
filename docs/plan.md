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
1. `TBD` — Per-extractor timeouts, graph validation, Kafka fallback tests (Weaver + Lens)

---

## Dependency Graph

```
Phase 1 (DONE) ──> Commits adcdc76, 61a194a, 3deb972
                        │
                        v
Phase 2E (Forge: URL dispatch) ─────────┐
Phase 2F (Prism: UX audit) ─────────────┤
Phase 2G (Lens: node_details tests) ────┼──> PR #2
Phase 2H (Anvil: CI/CD) ───────────────┘
                                        v
Phase 3I (Blueprint+Weaver: UI decomp) ─┐
Phase 3J (Lens: graph_renderer tests) ──┼──> PR #3
                                        v
Phase 4 (Polish + hardening) ───────────┼──> PR #4 + v0.2.0
```
