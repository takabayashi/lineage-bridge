# Refactor Plan — Modularity, Extensibility, Storage

**Status:** Planning — not yet started
**Created:** 2026-05-01
**Owner:** TBD

**Goal:** Restructure LineageBridge so the API layer is fully independent of the UI, the catalog protocol cleanly supports any number of catalogs (Snowflake, Watsonx, future), persistent storage is pluggable (file → sqlite → postgres/s3), and the codebase is smaller and more cohesive.

**Architectural principles (preserved from `docs/plan.md`):**
- Adding a new catalog = one file in `catalogs/`, register in `__init__.py`. Zero changes to orchestrator, models, or UI.
- API and UI must call the same service layer with the same request shape.
- Storage backends are interchangeable; switching backend is a config flag, not a code change.
- No commit without Sentinel review.

**Decisions baked in (from planning conversation, 2026-05-01):**
1. Catalog protocol v2 is a **breaking change** with a one-shot migration helper for stored graphs.
2. First production storage tier is **SQLite** (file backend default, sqlite for single-node prod). Postgres/S3 deferred.
3. New catalogs to land after refactor: **Snowflake** (via Kafka Connect Sink) and **IBM Watsonx.data** (Iceberg origin).
4. **Watcher becomes the third independent service** (alongside API and UI). Runs as its own process; UI and API both consume its state via the storage layer + a watcher control router. No more in-process thread inside Streamlit.
5. This plan lives at `docs/plan-refactor.md`; merges into `docs/plan.md` once complete.

---

## Critical findings driving this refactor

### Finding 1 — UI/API have no shared service layer
`ui/extraction.py:126-163` and `api/routers/tasks.py:94-211` both wrap the orchestrator with **divergent signatures**:
- UI version takes 9 enable_* flags + sr_endpoints + per-cluster credential merging.
- API version takes only `environment_ids`, hardcodes `enable_enrichment=True`.

Adding a feature today requires touching both. The API is silently behind the UI.

### Finding 2 — Catalog protocol cannot scale to 7+ catalogs
`catalogs/protocol.py:18-42` defines only 3 methods. Issues:
- **`push_lineage()` is missing** — push is implemented as bespoke `run_*_push` functions in the orchestrator (`extractors/orchestrator.py`), one per catalog.
- **`build_node()` signature is Tableflow-shaped** — `(ci_config, tableflow_node_id, topic_name, ...)` cannot represent Snowflake-via-Kafka-Connect or Watsonx-via-Iceberg origins.
- **Per-catalog `NodeType` enums** (`UC_TABLE`, `GLUE_TABLE`, `GOOGLE_TABLE`) — adding Snowflake adds an enum value, which cascades through models, API schemas, UI styles, sample data.

### Finding 3 — Backwards layering: catalog → API
`catalogs/google_lineage.py:233` imports `from lineage_bridge.api.openlineage.translator import graph_to_events`. A catalog provider depends on an API-internal module. Anyone shipping `catalogs/` standalone (CLI, watcher, third-party) drags in FastAPI.

### Finding 4 — Four ad-hoc in-memory stores, no abstraction
`api/state.py:GraphStore`, `api/task_store.py:TaskStore`, `api/openlineage/store.py:EventStore`, `config/cache.py` all reimplement dict-based storage. Process restart = data loss. Multi-worker deployment = split state. No path to swap backends.

### Finding 5 — `ui/sidebar.py` (1167 LOC) mixes UI with business logic
Lines 588-657 (`_resolve_extraction_context`) build extraction params — that's request building, not rendering. Lines 660-915 are extract/enrich/push button handlers that orchestrate the entire workflow inline.

### Finding 6 — Orchestrator (833 LOC) has monolithic phases
`_extract_environment()` mixes discovery, client instantiation, parallel fan-out via inline closures (`_run_connect`, `_run_ksqldb`, `_run_flink` at lines 566-643), and progress reporting. `ProgressCallback` is typed as `Any` (line 116). Three near-identical `run_*_push` functions.

### Finding 7 — Watcher is in-process, not a real service
`watcher/engine.py:WatcherEngine` is created and started by the UI (`ui/watcher.py:374-383`) and runs as a background **thread inside the Streamlit process**. All watcher state — `event_feed` (deque), `extraction_history` (list), `last_graph`, `poll_count` — lives on the engine instance. The UI reaches into these attributes directly (`ui/watcher.py:215, 233, 269`). Consequences:

- **No multi-worker safety:** two Streamlit instances each spawn their own watcher thread, both polling Confluent independently.
- **API can't see the watcher:** the API has no `/watcher` router; it cannot start, stop, or query a running watcher.
- **CLI runs differently:** `watcher/cli.py:132-133` bypasses threading and calls `engine._run_loop()` in the main thread — two execution modes for the same engine.
- **Dies with the UI:** restarting Streamlit kills the watcher and loses all event history.
- **Calls orchestrator directly:** `_do_extraction()` (lines 317-352) has the same anti-pattern as UI/API — duplicates the extraction-trigger code path.

The watcher should be the **third peer service** alongside the API and UI: its own process, its own deployment unit, its own router, with state in the storage layer so any UI/API instance can read it.

---

## Phase plan

### Phase 0 — Foundation ADRs (Blueprint, ~1 day, **sequential gate**)

Write three ADRs to `docs/decisions.md` that freeze the contracts everyone else builds against:

- **ADR-017: Service layer between API/UI and orchestrator.** Defines `ExtractionRequest`, `EnrichmentRequest`, `PushRequest` Pydantic v2 models and the `services/extraction_service.py` API surface.
- **ADR-018: Catalog protocol v2 (breaking).** New protocol methods, `MaterializationContext` discriminated union, collapsing per-catalog NodeTypes into `NodeType.CATALOG_TABLE` with a `catalog_type` attribute. Includes the migration helper for stored graphs.
- **ADR-019: Pluggable storage layer.** `Repository` protocol per entity, backends `memory`/`file`/`sqlite`, env-var-driven selection. Adds `WatcherRepository` to the protocol set (events, extraction history, control state).
- **ADR-020: Watcher as independent service.** Watcher becomes its own deployable process (no longer a thread inside Streamlit). State persisted via `WatcherRepository`. New `services/watcher_service.py` exposes the engine as a library; CLI/daemon wrapper runs it long-lived; new `api/routers/watcher.py` exposes start/stop/status endpoints. UI becomes stateless about the watcher — it polls the API like any other client.

**Sentinel sign-off required before Phase 1 starts.** This is the bottleneck — once these contracts are frozen, four streams can run in parallel.

---

### Phase 1 — Parallel core refactor (~3 days wall time, 4 streams)

These four streams touch disjoint files and can run concurrently. Only collision is `models/graph.py` (Phase 1B touches it); coordinate via Sentinel.

#### Phase 1A — Service layer + API parity (Forge + Lens)

**Files created:**
- `lineage_bridge/services/__init__.py`
- `lineage_bridge/services/extraction_service.py` — `ExtractionRequest` + `run_extraction(req, on_progress) -> LineageGraph`
- `lineage_bridge/services/enrichment_service.py`
- `lineage_bridge/services/push_service.py` — single `run_push(provider_name, graph, options)` replacing 3 `run_*_push` functions
- `lineage_bridge/services/request_builder.py` — pure function `build_extraction_request(session_dict) -> ExtractionRequest` (no Streamlit imports)
- `lineage_bridge/openlineage/` (moved from `api/openlineage/`) — fixes back-edge from Finding 3

**Files modified:**
- `api/routers/tasks.py` — call services instead of orchestrator
- `api/routers/__init__.py` — register new `push.py` router
- `api/routers/push.py` (NEW) — `POST /api/v1/push/{provider}`
- `ui/extraction.py` — call services instead of orchestrator
- `extractors/orchestrator.py:116` — replace `ProgressCallback = Any` with `Protocol`
- `catalogs/google_lineage.py:233` — update import to `lineage_bridge.openlineage.translator`

**Tests:**
- `tests/services/test_extraction_service.py`, `test_push_service.py`, `test_request_builder.py`
- `tests/api/test_push_router.py`
- **Parity test:** assert UI's `build_extraction_request(mock_session)` and the API's request body parser produce identical `ExtractionRequest` instances for equivalent inputs.

#### Phase 1B — Catalog protocol v2 (Blueprint + Weaver + Lens)

**New protocol** (`catalogs/protocol.py`):
```python
class CatalogProvider(Protocol):
    catalog_type: str
    supported_origins: tuple[OriginType, ...]  # TABLEFLOW, KAFKA_CONNECT, ICEBERG, DIRECT_INGEST

    def build_node(self, ctx: MaterializationContext) -> tuple[LineageNode, LineageEdge]: ...
    async def enrich(self, graph: LineageGraph, options: EnrichOptions) -> None: ...
    async def push_lineage(self, graph: LineageGraph, options: PushOptions) -> PushResult: ...
    def build_url(self, node: LineageNode) -> str | None: ...
    async def discover(self) -> list[DiscoveredTable]: ...  # optional
```

**Model changes** (`models/graph.py`):
- Replace `NodeType.UC_TABLE`, `GLUE_TABLE`, `GOOGLE_TABLE` with single `NodeType.CATALOG_TABLE`.
- Add `catalog_type: str` discriminator on node attributes (e.g. `"UNITY_CATALOG"`, `"AWS_GLUE"`, `"GOOGLE_DATA_LINEAGE"`, `"SNOWFLAKE"`, `"WATSONX"`).
- Add `LineageGraph.from_dict()` migration: detect old node types, rewrite to `CATALOG_TABLE` with catalog_type set.

**Migration helper** (`scripts/migrate_graphs_to_v2.py`):
- Reads old graph JSON files (in storage directory and any user-supplied path)
- Rewrites nodes in place
- Writes a backup with `.v1.bak` suffix
- Idempotent (skips already-migrated graphs)

**Settings** (`config/settings.py`):
- Nested catalog config via `pydantic_settings.SettingsConfigDict(env_nested_delimiter="__")`
- Example: `LINEAGE_BRIDGE_CATALOGS__SNOWFLAKE__ACCOUNT=...`, `LINEAGE_BRIDGE_CATALOGS__WATSONX__INSTANCE_ID=...`

**Files modified:**
- `catalogs/databricks_uc.py`, `aws_glue.py`, `google_lineage.py` — implement v2 protocol, including `push_lineage()`
- `catalogs/__init__.py` — registry uses `catalog_type` only (no NodeType lookup)
- `extractors/orchestrator.py` — remove `run_lineage_push`, `run_glue_push`, `run_google_push` (replaced by `push_service`)
- `ui/styles.py` — color/icon mapping keyed on `catalog_type` attribute, not NodeType

**Optional dependencies** (`pyproject.toml`):
```toml
[project.optional-dependencies]
snowflake = ["snowflake-connector-python>=3.7"]
watsonx = ["ibm-watsonx-data"]
bigquery = ["google-cloud-bigquery>=3.20"]
```
All catalog-specific imports are lazy (inside methods) so missing extras don't break import.

**Tests:**
- `tests/catalogs/test_protocol_conformance.py` — parametrized over all providers, asserts each implements the full v2 contract.
- `tests/models/test_graph_migration.py` — round-trip old format → new format, verify backward compatibility.
- Update `tests/fixtures/` for new node type.

#### Phase 1C — Storage layer with file backend (Weaver + Lens)

**New module** (`lineage_bridge/storage/`):

```
storage/
  __init__.py             # public API
  protocol.py             # GraphRepository, TaskRepository, EventRepository protocols
  factory.py              # make_repositories(settings) -> Repositories bundle
  backends/
    __init__.py
    memory.py             # current in-memory behavior — default for tests
    file.py               # JSON files in ~/.lineage_bridge/storage/{graphs,tasks,events}/
                          # uses portalocker for cross-process safety
```

**Repository protocols** (all async):
```python
class GraphRepository(Protocol):
    async def save(self, graph_id: str, graph: LineageGraph) -> None: ...
    async def get(self, graph_id: str) -> LineageGraph | None: ...
    async def list(self, *, limit: int = 50) -> list[GraphSummary]: ...
    async def delete(self, graph_id: str) -> bool: ...
    async def touch(self, graph_id: str) -> None: ...
```
(Same shape for Task, Event repositories.)

**Files refactored:**
- `api/state.py:GraphStore` → thin adapter calling `GraphRepository` (preserves public API)
- `api/task_store.py:TaskStore` → thin adapter calling `TaskRepository`
- `api/openlineage/store.py:EventStore` → thin adapter calling `EventRepository`
- `api/app.py:38-41` — instantiate via `factory.make_repositories(settings)` based on `LINEAGE_BRIDGE_STORAGE__BACKEND` env var (default `memory`)

**Settings additions:**
```python
class StorageSettings(BaseModel):
    backend: Literal["memory", "file", "sqlite"] = "memory"
    path: Path = Path.home() / ".lineage_bridge" / "storage"
    # sqlite-specific (Phase 2F):
    sqlite_url: str = "sqlite+aiosqlite:///~/.lineage_bridge/storage.db"
```

**Tests:**
- `tests/storage/test_protocol_conformance.py` — same test suite parametrized over all backends (memory, file). Adding sqlite later just adds a fixture.
- Existing `tests/api/test_state.py`, `test_task_store.py`, `test_event_store.py` should pass unchanged (adapters preserve behavior).

#### Phase 1D — Orchestrator phase abstraction (Weaver + Lens)

**New module** (`lineage_bridge/extractors/phases/`):

```
extractors/
  orchestrator.py         # SHRINKS to ~250 LOC — composition only
  phase.py                # ExtractionPhase Protocol, PhaseRunner
  context.py              # ExtractionContext dataclass (shared state across phases)
  phases/
    __init__.py
    kafka_admin.py        # Phase 1
    processing.py         # Phase 2 — Connect + ksqlDB + Flink (parallel fan-out lives here)
    enrichment.py         # Phase 3 — SchemaRegistry + StreamCatalog
    tableflow.py          # Phase 4 — Tableflow → CatalogProvider.build_node
    catalog_enrichment.py # Phase 4b — provider.enrich() fan-out
    metrics.py            # Phase 5 — optional
```

**Phase contract:**
```python
class ExtractionPhase(Protocol):
    name: str
    parallel_with: tuple[str, ...]  # other phase names this can run alongside
    async def execute(self, ctx: ExtractionContext) -> PhaseResult: ...
```

**Files modified:**
- `extractors/orchestrator.py` — becomes a `PhaseRunner` driver; loops phases, handles parallel groups, dispatches progress.
- Inline closures `_run_connect/_run_ksqldb/_run_flink` (orchestrator lines 566-643) move to `phases/processing.py` as standalone functions.

**Tests:**
- Per-phase unit tests in `tests/extractors/phases/`
- Integration test of full pipeline (already exists, should pass unchanged).

---

### Phase 2 — Sequential build-out (~5 days)

#### Phase 2E — UI decomposition (Prism + Weaver, ~2 days, **after Phase 1A**)

**Split `ui/sidebar.py` (1167 LOC):**
```
ui/sidebar/
  __init__.py             # ~50 LOC — composition, exports _render_sidebar
  connection.py           # ~100 LOC — connect/disconnect widgets only
  scope.py                # ~250 LOC — env/cluster pickers, discovery widgets
  credentials.py          # ~150 LOC — per-cluster/SR/Flink credential UI
  actions.py              # ~200 LOC — extract/enrich/push buttons calling services.*
  filters.py              # ~140 LOC — graph filters, legend, search
```

**Other UI cleanups:**
- Extract `ui/app.py:44-265` (220 LOC of CSS) → `ui/static/styles.css`. Load via `st.markdown(read_file("styles.css"))`.
- Extract `ui/app.py:289-407` (empty-state HTML) → `ui/empty_state.py`.
- Replace `ui/sample_data.py` (901 LOC) with `ui/static/sample_graph.json`. Loader becomes ~20 LOC.
- Delete `clients/audit_consumer.py` (418 LOC) — already documented as unused; git history preserves it.
- Optional: `ui/state_wrapper.py` for typed `st.session_state` accessors. Lower priority.

**Net LOC change:** ~-2,200 LOC from UI layer.

#### Phase 2F — SQLite backend (Anvil + Weaver, ~2 days, **after Phase 1C**)

**Files added:**
- `storage/backends/sqlite.py` — uses `aiosqlite`. Single-file DB at `~/.lineage_bridge/storage.db`.
- `storage/migrations/` — schema migrations via simple version table (alembic is overkill for sqlite).
- `storage/migrations/001_initial.sql` — `graphs`, `tasks`, `events` tables.
- `storage/migrations/002_v1_to_v2_node_types.sql` — applies the catalog NodeType migration in-place for stored graphs.

**Files modified:**
- `storage/factory.py` — register sqlite backend.
- `pyproject.toml` — add `aiosqlite` to base deps (small footprint, worth being default-available).

**Tests:**
- `tests/storage/test_sqlite_backend.py` (uses tmp_path fixture).
- Conformance suite from Phase 1C runs against sqlite automatically.

**Out of scope (deferred):** postgres backend, S3 backend, redis. Documented in ADR-019 as future work; can be added in a single PR each when there's a real production driver.

#### Phase 2G — Watcher as independent service (Forge + Weaver + Lens, ~3 days, **after Phase 1A + 1C + 1D**)

**Goal:** Watcher becomes the third peer service alongside API and UI. Runs in its own process, persists state via the storage layer, and is controllable from any number of UI/API instances.

**Architecture target:**

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│   UI (web)   │────▶│  API (REST)  │────▶│ Watcher Service  │
└──────────────┘     └──────────────┘     │   (daemon)       │
        │                    │            └──────────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
        └────────────  Storage Layer  ────────────┘
                  (graphs, tasks, events,
                   watcher_status, watcher_events)
```

UI polls the API; API reads from storage; watcher writes to storage. No UI/watcher direct coupling. Multiple UI instances see consistent state.

**New files:**
- `lineage_bridge/services/watcher_service.py` — pure logic, no threading. Wraps audit-log + REST-polling modes behind one `WatcherService` class. Calls `services.extraction_service.run_extraction()` (not orchestrator directly).
- `lineage_bridge/services/watcher_runner.py` — the long-running loop. Owns the asyncio event loop. Persists state on every tick to `WatcherRepository`. Designed to run as either a foreground process (CLI) or a containerized daemon.
- `lineage_bridge/api/routers/watcher.py` — endpoints:
  - `POST /api/v1/watcher/start` — body: `WatcherConfig` (mode, poll_interval, cooldown, extraction_request, push_options). Returns `watcher_id`.
  - `POST /api/v1/watcher/{watcher_id}/stop`
  - `GET /api/v1/watcher/{watcher_id}/status` — current state, poll counts, cooldown remaining
  - `GET /api/v1/watcher/{watcher_id}/events?limit=100&since=...` — event feed with cursor pagination
  - `GET /api/v1/watcher/{watcher_id}/history?limit=50` — extraction runs
  - `GET /api/v1/watcher` — list all known watchers (across processes)
- `lineage_bridge/storage/protocol.py` (extend) — add `WatcherRepository`:
  ```python
  class WatcherRepository(Protocol):
      async def register(self, watcher_id: str, config: WatcherConfig) -> None: ...
      async def update_status(self, watcher_id: str, status: WatcherStatus) -> None: ...
      async def get_status(self, watcher_id: str) -> WatcherStatus | None: ...
      async def append_event(self, watcher_id: str, event: WatcherEvent) -> None: ...
      async def list_events(self, watcher_id: str, *, limit: int, since: datetime | None) -> list[WatcherEvent]: ...
      async def append_extraction(self, watcher_id: str, record: ExtractionRecord) -> None: ...
      async def list_extractions(self, watcher_id: str, *, limit: int) -> list[ExtractionRecord]: ...
      async def list_watchers(self) -> list[WatcherSummary]: ...
      async def deregister(self, watcher_id: str) -> None: ...
  ```
  Implement against `memory`, `file`, and `sqlite` backends (uses the conformance suite from Phase 1C).
- `infra/docker/Dockerfile.watcher` updated for the new entry point.
- `docker-compose.yml` — `watcher` service now points at the daemon; uses the same storage backend as API.

**Files refactored:**
- `lineage_bridge/watcher/engine.py` — split. The pure logic (poll, debounce, trigger) moves into `services/watcher_service.py`. The threading wrapper is **deleted** — the daemon owns its event loop directly. Remove the `_use_audit_log` private property — mode is explicit in `WatcherConfig`.
- `lineage_bridge/watcher/cli.py` — becomes a thin wrapper over `services.watcher_runner.run_forever(config)`. No more `engine._run_loop()` direct invocation. CLI is just config parsing + signal handling.
- `lineage_bridge/ui/watcher.py` — stops creating `WatcherEngine` instances. All UI controls become API calls (`POST /watcher/start`, `GET /watcher/{id}/status`, etc.). The `@st.fragment(run_every=5)` (line 229) polls the API instead of reading engine attributes. Remove `st.session_state["watcher_engine"]`; replace with `st.session_state["watcher_id"]` (a UUID string).
- `lineage_bridge/api/app.py` — register the new `watcher` router.

**Files deleted:**
- `lineage_bridge/watcher/engine.py:WatcherEngine` class — replaced by `services.watcher_service.WatcherService`. The `watcher/` package can either go away entirely (logic moves to `services/`) or stay as a thin re-export layer for backward compatibility — TBD in the ADR.

**CLI surface stays:**
```bash
lineage-bridge-watch --env env-abc123 --cooldown 30
```
Still works, but now starts a daemon that registers itself in storage and is visible via `GET /watcher`. The CLI prints the assigned `watcher_id` so users can target it from the API.

**Tests:**
- `tests/services/test_watcher_service.py` — pure-logic tests (no threading, no daemon).
- `tests/services/test_watcher_runner.py` — long-running loop with fakes; verifies storage writes happen.
- `tests/api/test_watcher_router.py` — start/stop/status/events/history endpoints.
- `tests/storage/test_watcher_repository.py` — conformance against memory + file + sqlite.
- **Multi-process integration test:** start the daemon as a subprocess, query it via the API in the test process, verify state matches.

**Net effect:**
- Watcher runs as a peer service: separate container in compose, separate scaling unit in k8s.
- UI is now stateless about the watcher — it just polls the API.
- API gains full control over the watcher (currently a gap).
- Multiple UI instances see the same watcher state.
- Restarting the UI no longer kills the watcher.
- CLI and UI/API paths converge on one code path (`WatcherService`), eliminating the dual-mode hazard at `watcher/cli.py:132-133`.

---

### Phase 3 — Polish, docs, proof (~3-5 days, all parallel)

#### Phase 3G — Documentation sweep (Scout + Blueprint, parallel throughout, ~2 days total)

**Files updated:**
- `docs/architecture/index.md` — new module map (services/, storage/, extractors/phases/, openlineage/ at top level)
- `docs/architecture/clients.md` — split clients vs services discussion
- `docs/architecture/services.md` (NEW) — service layer responsibilities, request/response shapes
- `docs/architecture/extraction-pipeline.md` — update for phase abstraction
- `docs/architecture/storage.md` (NEW) — repository protocol, backend selection table, deployment tier guidance, future postgres/s3 notes
- `docs/catalog-integration/adding-new-catalogs.md` — rewrite for v2 protocol; full example using new `MaterializationContext`
- `docs/catalog-integration/snowflake.md` (NEW) — landed in Phase 3H
- `docs/catalog-integration/watsonx.md` (NEW) — landed in Phase 3H
- `docs/api-reference/openapi.md` — regenerated after Phase 1A
- `docs/openapi.yaml` — regenerated, includes `/push/{provider}` endpoints
- `docs/api-reference/examples.md` — add push examples, service layer examples
- `docs/contributing/adding-extractors.md` — service layer pattern, no longer call orchestrator directly
- `docs/decisions.md` — finalize ADRs 017/018/019; supersede ADR-009 (Glue stub note) and any others made stale
- `CLAUDE.md` — update module map (add services/, storage/, openlineage/ at top level; update catalog protocol description)
- `README.md` — refreshed architecture diagram, link to storage tiers
- `docs/plan.md` — append "Refactor (2026-MM-DD)" section once complete; mark this `plan-refactor.md` as superseded
- `docs/STATUS.md` — update component status

**Doc convention preserved:** every new module gets an entry in `docs/architecture/` matching the existing pattern.

#### Phase 3H — Snowflake + Watsonx providers (Weaver + Lens, ~3-4 days)

These are the **acceptance test** for the v2 protocol. If either requires changes outside `catalogs/`, the protocol failed and we iterate.

- `catalogs/snowflake.py` — uses Kafka Connect Snowflake Sink Connector as the materialization origin (`OriginType.KAFKA_CONNECT`). Enrichment via `snowflake-connector-python`. Push via `ALTER TABLE ... SET TAG` for lineage metadata.
- `catalogs/watsonx.py` — Iceberg-origin materialization (`OriginType.ICEBERG`). Enrichment via Watsonx data REST API. Push via Watsonx Lineage API if available, else metadata tags.
- `catalogs/__init__.py` — register both.
- Tests in `tests/catalogs/test_snowflake.py`, `test_watsonx.py` against the conformance suite.
- Sample data in `ui/static/sample_graph.json` includes one node from each.
- Settings: `LINEAGE_BRIDGE_CATALOGS__SNOWFLAKE__*`, `LINEAGE_BRIDGE_CATALOGS__WATSONX__*`.

#### Phase 3I — Comment pass on non-obvious code (Weaver + Sentinel review, ~1 day)

**Comment policy** (added here so it's not ambiguous):
- Default: **no comment**. Self-documenting names + types do the work.
- Add a comment when the **why** is non-obvious: hidden invariant, workaround for a specific upstream bug, performance trade-off, unusual error-handling, retry/timeout rationale tied to a documented API behavior, or a constraint imposed by a vendor quirk.
- Don't comment **what** the code does — well-named identifiers already do that.
- Don't reference current tasks/PRs/callers in comments — those rot. Put that context in the commit message or PR description.
- Each module gets a one-paragraph docstring (most already have this — verify and fill gaps).
- Each Protocol/abstract method gets a docstring (these are the public contract).

**Targeted files for this pass** (places where domain knowledge is genuinely hard to recover from reading the code):
- `clients/flink.py` — SQL parser regex magic; why backticks are preserved (preserves dots inside quoted identifiers, per Flink SQL spec)
- `clients/connect.py` — connector class → external dataset mapping; vendor naming quirks
- `extractors/orchestrator.py` (post-refactor) — phase ordering rationale
- `extractors/phases/processing.py` — why Connect/ksqlDB/Flink can run in parallel (no shared write set)
- `catalogs/databricks_uc.py` — UC dot-to-underscore quirk (Confluent replaces dots in table names; documented in ADR)
- `catalogs/aws_glue.py` — format-based catalog matching (DELTA → UC, ICEBERG → Glue)
- `catalogs/snowflake.py`, `watsonx.py` — vendor-specific connection origin mapping
- `models/graph.py` — node ID format invariant (`{system}:{type}:{env}:{qname}` — used as primary key everywhere)
- `watcher/engine.py` — debouncing rationale (30s cooldown chosen because Confluent Tableflow takes ~20s to reflect topic creation in the API)
- `storage/backends/file.py` — locking rationale, why fsync points are where they are
- `services/request_builder.py` — credential precedence chain (per-cluster → global kafka → cloud-level)

---

## Parallel execution timeline

```
Day:    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16
        │
P0  ████│         (foundation ADRs incl. ADR-020 — Blueprint, gate)
        │
P1A      ████████│         (services + API parity — Forge+Lens)
P1B      █████████████│    (catalog protocol v2 + migration — Blueprint+Weaver+Lens)
P1C      █████████████│    (storage layer file backend incl. WatcherRepository — Weaver+Lens)
P1D      ████████│         (phase abstraction — Weaver+Lens)
                 │
P2E              ████████│              (UI decomposition — Prism+Weaver, after P1A)
P2F                       ████████│     (sqlite backend — Anvil+Weaver, after P1C)
P2G                       ████████████│ (watcher as service — Forge+Weaver+Lens, after P1A+1C+1D)
                                       │
P3G   ████████████████████████████████ │  (docs — runs throughout, completes here)
P3H                                    ████████████│  (Snowflake + Watsonx — Weaver+Lens)
P3I                                    ████│           (comment pass — Weaver+Sentinel)
```

**Critical path:** P0 → P1{A,C,D} → P2G → P3H. Approximately **15-16 working days** end-to-end (P2G adds ~2 days to the previous critical path because Snowflake/Watsonx tests should run with the daemon-mode watcher to validate end-to-end).

**Coordination points (Sentinel checkpoints):**
1. End of P0 — four ADRs frozen before P1 starts.
2. End of P1 — all four streams merge to main; integration test must pass; storage protocol includes `WatcherRepository`.
3. End of P2 — feature freeze; watcher runs as standalone process in docker-compose; only docs/comment changes from here.
4. End of P3 — Snowflake/Watsonx integration tests prove protocol v2 works; watcher tested end-to-end (daemon → storage → API → UI).

---

## File-touch matrix (collision risk)

| File | P0 | P1A | P1B | P1C | P1D | P2E | P2F | P2G | P3G | P3H | P3I |
|------|----|----|----|----|----|----|----|----|----|----|----|
| `services/extraction,enrichment,push,request_builder` (new) | | ✏️ | | | | | | | 📖 | | 💬 |
| `services/watcher_service.py`, `watcher_runner.py` (new) | | | | | | | | ✏️ | 📖 | | 💬 |
| `openlineage/*` (moved) | | ✏️ | | | | | | | 📖 | | |
| `catalogs/protocol.py` | | | ✏️ | | | | | | 📖 | | |
| `catalogs/{databricks,glue,google}.py` | | ✏️ | ✏️ | | | | | | | | 💬 |
| `catalogs/{snowflake,watsonx}.py` | | | | | | | | | | ✏️ | 💬 |
| `catalogs/__init__.py` | | | ✏️ | | | | | | | ✏️ | |
| `models/graph.py` | | | ✏️ | | | | | | 📖 | | 💬 |
| `extractors/orchestrator.py` | | ✏️ | ✏️ | | ✏️ | | | | 📖 | | 💬 |
| `extractors/phases/*` (new) | | | | | ✏️ | | | | 📖 | | 💬 |
| `storage/protocol.py`, `factory.py`, `backends/{memory,file}.py` (new) | | | | ✏️ | | | | ✏️ | 📖 | | 💬 |
| `storage/backends/sqlite.py` (new) | | | | | | | ✏️ | ✏️ | | | 💬 |
| `api/state.py`, `task_store.py` | | | | ✏️ | | | | | | | |
| `api/openlineage/store.py` | | | | ✏️ | | | | | | | |
| `api/routers/tasks.py` | | ✏️ | | | | | | | 📖 | | |
| `api/routers/push.py` (new) | | ✏️ | | | | | | | 📖 | | |
| `api/routers/watcher.py` (new) | | | | | | | | ✏️ | 📖 | | |
| `api/app.py` | | ✏️ | | ✏️ | | | | ✏️ | | | |
| `ui/sidebar.py` → `ui/sidebar/*` | | | | | | ✏️ | | | 📖 | | |
| `ui/app.py` | | | | | | ✏️ | | | | | |
| `ui/extraction.py` | | ✏️ | | | | | | | | | |
| `ui/watcher.py` | | | | | | | | ✏️ | | | 💬 |
| `ui/styles.py` | | | ✏️ | | | | | | | | |
| `ui/sample_data.py` → JSON | | | | | | ✏️ | | | | | |
| `watcher/engine.py` | | | | | | | | 🗑️ | | | |
| `watcher/cli.py` | | | | | | | | ✏️ | | | |
| `clients/audit_consumer.py` | | | | | | 🗑️ | | | | | |
| `config/settings.py` | | | ✏️ | ✏️ | | | | ✏️ | 📖 | ✏️ | |
| `pyproject.toml` | | | ✏️ | | | | ✏️ | | | ✏️ | |
| `infra/docker/Dockerfile.watcher` | | | | | | | | ✏️ | | | |
| `docker-compose.yml` | | | | | | | ✏️ | ✏️ | | | |
| `docs/decisions.md` | ✏️ | | | | | | | | ✏️ | | |
| `docs/architecture/*` | | | | | | | | | ✏️ | | |
| `docs/catalog-integration/*` | | | ✏️ | | | | | | ✏️ | ✏️ | |
| `docs/user-guide/change-detection.md` | | | | | | | | ✏️ | ✏️ | | |
| `CLAUDE.md`, `README.md` | | | | | | | | | ✏️ | | |

Legend: ✏️ edits, 📖 reads for docs, 💬 comment pass, 🗑️ deletes.

**Real collisions:**
- P1A/P1B both edit `catalogs/{databricks,glue,google}.py` (one fixes the import, one implements v2 protocol). Sequence: P1A first, P1B builds on top.
- P1A/P1D both edit `extractors/orchestrator.py`. Sequence: P1D refactors structure first, P1A wires services on top.
- P1C/P2G both edit `storage/protocol.py` (P2G adds `WatcherRepository`). Sequence: P1C lands the base protocol, P2G extends it.
- P1A/P2G both edit `api/app.py` (router registration). Trivial merge.
- P2F/P2G both edit `docker-compose.yml`. Sequence: P2F adds sqlite service profile first, P2G points the watcher container at it.

Resolve by claiming a file in the crew tracker before starting.

---

## Acceptance criteria

This refactor is complete when:

1. **API/UI independence:** `pip install lineage-bridge[api]` works without `streamlit` installed; `pytest tests/api -v` passes with no UI imports loaded.
2. **Catalog extensibility proven:** Adding Snowflake required only `catalogs/snowflake.py` + a registry line + 3 settings fields. Same for Watsonx. Zero edits in `models/`, `extractors/`, `api/`, `ui/`.
3. **Storage independence:** `LINEAGE_BRIDGE_STORAGE__BACKEND=sqlite` and `=file` both pass the full conformance suite. Process restart preserves graphs/tasks/events/watcher state.
4. **Service layer parity:** UI and API generate identical `ExtractionRequest` for equivalent inputs (parity test passes).
5. **Watcher independence:** Watcher daemon runs as its own container in `docker-compose up`; restarting the UI does NOT kill the watcher; `GET /api/v1/watcher` from a fresh API process lists the running watcher; UI no longer instantiates `WatcherEngine` (grep returns zero hits in `ui/`).
6. **Three-service deployment:** `docker-compose up api ui watcher` starts three independent services; killing any one does not kill the others; all three share state via the storage backend.
7. **Module size:** `ui/sidebar/` total < 900 LOC (down from 1167); `extractors/orchestrator.py` < 300 LOC (down from 833); `services/watcher_service.py` < 400 LOC (down from `watcher/engine.py` 352 + threading).
8. **Test suite:** ≥ 70% coverage maintained, all green. New conformance suites for catalogs, storage, and watcher repository added. Multi-process integration test for the watcher passes.
9. **Docs:** Every new module has an entry in `docs/architecture/`. `CLAUDE.md` and `README.md` reflect new structure (three peer services). `docs/user-guide/change-detection.md` updated for the new daemon model. ADRs 017/018/019/020 finalized.
10. **No backwards-imports:** `grep -r "from lineage_bridge.api" lineage_bridge/{catalogs,clients,models,extractors,services,storage,openlineage,watcher}` returns nothing. `grep -r "WatcherEngine" lineage_bridge/ui/` returns nothing.

---

## Out of scope (deferred to follow-up plans)

- Postgres / S3 / Redis storage backends (documented as future work in ADR-019).
- BigQuery direct provider, Apache Polaris / Iceberg REST catalog provider.
- Redis-backed distributed task queue.
- Distributed (multi-worker) extraction.
- UI session state typed wrapper (touch volume too high for the win).
- Migrating off Streamlit (separate strategic decision).
