# Design Decisions & Tradeoffs

Living document. Every significant design or implementation decision made by the crew gets recorded here with context, alternatives considered, and the tradeoff rationale. This prevents re-litigating settled decisions and helps new conversations understand *why* the code looks the way it does.

**Format:** Each entry has a status (accepted/superseded/revisit), the date, which personas were involved, and a concise record of what was decided and why.

---

## ADR-001: Protocol over ABC for CatalogProvider

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Blueprint
- **Context:** Need an interface for catalog integrations (UC, Glue, future catalogs).
- **Decision:** Use `typing.Protocol` (structural typing) instead of `abc.ABC`.
- **Alternatives:** ABC with `@abstractmethod` for stricter enforcement.
- **Tradeoff:** Protocol enables duck typing — no inheritance required, easier to mock in tests, less coupling. ABC would catch missing methods earlier but adds ceremony for what are simple 3-method interfaces.
- **Files:** `catalogs/protocol.py`

## ADR-002: Singleton provider registry

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Blueprint, Forge
- **Context:** Need to look up the right provider for a catalog type string from Tableflow API.
- **Decision:** Module-level `_PROVIDERS` dict in `catalogs/__init__.py` with `get_provider()` and `get_active_providers()`.
- **Alternatives:** Pass registry as dependency through orchestrator chain; provider factory pattern.
- **Tradeoff:** Singleton is simple, avoids threading a registry through deep call stacks. Cost: singleton instances have no credentials (see ADR-007), harder to test with multiple configurations.
- **Files:** `catalogs/__init__.py`

## ADR-003: httpx over databricks-sdk

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Scout, Weaver
- **Context:** Databricks UC enrichment needs to call the UC REST API.
- **Decision:** Use `httpx.AsyncClient` directly with Bearer auth.
- **Alternatives:** Official `databricks-sdk` which handles auth, pagination, retries.
- **Tradeoff:** httpx is lightweight, async-native, and gives explicit control over retry behavior. SDK would add dependency weight (~50+ transitive deps) and its sync-first design doesn't fit our async pipeline. Revisit if auth complexity grows (OAuth M2M, managed identity).
- **Files:** `catalogs/databricks_uc.py`

## ADR-004: Fixed retry policy (3 retries, exponential backoff)

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Weaver
- **Context:** UC API calls can hit transient 429/503 errors.
- **Decision:** `_MAX_RETRIES = 3`, `_BACKOFF_BASE = 1.0 * (2^attempt)`. No jitter.
- **Alternatives:** Configurable retry strategy per provider; jitter to prevent thundering herd; circuit breaker.
- **Tradeoff:** Fixed policy is simple and covers most transient failures for a single-user tool. No jitter is fine — we're not a fleet of workers. Revisit if used in multi-tenant or high-throughput context.
- **Files:** `catalogs/databricks_uc.py`

## ADR-005: In-place graph mutation for enrichment

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Blueprint, Weaver
- **Context:** `enrich()` needs to add metadata to existing nodes.
- **Decision:** `enrich()` calls `graph.add_node(enriched_copy)` directly — mutates the graph in place, returns `None`.
- **Alternatives:** Return enriched nodes as a list for the orchestrator to merge.
- **Tradeoff:** In-place is simpler — provider owns the full enrich lifecycle, orchestrator just calls and moves on. Returning nodes would give the orchestrator merge control but adds complexity for no current benefit. Matches how `StreamCatalogClient.enrich()` and `MetricsClient.enrich()` already work.
- **Files:** `catalogs/databricks_uc.py`, `catalogs/aws_glue.py`, `extractors/orchestrator.py`

## ADR-006: Separate node types per catalog (UC_TABLE, GLUE_TABLE)

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Blueprint
- **Context:** Graph needs to represent tables from different catalogs.
- **Decision:** Each catalog gets its own `NodeType` enum value and `SystemType`.
- **Alternatives:** Generic `CATALOG_TABLE` with catalog name in attributes.
- **Tradeoff:** Separate types enable type-safe filtering, provider dispatch, and per-type styling without string comparisons. Cost: each new catalog adds an enum value. This is acceptable — catalogs are added rarely and the enum is the natural extension point.
- **Files:** `models/graph.py`

## ADR-007: Phase 4b creates fresh provider with credentials

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Forge, Blueprint
- **Context:** Registry singletons are created without credentials (fine for `build_node()`), but `enrich()` needs API credentials.
- **Decision:** Orchestrator checks `provider.catalog_type` and creates a new `DatabricksUCProvider(workspace_url, token)` with credentials from Settings.
- **Alternatives:** Store credentials in registry at startup; inject credentials via `enrich(graph, credentials)` parameter.
- **Tradeoff:** Fresh instance avoids storing secrets in module-level singletons. Cost: catalog_type string matching in orchestrator, repeated instantiation. String check was chosen over `isinstance` because mocking in tests patches the class — `isinstance` fails on mocks. Revisit if credential injection pattern emerges across multiple providers.
- **Files:** `extractors/orchestrator.py`

## ADR-008: Separate build and enrich phases

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Blueprint, Forge
- **Context:** Catalog nodes need to exist in the graph before enrichment can query the catalog API.
- **Decision:** `build_node()` runs in Phase 4 (Tableflow extraction), `enrich()` runs in Phase 4b (after merge).
- **Alternatives:** Single pass — build + enrich together in Tableflow.
- **Tradeoff:** Separation allows Tableflow to remain a pure Confluent API client (no external catalog credentials needed). Enrichment is independently retryable and can fail without losing the node structure. Cost: two-phase adds orchestration complexity.
- **Files:** `clients/tableflow.py`, `extractors/orchestrator.py`

## ADR-009: Glue enrich() implementation

- **Status:** Superseded (implemented 2026-04-06)
- **Date:** 2026-04-04 (original), 2026-04-06 (implemented)
- **Decided by:** Weaver, Blueprint
- **Context:** AWS Glue provider initially had a no-op `enrich()` stub. Full implementation was added in commit `7ebaed6` using boto3 `get_table` calls with `asyncio.to_thread()` (see ADR-013).
- **Decision:** `enrich()` now fetches table metadata from Glue (location, input format, serde, parameters) and updates graph nodes. `push_lineage()` writes lineage metadata back via `update_table`.
- **Files:** `catalogs/aws_glue.py`

## ADR-012: Lineage push via SQL Statement Execution API

- **Status:** Accepted
- **Date:** 2026-04-06
- **Decided by:** Blueprint, Weaver
- **Context:** Databricks Lineage Tracking API is read-only — no public endpoint exists to inject external lineage events. Need to make Confluent-side lineage visible within Databricks UC.
- **Decision:** Use the Databricks Statement Execution API (`/api/2.0/sql/statements`) to write lineage metadata via SQL: `ALTER TABLE SET TBLPROPERTIES` for machine-readable metadata, `COMMENT ON TABLE` for human-readable summaries, and an optional lineage bridge table for queryable records.
- **Alternatives:** (1) UC REST API `PATCH` for table properties — requires different permissions, less flexible property format. (2) OpenLineage integration — undocumented for external push into Databricks. (3) Databricks SDK — heavy dependency for simple SQL operations (see ADR-003).
- **Tradeoff:** SQL via Statement Execution API uses the same SQL Warehouse already configured for Tableflow, requires only `CAN_USE` warehouse + table permissions, and matches how users interact with Databricks. The approach is immediately visible in the Databricks UI with zero additional setup. Limitation: metadata is stored as properties/comments, not in the native lineage graph — users see provenance info on table pages but not in the lineage visualization. Revisit if Databricks adds a write API for external lineage.
- **Files:** `clients/databricks_sql.py`, `catalogs/databricks_uc.py`, `extractors/orchestrator.py`

## ADR-013: boto3 for AWS Glue API

- **Status:** Accepted
- **Date:** 2026-04-06
- **Decided by:** Blueprint, Weaver
- **Context:** AWS Glue enrichment needs to call `glue:GetTable` to fetch table metadata. Two options: (1) boto3, the canonical AWS SDK, or (2) httpx with manual AWS SigV4 signing.
- **Decision:** Use boto3 with `asyncio.to_thread()` to bridge its synchronous API into our async pipeline. Import lazily inside `enrich()` to avoid import errors when boto3 is not installed.
- **Alternatives:** httpx + botocore SigV4 signer; httpx + `aws-requests-auth`; manual SigV4 with httpx.
- **Tradeoff:** boto3 handles credentials (env, `~/.aws/credentials`, IAM roles, IRSA) automatically with zero configuration. SigV4 signing is correct by default. The sync overhead is negligible for metadata calls (one per table). `asyncio.to_thread()` keeps the event loop unblocked. Cost: ~70MB dependency. Acceptable for an optional enrichment feature. Revisit if we add more AWS service calls (could justify aioboto3).
- **Files:** `catalogs/aws_glue.py`, `pyproject.toml`

## ADR-010: Fixed per-extractor timeout (120s)

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Weaver
- **Context:** `_safe_extract()` catches exceptions but has no timeout — a hung extractor blocks the entire pipeline.
- **Decision:** Wrap coroutine in `asyncio.wait_for(coro, timeout=120)`. Module-level `_EXTRACTOR_TIMEOUT = 120` constant.
- **Alternatives:** Configurable timeout per extractor via Settings; no timeout (rely on httpx timeout).
- **Tradeoff:** Fixed 120s is generous enough for any REST API call chain while preventing indefinite hangs. httpx client timeout only covers individual HTTP requests, not the full extraction pipeline. Configurable timeout adds complexity for no current need. Revisit if extractors with legitimately long operations are added.
- **Files:** `extractors/orchestrator.py`

## ADR-011: Graph validation as warnings, not errors

- **Status:** Accepted
- **Date:** 2026-04-04
- **Decided by:** Weaver, Blueprint
- **Context:** Orphan nodes (no edges) and dangling edges indicate extraction issues, but should not block the graph from being displayed.
- **Decision:** `LineageGraph.validate()` returns `list[str]` of warnings. Orchestrator logs them and emits a progress callback. No exception raised.
- **Alternatives:** Raise `ValidationError` to fail the extraction; silently ignore.
- **Tradeoff:** Warnings surface issues for debugging without blocking the user. Orphan nodes are common in partial extractions (e.g., when some extractors are disabled). Raising would force users to fix upstream data before seeing any graph. Silent ignore would hide real bugs.
- **Files:** `models/graph.py`, `extractors/orchestrator.py`

## ADR-014: Change-detection watcher — REST polling + debounce

- **Status:** Accepted (supersedes Kafka consumer approach)
- **Date:** 2026-04-06
- **Decided by:** Blueprint, Forge
- **Context:** LineageBridge needs a reactive mode that detects lineage-relevant changes in Confluent Cloud and triggers extraction automatically. Initially designed around `confluent_kafka.Consumer` on the audit log cluster, but the audit log cluster is private (separate from data clusters) and has a 2-API-key limit, making it impractical for a development tool.
- **Decision:** Poll Confluent Cloud REST APIs every 10 seconds using existing Cloud API keys. `ChangePoller` takes snapshots of topics, connectors, ksqlDB clusters, and Flink statements, compares hashes to detect changes, and emits synthetic `AuditEvent` objects. A `WatcherEngine` runs the poller in a background `threading.Thread` with a 30-second debounce cooldown. No additional credentials are needed — reuses the existing Cloud API key.
- **Alternatives:** (1) Kafka consumer on audit log cluster — rejected due to private cluster + 2-key limit. (2) Webhook/event-driven — no Confluent Cloud webhook API available. (3) `asyncio` task — rejected because Streamlit's sync context creates a new event loop per call.
- **Tradeoff:** REST polling is lightweight and requires no additional credentials, but can only detect state changes (not who/when/why). 10-second poll interval + 30-second cooldown means changes are reflected within ~40 seconds. Polling adds minimal API load (4 lightweight list endpoints per poll). The audit log consumer code is retained in `models/audit_event.py` for potential future use if audit log access becomes available.
- **Files:** `watcher/engine.py`, `watcher/cli.py`, `clients/audit_consumer.py`, `models/audit_event.py`, `ui/watcher.py`

## ADR-015: OpenLineage-compatible REST API

- **Status:** Accepted
- **Date:** 2026-04-14
- **Decided by:** Blueprint, Forge
- **Context:** Confluent Cloud has no lineage API. External catalogs (UC, Google Data Lineage, Glue, Atlan, etc.) that speak OpenLineage cannot consume Confluent stream lineage natively. LineageBridge extracts this lineage but has no standard API to serve it.
- **Decision:** Build a FastAPI REST API that speaks OpenLineage natively. The API is bidirectional: it serves Confluent stream lineage as OpenLineage RunEvents outward AND accepts OpenLineage events from external catalogs inward. Two graph views: Confluent-only (pure stream lineage) and enriched (cross-platform). In-memory stores (GraphStore, EventStore, TaskStore) — no external database dependency.
- **Alternatives:** (1) Custom proprietary API — would require every consumer to learn a new format. (2) GraphQL — more flexible but higher complexity, less tooling ecosystem. (3) gRPC — better performance but less accessible for debugging and integration.
- **Tradeoff:** OpenLineage is the emerging standard adopted by Databricks, Google, Marquez, and others. Native compatibility means zero translation cost for consumers. REST + JSON is universally accessible. In-memory stores keep deployment simple (single binary, no database) but data is lost on restart. Acceptable for a bridge tool — the source of truth is the upstream systems.
- **Files:** `api/app.py`, `api/openlineage/models.py`, `api/openlineage/translator.py`, `api/openlineage/store.py`, `api/routers/`

## ADR-016: Google Data Lineage provider

- **Status:** Accepted
- **Date:** 2026-04-14
- **Decided by:** Blueprint, Weaver
- **Context:** Google Data Lineage (part of Dataplex) natively speaks OpenLineage via its `ProcessOpenLineageRunEvent` endpoint. This makes it a natural integration point and proof that the OpenLineage API works end-to-end.
- **Decision:** Add `GoogleLineageProvider` implementing `CatalogProvider` protocol. Uses BigQuery REST API for metadata enrichment and Google Data Lineage API for pushing lineage. Auth via Application Default Credentials (google-auth library). New enum values: `NodeType.GOOGLE_TABLE`, `SystemType.GOOGLE`.
- **Alternatives:** (1) BigQuery-only integration without Data Lineage — would miss the lineage push capability. (2) Client library (google-cloud-datacatalog) — heavy dependency for a few REST calls.
- **Tradeoff:** Follows the same pattern as UC and Glue providers (httpx + lazy import). google-auth is optional — the provider gracefully skips enrichment/push if no credentials are configured. Separate `GOOGLE_TABLE` node type (see ADR-006) enables type-safe filtering and per-type styling.
- **Files:** `catalogs/google_lineage.py`, `catalogs/__init__.py`, `models/graph.py`

## ADR-017: Persist demo credentials into the local encrypted cache

- **Status:** Accepted
- **Date:** 2026-05-01
- **Decided by:** Forge, Blueprint
- **Context:** Each `make demo-{uc,glue,bq}-up` provisions a fresh Confluent environment + cluster and overwrites the project-root `.env` with that demo's credentials, backing up the previous file as `.env.backup.<timestamp>`. A user who provisions all three demos ends up with only the latest demo's keys live; earlier demos' Kafka/SR/Flink credentials get buried in numbered backup files (~18 already on disk before this change). The local encrypted cache at `~/.lineage_bridge/cache.json` already stores `cluster_credentials`, `sr_credentials`, `flink_credentials`, and `provisioned_keys` keyed by cluster_id / env_id / display_name — so it can hold credentials for many demos simultaneously, but until now only the UI ever wrote to it.
- **Decision:** Each demo's `provision-demo.sh` invokes `scripts/cache_demo_credentials.py` after the `.env` is written. The helper parses the freshly generated `.env`, then merges Kafka, Schema Registry, Flink, Tableflow, and ksqlDB credentials into the cache via the existing `load_cache` / `save_cache` API (which handles Fernet encryption). Tableflow and ksqlDB are stored under `provisioned_keys` with display names like `lineage-bridge-tableflow-{demo}-{env_id}`.
- **Alternatives:** (1) Keep stacking `.env.backup.<timestamp>` files and ask users to merge by hand — error-prone, secrets sprawled across many files. (2) Have the UI scan `.env.backup.*` at startup and auto-merge — fragile (depends on filename convention, mixes backup/restore concerns into the UI). (3) Build a separate `lineage-bridge creds` CLI vault — more surface area than the problem warrants when the existing cache already supports the shape we need.
- **Tradeoff:** Adds one Python invocation per `demo-up` run (~1s). The cache becomes the durable cross-demo source of truth while `.env` stays a per-session convenience for CLI/Docker workflows. Cache entries are not auto-evicted on `demo-down`; users wanting to revoke them can clear `~/.lineage_bridge/cache.json` manually (acceptable: tearing down a demo deletes the upstream Confluent keys anyway, so stale cache entries become inert).
- **Files:** `scripts/cache_demo_credentials.py`, `infra/demos/uc/scripts/provision-demo.sh`, `infra/demos/glue/scripts/provision-demo.sh`, `infra/demos/bigquery/scripts/provision-demo.sh`

## ADR-018: Push the full upstream chain (not just the immediate source) to every catalog

- **Status:** Accepted
- **Date:** 2026-05-01
- **Decided by:** Forge, Blueprint
- **Context:** The original UC and Glue push paths only encoded the *direct* upstream of each catalog table — a flat `lineage_bridge.source_topics` / `source_connectors` list. The Google push path filtered OpenLineage events to those with a BigQuery output, dropping Flink/ksqlDB intermediate events. As a result, when a user clicked an upstream Kafka node in any catalog UI they couldn't tell whether the topic came from Datagen, a Debezium connector, or another Kafka topic via Flink — the multi-hop pipeline was invisible.
- **Decision:** Introduce a single shared `lineage_bridge/catalogs/upstream_chain.py` that walks `LineageGraph.get_upstream()` and emits ordered `ChainHop` records carrying hop distance, kind, qualified name, optional SQL (Flink/ksqlDB), optional connector class, and schema fields (for topics with `HAS_SCHEMA` edges). Wire it into:
  - **UC**: a new `lineage_bridge.upstream_chain` TBLPROPERTY (capped at 3 KB to fit Databricks' 4 KB-per-property limit, with a `lineage_bridge.upstream_truncated` flag), the `COMMENT ON TABLE` rendered as an indented chain, and a `chain_json` column on the optional bridge table.
  - **Glue**: `lineage_bridge.upstream_chain` table parameter (capped at 64 KB) and the same chain summary in `Description`.
  - **Google**: removed the BQ-output filter so every Job-event (Flink, ksqlDB, source connectors, sinks) is pushed; namespace normalisation now also rewrites `confluent://` *outputs* to `kafka://` so Flink writes back to Kafka are preserved.
  - **DataZone**: same OpenLineage flow as Google, with namespace allowlist `{kafka, bigquery, aws}`.
- **Alternatives:** (1) Keep flat lists and document that users should look at the LineageBridge UI for the chain — rejected because catalog UIs are where data teams actually live. (2) Encode the chain as separate per-hop properties — rejected because property count would explode and TBLPROPERTIES naming becomes unwieldy. (3) Push lineage facets (schema, columnLineage) on the events themselves — Google strips all facets at storage time (verified via API probe), so this only helps OpenLineage-native consumers like Marquez and isn't sufficient for catalog UIs.
- **Tradeoff:** One JSON-serialised payload per push, capped per catalog's value-size limit. Backwards compatible — the flat `source_topics` / `source_connectors` props are still written. Centralising the chain shape in `upstream_chain.py` means a hop added there surfaces everywhere; the shared `lineage_bridge/api/openlineage/normalize.py` likewise centralises the namespace rewrite for both Google and DataZone (parametrised by an allowlist).
- **Files:** `catalogs/upstream_chain.py`, `catalogs/databricks_uc.py`, `catalogs/aws_glue.py`, `catalogs/google_lineage.py`, `api/openlineage/normalize.py`

## ADR-019: Register Kafka assets in Dataplex Catalog and DataZone Catalog

- **Status:** Accepted
- **Date:** 2026-05-01
- **Decided by:** Forge, Blueprint
- **Context:** Google's `processOpenLineageRunEvent` and AWS DataZone's `post_lineage_event` both store **only the link FQNs** — facets (schema, columnLineage, custom) are discarded at storage time, verified via direct API probe. As a result, when a user clicks an upstream Kafka node in the BigQuery Lineage tab or DataZone lineage UI, the asset detail panel shows just the FQN with no column metadata. For BigQuery / Glue tables this isn't an issue (the catalog itself owns the schema), but for Confluent topics the catalog has no source of truth.
- **Decision:** Register each `KAFKA_TOPIC` node as a native catalog entry whose FQN matches the lineage event reference, then attach the schema as a typed aspect:
  - **Google → Dataplex Catalog** via `dataplex.googleapis.com/v1`: bootstrap entry group `lineage-bridge`, entry type `lineage-bridge-kafka-topic`, aspect type `lineage-bridge-schema`. Per-topic POST entry (idempotent, falls back to PATCH on 409). FQN format `kafka:lkc-yr5dok.\`lineage_bridge.enriched_orders\`` (backtick-escaped when topic contains dots — matches Google's auto-derived FQN from `kafka://<bootstrap>` + topic name).
  - **AWS → DataZone Catalog** via boto3 `datazone`: bootstrap custom asset type `LineageBridgeKafkaTopic` with a Smithy form (`KafkaSchemaForm`) carrying `fieldsJson`, `clusterId`, `environmentId`. Per-topic `create_asset` (idempotent, falls back to `create_asset_revision` on `ConflictException`). `externalIdentifier` matches the DataZone-recognised FQN (same backtick rule).
- **Alternatives:** (1) Encode schema in OpenLineage facets and rely on the processor — rejected, both Google and DataZone strip facets. (2) Use system-provided entry/asset types — rejected, neither service ships a "Kafka topic" type with the schema shape we need, and `dataplex-types` system entry types aren't accessible from user projects. (3) Skip catalog registration and document the limitation — rejected because the user explicitly wants schema visible in the catalog UI.
- **Tradeoff:** Adds one bootstrap call per project/domain (idempotent, ~5s on first run, near-zero after) plus one upsert per topic (~200ms each). Failures are non-fatal — collected into `PushResult.errors` so a missing IAM permission only loses schema visibility for the affected topics, not the whole push. Catalog entries are not auto-deleted when topics disappear from Confluent — manual cleanup is the user's call.
- **Files:** `catalogs/google_dataplex.py`, `catalogs/aws_datazone.py`, `catalogs/google_lineage.py`, `extractors/orchestrator.py`, `ui/sidebar.py`, `config/settings.py`

## ADR-020: Service layer between API/UI and orchestrator

- **Status:** Accepted
- **Date:** 2026-05-01
- **Decided by:** Blueprint, Forge, Lens
- **Context:** The UI (`ui/extraction.py:126-163`) and the API (`api/routers/tasks.py:94-211`) each wrap the orchestrator with **divergent signatures**: the UI passes nine `enable_*` flags plus per-cluster credential merging; the API takes only `environment_ids` and hardcodes `enable_enrichment=True`. Adding any feature requires touching both call sites, and the API silently lags the UI. The watcher (`watcher/engine.py:_do_extraction`) is a third copy of the same anti-pattern. There is no shared request shape, no shared progress contract, and no place for cross-cutting concerns (validation, request logging, future authz). `extractors/orchestrator.py:116` types `ProgressCallback` as `Any`, so neither caller has type-checked progress payloads.
- **Decision:** Introduce `lineage_bridge/services/` as the single entry point for extraction, enrichment, and push. The package owns:
  - **Pydantic v2 request models** — `ExtractionRequest`, `EnrichmentRequest`, `PushRequest` — with `model_config = ConfigDict(frozen=True)` so the same instance can be passed across threads/processes safely.
  - **`extraction_service.run_extraction(req, on_progress) -> LineageGraph`** — the only function that calls the orchestrator. Replaces the inline orchestrator wiring in UI, API, and watcher.
  - **`enrichment_service.run_enrichment(req, graph) -> LineageGraph`** — collapses the per-catalog enrichment fan-out behind one entry.
  - **`push_service.run_push(provider_name, graph, options) -> PushResult`** — a single dispatcher replacing the three near-identical `run_lineage_push` / `run_glue_push` / `run_google_push` functions in the orchestrator.
  - **`request_builder.build_extraction_request(session_dict) -> ExtractionRequest`** — pure function (no Streamlit imports) that the UI calls to translate `st.session_state` into a request. The API parses its JSON body into the same model. A parity test asserts both paths produce identical instances for equivalent inputs.
  - **`ProgressCallback` Protocol** — typed payloads (`PhaseStarted`, `PhaseCompleted`, `Warning`, `Error`) replacing `Any` at `extractors/orchestrator.py:116`.

  As a structural cleanup, `api/openlineage/` moves up to `lineage_bridge/openlineage/` so `catalogs/google_lineage.py` can import the translator without dragging in FastAPI (fixes the backwards layering at `catalogs/google_lineage.py:233`).
- **Alternatives:** (1) Lift the UI's `_resolve_extraction_context` into a shared helper but keep two call sites — rejected, doesn't solve API parity and leaves the request shape implicit. (2) Move the orchestrator's signature to match the UI's nine-flag form — rejected, locks the API into UI-shaped concerns and doesn't address watcher duplication. (3) Build a CQRS-style command bus — rejected as over-engineered for three operations and one process.
- **Tradeoff:** One extra layer between callers and the orchestrator, and a small amount of duplication between Pydantic request models and the orchestrator's internal kwargs. In return, every feature lands in one place, the API is no longer behind the UI by construction, the watcher converges on the same code path (eliminating the dual-mode hazard at `watcher/cli.py:132-133`), and progress payloads become type-checked. Callers outside this repo (CLI, tests, future SDKs) get a stable request surface that doesn't change when orchestrator internals change.
- **Files:** `services/extraction_service.py`, `services/enrichment_service.py`, `services/push_service.py`, `services/request_builder.py` (all new); `openlineage/` (moved from `api/openlineage/`); `api/routers/tasks.py`, `api/routers/push.py` (new), `ui/extraction.py`, `extractors/orchestrator.py`, `catalogs/google_lineage.py`

## ADR-021: Catalog protocol v2 — discriminated MaterializationContext, single CATALOG_TABLE node type

- **Status:** Accepted (supersedes ADR-006 once Phase 1B lands; ADR-016's `GOOGLE_TABLE` enum value is collapsed by this ADR)
- **Date:** 2026-05-01
- **Decided by:** Blueprint, Weaver, Lens
- **Context:** `catalogs/protocol.py:18-42` defines only `build_node`, `enrich`, and `build_url`. Three problems block scaling to Snowflake, Watsonx, and beyond:
  1. **`push_lineage()` is missing.** Push is implemented as bespoke `run_lineage_push` / `run_glue_push` / `run_google_push` functions inside `extractors/orchestrator.py`, one per catalog, with no shared signature.
  2. **`build_node()` is Tableflow-shaped.** Its signature `(ci_config, tableflow_node_id, topic_name, ...)` assumes a Confluent Tableflow integration is the only materialization origin. Snowflake (via Kafka Connect Sink) and Watsonx (via Iceberg) cannot be expressed without bending the contract.
  3. **Per-catalog `NodeType` enums** (`UC_TABLE`, `GLUE_TABLE`, `GOOGLE_TABLE`) cascade through `models/graph.py`, API schemas, `ui/styles.py`, and sample data. Each new catalog adds an enum value and edits four unrelated files — the opposite of the ADR-002 promise that "adding a new catalog = one file in `catalogs/`".

  ADR-006 chose separate node types for type-safe filtering and per-type styling. With three catalogs already in tree and four more on the roadmap (Snowflake, Watsonx, BigQuery direct, Polaris), the cost has flipped: the enum is no longer a natural extension point.
- **Decision:** Rev the catalog protocol to v2 as a **clean breaking change** — no migration helper, no compatibility shim. Concretely:
  - **New protocol** in `catalogs/protocol.py`:
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
  - **`MaterializationContext`** is a Pydantic discriminated union over `OriginType`. Each origin variant carries the fields that origin needs (Tableflow integration config, Kafka Connect connector class + config, Iceberg table reference, etc.). Providers declare which origins they accept via `supported_origins`; the dispatcher in `extractors/phases/tableflow.py` routes by origin → provider.
  - **Single `NodeType.CATALOG_TABLE`** replaces `UC_TABLE`, `GLUE_TABLE`, `GOOGLE_TABLE`. A new `catalog_type: str` attribute on the node (values `"UNITY_CATALOG"`, `"AWS_GLUE"`, `"GOOGLE_DATA_LINEAGE"`, `"SNOWFLAKE"`, `"WATSONX"`, …) drives provider dispatch, styling, and URL building. `ui/styles.py` keys colors and icons on `catalog_type` instead of `NodeType`.
  - **No migration code.** LineageBridge has no production users and no stored-graph compatibility surface to defend. The pre-v2 NodeType values (`UC_TABLE` / `GLUE_TABLE` / `GOOGLE_TABLE`) are deleted outright. Old JSON files fail Pydantic validation on load with a standard enum-mismatch traceback — re-extract instead of porting. If a real user with stored data appears later, a one-shot rewriter is a small follow-up PR. (Phase 2F's SQLite schema is greenfield for the same reason — no `002_v1_to_v2_node_types.sql` migration shipped.)
  - **Optional dependencies** in `pyproject.toml` (`snowflake`, `watsonx`, `bigquery` extras) keep catalog-specific imports lazy inside provider methods so missing extras don't break import.
- **Alternatives:** (1) Keep ADR-006's per-catalog node types and add `push_lineage` to the protocol only — rejected, doesn't solve the build_node Tableflow shape and still cascades enum changes through the codebase. (2) Generic `dict[str, Any]` for the materialization context — rejected, loses type safety exactly where we need it (provider dispatch). (3) Inheritance hierarchy (`CatalogTable(LineageNode)` with subclasses per catalog) — rejected, fights Pydantic v2 discriminated unions and breaks the protocol-not-ABC choice from ADR-001. (4) Introduce v2 alongside v1 with a shim layer — rejected, doubles surface area indefinitely. (5) Ship a `scripts/migrate_graphs_to_v2.py` rewriter + `LineageGraph.from_dict()` shim (the original ADR-021 plan) — rejected after re-evaluation: zero production users, zero promised stable JSON interchange, all test fixtures + sample data are source code that gets updated atomically with the protocol change. The migration helper would be ~150 LOC + tests + docs defending data that doesn't exist.
- **Tradeoff:** This is a breaking change for any out-of-tree code that imports `NodeType.UC_TABLE` / `GLUE_TABLE` / `GOOGLE_TABLE` directly. Cost is bounded: there are no known external consumers and no stored data to port. Old JSON files raise a standard Pydantic enum-mismatch ValidationError on `LineageGraph.from_dict()` — noisy but accurate. The discriminated union adds Pydantic ceremony, but it's the standard v2 pattern and gives provider dispatch type-checked routing for free. The `catalog_type` string is intentionally not an enum so that out-of-tree providers can register themselves without editing this repo.
- **Files:** `catalogs/protocol.py`, `catalogs/__init__.py`, `catalogs/databricks_uc.py`, `catalogs/aws_glue.py`, `catalogs/google_lineage.py`, `models/graph.py`, `extractors/orchestrator.py`, `ui/styles.py`, `config/settings.py`, `pyproject.toml`

## ADR-022: Pluggable storage layer with file → sqlite tier

- **Status:** Accepted
- **Date:** 2026-05-01
- **Decided by:** Weaver, Blueprint, Lens
- **Context:** Four ad-hoc in-memory stores have grown up independently: `api/state.py:GraphStore`, `api/task_store.py:TaskStore`, `api/openlineage/store.py:EventStore`, and `config/cache.py`. Each reimplements dict-backed storage with its own locking style, serialization, and lifecycle. Consequences: a process restart loses every graph, task, and OpenLineage event; a multi-worker uvicorn deployment splits state across workers (a task started on worker A is invisible to worker B); there's no path to swap in a real backend. ADR-015 explicitly accepted in-memory stores for the API ("the source of truth is the upstream systems"), but the in-memory choice is now blocking three things: multi-worker deployment, the watcher-as-service refactor (ADR-023, which needs cross-process state), and any future Postgres/S3 tier.
- **Decision:** Introduce `lineage_bridge/storage/` with a per-entity repository protocol and pluggable backends. Backend selection is a single env var; the rest of the codebase calls the protocol.
  - **Protocols** (all async, in `storage/protocol.py`):
    ```python
    class GraphRepository(Protocol):
        async def save(self, graph_id: str, graph: LineageGraph) -> None: ...
        async def get(self, graph_id: str) -> LineageGraph | None: ...
        async def list(self, *, limit: int = 50) -> list[GraphSummary]: ...
        async def delete(self, graph_id: str) -> bool: ...
        async def touch(self, graph_id: str) -> None: ...
    ```
    Same shape for `TaskRepository`, `EventRepository`, and (added by ADR-023) `WatcherRepository`.
  - **Backends** (`storage/backends/`):
    - `memory.py` — current in-memory behavior. Default for tests.
    - `file.py` — JSON files under `~/.lineage_bridge/storage/{graphs,tasks,events,watchers}/` with `portalocker` for cross-process safety. Default for local single-node use.
    - `sqlite.py` (Phase 2F) — `aiosqlite`, single-file DB, schema versioned via `storage/migrations/NNN_*.sql`. Default for production single-node.
  - **Factory** (`storage/factory.py`) — `make_repositories(settings) -> Repositories` bundle, called once at app startup. `api/app.py` injects the bundle into routers via FastAPI's dependency injection; the watcher daemon and CLI use the same factory.
  - **Settings** — `LINEAGE_BRIDGE_STORAGE__BACKEND={memory,file,sqlite}` and `LINEAGE_BRIDGE_STORAGE__PATH=...`. Defaults: `memory` for tests, `file` for local CLI, `sqlite` for the docker-compose API/watcher containers.
  - **Conformance test suite** — one set of tests in `tests/storage/test_protocol_conformance.py` parametrized over all backends. Adding a new backend = one fixture + zero new tests. Existing `tests/api/test_state.py` etc. continue to pass against the memory adapter.
  - **Existing stores become thin adapters** — `GraphStore`, `TaskStore`, `EventStore` keep their public signatures but delegate to the repository protocol. No router or test needs to change in the same PR.
- **Alternatives:** (1) Jump straight to Postgres — rejected, adds a deployment dependency for a tool whose primary user is a single developer at a laptop. The file backend is the right default for that user. (2) Use SQLAlchemy across all backends — rejected, drags in an ORM for what are key/value lookups + a few list queries; `aiosqlite` directly is enough. (3) Keep the four stores and add file persistence to each — rejected, locks in the ad-hoc shape and fails the "switching backend is a config flag" principle. (4) Use Redis as the default backend — rejected, adds a process to run for the laptop user.
- **Tradeoff:** Adds an abstraction layer and requires every backend to pass the conformance suite, which is real work for each new backend. In return: process restarts preserve state, multi-worker deployment works, the watcher service has a place to write its event feed and extraction history (ADR-023), and Postgres/S3/Redis become single-PR additions when there's a real driver. The file backend's per-write fsync + portalocker is slower than in-memory but is fine for this tool's throughput (tens of writes/sec at peak, not thousands). SQLite is the explicit production-tier ceiling for now — Postgres and S3 are deferred until a concrete driver appears (documented as future work in this ADR).
- **Files:** `storage/protocol.py`, `storage/factory.py`, `storage/backends/memory.py`, `storage/backends/file.py`, `storage/backends/sqlite.py` (Phase 2F), `storage/migrations/` (Phase 2F) — all new; `api/state.py`, `api/task_store.py`, `api/openlineage/store.py`, `api/app.py`, `config/settings.py`, `pyproject.toml`, `docker-compose.yml`

## ADR-023: Watcher as an independent service with persisted state

- **Status:** Accepted (supersedes the in-process threading model from ADR-014; the polling cadence and debounce semantics from ADR-014 are kept)
- **Date:** 2026-05-01
- **Decided by:** Forge, Blueprint, Weaver, Lens
- **Context:** ADR-014 chose REST polling + a 30s debounce for change detection, which is still correct. What is *not* correct is the deployment shape: `watcher/engine.py:WatcherEngine` is created and started by the UI (`ui/watcher.py:374-383`) as a **`threading.Thread` inside the Streamlit process**, with all state (`event_feed` deque, `extraction_history` list, `last_graph`, `poll_count`) living on the engine instance and the UI reaching into those attributes directly (`ui/watcher.py:215, 233, 269`). This produces five concrete problems:
  1. Two Streamlit instances spawn two watcher threads, both polling Confluent independently.
  2. The API has no `/watcher` router — it cannot start, stop, or query a running watcher.
  3. `watcher/cli.py:132-133` bypasses the threading wrapper and calls `engine._run_loop()` in the main thread, so the CLI runs the same engine in a different mode than the UI does.
  4. Restarting Streamlit kills the watcher and loses all event history.
  5. `_do_extraction()` (`watcher/engine.py:317-352`) calls the orchestrator directly — a third copy of the divergent extraction call site that ADR-020 is collapsing.

  ADR-022's storage layer creates the missing piece: a place to put watcher state that survives process restarts and is visible to other processes.
- **Decision:** Promote the watcher to the **third peer service** alongside the API and UI. It becomes its own deployable process, persists state via the storage layer, and is controlled exclusively through the API.
  - **`services/watcher_service.py`** — pure logic, no threading. Wraps the polling loop and debounce behind one `WatcherService` class. Calls `services.extraction_service.run_extraction()` (per ADR-020), so the CLI / UI / watcher all run the same extraction code path.
  - **`services/watcher_runner.py`** — the long-running asyncio loop that owns the event loop. Persists state on every tick to `WatcherRepository`. Designed to run as either a foreground CLI process or a containerized daemon.
  - **`api/routers/watcher.py`** — REST endpoints:
    - `POST /api/v1/watcher/start` (body: `WatcherConfig`; returns `watcher_id`)
    - `POST /api/v1/watcher/{id}/stop`
    - `GET /api/v1/watcher/{id}/status`
    - `GET /api/v1/watcher/{id}/events?limit=&since=`
    - `GET /api/v1/watcher/{id}/history?limit=`
    - `GET /api/v1/watcher` (list all known watchers across processes)
  - **`WatcherRepository`** — added to `storage/protocol.py` (extends ADR-022). Stores config, status, event feed, and extraction history. Implemented against memory, file, and sqlite backends; covered by the conformance suite from ADR-022.
  - **CLI** — `lineage-bridge-watch` becomes a thin wrapper over `services.watcher_runner.run_forever(config)`. It registers the watcher in storage, prints the assigned `watcher_id`, and handles signals. Same code path as the daemon.
  - **UI** — stops creating `WatcherEngine` instances. The `@st.fragment(run_every=5)` poller in `ui/watcher.py:229` calls the API instead of reading engine attributes. Session state holds a `watcher_id` (UUID string) instead of a thread handle. Multiple UI instances see consistent state because they all read from the same storage backend via the API.
  - **Deployment** — `infra/docker/Dockerfile.watcher` updated for the daemon entry point; `docker-compose.yml` runs `watcher` as its own service pointed at the same storage backend as `api`.
  - **Cleanup** — `watcher/engine.py:WatcherEngine` deleted; logic lives entirely in `services/watcher_service.py`. `_use_audit_log` private flag removed — mode is explicit in `WatcherConfig`. The `watcher/` package becomes a thin re-export shim or goes away (decided during Phase 2G implementation).
- **Alternatives:** (1) Keep the threading model but add an API router that pokes the in-process engine — rejected, doesn't solve multi-worker safety, dies-with-UI, or the dual-mode hazard. (2) Run the watcher as a `multiprocessing.Process` spawned by the API — rejected, ties the watcher's lifecycle to the API process and complicates k8s deployment (one container, two roles). (3) Rebuild the watcher on top of Celery / Arq / RQ — rejected, drags in a queue + broker for what is one long-running process per Confluent environment. (4) Push state into the existing in-memory stores instead of a real repository — rejected, loses the cross-process visibility that's the whole point.
- **Tradeoff:** Adds a third deployable unit and a network hop between the UI and the watcher, plus the WatcherRepository implementation work across three backends. In return: the watcher survives UI restarts, multiple UI instances see the same watcher state, the API gains full programmatic control (currently a gap), the CLI and UI/API converge on one execution path (no more `engine._run_loop()` shortcut), and the watcher scales independently in k8s. The latency cost of "UI polls API polls storage" instead of "UI reads engine attribute" is ~50-100ms per refresh, well under the 5-second fragment cadence. The deployment-complexity cost is bounded by the docker-compose template — single-binary local use is preserved through `lineage-bridge-watch` running against the file backend, no daemon container required.
- **Files:** `services/watcher_service.py`, `services/watcher_runner.py`, `api/routers/watcher.py`, `storage/protocol.py` (extend with `WatcherRepository`) — all new; `watcher/engine.py` (delete), `watcher/cli.py`, `ui/watcher.py`, `api/app.py`, `config/settings.py`, `infra/docker/Dockerfile.watcher`, `docker-compose.yml`
