# Design Decisions & Tradeoffs

Living document. Every significant design or implementation decision made by the crew gets recorded here with context, alternatives considered, and the tradeoff rationale. This prevents re-litigating settled decisions and helps new conversations understand why the code looks the way it does.

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

- **Status:** SUPERSEDED by ADR-021 (2026-05-01) — node types collapsed into one `CATALOG_TABLE` with a `catalog_type` discriminator. Per-catalog dispatch and styling moved to runtime helpers (`get_provider(catalog_type)`, `icon_for_node(node)`).
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
