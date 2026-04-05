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

## ADR-009: Glue enrich() as no-op stub

- **Status:** Accepted (temporary)
- **Date:** 2026-04-04
- **Decided by:** Weaver, Blueprint
- **Context:** AWS Glue provider needs an `enrich()` method per the protocol, but Glue API enrichment isn't built yet.
- **Decision:** Implement as `async def enrich(self, graph): pass` — satisfies the protocol, does nothing.
- **Alternatives:** Make `enrich()` optional in the protocol; raise `NotImplementedError`.
- **Tradeoff:** No-op keeps the interface uniform — orchestrator calls all providers the same way without type checking. `NotImplementedError` would force try/except in the orchestrator. Revisit when Glue enrichment is implemented (boto3 `get_table` calls).
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
