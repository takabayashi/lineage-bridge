-- Initial SQLite schema for the lineage_bridge storage backend (Phase 2F).
--
-- Three entity tables matching the three repository protocols
-- (GraphRepository, TaskRepository, EventRepository). Payloads are stored
-- as JSON TEXT so we don't have to keep migrations in lockstep with the
-- pydantic models — each repo serialises with the same `model_dump_json`
-- the file backend uses, and we let the schema breathe.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS graphs (
    graph_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_modified TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);

-- Events are append-only; the AUTOINCREMENT id preserves insertion order
-- across `all()`. `run_id` gets its own index because `by_run_id()` is the
-- one query we issue per request.
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
