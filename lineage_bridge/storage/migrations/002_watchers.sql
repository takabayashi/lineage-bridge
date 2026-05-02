-- Phase 2G watcher tables.
--
-- One row per watcher in `watchers` (config + the latest status snapshot
-- live together so the API's GET /watcher/{id}/status is one row read).
-- Append-only log tables for events + extractions, each indexed by
-- (watcher_id, time-ish column) so the per-watcher list endpoints can
-- range-scan instead of full-table-scan.

CREATE TABLE IF NOT EXISTS watchers (
    watcher_id TEXT PRIMARY KEY,
    config_payload TEXT NOT NULL,
    status_payload TEXT
);

CREATE TABLE IF NOT EXISTS watcher_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watcher_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watcher_events_lookup
    ON watcher_events(watcher_id, event_time);

CREATE TABLE IF NOT EXISTS watcher_extractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watcher_id TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_watcher_extractions_lookup
    ON watcher_extractions(watcher_id, triggered_at);
