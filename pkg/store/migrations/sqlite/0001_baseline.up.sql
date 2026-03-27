CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL DEFAULT '',
    applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tuples (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    tags TEXT NOT NULL DEFAULT '[]',
    owner TEXT NOT NULL DEFAULT '',
    parent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    ttl_ms INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    owner_time_ms INTEGER,
    lease_token TEXT NOT NULL DEFAULT '',
    lease_until_ms INTEGER,
    version INTEGER NOT NULL DEFAULT 0,
    metadata TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_tuples_kind ON tuples(kind);
CREATE INDEX IF NOT EXISTS idx_tuples_status ON tuples(status);
CREATE INDEX IF NOT EXISTS idx_tuples_owner ON tuples(owner);
CREATE INDEX IF NOT EXISTS idx_tuples_parent ON tuples(parent_id);
CREATE INDEX IF NOT EXISTS idx_tuples_trace ON tuples(trace_id);

CREATE TABLE IF NOT EXISTS tuple_events (
    id TEXT PRIMARY KEY,
    tuple_id TEXT NOT NULL,
    type TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT '',
    timestamp_ms INTEGER NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL,
    data TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_tuple_events_tuple_id ON tuple_events(tuple_id);
CREATE INDEX IF NOT EXISTS idx_tuple_events_timestamp ON tuple_events(timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_tuple_events_type ON tuple_events(type);
CREATE INDEX IF NOT EXISTS idx_tuple_events_kind ON tuple_events(kind);
CREATE INDEX IF NOT EXISTS idx_tuple_events_trace_id ON tuple_events(trace_id);
