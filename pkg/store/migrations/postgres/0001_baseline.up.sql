CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tuples (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags TEXT[] NOT NULL DEFAULT '{}',
    owner TEXT NOT NULL DEFAULT '',
    parent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    queue TEXT NOT NULL DEFAULT '',
    ttl_ms BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    owner_time TIMESTAMPTZ,
    lease_token TEXT NOT NULL DEFAULT '',
    lease_until TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_tuples_kind ON tuples(kind);
CREATE INDEX IF NOT EXISTS idx_tuples_status ON tuples(status);
CREATE INDEX IF NOT EXISTS idx_tuples_owner ON tuples(owner);
CREATE INDEX IF NOT EXISTS idx_tuples_parent ON tuples(parent_id);
CREATE INDEX IF NOT EXISTS idx_tuples_trace ON tuples(trace_id);
CREATE INDEX IF NOT EXISTS idx_tuples_queue ON tuples(queue);
CREATE INDEX IF NOT EXISTS idx_tuples_created_new ON tuples(created_at, id) WHERE status='NEW';
CREATE INDEX IF NOT EXISTS idx_tuples_queue_created_new ON tuples(queue, created_at) WHERE status='NEW';
CREATE INDEX IF NOT EXISTS idx_tuples_queue_kind_created_new ON tuples(queue, kind, created_at) WHERE status='NEW';
CREATE INDEX IF NOT EXISTS idx_tuples_tags ON tuples USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_tuples_metadata ON tuples USING GIN (metadata);

CREATE TABLE IF NOT EXISTS tuple_events (
    id TEXT PRIMARY KEY,
    tuple_id TEXT NOT NULL,
    type TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT '',
    timestamp TIMESTAMPTZ NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_tuple_events_tuple_id ON tuple_events(tuple_id);
CREATE INDEX IF NOT EXISTS idx_tuple_events_timestamp ON tuple_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tuple_events_type ON tuple_events(type);
CREATE INDEX IF NOT EXISTS idx_tuple_events_kind ON tuple_events(kind);
CREATE INDEX IF NOT EXISTS idx_tuple_events_trace_id ON tuple_events(trace_id);

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL DEFAULT '',
    span_id TEXT NOT NULL DEFAULT '',
    parent_span_id TEXT NOT NULL DEFAULT '',
    tuple_id TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL DEFAULT '',
    ts_start TIMESTAMPTZ NOT NULL,
    ts_end TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT '',
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_key TEXT NOT NULL DEFAULT '',
    metric_value_bool BOOLEAN,
    metric_value_num DOUBLE PRECISION,
    metric_value_str TEXT
);

CREATE INDEX IF NOT EXISTS idx_telemetry_trace_id ON telemetry_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_tuple_id ON telemetry_events(tuple_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_name ON telemetry_events(name);
CREATE INDEX IF NOT EXISTS idx_telemetry_kind ON telemetry_events(kind);
CREATE INDEX IF NOT EXISTS idx_telemetry_ts_start ON telemetry_events(ts_start DESC);
