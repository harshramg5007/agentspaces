LOCK TABLE tuple_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE telemetry_events IN ACCESS EXCLUSIVE MODE;

ALTER TABLE tuple_events RENAME TO tuple_events_partitioned;
ALTER TABLE telemetry_events RENAME TO telemetry_events_partitioned;

CREATE TABLE tuple_events (
    id TEXT PRIMARY KEY,
    tuple_id TEXT NOT NULL,
    type TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT '',
    timestamp TIMESTAMPTZ NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    namespace_id TEXT NOT NULL DEFAULT ''
);

CREATE TABLE telemetry_events (
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
    metric_value_str TEXT,
    namespace_id TEXT NOT NULL DEFAULT ''
);

INSERT INTO tuple_events (id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id)
SELECT id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id
FROM tuple_events_partitioned;

INSERT INTO telemetry_events (
    event_id, trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
    attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, namespace_id
)
SELECT
    event_id, trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
    attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, namespace_id
FROM telemetry_events_partitioned;

DROP TABLE tuple_events_partitioned CASCADE;
DROP TABLE telemetry_events_partitioned CASCADE;

CREATE INDEX idx_tuple_events_tuple_id ON tuple_events(tuple_id);
CREATE INDEX idx_tuple_events_timestamp ON tuple_events(timestamp DESC);
CREATE INDEX idx_tuple_events_type ON tuple_events(type);
CREATE INDEX idx_tuple_events_kind ON tuple_events(kind);
CREATE INDEX idx_tuple_events_trace_id ON tuple_events(trace_id);
CREATE INDEX idx_tuple_events_namespace_timestamp ON tuple_events(namespace_id, timestamp DESC);

CREATE INDEX idx_telemetry_trace_id ON telemetry_events(trace_id);
CREATE INDEX idx_telemetry_tuple_id ON telemetry_events(tuple_id);
CREATE INDEX idx_telemetry_name ON telemetry_events(name);
CREATE INDEX idx_telemetry_kind ON telemetry_events(kind);
CREATE INDEX idx_telemetry_ts_start ON telemetry_events(ts_start DESC);
CREATE INDEX idx_telemetry_namespace_ts_start ON telemetry_events(namespace_id, ts_start DESC);
