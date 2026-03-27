LOCK TABLE tuple_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE telemetry_events IN ACCESS EXCLUSIVE MODE;

ALTER TABLE tuple_events RENAME TO tuple_events_legacy;
ALTER TABLE telemetry_events RENAME TO telemetry_events_legacy;

CREATE TABLE tuple_events (
    id TEXT NOT NULL,
    tuple_id TEXT NOT NULL,
    type TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT '',
    timestamp TIMESTAMPTZ NOT NULL,
    agent_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    namespace_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);

CREATE TABLE telemetry_events (
    event_id TEXT NOT NULL,
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
    namespace_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (ts_start, event_id)
) PARTITION BY RANGE (ts_start);

DO $$
DECLARE
    day_start TIMESTAMPTZ;
    day_end TIMESTAMPTZ;
    day_cursor TIMESTAMPTZ;
    today_day TIMESTAMPTZ := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    future_day TIMESTAMPTZ := date_trunc('day', (now() + INTERVAL '7 days') AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
BEGIN
    SELECT
        date_trunc('day', MIN(timestamp) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
        date_trunc('day', MAX(timestamp) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    INTO day_start, day_end
    FROM tuple_events_legacy;

    IF day_start IS NULL THEN
        day_start := today_day;
        day_end := future_day;
    ELSE
        day_start := LEAST(day_start, today_day);
        day_end := GREATEST(day_end, future_day);
    END IF;

    day_cursor := day_start;
    WHILE day_cursor <= day_end LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF tuple_events FOR VALUES FROM (%L) TO (%L)',
            'tuple_events_' || to_char(day_cursor AT TIME ZONE 'UTC', 'YYYYMMDD'),
            day_cursor,
            day_cursor + INTERVAL '1 day'
        );
        day_cursor := day_cursor + INTERVAL '1 day';
    END LOOP;
END $$;

DO $$
DECLARE
    day_start TIMESTAMPTZ;
    day_end TIMESTAMPTZ;
    day_cursor TIMESTAMPTZ;
    today_day TIMESTAMPTZ := date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    future_day TIMESTAMPTZ := date_trunc('day', (now() + INTERVAL '7 days') AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
BEGIN
    SELECT
        date_trunc('day', MIN(ts_start) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
        date_trunc('day', MAX(ts_start) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    INTO day_start, day_end
    FROM telemetry_events_legacy;

    IF day_start IS NULL THEN
        day_start := today_day;
        day_end := future_day;
    ELSE
        day_start := LEAST(day_start, today_day);
        day_end := GREATEST(day_end, future_day);
    END IF;

    day_cursor := day_start;
    WHILE day_cursor <= day_end LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF telemetry_events FOR VALUES FROM (%L) TO (%L)',
            'telemetry_events_' || to_char(day_cursor AT TIME ZONE 'UTC', 'YYYYMMDD'),
            day_cursor,
            day_cursor + INTERVAL '1 day'
        );
        day_cursor := day_cursor + INTERVAL '1 day';
    END LOOP;
END $$;

INSERT INTO tuple_events (id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id)
SELECT id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, COALESCE(namespace_id, '')
FROM tuple_events_legacy;

INSERT INTO telemetry_events (
    event_id, trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
    attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, namespace_id
)
SELECT
    event_id, trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
    attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, COALESCE(namespace_id, '')
FROM telemetry_events_legacy;

DROP TABLE tuple_events_legacy;
DROP TABLE telemetry_events_legacy;

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
