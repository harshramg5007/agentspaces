CREATE INDEX IF NOT EXISTS idx_tuples_namespace_status ON tuples(namespace_id, status);
CREATE INDEX IF NOT EXISTS idx_tuples_namespace_created_new ON tuples(namespace_id, created_at, id) WHERE status='NEW';
CREATE INDEX IF NOT EXISTS idx_tuples_namespace_queue_created_new ON tuples(namespace_id, queue, created_at) WHERE status='NEW';
CREATE INDEX IF NOT EXISTS idx_tuple_events_namespace_timestamp ON tuple_events(namespace_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_namespace_ts_start ON telemetry_events(namespace_id, ts_start DESC);
