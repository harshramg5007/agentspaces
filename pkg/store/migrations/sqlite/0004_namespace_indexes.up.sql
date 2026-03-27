CREATE INDEX IF NOT EXISTS idx_tuples_namespace_status ON tuples(namespace_id, status);
CREATE INDEX IF NOT EXISTS idx_tuples_namespace_created ON tuples(namespace_id, created_at_ms);
CREATE INDEX IF NOT EXISTS idx_tuple_events_namespace_timestamp ON tuple_events(namespace_id, timestamp_ms DESC);
