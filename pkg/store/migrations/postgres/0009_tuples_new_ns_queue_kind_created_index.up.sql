-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_new_ns_queue_kind_created
ON tuples (namespace_id, queue, kind, created_at, id)
WHERE status = 'NEW';
