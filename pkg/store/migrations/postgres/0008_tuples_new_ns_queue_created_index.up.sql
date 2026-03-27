-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_new_ns_queue_created
ON tuples (namespace_id, queue, created_at, id)
WHERE status = 'NEW';
