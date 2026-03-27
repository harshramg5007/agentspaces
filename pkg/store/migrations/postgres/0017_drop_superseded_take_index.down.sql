-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agents_new_ns_queue_created
ON agents (namespace_id, queue, created_at, id)
WHERE status = 'NEW';
