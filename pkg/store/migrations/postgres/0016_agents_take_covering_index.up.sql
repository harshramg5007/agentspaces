-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agents_new_ns_queue_created_covering
ON agents (namespace_id, queue, created_at, id)
INCLUDE (metadata, kind, status, payload, tags, owner, parent_id, trace_id,
         shard_id, ttl_ms, updated_at, owner_time, lease_token, lease_until, version)
WHERE status = 'NEW';
