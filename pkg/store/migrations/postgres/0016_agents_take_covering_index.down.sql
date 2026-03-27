-- migrate:nontransactional
DROP INDEX CONCURRENTLY IF EXISTS idx_agents_new_ns_queue_created_covering;
