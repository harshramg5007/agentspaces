-- migrate:nontransactional
DROP INDEX CONCURRENTLY IF EXISTS idx_tuples_new_ns_queue_kind_created;
