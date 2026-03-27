-- migrate:nontransactional
DROP INDEX CONCURRENTLY IF EXISTS idx_tuples_queue_kind_created_new;
