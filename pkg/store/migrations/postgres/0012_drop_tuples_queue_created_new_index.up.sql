-- migrate:nontransactional
DROP INDEX CONCURRENTLY IF EXISTS idx_tuples_queue_created_new;
