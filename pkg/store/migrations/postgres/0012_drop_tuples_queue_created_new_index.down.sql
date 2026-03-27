-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_queue_created_new ON tuples(queue, created_at) WHERE status='NEW';
