-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_queue_kind_created_new ON tuples(queue, kind, created_at) WHERE status='NEW';
