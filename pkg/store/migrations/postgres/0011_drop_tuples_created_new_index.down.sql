-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_created_new ON tuples(created_at, id) WHERE status='NEW';
