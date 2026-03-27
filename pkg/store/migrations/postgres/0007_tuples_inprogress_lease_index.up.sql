-- migrate:nontransactional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tuples_inprogress_lease_until
ON tuples (lease_until, id)
WHERE status = 'IN_PROGRESS' AND lease_until IS NOT NULL;
