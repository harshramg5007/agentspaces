-- migrate:nontransactional
DROP INDEX CONCURRENTLY IF EXISTS idx_tuples_inprogress_lease_until;
