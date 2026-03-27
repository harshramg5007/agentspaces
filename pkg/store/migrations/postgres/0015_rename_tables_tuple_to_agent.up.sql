-- Rename tables: tuples -> agents, tuple_events -> agent_events
ALTER TABLE tuples RENAME TO agents;
ALTER TABLE tuple_events RENAME TO agent_events;

-- Rename indexes on agents table (formerly tuples)
ALTER INDEX IF EXISTS idx_tuples_kind RENAME TO idx_agents_kind;
ALTER INDEX IF EXISTS idx_tuples_status RENAME TO idx_agents_status;
ALTER INDEX IF EXISTS idx_tuples_owner RENAME TO idx_agents_owner;
ALTER INDEX IF EXISTS idx_tuples_parent RENAME TO idx_agents_parent;
ALTER INDEX IF EXISTS idx_tuples_trace RENAME TO idx_agents_trace;
ALTER INDEX IF EXISTS idx_tuples_queue RENAME TO idx_agents_queue;
ALTER INDEX IF EXISTS idx_tuples_tags RENAME TO idx_agents_tags;
ALTER INDEX IF EXISTS idx_tuples_inprogress_lease_until RENAME TO idx_agents_inprogress_lease_until;
ALTER INDEX IF EXISTS idx_tuples_new_ns_queue_created RENAME TO idx_agents_new_ns_queue_created;
ALTER INDEX IF EXISTS idx_tuples_new_ns_queue_kind_created RENAME TO idx_agents_new_ns_queue_kind_created;

-- Rename partitions of agent_events (formerly tuple_events) dynamically
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE ns.nspname = current_schema()
          AND parent.relname = 'agent_events'
          AND child.relname LIKE 'tuple_events_%'
    LOOP
        EXECUTE format('ALTER TABLE %I RENAME TO %I',
            r.relname,
            replace(r.relname, 'tuple_events_', 'agent_events_'));
    END LOOP;
END $$;
