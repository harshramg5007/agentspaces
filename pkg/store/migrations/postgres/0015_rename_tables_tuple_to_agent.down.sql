-- Reverse rename: agents -> tuples, agent_events -> tuple_events
ALTER TABLE agents RENAME TO tuples;
ALTER TABLE agent_events RENAME TO tuple_events;

-- Reverse index renames
ALTER INDEX IF EXISTS idx_agents_kind RENAME TO idx_tuples_kind;
ALTER INDEX IF EXISTS idx_agents_status RENAME TO idx_tuples_status;
ALTER INDEX IF EXISTS idx_agents_owner RENAME TO idx_tuples_owner;
ALTER INDEX IF EXISTS idx_agents_parent RENAME TO idx_tuples_parent;
ALTER INDEX IF EXISTS idx_agents_trace RENAME TO idx_tuples_trace;
ALTER INDEX IF EXISTS idx_agents_queue RENAME TO idx_tuples_queue;
ALTER INDEX IF EXISTS idx_agents_tags RENAME TO idx_tuples_tags;
ALTER INDEX IF EXISTS idx_agents_inprogress_lease_until RENAME TO idx_tuples_inprogress_lease_until;
ALTER INDEX IF EXISTS idx_agents_new_ns_queue_created RENAME TO idx_tuples_new_ns_queue_created;
ALTER INDEX IF EXISTS idx_agents_new_ns_queue_kind_created RENAME TO idx_tuples_new_ns_queue_kind_created;

-- Reverse partition renames
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
          AND parent.relname = 'tuple_events'
          AND child.relname LIKE 'agent_events_%'
    LOOP
        EXECUTE format('ALTER TABLE %I RENAME TO %I',
            r.relname,
            replace(r.relname, 'agent_events_', 'tuple_events_'));
    END LOOP;
END $$;
