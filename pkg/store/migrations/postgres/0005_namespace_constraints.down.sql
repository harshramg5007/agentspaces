ALTER TABLE telemetry_events ALTER COLUMN namespace_id DROP NOT NULL;
ALTER TABLE telemetry_events ALTER COLUMN namespace_id DROP DEFAULT;

ALTER TABLE tuple_events ALTER COLUMN namespace_id DROP NOT NULL;
ALTER TABLE tuple_events ALTER COLUMN namespace_id DROP DEFAULT;

ALTER TABLE tuples ALTER COLUMN namespace_id DROP NOT NULL;
ALTER TABLE tuples ALTER COLUMN namespace_id DROP DEFAULT;
