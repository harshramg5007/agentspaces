ALTER TABLE tuples ALTER COLUMN namespace_id SET DEFAULT '';
UPDATE tuples SET namespace_id = '' WHERE namespace_id IS NULL;
ALTER TABLE tuples ALTER COLUMN namespace_id SET NOT NULL;

ALTER TABLE tuple_events ALTER COLUMN namespace_id SET DEFAULT '';
UPDATE tuple_events SET namespace_id = '' WHERE namespace_id IS NULL;
ALTER TABLE tuple_events ALTER COLUMN namespace_id SET NOT NULL;

ALTER TABLE telemetry_events ALTER COLUMN namespace_id SET DEFAULT '';
UPDATE telemetry_events SET namespace_id = '' WHERE namespace_id IS NULL;
ALTER TABLE telemetry_events ALTER COLUMN namespace_id SET NOT NULL;
