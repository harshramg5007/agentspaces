ALTER TABLE telemetry_events DROP COLUMN IF EXISTS shard_id;

ALTER TABLE tuple_events DROP COLUMN IF EXISTS shard_id;

ALTER TABLE tuples DROP COLUMN IF EXISTS shard_id;
