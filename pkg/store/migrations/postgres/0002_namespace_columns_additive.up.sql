ALTER TABLE tuples ADD COLUMN IF NOT EXISTS namespace_id TEXT;
ALTER TABLE tuple_events ADD COLUMN IF NOT EXISTS namespace_id TEXT;
ALTER TABLE telemetry_events ADD COLUMN IF NOT EXISTS namespace_id TEXT;
