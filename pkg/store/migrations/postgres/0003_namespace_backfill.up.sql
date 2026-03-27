UPDATE tuples
SET namespace_id = NULLIF(metadata->>'namespace_id', '')
WHERE namespace_id IS NULL;

UPDATE tuple_events e
SET namespace_id = t.namespace_id
FROM tuples t
WHERE e.tuple_id = t.id
  AND e.namespace_id IS NULL;

UPDATE telemetry_events te
SET namespace_id = t.namespace_id
FROM tuples t
WHERE te.tuple_id = t.id
  AND te.namespace_id IS NULL;
