UPDATE tuples
SET namespace_id = NULLIF(json_extract(metadata, '$.namespace_id'), '')
WHERE namespace_id IS NULL;

UPDATE tuple_events
SET namespace_id = (
    SELECT t.namespace_id
    FROM tuples t
    WHERE t.id = tuple_events.tuple_id
)
WHERE namespace_id IS NULL;
