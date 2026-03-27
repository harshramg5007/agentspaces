UPDATE tuples SET namespace_id = '' WHERE namespace_id IS NULL;
UPDATE tuple_events SET namespace_id = '' WHERE namespace_id IS NULL;
