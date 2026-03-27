-- sqlite cannot drop columns directly without table rebuild.
UPDATE tuples SET namespace_id = NULL;
UPDATE tuple_events SET namespace_id = NULL;
