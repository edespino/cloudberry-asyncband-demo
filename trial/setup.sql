-- Demo table spread evenly across all segments.
-- 10M rows; raise the upper bound for a more dramatic run.

-- Uncomment only if DECLARE ... PARALLEL RETRIEVE or gp_endpoints is missing.
-- Some Greenplum-lineage builds ship the feature as an extension.
-- CREATE EXTENSION IF NOT EXISTS gp_parallel_retrieve_cursor;

DROP TABLE IF EXISTS retrieve_demo;

CREATE TABLE retrieve_demo AS
SELECT g AS id,
       md5(g::text) AS payload,
       now() - (g % 86400) * interval '1 second' AS ts
FROM generate_series(1, 10000000) AS g
DISTRIBUTED BY (id);

ANALYZE retrieve_demo;

-- Sanity check: rows per segment should be roughly equal.
SELECT gp_segment_id, count(*) FROM retrieve_demo GROUP BY 1 ORDER BY 1;
