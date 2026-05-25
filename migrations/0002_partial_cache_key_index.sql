-- Replace the all-rows cache_key index with a partial one that
-- matches the only query that uses it (find_cache_hit_candidate):
--
--     SELECT ... FROM runs
--     WHERE cache_key = $1
--       AND status IN ('succeeded','cache_hit')
--     ORDER BY created_at DESC LIMIT N
--
-- Smaller index (most runs are non-terminal-success at any moment),
-- and PG only has to walk rows that are actually cache-hit candidates.
-- Non-cache-hit-eligible rows (pre-terminal, failed, etc.) no longer
-- bloat the b-tree.
--
-- DROP INDEX is safe — no online writer relies on the all-rows variant;
-- the WHERE-shape covers every existing call site.

DROP INDEX IF EXISTS idx_runs_cache_key;

CREATE INDEX idx_runs_cache_key
    ON runs (cache_key, created_at DESC)
    WHERE cache_key IS NOT NULL
      AND status IN ('succeeded', 'cache_hit');
