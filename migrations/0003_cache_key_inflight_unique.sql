-- Partial unique index that makes the "two concurrent submissions for
-- the same cache_key both decide to run" race impossible at the schema
-- level. PG rejects any second INSERT/UPDATE that would land a
-- non-terminal run row with a cache_key already held by another non-
-- terminal row. The loser catches the conflict (ON CONFLICT DO NOTHING)
-- and attaches itself as a follower of the winner — see
-- `try_claim_or_follow` in pg_store.rs.
--
-- =========================================================================
-- BREAKING CHANGE in this rollout — historical `cache_key` values are
-- invalidated. Two concurrent code changes shift the `cache_key` payload
-- bytes, so a new submission's `cache_key` cannot match any pre-rollout
-- row's `cache_key` even when the recipe is unchanged:
--
--   1. `recipe_hash` switched from `sha256(serde_json::to_vec(recipe))`
--      to `canonical_value_hash(value, "recipe_hash/v1")`. Different
--      bytes for the same recipe → different hash → different cache_key.
--
--   2. `untracked_files_hash` is now a field in the cache_key payload.
--      Adding any field (even one that's `null` for clean repos) changes
--      the outer hash.
--
-- Effect: no new submission cache-hits a pre-rollout row. Existing
-- artifacts remain on disk and resolve normally as inputs to new runs;
-- they just stop being cache-hit *candidates*. Caches warm back up from
-- this rollout's first submissions onward. A backfill that recomputes
-- `cache_key` on every existing row could preserve historical hits, but
-- is not provided here — for a pre-rollout deploy this is moot, and for
-- a low-volume internal deploy the warm-up cost is acceptable.
--
-- The schema-output format of `recipe_hash` is unchanged (still 64-char
-- sha256 hex, satisfies `runs_recipe_hash_format`); the version tag
-- lives in the input bytes only, so future intentional invalidations
-- bump the tag without touching the schema.
-- =========================================================================
--
-- Lock footprint: this migration runs `CREATE UNIQUE INDEX` (no
-- `CONCURRENTLY` — sqlx::migrate! wraps each migration in a transaction
-- and `CONCURRENTLY` is disallowed inside a tx). The build takes an
-- `ACCESS EXCLUSIVE` lock on `runs` for its duration, which stalls every
-- concurrent writer. Fine on a small / pre-rollout `runs` table — the
-- build completes in well under a second. If the table ever grows to
-- where the build takes more than a few seconds, switch to an
-- out-of-band `CREATE UNIQUE INDEX CONCURRENTLY` applied manually
-- before the migration, then drop this `CREATE UNIQUE INDEX` to a no-op
-- (`CREATE UNIQUE INDEX IF NOT EXISTS ...`) so the migration becomes a
-- bookkeeping noop on already-prepared databases.
--
-- Predicate notes:
--   * `cache_key IS NOT NULL`: rows that opt out of caching (synthetic-
--     data generators, pre-pipeline placeholders, etc.) keep working —
--     they're invisible to this constraint.
--   * `status IN ('created','submitted','running')`: only in-flight rows.
--     Terminal rows (succeeded / cache_hit / failed / cancelled /
--     timeout / oom / unknown_terminal) are exempt, so a long-lived
--     cache_hit row never blocks a fresh re-submission of the same key.
--
-- Coexists with idx_runs_cache_key (0002), which is the *lookup* index
-- for find_cache_hit_candidates and is partial over the
-- terminal-success predicate. The two predicates are mutually exclusive
-- so there's no overlap.

-- Migration guard: refuse to apply if the table already contains two
-- non-terminal rows with the same cache_key. CREATE UNIQUE INDEX would
-- otherwise fail with a much less actionable error. Pre-rollout this
-- is a no-op; post-rollout it's the safety net that turns a bad deploy
-- into a halted deploy.
DO $$
DECLARE
    dup_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO dup_count
    FROM (
        SELECT cache_key
        FROM runs
        WHERE cache_key IS NOT NULL
          AND status IN ('created','submitted','running')
        GROUP BY cache_key
        HAVING COUNT(*) > 1
    ) AS dups;
    IF dup_count > 0 THEN
        RAISE EXCEPTION
            'migration 0003: % cache_key value(s) already have multiple in-flight rows; '
            'resolve duplicates before applying the partial unique index',
            dup_count;
    END IF;
END$$;

CREATE UNIQUE INDEX runs_cache_key_inflight_unique
    ON runs (cache_key)
    WHERE cache_key IS NOT NULL
      AND status IN ('created', 'submitted', 'running');
