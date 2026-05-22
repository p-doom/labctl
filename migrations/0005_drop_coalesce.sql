-- Remove the coalesce-slot machinery that handled simultaneous
-- in-flight submissions of identical work (same cache_key, neither
-- terminal). The post-completion cache-hit path
-- (find_cache_hit_candidate → status='cache_hit' + linked outputs)
-- stays — that's what actually carries the cache value. The only
-- thing this machinery prevented was a duplicate SLURM job when
-- two submitters raced before either reached terminal. In a
-- small-lab setting the rate is far below the maintenance cost
-- of the table + the awaiting_peer lifecycle + the follower
-- trampoline + the resolver loop.
--
-- After this migration `runs.status` no longer admits 'awaiting_peer'.
-- The Rust side simultaneously deletes register_follower,
-- reconcile_follower, the follower script, and all the
-- claim/release/find/set methods that referenced this table.

-- Drop the constraint first so the column drop doesn't fail on the
-- self-referential FK.
ALTER TABLE runs DROP CONSTRAINT runs_coalesced_peer_fk;
ALTER TABLE runs DROP COLUMN coalesced_peer_run_id;

-- Narrow the status CHECK to the surviving lifecycle vocabulary.
ALTER TABLE runs DROP CONSTRAINT runs_status_check;
ALTER TABLE runs ADD CONSTRAINT runs_status_check CHECK (
    status IN (
        'created',
        'submitted',
        'running',
        'succeeded',
        'failed',
        'cancelled',
        'timeout',
        'oom',
        'unknown_terminal',
        'cache_hit'
    )
);

-- coalesce_claims itself: the FK to runs (with ON DELETE CASCADE)
-- and the table.
DROP TABLE coalesce_claims;
