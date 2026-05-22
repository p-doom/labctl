-- Tightens 0001 into a production-grade schema:
--
--   * Foreign keys everywhere. The pre-migration filesystem-truth store
--     rebuilt itself from sidecars at boot and could absorb orphan rows;
--     PG is authoritative now, so referential integrity is enforced by
--     the schema. Self-referential / cycle-prone columns are
--     DEFERRABLE INITIALLY DEFERRED so the importer (and any future
--     bulk loader) can insert in dependency order within a transaction
--     without tripping on intra-batch references.
--   * CHECK constraints lock down the closed-set status / state enums
--     to exactly the values the Rust code uses. Anything else is a
--     bug, not a "future extension."
--   * UNIQUE (kind, content_hash) makes content-addressed dedup a
--     schema invariant instead of a code convention.
--   * A trigger on `events` calls `pg_notify('labctl_events', id)` so
--     SSE subscribers can `LISTEN` for live deltas instead of polling.
--
-- Applied automatically by `PgStore::connect` via `sqlx::migrate!`.
-- No skip-on-error path. A failing constraint surfaces as a hard
-- startup failure — which is the point.

-- ---------- foreign keys ----------

ALTER TABLE runs
    ADD CONSTRAINT runs_pipeline_fk
        FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE runs
    ADD CONSTRAINT runs_coalesced_peer_fk
        FOREIGN KEY (coalesced_peer_run_id) REFERENCES runs(id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_producer_fk
        FOREIGN KEY (producer_run_id) REFERENCES runs(id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE artifact_aliases
    ADD CONSTRAINT artifact_aliases_artifact_fk
        FOREIGN KEY (artifact_id) REFERENCES artifacts(id);

ALTER TABLE artifact_user_aliases
    ADD CONSTRAINT artifact_user_aliases_artifact_fk
        FOREIGN KEY (artifact_id) REFERENCES artifacts(id);

ALTER TABLE run_inputs
    ADD CONSTRAINT run_inputs_run_fk
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    ADD CONSTRAINT run_inputs_artifact_fk
        FOREIGN KEY (artifact_id) REFERENCES artifacts(id);

ALTER TABLE run_outputs
    ADD CONSTRAINT run_outputs_run_fk
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    ADD CONSTRAINT run_outputs_artifact_fk
        FOREIGN KEY (artifact_id) REFERENCES artifacts(id);

ALTER TABLE eval_requests
    ADD CONSTRAINT eval_requests_checkpoint_fk
        FOREIGN KEY (checkpoint_artifact_id) REFERENCES artifacts(id),
    ADD CONSTRAINT eval_requests_run_fk
        FOREIGN KEY (eval_run_id) REFERENCES runs(id)
        DEFERRABLE INITIALLY DEFERRED;

-- Cascade delete: when a run row is removed, its coalesce claim goes
-- with it. Combined with the periodic janitor sweep in the agent, this
-- closes the leak path where a producer died before
-- release_coalesce_slot.
ALTER TABLE coalesce_claims
    ADD CONSTRAINT coalesce_claims_producer_fk
        FOREIGN KEY (producer_run_id) REFERENCES runs(id) ON DELETE CASCADE;

ALTER TABLE tracking
    ADD CONSTRAINT tracking_run_fk
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;

ALTER TABLE events
    ADD CONSTRAINT events_run_fk
        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE;

-- ---------- CHECK constraints ----------

ALTER TABLE runs
    ADD CONSTRAINT runs_status_check CHECK (
        status IN (
            'created',
            'submitted',
            'running',
            'awaiting_peer',
            'succeeded',
            'failed',
            'cancelled',
            'timeout',
            'oom',
            'unknown_terminal',
            'cache_hit'
        )
    );

-- eval_requests.state is what Rust writes on insert/retry. The column
-- exists for future liveness tracking but current writers only ever
-- store 'submitted'; lock that down so any drift surfaces as a CHECK
-- failure rather than a silent data inconsistency.
ALTER TABLE eval_requests
    ADD CONSTRAINT eval_requests_state_check CHECK (state = 'submitted');

-- ---------- UNIQUE constraints ----------

-- Content-addressed dedup: one row per (kind, content_hash).
-- find_artifact_by_hash + insert_artifact already enforce this in code;
-- the unique index makes it a schema invariant and lets PG short-circuit
-- the hash lookup.
ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_kind_hash_unique UNIQUE (kind, content_hash);

-- ---------- LISTEN/NOTIFY for SSE ----------

-- pg_notify channel: 'labctl_events'. Payload: the new event id, as
-- TEXT (pg_notify only accepts TEXT payloads). Subscribers re-read the
-- row from `events` to get the body — the channel only delivers the
-- cursor.
CREATE OR REPLACE FUNCTION notify_labctl_event() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('labctl_events', NEW.id::text);
    RETURN NEW;
END;
$$;

CREATE TRIGGER events_notify
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_labctl_event();
