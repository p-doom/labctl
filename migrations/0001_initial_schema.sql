-- labctl schema. Postgres is the source of truth for run metadata,
-- artifact lineage, eval requests, tracking, and the event log. NFS
-- holds only artifact bytes, per-run snapshots, and provenance
-- bundles — pointers in this DB, contents on disk.
--
-- Conventions:
--   * timestamps are `BIGINT` seconds-since-epoch.
--   * JSON columns are `JSONB` (path-queryable, index-friendly).
--   * `"user"` is quoted everywhere — reserved keyword, quoting
--     makes joins and views unambiguous.
--
-- FKs that participate in cycles (runs → pipelines, artifacts →
-- runs, eval_requests → runs) are DEFERRABLE INITIALLY DEFERRED so
-- bulk loaders can populate the graph inside one transaction without
-- ordering tricks.

-- ---------- users ----------

CREATE TABLE users (
    name        TEXT PRIMARY KEY,
    created_at  BIGINT NOT NULL,
    -- Intentionally redundant with `name` today; left distinct so a
    -- deployment can diverge the two without a schema change (e.g.
    -- labctl-side username distinct from the connecting PG role).
    pg_role     TEXT NOT NULL
);

-- ---------- pipelines ----------

CREATE TABLE pipelines (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    pipeline_path TEXT,
    "user"        TEXT NOT NULL REFERENCES users(name),
    created_at    BIGINT NOT NULL,
    CONSTRAINT pipelines_id_format CHECK (id LIKE 'pipeline_%')
);

-- ---------- runs ----------

CREATE TABLE runs (
    id                       TEXT PRIMARY KEY,
    recipe_name              TEXT NOT NULL,
    recipe_hash              TEXT NOT NULL,
    status                   TEXT NOT NULL,
    job_id                   TEXT,
    run_dir                  TEXT NOT NULL,
    repo                     TEXT NOT NULL,
    source_path              TEXT NOT NULL,
    recipe_json              JSONB NOT NULL,
    context_json             JSONB NOT NULL,
    created_at               BIGINT NOT NULL,
    finished_at              BIGINT,
    pipeline_id              TEXT,
    dependency_on            JSONB,
    stage_name               TEXT,
    submitted_by             TEXT NOT NULL REFERENCES users(name),
    cache_key                TEXT,
    CONSTRAINT runs_id_format CHECK (id LIKE 'run_%'),
    CONSTRAINT runs_recipe_name_nonempty CHECK (length(recipe_name) > 0),
    CONSTRAINT runs_repo_nonempty CHECK (length(repo) > 0),
    CONSTRAINT runs_created_at_positive CHECK (created_at > 0),
    -- Either both are unset (run hasn't completed) or finished_at is
    -- at or after created_at. Forbids zero/negative epochs and reversed
    -- timestamps.
    CONSTRAINT runs_finished_at_sane
        CHECK (finished_at IS NULL OR finished_at >= created_at),
    -- finished_at is bound 1:1 to the terminal-status set. Set ↔
    -- terminal, NULL ↔ pre-terminal. Catches "succeeded but never
    -- finalised" and "running but already finished" rows at write time.
    CONSTRAINT runs_terminal_finished_at CHECK (
        (status IN ('created','submitted','running')) = (finished_at IS NULL)
    ),
    CONSTRAINT runs_status_check CHECK (
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
    ),
    CONSTRAINT runs_pipeline_fk
        FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- One labctl run per SLURM job_id. Partial so pre-submission rows
-- (job_id NULL) don't collide.
CREATE UNIQUE INDEX runs_job_id_unique ON runs(job_id) WHERE job_id IS NOT NULL;

-- ---------- artifacts ----------

CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    path            TEXT NOT NULL,
    producer_run_id TEXT,
    metadata_json   JSONB NOT NULL,
    created_at      BIGINT NOT NULL,
    "user"          TEXT NOT NULL REFERENCES users(name),
    CONSTRAINT artifacts_id_format CHECK (id LIKE 'artifact_%'),
    CONSTRAINT artifacts_kind_nonempty CHECK (length(kind) > 0),
    CONSTRAINT artifacts_path_nonempty CHECK (length(path) > 0),
    -- Identity is path-canonical (`id = sha256(canonical_path)[..16]`),
    -- so path collisions are impossible by construction; the UNIQUE
    -- promotes that invariant into the schema and gives us an index
    -- on path for free (idx_artifacts_path is therefore redundant
    -- and is not created).
    CONSTRAINT artifacts_path_unique UNIQUE (path),
    CONSTRAINT artifacts_producer_fk
        FOREIGN KEY (producer_run_id) REFERENCES runs(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- ---------- artifact_aliases ----------

CREATE TABLE artifact_aliases (
    alias        TEXT PRIMARY KEY,
    artifact_id  TEXT NOT NULL REFERENCES artifacts(id),
    created_at   BIGINT NOT NULL
);

-- ---------- run_inputs / run_outputs ----------

CREATE TABLE run_inputs (
    run_id        TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role          TEXT NOT NULL,
    artifact_id   TEXT REFERENCES artifacts(id),
    resolved_path TEXT NOT NULL,
    PRIMARY KEY (run_id, role),
    CONSTRAINT run_inputs_role_nonempty CHECK (length(role) > 0)
);

CREATE TABLE run_outputs (
    run_id      TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role        TEXT NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id),
    PRIMARY KEY (run_id, role, artifact_id),
    CONSTRAINT run_outputs_role_nonempty CHECK (length(role) > 0)
);

-- ---------- eval_requests ----------

CREATE TABLE eval_requests (
    eval_key                 TEXT PRIMARY KEY,
    checkpoint_artifact_id   TEXT NOT NULL REFERENCES artifacts(id),
    eval_recipe_hash         TEXT NOT NULL,
    policy_id                TEXT NOT NULL,
    eval_run_id              TEXT,
    state                    TEXT NOT NULL,
    attempts                 BIGINT NOT NULL DEFAULT 0,
    "user"                   TEXT NOT NULL REFERENCES users(name),
    created_at               BIGINT NOT NULL,
    updated_at               BIGINT NOT NULL,
    CONSTRAINT eval_requests_state_check CHECK (state = 'submitted'),
    CONSTRAINT eval_requests_attempts_positive CHECK (attempts >= 0),
    CONSTRAINT eval_requests_run_fk
        FOREIGN KEY (eval_run_id) REFERENCES runs(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- One eval_request per submitted eval run. Partial so the pre-claim
-- window (eval_run_id NULL) doesn't collide.
CREATE UNIQUE INDEX eval_requests_eval_run_id_unique
    ON eval_requests(eval_run_id) WHERE eval_run_id IS NOT NULL;

-- ---------- tracking ----------

CREATE TABLE tracking (
    run_id      TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
    entity      TEXT NOT NULL,
    project     TEXT NOT NULL,
    url         TEXT NOT NULL,
    group_name  TEXT,
    source      TEXT NOT NULL,
    created_at  BIGINT NOT NULL
);

-- ---------- events ----------
--
-- BIGSERIAL `id` is the cursor SSE subscribers use to resume. The
-- AFTER INSERT trigger fans the new id over `pg_notify` so listeners
-- get push delivery instead of polling.

CREATE TABLE events (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT REFERENCES runs(id) ON DELETE CASCADE,
    event_type    TEXT NOT NULL,
    payload_json  JSONB NOT NULL,
    created_at    BIGINT NOT NULL,
    CONSTRAINT events_event_type_nonempty CHECK (length(event_type) > 0)
);

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

-- ---------- indexes ----------

CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_pipeline ON runs(pipeline_id);
CREATE INDEX idx_runs_recipe ON runs(recipe_name);
CREATE INDEX idx_runs_user ON runs(submitted_by);
CREATE INDEX idx_runs_cache_key ON runs(cache_key);

CREATE INDEX idx_artifacts_kind ON artifacts(kind);
CREATE INDEX idx_artifacts_producer ON artifacts(producer_run_id);
-- artifacts(path) is covered by the UNIQUE constraint above.

CREATE INDEX idx_eval_requests_checkpoint ON eval_requests(checkpoint_artifact_id);

CREATE INDEX idx_run_inputs_path ON run_inputs(resolved_path);
CREATE INDEX idx_run_inputs_artifact ON run_inputs(artifact_id);

CREATE INDEX idx_run_outputs_run ON run_outputs(run_id);

CREATE INDEX idx_aliases_artifact ON artifact_aliases(artifact_id);

CREATE INDEX idx_events_run ON events(run_id);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_created ON events(created_at);
