-- labctl initial schema. Ported from the in-memory SQLite cache that
-- previously rebuilt from filesystem sidecars on every startup. PG
-- is now the source of truth for metadata; sidecars on NFS remain
-- only as the compute→login bridge (status.json, outputs.json) and
-- as human-debuggable projections. Artifact bytes and provenance
-- bundles continue to live in content-addressed FS trees.
--
-- Conventions vs. the prior SQLite schema:
--   * `INTEGER`-as-timestamp → `BIGINT` (seconds since epoch).
--   * `TEXT`-as-JSON-blob → `JSONB` (queryable + path indexable).
--   * `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL` / `IDENTITY`.
--   * `"user"` is quoted everywhere — it's a reserved keyword
--     associated with `current_user()`; quoting makes the column
--     unambiguous in joins and views.

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
    dependency_on            TEXT,
    stage_name               TEXT,
    submitted_by             TEXT NOT NULL,
    cache_key                TEXT,
    coalesced_peer_run_id    TEXT
);

CREATE TABLE pipelines (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    pipeline_path TEXT,
    "user"        TEXT NOT NULL,
    created_at    BIGINT NOT NULL
);

CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    path            TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    producer_run_id TEXT,
    metadata_json   JSONB NOT NULL,
    created_at      BIGINT NOT NULL,
    "user"          TEXT NOT NULL,
    alias_segment   TEXT NOT NULL
);

CREATE TABLE artifact_aliases (
    alias        TEXT PRIMARY KEY,
    artifact_id  TEXT NOT NULL,
    created_at   BIGINT NOT NULL
);

-- Per-user alias overlay onto content-addressed artifacts. An artifact
-- (one content_hash, one id) can be referenced by multiple
-- ("user", alias, kind) tuples — e.g. Alice produces ds, Bob produces
-- byte-identical ds with the same alias name; both get rows here
-- pointing at the same artifact_id. Disk truth lives at
-- `<artifact_roots[kind]>/aliases/<user>/<alias>` as a symlink to the
-- artifact's `_objects/<prefix>/<hash>/` dir.
CREATE TABLE artifact_user_aliases (
    "user"       TEXT NOT NULL,
    alias        TEXT NOT NULL,
    kind         TEXT NOT NULL,
    artifact_id  TEXT NOT NULL,
    created_at   BIGINT NOT NULL,
    PRIMARY KEY ("user", alias, kind)
);

CREATE TABLE run_inputs (
    run_id        TEXT NOT NULL,
    role          TEXT NOT NULL,
    artifact_id   TEXT,
    resolved_path TEXT NOT NULL,
    PRIMARY KEY (run_id, role)
);

CREATE TABLE run_outputs (
    run_id      TEXT NOT NULL,
    role        TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (run_id, role, artifact_id)
);

CREATE TABLE eval_requests (
    eval_key                 TEXT PRIMARY KEY,
    checkpoint_artifact_id   TEXT NOT NULL,
    eval_recipe_hash         TEXT NOT NULL,
    policy_id                TEXT NOT NULL,
    eval_run_id              TEXT,
    state                    TEXT NOT NULL,
    attempts                 BIGINT NOT NULL DEFAULT 0,
    "user"                   TEXT NOT NULL,
    created_at               BIGINT NOT NULL,
    updated_at               BIGINT NOT NULL
);

CREATE TABLE tracking (
    run_id      TEXT PRIMARY KEY,
    entity      TEXT NOT NULL,
    project     TEXT NOT NULL,
    url         TEXT NOT NULL,
    group_name  TEXT,
    source      TEXT NOT NULL,
    created_at  BIGINT NOT NULL
);

-- Events table is now authoritative — replaces the prior events/<date>.jsonl
-- on disk. The `id` column is a monotonically-increasing cursor SSE
-- subscribers use to resume. Distinct from the cache-rebuild-derived ids
-- the prior SQLite cache used: now PG's BIGSERIAL is stable across
-- restarts.
CREATE TABLE events (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT,
    event_type    TEXT NOT NULL,
    payload_json  JSONB NOT NULL,
    created_at    BIGINT NOT NULL
);

CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_pipeline ON runs(pipeline_id);
CREATE INDEX idx_runs_recipe ON runs(recipe_name);
CREATE INDEX idx_runs_user ON runs(submitted_by);
CREATE INDEX idx_runs_cache_key ON runs(cache_key);

CREATE INDEX idx_artifacts_kind ON artifacts(kind);
CREATE INDEX idx_artifacts_producer ON artifacts(producer_run_id);
CREATE INDEX idx_artifacts_path ON artifacts(path);
CREATE INDEX idx_artifacts_hash ON artifacts(kind, content_hash);

CREATE INDEX idx_eval_requests_checkpoint ON eval_requests(checkpoint_artifact_id);

CREATE INDEX idx_run_inputs_path ON run_inputs(resolved_path);
CREATE INDEX idx_run_inputs_artifact ON run_inputs(artifact_id);

CREATE INDEX idx_run_outputs_run ON run_outputs(run_id);

CREATE INDEX idx_aliases_artifact ON artifact_aliases(artifact_id);
CREATE INDEX idx_user_aliases_artifact ON artifact_user_aliases(artifact_id);
CREATE INDEX idx_user_aliases_kind ON artifact_user_aliases(kind, "user");

CREATE INDEX idx_events_run ON events(run_id);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_created ON events(created_at);
