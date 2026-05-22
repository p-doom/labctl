-- Users table + FKs from every column that previously held a free-text
-- Unix username (`runs.submitted_by`, `pipelines."user"`,
-- `artifacts."user"`, `artifact_user_aliases."user"`,
-- `eval_requests."user"`). Replaces the pre-migration arrangement where
-- adding a new collaborator was a manual `psql` script (see prior
-- POSTGRES_DEPLOY.md). The `labctl admin add-user <name>` subcommand
-- now owns the lifecycle: it inserts here, creates the matching PG
-- role + GRANTs, and materialises the per-user FS dirs in one
-- transaction.
--
-- Backfill: every distinct user observed in existing data is copied
-- into `users` BEFORE the FK constraints kick in, so this migration is
-- idempotent on a live cutover from 0002.
--
-- `name` is the canonical identifier (Unix username) used in path
-- segments and as the PG role name. `pg_role` is intentionally
-- redundant with `name` today — it exists so future deployments can
-- diverge the two without a schema change (e.g. a labctl-side username
-- distinct from the connecting role).

CREATE TABLE users (
    name        TEXT PRIMARY KEY,
    created_at  BIGINT NOT NULL,
    pg_role     TEXT NOT NULL
);

-- ---------- backfill ----------
-- Union of every column that's about to become a FK source. Coalesce
-- timestamps to the earliest reference we have for each user; falls
-- back to now() only if we have no anchor.

WITH observed AS (
    SELECT submitted_by AS name, MIN(created_at) AS first_seen
        FROM runs WHERE submitted_by IS NOT NULL GROUP BY submitted_by
    UNION ALL
    SELECT "user", MIN(created_at) FROM pipelines GROUP BY "user"
    UNION ALL
    SELECT "user", MIN(created_at) FROM artifacts GROUP BY "user"
    UNION ALL
    SELECT "user", MIN(created_at) FROM artifact_user_aliases GROUP BY "user"
    UNION ALL
    SELECT "user", MIN(created_at) FROM eval_requests GROUP BY "user"
)
INSERT INTO users (name, created_at, pg_role)
SELECT name, MIN(first_seen), name
FROM observed
GROUP BY name;

-- ---------- foreign keys ----------
-- All immediate (not DEFERRABLE) because the backfill above guarantees
-- referenced rows exist NOW, and future writes go through the
-- `labctl admin add-user` command which inserts here first.

ALTER TABLE runs
    ADD CONSTRAINT runs_submitted_by_fk
        FOREIGN KEY (submitted_by) REFERENCES users(name);

ALTER TABLE pipelines
    ADD CONSTRAINT pipelines_user_fk
        FOREIGN KEY ("user") REFERENCES users(name);

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_user_fk
        FOREIGN KEY ("user") REFERENCES users(name);

ALTER TABLE artifact_user_aliases
    ADD CONSTRAINT artifact_user_aliases_user_fk
        FOREIGN KEY ("user") REFERENCES users(name);

ALTER TABLE eval_requests
    ADD CONSTRAINT eval_requests_user_fk
        FOREIGN KEY ("user") REFERENCES users(name);
