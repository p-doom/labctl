-- Drop the schema surface left behind by the content-addressed-storage
-- removal (c1d31e8 / 59eee8c) and the matching compute-side hash
-- computation step.
--
-- After c1d31e8 artifacts stay at their producer-written path; the
-- `_objects/<prefix>/<hash>/` rename was removed. After 59eee8c the
-- per-user `aliases/<user>/<alias>` symlink overlay was removed too —
-- the staging path IS the stable name. With the FS side fully
-- collapsed, the `artifact_user_aliases` table has no writer and no
-- reader (`add_user_alias` and `find_artifact_by_hash` are both gone
-- from PgStore + Store). Drop the table.
--
-- The content_hash column also loses its last load-bearing role: the
-- compute-side `labctl hash-outputs` step and the `register_outputs`
-- prefer-manifest branch are deleted in the same change, so new rows
-- have no producer for the hash. Relax NOT NULL so future inserts can
-- carry NULL; drop the UNIQUE (kind, content_hash) since on-write
-- dedup is gone; drop the (kind, content_hash) index since no query
-- consults it on the hot path. Legacy values populated by the
-- importer / prior code are preserved as-is — useful for diagnostics
-- and any future cross-cluster reconciliation that wants to read them
-- back, but never required.

-- ---------- drop artifact_user_aliases ----------

ALTER TABLE artifact_user_aliases
    DROP CONSTRAINT artifact_user_aliases_artifact_fk;
ALTER TABLE artifact_user_aliases
    DROP CONSTRAINT artifact_user_aliases_user_fk;
DROP INDEX IF EXISTS idx_user_aliases_artifact;
DROP INDEX IF EXISTS idx_user_aliases_kind;
DROP TABLE artifact_user_aliases;

-- ---------- relax artifacts.content_hash ----------

ALTER TABLE artifacts DROP CONSTRAINT artifacts_kind_hash_unique;
DROP INDEX IF EXISTS idx_artifacts_hash;
ALTER TABLE artifacts ALTER COLUMN content_hash DROP NOT NULL;
