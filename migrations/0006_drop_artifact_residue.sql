-- Drop two columns on `artifacts` that survived the
-- content-addressed-storage removal but no longer carry their weight:
--
--   content_hash: post-c1d31e8 nothing populates it on the live insert
--     path. 0004 already relaxed NOT NULL + dropped the UNIQUE + index.
--     The remaining argument for keeping it was "future cross-cluster
--     sync might want it" — speculation. If that feature ever lands,
--     it can add the column back with its own populator.
--
--   alias_segment: stores the last path segment under `<user>/`. Fully
--     derivable from `artifacts.path`, never queried directly, never
--     referenced relationally. Pure denormalisation debt.

ALTER TABLE artifacts DROP COLUMN content_hash;
ALTER TABLE artifacts DROP COLUMN alias_segment;
