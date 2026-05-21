# Content-addressed artifact storage (design)

Status: **design draft**. No code or live-data changes yet.

This document specifies the move from per-user-prefixed artifact paths to
content-addressed storage. It's the long-form companion to "option (D)"
discussed in the chain-input-wiring work; the immediate cross-user
correctness was solved by option (A) (`resolved_path` sourced from
`artifact.path`), and (D) is now a model-cleanup project, not a
correctness fix.

## Why

Today an artifact's filesystem identity encodes its producer:

```
<artifact_root>/<producer_user>/<alias>/
                ^^^^^^^^^^^^^^^         <-- producer identity baked into path
```

That couples three things that should be independent:

- **Bytes** — the artifact content.
- **Provenance** — who produced it, when, with what recipe.
- **Naming** — the alias by which downstream consumers refer to it.

Concrete pain points:

1. **Multi-row-same-path edge case.** If user re-registers the same alias
   with different content (different content_hash), two `artifacts` rows
   coexist with the same path; `find_artifact_by_path` returns whichever
   SQLite picks. Latent bug.
2. **GC awkwardness.** When `producer_user`'s account is purged, their
   home dir / artifact prefix gets deleted. Any artifacts they produced
   become orphaned filesystem state under a now-absent user.
3. **Aliasing.** Renaming an alias today means moving its physical
   directory and rewriting `meta.json`. There's no clean "this alias
   points at that content" decoupling.
4. **Cross-cluster mirroring.** `import-from-cluster` already rsyncs
   bytes from a foreign cluster and registers them locally. The local
   path encodes the local importer's user, not the foreign producer.
   This is fine but loses fidelity: the same content_hash on two
   clusters can sit at different paths.
5. **Cross-user cache hits.** Resolved by (A), but the underlying
   awkwardness — "Bob runs Alice's cached output, which lives under
   Alice's prefix" — is still there. (D) removes the asymmetry: the
   bytes live somewhere neutral, and aliases are per-user pointers.

## Target on-disk layout

```
<artifact_root>/
    by-hash/
        ab/
            ab12cd...ef/                  <-- artifact dir (bytes + meta.json)
                meta.json
                <recipe-produced files>
        ...
    aliases/
        alice/
            yll_annotation_pilot_8k/      <-- per-user alias name
                .target.json              <-- {"content_hash": "ab12...", "kind": "dataset"}
        bob/
            ...
```

- `by-hash/<prefix>/<content_hash>/` is the canonical home for an
  artifact's bytes. The two-char prefix bounds dir entries per
  directory (otherwise `ls` of `by-hash/` is unbounded).
- `meta.json` (the artifact sidecar) lives inside the by-hash dir.
- `aliases/<user>/<alias>/` is a thin pointer namespace. Multiple
  users can have aliases pointing at the same by-hash dir (the
  multi-user dedup case). Multiple aliases per user, too (alias-of-
  alias) once the cleanup lands.

The global `aliases/<name>/.target.json` namespace (currently under
`runs_base/aliases/`) stays where it is — it's a separate, opt-in
overlay, not the artifact's home.

## Schema changes

Current `artifacts` row has `user` and `alias_segment` baked in, which
means it implicitly assumes "one user, one alias per artifact." With
(D), an artifact (one content_hash) may have multiple (user, alias)
overlays. Split the schema:

```sql
-- Artifacts: identity = content_hash. Producer user/alias drop out.
-- `path` becomes the by-hash dir.
CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,             -- artifact_<first16_of_content_hash>
    kind TEXT NOT NULL,
    path TEXT NOT NULL,              -- <root>/by-hash/<prefix>/<hash>
    content_hash TEXT NOT NULL,
    producer_run_id TEXT,            -- still useful for provenance
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

-- Per-user alias overlay. (user, alias, kind) unique.
CREATE TABLE artifact_user_aliases (
    user TEXT NOT NULL,
    alias TEXT NOT NULL,
    kind TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user, alias, kind)
);
```

`artifact_aliases` (global namespace) stays as-is.

## Recipe contract

Unchanged for callers:

- `{outputs.X.path}` still expands to a writable staging path (the
  output_root for the resolution kind, plus rendered alias). The run
  writes bytes there. After the marker is observed, `register_outputs`
  computes content_hash, moves the staging dir into `by-hash/`, and
  writes the per-user alias pointer.
- `{inputs.X.path}` resolves to `artifact.path` (the by-hash dir). Same
  as post-(A) today, just with a different on-disk shape.

The staging-then-move flow is the only behavioral change visible to
recipe authors; if they `ls` the output path after the job exits,
it'll now be empty (bytes moved out). Document this; it's the same
discipline as "don't peek at intermediate state."

## Code changes (foundation phase)

1. `fs_layout.rs`:
    - Add `by_hash_dir(root, content_hash)` builder.
    - Add `user_alias_pointer(root, user, alias)` builder.
    - Update `decompose_artifact_path` semantics OR retire it — the
      `<user>/<alias>` decomposition becomes a property of the alias
      pointer, not the artifact dir.
    - Bump `schema_version` in sidecars to `2`. The rebuild logic
      tolerates both versions during the transition.

2. `store.rs`:
    - Schema changes per above.
    - `insert_artifact`: write into by-hash, also write per-user alias
      pointer when called with an explicit `(user, alias)` overlay.
      Existing callers (register_outputs) pass `(producer_user,
      rendered_alias)`.
    - `find_artifact_by_path`: takes a by-hash path; returns the row.
    - Rebuild walk: scan `by-hash/<prefix>/<hash>` for artifacts;
      scan `aliases/<user>/<alias>` for the per-user overlay.

3. `runner.rs`:
    - `register_outputs`: after a successful run, atomically move the
      output dir from its staging path to `by-hash/<prefix>/<hash>`,
      then write the per-user alias pointer.
    - `resolve_inputs` (already does the right thing post-(A): uses
      `artifact.path`).

4. `artifacts.rs` (import-from-cluster):
    - Rsync target becomes the local `by-hash/` slot.
    - Foreign aliases get a per-user pointer on import.

## Migration

Two-phase:

### Phase M1 — Forward compat (writes new layout, reads either)

- Schema gets the new tables.
- `insert_artifact` writes new layout (by-hash + per-user alias).
- Rebuild walk reads both old (`<user>/<alias>/meta.json`) and new
  (`by-hash/<prefix>/<hash>/meta.json`) layouts, surfacing both as the
  same `artifacts` row shape.
- This is the **safe checkpoint**: deploy this, run it for a while,
  verify both layouts coexist correctly, then move to M2.

### Phase M2 — Migrate existing artifacts

A one-shot tool (similar pattern to the chain-input migration we
already discarded): for each existing `<root>/<user>/<alias>/`:

1. Read `meta.json` for content_hash.
2. Compute target `by-hash/<prefix>/<content_hash>/` path.
3. Atomically `rename(2)` the directory (same-filesystem move).
4. Write `aliases/<user>/<alias>/.target.json`.
5. Update the `artifacts` row's `path` field.

Same-filesystem `rename(2)` is atomic on Linux; if interrupted, the
old path either still exists (no-op on retry) or has been fully moved
(target is in place; retry idempotently rewrites the alias pointer).

Risks:
- Concurrent writes during migration (a running stage producing into
  the old staging path while migration moves an unrelated artifact in
  the same parent dir). Mitigate by pausing the agent dispatch loop
  during M2, or by skipping any artifact whose `meta.json` mtime is
  within the last hour.
- Filesystem-crossing the move would be expensive. Verify
  `<artifact_roots[kind]>` and `<artifact_roots[kind]>/by-hash/` are
  on the same mount before starting.

## Out of scope for this design

- GC by content_hash (depends on tracking artifact references; not
  hard, but separate work).
- Alias-of-alias (cheap to add once per-user aliases are first-class).
- Storage tiers (cold/hot artifact placement) — fundamentally easier
  once content-addressing exists, but a separate project.
