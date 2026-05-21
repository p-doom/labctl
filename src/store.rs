// Many query methods on `Store` (and `EventRow` itself) are exclusively
// consumed by `server.rs`, which is gated behind the `ui` feature.
// Without that feature they look dead to the compiler but they're load-
// bearing for the UI build; tolerate the rust-analyzer noise rather
// than peppering each method with `#[cfg(feature = "ui")]`.
#![allow(dead_code)]

//! Filesystem-truth registry with in-memory SQLite cache.
//!
//! The on-disk tree under `<runs_base>` (and the per-kind artifact roots)
//! is the only authoritative state. See `fs_layout` for the schema.
//!
//! Every mutation here is a two-step:
//!   1. Atomically update the filesystem (tmp+rename for sidecars,
//!      first-writer-wins mkdir for namespace claims, append for events).
//!   2. Mirror the change into an in-memory SQLite cache so reads stay
//!      sub-millisecond.
//!
//! The cache schema is the same shape as the legacy on-disk schema — that
//! lets handlers in `server.rs` keep their queries verbatim. On
//! `Store::open` the indexer walks the tree and populates the cache.
//! The cache is disposable; if it ever drifts, restart the process.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::{ClusterConfig, InputSpec, Recipe},
    fs_layout::{
        self, AliasTargetSidecar, ArtifactSidecar, ClaimOutcome, EvalRequestSidecar, EventLine,
        InputSidecar, OutputLink, PipelineSidecar, RunSidecar, TrackingSidecar,
    },
    util,
};

// ---------- in-memory cache schema ----------
//
// Same shape as the legacy on-disk schema. Indexes are kept because the
// query planner relies on them; in-memory has plenty of headroom but the
// queries are unchanged from the on-disk era so we ship the same indexes.

const CACHE_SCHEMA: &str = r#"
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    recipe_name TEXT NOT NULL,
    recipe_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    job_id TEXT,
    run_dir TEXT NOT NULL,
    repo TEXT NOT NULL,
    source_path TEXT NOT NULL,
    recipe_json TEXT NOT NULL,
    context_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    finished_at INTEGER,
    pipeline_id TEXT,
    dependency_on TEXT,
    stage_name TEXT,
    submitted_by TEXT NOT NULL,
    cache_key TEXT,
    coalesced_peer_run_id TEXT
);

CREATE TABLE pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    pipeline_path TEXT,
    user TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    producer_run_id TEXT,
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    user TEXT NOT NULL,
    alias_segment TEXT NOT NULL
);

CREATE TABLE artifact_aliases (
    alias TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

-- Per-user alias overlay onto content-addressed artifacts. An artifact
-- (one content_hash, one id) can be referenced by multiple (user, alias,
-- kind) tuples — e.g. Alice produces ds, Bob produces byte-identical ds
-- with the same alias name; both get rows here pointing at the same
-- artifact_id. Disk truth lives at
-- `<artifact_roots[kind]>/aliases/<user>/<alias>` as a symlink to the
-- artifact's `_objects/<prefix>/<hash>/` dir.
CREATE TABLE artifact_user_aliases (
    user TEXT NOT NULL,
    alias TEXT NOT NULL,
    kind TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user, alias, kind)
);

CREATE TABLE run_inputs (
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    artifact_id TEXT,
    resolved_path TEXT NOT NULL,
    PRIMARY KEY (run_id, role)
);

CREATE TABLE run_outputs (
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (run_id, role, artifact_id)
);

CREATE TABLE eval_requests (
    eval_key TEXT PRIMARY KEY,
    checkpoint_artifact_id TEXT NOT NULL,
    eval_recipe_hash TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    eval_run_id TEXT,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    user TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE tracking (
    run_id TEXT PRIMARY KEY,
    entity TEXT NOT NULL,
    project TEXT NOT NULL,
    url TEXT NOT NULL,
    group_name TEXT,
    source TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

-- The events table exists in the cache for fast queries (events_for_run,
-- events_after). The disk truth is `events/<YYYYMMDD>.jsonl`. Cache ids
-- are process-local — two processes that both rebuild from the same
-- JSONL files will agree on ordering but their numeric ids may differ.
-- The SSE tailer treats id as an opaque cursor and never compares
-- across processes, so this is fine.
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_pipeline ON runs(pipeline_id);
CREATE INDEX idx_runs_recipe ON runs(recipe_name);
CREATE INDEX idx_runs_user ON runs(submitted_by);
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
CREATE INDEX idx_user_aliases_kind ON artifact_user_aliases(kind, user);
CREATE INDEX idx_runs_cache_key ON runs(cache_key);
"#;

// ---------- public types ----------
//
// Identical to the legacy types so call sites don't churn. `submitted_by`
// is now mandatory: the new layout encodes it in the path, so every row
// must have a value. Callers that don't know who they are should use the
// `submitted_by_or_unknown` helper or pass the legacy "unknown" sentinel.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRow {
    pub id: String,
    pub recipe_name: String,
    pub recipe_hash: String,
    pub status: String,
    pub job_id: Option<String>,
    pub run_dir: PathBuf,
    pub repo: String,
    pub source_path: PathBuf,
    pub recipe_json: Value,
    pub context_json: Value,
    pub created_at: i64,
    pub finished_at: Option<i64>,
    pub pipeline_id: Option<String>,
    pub stage_name: Option<String>,
    pub dependency_on: Option<Value>,
    pub submitted_by: Option<String>,
    pub cache_key: Option<String>,
    /// Set on follower runs: the run_id this run is coalesced against.
    /// When the peer reaches a terminal state, the resolver flips this run
    /// to ``cache_hit`` (peer succeeded) or ``failed`` (peer failed) and
    /// links the peer's outputs in. None on producer / non-coalesced runs.
    pub coalesced_peer_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRow {
    pub id: String,
    pub kind: String,
    pub path: PathBuf,
    pub content_hash: String,
    pub producer_run_id: Option<String>,
    pub metadata_json: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub id: i64,
    pub run_id: Option<String>,
    pub event_type: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingRow {
    pub run_id: String,
    pub entity: String,
    pub project: String,
    pub url: String,
    pub group_name: Option<String>,
    pub source: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputResolution {
    pub role: String,
    pub artifact_id: Option<String>,
    pub resolved_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunView {
    pub run: RunRow,
    pub inputs: Vec<InputResolution>,
    pub outputs: Vec<ArtifactRow>,
    pub aliases: Vec<(String, String)>,
    pub eval_requests: Vec<Value>,
}

#[derive(Debug, Clone, Copy)]
pub enum EvalRequestSlot {
    Fresh,
    Active,
    Retry { previous_attempts: i64 },
    Exhausted { attempts: i64 },
}

pub struct NewRun<'a> {
    pub id: &'a str,
    pub recipe: &'a Recipe,
    pub recipe_hash: &'a str,
    pub status: &'a str,
    pub run_dir: &'a Path,
    pub source_path: &'a Path,
    pub context_json: &'a Value,
    /// Leave None to default to the current OS user. The path-canonical
    /// layout requires a value at write time; None is resolved against
    /// `$USER` inside `insert_run`. Callers that already know who the
    /// submitter is (CLI side) should pass `Some(...)`.
    pub submitted_by: Option<&'a str>,
    /// Stage-level cache key (sha256 of recipe + provenance + inputs + args).
    /// Used at submit time to short-circuit re-execution of an already-
    /// materialized stage. None disables cache-hit lookup for this run.
    pub cache_key: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRow {
    pub id: String,
    pub name: String,
    pub pipeline_path: Option<PathBuf>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySummaryRow {
    pub name: String,
    pub total: i64,
    pub failed: i64,
    pub running: i64,
    pub last_fired_at: i64,
}

pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "failed" | "cancelled" | "timeout" | "oom" | "unknown_terminal" | "cache_hit"
    )
}

/// Followers waiting on a coalesce peer. Non-terminal: the resolver loop
/// must keep watching them. Separate from ``is_terminal`` so callers that
/// gate on "this run can never change" don't accidentally treat a follower
/// as final.
pub fn is_awaiting_peer(status: &str) -> bool {
    status == "awaiting_peer"
}

// ---------- the store ----------

pub struct Store {
    runs_base: PathBuf,
    artifact_roots: BTreeMap<String, PathBuf>,
    cache: Connection,
}

/// Snapshot of one events-table row, in the column order required by
/// `restore_events_into`. Used to ferry the live events table across
/// a cache rebuild (the new connection starts empty; we re-insert
/// these rows preserving their ids so the SSE tailer's cursor stays
/// valid).
type EventSnapshot = (i64, Option<String>, String, String, i64);

impl Store {
    /// Open the registry against a cluster config. Walks the filesystem
    /// tree under `cluster.filesystem.runs_base` and the per-kind
    /// `artifact_roots`, populating an in-memory SQLite cache.
    pub fn open(cluster: &ClusterConfig) -> Result<Self> {
        let runs_base = cluster.filesystem.runs_base.clone();
        // Pre-create the top-level subdirs so the indexer's walks don't
        // need to handle "first run, nothing exists" specially.
        for sub in [
            fs_layout::RUNS_DIR,
            fs_layout::ALIASES_DIR,
            fs_layout::EVAL_STATE_DIR,
            fs_layout::PIPELINES_DIR,
            fs_layout::EVENTS_DIR,
            fs_layout::COALESCE_CLAIMS_DIR,
        ] {
            fs::create_dir_all(runs_base.join(sub))
                .with_context(|| format!("failed to create {}/{}", runs_base.display(), sub))?;
        }
        let artifact_roots = cluster.filesystem.artifact_roots.clone();
        // Initial cache build runs under no concurrency (no other
        // threads yet) so we just do it inline. Subsequent refreshes
        // use the off-mutex swap path (see `build_disk_snapshot`).
        let cache = build_in_memory_cache(&runs_base, &artifact_roots, true, &[])
            .context("filesystem indexer failed")?;
        Ok(Self {
            runs_base,
            artifact_roots,
            cache,
        })
    }

    /// Paths needed to build a fresh disk snapshot without holding a
    /// reference to the Store. The caller takes the brief Store lock,
    /// invokes this, releases the lock, then calls
    /// `build_disk_snapshot` off-thread. Used by the periodic refresh
    /// task to keep the slow walk off the std::Mutex.
    pub fn snapshot_paths(&self) -> (PathBuf, BTreeMap<String, PathBuf>) {
        (self.runs_base.clone(), self.artifact_roots.clone())
    }

    /// Snapshot the events table for ferrying across a cache rebuild.
    /// Pairs with `build_disk_snapshot`. Held under the Store lock
    /// only for the SELECT; the slow part (filesystem walk) happens
    /// after release with these events in hand.
    pub fn snapshot_events(&self) -> Result<Vec<EventSnapshot>> {
        let mut stmt = self.cache.prepare(
            "SELECT id, run_id, event_type, payload_json, created_at FROM events",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Build a fresh in-memory cache from disk + a snapshot of the
    /// current events table. Pure — no Store reference, no mutex —
    /// suitable to call from `tokio::task::spawn_blocking`. The
    /// returned Connection is ready to replace the live cache via
    /// `replace_cache`. Skipping events from the disk walk (the JSONL
    /// log) is deliberate: rebuilding from JSONL would assign new
    /// AUTOINCREMENT ids and invalidate the SSE tailer's cursor;
    /// instead we carry the live in-process events forward.
    pub fn build_disk_snapshot(
        runs_base: &Path,
        artifact_roots: &BTreeMap<String, PathBuf>,
        events: &[EventSnapshot],
    ) -> Result<Connection> {
        build_in_memory_cache(runs_base, artifact_roots, false, events)
    }

    /// Atomically replace the in-memory cache. The previous Connection
    /// is dropped at the end of the call. The Store lock must be held
    /// during the call but only for the duration of the field swap —
    /// microseconds.
    pub fn replace_cache(&mut self, new_cache: Connection) {
        self.cache = new_cache;
    }

    /// Walk the filesystem tree and populate the in-memory cache from
    /// scratch. Idempotent — clears every cache table before re-ingesting.
    pub fn reindex(&mut self) -> Result<()> {
        self.reindex_inner(true)
    }

    /// Same walk as `reindex`, but leaves the events table untouched.
    /// Used by single-threaded callers that don't need the off-mutex
    /// swap path (e.g. legacy CLI subcommands). The HTTP/agent
    /// processes use `snapshot_events` + `build_disk_snapshot` +
    /// `replace_cache` instead — that path is what makes refresh
    /// non-blocking for concurrent readers.
    pub fn refresh_from_disk(&mut self) -> Result<()> {
        self.reindex_inner(false)
    }

    fn reindex_inner(&mut self, include_events: bool) -> Result<()> {
        // Single-threaded in-place rebuild: snapshot the live events
        // first (if we're preserving them), then build a fresh cache
        // and swap it in. The Store's std::Mutex is held by whoever
        // called us, so concurrency isn't a concern at this layer —
        // this path is for CLI subcommands and one-shot work.
        let preserved = if include_events {
            Vec::new()
        } else {
            self.snapshot_events()?
        };
        self.cache = build_in_memory_cache(
            &self.runs_base,
            &self.artifact_roots,
            include_events,
            &preserved,
        )?;
        Ok(())
    }

    // ---------- runs ----------

    pub fn insert_run(&mut self, run: NewRun<'_>, inputs: &[InputResolution]) -> Result<()> {
        let resolved_user = match run.submitted_by {
            Some(u) => u.to_string(),
            None => current_user()?,
        };
        fs_layout::validate_user(&resolved_user)?;
        let submitted_by = resolved_user.as_str();
        let now = util::now_ts();
        let lab_dir = fs_layout::run_lab_dir(&self.runs_base, submitted_by, run.id);
        fs::create_dir_all(&lab_dir).with_context(|| {
            format!("failed to create lab dir {}", lab_dir.display())
        })?;

        // 1. Filesystem: run.json + inputs.json sidecars.
        let sidecar = RunSidecar {
            id: run.id.to_string(),
            recipe_name: run.recipe.name.clone(),
            recipe_hash: run.recipe_hash.to_string(),
            repo: run.recipe.repo.clone(),
            run_dir: run.run_dir.to_path_buf(),
            source_path: run.source_path.to_path_buf(),
            created_at: now,
            submitted_by: submitted_by.to_string(),
            recipe: serde_json::to_value(run.recipe)?,
            context: run.context_json.clone(),
            pipeline_id: None,
            stage_name: None,
            dependency_on: None,
            status: run.status.to_string(),
            job_id: None,
            finished_at: None,
            cache_key: run.cache_key.map(|s| s.to_string()),
            coalesced_peer_run_id: None,
        };
        fs_layout::atomic_write_json(&lab_dir.join(fs_layout::RUN_JSON), &sidecar)?;

        let input_sidecars: Vec<InputSidecar> = inputs
            .iter()
            .map(|i| InputSidecar {
                role: i.role.clone(),
                artifact_id: i.artifact_id.clone(),
                resolved_path: i.resolved_path.clone(),
            })
            .collect();
        fs_layout::atomic_write_json(&lab_dir.join(fs_layout::INPUTS_JSON), &input_sidecars)?;

        // 2. Cache mirror.
        let tx = self.cache.transaction()?;
        tx.execute(
            "INSERT INTO runs
             (id, recipe_name, recipe_hash, status, run_dir, repo, source_path,
              recipe_json, context_json, created_at, submitted_by, cache_key,
              coalesced_peer_run_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            params![
                run.id,
                run.recipe.name,
                run.recipe_hash,
                run.status,
                run.run_dir.display().to_string(),
                run.recipe.repo,
                run.source_path.display().to_string(),
                serde_json::to_string(run.recipe)?,
                serde_json::to_string(run.context_json)?,
                now,
                submitted_by,
                run.cache_key,
            ],
        )?;
        for input in inputs {
            tx.execute(
                "INSERT INTO run_inputs (run_id, role, artifact_id, resolved_path)
                 VALUES (?, ?, ?, ?)",
                params![
                    run.id,
                    input.role,
                    input.artifact_id,
                    input.resolved_path.display().to_string()
                ],
            )?;
        }
        tx.commit()?;

        // 3. Append a creation event.
        self.append_event(EventLine {
            run_id: Some(run.id.to_string()),
            event_type: "run_created".to_string(),
            payload: run.context_json.clone(),
            created_at: now,
        })?;
        Ok(())
    }

    pub fn set_submitted(&mut self, run_id: &str, job_id: &str) -> Result<()> {
        let user = self.user_for_run(run_id)?;
        let lab = fs_layout::run_lab_dir(&self.runs_base, &user, run_id);
        let mut sidecar: RunSidecar =
            fs_layout::read_json(&lab.join(fs_layout::RUN_JSON))?;
        sidecar.status = "submitted".to_string();
        sidecar.job_id = Some(job_id.to_string());
        fs_layout::atomic_write_json(&lab.join(fs_layout::RUN_JSON), &sidecar)?;
        self.cache.execute(
            "UPDATE runs SET status='submitted', job_id=? WHERE id=?",
            params![job_id, run_id],
        )?;
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "run_submitted".to_string(),
            payload: json!({ "job_id": job_id }),
            created_at: util::now_ts(),
        })?;
        Ok(())
    }

    pub fn update_status(
        &mut self,
        run_id: &str,
        status: &str,
        finished_at: Option<i64>,
    ) -> Result<()> {
        let terminal = is_terminal(status);
        let ts = finished_at.unwrap_or_else(util::now_ts);
        let user = self.user_for_run(run_id)?;
        let lab = fs_layout::run_lab_dir(&self.runs_base, &user, run_id);
        let mut sidecar: RunSidecar =
            fs_layout::read_json(&lab.join(fs_layout::RUN_JSON))?;
        sidecar.status = status.to_string();
        if terminal && sidecar.finished_at.is_none() {
            sidecar.finished_at = Some(ts);
        }
        fs_layout::atomic_write_json(&lab.join(fs_layout::RUN_JSON), &sidecar)?;
        self.cache.execute(
            "UPDATE runs SET status=?, finished_at=CASE WHEN ? THEN ? ELSE finished_at END WHERE id=?",
            params![status, terminal, ts, run_id],
        )?;
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "run_status".to_string(),
            payload: json!({ "status": status }),
            created_at: util::now_ts(),
        })?;
        Ok(())
    }

    pub fn set_finished_at(&mut self, run_id: &str, finished_at: i64) -> Result<()> {
        let user = self.user_for_run(run_id)?;
        let lab = fs_layout::run_lab_dir(&self.runs_base, &user, run_id);
        let mut sidecar: RunSidecar =
            fs_layout::read_json(&lab.join(fs_layout::RUN_JSON))?;
        sidecar.finished_at = Some(finished_at);
        fs_layout::atomic_write_json(&lab.join(fs_layout::RUN_JSON), &sidecar)?;
        self.cache.execute(
            "UPDATE runs SET finished_at=? WHERE id=?",
            params![finished_at, run_id],
        )?;
        Ok(())
    }

    pub fn get_run(&self, run_id: &str) -> Result<RunRow> {
        self.cache
            .query_row("SELECT * FROM runs WHERE id=?", params![run_id], row_to_run)
            .optional()?
            .with_context(|| format!("run not found: {run_id}"))
    }

    pub fn runs_by_recipe(&self, recipe_name: &str) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs WHERE recipe_name=? ORDER BY created_at DESC",
        )?;
        Ok(stmt
            .query_map(params![recipe_name], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_runs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self
            .cache
            .prepare("SELECT * FROM runs ORDER BY created_at DESC")?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn terminal_runs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs WHERE status IN
             ('succeeded','failed','cancelled','timeout','oom','unknown_terminal')
             ORDER BY created_at DESC",
        )?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn terminal_runs_without_outputs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT r.* FROM runs r
             LEFT JOIN run_outputs ro ON ro.run_id = r.id
             WHERE r.status IN ('succeeded', 'failed', 'cancelled', 'timeout', 'oom', 'unknown_terminal')
               AND ro.run_id IS NULL
             ORDER BY r.created_at DESC",
        )?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Active runs owned by `submitted_by`. Scoped so a daemon never
    /// reconciles another user's runs — critical for multi-tenant
    /// deployments where each user runs their own daemon over a shared
    /// filesystem-truth registry. A daemon writing status updates,
    /// registered outputs, or tracking rows for runs it doesn't own
    /// would race with that user's own daemon.
    pub fn list_active_runs(&self, submitted_by: &str) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs
              WHERE status IN ('created', 'submitted', 'running', 'awaiting_peer')
                AND submitted_by = ?
              ORDER BY created_at ASC",
        )?;
        Ok(stmt
            .query_map(params![submitted_by], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- artifacts ----------

    /// Register an artifact under content-addressed storage.
    ///
    /// `staging_path` is where the run wrote its bytes — `<artifact_root>/
    /// <user>/<alias>/` (the legacy convention). The function:
    ///   1. dedups by `(kind, content_hash)` — if a prior artifact has the
    ///      same content, the staging bytes are discarded (atomic rmdir-
    ///      if-empty / remove_dir_all on a duplicate) and a new per-user
    ///      alias overlay is added pointing at the existing artifact.
    ///   2. otherwise atomically `rename(2)`s the staging dir into
    ///      `<artifact_root>/_objects/<prefix>/<content_hash>/`,
    ///      writes the sidecar inside it, inserts the artifact row,
    ///      and creates the per-user alias symlink + overlay row.
    ///
    /// Same-filesystem rename is atomic on Linux; if the staging path
    /// and the by-hash path live on different mounts the rename returns
    /// EXDEV and the caller gets a clear error. `labctl init`'s
    /// shared-perms setup keeps both under the same `artifact_root`.
    pub fn insert_artifact(
        &mut self,
        kind: &str,
        staging_path: &Path,
        content_hash: &str,
        producer_run_id: Option<&str>,
        metadata: &Value,
    ) -> Result<ArtifactRow> {
        let root = self
            .artifact_roots
            .get(kind)
            .with_context(|| format!("kind {kind:?} not in cluster.filesystem.artifact_roots"))?
            .clone();
        // The staging path is <root>/<user>/<alias>/. The (user, alias)
        // tuple becomes the first per-user overlay onto the artifact.
        // Tolerate paths already inside _objects/ (re-registration after
        // crash recovery): treat the by-hash dir itself as the source.
        let user_alias = decompose_artifact_path(staging_path, &root).ok();

        if let Some(existing) = self.find_artifact_by_hash(kind, content_hash)? {
            // Dedup: bytes already canonical. Discard the staging copy
            // if it differs from the canonical location.
            if staging_path != existing.path && staging_path.exists() {
                fs::remove_dir_all(staging_path).with_context(|| {
                    format!(
                        "failed to remove redundant staging dir {} after \
                         content-hash dedup matched existing {}",
                        staging_path.display(),
                        existing.path.display()
                    )
                })?;
            }
            // Even on dedup we may need to record a new per-user alias
            // overlay (Bob just registered byte-identical content that
            // Alice produced earlier).
            if let Some((user, alias)) = user_alias.as_ref() {
                self.add_user_alias(kind, user, alias, &existing.id, &existing.path)?;
            }
            self.rehydrate_inputs_by_path(&existing.path, &existing.id)?;
            return Ok(existing);
        }

        // Fresh artifact. Move bytes to the by-hash slot.
        let canonical = fs_layout::content_addressed_dir(&root, content_hash);
        if !canonical.exists() {
            if let Some(parent) = canonical.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create {}", parent.display())
                })?;
            }
            fs::rename(staging_path, &canonical).with_context(|| {
                format!(
                    "failed to move staging dir {} -> {} (must be same filesystem)",
                    staging_path.display(),
                    canonical.display(),
                )
            })?;
        } else if staging_path != canonical && staging_path.exists() {
            // Race: another process moved bytes here first. Drop ours.
            fs::remove_dir_all(staging_path).ok();
        }

        let id = format!("artifact_{}", &content_hash[..16.min(content_hash.len())]);
        let now = util::now_ts();
        // For artifacts produced without a (user, alias) overlay
        // (e.g. ad-hoc rebuild flows), fall back to placeholder values
        // in the sidecar — the row's authoritative user/alias_segment
        // come from the artifact_user_aliases overlay rows.
        let (user, alias_segment) = user_alias.unwrap_or_else(|| ("unknown".into(), id.clone()));
        let sidecar = ArtifactSidecar {
            id: id.clone(),
            kind: kind.to_string(),
            user: user.clone(),
            alias: alias_segment.clone(),
            content_hash: content_hash.to_string(),
            producer_run_id: producer_run_id.map(str::to_string),
            metadata: metadata.clone(),
            created_at: now,
        };
        fs_layout::atomic_write_json(&canonical.join(fs_layout::ARTIFACT_META), &sidecar)?;

        self.cache.execute(
            "INSERT INTO artifacts
             (id, kind, path, content_hash, producer_run_id, metadata_json,
              created_at, user, alias_segment)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                id,
                kind,
                canonical.display().to_string(),
                content_hash,
                producer_run_id,
                serde_json::to_string(metadata)?,
                now,
                user,
                alias_segment,
            ],
        )?;
        self.add_user_alias(kind, &user, &alias_segment, &id, &canonical)?;
        self.rehydrate_inputs_by_path(&canonical, &id)?;
        if let Some(run_id) = producer_run_id {
            self.append_event(EventLine {
                run_id: Some(run_id.to_string()),
                event_type: "artifact_registered".to_string(),
                payload: json!({ "artifact_id": id, "kind": kind, "path": canonical }),
                created_at: now,
            })?;
        }
        self.get_artifact(&id)
    }

    /// Write a per-user alias overlay: a symlink at
    /// `<artifact_root>/aliases/<user>/<alias>` pointing at the
    /// artifact's canonical `_objects/<prefix>/<hash>/` dir, plus an
    /// `artifact_user_aliases` row. Idempotent — re-adding the same
    /// (user, alias, kind) is a no-op.
    fn add_user_alias(
        &self,
        kind: &str,
        user: &str,
        alias: &str,
        artifact_id: &str,
        target: &Path,
    ) -> Result<()> {
        let root = self.artifact_roots.get(kind).with_context(|| {
            format!("kind {kind:?} not in cluster.filesystem.artifact_roots")
        })?;
        let link = fs_layout::alias_symlink_path(root, user, alias);
        fs_layout::create_alias_symlink(&link, target)?;
        self.cache.execute(
            "INSERT OR IGNORE INTO artifact_user_aliases
             (user, alias, kind, artifact_id, created_at)
             VALUES (?, ?, ?, ?, ?)",
            params![user, alias, kind, artifact_id, util::now_ts()],
        )?;
        Ok(())
    }

    /// Path-based rehydration: update any run_inputs rows whose
    /// `resolved_path` matches this artifact's path but whose `artifact_id`
    /// is NULL. Writes through cache + `inputs.json` sidecars.
    ///
    /// **Not the primary chain-input mechanism.** Pipeline-stage chain
    /// wiring is done structurally via `backfill_stage_consumers` (called
    /// from `register_outputs`, `copy_run_outputs`, and at submit time in
    /// `runner::resolve_inputs`). This function is the narrower fallback
    /// for *out-of-band* artifact materializations: a `register-external`
    /// invocation, or an artifact arriving at a path some run had pre-
    /// recorded as its `External` / non-pipeline input. Keep it for that
    /// case; do not rely on it for pipeline correctness.
    fn rehydrate_inputs_by_path(&self, artifact_path: &Path, artifact_id: &str) -> Result<()> {
        let path_str = artifact_path.display().to_string();
        // Find the runs whose inputs reference this path with NULL artifact_id.
        let runs: Vec<String> = {
            let mut stmt = self.cache.prepare(
                "SELECT DISTINCT run_id FROM run_inputs
                 WHERE resolved_path = ? AND artifact_id IS NULL",
            )?;
            stmt.query_map(params![&path_str], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        // Update the cache.
        self.cache.execute(
            "UPDATE run_inputs SET artifact_id = ?
             WHERE resolved_path = ? AND artifact_id IS NULL",
            params![artifact_id, &path_str],
        )?;
        // Update each affected inputs.json on disk.
        for run_id in runs {
            let user = match self.user_for_run(&run_id) {
                Ok(u) => u,
                Err(_) => continue, // run may not yet have a sidecar in degenerate states
            };
            let inputs_path = fs_layout::run_lab_dir(&self.runs_base, &user, &run_id)
                .join(fs_layout::INPUTS_JSON);
            let mut inputs: Vec<InputSidecar> = match fs_layout::read_json_optional(&inputs_path)? {
                Some(v) => v,
                None => continue,
            };
            let mut changed = false;
            for input in &mut inputs {
                if input.artifact_id.is_none() && input.resolved_path == artifact_path {
                    input.artifact_id = Some(artifact_id.to_string());
                    changed = true;
                }
            }
            if changed {
                fs_layout::atomic_write_json(&inputs_path, &inputs)?;
            }
        }
        Ok(())
    }

    pub fn find_artifact_by_hash(
        &self,
        kind: &str,
        content_hash: &str,
    ) -> Result<Option<ArtifactRow>> {
        self.cache
            .query_row(
                "SELECT * FROM artifacts WHERE kind=? AND content_hash=?",
                params![kind, content_hash],
                row_to_artifact,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn find_artifact_by_path(
        &self,
        kind: &str,
        path: &Path,
    ) -> Result<Option<ArtifactRow>> {
        self.cache
            .query_row(
                "SELECT * FROM artifacts WHERE kind=? AND path=?",
                params![kind, path.display().to_string()],
                row_to_artifact,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_artifact(&self, id: &str) -> Result<ArtifactRow> {
        self.get_artifact_optional(id)?
            .with_context(|| format!("artifact not found: {id}"))
    }

    pub fn get_artifact_optional(&self, id: &str) -> Result<Option<ArtifactRow>> {
        self.cache
            .query_row(
                "SELECT * FROM artifacts WHERE id=?",
                params![id],
                row_to_artifact,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn artifacts_by_kind(&self, kind: &str) -> Result<Vec<ArtifactRow>> {
        let mut stmt = self
            .cache
            .prepare("SELECT * FROM artifacts WHERE kind=? ORDER BY created_at ASC")?;
        Ok(stmt
            .query_map(params![kind], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Artifacts of a given kind whose producing run was submitted by
    /// `user`. Used by evald to scope each daemon's dispatch to its own
    /// user's checkpoints. Externally-registered artifacts (no producer
    /// run) are excluded — they have no owner attribution, so no
    /// per-user daemon claims them. Re-register them under your own
    /// uid if you want them evaluated.
    pub fn artifacts_by_kind_for_producer_user(
        &self,
        kind: &str,
        user: &str,
    ) -> Result<Vec<ArtifactRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT a.* FROM artifacts a
             JOIN runs r ON a.producer_run_id = r.id
             WHERE a.kind = ? AND r.submitted_by = ?
             ORDER BY a.created_at ASC",
        )?;
        Ok(stmt
            .query_map(params![kind, user], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_artifacts(&self) -> Result<Vec<ArtifactRow>> {
        let mut stmt = self
            .cache
            .prepare("SELECT * FROM artifacts ORDER BY created_at DESC")?;
        Ok(stmt
            .query_map([], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn artifact_consumers(&self, artifact_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.cache.prepare(
            "SELECT run_id, role FROM run_inputs WHERE artifact_id=? ORDER BY run_id",
        )?;
        Ok(stmt
            .query_map(params![artifact_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn aliases_for_artifact(&self, artifact_id: &str) -> Result<Vec<String>> {
        let mut stmt = self.cache.prepare(
            "SELECT alias FROM artifact_aliases WHERE artifact_id=? ORDER BY alias",
        )?;
        Ok(stmt
            .query_map(params![artifact_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- aliases ----------

    pub fn set_alias(&mut self, alias: &str, artifact_id: &str) -> Result<()> {
        let artifact = self.get_artifact(artifact_id)?;
        let alias_dir = fs_layout::alias_dir(&self.runs_base, alias);
        // mkdir is the atomic claim. If someone else won, we still
        // overwrite the .target.json — that matches the legacy
        // INSERT OR REPLACE semantics where the latest writer wins.
        let _ = fs_layout::claim_dir(&alias_dir)?;
        let now = util::now_ts();
        let target = AliasTargetSidecar {
            artifact_id: artifact_id.to_string(),
            artifact_path: artifact.path.clone(),
            created_at: now,
        };
        fs_layout::atomic_write_json(
            &fs_layout::alias_target(&self.runs_base, alias),
            &target,
        )?;
        self.cache.execute(
            "INSERT OR REPLACE INTO artifact_aliases (alias, artifact_id, created_at)
             VALUES (?, ?, ?)",
            params![alias, artifact_id, now],
        )?;
        Ok(())
    }

    pub fn resolve_artifact_ref(&self, reference: &str) -> Result<ArtifactRow> {
        if let Some(row) = self.get_artifact_optional(reference)? {
            return Ok(row);
        }
        let artifact_id: Option<String> = self
            .cache
            .query_row(
                "SELECT artifact_id FROM artifact_aliases WHERE alias=?",
                params![reference],
                |row| row.get(0),
            )
            .optional()?;
        match artifact_id {
            Some(id) => self.get_artifact(&id),
            None => bail!("artifact or alias not found: {reference}"),
        }
    }

    // ---------- run inputs/outputs ----------

    pub fn link_run_output(&mut self, run_id: &str, role: &str, artifact_id: &str) -> Result<()> {
        // Update the outputs.json sidecar (insert if not present).
        let user = self.user_for_run(run_id)?;
        let outputs_path = fs_layout::run_lab_dir(&self.runs_base, &user, run_id)
            .join(fs_layout::OUTPUTS_JSON);
        let mut links: Vec<OutputLink> =
            fs_layout::read_json_optional(&outputs_path)?.unwrap_or_default();
        if !links
            .iter()
            .any(|l| l.role == role && l.artifact_id == artifact_id)
        {
            links.push(OutputLink {
                role: role.to_string(),
                artifact_id: artifact_id.to_string(),
            });
            fs_layout::atomic_write_json(&outputs_path, &links)?;
        }
        self.cache.execute(
            "INSERT OR IGNORE INTO run_outputs (run_id, role, artifact_id) VALUES (?, ?, ?)",
            params![run_id, role, artifact_id],
        )?;
        Ok(())
    }

    pub fn run_inputs(&self, run_id: &str) -> Result<Vec<InputResolution>> {
        let mut stmt = self.cache.prepare(
            "SELECT role, artifact_id, resolved_path FROM run_inputs WHERE run_id=? ORDER BY role",
        )?;
        Ok(stmt
            .query_map(params![run_id], |row| {
                Ok(InputResolution {
                    role: row.get(0)?,
                    artifact_id: row.get(1)?,
                    resolved_path: PathBuf::from(row.get::<_, String>(2)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn run_outputs(&self, run_id: &str) -> Result<Vec<ArtifactRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT a.* FROM artifacts a
             JOIN run_outputs ro ON ro.artifact_id=a.id
             WHERE ro.run_id=?
             ORDER BY a.created_at, a.id",
        )?;
        Ok(stmt
            .query_map(params![run_id], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Look up the most-recent succeeded or cache-hit run with this cache key.
    /// Returns None if no prior run matches. Caller must still verify the
    /// run's outputs are on disk before declaring a real cache hit.
    pub fn find_cache_hit_candidate(&self, cache_key: &str) -> Result<Option<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs
             WHERE cache_key = ? AND status IN ('succeeded', 'cache_hit')
             ORDER BY created_at DESC
             LIMIT 1",
        )?;
        let row = stmt
            .query_map(params![cache_key], row_to_run)?
            .next()
            .transpose()?;
        Ok(row)
    }

    pub fn append_stage_cache_hit_event(
        &mut self,
        run_id: &str,
        cache_key: &str,
        source_run_id: &str,
    ) -> Result<()> {
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "stage_cache_hit".to_string(),
            payload: json!({
                "cache_key": cache_key,
                "source_run_id": source_run_id,
            }),
            created_at: util::now_ts(),
        })
    }

    // ---------- in-flight coalescing ----------
    //
    // Pairs with the stage cache-hit feature: when a brand-new submission's
    // cache_key matches an *in-flight* peer's cache_key, register the new
    // run as a follower instead of duplicating the work. The first writer
    // wins the slot via atomic mkdir; subsequent writers read the producer
    // run_id and depend on its job_id.

    /// Find an in-flight producer with this cache_key. Excludes followers
    /// (status = 'awaiting_peer') so we never form chains of followers, and
    /// requires a job_id so the caller can build an ``afterok:`` SLURM
    /// dependency right away. ``find_cache_hit_candidate`` covers the
    /// already-terminal case; this one is the active-peer counterpart.
    pub fn find_coalesce_peer(&self, cache_key: &str) -> Result<Option<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs
              WHERE cache_key = ?
                AND status IN ('submitted', 'running')
                AND job_id IS NOT NULL
              ORDER BY created_at ASC
              LIMIT 1",
        )?;
        let row = stmt
            .query_map(params![cache_key], row_to_run)?
            .next()
            .transpose()?;
        Ok(row)
    }

    /// Atomically claim the coalesce slot for ``cache_key``. First writer
    /// wins via mkdir; subsequent writers get ``AlreadyExists`` and should
    /// look up the producer via ``find_coalesce_peer``. Writes a
    /// ``.target.json`` recording the claimer; later callers can use it to
    /// recover from stale slots.
    pub fn claim_coalesce_slot(
        &self,
        cache_key: &str,
        producer_run_id: &str,
    ) -> Result<fs_layout::ClaimOutcome> {
        let dir = fs_layout::coalesce_claim_dir(&self.runs_base, cache_key);
        let outcome = fs_layout::claim_dir(&dir)?;
        if outcome == fs_layout::ClaimOutcome::Claimed {
            let target = fs_layout::coalesce_claim_target(&self.runs_base, cache_key);
            let sidecar = fs_layout::CoalesceClaimSidecar {
                producer_run_id: producer_run_id.to_string(),
                claimed_at: util::now_ts(),
            };
            fs_layout::atomic_write_json(&target, &sidecar)?;
        }
        Ok(outcome)
    }

    /// Read the claimer recorded in the slot's ``.target.json``. Used by a
    /// follower right after it gets ``AlreadyExists`` from
    /// ``claim_coalesce_slot``. Returns ``None`` if the slot exists but the
    /// sidecar hasn't been written yet (the claimer is between mkdir and
    /// atomic_write_json) — caller should treat that as a stale-slot signal.
    pub fn read_coalesce_claim(
        &self,
        cache_key: &str,
    ) -> Result<Option<fs_layout::CoalesceClaimSidecar>> {
        let target = fs_layout::coalesce_claim_target(&self.runs_base, cache_key);
        fs_layout::read_json_optional(&target)
    }

    /// Force-remove a coalesce slot. Used to clear a stale claim whose
    /// producer either never inserted a registry row (crashed between
    /// mkdir and insert_run) or finished long ago without anyone GC'ing
    /// the dir. Best-effort: missing dir is not an error.
    pub fn release_coalesce_slot(&self, cache_key: &str) -> Result<()> {
        let dir = fs_layout::coalesce_claim_dir(&self.runs_base, cache_key);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| {
                format!("failed to release coalesce slot {}", dir.display())
            }),
        }
    }

    /// Mark a freshly-submitted follower as ``awaiting_peer``. Sets the
    /// trampoline's ``job_id``, the peer run_id, and flips status all in one
    /// step — paired with the producer's ``set_submitted`` but for the
    /// follower path. Emits a ``stage_coalesced`` event so downstream
    /// tooling can attribute the wait.
    pub fn set_awaiting_peer(
        &mut self,
        run_id: &str,
        job_id: &str,
        peer_run_id: &str,
        cache_key: &str,
    ) -> Result<()> {
        let user = self.user_for_run(run_id)?;
        let lab = fs_layout::run_lab_dir(&self.runs_base, &user, run_id);
        let mut sidecar: RunSidecar =
            fs_layout::read_json(&lab.join(fs_layout::RUN_JSON))?;
        sidecar.status = "awaiting_peer".to_string();
        sidecar.job_id = Some(job_id.to_string());
        sidecar.coalesced_peer_run_id = Some(peer_run_id.to_string());
        fs_layout::atomic_write_json(&lab.join(fs_layout::RUN_JSON), &sidecar)?;
        self.cache.execute(
            "UPDATE runs SET status='awaiting_peer', job_id=?, coalesced_peer_run_id=? WHERE id=?",
            params![job_id, peer_run_id, run_id],
        )?;
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "stage_coalesced".to_string(),
            payload: json!({
                "peer_run_id": peer_run_id,
                "cache_key": cache_key,
                "job_id": job_id,
            }),
            created_at: util::now_ts(),
        })?;
        Ok(())
    }

    pub fn append_stage_coalesce_resolved_event(
        &mut self,
        run_id: &str,
        peer_run_id: &str,
    ) -> Result<()> {
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "stage_coalesce_resolved".to_string(),
            payload: json!({
                "peer_run_id": peer_run_id,
                "outcome": "cache_hit",
            }),
            created_at: util::now_ts(),
        })
    }

    pub fn append_stage_coalesce_failed_event(
        &mut self,
        run_id: &str,
        peer_run_id: &str,
        peer_status: &str,
    ) -> Result<()> {
        self.append_event(EventLine {
            run_id: Some(run_id.to_string()),
            event_type: "stage_coalesce_failed".to_string(),
            payload: json!({
                "peer_run_id": peer_run_id,
                "peer_status": peer_status,
            }),
            created_at: util::now_ts(),
        })
    }

    /// Copy the run_outputs rows from `source_run_id` to `dest_run_id`,
    /// preserving role and artifact_id. Used by cache-hit submission to
    /// link existing artifacts as the new run's outputs without touching
    /// the artifacts table (so producer_run_id stays pointing at the
    /// original producer). After linking, walks downstream pipeline
    /// stages and backfills any `type=stage` inputs that point at
    /// `dest_run_id` — see `backfill_stage_consumers`.
    pub fn copy_run_outputs(&mut self, source_run_id: &str, dest_run_id: &str) -> Result<()> {
        let rows = self.run_output_links(source_run_id)?;
        for (role, artifact_id) in &rows {
            self.link_run_output(dest_run_id, role, artifact_id)?;
        }
        self.backfill_stage_consumers(dest_run_id, &rows)?;
        Ok(())
    }

    /// `(role, artifact_id)` tuples for every output linked to `run_id`.
    /// Sister of `run_outputs`, which returns the joined artifact rows.
    pub fn run_output_links(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self
            .cache
            .prepare("SELECT role, artifact_id FROM run_outputs WHERE run_id = ?")?;
        Ok(stmt
            .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Look up the artifact_id linked as `run_id`'s output for `role`.
    /// `None` if the role is unfilled (run hasn't produced this output yet).
    pub fn run_output_artifact_id(
        &self,
        run_id: &str,
        role: &str,
    ) -> Result<Option<String>> {
        self.cache
            .query_row(
                "SELECT artifact_id FROM run_outputs WHERE run_id=? AND role=?",
                params![run_id, role],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Set `run_inputs[(run_id, role)].artifact_id` and `resolved_path`
    /// if and only if `artifact_id` is currently NULL. Sources
    /// `resolved_path` from the linked artifact's actual on-disk path —
    /// not from the consumer's `<root>/<submitter>/<alias>` prediction —
    /// so recipe args (`{inputs.X.path}/...`) resolve at the producer's
    /// directory. That makes cross-user cache hits work: Bob's downstream
    /// reads from `<root>/alice/<alias>` (where the bytes live), instead
    /// of from a path under `<root>/bob/<alias>` that was never created.
    /// Co-requisite: artifact roots must be cross-user readable (setgid +
    /// shared group, or equivalent).
    ///
    /// Writes through both the cache and the on-disk `inputs.json`
    /// sidecar. Returns `true` if a row was actually patched. Idempotent.
    pub fn set_run_input_artifact(
        &self,
        run_id: &str,
        role: &str,
        artifact_id: &str,
    ) -> Result<bool> {
        let artifact = self.get_artifact(artifact_id)?;
        let path_str = artifact.path.display().to_string();
        let updated = self.cache.execute(
            "UPDATE run_inputs SET artifact_id=?, resolved_path=?
             WHERE run_id=? AND role=? AND artifact_id IS NULL",
            params![artifact_id, &path_str, run_id, role],
        )?;
        if updated == 0 {
            return Ok(false);
        }
        // Mirror into inputs.json. If the sidecar isn't there yet
        // (degenerate state mid-creation), the cache patch still stands;
        // the next refresh from disk would clobber it, but in practice
        // insert_run writes the sidecar before any caller can race here.
        let user = match self.user_for_run(run_id) {
            Ok(u) => u,
            Err(_) => return Ok(true),
        };
        let inputs_path =
            fs_layout::run_lab_dir(&self.runs_base, &user, run_id).join(fs_layout::INPUTS_JSON);
        let mut inputs: Vec<InputSidecar> = match fs_layout::read_json_optional(&inputs_path)? {
            Some(v) => v,
            None => return Ok(true),
        };
        let mut changed = false;
        for input in &mut inputs {
            if input.role == role && input.artifact_id.is_none() {
                input.artifact_id = Some(artifact_id.to_string());
                input.resolved_path = artifact.path.clone();
                changed = true;
            }
        }
        if changed {
            fs_layout::atomic_write_json(&inputs_path, &inputs)?;
        }
        Ok(true)
    }

    /// Structural pipeline-graph backfill: given a producer run that has
    /// just had `outputs` linked (role → artifact_id), find every other
    /// run in the same pipeline whose recipe declares
    /// `inputs.X = {type=stage, stage=<producer.stage_name>, role=<R>}`
    /// and set that downstream input row's artifact_id to the matching
    /// output's artifact_id.
    ///
    /// This is the "downstream-already-submitted" half of the chain-input
    /// wiring (the cache-hit / coalesced-follower path that doesn't go
    /// through `insert_artifact`). For the "downstream-not-yet-submitted"
    /// half, see the InputSpec::Stage branch of `runner::resolve_inputs`,
    /// which pre-fills artifact_id at submit time when the upstream is
    /// already satisfied.
    ///
    /// Only NULL run_inputs rows are touched; existing non-NULL wiring is
    /// never overwritten. Producers outside a pipeline (no pipeline_id /
    /// stage_name) are a no-op.
    pub fn backfill_stage_consumers(
        &self,
        producer_run_id: &str,
        outputs: &[(String, String)],
    ) -> Result<usize> {
        let producer = self.get_run(producer_run_id)?;
        let (pipeline_id, stage_name) =
            match (producer.pipeline_id.as_deref(), producer.stage_name.as_deref()) {
                (Some(p), Some(s)) => (p.to_string(), s.to_string()),
                _ => return Ok(0),
            };
        let outputs_by_role: BTreeMap<&str, &str> = outputs
            .iter()
            .map(|(r, a)| (r.as_str(), a.as_str()))
            .collect();
        let mut patched = 0;
        for sibling in self.list_pipeline_runs(&pipeline_id)? {
            if sibling.id == producer_run_id {
                continue;
            }
            let recipe: Recipe = match serde_json::from_value(sibling.recipe_json.clone()) {
                Ok(r) => r,
                Err(_) => continue, // tolerate corrupt rows; cache will be rebuilt
            };
            for (input_role, spec) in &recipe.inputs {
                let InputSpec::Stage { stage, role: parent_role } = spec else {
                    continue;
                };
                if stage != &stage_name {
                    continue;
                }
                let Some(artifact_id) = outputs_by_role.get(parent_role.as_str()) else {
                    continue;
                };
                if self.set_run_input_artifact(&sibling.id, input_role, artifact_id)? {
                    patched += 1;
                }
            }
        }
        Ok(patched)
    }

    fn aliases_for_run_outputs(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.cache.prepare(
            "SELECT aa.alias, aa.artifact_id FROM artifact_aliases aa
             JOIN run_outputs ro ON ro.artifact_id=aa.artifact_id
             WHERE ro.run_id=?
             ORDER BY aa.alias",
        )?;
        Ok(stmt
            .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn run_view(&self, run_id: &str) -> Result<RunView> {
        let run = self.get_run(run_id)?;
        let inputs = self.run_inputs(run_id)?;
        let outputs = self.run_outputs(run_id)?;
        let aliases = self.aliases_for_run_outputs(run_id)?;
        let eval_requests = self.eval_requests_for_run(run_id)?;
        Ok(RunView {
            run,
            inputs,
            outputs,
            aliases,
            eval_requests,
        })
    }

    // ---------- pipelines ----------

    pub fn insert_pipeline(
        &mut self,
        id: &str,
        name: &str,
        pipeline_path: Option<&Path>,
    ) -> Result<()> {
        // Pipelines are owned by whoever submits them — but the existing
        // call site doesn't know who that is at this layer. Use the path-
        // based scheme: the pipeline's owner is taken from $USER at the
        // time of insertion. (CLI-side submit paths set $USER themselves.)
        let user = current_user()?;
        let now = util::now_ts();
        let sidecar = PipelineSidecar {
            id: id.to_string(),
            name: name.to_string(),
            pipeline_path: pipeline_path.map(Path::to_path_buf),
            created_at: now,
            user: user.clone(),
        };
        fs_layout::atomic_write_json(
            &fs_layout::pipeline_path(&self.runs_base, &user, id),
            &sidecar,
        )?;
        self.cache.execute(
            "INSERT INTO pipelines (id, name, pipeline_path, user, created_at)
             VALUES (?, ?, ?, ?, ?)",
            params![
                id,
                name,
                pipeline_path.map(|p| p.display().to_string()),
                user,
                now,
            ],
        )?;
        Ok(())
    }

    pub fn set_pipeline_membership(
        &mut self,
        run_id: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        // Update the run sidecar.
        let user = self.user_for_run(run_id)?;
        let lab = fs_layout::run_lab_dir(&self.runs_base, &user, run_id);
        let mut sidecar: RunSidecar =
            fs_layout::read_json(&lab.join(fs_layout::RUN_JSON))?;
        sidecar.pipeline_id = Some(pipeline_id.to_string());
        sidecar.stage_name = Some(stage_name.to_string());
        sidecar.dependency_on = Some(dependency_on.clone());
        fs_layout::atomic_write_json(&lab.join(fs_layout::RUN_JSON), &sidecar)?;

        self.cache.execute(
            "UPDATE runs SET pipeline_id=?, stage_name=?, dependency_on=? WHERE id=?",
            params![
                pipeline_id,
                stage_name,
                serde_json::to_string(dependency_on)?,
                run_id,
            ],
        )?;
        Ok(())
    }

    pub fn list_pipeline_runs(&self, pipeline_id: &str) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT * FROM runs WHERE pipeline_id=? ORDER BY created_at ASC",
        )?;
        Ok(stmt
            .query_map(params![pipeline_id], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<PipelineRow>> {
        self.cache
            .query_row(
                "SELECT id, name, pipeline_path, created_at FROM pipelines WHERE id=?",
                params![pipeline_id],
                |row| {
                    Ok(PipelineRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        pipeline_path: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                        created_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_pipelines(&self) -> Result<Vec<PipelineRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT id, name, pipeline_path, created_at FROM pipelines ORDER BY created_at DESC",
        )?;
        Ok(stmt
            .query_map([], |row| {
                Ok(PipelineRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    pipeline_path: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
                    created_at: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- eval_requests ----------

    pub fn eval_request_status(
        &self,
        eval_key: &str,
        max_attempts: i64,
    ) -> Result<EvalRequestSlot> {
        let row: Option<(String, i64)> = self
            .cache
            .query_row(
                "SELECT COALESCE(r.status, ''), er.attempts
                 FROM eval_requests er
                 LEFT JOIN runs r ON r.id = er.eval_run_id
                 WHERE er.eval_key=?",
                params![eval_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(EvalRequestSlot::Fresh),
            Some((status, attempts)) => {
                let stale = matches!(status.as_str(), "cancelled" | "failed" | "timeout");
                if !stale {
                    Ok(EvalRequestSlot::Active)
                } else if attempts >= max_attempts {
                    Ok(EvalRequestSlot::Exhausted { attempts })
                } else {
                    Ok(EvalRequestSlot::Retry {
                        previous_attempts: attempts,
                    })
                }
            }
        }
    }

    pub fn insert_eval_request(
        &mut self,
        eval_key: &str,
        checkpoint_artifact_id: &str,
        eval_recipe_hash: &str,
        policy_id: &str,
        eval_run_id: &str,
    ) -> Result<()> {
        let user = current_user()?;
        let dir = fs_layout::eval_request_dir(&self.runs_base, &user, eval_key);
        // mkdir is the atomic claim. If it already exists, the caller
        // should have called eval_request_status first and routed to
        // retry_eval_request — bail loudly if it didn't.
        match fs_layout::claim_dir(&dir)? {
            ClaimOutcome::Claimed => {}
            ClaimOutcome::AlreadyExists => bail!(
                "insert_eval_request: eval_key {eval_key:?} already claimed; \
                 caller missed the Fresh slot"
            ),
        }
        let now = util::now_ts();
        let sidecar = EvalRequestSidecar {
            eval_key: eval_key.to_string(),
            checkpoint_artifact_id: checkpoint_artifact_id.to_string(),
            eval_recipe_hash: eval_recipe_hash.to_string(),
            policy_id: policy_id.to_string(),
            eval_run_id: Some(eval_run_id.to_string()),
            state: "submitted".to_string(),
            attempts: 1,
            created_at: now,
            updated_at: now,
        };
        fs_layout::atomic_write_json(
            &fs_layout::eval_request_path(&self.runs_base, &user, eval_key),
            &sidecar,
        )?;
        self.cache.execute(
            "INSERT INTO eval_requests
             (eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
              eval_run_id, state, attempts, user, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'submitted', 1, ?, ?, ?)",
            params![
                eval_key,
                checkpoint_artifact_id,
                eval_recipe_hash,
                policy_id,
                eval_run_id,
                user,
                now,
                now
            ],
        )?;
        Ok(())
    }

    pub fn retry_eval_request(
        &mut self,
        eval_key: &str,
        new_eval_run_id: &str,
        new_attempts: i64,
    ) -> Result<()> {
        // Find the user this eval_request was claimed under.
        let user: String = self
            .cache
            .query_row(
                "SELECT user FROM eval_requests WHERE eval_key=?",
                params![eval_key],
                |row| row.get(0),
            )
            .optional()?
            .with_context(|| {
                format!("retry_eval_request: no row for eval_key={eval_key} (caller missed the Retry slot)")
            })?;
        let path = fs_layout::eval_request_path(&self.runs_base, &user, eval_key);
        let mut sidecar: EvalRequestSidecar = fs_layout::read_json(&path)?;
        let now = util::now_ts();
        sidecar.eval_run_id = Some(new_eval_run_id.to_string());
        sidecar.state = "submitted".to_string();
        sidecar.attempts = new_attempts;
        sidecar.updated_at = now;
        fs_layout::atomic_write_json(&path, &sidecar)?;
        self.cache.execute(
            "UPDATE eval_requests
             SET eval_run_id=?, state='submitted', attempts=?, updated_at=?
             WHERE eval_key=?",
            params![new_eval_run_id, new_attempts, now, eval_key],
        )?;
        Ok(())
    }

    pub fn eval_requests_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.cache.prepare(
            "SELECT er.eval_key, er.checkpoint_artifact_id, er.eval_recipe_hash,
                    er.policy_id, er.eval_run_id, er.state
             FROM eval_requests er
             LEFT JOIN artifacts a ON a.id=er.checkpoint_artifact_id
             WHERE a.producer_run_id=? OR er.eval_run_id=?
             ORDER BY er.created_at",
        )?;
        Ok(stmt
            .query_map(params![run_id, run_id], |row| {
                Ok(json!({
                    "eval_key": row.get::<_, String>(0)?,
                    "checkpoint_artifact_id": row.get::<_, String>(1)?,
                    "eval_recipe_hash": row.get::<_, String>(2)?,
                    "policy_id": row.get::<_, String>(3)?,
                    "eval_run_id": row.get::<_, Option<String>>(4)?,
                    "state": row.get::<_, String>(5)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_eval_requests(&self) -> Result<Vec<Value>> {
        let mut stmt = self.cache.prepare(
            "SELECT eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
                    eval_run_id, state, created_at, updated_at
             FROM eval_requests ORDER BY updated_at DESC",
        )?;
        Ok(stmt
            .query_map([], |row| {
                Ok(json!({
                    "eval_key": row.get::<_, String>(0)?,
                    "checkpoint_artifact_id": row.get::<_, String>(1)?,
                    "eval_recipe_hash": row.get::<_, String>(2)?,
                    "policy_id": row.get::<_, String>(3)?,
                    "eval_run_id": row.get::<_, Option<String>>(4)?,
                    "state": row.get::<_, String>(5)?,
                    "created_at": row.get::<_, i64>(6)?,
                    "updated_at": row.get::<_, i64>(7)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Eval requests for one policy, newest first. Drives the policy
    /// detail page and the activity drawer inside it.
    pub fn eval_requests_by_policy(&self, policy_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.cache.prepare(
            "SELECT eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
                    eval_run_id, state, created_at, updated_at
             FROM eval_requests WHERE policy_id=? ORDER BY updated_at DESC",
        )?;
        Ok(stmt
            .query_map(params![policy_id], |row| {
                Ok(json!({
                    "eval_key": row.get::<_, String>(0)?,
                    "checkpoint_artifact_id": row.get::<_, String>(1)?,
                    "eval_recipe_hash": row.get::<_, String>(2)?,
                    "policy_id": row.get::<_, String>(3)?,
                    "eval_run_id": row.get::<_, Option<String>>(4)?,
                    "state": row.get::<_, String>(5)?,
                    "created_at": row.get::<_, i64>(6)?,
                    "updated_at": row.get::<_, i64>(7)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// One row per distinct policy_id with aggregate stats. The UI uses
    /// this to populate the policies-list view without forcing per-policy
    /// follow-up queries for counts.
    pub fn policy_summaries(&self) -> Result<Vec<PolicySummaryRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT policy_id,
                    COUNT(*) as total,
                    SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN state IN ('running','submitted') THEN 1 ELSE 0 END) as running,
                    MAX(updated_at) as last_fired
             FROM eval_requests GROUP BY policy_id ORDER BY last_fired DESC",
        )?;
        Ok(stmt
            .query_map([], |row| {
                Ok(PolicySummaryRow {
                    name: row.get::<_, String>(0)?,
                    total: row.get::<_, i64>(1)?,
                    failed: row.get::<_, i64>(2)?,
                    running: row.get::<_, i64>(3)?,
                    last_fired_at: row.get::<_, i64>(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- recipe history ----------

    pub fn recipe_history(&self, recipe_name: &str, limit: usize) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.cache.prepare(
            "SELECT status, created_at FROM runs WHERE recipe_name=?
             ORDER BY created_at DESC LIMIT ?",
        )?;
        let mut rows = stmt
            .query_map(params![recipe_name, limit as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.reverse();
        Ok(rows)
    }

    // ---------- events ----------

    pub fn events_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.cache.prepare(
            "SELECT event_type, payload_json, created_at FROM events
             WHERE run_id=? ORDER BY id ASC",
        )?;
        Ok(stmt
            .query_map(params![run_id], |row| {
                let payload: String = row.get(1)?;
                Ok(json!({
                    "event_type": row.get::<_, String>(0)?,
                    "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    "created_at": row.get::<_, i64>(2)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn append_event(&mut self, event: EventLine) -> Result<()> {
        // Append to the JSONL log on disk (canonical, durable across
        // restarts) and mirror into this process's events cache so SSE
        // tailers see the new row immediately. Single-process model:
        // every writer is also the only reader of its own cache, so no
        // duplicate-insert risk.
        fs_layout::append_event(&self.runs_base, &event)?;
        self.cache.execute(
            "INSERT INTO events (run_id, event_type, payload_json, created_at)
             VALUES (?, ?, ?, ?)",
            params![
                event.run_id,
                event.event_type,
                serde_json::to_string(&event.payload)?,
                event.created_at,
            ],
        )?;
        Ok(())
    }

    /// Highest event id currently in the cache; the SSE tailer's
    /// initial cursor lives here so we don't replay the entire backlog
    /// on every server boot.
    pub fn max_event_id(&self) -> Result<i64> {
        self.cache
            .query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(Into::into)
    }

    /// Events newer than `after_id`, in cache order. Drives the SSE
    /// push pipeline.
    pub fn events_after(&self, after_id: i64) -> Result<Vec<EventRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT id, run_id, event_type, payload_json, created_at
             FROM events WHERE id > ? ORDER BY id ASC",
        )?;
        Ok(stmt
            .query_map(params![after_id], |row| {
                let payload: String = row.get(3)?;
                Ok(EventRow {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    event_type: row.get(2)?,
                    payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
                    created_at: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- tracking ----------

    pub fn set_tracking(
        &mut self,
        run_id: &str,
        entity: &str,
        project: &str,
        url: &str,
        group: Option<&str>,
        source: &str,
    ) -> Result<()> {
        let user = self.user_for_run(run_id)?;
        let path = fs_layout::run_lab_dir(&self.runs_base, &user, run_id)
            .join(fs_layout::TRACKING_JSON);
        let now = util::now_ts();
        let sidecar = TrackingSidecar {
            entity: entity.to_string(),
            project: project.to_string(),
            url: url.to_string(),
            group_name: group.map(str::to_string),
            source: source.to_string(),
            created_at: now,
        };
        fs_layout::atomic_write_json(&path, &sidecar)?;
        self.cache.execute(
            "INSERT INTO tracking (run_id, entity, project, url, group_name, source, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(run_id) DO UPDATE SET
                entity=excluded.entity,
                project=excluded.project,
                url=excluded.url,
                group_name=excluded.group_name,
                source=excluded.source",
            params![run_id, entity, project, url, group, source, now],
        )?;
        Ok(())
    }

    pub fn get_tracking(&self, run_id: &str) -> Result<Option<TrackingRow>> {
        self.cache
            .query_row(
                "SELECT run_id, entity, project, url, group_name, source, created_at
                 FROM tracking WHERE run_id=?",
                params![run_id],
                |row| {
                    Ok(TrackingRow {
                        run_id: row.get(0)?,
                        entity: row.get(1)?,
                        project: row.get(2)?,
                        url: row.get(3)?,
                        group_name: row.get(4)?,
                        source: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn runs_missing_tracking(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.cache.prepare(
            "SELECT r.* FROM runs r
             LEFT JOIN tracking t ON t.run_id = r.id
             WHERE t.run_id IS NULL
             ORDER BY r.created_at DESC",
        )?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- helpers ----------

    /// Look up the path-canonical user for a given run id. The cache
    /// stores `submitted_by` directly; this is the path segment under
    /// `runs/`.
    fn user_for_run(&self, run_id: &str) -> Result<String> {
        self.cache
            .query_row(
                "SELECT submitted_by FROM runs WHERE id=?",
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .with_context(|| format!("run not found in cache: {run_id}"))
    }
}

// ---------- row → struct ----------

fn row_to_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRow> {
    let recipe_json: String = row.get("recipe_json")?;
    let context_json: String = row.get("context_json")?;
    let dependency_on: Option<String> = row.get("dependency_on").ok().flatten();
    let dependency_on = dependency_on
        .map(|s| serde_json::from_str(&s).map_err(to_sql_err))
        .transpose()?;
    Ok(RunRow {
        id: row.get("id")?,
        recipe_name: row.get("recipe_name")?,
        recipe_hash: row.get("recipe_hash")?,
        status: row.get("status")?,
        job_id: row.get("job_id")?,
        run_dir: PathBuf::from(row.get::<_, String>("run_dir")?),
        repo: row.get("repo")?,
        source_path: PathBuf::from(row.get::<_, String>("source_path")?),
        recipe_json: serde_json::from_str(&recipe_json).map_err(to_sql_err)?,
        context_json: serde_json::from_str(&context_json).map_err(to_sql_err)?,
        created_at: row.get("created_at")?,
        finished_at: row.get("finished_at")?,
        pipeline_id: row.get("pipeline_id").ok().flatten(),
        stage_name: row.get("stage_name").ok().flatten(),
        dependency_on,
        submitted_by: row.get("submitted_by").ok(),
        cache_key: row.get("cache_key").ok().flatten(),
        coalesced_peer_run_id: row.get("coalesced_peer_run_id").ok().flatten(),
    })
}

fn row_to_artifact(row: &rusqlite::Row<'_>) -> rusqlite::Result<ArtifactRow> {
    let metadata_json: String = row.get("metadata_json")?;
    Ok(ArtifactRow {
        id: row.get("id")?,
        kind: row.get("kind")?,
        path: PathBuf::from(row.get::<_, String>("path")?),
        content_hash: row.get("content_hash")?,
        producer_run_id: row.get("producer_run_id")?,
        metadata_json: serde_json::from_str(&metadata_json).map_err(to_sql_err)?,
        created_at: row.get("created_at")?,
    })
}

fn to_sql_err<E>(err: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

// ---------- the indexer ----------
//
// Pure functions — they operate on the cache via &Connection rather than
// taking &mut Store, which lets reindex() compose them without lifetime
// gymnastics.

/// Build a fresh in-memory cache by walking the filesystem-truth
/// registry. Used by `Store::open` (initial build), `Store::reindex` /
/// `refresh_from_disk` (in-place rebuild), and `Store::build_disk_snapshot`
/// (off-mutex rebuild for the periodic refresh swap). When
/// `read_events_from_jsonl` is true, the events table is populated by
/// walking `events/<YYYYMMDD>.jsonl`. When false, callers can ferry
/// the live events table forward by passing them in
/// `events_to_preserve` (preserves AUTOINCREMENT ids so the SSE
/// tailer's cursor stays valid across the swap).
fn build_in_memory_cache(
    runs_base: &Path,
    artifact_roots: &BTreeMap<String, PathBuf>,
    read_events_from_jsonl: bool,
    events_to_preserve: &[(i64, Option<String>, String, String, i64)],
) -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(CACHE_SCHEMA)?;

    // 1. Pipelines first — runs reference them.
    index_pipelines(runs_base, &conn)?;
    // 2. Artifacts — runs reference them. One walk per unique root
    //    path; sidecar kinds are validated against the set of kinds
    //    configured for that root (see store.rs's index_artifacts_under).
    let mut roots_to_kinds: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for (kind, root) in artifact_roots {
        roots_to_kinds
            .entry(root.clone())
            .or_default()
            .insert(kind.clone());
    }
    for (root, kinds) in &roots_to_kinds {
        index_artifacts_under(root, kinds, &conn)?;
    }
    // Walk the per-user alias overlay symlinks at
    // `<root>/aliases/<user>/<alias>` and populate the
    // artifact_user_aliases table. Pre-(D) artifacts (no symlinks) are
    // surfaced by `index_artifacts_under` using their `<user>/<alias>`
    // path; their per-user alias is implicit in the sidecar's
    // `(user, alias)` fields and we'd want a one-shot to materialize
    // those rows during M2 migration.
    for (root, kinds) in &roots_to_kinds {
        index_artifact_user_aliases_under(root, kinds, &conn)?;
    }
    index_aliases(runs_base, &conn)?;
    index_runs(runs_base, &conn)?;
    index_eval_requests(runs_base, &conn)?;
    index_tracking(runs_base, &conn)?;

    if read_events_from_jsonl {
        index_events(runs_base, &conn)?;
    } else {
        // Carry the live events forward with their original ids so
        // any SSE subscriber's last-seen cursor remains a valid
        // strictly-monotonic reference into the events stream.
        restore_events_into(&conn, events_to_preserve)?;
    }
    Ok(conn)
}

/// Re-insert events rows into a freshly-built cache, preserving their
/// ids so AUTOINCREMENT continues from the original max. The
/// sqlite_sequence row for the events table is updated implicitly by
/// the explicit-id inserts.
fn restore_events_into(
    conn: &Connection,
    events: &[(i64, Option<String>, String, String, i64)],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO events (id, run_id, event_type, payload_json, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )?;
        for (id, run_id, event_type, payload_json, created_at) in events {
            stmt.execute(params![id, run_id, event_type, payload_json, created_at])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn index_pipelines(runs_base: &Path, conn: &Connection) -> Result<()> {
    let root = fs_layout::pipelines_root(runs_base);
    let user_dirs = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for entry in user_dirs {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for pipeline in fs::read_dir(entry.path())? {
            let pipeline = pipeline?;
            if !pipeline.file_type()?.is_dir() {
                continue;
            }
            let path = pipeline.path().join(fs_layout::PIPELINE_JSON);
            let Some(sidecar) = fs_layout::read_json_optional::<PipelineSidecar>(&path)?
            else {
                continue;
            };
            conn.execute(
                "INSERT INTO pipelines (id, name, pipeline_path, user, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![
                    sidecar.id,
                    sidecar.name,
                    sidecar.pipeline_path.map(|p| p.display().to_string()),
                    sidecar.user,
                    sidecar.created_at,
                ],
            )?;
        }
    }
    Ok(())
}

fn index_artifacts_under(
    root: &Path,
    allowed_kinds: &BTreeSet<String>,
    conn: &Connection,
) -> Result<()> {
    // Content-addressed layout: artifact bytes + sidecar live at
    // `<root>/_objects/<prefix>/<hash>/.meta.json`. Per-user aliases
    // are symlinks at `<root>/aliases/<user>/<alias>` and indexed
    // separately by `index_artifact_user_aliases_under`. A single root
    // may host several artifact kinds (checkpoints' root hosts both
    // `checkpoint` and the `checkpoint_stream` resolution-kind output
    // tree); each sidecar's declared kind decides which it is.
    let objects_root = root.join(fs_layout::OBJECTS_DIR);
    walk_objects_dir(&objects_root, allowed_kinds, conn)
}

/// Walk `<root>/_objects/<prefix>/<hash>/.meta.json` and ingest each
/// content-addressed artifact. The prefix dir is a 2-char shard
/// (`ab/`, `cd/`, ...) bounding directory size; we don't validate the
/// prefix matches `hash[:2]` here — the sidecar is authoritative and
/// any mismatch would surface as an artifact at an odd path, which is
/// surprising but not broken.
fn walk_objects_dir(
    objects_root: &Path,
    allowed_kinds: &BTreeSet<String>,
    conn: &Connection,
) -> Result<()> {
    let prefix_dirs = match fs::read_dir(objects_root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", objects_root.display())),
    };
    for prefix_entry in prefix_dirs {
        let prefix_entry = prefix_entry?;
        if !prefix_entry.file_type()?.is_dir() {
            continue;
        }
        for hash_entry in fs::read_dir(prefix_entry.path())? {
            let hash_entry = hash_entry?;
            if !hash_entry.file_type()?.is_dir() {
                continue;
            }
            let hash_dir = hash_entry.path();
            if hash_dir.join(fs_layout::ARTIFACT_META).is_file() {
                ingest_artifact_meta(&hash_dir, allowed_kinds, conn)?;
            }
        }
    }
    Ok(())
}

/// Walk `<root>/aliases/<user>/<alias>` symlinks and populate the
/// `artifact_user_aliases` table by resolving each symlink back to a
/// `_objects/<prefix>/<hash>/` dir, reading its sidecar to recover the
/// artifact_id + kind, and writing the (user, alias, kind, artifact_id)
/// row. Tolerant of dangling symlinks (skip) and wrong-kind sidecars
/// (skip).
fn index_artifact_user_aliases_under(
    root: &Path,
    allowed_kinds: &BTreeSet<String>,
    conn: &Connection,
) -> Result<()> {
    let aliases_root = root.join(fs_layout::ALIASES_USER_DIR);
    let user_dirs = match fs::read_dir(&aliases_root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", aliases_root.display())),
    };
    for user_entry in user_dirs {
        let user_entry = user_entry?;
        if !user_entry.file_type()?.is_dir() {
            continue;
        }
        let user = match user_entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => continue,
        };
        for alias_entry in fs::read_dir(user_entry.path())? {
            let alias_entry = alias_entry?;
            // Per-user aliases are symlinks (created by add_user_alias).
            // Skip anything else.
            if !alias_entry.file_type()?.is_symlink() {
                continue;
            }
            let alias = match alias_entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            // Resolve the symlink to the canonical _objects dir, then
            // read its sidecar to recover artifact_id + kind.
            let target = match alias_entry.path().canonicalize() {
                Ok(p) => p,
                Err(_) => continue, // dangling symlink
            };
            let sidecar_path = target.join(fs_layout::ARTIFACT_META);
            let Some(sidecar) =
                fs_layout::read_json_optional::<ArtifactSidecar>(&sidecar_path)?
            else {
                continue;
            };
            if !allowed_kinds.contains(&sidecar.kind) {
                continue;
            }
            conn.execute(
                "INSERT OR IGNORE INTO artifact_user_aliases
                 (user, alias, kind, artifact_id, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![user, alias, sidecar.kind, sidecar.id, sidecar.created_at],
            )?;
        }
    }
    Ok(())
}

fn ingest_artifact_meta(
    dir: &Path,
    allowed_kinds: &BTreeSet<String>,
    conn: &Connection,
) -> Result<()> {
    let path = dir.join(fs_layout::ARTIFACT_META);
    let Some(sidecar) = fs_layout::read_json_optional::<ArtifactSidecar>(&path)? else {
        return Ok(());
    };
    if !allowed_kinds.contains(&sidecar.kind) {
        // Sidecar declares a kind that isn't configured for this root.
        // Surface it loudly and skip; silently coercing to a different
        // kind (the previous behavior) hides genuine misconfigurations
        // and was what masked the `checkpoint`/`checkpoint_stream` mix-up.
        eprintln!(
            "labctl indexer: artifact {} declares kind={:?} but no such kind is \
             configured for this root in [filesystem.artifact_roots] (allowed: {:?}); skipping",
            path.display(),
            sidecar.kind,
            allowed_kinds,
        );
        return Ok(());
    }
    conn.execute(
        "INSERT OR REPLACE INTO artifacts
         (id, kind, path, content_hash, producer_run_id, metadata_json,
          created_at, user, alias_segment)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            sidecar.id,
            sidecar.kind,
            dir.display().to_string(),
            sidecar.content_hash,
            sidecar.producer_run_id,
            serde_json::to_string(&sidecar.metadata)?,
            sidecar.created_at,
            sidecar.user,
            sidecar.alias,
        ],
    )?;
    Ok(())
}

fn index_aliases(runs_base: &Path, conn: &Connection) -> Result<()> {
    let root = fs_layout::aliases_root(runs_base);
    let entries = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let alias = match entry.file_name().into_string() {
            Ok(s) => s,
            Err(_) => continue,
        };
        let target_path = entry.path().join(fs_layout::ALIAS_TARGET);
        let Some(target) =
            fs_layout::read_json_optional::<AliasTargetSidecar>(&target_path)?
        else {
            continue;
        };
        conn.execute(
            "INSERT OR REPLACE INTO artifact_aliases (alias, artifact_id, created_at)
             VALUES (?, ?, ?)",
            params![alias, target.artifact_id, target.created_at],
        )?;
    }
    Ok(())
}

fn index_runs(runs_base: &Path, conn: &Connection) -> Result<()> {
    let root = fs_layout::runs_root(runs_base);
    let user_dirs = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for user_entry in user_dirs {
        let user_entry = user_entry?;
        if !user_entry.file_type()?.is_dir() {
            continue;
        }
        for run_entry in fs::read_dir(user_entry.path())? {
            let run_entry = run_entry?;
            if !run_entry.file_type()?.is_dir() {
                continue;
            }
            let lab = run_entry.path().join(fs_layout::LAB_DIRNAME);
            let run_json = lab.join(fs_layout::RUN_JSON);
            let Some(sidecar) = fs_layout::read_json_optional::<RunSidecar>(&run_json)?
            else {
                continue;
            };
            ingest_run_sidecar(&sidecar, &lab, conn)?;
        }
    }
    Ok(())
}

fn ingest_run_sidecar(
    sidecar: &RunSidecar,
    lab: &Path,
    conn: &Connection,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO runs
         (id, recipe_name, recipe_hash, status, job_id, run_dir, repo, source_path,
          recipe_json, context_json, created_at, finished_at,
          pipeline_id, dependency_on, stage_name, submitted_by,
          cache_key, coalesced_peer_run_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            sidecar.id,
            sidecar.recipe_name,
            sidecar.recipe_hash,
            sidecar.status,
            sidecar.job_id,
            sidecar.run_dir.display().to_string(),
            sidecar.repo,
            sidecar.source_path.display().to_string(),
            serde_json::to_string(&sidecar.recipe)?,
            serde_json::to_string(&sidecar.context)?,
            sidecar.created_at,
            sidecar.finished_at,
            sidecar.pipeline_id,
            sidecar.dependency_on
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
            sidecar.stage_name,
            sidecar.submitted_by,
            sidecar.cache_key,
            sidecar.coalesced_peer_run_id,
        ],
    )?;

    // inputs.json
    let inputs_path = lab.join(fs_layout::INPUTS_JSON);
    if let Some(inputs) = fs_layout::read_json_optional::<Vec<InputSidecar>>(&inputs_path)? {
        for input in inputs {
            conn.execute(
                "INSERT OR REPLACE INTO run_inputs (run_id, role, artifact_id, resolved_path)
                 VALUES (?, ?, ?, ?)",
                params![
                    sidecar.id,
                    input.role,
                    input.artifact_id,
                    input.resolved_path.display().to_string(),
                ],
            )?;
        }
    }

    // outputs.json
    let outputs_path = lab.join(fs_layout::OUTPUTS_JSON);
    if let Some(outputs) = fs_layout::read_json_optional::<Vec<OutputLink>>(&outputs_path)? {
        for link in outputs {
            conn.execute(
                "INSERT OR IGNORE INTO run_outputs (run_id, role, artifact_id) VALUES (?, ?, ?)",
                params![sidecar.id, link.role, link.artifact_id],
            )?;
        }
    }

    Ok(())
}

fn index_eval_requests(runs_base: &Path, conn: &Connection) -> Result<()> {
    let root = fs_layout::eval_state_root(runs_base);
    let user_dirs = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for user_entry in user_dirs {
        let user_entry = user_entry?;
        if !user_entry.file_type()?.is_dir() {
            continue;
        }
        let user = user_entry.file_name().to_string_lossy().into_owned();
        for key_entry in fs::read_dir(user_entry.path())? {
            let key_entry = key_entry?;
            if !key_entry.file_type()?.is_dir() {
                continue;
            }
            let req_path = key_entry.path().join(fs_layout::EVAL_REQUEST_JSON);
            let Some(req) =
                fs_layout::read_json_optional::<EvalRequestSidecar>(&req_path)?
            else {
                continue;
            };
            conn.execute(
                "INSERT OR REPLACE INTO eval_requests
                 (eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
                  eval_run_id, state, attempts, user, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    req.eval_key,
                    req.checkpoint_artifact_id,
                    req.eval_recipe_hash,
                    req.policy_id,
                    req.eval_run_id,
                    req.state,
                    req.attempts,
                    user,
                    req.created_at,
                    req.updated_at,
                ],
            )?;
        }
    }
    Ok(())
}

fn index_tracking(runs_base: &Path, conn: &Connection) -> Result<()> {
    let root = fs_layout::runs_root(runs_base);
    let user_dirs = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for user_entry in user_dirs {
        let user_entry = user_entry?;
        if !user_entry.file_type()?.is_dir() {
            continue;
        }
        for run_entry in fs::read_dir(user_entry.path())? {
            let run_entry = run_entry?;
            if !run_entry.file_type()?.is_dir() {
                continue;
            }
            let run_id = run_entry.file_name().to_string_lossy().into_owned();
            let path = run_entry
                .path()
                .join(fs_layout::LAB_DIRNAME)
                .join(fs_layout::TRACKING_JSON);
            let Some(t) = fs_layout::read_json_optional::<TrackingSidecar>(&path)? else {
                continue;
            };
            conn.execute(
                "INSERT OR REPLACE INTO tracking
                 (run_id, entity, project, url, group_name, source, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    run_id,
                    t.entity,
                    t.project,
                    t.url,
                    t.group_name,
                    t.source,
                    t.created_at,
                ],
            )?;
        }
    }
    Ok(())
}

fn index_events(runs_base: &Path, conn: &Connection) -> Result<()> {
    use std::io::{BufRead, BufReader};

    let root = fs_layout::events_root(runs_base);
    let mut entries: Vec<PathBuf> = match fs::read_dir(&root) {
        Ok(rd) => rd
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("jsonl"))
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    entries.sort(); // YYYYMMDD.jsonl sorts chronologically
    for path in entries {
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("labctl indexer: failed to open {}: {e}", path.display());
                continue;
            }
        };
        for line in BufReader::new(file).lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("labctl indexer: read error in {}: {e}", path.display());
                    break;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            let ev: EventLine = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!(
                        "labctl indexer: skipping malformed event line in {}: {e}",
                        path.display()
                    );
                    continue;
                }
            };
            conn.execute(
                "INSERT INTO events (run_id, event_type, payload_json, created_at)
                 VALUES (?, ?, ?, ?)",
                params![
                    ev.run_id,
                    ev.event_type,
                    serde_json::to_string(&ev.payload)?,
                    ev.created_at,
                ],
            )?;
        }
    }
    Ok(())
}

// ---------- helpers ----------

pub(crate) fn current_user() -> Result<String> {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .unwrap_or_else(|| "unknown".to_string());
    fs_layout::validate_user(&user)?;
    Ok(user)
}

/// `<artifact_root>/<user>/<alias>` decomposition. Used by
/// `insert_artifact` to compute the meta sidecar path from the legacy
/// `path` argument that callers still pass.
fn decompose_artifact_path(path: &Path, root: &Path) -> Result<(String, String)> {
    let rel = path.strip_prefix(root).with_context(|| {
        format!(
            "{} is not under artifact root {}",
            path.display(),
            root.display()
        )
    })?;
    let mut comps = rel.components();
    let user = comps
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .map(str::to_owned)
        .with_context(|| {
            format!(
                "artifact path {} has no <user> segment under {}",
                path.display(),
                root.display()
            )
        })?;
    let rest: PathBuf = comps.collect();
    if rest.as_os_str().is_empty() {
        bail!(
            "artifact path {} has no <alias> segment under {}/<user>",
            path.display(),
            root.display()
        );
    }
    Ok((user, rest.display().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FilesystemConfig;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn test_cluster(runs_base: &Path, artifact_roots: BTreeMap<String, PathBuf>) -> ClusterConfig {
        ClusterConfig {
            name: "test".into(),
            filesystem: FilesystemConfig {
                runs_base: runs_base.to_path_buf(),
                artifact_roots,
                output_roots: BTreeMap::new(),
                shared_group: None,
            },
            repos: BTreeMap::new(),
            env: BTreeMap::new(),
            modules: Vec::new(),
            scheduler: Default::default(),
            slurm: Default::default(),
            dispatch: None,
            remote: None,
        }
    }

    #[test]
    fn open_creates_subdirs_and_indexes_empty_tree() {
        let dir = tempdir().unwrap();
        let cluster = test_cluster(dir.path(), BTreeMap::new());
        let store = Store::open(&cluster).unwrap();
        assert_eq!(store.list_runs().unwrap().len(), 0);
        assert!(dir.path().join("runs").is_dir());
        assert!(dir.path().join("aliases").is_dir());
    }

    #[test]
    fn insert_run_writes_sidecar_and_indexes() {
        let dir = tempdir().unwrap();
        let cluster = test_cluster(dir.path(), BTreeMap::new());
        let mut store = Store::open(&cluster).unwrap();
        let recipe = Recipe {
            name: "demo".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let run_dir = dir.path().join("runs/alice/run_xyz");
        let source_path = run_dir.join("source/foo");
        store
            .insert_run(
                NewRun {
                    id: "run_xyz",
                    recipe: &recipe,
                    recipe_hash: "deadbeef",
                    status: "created",
                    run_dir: &run_dir,
                    source_path: &source_path,
                    context_json: &json!({"hello": 1}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();
        // Sidecar written.
        assert!(run_dir.join(".lab/run.json").is_file());
        // Cache populated.
        let row = store.get_run("run_xyz").unwrap();
        assert_eq!(row.recipe_name, "demo");
        assert_eq!(row.submitted_by.as_deref(), Some("alice"));
        // Reindex from disk recovers the same row.
        let store2 = Store::open(&cluster).unwrap();
        let row2 = store2.get_run("run_xyz").unwrap();
        assert_eq!(row2.recipe_hash, "deadbeef");
    }

    #[test]
    fn claim_coalesce_slot_first_writer_wins() {
        let dir = tempdir().unwrap();
        let cluster = test_cluster(dir.path(), BTreeMap::new());
        let store = Store::open(&cluster).unwrap();
        let key = "deadbeef".repeat(8); // resembles a real sha256 hex
        assert_eq!(
            store.claim_coalesce_slot(&key, "run_first").unwrap(),
            fs_layout::ClaimOutcome::Claimed
        );
        // Subsequent submitters get AlreadyExists with the .target.json
        // populated by the first writer.
        assert_eq!(
            store.claim_coalesce_slot(&key, "run_second").unwrap(),
            fs_layout::ClaimOutcome::AlreadyExists
        );
        let claim = store.read_coalesce_claim(&key).unwrap().unwrap();
        assert_eq!(claim.producer_run_id, "run_first");

        // Releasing the slot lets a new caller win.
        store.release_coalesce_slot(&key).unwrap();
        assert_eq!(
            store.claim_coalesce_slot(&key, "run_third").unwrap(),
            fs_layout::ClaimOutcome::Claimed
        );
        let claim = store.read_coalesce_claim(&key).unwrap().unwrap();
        assert_eq!(claim.producer_run_id, "run_third");
    }

    #[test]
    fn find_coalesce_peer_excludes_followers_and_jobless_rows() {
        let dir = tempdir().unwrap();
        let cluster = test_cluster(dir.path(), BTreeMap::new());
        let mut store = Store::open(&cluster).unwrap();
        let recipe = Recipe {
            name: "demo".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let cache_key = "feedfeed".repeat(8);

        // A jobless producer (insert_run only — no set_submitted). Won't
        // match because the follower has no job_id to depend on.
        let run_a_dir = dir.path().join("runs/alice/run_a");
        store
            .insert_run(
                NewRun {
                    id: "run_a",
                    recipe: &recipe,
                    recipe_hash: "h",
                    status: "created",
                    run_dir: &run_a_dir,
                    source_path: &run_a_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: Some(&cache_key),
                },
                &[],
            )
            .unwrap();
        assert!(store.find_coalesce_peer(&cache_key).unwrap().is_none());

        // Now A reaches set_submitted. find_coalesce_peer matches.
        store.set_submitted("run_a", "job_42").unwrap();
        let peer = store.find_coalesce_peer(&cache_key).unwrap().unwrap();
        assert_eq!(peer.id, "run_a");
        assert_eq!(peer.job_id.as_deref(), Some("job_42"));

        // A second follower (status awaiting_peer) must NOT be a coalesce
        // target — we never form chains of followers.
        let run_b_dir = dir.path().join("runs/alice/run_b");
        store
            .insert_run(
                NewRun {
                    id: "run_b",
                    recipe: &recipe,
                    recipe_hash: "h",
                    status: "created",
                    run_dir: &run_b_dir,
                    source_path: &run_b_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: Some(&cache_key),
                },
                &[],
            )
            .unwrap();
        store
            .set_awaiting_peer("run_b", "job_43", "run_a", &cache_key)
            .unwrap();
        // Still only A matches.
        let peer = store.find_coalesce_peer(&cache_key).unwrap().unwrap();
        assert_eq!(peer.id, "run_a");
    }

    #[test]
    fn copy_run_outputs_backfills_downstream_stage_inputs() {
        // Two-stage pipeline. The upstream stage's output is registered
        // as an artifact, but the upstream run never went through
        // insert_artifact for the new run (cache-hit path: copy_run_outputs
        // links a pre-existing artifact). The downstream stage was
        // submitted with a NULL run_inputs row pointing at upstream by
        // stage+role. copy_run_outputs should backfill the NULL row.
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        // Plant the upstream's output artifact.
        let artifact_dir = roots["dataset"].join("alice/upstream_out");
        fs::create_dir_all(&artifact_dir).unwrap();
        let artifact = store
            .insert_artifact(
                "dataset",
                &artifact_dir,
                &"a".repeat(64),
                None,
                &json!({}),
            )
            .unwrap();

        // Upstream run (the cache-hit destination — owns the link to the
        // pre-existing artifact via copy_run_outputs, not insert_artifact).
        let upstream_recipe = Recipe {
            name: "ingest".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let upstream_dir = dir.path().join("runs/alice/run_upstream");
        store
            .insert_run(
                NewRun {
                    id: "run_upstream",
                    recipe: &upstream_recipe,
                    recipe_hash: "u",
                    status: "created",
                    run_dir: &upstream_dir,
                    source_path: &upstream_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();

        // Downstream recipe declares a type=stage input pointing at the upstream's
        // "ds" output role.
        let mut downstream_inputs = BTreeMap::new();
        downstream_inputs.insert(
            "data".to_string(),
            InputSpec::Stage {
                stage: "ingest".into(),
                role: "ds".into(),
            },
        );
        let downstream_recipe = Recipe {
            name: "train".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: downstream_inputs,
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let downstream_dir = dir.path().join("runs/alice/run_downstream");
        store
            .insert_run(
                NewRun {
                    id: "run_downstream",
                    recipe: &downstream_recipe,
                    recipe_hash: "d",
                    status: "created",
                    run_dir: &downstream_dir,
                    source_path: &downstream_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[InputResolution {
                    role: "data".into(),
                    artifact_id: None,
                    resolved_path: artifact_dir.clone(),
                }],
            )
            .unwrap();

        // Join the two runs under one pipeline and tag the upstream's stage.
        store.insert_pipeline("pipe_1", "demo", None).unwrap();
        store
            .set_pipeline_membership("run_upstream", "pipe_1", "ingest", &json!({"afterok": []}))
            .unwrap();
        store
            .set_pipeline_membership(
                "run_downstream",
                "pipe_1",
                "train",
                &json!({"afterok": []}),
            )
            .unwrap();

        // Sanity: downstream's input is currently NULL.
        let inputs_before = store.run_inputs("run_downstream").unwrap();
        assert_eq!(inputs_before.len(), 1);
        assert!(inputs_before[0].artifact_id.is_none());

        // The cache-hit linkage: copy_run_outputs from a synthetic
        // prior-run row that owned the artifact, into run_upstream.
        // For this test we shortcut by directly linking the output and
        // then calling backfill_stage_consumers; copy_run_outputs is
        // exercised end-to-end in the second test below.
        store
            .link_run_output("run_upstream", "ds", &artifact.id)
            .unwrap();
        store
            .backfill_stage_consumers(
                "run_upstream",
                &[("ds".to_string(), artifact.id.clone())],
            )
            .unwrap();

        let inputs_after = store.run_inputs("run_downstream").unwrap();
        assert_eq!(
            inputs_after[0].artifact_id.as_deref(),
            Some(artifact.id.as_str()),
            "downstream stage input should have been backfilled"
        );

        // Idempotent: second call doesn't churn or err.
        store
            .backfill_stage_consumers(
                "run_upstream",
                &[("ds".to_string(), artifact.id.clone())],
            )
            .unwrap();
        let inputs_again = store.run_inputs("run_downstream").unwrap();
        assert_eq!(inputs_again[0].artifact_id.as_deref(), Some(artifact.id.as_str()));

        // Sidecar on disk reflects the patched id.
        let sidecar_path = dir
            .path()
            .join("runs/alice/run_downstream/.lab/inputs.json");
        let sidecar_bytes = std::fs::read(&sidecar_path).unwrap();
        let sidecar_text = String::from_utf8_lossy(&sidecar_bytes).to_string();
        assert!(
            sidecar_text.contains(artifact.id.as_str()),
            "inputs.json did not contain {}: {}",
            artifact.id,
            sidecar_text,
        );

        // Refresh from disk: the patched artifact_id survives a cache rebuild.
        let store2 = Store::open(&cluster).unwrap();
        let inputs_after_refresh = store2.run_inputs("run_downstream").unwrap();
        assert_eq!(
            inputs_after_refresh[0].artifact_id.as_deref(),
            Some(artifact.id.as_str())
        );
    }

    #[test]
    fn insert_artifact_moves_bytes_to_objects_and_creates_alias_symlink() {
        // M1 forward-compat: insert_artifact takes a staging dir under
        // <root>/<user>/<alias>/, atomically moves it to
        // <root>/_objects/<prefix>/<hash>/, and leaves the alias as a
        // relative symlink at <root>/aliases/<user>/<alias>.
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        let staging = roots["dataset"].join("alice/ds_v1");
        fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("payload.bin"), b"hello").unwrap();

        let hash = "abcd".repeat(16);
        let a = store
            .insert_artifact("dataset", &staging, &hash, None, &json!({}))
            .unwrap();

        // Bytes moved into _objects/.
        let canonical = roots["dataset"].join("_objects").join("ab").join(&hash);
        assert!(canonical.is_dir(), "{} should be a directory", canonical.display());
        assert!(canonical.join("payload.bin").is_file());
        assert!(!staging.exists(), "staging dir should have been moved away");
        assert_eq!(a.path, canonical);

        // Per-user alias overlay is a relative symlink.
        let link = roots["dataset"].join("aliases/alice/ds_v1");
        let target = std::fs::read_link(&link).unwrap();
        assert!(target.is_relative(), "alias symlink target should be relative: {target:?}");
        let resolved = std::fs::canonicalize(&link).unwrap();
        assert_eq!(resolved, canonical.canonicalize().unwrap());

        // Cache rebuild from disk re-discovers the artifact at the by-
        // hash path; the (D) layout is read by the existing walk.
        let store2 = Store::open(&cluster).unwrap();
        let recovered = store2.get_artifact(&a.id).unwrap();
        assert_eq!(recovered.path, canonical);
        assert_eq!(recovered.content_hash, hash);
    }

    #[test]
    fn insert_artifact_dedups_same_hash_and_records_second_user_alias() {
        // Two users register byte-identical content under different
        // aliases. The bytes get one canonical home; both alice and bob
        // get per-user alias symlinks pointing at it.
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        let hash = "feed".repeat(16);
        let alice_staging = roots["dataset"].join("alice/ds");
        fs::create_dir_all(&alice_staging).unwrap();
        std::fs::write(alice_staging.join("a.bin"), b"x").unwrap();
        let a1 = store
            .insert_artifact("dataset", &alice_staging, &hash, None, &json!({}))
            .unwrap();

        let bob_staging = roots["dataset"].join("bob/ds_mine");
        fs::create_dir_all(&bob_staging).unwrap();
        std::fs::write(bob_staging.join("a.bin"), b"x").unwrap();
        let a2 = store
            .insert_artifact("dataset", &bob_staging, &hash, None, &json!({}))
            .unwrap();

        // Same artifact returned.
        assert_eq!(a1.id, a2.id);
        // Bob's staging dir was discarded (bytes are deduplicated).
        assert!(!bob_staging.exists());
        // Both per-user symlinks exist and point at the canonical dir.
        let alice_link = roots["dataset"].join("aliases/alice/ds");
        let bob_link = roots["dataset"].join("aliases/bob/ds_mine");
        assert_eq!(
            std::fs::canonicalize(&alice_link).unwrap(),
            a1.path.canonicalize().unwrap()
        );
        assert_eq!(
            std::fs::canonicalize(&bob_link).unwrap(),
            a1.path.canonicalize().unwrap()
        );
    }

    #[test]
    fn cross_user_cache_hit_patches_resolved_path_to_producer_dir() {
        // The point of option (A): when an upstream stage cache-hits to
        // a different user's prior run, the downstream's resolved_path
        // must point at the producer's directory, not at a synthetic
        // <root>/<consumer>/<alias> path that was never written to disk.
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        // Artifact lives under alice.
        let artifact_dir = roots["dataset"].join("alice/shared_ds");
        fs::create_dir_all(&artifact_dir).unwrap();
        let artifact = store
            .insert_artifact("dataset", &artifact_dir, &"c".repeat(64), None, &json!({}))
            .unwrap();

        let empty_recipe = Recipe {
            name: "ingest".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };

        // Bob's "new" ingest run (cache-hit destination); the prior run
        // it caches against is alice's.
        let alice_prior_dir = dir.path().join("runs/alice/run_prior");
        store
            .insert_run(
                NewRun {
                    id: "run_alice_prior",
                    recipe: &empty_recipe,
                    recipe_hash: "u",
                    status: "succeeded",
                    run_dir: &alice_prior_dir,
                    source_path: &alice_prior_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();
        store
            .link_run_output("run_alice_prior", "ds", &artifact.id)
            .unwrap();

        let bob_new_dir = dir.path().join("runs/bob/run_new");
        store
            .insert_run(
                NewRun {
                    id: "run_bob_new",
                    recipe: &empty_recipe,
                    recipe_hash: "u",
                    status: "created",
                    run_dir: &bob_new_dir,
                    source_path: &bob_new_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("bob"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();

        // Bob's downstream stage, submitted with the consumer-predicted
        // path (under bob's dir — which does NOT exist on disk).
        let consumer_predicted_path = roots["dataset"].join("bob/shared_ds");
        let mut downstream_inputs = BTreeMap::new();
        downstream_inputs.insert(
            "data".into(),
            InputSpec::Stage {
                stage: "ingest".into(),
                role: "ds".into(),
            },
        );
        let downstream_recipe = Recipe {
            name: "train".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: downstream_inputs,
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let bob_downstream_dir = dir.path().join("runs/bob/run_downstream");
        store
            .insert_run(
                NewRun {
                    id: "run_bob_downstream",
                    recipe: &downstream_recipe,
                    recipe_hash: "d",
                    status: "created",
                    run_dir: &bob_downstream_dir,
                    source_path: &bob_downstream_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("bob"),
                    cache_key: None,
                },
                &[InputResolution {
                    role: "data".into(),
                    artifact_id: None,
                    resolved_path: consumer_predicted_path.clone(),
                }],
            )
            .unwrap();

        // Same pipeline; bob's new run is the "ingest" stage in his
        // pipeline (which cache-hit to alice's prior).
        store.insert_pipeline("pipe_xuser", "demo", None).unwrap();
        store
            .set_pipeline_membership(
                "run_bob_new",
                "pipe_xuser",
                "ingest",
                &json!({"afterok": []}),
            )
            .unwrap();
        store
            .set_pipeline_membership(
                "run_bob_downstream",
                "pipe_xuser",
                "train",
                &json!({"afterok": []}),
            )
            .unwrap();

        // The cache-hit move: copy alice's prior outputs into bob's new run.
        store
            .copy_run_outputs("run_alice_prior", "run_bob_new")
            .unwrap();

        // Bob's downstream input must point at the canonical content-
        // addressed dir (where the bytes actually live), not at the
        // never-written-to-disk consumer-predicted path. Under (D) this
        // is `_objects/<prefix>/<hash>/`, accessible to any user in the
        // shared group.
        let inputs = store.run_inputs("run_bob_downstream").unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].artifact_id.as_deref(), Some(artifact.id.as_str()));
        assert_eq!(inputs[0].resolved_path, artifact.path);
        assert!(
            inputs[0]
                .resolved_path
                .to_string_lossy()
                .contains("_objects/"),
            "resolved_path should point inside _objects/, got {:?}",
            inputs[0].resolved_path
        );
        assert_ne!(inputs[0].resolved_path, consumer_predicted_path);

        // Alice's per-user alias overlay is in place: a symlink from
        // <root>/aliases/alice/shared_ds → the by-hash dir.
        let alice_link = roots["dataset"].join("aliases/alice/shared_ds");
        let resolved = std::fs::canonicalize(&alice_link).unwrap();
        assert_eq!(resolved, artifact.path.canonicalize().unwrap());

        // Sidecar reflects the by-hash path.
        let sidecar_text = std::fs::read_to_string(
            dir.path().join("runs/bob/run_bob_downstream/.lab/inputs.json"),
        )
        .unwrap();
        assert!(
            sidecar_text.contains("_objects/"),
            "inputs.json should reference the by-hash dir, got: {sidecar_text}"
        );
    }

    #[test]
    fn copy_run_outputs_end_to_end_wires_chain() {
        // Same scenario as above, but driving the whole thing through
        // copy_run_outputs (no direct call to backfill_stage_consumers).
        // Mirrors the register_cache_hit / reconcile_follower code paths.
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        // Plant the artifact and a "prior" producer that links it.
        let artifact_dir = roots["dataset"].join("alice/up");
        fs::create_dir_all(&artifact_dir).unwrap();
        let artifact = store
            .insert_artifact("dataset", &artifact_dir, &"b".repeat(64), None, &json!({}))
            .unwrap();

        let empty_recipe = Recipe {
            name: "ingest".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        // Prior run that owns the artifact link.
        let prior_dir = dir.path().join("runs/alice/run_prior");
        store
            .insert_run(
                NewRun {
                    id: "run_prior",
                    recipe: &empty_recipe,
                    recipe_hash: "u",
                    status: "succeeded",
                    run_dir: &prior_dir,
                    source_path: &prior_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();
        store.link_run_output("run_prior", "ds", &artifact.id).unwrap();

        // New ingest run (the cache-hit destination).
        let new_dir = dir.path().join("runs/alice/run_new");
        store
            .insert_run(
                NewRun {
                    id: "run_new",
                    recipe: &empty_recipe,
                    recipe_hash: "u",
                    status: "created",
                    run_dir: &new_dir,
                    source_path: &new_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[],
            )
            .unwrap();

        // Downstream stage with type=stage input.
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "data".into(),
            InputSpec::Stage {
                stage: "ingest".into(),
                role: "ds".into(),
            },
        );
        let downstream_recipe = Recipe {
            name: "train".into(),
            repo: "foo".into(),
            command: vec!["true".into()],
            resources: Default::default(),
            inputs,
            outputs: BTreeMap::new(),
            params: BTreeMap::new(),
            args: BTreeMap::new(),
            env: BTreeMap::new(),
            tracking: Default::default(),
            sweep: None,
        };
        let downstream_dir = dir.path().join("runs/alice/run_downstream");
        store
            .insert_run(
                NewRun {
                    id: "run_downstream",
                    recipe: &downstream_recipe,
                    recipe_hash: "d",
                    status: "created",
                    run_dir: &downstream_dir,
                    source_path: &downstream_dir.join("source/foo"),
                    context_json: &json!({}),
                    submitted_by: Some("alice"),
                    cache_key: None,
                },
                &[InputResolution {
                    role: "data".into(),
                    artifact_id: None,
                    resolved_path: artifact_dir.clone(),
                }],
            )
            .unwrap();

        // Pipeline membership.
        store.insert_pipeline("pipe_2", "demo", None).unwrap();
        store
            .set_pipeline_membership("run_new", "pipe_2", "ingest", &json!({"afterok": []}))
            .unwrap();
        store
            .set_pipeline_membership(
                "run_downstream",
                "pipe_2",
                "train",
                &json!({"afterok": []}),
            )
            .unwrap();

        // The cache-hit move: link prior's outputs into new run.
        store.copy_run_outputs("run_prior", "run_new").unwrap();

        let inputs = store.run_inputs("run_downstream").unwrap();
        assert_eq!(inputs[0].artifact_id.as_deref(), Some(artifact.id.as_str()));
    }

    #[test]
    fn alias_set_replaces_target_atomically() {
        let dir = tempdir().unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("dataset".to_string(), dir.path().join("datasets"));
        let cluster = test_cluster(dir.path(), roots.clone());
        let mut store = Store::open(&cluster).unwrap();

        // Manually plant an artifact dir (insert_artifact requires a
        // valid path under the kind's root).
        let artifact_dir = roots["dataset"].join("alice/foo");
        fs::create_dir_all(&artifact_dir).unwrap();
        let a = store
            .insert_artifact(
                "dataset",
                &artifact_dir,
                "abc".repeat(10).as_str(),
                None,
                &json!({}),
            )
            .unwrap();
        store.set_alias("ds:latest", &a.id).unwrap();
        let resolved = store.resolve_artifact_ref("ds:latest").unwrap();
        assert_eq!(resolved.id, a.id);
    }
}
