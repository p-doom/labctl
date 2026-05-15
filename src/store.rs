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
    config::{ClusterConfig, Recipe},
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
    submitted_by TEXT NOT NULL
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
        "succeeded" | "failed" | "cancelled" | "timeout" | "oom" | "unknown_terminal"
    )
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
              recipe_json, context_json, created_at, submitted_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            "SELECT * FROM runs WHERE status IN ('created', 'submitted', 'running')
               AND submitted_by = ?
             ORDER BY created_at ASC",
        )?;
        Ok(stmt
            .query_map(params![submitted_by], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    // ---------- artifacts ----------

    pub fn insert_artifact(
        &mut self,
        kind: &str,
        path: &Path,
        content_hash: &str,
        producer_run_id: Option<&str>,
        metadata: &Value,
    ) -> Result<ArtifactRow> {
        if let Some(existing) = self.find_artifact_by_hash(kind, content_hash)? {
            self.backfill_chain_inputs(&existing.path, &existing.id)?;
            return Ok(existing);
        }
        let root = self.artifact_roots.get(kind).with_context(|| {
            format!("kind {kind:?} not in cluster.filesystem.artifact_roots")
        })?;
        let (user, alias_segment) = decompose_artifact_path(path, root).with_context(|| {
            format!(
                "artifact path {} is not under artifact_roots[{kind}]={} as <user>/<alias>",
                path.display(),
                root.display()
            )
        })?;
        let id = format!("artifact_{}", &content_hash[..16.min(content_hash.len())]);
        let now = util::now_ts();
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
        let meta_path = fs_layout::artifact_meta_path(root, &user, &alias_segment);
        fs_layout::atomic_write_json(&meta_path, &sidecar)?;

        self.cache.execute(
            "INSERT INTO artifacts
             (id, kind, path, content_hash, producer_run_id, metadata_json,
              created_at, user, alias_segment)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                id,
                kind,
                path.display().to_string(),
                content_hash,
                producer_run_id,
                serde_json::to_string(metadata)?,
                now,
                user,
                alias_segment,
            ],
        )?;
        self.backfill_chain_inputs(path, &id)?;
        if let Some(run_id) = producer_run_id {
            self.append_event(EventLine {
                run_id: Some(run_id.to_string()),
                event_type: "artifact_registered".to_string(),
                payload: json!({ "artifact_id": id, "kind": kind, "path": path }),
                created_at: now,
            })?;
        }
        self.get_artifact(&id)
    }

    /// Update any run_inputs rows whose `resolved_path` matches this
    /// artifact's path but whose `artifact_id` is NULL. This is the
    /// pipeline-stage chain edge: stage 2's input was pre-recorded
    /// referencing stage 1's not-yet-existent output. As soon as stage 1
    /// produces its artifact, those NULL rows get backfilled. We update
    /// both the cache and the corresponding `inputs.json` sidecars.
    fn backfill_chain_inputs(&self, artifact_path: &Path, artifact_id: &str) -> Result<()> {
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
    // Layout: <root>/<user>/<alias>/.meta.json for directly-produced
    // artifacts, and <root>/<user>/<alias>/<step>/.meta.json for
    // per-step artifacts in a checkpoint stream. We descend up to two
    // levels under <user>/. A single root may host several artifact
    // kinds (e.g. the checkpoints tree hosts both `checkpoint` per-step
    // artifacts and the resolution-kind `checkpoint_stream` recipe
    // output root); we walk the tree once and let each sidecar's
    // declared kind decide which it is.
    let user_dirs = match fs::read_dir(root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).context(format!("reading {}", root.display())),
    };
    for user_entry in user_dirs {
        let user_entry = user_entry?;
        if !user_entry.file_type()?.is_dir() {
            continue;
        }
        for alias_entry in fs::read_dir(user_entry.path())? {
            let alias_entry = alias_entry?;
            if !alias_entry.file_type()?.is_dir() {
                continue;
            }
            let alias_path = alias_entry.path();
            if alias_path.join(fs_layout::ARTIFACT_META).is_file() {
                ingest_artifact_meta(&alias_path, allowed_kinds, conn)?;
            }
            for step_entry in fs::read_dir(&alias_path)? {
                let step_entry = step_entry?;
                if !step_entry.file_type()?.is_dir() {
                    continue;
                }
                if step_entry.path().join(fs_layout::ARTIFACT_META).is_file() {
                    ingest_artifact_meta(&step_entry.path(), allowed_kinds, conn)?;
                }
            }
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
          pipeline_id, dependency_on, stage_name, submitted_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
