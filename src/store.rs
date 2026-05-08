use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{config::Recipe, util};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
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
    submitted_by TEXT
);

CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    pipeline_path TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    producer_run_id TEXT,
    metadata_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (producer_run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS artifact_aliases (
    alias TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id)
);

CREATE TABLE IF NOT EXISTS run_inputs (
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    artifact_id TEXT,
    resolved_path TEXT NOT NULL,
    PRIMARY KEY (run_id, role),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id)
);

CREATE TABLE IF NOT EXISTS run_outputs (
    run_id TEXT NOT NULL,
    role TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (run_id, role, artifact_id),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (artifact_id) REFERENCES artifacts(id)
);

CREATE TABLE IF NOT EXISTS eval_requests (
    eval_key TEXT PRIMARY KEY,
    checkpoint_artifact_id TEXT NOT NULL,
    eval_recipe_hash TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    eval_run_id TEXT,
    state TEXT NOT NULL,
    -- Retry counter: incremented every time the dispatcher re-fires this
    -- eval after the previous attempt's run terminally failed. Capped at
    -- ``evald::MAX_EVAL_ATTEMPTS`` to prevent infinite retry storms when
    -- the failure is deterministic (e.g. ``uv: command not found``).
    -- Migration for older registries lives in ``Store::open``.
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (checkpoint_artifact_id) REFERENCES artifacts(id),
    FOREIGN KEY (eval_run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

-- One row per (run, tracker). Today the only tracker is W&B so we keep the
-- table flat; if MLflow/TensorBoard/etc. join, add a `kind` column. Source
-- is "schema" (recipe declared it; written at submission time) or "log"
-- (legacy run, populated by `labctl backfill-tracking`).
CREATE TABLE IF NOT EXISTS tracking (
    run_id TEXT PRIMARY KEY,
    entity TEXT NOT NULL,
    project TEXT NOT NULL,
    url TEXT NOT NULL,
    group_name TEXT,
    source TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON artifacts(kind);
CREATE INDEX IF NOT EXISTS idx_artifacts_producer ON artifacts(producer_run_id);
CREATE INDEX IF NOT EXISTS idx_eval_requests_checkpoint ON eval_requests(checkpoint_artifact_id);
"#;

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

/// Result of inspecting an existing eval_request row's retry-eligibility.
/// ``Fresh``: no row — fire a new eval and call ``insert_eval_request``.
/// ``Active``: a row exists whose linked run is still pending or
/// succeeded — leave it alone, dispatcher should skip.
/// ``Retry``: linked run terminally failed and the retry budget has
/// not been exhausted — submit a fresh slurm job, then call
/// ``retry_eval_request`` with ``previous_attempts + 1``.
/// ``Exhausted``: linked run terminally failed and ``attempts`` has
/// reached ``max_attempts`` — skip silently (with an info log) until
/// the row is manually cleared. Prevents deterministic-failure storms.
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
    pub submitted_by: Option<&'a str>,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open registry {}", path.display()))?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;")?;
        conn.execute_batch(SCHEMA)?;
        // Idempotent column-additions for legacy registries. SQLite has
        // no ``ADD COLUMN IF NOT EXISTS``; we just attempt and tolerate
        // the duplicate-column error. Each migration is its own match so
        // future migrations can be added without disturbing earlier ones.
        match conn.execute(
            "ALTER TABLE eval_requests ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            [],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
                if msg.contains("duplicate column name") => {}
            Err(e) => return Err(e.into()),
        }
        match conn.execute("ALTER TABLE runs ADD COLUMN submitted_by TEXT", []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
                if msg.contains("duplicate column name") => {}
            Err(e) => return Err(e.into()),
        }
        Ok(Self { conn })
    }

    pub fn insert_run(&mut self, run: NewRun<'_>, inputs: &[InputResolution]) -> Result<()> {
        let tx = self.conn.transaction()?;
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
                util::now_ts(),
                run.submitted_by,
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
        tx.execute(
            "INSERT INTO events (run_id, event_type, payload_json, created_at)
             VALUES (?, 'run_created', ?, ?)",
            params![
                run.id,
                serde_json::to_string(run.context_json)?,
                util::now_ts()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_submitted(&mut self, run_id: &str, job_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE runs SET status='submitted', job_id=? WHERE id=?",
            params![job_id, run_id],
        )?;
        self.event(run_id, "run_submitted", json!({ "job_id": job_id }))?;
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
        self.conn.execute(
            "UPDATE runs SET status=?, finished_at=CASE WHEN ? THEN ? ELSE finished_at END WHERE id=?",
            params![status, terminal, ts, run_id],
        )?;
        self.event(run_id, "run_status", json!({ "status": status }))?;
        Ok(())
    }

    pub fn set_finished_at(&mut self, run_id: &str, finished_at: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE runs SET finished_at=? WHERE id=?",
            params![finished_at, run_id],
        )?;
        Ok(())
    }

    pub fn get_run(&self, run_id: &str) -> Result<RunRow> {
        self.conn
            .query_row("SELECT * FROM runs WHERE id=?", params![run_id], row_to_run)
            .optional()?
            .with_context(|| format!("run not found: {run_id}"))
    }

    pub fn runs_by_recipe(&self, recipe_name: &str) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM runs WHERE recipe_name=? ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map(params![recipe_name], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn list_runs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM runs ORDER BY created_at DESC")?;
        let rows = stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// All terminal runs — used by `labctl repair-finish-times` which
    /// recomputes ``finished_at`` from sacct/status.json. Idempotent: if
    /// the recomputed value matches the stored one, the row is left alone.
    pub fn terminal_runs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM runs WHERE status IN
             ('succeeded','failed','cancelled','timeout','oom','unknown_terminal')
             ORDER BY created_at DESC",
        )?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Terminal runs that have no `run_outputs` rows at all — i.e. runs
    /// that hit the pre-fix gate bug or were never given a chance to
    /// register their outputs. Drives `labctl recover-outputs`.
    pub fn terminal_runs_without_outputs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare(
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

    pub fn list_active_runs(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM runs WHERE status IN ('created', 'submitted', 'running')
             ORDER BY created_at ASC",
        )?;
        let rows = stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn insert_artifact(
        &mut self,
        kind: &str,
        path: &Path,
        content_hash: &str,
        producer_run_id: Option<&str>,
        metadata: &Value,
    ) -> Result<ArtifactRow> {
        if let Some(existing) = self.find_artifact_by_hash(kind, content_hash)? {
            // Even on dedupe, backfill any chain-edge inputs that pre-recorded
            // this path: those rows were inserted with artifact_id=NULL because
            // the upstream run hadn't produced its output yet.
            self.backfill_chain_inputs(&existing.path, &existing.id)?;
            return Ok(existing);
        }
        let id = format!("artifact_{}", &content_hash[..16.min(content_hash.len())]);
        self.conn.execute(
            "INSERT OR IGNORE INTO artifacts
             (id, kind, path, content_hash, producer_run_id, metadata_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                id,
                kind,
                path.display().to_string(),
                content_hash,
                producer_run_id,
                serde_json::to_string(metadata)?,
                util::now_ts(),
            ],
        )?;
        self.backfill_chain_inputs(path, &id)?;
        if let Some(run_id) = producer_run_id {
            self.event(
                run_id,
                "artifact_registered",
                json!({ "artifact_id": id, "kind": kind, "path": path }),
            )?;
        }
        self.get_artifact(&id)
    }

    fn backfill_chain_inputs(&self, artifact_path: &Path, artifact_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE run_inputs SET artifact_id = ?
             WHERE resolved_path = ? AND artifact_id IS NULL",
            params![artifact_id, artifact_path.display().to_string()],
        )?;
        Ok(())
    }

    pub fn insert_pipeline(
        &mut self,
        id: &str,
        name: &str,
        pipeline_path: Option<&Path>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pipelines (id, name, pipeline_path, created_at) VALUES (?, ?, ?, ?)",
            params![
                id,
                name,
                pipeline_path.map(|p| p.display().to_string()),
                util::now_ts(),
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
        self.conn.execute(
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
        let mut stmt = self.conn.prepare(
            "SELECT * FROM runs WHERE pipeline_id=? ORDER BY created_at ASC",
        )?;
        Ok(stmt
            .query_map(params![pipeline_id], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<PipelineRow>> {
        self.conn
            .query_row(
                "SELECT id, name, pipeline_path, created_at FROM pipelines WHERE id=?",
                params![pipeline_id],
                |row| {
                    Ok(PipelineRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        pipeline_path: row
                            .get::<_, Option<String>>(2)?
                            .map(PathBuf::from),
                        created_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn link_run_output(&mut self, run_id: &str, role: &str, artifact_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO run_outputs (run_id, role, artifact_id) VALUES (?, ?, ?)",
            params![run_id, role, artifact_id],
        )?;
        Ok(())
    }

    pub fn set_alias(&mut self, alias: &str, artifact_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO artifact_aliases (alias, artifact_id, created_at)
             VALUES (?, ?, ?)",
            params![alias, artifact_id, util::now_ts()],
        )?;
        Ok(())
    }

    pub fn resolve_artifact_ref(&self, reference: &str) -> Result<ArtifactRow> {
        if let Some(row) = self.get_artifact_optional(reference)? {
            return Ok(row);
        }
        let artifact_id: Option<String> = self
            .conn
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

    pub fn get_artifact(&self, id: &str) -> Result<ArtifactRow> {
        self.get_artifact_optional(id)?
            .with_context(|| format!("artifact not found: {id}"))
    }

    pub fn get_artifact_optional(&self, id: &str) -> Result<Option<ArtifactRow>> {
        self.conn
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
            .conn
            .prepare("SELECT * FROM artifacts WHERE kind=? ORDER BY created_at ASC")?;
        Ok(stmt
            .query_map(params![kind], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn find_artifact_by_hash(
        &self,
        kind: &str,
        content_hash: &str,
    ) -> Result<Option<ArtifactRow>> {
        self.conn
            .query_row(
                "SELECT * FROM artifacts WHERE kind=? AND content_hash=?",
                params![kind, content_hash],
                row_to_artifact,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Look up any existing artifact at exactly this path. Cheap O(1)
    /// SQLite lookup. Used as a pre-hash short-circuit for
    /// register_outputs: if we already registered this step directory,
    /// skip the multi-GB SHA-256 walk.
    pub fn find_artifact_by_path(
        &self,
        kind: &str,
        path: &Path,
    ) -> Result<Option<ArtifactRow>> {
        self.conn
            .query_row(
                "SELECT * FROM artifacts WHERE kind=? AND path=?",
                params![kind, path.display().to_string()],
                row_to_artifact,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn eval_request_exists(&self, eval_key: &str) -> Result<bool> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM eval_requests WHERE eval_key=?",
                params![eval_key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(exists.is_some())
    }

    /// Inspect the eval_request row for ``eval_key`` against the linked
    /// run's status, and decide whether the dispatcher should fire a new
    /// attempt. Replaces the older ``eval_request_active`` which auto-
    /// deleted stale rows on every iteration — that path produced
    /// unbounded retry storms when failures were deterministic (e.g.
    /// ``uv: command not found``). The new contract preserves the row
    /// across retries so an ``attempts`` counter can be carried forward
    /// and capped.
    pub fn eval_request_status(
        &self,
        eval_key: &str,
        max_attempts: i64,
    ) -> Result<EvalRequestSlot> {
        let row: Option<(String, i64)> = self
            .conn
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
        let now = util::now_ts();
        self.conn.execute(
            "INSERT INTO eval_requests
             (eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
              eval_run_id, state, attempts, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'submitted', 1, ?, ?)",
            params![
                eval_key,
                checkpoint_artifact_id,
                eval_recipe_hash,
                policy_id,
                eval_run_id,
                now,
                now
            ],
        )?;
        Ok(())
    }

    /// Re-fire an existing eval_request whose previous attempt terminally
    /// failed: bump ``attempts``, swap in the new run id, reset state to
    /// ``submitted``. ``created_at`` is preserved so the row's age
    /// reflects when the eval was first attempted, not the latest retry.
    pub fn retry_eval_request(
        &mut self,
        eval_key: &str,
        new_eval_run_id: &str,
        new_attempts: i64,
    ) -> Result<()> {
        let now = util::now_ts();
        let updated = self.conn.execute(
            "UPDATE eval_requests
             SET eval_run_id=?, state='submitted',
                 attempts=?, updated_at=?
             WHERE eval_key=?",
            params![new_eval_run_id, new_attempts, now, eval_key],
        )?;
        if updated == 0 {
            anyhow::bail!(
                "retry_eval_request: no row for eval_key={} (caller missed the Retry slot)",
                eval_key
            );
        }
        Ok(())
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

    pub fn run_inputs(&self, run_id: &str) -> Result<Vec<InputResolution>> {
        let mut stmt = self.conn.prepare(
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
        let mut stmt = self.conn.prepare(
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
        let mut stmt = self.conn.prepare(
            "SELECT aa.alias, aa.artifact_id FROM artifact_aliases aa
             JOIN run_outputs ro ON ro.artifact_id=aa.artifact_id
             WHERE ro.run_id=?
             ORDER BY aa.alias",
        )?;
        Ok(stmt
            .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn eval_requests_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(
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

    pub fn list_pipelines(&self) -> Result<Vec<PipelineRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, pipeline_path, created_at FROM pipelines
             ORDER BY created_at DESC",
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

    pub fn list_artifacts(&self) -> Result<Vec<ArtifactRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM artifacts ORDER BY created_at DESC")?;
        Ok(stmt
            .query_map([], row_to_artifact)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Runs that consume this artifact as an input. Returns (run_id, role).
    pub fn artifact_consumers(&self, artifact_id: &str) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, role FROM run_inputs WHERE artifact_id=? ORDER BY run_id",
        )?;
        Ok(stmt
            .query_map(params![artifact_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn aliases_for_artifact(&self, artifact_id: &str) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT alias FROM artifact_aliases WHERE artifact_id=? ORDER BY alias",
        )?;
        Ok(stmt
            .query_map(params![artifact_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Most recent N runs of the same recipe (by name), oldest→newest.
    /// Drives sparkline of pass/fail history per recipe in the runs list.
    pub fn recipe_history(&self, recipe_name: &str, limit: usize) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
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

    /// Highest event id currently in the table; used by the SSE tailer to
    /// pick its starting cursor so we don't replay old events on every
    /// `labctl serve` boot.
    pub fn max_event_id(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(Into::into)
    }

    /// Fetch events newer than `after_id`. Drives the SSE push pipeline.
    pub fn events_after(&self, after_id: i64) -> Result<Vec<EventRow>> {
        let mut stmt = self.conn.prepare(
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

    pub fn events_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(
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

    pub fn list_eval_requests(&self) -> Result<Vec<Value>> {
        let mut stmt = self.conn.prepare(
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

    /// Insert (or replace) the tracking row for a run. Idempotent — the
    /// backfill command relies on this to be safe to re-run, and submission
    /// uses it once per run.
    pub fn set_tracking(
        &mut self,
        run_id: &str,
        entity: &str,
        project: &str,
        url: &str,
        group: Option<&str>,
        source: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tracking (run_id, entity, project, url, group_name, source, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(run_id) DO UPDATE SET
                entity=excluded.entity,
                project=excluded.project,
                url=excluded.url,
                group_name=excluded.group_name,
                source=excluded.source",
            params![run_id, entity, project, url, group, source, util::now_ts()],
        )?;
        Ok(())
    }

    pub fn get_tracking(&self, run_id: &str) -> Result<Option<TrackingRow>> {
        self.conn
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

    /// Runs without a tracking row — input set for `labctl backfill-tracking`.
    pub fn runs_missing_tracking(&self) -> Result<Vec<RunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.* FROM runs r
             LEFT JOIN tracking t ON t.run_id = r.id
             WHERE t.run_id IS NULL
             ORDER BY r.created_at DESC",
        )?;
        Ok(stmt
            .query_map([], row_to_run)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn event(&mut self, run_id: &str, event_type: &str, payload: Value) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events (run_id, event_type, payload_json, created_at)
             VALUES (?, ?, ?, ?)",
            params![
                run_id,
                event_type,
                serde_json::to_string(&payload)?,
                util::now_ts()
            ],
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRow {
    pub id: String,
    pub name: String,
    pub pipeline_path: Option<PathBuf>,
    pub created_at: i64,
}

pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "failed" | "cancelled" | "timeout" | "oom" | "unknown_terminal"
    )
}

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
        submitted_by: row.get("submitted_by").ok().flatten(),
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
