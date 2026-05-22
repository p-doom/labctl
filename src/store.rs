// Many query methods on `Store` (and `EventRow` itself) are exclusively
// consumed by `server.rs`, which is gated behind the `ui` feature.
// Without that feature they look dead to the compiler but they're load-
// bearing for the UI build; tolerate the rust-analyzer noise rather
// than peppering each method with `#[cfg(feature = "ui")]`.
#![allow(dead_code)]

//! Sync facade over the async `PgStore`.
//!
//! The legacy `Store` was a filesystem-truth registry mirrored into an
//! in-memory SQLite cache, rebuilt from disk on every process start.
//! The Postgres-as-truth migration has replaced that model: PG is the
//! authoritative store, and call sites still use sync method calls on
//! `Store` (the harness is `Arc<Mutex<Store>>` everywhere — runner,
//! agent, server, evald, CLI). This wrapper bridges the two by holding
//! a `PgStore` plus a dedicated Tokio runtime, and dispatching each
//! sync method onto the async PG client via `block_on_pg`.
//!
//! Methods that previously did sidecar writes alongside DB writes are
//! now DB-only — the FS sidecars exist as the slurm-compute → login
//! bridge and as human-debuggable projections, not as a source of
//! truth. The two exceptions are `insert_artifact` and the private
//! `add_user_alias`, which still move bytes / create symlinks under the
//! per-kind artifact roots because nothing else does. Everything else
//! (run rows, run inputs/outputs, pipelines, eval requests, tracking,
//! events) is a pure PG operation.
//!
//! Tests live in `pg_store::tests` (live PG smoke tests with `#[ignore]`).

use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::runtime::Runtime;

use crate::{
    config::{ClusterConfig, Recipe},
    fs_layout::{self, ArtifactSidecar, ClaimOutcome},
    pg_store::PgStore,
    util,
};

// ---------- public types ----------
//
// Identical to the legacy types so call sites don't churn.

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
    /// Leave None to default to the current OS user.
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
    pg: PgStore,
    rt: Runtime,
    runs_base: PathBuf,
    artifact_roots: BTreeMap<String, PathBuf>,
}

impl Store {
    /// Open the registry against a cluster config. Connects to PG (via
    /// `PgStore::connect`) on a freshly-built multi-thread Tokio runtime
    /// owned by this Store; nothing else uses that runtime, but
    /// `block_in_place` requires multi-thread so the wrapper composes
    /// cleanly when invoked from inside an existing tokio context (e.g.
    /// the agent or HTTP server). Creates the top-level FS subdirs so
    /// any code path that still writes legacy sidecars (per-user alias
    /// symlinks, artifact `_objects/` tree) has a stable layout.
    pub fn open(cluster: &ClusterConfig) -> Result<Self> {
        let runs_base = cluster.filesystem.runs_base.clone();
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("failed to build tokio runtime for Store")?;
        let pg = rt
            .block_on(PgStore::connect(cluster))
            .context("Store::open: PgStore::connect failed")?;
        Ok(Self {
            pg,
            rt,
            runs_base,
            artifact_roots,
        })
    }

    /// Bridge sync → async. Works both inside an existing multi-thread
    /// tokio runtime (server.rs, agent.rs) and outside one (CLI sync
    /// entry). `block_in_place` requires the multi-thread flavor; the
    /// runtime built in `Store::open` is `new_multi_thread().enable_all()`
    /// so this works regardless of caller context.
    fn block_on_pg<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T>,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(h) => tokio::task::block_in_place(|| h.block_on(fut)),
            Err(_) => self.rt.block_on(fut),
        }
    }

    // ---------- runs ----------

    pub fn insert_run(&mut self, run: NewRun<'_>, inputs: &[InputResolution]) -> Result<()> {
        self.block_on_pg(self.pg.insert_run(run, inputs))
    }

    pub fn set_submitted(&mut self, run_id: &str, job_id: &str) -> Result<()> {
        self.block_on_pg(self.pg.set_submitted(run_id, job_id))
    }

    pub fn update_status(
        &mut self,
        run_id: &str,
        status: &str,
        finished_at: Option<i64>,
    ) -> Result<()> {
        self.block_on_pg(self.pg.update_status(run_id, status, finished_at))
    }

    pub fn set_finished_at(&mut self, run_id: &str, finished_at: i64) -> Result<()> {
        self.block_on_pg(self.pg.set_finished_at(run_id, finished_at))
    }

    pub fn get_run(&self, run_id: &str) -> Result<RunRow> {
        self.block_on_pg(self.pg.get_run(run_id))?
            .with_context(|| format!("run not found: {run_id}"))
    }

    pub fn runs_by_recipe(&self, recipe_name: &str) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.runs_by_recipe(recipe_name))
    }

    pub fn list_runs(&self) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.list_runs())
    }

    pub fn terminal_runs(&self) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.terminal_runs())
    }

    pub fn terminal_runs_without_outputs(&self) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.terminal_runs_without_outputs())
    }

    /// Active runs owned by `submitted_by`. Scoped so a daemon never
    /// reconciles another user's runs.
    pub fn list_active_runs(&self, submitted_by: &str) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.list_active_runs(submitted_by))
    }

    // ---------- artifacts ----------

    /// Register an artifact under content-addressed storage. Bridges the
    /// legacy `(kind, staging_path, content_hash, producer_run_id, metadata)`
    /// shape onto the DB-only `PgStore::insert_artifact` (which takes
    /// pre-decomposed args) by doing all the FS work in the wrapper:
    /// dedup-or-rename of the staging dir into `_objects/<prefix>/<hash>/`,
    /// sidecar write, and per-user alias symlink creation.
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
        let id = format!("artifact_{}", &content_hash[..16.min(content_hash.len())]);
        // The staging path is <root>/<user>/<alias>/. Falls back to
        // placeholders for ad-hoc registrations not under a user dir.
        let (user, alias_segment) = decompose_artifact_path(staging_path, &root)
            .unwrap_or_else(|_| ("unknown".into(), id.clone()));

        // Dedup: if some prior artifact has the same content_hash for
        // this kind, drop our staging copy, register a per-user alias
        // overlay pointing at the existing artifact, and rehydrate any
        // run_inputs waiting on the canonical path.
        if let Some(existing) = self.find_artifact_by_hash(kind, content_hash)? {
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
            self.add_user_alias(kind, &user, &alias_segment, &existing.id, &existing.path)?;
            self.block_on_pg(
                self.pg
                    .rehydrate_inputs_by_path(&existing.path.display().to_string(), &existing.id),
            )?;
            return Ok(existing);
        }

        // Fresh artifact: move bytes into the by-hash slot, write the
        // sidecar, insert the row, register the alias overlay, fan out
        // path-based rehydration, and emit an `artifact_registered`
        // event if a producer is known.
        let canonical = fs_layout::content_addressed_dir(&root, content_hash);
        if !canonical.exists() {
            if let Some(parent) = canonical.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
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
        fs_layout::atomic_write_json(&canonical.join(fs_layout::ARTIFACT_META), &sidecar)?;

        self.block_on_pg(self.pg.insert_artifact(
            &id,
            kind,
            &canonical,
            content_hash,
            producer_run_id,
            metadata,
            &user,
            &alias_segment,
            now,
        ))?;
        self.add_user_alias(kind, &user, &alias_segment, &id, &canonical)?;
        self.block_on_pg(
            self.pg
                .rehydrate_inputs_by_path(&canonical.display().to_string(), &id),
        )?;
        if let Some(run_id) = producer_run_id {
            let payload = serde_json::json!({
                "artifact_id": id,
                "kind": kind,
                "path": canonical,
            });
            self.block_on_pg(self.pg.append_event(
                Some(run_id),
                "artifact_registered",
                &payload,
                now,
            ))?;
        }
        self.get_artifact(&id)
    }

    /// Write a per-user alias overlay: a symlink at
    /// `<artifact_root>/aliases/<user>/<alias>` pointing at the
    /// artifact's canonical `_objects/<prefix>/<hash>/` dir, plus an
    /// `artifact_user_aliases` row. Idempotent.
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
        self.block_on_pg(self.pg.add_user_alias(
            user,
            alias,
            kind,
            artifact_id,
            util::now_ts(),
        ))?;
        Ok(())
    }

    pub fn find_artifact_by_hash(
        &self,
        kind: &str,
        content_hash: &str,
    ) -> Result<Option<ArtifactRow>> {
        self.block_on_pg(self.pg.find_artifact_by_hash(kind, content_hash))
    }

    /// Look up an artifact by `(kind, path)`. The PG `find_artifact_by_path`
    /// query doesn't take a kind (the `(path)` column isn't unique on its
    /// own — multiple kinds can in principle share a path), so we filter
    /// the result by kind on the client side. Matches legacy semantics:
    /// returns the unique row at this path for this kind, or `None`.
    pub fn find_artifact_by_path(
        &self,
        kind: &str,
        path: &Path,
    ) -> Result<Option<ArtifactRow>> {
        Ok(self
            .block_on_pg(self.pg.find_artifact_by_path(&path.display().to_string()))?
            .filter(|a| a.kind == kind))
    }

    pub fn get_artifact(&self, id: &str) -> Result<ArtifactRow> {
        self.get_artifact_optional(id)?
            .with_context(|| format!("artifact not found: {id}"))
    }

    pub fn get_artifact_optional(&self, id: &str) -> Result<Option<ArtifactRow>> {
        self.block_on_pg(self.pg.get_artifact_optional(id))
    }

    pub fn artifacts_by_kind(&self, kind: &str) -> Result<Vec<ArtifactRow>> {
        self.block_on_pg(self.pg.artifacts_by_kind(kind))
    }

    /// Artifacts of a given kind whose producing run was submitted by
    /// `user`. Used by evald to scope each daemon's dispatch to its own
    /// user's checkpoints.
    pub fn artifacts_by_kind_for_producer_user(
        &self,
        kind: &str,
        user: &str,
    ) -> Result<Vec<ArtifactRow>> {
        self.block_on_pg(self.pg.artifacts_by_kind_for_producer_user(kind, user))
    }

    pub fn list_artifacts(&self) -> Result<Vec<ArtifactRow>> {
        self.block_on_pg(self.pg.list_artifacts())
    }

    pub fn artifact_consumers(&self, artifact_id: &str) -> Result<Vec<(String, String)>> {
        self.block_on_pg(self.pg.artifact_consumers(artifact_id))
    }

    pub fn aliases_for_artifact(&self, artifact_id: &str) -> Result<Vec<String>> {
        self.block_on_pg(self.pg.aliases_for_artifact(artifact_id))
    }

    // ---------- aliases ----------

    pub fn set_alias(&mut self, alias: &str, artifact_id: &str) -> Result<()> {
        self.block_on_pg(self.pg.set_alias(alias, artifact_id))
    }

    pub fn resolve_artifact_ref(&self, reference: &str) -> Result<ArtifactRow> {
        self.block_on_pg(self.pg.resolve_artifact_ref(reference))
    }

    // ---------- run inputs/outputs ----------

    pub fn link_run_output(&mut self, run_id: &str, role: &str, artifact_id: &str) -> Result<()> {
        self.block_on_pg(self.pg.link_run_output(run_id, role, artifact_id))
    }

    pub fn run_inputs(&self, run_id: &str) -> Result<Vec<InputResolution>> {
        self.block_on_pg(self.pg.run_inputs(run_id))
    }

    pub fn run_outputs(&self, run_id: &str) -> Result<Vec<ArtifactRow>> {
        self.block_on_pg(self.pg.run_outputs(run_id))
    }

    /// Look up the most-recent succeeded or cache-hit run with this cache key.
    pub fn find_cache_hit_candidate(&self, cache_key: &str) -> Result<Option<RunRow>> {
        self.block_on_pg(self.pg.find_cache_hit_candidate(cache_key))
    }

    pub fn append_stage_cache_hit_event(
        &mut self,
        run_id: &str,
        cache_key: &str,
        source_run_id: &str,
    ) -> Result<()> {
        self.block_on_pg(
            self.pg
                .append_stage_cache_hit_event(run_id, cache_key, source_run_id),
        )
    }

    // ---------- in-flight coalescing ----------

    pub fn find_coalesce_peer(&self, cache_key: &str) -> Result<Option<RunRow>> {
        self.block_on_pg(self.pg.find_coalesce_peer(cache_key))
    }

    pub fn claim_coalesce_slot(
        &self,
        cache_key: &str,
        producer_run_id: &str,
    ) -> Result<ClaimOutcome> {
        self.block_on_pg(self.pg.claim_coalesce_slot(cache_key, producer_run_id))
    }

    pub fn read_coalesce_claim(
        &self,
        cache_key: &str,
    ) -> Result<Option<fs_layout::CoalesceClaimSidecar>> {
        self.block_on_pg(self.pg.read_coalesce_claim(cache_key))
    }

    pub fn release_coalesce_slot(&self, cache_key: &str) -> Result<()> {
        self.block_on_pg(self.pg.release_coalesce_slot(cache_key))
    }

    pub fn set_awaiting_peer(
        &mut self,
        run_id: &str,
        job_id: &str,
        peer_run_id: &str,
        cache_key: &str,
    ) -> Result<()> {
        self.block_on_pg(
            self.pg
                .set_awaiting_peer(run_id, job_id, peer_run_id, cache_key),
        )
    }

    pub fn append_stage_coalesce_resolved_event(
        &mut self,
        run_id: &str,
        peer_run_id: &str,
    ) -> Result<()> {
        self.block_on_pg(
            self.pg
                .append_stage_coalesce_resolved_event(run_id, peer_run_id),
        )
    }

    pub fn append_stage_coalesce_failed_event(
        &mut self,
        run_id: &str,
        peer_run_id: &str,
        peer_status: &str,
    ) -> Result<()> {
        self.block_on_pg(
            self.pg
                .append_stage_coalesce_failed_event(run_id, peer_run_id, peer_status),
        )
    }

    pub fn copy_run_outputs(&mut self, source_run_id: &str, dest_run_id: &str) -> Result<()> {
        self.block_on_pg(self.pg.copy_run_outputs(source_run_id, dest_run_id))
    }

    pub fn run_output_links(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        self.block_on_pg(self.pg.run_output_links(run_id))
    }

    pub fn run_output_artifact_id(
        &self,
        run_id: &str,
        role: &str,
    ) -> Result<Option<String>> {
        self.block_on_pg(self.pg.run_output_artifact_id(run_id, role))
    }

    pub fn set_run_input_artifact(
        &self,
        run_id: &str,
        role: &str,
        artifact_id: &str,
    ) -> Result<bool> {
        self.block_on_pg(self.pg.set_run_input_artifact(run_id, role, artifact_id))
    }

    pub fn backfill_stage_consumers(
        &self,
        producer_run_id: &str,
        outputs: &[(String, String)],
    ) -> Result<usize> {
        self.block_on_pg(self.pg.backfill_stage_consumers(producer_run_id, outputs))
    }

    pub fn run_view(&self, run_id: &str) -> Result<RunView> {
        self.block_on_pg(self.pg.run_view(run_id))
    }

    // ---------- pipelines ----------

    pub fn insert_pipeline(
        &mut self,
        id: &str,
        name: &str,
        pipeline_path: Option<&Path>,
    ) -> Result<()> {
        // Legacy derived `user` from $USER internally; PgStore expects it
        // as an explicit arg, so we resolve here before crossing the
        // async boundary.
        let user = current_user()?;
        self.block_on_pg(self.pg.insert_pipeline(id, name, pipeline_path, &user))
    }

    pub fn set_pipeline_membership(
        &mut self,
        run_id: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        self.block_on_pg(self.pg.set_pipeline_membership(
            run_id,
            pipeline_id,
            stage_name,
            dependency_on,
        ))
    }

    pub fn list_pipeline_runs(&self, pipeline_id: &str) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.list_pipeline_runs(pipeline_id))
    }

    pub fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<PipelineRow>> {
        self.block_on_pg(self.pg.get_pipeline(pipeline_id))
    }

    pub fn list_pipelines(&self) -> Result<Vec<PipelineRow>> {
        self.block_on_pg(self.pg.list_pipelines())
    }

    // ---------- eval_requests ----------

    pub fn eval_request_status(
        &self,
        eval_key: &str,
        max_attempts: i64,
    ) -> Result<EvalRequestSlot> {
        self.block_on_pg(self.pg.eval_request_status(eval_key, max_attempts))
    }

    pub fn insert_eval_request(
        &mut self,
        eval_key: &str,
        checkpoint_artifact_id: &str,
        eval_recipe_hash: &str,
        policy_id: &str,
        eval_run_id: &str,
    ) -> Result<()> {
        // PgStore takes an explicit `user`; legacy derived it from $USER.
        let user = current_user()?;
        self.block_on_pg(self.pg.insert_eval_request(
            eval_key,
            checkpoint_artifact_id,
            eval_recipe_hash,
            policy_id,
            eval_run_id,
            &user,
        ))
    }

    pub fn retry_eval_request(
        &mut self,
        eval_key: &str,
        new_eval_run_id: &str,
        new_attempts: i64,
    ) -> Result<()> {
        self.block_on_pg(self.pg.retry_eval_request(eval_key, new_eval_run_id, new_attempts))
    }

    pub fn eval_requests_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        self.block_on_pg(self.pg.eval_requests_for_run(run_id))
    }

    pub fn list_eval_requests(&self) -> Result<Vec<Value>> {
        self.block_on_pg(self.pg.list_eval_requests())
    }

    pub fn eval_requests_by_policy(&self, policy_id: &str) -> Result<Vec<Value>> {
        self.block_on_pg(self.pg.eval_requests_by_policy(policy_id))
    }

    pub fn policy_summaries(&self) -> Result<Vec<PolicySummaryRow>> {
        self.block_on_pg(self.pg.policy_summaries())
    }

    // ---------- recipe history ----------

    pub fn recipe_history(&self, recipe_name: &str, limit: usize) -> Result<Vec<(String, i64)>> {
        self.block_on_pg(self.pg.recipe_history(recipe_name, limit as i64))
    }

    // ---------- events ----------

    pub fn events_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        self.block_on_pg(self.pg.events_for_run(run_id))
    }

    pub fn max_event_id(&self) -> Result<i64> {
        self.block_on_pg(self.pg.max_event_id())
    }

    /// Events newer than `after_id`. The PG variant takes an explicit
    /// `limit`; the legacy didn't. Pass a very large limit so all
    /// pending events flow through; SSE callers tail in batches anyway
    /// and the next iteration will pick up anything truncated here.
    pub fn events_after(&self, after_id: i64) -> Result<Vec<EventRow>> {
        self.block_on_pg(self.pg.events_after(after_id, i64::MAX))
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
        self.block_on_pg(self.pg.set_tracking(run_id, entity, project, url, group, source))
    }

    pub fn get_tracking(&self, run_id: &str) -> Result<Option<TrackingRow>> {
        self.block_on_pg(self.pg.get_tracking(run_id))
    }

    pub fn runs_missing_tracking(&self) -> Result<Vec<RunRow>> {
        self.block_on_pg(self.pg.runs_missing_tracking())
    }
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
/// `insert_artifact` to derive the per-user alias overlay segment from
/// the legacy `staging_path` argument.
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
        anyhow::bail!(
            "artifact path {} has no <alias> segment under {}/<user>",
            path.display(),
            root.display()
        );
    }
    Ok((user, rest.display().to_string()))
}
