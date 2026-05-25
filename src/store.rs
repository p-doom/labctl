#![allow(dead_code)]

//! Async-native facade over `PgStore` for the writer paths.
//!
//! Postgres is the source of truth. Every method is `async fn` and
//! `.await`s `PgStore` directly — there is no embedded runtime or
//! `block_in_place` bridge. Callers must run inside a tokio context;
//! the CLI entry point (`labctl ...`) is `#[tokio::main]`, the agent
//! uses a `runtime.block_on(...)` at its entry, and the HTTP server is
//! already async end-to-end.
//!
//! Sharing is via `Arc<Store>` — no Mutex; PG's own locking handles
//! concurrent writes. The HTTP server can also pull the inner
//! `Arc<PgStore>` via `Store::pg` for read-heavy paths that don't need
//! the FS-aware helpers.
//!
//! `insert_artifact` is the one method that still writes to the FS
//! alongside the PG insert (the per-artifact `.meta.json` projection
//! at the artifact's on-disk location). The write is small and is
//! offloaded via `spawn_blocking` so it doesn't stall the runtime on
//! slow NFS.
//!
//! Tests live in `pg_store::tests` (live PG smoke tests with `#[ignore]`).

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    config::{ClusterConfig, Recipe},
    fs_layout::{self, ArtifactSidecar},
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRow {
    pub id: String,
    pub kind: String,
    pub path: PathBuf,
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

/// One enriched row from the eval-series query: an `eval_requests` row
/// joined with its checkpoint artifact's metadata (`checkpoint_metadata`,
/// source of the `step` field) and with the first `eval_result`
/// artifact's metadata produced by the eval run (`eval_result_metadata`,
/// source of the headline metric). Both metadata fields are `Option`
/// because the checkpoint may have been GC'd or the eval run may not
/// have produced an `eval_result` artifact yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalSeriesRow {
    pub eval_key: String,
    pub checkpoint_artifact_id: String,
    pub eval_recipe_hash: String,
    pub policy_id: String,
    pub eval_run_id: Option<String>,
    pub state: String,
    pub checkpoint_metadata: Option<Value>,
    pub eval_result_metadata: Option<Value>,
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

#[derive(Copy, Clone)]
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

/// Outcome of `try_claim_or_follow`: the submitter either won the
/// per-`cache_key` slot and should proceed with the full submission,
/// or lost it to a concurrent in-flight run and was attached as a
/// follower whose row is now a placeholder waiting on the leader.
#[derive(Debug, Clone)]
pub enum ClaimOutcome {
    Won,
    Following { leader_run_id: String },
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

// ---------- the store ----------

pub struct Store {
    pg: Arc<PgStore>,
    runs_base: PathBuf,
    artifact_roots: BTreeMap<String, PathBuf>,
}

impl Store {
    /// Connect to PG. Must be called from inside a tokio runtime.
    pub async fn connect(cluster: &ClusterConfig) -> Result<Self> {
        let runs_base = cluster.filesystem.runs_base.clone();
        let artifact_roots = cluster.filesystem.artifact_roots.clone();
        let pg = PgStore::connect(cluster)
            .await
            .context("Store::connect: PgStore::connect failed")?;
        Ok(Self {
            pg: Arc::new(pg),
            runs_base,
            artifact_roots,
        })
    }

    /// Shared async PG handle for callers that want the raw client
    /// (notably `server.rs`, which doesn't need the FS-aware helpers).
    pub fn pg(&self) -> Arc<PgStore> {
        self.pg.clone()
    }

    // ---------- runs ----------

    pub async fn insert_run(&self, run: NewRun<'_>, inputs: &[InputResolution]) -> Result<()> {
        self.pg.insert_run(run, inputs).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_pending_pipeline_stage(
        &self,
        run_id: &str,
        recipe: &crate::config::Recipe,
        recipe_hash: &str,
        run_dir: &Path,
        source_path: &Path,
        submitted_by: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        self.pg
            .insert_pending_pipeline_stage(
                run_id,
                recipe,
                recipe_hash,
                run_dir,
                source_path,
                submitted_by,
                pipeline_id,
                stage_name,
                dependency_on,
            )
            .await
    }

    pub async fn pending_children_of(&self, parent_run_id: &str) -> Result<Vec<RunRow>> {
        self.pg.pending_children_of(parent_run_id).await
    }

    pub async fn set_submitted(&self, run_id: &str, job_id: &str) -> Result<()> {
        self.pg.set_submitted(run_id, job_id).await
    }

    pub async fn update_status(
        &self,
        run_id: &str,
        status: &str,
        finished_at: Option<i64>,
    ) -> Result<()> {
        self.pg.update_status(run_id, status, finished_at).await
    }

    pub async fn set_finished_at(&self, run_id: &str, finished_at: i64) -> Result<()> {
        self.pg.set_finished_at(run_id, finished_at).await
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRow> {
        self.pg
            .get_run(run_id)
            .await?
            .with_context(|| format!("run not found: {run_id}"))
    }

    /// `Option<RunRow>` flavour for callers that need to differentiate
    /// missing-row from PG-error (orphan-dir GC, polling existence
    /// checks, etc.). `get_run` keeps the panic-on-absence shape for
    /// the common "I expect this run to exist" call sites.
    pub async fn get_run_optional(&self, run_id: &str) -> Result<Option<RunRow>> {
        self.pg.get_run(run_id).await
    }

    pub async fn runs_by_recipe(&self, recipe_name: &str) -> Result<Vec<RunRow>> {
        self.pg.runs_by_recipe(recipe_name).await
    }

    pub async fn list_runs(&self) -> Result<Vec<RunRow>> {
        self.pg.list_runs().await
    }

    /// Active runs owned by `submitted_by`. Scoped so a daemon never
    /// reconciles another user's runs.
    pub async fn list_active_runs(&self, submitted_by: &str) -> Result<Vec<RunRow>> {
        self.pg.list_active_runs(submitted_by).await
    }

    /// Terminal runs owned by `submitted_by` that still have at least one
    /// pending child. Used by reconcile to retroactively advance children
    /// stranded by a prior agent restart between parent transition and
    /// child sweep.
    pub async fn list_terminal_runs_with_pending_children(
        &self,
        submitted_by: &str,
    ) -> Result<Vec<RunRow>> {
        self.pg
            .list_terminal_runs_with_pending_children(submitted_by)
            .await
    }

    // ---------- artifacts ----------

    /// Register an artifact at the staging path written by the producer.
    /// The producer writes to `<root>/<user>/<alias>/` and the artifact
    /// lives there permanently — no content-addressed relocation, no
    /// hash computation. Identity is path-canonical: `artifact_<sha256
    /// of the canonical path, first 16 hex>`. Re-registering the same
    /// path therefore yields the same id.
    pub async fn insert_artifact(
        &self,
        kind: &str,
        staging_path: &Path,
        producer_run_id: Option<&str>,
        metadata: &Value,
    ) -> Result<ArtifactRow> {
        let root = self
            .artifact_roots
            .get(kind)
            .with_context(|| format!("kind {kind:?} not in cluster.filesystem.artifact_roots"))?
            .clone();
        // Canonicalize off the runtime — symlink resolution stats the FS.
        let canonical = {
            let p = staging_path.to_path_buf();
            tokio::task::spawn_blocking(move || p.canonicalize().unwrap_or_else(|_| p.clone()))
                .await
                .context("canonicalize join")?
        };
        let path_hash = util::sha256_bytes(canonical.display().to_string().as_bytes());
        let id = format!("artifact_{}", &path_hash[..16]);
        // The staging path must be `<root>/<user>/<alias>/` — the
        // schema FK from artifacts."user" → users(name) requires a
        // real user, and a placeholder would FK-violate at INSERT.
        // Fail loudly at the boundary instead of writing junk.
        let (user, _alias) = decompose_artifact_path(staging_path, &root).with_context(|| {
            format!(
                "insert_artifact: staging path {} not under <root>/<user>/<alias>/ \
                     (root={}); the writer is producing artifacts outside the canonical \
                     per-user layout",
                staging_path.display(),
                root.display(),
            )
        })?;

        let now = util::now_ts();
        let alias = _alias;
        let sidecar = ArtifactSidecar {
            id: id.clone(),
            kind: kind.to_string(),
            user: user.clone(),
            alias,
            producer_run_id: producer_run_id.map(str::to_string),
            metadata: metadata.clone(),
            created_at: now,
        };
        // Small JSON write to NFS — offload so we don't pin a worker on
        // slow filesystems.
        {
            let target = staging_path.join(fs_layout::ARTIFACT_META);
            tokio::task::spawn_blocking(move || fs_layout::atomic_write_json(&target, &sidecar))
                .await
                .context("artifact sidecar write join")??;
        }

        self.pg
            .insert_artifact(
                &id,
                kind,
                staging_path,
                producer_run_id,
                metadata,
                &user,
                now,
            )
            .await?;
        self.pg
            .rehydrate_inputs_by_path(&staging_path.display().to_string(), &id)
            .await?;
        if let Some(run_id) = producer_run_id {
            let payload = serde_json::json!({
                "artifact_id": id,
                "kind": kind,
                "path": staging_path,
            });
            self.pg
                .append_event(Some(run_id), "artifact_registered", &payload, now)
                .await?;
        }
        self.get_artifact(&id).await
    }

    /// Look up an artifact by `(kind, path)`. The PG `find_artifact_by_path`
    /// query doesn't take a kind (the `(path)` column isn't unique on its
    /// own — multiple kinds can in principle share a path), so we filter
    /// the result by kind on the client side. Matches legacy semantics:
    /// returns the unique row at this path for this kind, or `None`.
    pub async fn find_artifact_by_path(
        &self,
        kind: &str,
        path: &Path,
    ) -> Result<Option<ArtifactRow>> {
        Ok(self
            .pg
            .find_artifact_by_path(&path.display().to_string())
            .await?
            .filter(|a| a.kind == kind))
    }

    pub async fn get_artifact(&self, id: &str) -> Result<ArtifactRow> {
        self.get_artifact_optional(id)
            .await?
            .with_context(|| format!("artifact not found: {id}"))
    }

    pub async fn get_artifact_optional(&self, id: &str) -> Result<Option<ArtifactRow>> {
        self.pg.get_artifact_optional(id).await
    }

    pub async fn artifacts_by_kind(&self, kind: &str) -> Result<Vec<ArtifactRow>> {
        self.pg.artifacts_by_kind(kind).await
    }

    /// Artifacts of a given kind whose producing run was submitted by
    /// `user`. Used by evald to scope each daemon's dispatch to its own
    /// user's checkpoints.
    pub async fn artifacts_by_kind_for_producer_user(
        &self,
        kind: &str,
        user: &str,
    ) -> Result<Vec<ArtifactRow>> {
        self.pg
            .artifacts_by_kind_for_producer_user(kind, user)
            .await
    }

    pub async fn list_artifacts(&self) -> Result<Vec<ArtifactRow>> {
        self.pg.list_artifacts().await
    }

    pub async fn artifact_consumers(&self, artifact_id: &str) -> Result<Vec<(String, String)>> {
        self.pg.artifact_consumers(artifact_id).await
    }

    pub async fn aliases_for_artifact(&self, artifact_id: &str) -> Result<Vec<String>> {
        self.pg.aliases_for_artifact(artifact_id).await
    }

    // ---------- aliases ----------

    pub async fn set_alias(&self, alias: &str, artifact_id: &str) -> Result<()> {
        self.pg.set_alias(alias, artifact_id).await
    }

    pub async fn resolve_artifact_ref(&self, reference: &str) -> Result<ArtifactRow> {
        self.pg.resolve_artifact_ref(reference).await
    }

    // ---------- run inputs/outputs ----------

    pub async fn link_run_output(&self, run_id: &str, role: &str, artifact_id: &str) -> Result<()> {
        self.pg.link_run_output(run_id, role, artifact_id).await
    }

    pub async fn run_inputs(&self, run_id: &str) -> Result<Vec<InputResolution>> {
        self.pg.run_inputs(run_id).await
    }

    pub async fn run_outputs(&self, run_id: &str) -> Result<Vec<ArtifactRow>> {
        self.pg.run_outputs(run_id).await
    }

    /// Up to `limit` cache-hit candidates for `cache_key`, newest first.
    /// The caller walks the list and accepts the first whose outputs are
    /// still on disk — older rows are a valid fallback when the most-
    /// recent's artifacts have been scrubbed.
    pub async fn find_cache_hit_candidates(
        &self,
        cache_key: &str,
        limit: i64,
    ) -> Result<Vec<RunRow>> {
        self.pg.find_cache_hit_candidates(cache_key, limit).await
    }

    /// Submit-time singleflight: insert `run` as the owner of its
    /// `cache_key`, or — when another non-terminal row already holds
    /// it — return `ClaimOutcome::Following { leader_run_id }`. See
    /// `PgStore::try_claim_or_follow` for the read-then-insert
    /// algorithm and the partial unique index that backs it.
    pub async fn try_claim_or_follow(
        &self,
        run: NewRun<'_>,
        inputs: &[InputResolution],
    ) -> Result<ClaimOutcome> {
        self.pg.try_claim_or_follow(run, inputs).await
    }

    /// Insert a follower placeholder pointing at `leader_run_id`. The
    /// existing pending-children cascade picks the follower up when the
    /// leader terminates, at which point `submit_recipe_inner` re-runs
    /// for the follower and resolves it as `cache_hit`.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_cache_follower(
        &self,
        run_id: &str,
        recipe: &Recipe,
        recipe_hash: &str,
        run_dir: &std::path::Path,
        source_path: &std::path::Path,
        submitted_by: &str,
        cache_key_of_leader: &str,
        leader_run_id: &str,
    ) -> Result<()> {
        self.pg
            .insert_cache_follower(
                run_id,
                recipe,
                recipe_hash,
                run_dir,
                source_path,
                submitted_by,
                cache_key_of_leader,
                leader_run_id,
            )
            .await
    }

    pub async fn append_stage_cache_hit_event(
        &self,
        run_id: &str,
        cache_key: &str,
        source_run_id: &str,
    ) -> Result<()> {
        self.pg
            .append_stage_cache_hit_event(run_id, cache_key, source_run_id)
            .await
    }

    pub async fn copy_run_outputs(&self, source_run_id: &str, dest_run_id: &str) -> Result<()> {
        self.pg.copy_run_outputs(source_run_id, dest_run_id).await
    }

    /// Atomically register a cache hit: locks the source run with
    /// `FOR SHARE` so a concurrent GC cannot delete it between
    /// observation and link, upserts the new run directly into
    /// terminal `cache_hit`, copies outputs, and emits the event —
    /// all in one transaction. The pipeline-graph backfill (sibling
    /// `type=stage` inputs that point at this new run's stage) is
    /// applied after the tx commits using the returned output links;
    /// it touches other rows, is idempotent, and doesn't need to be
    /// inside the atomicity envelope.
    pub async fn register_cache_hit_tx(
        &self,
        run: NewRun<'_>,
        inputs: &[InputResolution],
        source_run_id: &str,
        cache_key: &str,
        finished_at: i64,
    ) -> Result<()> {
        let dest_run_id = run.id.to_string();
        let output_links = self
            .pg
            .register_cache_hit_tx(run, inputs, source_run_id, cache_key, finished_at)
            .await?;
        if !output_links.is_empty() {
            self.pg
                .backfill_stage_consumers(&dest_run_id, &output_links)
                .await?;
        }
        Ok(())
    }

    pub async fn run_output_links(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        self.pg.run_output_links(run_id).await
    }

    pub async fn run_output_artifact_id(&self, run_id: &str, role: &str) -> Result<Option<String>> {
        self.pg.run_output_artifact_id(run_id, role).await
    }

    pub async fn set_run_input_artifact(
        &self,
        run_id: &str,
        role: &str,
        artifact_id: &str,
    ) -> Result<bool> {
        self.pg
            .set_run_input_artifact(run_id, role, artifact_id)
            .await
    }

    pub async fn backfill_stage_consumers(
        &self,
        producer_run_id: &str,
        outputs: &[(String, String)],
    ) -> Result<usize> {
        self.pg
            .backfill_stage_consumers(producer_run_id, outputs)
            .await
    }

    pub async fn run_view(&self, run_id: &str) -> Result<RunView> {
        self.pg.run_view(run_id).await
    }

    // ---------- users / admin ----------

    pub async fn insert_user(&self, name: &str, created_at: i64) -> Result<bool> {
        self.pg.insert_user(name, created_at).await
    }

    pub async fn ensure_pg_role(&self, name: &str) -> Result<bool> {
        self.pg.ensure_pg_role(name).await
    }

    // ---------- pipelines ----------

    pub async fn insert_pipeline(
        &self,
        id: &str,
        name: &str,
        pipeline_path: Option<&Path>,
    ) -> Result<()> {
        let user = current_user()?;
        self.pg
            .insert_pipeline(id, name, pipeline_path, &user)
            .await
    }

    pub async fn set_pipeline_membership(
        &self,
        run_id: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        self.pg
            .set_pipeline_membership(run_id, pipeline_id, stage_name, dependency_on)
            .await
    }

    pub async fn list_pipeline_runs(&self, pipeline_id: &str) -> Result<Vec<RunRow>> {
        self.pg.list_pipeline_runs(pipeline_id).await
    }

    pub async fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<PipelineRow>> {
        self.pg.get_pipeline(pipeline_id).await
    }

    pub async fn list_pipelines(&self) -> Result<Vec<PipelineRow>> {
        self.pg.list_pipelines().await
    }

    // ---------- eval_requests ----------

    pub async fn eval_request_status(
        &self,
        eval_key: &str,
        max_attempts: i64,
    ) -> Result<EvalRequestSlot> {
        self.pg.eval_request_status(eval_key, max_attempts).await
    }

    /// Atomically take the eval slot for `eval_key` (Fresh path). True
    /// iff we won the insert race. The user comes from `$USER`; PgStore
    /// itself doesn't pull env so the facade resolves it.
    pub async fn claim_eval_slot_fresh(
        &self,
        eval_key: &str,
        checkpoint_artifact_id: &str,
        eval_recipe_hash: &str,
        policy_id: &str,
        eval_run_id: &str,
    ) -> Result<bool> {
        let user = current_user()?;
        self.pg
            .claim_eval_slot_fresh(
                eval_key,
                checkpoint_artifact_id,
                eval_recipe_hash,
                policy_id,
                eval_run_id,
                &user,
            )
            .await
    }

    /// Atomically advance the eval slot to a new attempt (Retry path).
    /// `expected_run_id` and `expected_attempts` come from the snapshot
    /// the caller computed; the UPDATE only fires if those still match
    /// in PG. Returns true iff the row was updated.
    pub async fn claim_eval_slot_retry(
        &self,
        eval_key: &str,
        expected_run_id: &str,
        expected_attempts: i64,
        new_eval_run_id: &str,
    ) -> Result<bool> {
        self.pg
            .claim_eval_slot_retry(
                eval_key,
                expected_run_id,
                expected_attempts,
                new_eval_run_id,
            )
            .await
    }

    /// Read the current `eval_run_id` bound to `eval_key`, if any.
    /// Used by the retry path to capture the optimistic-concurrency
    /// witness from the same snapshot that produced the `Retry` slot
    /// decision.
    pub async fn eval_request_run_id(&self, eval_key: &str) -> Result<Option<String>> {
        self.pg.eval_request_run_id(eval_key).await
    }

    pub async fn eval_requests_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        self.pg.eval_requests_for_run(run_id).await
    }

    /// Enriched per-eval rows for the chart/series payload. Single
    /// query, no N+1.
    pub async fn eval_series_rows(&self, run_id: &str) -> Result<Vec<EvalSeriesRow>> {
        self.pg.eval_series_rows(run_id).await
    }

    pub async fn list_eval_requests(&self) -> Result<Vec<Value>> {
        self.pg.list_eval_requests().await
    }

    pub async fn eval_requests_by_policy(&self, policy_id: &str) -> Result<Vec<Value>> {
        self.pg.eval_requests_by_policy(policy_id).await
    }

    pub async fn policy_summaries(&self) -> Result<Vec<PolicySummaryRow>> {
        self.pg.policy_summaries().await
    }

    // ---------- recipe history ----------

    pub async fn recipe_history(
        &self,
        recipe_name: &str,
        limit: usize,
    ) -> Result<Vec<(String, i64)>> {
        self.pg.recipe_history(recipe_name, limit as i64).await
    }

    // ---------- events ----------

    pub async fn events_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        self.pg.events_for_run(run_id).await
    }

    pub async fn max_event_id(&self) -> Result<i64> {
        self.pg.max_event_id().await
    }

    /// Events newer than `after_id`. The PG variant takes an explicit
    /// `limit`; the legacy didn't. Pass a very large limit so all
    /// pending events flow through; SSE callers tail in batches anyway
    /// and the next iteration will pick up anything truncated here.
    pub async fn events_after(&self, after_id: i64) -> Result<Vec<EventRow>> {
        self.pg.events_after(after_id, i64::MAX).await
    }

    // ---------- tracking ----------

    pub async fn set_tracking(
        &self,
        run_id: &str,
        entity: &str,
        project: &str,
        url: &str,
        group: Option<&str>,
        source: &str,
    ) -> Result<()> {
        self.pg
            .set_tracking(run_id, entity, project, url, group, source)
            .await
    }

    pub async fn get_tracking(&self, run_id: &str) -> Result<Option<TrackingRow>> {
        self.pg.get_tracking(run_id).await
    }
}

// ---------- helpers ----------

pub(crate) fn current_user() -> Result<String> {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .context(
            "USER (or USERNAME) is unset; refuse to fall back to a placeholder because \
             every labctl write attributes ownership and PG enforces submitted_by, \
             pipelines.\"user\", artifacts.\"user\", eval_requests.\"user\" against users(name)",
        )?;
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
