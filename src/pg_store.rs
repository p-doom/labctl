//! Postgres-backed registry store — the authoritative registry.
//!
//! Async-everywhere. The HTTP server (`server.rs`) calls these methods
//! directly via `.await`. Sync callers (`runner`, `evald`, CLI
//! subcommands) go through the `Store` sync facade in `store.rs`, which
//! holds its own tokio runtime.
//!
//! Row types come from `store.rs` (`RunRow`, `ArtifactRow`, `EventRow`,
//! `TrackingRow`, `InputResolution`, `PipelineRow`); JSON columns map to
//! `serde_json::Value` via sqlx's `Json` wrapper.
//!
//! Schema lives in `migrations/`; `PgStore::connect` runs `sqlx::migrate!`
//! before returning, so every process applies pending migrations at
//! startup. There is no skip-on-error path: a failing migration aborts
//! `connect()`.

// Many query methods on PgStore are exclusively consumed by `server.rs`
// (behind the `ui` feature). Without `ui` they look dead to the
// compiler but they're load-bearing for the UI build; tolerate the
// noise rather than peppering each method with `#[cfg(feature = "ui")]`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::config::{ClusterConfig, InputSpec, PgConfig, Recipe};
use crate::fs_layout::ClaimOutcome;
use crate::store::{
    ArtifactRow, EvalRequestSlot, EvalSeriesRow, EventRow, InputResolution, NewRun, PipelineRow,
    PolicySummaryRow, RunRow, RunView, TrackingRow, is_terminal,
};

/// Embedded migration set. Resolved at compile time from `migrations/`;
/// applied to every PG instance the first time `PgStore::connect` runs
/// against it. Tracked in the standard `_sqlx_migrations` table.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Open a connection pool against the cluster's configured PG
    /// instance and apply any pending schema migrations. `cluster.postgres`
    /// must be set — this is a hard requirement post-migration, no
    /// fallback. A migration failure aborts startup: we never run
    /// against a half-applied schema.
    pub async fn connect(cluster: &ClusterConfig) -> Result<Self> {
        let pg = cluster.postgres.as_ref().with_context(|| {
            format!(
                "cluster {:?} has no [postgres] section; the PG-as-truth \
                 registry is required — see docs/POSTGRES_DEPLOY.md",
                cluster.name,
            )
        })?;
        let opts = build_connect_options(pg)?;
        let pool = PgPoolOptions::new()
            .max_connections(pg.max_connections)
            .connect_with(opts)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to PG at {}:{} (db={})",
                    pg.host, pg.port, pg.database
                )
            })?;
        MIGRATOR
            .run(&pool)
            .await
            .context("sqlx::migrate! failed; refusing to run on partial schema")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ---------- read paths ----------

    pub async fn list_runs(&self) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(RUN_SELECT_ALL)
            .fetch_all(&self.pool)
            .await
            .context("list_runs query")?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn get_run(&self, id: &str) -> Result<Option<RunRow>> {
        let row = sqlx::query(&format!("{RUN_SELECT_BASE} WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get_run({id})"))?;
        row.map(row_to_run).transpose()
    }

    pub async fn get_artifact(&self, id: &str) -> Result<Option<ArtifactRow>> {
        let row = sqlx::query(&format!("{ARTIFACT_SELECT_BASE} WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get_artifact({id})"))?;
        row.map(row_to_artifact).transpose()
    }

    pub async fn list_artifacts(&self) -> Result<Vec<ArtifactRow>> {
        let rows = sqlx::query(&format!(
            "{ARTIFACT_SELECT_BASE} ORDER BY created_at DESC"
        ))
        .fetch_all(&self.pool)
        .await
        .context("list_artifacts query")?;
        rows.into_iter().map(row_to_artifact).collect()
    }

    pub async fn run_inputs(&self, run_id: &str) -> Result<Vec<InputResolution>> {
        let rows = sqlx::query(
            "SELECT role, artifact_id, resolved_path FROM run_inputs WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("run_inputs({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                Ok(InputResolution {
                    role: r.try_get("role")?,
                    artifact_id: r.try_get("artifact_id")?,
                    resolved_path: PathBuf::from(r.try_get::<String, _>("resolved_path")?),
                })
            })
            .collect()
    }

    pub async fn events_after(&self, cursor: i64, limit: i64) -> Result<Vec<EventRow>> {
        let rows = sqlx::query(
            "SELECT id, run_id, event_type, payload_json, created_at \
             FROM events WHERE id > $1 ORDER BY id ASC LIMIT $2",
        )
        .bind(cursor)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("events_after query")?;
        rows.into_iter()
            .map(|r| {
                let payload: sqlx::types::Json<Value> = r.try_get("payload_json")?;
                Ok(EventRow {
                    id: r.try_get("id")?,
                    run_id: r.try_get("run_id")?,
                    event_type: r.try_get("event_type")?,
                    payload: payload.0,
                    created_at: r.try_get("created_at")?,
                })
            })
            .collect()
    }

    pub async fn list_pipelines(&self) -> Result<Vec<PipelineRow>> {
        let rows = sqlx::query(
            "SELECT id, name, pipeline_path, created_at FROM pipelines ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("list_pipelines query")?;
        rows.into_iter()
            .map(|r| {
                Ok(PipelineRow {
                    id: r.try_get("id")?,
                    name: r.try_get("name")?,
                    pipeline_path: r
                        .try_get::<Option<String>, _>("pipeline_path")?
                        .map(PathBuf::from),
                    created_at: r.try_get("created_at")?,
                })
            })
            .collect()
    }

    pub async fn list_tracking(&self) -> Result<Vec<TrackingRow>> {
        let rows = sqlx::query(
            "SELECT run_id, entity, project, url, group_name, source, created_at FROM tracking",
        )
        .fetch_all(&self.pool)
        .await
        .context("list_tracking query")?;
        rows.into_iter()
            .map(|r| {
                Ok(TrackingRow {
                    run_id: r.try_get("run_id")?,
                    entity: r.try_get("entity")?,
                    project: r.try_get("project")?,
                    url: r.try_get("url")?,
                    group_name: r.try_get("group_name")?,
                    source: r.try_get("source")?,
                    created_at: r.try_get("created_at")?,
                })
            })
            .collect()
    }

    // ---------- write paths (smoke test minimum) ----------

    /// Append an event. Returns the new row id.
    pub async fn append_event(
        &self,
        run_id: Option<&str>,
        event_type: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO events (run_id, event_type, payload_json, created_at) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(run_id)
        .bind(event_type)
        .bind(sqlx::types::Json(payload))
        .bind(created_at)
        .fetch_one(&self.pool)
        .await
        .context("append_event insert")?;
        Ok(row.try_get("id")?)
    }

    // ---------- additional read paths ----------

    pub async fn runs_by_recipe(&self, recipe_name: &str) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} WHERE recipe_name = $1 ORDER BY created_at DESC"
        ))
        .bind(recipe_name)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("runs_by_recipe({recipe_name})"))?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn terminal_runs(&self) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} WHERE status IN \
             ('succeeded','failed','cancelled','timeout','oom','unknown_terminal') \
             ORDER BY created_at DESC"
        ))
        .fetch_all(&self.pool)
        .await
        .context("terminal_runs query")?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn terminal_runs_without_outputs(&self) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(
            "SELECT r.id, r.recipe_name, r.recipe_hash, r.status, r.job_id, r.run_dir,
                    r.repo, r.source_path, r.recipe_json, r.context_json, r.created_at,
                    r.finished_at, r.pipeline_id, r.stage_name, r.dependency_on,
                    r.submitted_by, r.cache_key, r.coalesced_peer_run_id
             FROM runs r
             LEFT JOIN run_outputs ro ON ro.run_id = r.id
             WHERE r.status IN \
               ('succeeded','failed','cancelled','timeout','oom','unknown_terminal')
               AND ro.run_id IS NULL
             ORDER BY r.created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("terminal_runs_without_outputs query")?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn list_active_runs(&self, submitted_by: &str) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} \
             WHERE status IN ('created','submitted','running','awaiting_peer') \
               AND submitted_by = $1 \
             ORDER BY created_at ASC"
        ))
        .bind(submitted_by)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("list_active_runs({submitted_by})"))?;
        rows.into_iter().map(row_to_run).collect()
    }

    /// Look up an artifact by its on-disk path. Returns the first match.
    /// Note: the PG schema doesn't constrain `(path)` to be unique on its
    /// own, so callers that need disambiguation must filter further on
    /// the client side (e.g. by `kind`).
    pub async fn find_artifact_by_path(&self, path: &str) -> Result<Option<ArtifactRow>> {
        let row = sqlx::query(&format!(
            "{ARTIFACT_SELECT_BASE} WHERE path = $1 LIMIT 1"
        ))
        .bind(path)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("find_artifact_by_path({path})"))?;
        row.map(row_to_artifact).transpose()
    }

    /// Same as `get_artifact` but spelled with the "optional" suffix to
    /// mirror the SQLite Store's API surface. Returns `Ok(None)` when no
    /// row matches the id.
    pub async fn get_artifact_optional(&self, id: &str) -> Result<Option<ArtifactRow>> {
        self.get_artifact(id).await
    }

    pub async fn artifact_consumers(
        &self,
        artifact_id: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT run_id, role FROM run_inputs WHERE artifact_id = $1 ORDER BY run_id",
        )
        .bind(artifact_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("artifact_consumers({artifact_id})"))?;
        rows.into_iter()
            .map(|r| Ok((r.try_get::<String, _>("run_id")?, r.try_get::<String, _>("role")?)))
            .collect()
    }

    pub async fn aliases_for_artifact(&self, artifact_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT alias FROM artifact_aliases WHERE artifact_id = $1 ORDER BY alias",
        )
        .bind(artifact_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("aliases_for_artifact({artifact_id})"))?;
        rows.into_iter()
            .map(|r| Ok(r.try_get::<String, _>("alias")?))
            .collect()
    }

    /// Resolve a reference (either an artifact id or a global alias) to
    /// its artifact row. Errors if neither lookup succeeds.
    pub async fn resolve_artifact_ref(&self, reference: &str) -> Result<ArtifactRow> {
        if let Some(row) = self.get_artifact(reference).await? {
            return Ok(row);
        }
        let row = sqlx::query(
            "SELECT artifact_id FROM artifact_aliases WHERE alias = $1",
        )
        .bind(reference)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("resolve_artifact_ref({reference}) alias lookup"))?;
        let Some(row) = row else {
            bail!("artifact or alias not found: {reference}");
        };
        let id: String = row.try_get("artifact_id")?;
        self.get_artifact(&id)
            .await?
            .with_context(|| format!("alias {reference:?} points at missing artifact {id}"))
    }

    pub async fn run_outputs(&self, run_id: &str) -> Result<Vec<ArtifactRow>> {
        let rows = sqlx::query(
            "SELECT a.id, a.kind, a.path, a.content_hash, a.producer_run_id,
                    a.metadata_json, a.created_at
             FROM artifacts a
             JOIN run_outputs ro ON ro.artifact_id = a.id
             WHERE ro.run_id = $1
             ORDER BY a.created_at, a.id",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("run_outputs({run_id})"))?;
        rows.into_iter().map(row_to_artifact).collect()
    }

    /// `(role, artifact_id)` tuples for every output linked to `run_id`.
    /// Sister of `run_outputs`, which returns the joined artifact rows.
    pub async fn run_output_links(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT role, artifact_id FROM run_outputs WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("run_output_links({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("role")?,
                    r.try_get::<String, _>("artifact_id")?,
                ))
            })
            .collect()
    }

    pub async fn find_cache_hit_candidate(
        &self,
        cache_key: &str,
    ) -> Result<Option<RunRow>> {
        let row = sqlx::query(&format!(
            "{RUN_SELECT_BASE} \
             WHERE cache_key = $1 AND status IN ('succeeded','cache_hit') \
             ORDER BY created_at DESC LIMIT 1"
        ))
        .bind(cache_key)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("find_cache_hit_candidate({cache_key})"))?;
        row.map(row_to_run).transpose()
    }

    pub async fn find_coalesce_peer(&self, cache_key: &str) -> Result<Option<RunRow>> {
        let row = sqlx::query(&format!(
            "{RUN_SELECT_BASE} \
             WHERE cache_key = $1 \
               AND status IN ('submitted','running') \
               AND job_id IS NOT NULL \
             ORDER BY created_at ASC LIMIT 1"
        ))
        .bind(cache_key)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("find_coalesce_peer({cache_key})"))?;
        row.map(row_to_run).transpose()
    }

    /// Read the producer run id currently holding the coalesce slot for
    /// `cache_key`, plus the timestamp it was claimed. Returns `None` if
    /// no slot is held. Used by a follower right after `claim_coalesce_slot`
    /// returns `AlreadyExists` to learn who to wait on.
    pub async fn read_coalesce_claim(
        &self,
        cache_key: &str,
    ) -> Result<Option<crate::fs_layout::CoalesceClaimSidecar>> {
        let row = sqlx::query(
            "SELECT producer_run_id, claimed_at FROM coalesce_claims \
             WHERE cache_key = $1"
        )
        .bind(cache_key)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("read_coalesce_claim({cache_key})"))?;
        match row {
            Some(r) => Ok(Some(crate::fs_layout::CoalesceClaimSidecar {
                producer_run_id: r.try_get("producer_run_id")?,
                claimed_at: r.try_get("claimed_at")?,
            })),
            None => Ok(None),
        }
    }


    pub async fn run_output_artifact_id(
        &self,
        run_id: &str,
        role: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT artifact_id FROM run_outputs WHERE run_id = $1 AND role = $2",
        )
        .bind(run_id)
        .bind(role)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("run_output_artifact_id({run_id}, {role})"))?;
        match row {
            Some(r) => Ok(Some(r.try_get::<String, _>("artifact_id")?)),
            None => Ok(None),
        }
    }

    async fn aliases_for_run_outputs(
        &self,
        run_id: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT aa.alias, aa.artifact_id FROM artifact_aliases aa
             JOIN run_outputs ro ON ro.artifact_id = aa.artifact_id
             WHERE ro.run_id = $1
             ORDER BY aa.alias",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("aliases_for_run_outputs({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("alias")?,
                    r.try_get::<String, _>("artifact_id")?,
                ))
            })
            .collect()
    }

    pub async fn run_view(&self, run_id: &str) -> Result<RunView> {
        let run = self
            .get_run(run_id)
            .await?
            .with_context(|| format!("run not found: {run_id}"))?;
        let inputs = self.run_inputs(run_id).await?;
        let outputs = self.run_outputs(run_id).await?;
        let aliases = self.aliases_for_run_outputs(run_id).await?;
        let eval_requests = self.eval_requests_for_run(run_id).await?;
        Ok(RunView {
            run,
            inputs,
            outputs,
            aliases,
            eval_requests,
        })
    }

    pub async fn list_pipeline_runs(&self, pipeline_id: &str) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} WHERE pipeline_id = $1 ORDER BY created_at ASC"
        ))
        .bind(pipeline_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("list_pipeline_runs({pipeline_id})"))?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn get_pipeline(&self, pipeline_id: &str) -> Result<Option<PipelineRow>> {
        let row = sqlx::query(
            "SELECT id, name, pipeline_path, created_at FROM pipelines WHERE id = $1",
        )
        .bind(pipeline_id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("get_pipeline({pipeline_id})"))?;
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(PipelineRow {
                id: r.try_get("id")?,
                name: r.try_get("name")?,
                pipeline_path: r
                    .try_get::<Option<String>, _>("pipeline_path")?
                    .map(PathBuf::from),
                created_at: r.try_get("created_at")?,
            })),
        }
    }

    pub async fn eval_request_status(
        &self,
        eval_key: &str,
        max_attempts: i64,
    ) -> Result<EvalRequestSlot> {
        let row = sqlx::query(
            "SELECT COALESCE(r.status, '') AS status, er.attempts
             FROM eval_requests er
             LEFT JOIN runs r ON r.id = er.eval_run_id
             WHERE er.eval_key = $1",
        )
        .bind(eval_key)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("eval_request_status({eval_key})"))?;
        match row {
            None => Ok(EvalRequestSlot::Fresh),
            Some(r) => {
                let status: String = r.try_get("status")?;
                let attempts: i64 = r.try_get("attempts")?;
                // Retry-eligible iff the prior run ended in a non-success
                // terminal state. Must cover EVERY terminal failure
                // sacct can surface — previously omitted `oom` and
                // `unknown_terminal`, which would lock the slot in
                // Active forever and silently block retries.
                let stale = matches!(
                    status.as_str(),
                    "cancelled" | "failed" | "timeout" | "oom" | "unknown_terminal"
                );
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

    /// Enriched per-eval rows for a single parent run: each eval_request
    /// joined with its checkpoint artifact's metadata (for `step`), the
    /// first `eval_result` artifact's metadata (for the headline
    /// metric), and the eval-run's current `runs.status` (surfaced as
    /// `state`). One PG round-trip — the previous shape required N+1
    /// by looping `get_artifact_optional` + `run_outputs` per eval.
    ///
    /// `state` derives from the joined `runs.status` (the slurm-tracked
    /// lifecycle) rather than the static `eval_requests.state` column
    /// (which only ever holds 'submitted'). Falls back to 'pending'
    /// when the eval_run row is absent (eval_run_id NULL or stale
    /// reference).
    pub async fn eval_series_rows(&self, run_id: &str) -> Result<Vec<EvalSeriesRow>> {
        let rows = sqlx::query(
            "SELECT
                 er.eval_key,
                 er.checkpoint_artifact_id,
                 er.eval_recipe_hash,
                 er.policy_id,
                 er.eval_run_id,
                 COALESCE(er_run.status, 'pending') AS state,
                 cp.metadata_json AS checkpoint_metadata,
                 (
                     SELECT a.metadata_json
                     FROM run_outputs ro
                     JOIN artifacts a ON a.id = ro.artifact_id
                     WHERE ro.run_id = er.eval_run_id AND a.kind = 'eval_result'
                     ORDER BY a.created_at, a.id
                     LIMIT 1
                 ) AS eval_result_metadata
             FROM eval_requests er
             LEFT JOIN artifacts cp ON cp.id = er.checkpoint_artifact_id
             LEFT JOIN runs er_run ON er_run.id = er.eval_run_id
             WHERE cp.producer_run_id = $1 OR er.eval_run_id = $1
             ORDER BY er.created_at",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("eval_series_rows({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                let checkpoint_metadata: Option<sqlx::types::Json<Value>> =
                    r.try_get("checkpoint_metadata")?;
                let eval_result_metadata: Option<sqlx::types::Json<Value>> =
                    r.try_get("eval_result_metadata")?;
                Ok(EvalSeriesRow {
                    eval_key: r.try_get("eval_key")?,
                    checkpoint_artifact_id: r.try_get("checkpoint_artifact_id")?,
                    eval_recipe_hash: r.try_get("eval_recipe_hash")?,
                    policy_id: r.try_get("policy_id")?,
                    eval_run_id: r.try_get("eval_run_id")?,
                    state: r.try_get("state")?,
                    checkpoint_metadata: checkpoint_metadata.map(|j| j.0),
                    eval_result_metadata: eval_result_metadata.map(|j| j.0),
                })
            })
            .collect()
    }

    pub async fn eval_requests_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        // `state` surfaces the eval_run's *current* lifecycle status
        // (the joined runs.status). The static `er.state` column is
        // never inspected — it only ever holds 'submitted', the row
        // creation marker — and would otherwise leak that as the
        // outward-facing answer.
        let rows = sqlx::query(
            "SELECT er.eval_key, er.checkpoint_artifact_id, er.eval_recipe_hash,
                    er.policy_id, er.eval_run_id,
                    COALESCE(er_run.status, 'pending') AS state
             FROM eval_requests er
             LEFT JOIN artifacts a ON a.id = er.checkpoint_artifact_id
             LEFT JOIN runs er_run ON er_run.id = er.eval_run_id
             WHERE a.producer_run_id = $1 OR er.eval_run_id = $1
             ORDER BY er.created_at",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("eval_requests_for_run({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                Ok(json!({
                    "eval_key": r.try_get::<String, _>("eval_key")?,
                    "checkpoint_artifact_id": r.try_get::<String, _>("checkpoint_artifact_id")?,
                    "eval_recipe_hash": r.try_get::<String, _>("eval_recipe_hash")?,
                    "policy_id": r.try_get::<String, _>("policy_id")?,
                    "eval_run_id": r.try_get::<Option<String>, _>("eval_run_id")?,
                    "state": r.try_get::<String, _>("state")?,
                }))
            })
            .collect()
    }

    pub async fn list_eval_requests(&self) -> Result<Vec<Value>> {
        // See `eval_requests_for_run` for the `state` derivation
        // rationale.
        let rows = sqlx::query(
            "SELECT er.eval_key, er.checkpoint_artifact_id, er.eval_recipe_hash,
                    er.policy_id, er.eval_run_id,
                    COALESCE(er_run.status, 'pending') AS state,
                    er.created_at, er.updated_at
             FROM eval_requests er
             LEFT JOIN runs er_run ON er_run.id = er.eval_run_id
             ORDER BY er.updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("list_eval_requests query")?;
        rows.into_iter()
            .map(|r| {
                Ok(json!({
                    "eval_key": r.try_get::<String, _>("eval_key")?,
                    "checkpoint_artifact_id": r.try_get::<String, _>("checkpoint_artifact_id")?,
                    "eval_recipe_hash": r.try_get::<String, _>("eval_recipe_hash")?,
                    "policy_id": r.try_get::<String, _>("policy_id")?,
                    "eval_run_id": r.try_get::<Option<String>, _>("eval_run_id")?,
                    "state": r.try_get::<String, _>("state")?,
                    "created_at": r.try_get::<i64, _>("created_at")?,
                    "updated_at": r.try_get::<i64, _>("updated_at")?,
                }))
            })
            .collect()
    }

    pub async fn eval_requests_by_policy(
        &self,
        policy_id: &str,
    ) -> Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT er.eval_key, er.checkpoint_artifact_id, er.eval_recipe_hash,
                    er.policy_id, er.eval_run_id,
                    COALESCE(er_run.status, 'pending') AS state,
                    er.created_at, er.updated_at
             FROM eval_requests er
             LEFT JOIN runs er_run ON er_run.id = er.eval_run_id
             WHERE er.policy_id = $1
             ORDER BY er.updated_at DESC",
        )
        .bind(policy_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("eval_requests_by_policy({policy_id})"))?;
        rows.into_iter()
            .map(|r| {
                Ok(json!({
                    "eval_key": r.try_get::<String, _>("eval_key")?,
                    "checkpoint_artifact_id": r.try_get::<String, _>("checkpoint_artifact_id")?,
                    "eval_recipe_hash": r.try_get::<String, _>("eval_recipe_hash")?,
                    "policy_id": r.try_get::<String, _>("policy_id")?,
                    "eval_run_id": r.try_get::<Option<String>, _>("eval_run_id")?,
                    "state": r.try_get::<String, _>("state")?,
                    "created_at": r.try_get::<i64, _>("created_at")?,
                    "updated_at": r.try_get::<i64, _>("updated_at")?,
                }))
            })
            .collect()
    }

    pub async fn policy_summaries(&self) -> Result<Vec<PolicySummaryRow>> {
        // `failed` / `running` are computed against the joined
        // runs.status, not the static eval_requests.state column.
        // `failed` covers the full terminal-failure set (matches
        // `eval_request_status`'s stale predicate). `running` covers
        // every still-active lifecycle status.
        let rows = sqlx::query(
            "SELECT er.policy_id,
                    COUNT(*)::BIGINT AS total,
                    SUM(CASE WHEN COALESCE(er_run.status, '') IN
                        ('failed','cancelled','timeout','oom','unknown_terminal')
                        THEN 1 ELSE 0 END)::BIGINT AS failed,
                    SUM(CASE WHEN COALESCE(er_run.status, 'pending') IN
                        ('created','submitted','running','awaiting_peer','pending')
                        THEN 1 ELSE 0 END)::BIGINT AS running,
                    MAX(er.updated_at) AS last_fired
             FROM eval_requests er
             LEFT JOIN runs er_run ON er_run.id = er.eval_run_id
             GROUP BY er.policy_id
             ORDER BY last_fired DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("policy_summaries query")?;
        rows.into_iter()
            .map(|r| {
                Ok(PolicySummaryRow {
                    name: r.try_get::<String, _>("policy_id")?,
                    total: r.try_get::<i64, _>("total")?,
                    failed: r.try_get::<i64, _>("failed")?,
                    running: r.try_get::<i64, _>("running")?,
                    last_fired_at: r.try_get::<i64, _>("last_fired")?,
                })
            })
            .collect()
    }

    pub async fn events_for_run(&self, run_id: &str) -> Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT event_type, payload_json, created_at FROM events
             WHERE run_id = $1 ORDER BY id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("events_for_run({run_id})"))?;
        rows.into_iter()
            .map(|r| {
                let payload: sqlx::types::Json<Value> = r.try_get("payload_json")?;
                Ok(json!({
                    "event_type": r.try_get::<String, _>("event_type")?,
                    "payload": payload.0,
                    "created_at": r.try_get::<i64, _>("created_at")?,
                }))
            })
            .collect()
    }

    pub async fn max_event_id(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COALESCE(MAX(id), 0)::BIGINT AS max_id FROM events")
            .fetch_one(&self.pool)
            .await
            .context("max_event_id query")?;
        Ok(row.try_get::<i64, _>("max_id")?)
    }

    pub async fn get_tracking(&self, run_id: &str) -> Result<Option<TrackingRow>> {
        let row = sqlx::query(
            "SELECT run_id, entity, project, url, group_name, source, created_at
             FROM tracking WHERE run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("get_tracking({run_id})"))?;
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(TrackingRow {
                run_id: r.try_get("run_id")?,
                entity: r.try_get("entity")?,
                project: r.try_get("project")?,
                url: r.try_get("url")?,
                group_name: r.try_get("group_name")?,
                source: r.try_get("source")?,
                created_at: r.try_get("created_at")?,
            })),
        }
    }

    pub async fn runs_missing_tracking(&self) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(
            "SELECT r.id, r.recipe_name, r.recipe_hash, r.status, r.job_id, r.run_dir,
                    r.repo, r.source_path, r.recipe_json, r.context_json, r.created_at,
                    r.finished_at, r.pipeline_id, r.stage_name, r.dependency_on,
                    r.submitted_by, r.cache_key, r.coalesced_peer_run_id
             FROM runs r
             LEFT JOIN tracking t ON t.run_id = r.id
             WHERE t.run_id IS NULL
             ORDER BY r.created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .context("runs_missing_tracking query")?;
        rows.into_iter().map(row_to_run).collect()
    }

    // ---------- additional write paths ----------

    /// Insert a fresh run row plus its input-resolution rows in one
    /// transaction. Mirrors the SQLite `Store::insert_run` but is
    /// DB-only: no sidecar writes, no event emission. Callers that need
    /// the legacy "run_created" event must call `append_event` themselves.
    pub async fn insert_run(
        &self,
        run: NewRun<'_>,
        inputs: &[InputResolution],
    ) -> Result<()> {
        let submitted_by = run
            .submitted_by
            .map(|s| s.to_string())
            .or_else(|| std::env::var("USER").ok())
            .or_else(|| std::env::var("USERNAME").ok())
            .unwrap_or_else(|| "unknown".to_string());
        let now = crate::util::now_ts();
        let recipe_value = serde_json::to_value(run.recipe)
            .context("insert_run: serialise recipe")?;

        let mut tx = self.pool.begin().await.context("insert_run: begin tx")?;
        // Upsert. When agent-driven submission completes a previously
        // inserted pending placeholder (status='created', null job_id),
        // we hit ON CONFLICT and update with the now-known fields.
        // pipeline_id/stage_name/dependency_on are preserved because they
        // were set on the placeholder; we don't touch them here.
        sqlx::query(
            "INSERT INTO runs
             (id, recipe_name, recipe_hash, status, run_dir, repo, source_path,
              recipe_json, context_json, created_at, submitted_by, cache_key,
              coalesced_peer_run_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULL)
             ON CONFLICT (id) DO UPDATE SET
                 recipe_name = EXCLUDED.recipe_name,
                 recipe_hash = EXCLUDED.recipe_hash,
                 status      = EXCLUDED.status,
                 run_dir     = EXCLUDED.run_dir,
                 repo        = EXCLUDED.repo,
                 source_path = EXCLUDED.source_path,
                 recipe_json = EXCLUDED.recipe_json,
                 context_json= EXCLUDED.context_json,
                 submitted_by= EXCLUDED.submitted_by,
                 cache_key   = EXCLUDED.cache_key",
        )
        .bind(run.id)
        .bind(&run.recipe.name)
        .bind(run.recipe_hash)
        .bind(run.status)
        .bind(run.run_dir.display().to_string())
        .bind(&run.recipe.repo)
        .bind(run.source_path.display().to_string())
        .bind(sqlx::types::Json(&recipe_value))
        .bind(sqlx::types::Json(run.context_json))
        .bind(now)
        .bind(&submitted_by)
        .bind(run.cache_key)
        .execute(&mut *tx)
        .await
        .context("insert_run: runs upsert")?;

        // Clear any prior run_inputs (placeholder may not have had any,
        // but be defensive) then re-insert the now-resolved set.
        sqlx::query("DELETE FROM run_inputs WHERE run_id = $1")
            .bind(run.id)
            .execute(&mut *tx)
            .await
            .context("insert_run: run_inputs cleanup")?;
        for input in inputs {
            sqlx::query(
                "INSERT INTO run_inputs (run_id, role, artifact_id, resolved_path)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(run.id)
            .bind(&input.role)
            .bind(input.artifact_id.as_deref())
            .bind(input.resolved_path.display().to_string())
            .execute(&mut *tx)
            .await
            .with_context(|| {
                format!("insert_run: run_inputs insert role={}", input.role)
            })?;
        }
        tx.commit().await.context("insert_run: commit")?;
        Ok(())
    }

    pub async fn set_submitted(&self, run_id: &str, job_id: &str) -> Result<()> {
        sqlx::query("UPDATE runs SET status='submitted', job_id=$1 WHERE id=$2")
            .bind(job_id)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("set_submitted({run_id})"))?;
        Ok(())
    }

    /// Insert a placeholder row for a pipeline stage whose upstream
    /// dependencies haven't completed yet. status='created', no job_id,
    /// no cache_key, empty run_inputs. The agent's reconciler later
    /// upgrades this row to a real submission via the normal `insert_run`
    /// path (upsert) once upstream artifacts exist and inputs can be
    /// resolved.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_pending_pipeline_stage(
        &self,
        run_id: &str,
        recipe: &Recipe,
        recipe_hash: &str,
        run_dir: &Path,
        source_path: &Path,
        submitted_by: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        let now = crate::util::now_ts();
        let recipe_value = serde_json::to_value(recipe)
            .context("insert_pending_pipeline_stage: serialise recipe")?;
        // context_json is a placeholder — the agent rewrites it at the
        // moment of real submission with resolved inputs/outputs.
        let ctx = json!({});
        sqlx::query(
            "INSERT INTO runs
             (id, recipe_name, recipe_hash, status, run_dir, repo, source_path,
              recipe_json, context_json, created_at, submitted_by,
              pipeline_id, stage_name, dependency_on)
             VALUES ($1,$2,$3,'created',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        )
        .bind(run_id)
        .bind(&recipe.name)
        .bind(recipe_hash)
        .bind(run_dir.display().to_string())
        .bind(&recipe.repo)
        .bind(source_path.display().to_string())
        .bind(sqlx::types::Json(&recipe_value))
        .bind(sqlx::types::Json(ctx))
        .bind(now)
        .bind(submitted_by)
        .bind(pipeline_id)
        .bind(stage_name)
        .bind(sqlx::types::Json(dependency_on))
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert_pending_pipeline_stage({run_id})"))?;
        Ok(())
    }

    /// Return all pending (status='created', job_id IS NULL) runs whose
    /// `dependency_on->'afterok'` references `parent_run_id`. Caller
    /// decides whether each pending run's deps are NOW fully satisfied;
    /// this just narrows the candidate set.
    pub async fn pending_children_of(&self, parent_run_id: &str) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} \
             WHERE status = 'created' AND job_id IS NULL \
               AND dependency_on @> $1::jsonb"
        ))
        .bind(sqlx::types::Json(json!({"afterok": [{"run_id": parent_run_id}]})))
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("pending_children_of({parent_run_id})"))?;
        rows.into_iter().map(row_to_run).collect()
    }

    /// Terminal-state runs owned by `submitted_by` that still have at least
    /// one pending child waiting on them. Used by reconcile to retroactively
    /// advance children whose parent transitioned to terminal while the
    /// agent was down (or in a prior reconcile pass): the in-pass
    /// `try_submit_pending_children` only fires when `reconcile_one`
    /// observes the transition itself.
    pub async fn list_terminal_runs_with_pending_children(
        &self,
        submitted_by: &str,
    ) -> Result<Vec<RunRow>> {
        let rows = sqlx::query(&format!(
            "{RUN_SELECT_BASE} p \
             WHERE p.status IN ('succeeded','cache_hit','failed','cancelled','timeout','oom','unknown_terminal') \
               AND p.submitted_by = $1 \
               AND EXISTS ( \
                   SELECT 1 FROM runs c \
                   WHERE c.status = 'created' AND c.job_id IS NULL \
                     AND c.dependency_on @> jsonb_build_object('afterok', jsonb_build_array(jsonb_build_object('run_id', p.id))) \
               )"
        ))
        .bind(submitted_by)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("list_terminal_runs_with_pending_children({submitted_by})"))?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn update_status(
        &self,
        run_id: &str,
        status: &str,
        finished_at: Option<i64>,
    ) -> Result<()> {
        let terminal = is_terminal(status);
        let ts = finished_at.unwrap_or_else(crate::util::now_ts);
        sqlx::query(
            "UPDATE runs
             SET status = $1,
                 finished_at = CASE WHEN $2 THEN $3 ELSE finished_at END
             WHERE id = $4",
        )
        .bind(status)
        .bind(terminal)
        .bind(ts)
        .bind(run_id)
        .execute(&self.pool)
        .await
        .with_context(|| format!("update_status({run_id}, {status})"))?;
        Ok(())
    }

    pub async fn set_finished_at(&self, run_id: &str, finished_at: i64) -> Result<()> {
        sqlx::query("UPDATE runs SET finished_at=$1 WHERE id=$2")
            .bind(finished_at)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("set_finished_at({run_id})"))?;
        Ok(())
    }

    /// Insert an artifact row. DB-only: the caller owns the sidecar
    /// (`.meta.json`) write and any other FS work. `content_hash` is
    /// NULL on every row inserted through this method — the column
    /// only carries values populated by the original importer / legacy
    /// content-addressed code (migration 0004 relaxed NOT NULL and
    /// dropped UNIQUE).
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_artifact(
        &self,
        id: &str,
        kind: &str,
        path: &Path,
        producer_run_id: Option<&str>,
        metadata: &Value,
        user: &str,
        alias_segment: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO artifacts
             (id, kind, path, content_hash, producer_run_id, metadata_json,
              created_at, \"user\", alias_segment)
             VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8)",
        )
        .bind(id)
        .bind(kind)
        .bind(path.display().to_string())
        .bind(producer_run_id)
        .bind(sqlx::types::Json(metadata))
        .bind(created_at)
        .bind(user)
        .bind(alias_segment)
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert_artifact({id})"))?;
        Ok(())
    }

    /// Upsert into the GLOBAL `artifact_aliases` table. Mirrors the
    /// SQLite Store's `INSERT OR REPLACE` semantics.
    pub async fn set_alias(&self, alias: &str, artifact_id: &str) -> Result<()> {
        let now = crate::util::now_ts();
        sqlx::query(
            "INSERT INTO artifact_aliases (alias, artifact_id, created_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (alias) DO UPDATE SET
                 artifact_id = EXCLUDED.artifact_id,
                 created_at = EXCLUDED.created_at",
        )
        .bind(alias)
        .bind(artifact_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .with_context(|| format!("set_alias({alias})"))?;
        Ok(())
    }

    pub async fn link_run_output(
        &self,
        run_id: &str,
        role: &str,
        artifact_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO run_outputs (run_id, role, artifact_id)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(run_id)
        .bind(role)
        .bind(artifact_id)
        .execute(&self.pool)
        .await
        .with_context(|| format!("link_run_output({run_id}, {role})"))?;
        Ok(())
    }

    pub async fn copy_run_outputs(
        &self,
        source_run_id: &str,
        dest_run_id: &str,
    ) -> Result<()> {
        let rows = self.run_output_links(source_run_id).await?;
        for (role, artifact_id) in &rows {
            self.link_run_output(dest_run_id, role, artifact_id).await?;
        }
        self.backfill_stage_consumers(dest_run_id, &rows).await?;
        Ok(())
    }

    /// Bulk-patch run_inputs whose resolved_path matches `path` and whose
    /// artifact_id is NULL — the operation that lights up `inputs.artifact_id`
    /// once an artifact materializes at a path runs were waiting on.
    pub async fn rehydrate_inputs_by_path(
        &self,
        path: &str,
        artifact_id: &str,
    ) -> Result<usize> {
        let result = sqlx::query(
            "UPDATE run_inputs SET artifact_id = $1 \
             WHERE resolved_path = $2 AND artifact_id IS NULL",
        )
        .bind(artifact_id)
        .bind(path)
        .execute(&self.pool)
        .await
        .with_context(|| format!("rehydrate_inputs_by_path({path})"))?;
        Ok(result.rows_affected() as usize)
    }

    /// Set `run_inputs[(run_id, role)].artifact_id` and `resolved_path`
    /// iff currently NULL. DB-only mirror of the SQLite helper of the
    /// same name. Returns `true` if a row was actually patched.
    pub async fn set_run_input_artifact(
        &self,
        run_id: &str,
        role: &str,
        artifact_id: &str,
    ) -> Result<bool> {
        let artifact = self
            .get_artifact(artifact_id)
            .await?
            .with_context(|| format!("artifact not found: {artifact_id}"))?;
        let path_str = artifact.path.display().to_string();
        let result = sqlx::query(
            "UPDATE run_inputs
             SET artifact_id = $1, resolved_path = $2
             WHERE run_id = $3 AND role = $4 AND artifact_id IS NULL",
        )
        .bind(artifact_id)
        .bind(&path_str)
        .bind(run_id)
        .bind(role)
        .execute(&self.pool)
        .await
        .with_context(|| format!("set_run_input_artifact({run_id}, {role})"))?;
        Ok(result.rows_affected() > 0)
    }

    /// Structural pipeline-graph backfill. Mirrors the SQLite Store's
    /// `backfill_stage_consumers`: for every sibling run in the same
    /// pipeline whose recipe declares a `type=stage` input pointing at
    /// the producer's stage, patch the matching `run_inputs` row.
    pub async fn backfill_stage_consumers(
        &self,
        producer_run_id: &str,
        outputs: &[(String, String)],
    ) -> Result<usize> {
        let producer = self
            .get_run(producer_run_id)
            .await?
            .with_context(|| format!("backfill_stage_consumers: producer not found {producer_run_id}"))?;
        let (pipeline_id, stage_name) =
            match (producer.pipeline_id.as_deref(), producer.stage_name.as_deref()) {
                (Some(p), Some(s)) => (p.to_string(), s.to_string()),
                _ => return Ok(0),
            };
        let outputs_by_role: BTreeMap<&str, &str> = outputs
            .iter()
            .map(|(r, a)| (r.as_str(), a.as_str()))
            .collect();
        let mut patched = 0usize;
        for sibling in self.list_pipeline_runs(&pipeline_id).await? {
            if sibling.id == producer_run_id {
                continue;
            }
            let recipe: Recipe = match serde_json::from_value(sibling.recipe_json.clone()) {
                Ok(r) => r,
                Err(_) => continue,
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
                if self
                    .set_run_input_artifact(&sibling.id, input_role, artifact_id)
                    .await?
                {
                    patched += 1;
                }
            }
        }
        Ok(patched)
    }

    pub async fn append_stage_cache_hit_event(
        &self,
        run_id: &str,
        cache_key: &str,
        source_run_id: &str,
    ) -> Result<()> {
        self.append_event(
            Some(run_id),
            "stage_cache_hit",
            &json!({
                "cache_key": cache_key,
                "source_run_id": source_run_id,
            }),
            crate::util::now_ts(),
        )
        .await?;
        Ok(())
    }

    pub async fn append_stage_coalesce_resolved_event(
        &self,
        run_id: &str,
        peer_run_id: &str,
    ) -> Result<()> {
        self.append_event(
            Some(run_id),
            "stage_coalesce_resolved",
            &json!({
                "peer_run_id": peer_run_id,
                "outcome": "cache_hit",
            }),
            crate::util::now_ts(),
        )
        .await?;
        Ok(())
    }

    pub async fn append_stage_coalesce_failed_event(
        &self,
        run_id: &str,
        peer_run_id: &str,
        peer_status: &str,
    ) -> Result<()> {
        self.append_event(
            Some(run_id),
            "stage_coalesce_failed",
            &json!({
                "peer_run_id": peer_run_id,
                "peer_status": peer_status,
            }),
            crate::util::now_ts(),
        )
        .await?;
        Ok(())
    }

    // ---------- in-flight coalescing ----------
    //
    // Leader election via the `coalesce_claims` table. PRIMARY KEY on
    // `cache_key` + `INSERT ... ON CONFLICT DO NOTHING RETURNING` makes
    // the claim genuinely atomic across all PG clients — replaces the
    // legacy NFS-mkdir approach which had no atomicity guarantee across
    // NFS clients.

    pub async fn claim_coalesce_slot(
        &self,
        cache_key: &str,
        producer_run_id: &str,
    ) -> Result<ClaimOutcome> {
        let row = sqlx::query(
            "INSERT INTO coalesce_claims (cache_key, producer_run_id, claimed_at) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (cache_key) DO NOTHING \
             RETURNING producer_run_id",
        )
        .bind(cache_key)
        .bind(producer_run_id)
        .bind(crate::util::now_ts())
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("claim_coalesce_slot({cache_key})"))?;
        Ok(if row.is_some() {
            ClaimOutcome::Claimed
        } else {
            ClaimOutcome::AlreadyExists
        })
    }

    pub async fn release_coalesce_slot(&self, cache_key: &str) -> Result<()> {
        sqlx::query("DELETE FROM coalesce_claims WHERE cache_key = $1")
            .bind(cache_key)
            .execute(&self.pool)
            .await
            .with_context(|| format!("release_coalesce_slot({cache_key})"))?;
        Ok(())
    }

    /// Sweep coalesce_claims whose producer has reached a terminal status.
    /// The normal release path (`release_coalesce_slot` after the producer
    /// reconciles to terminal) covers the happy path; this sweep is the
    /// safety net for producers that died between reaching terminal and
    /// firing the release, plus any FK-cascade survivors from external
    /// row deletion. Returns the number of claims swept.
    pub async fn gc_terminal_coalesce_claims(&self) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM coalesce_claims
             WHERE producer_run_id IN (
                 SELECT id FROM runs
                 WHERE status IN (
                     'succeeded','failed','cancelled','timeout','oom',
                     'unknown_terminal','cache_hit'
                 )
             )",
        )
        .execute(&self.pool)
        .await
        .context("gc_terminal_coalesce_claims")?;
        Ok(result.rows_affected())
    }

    pub async fn set_awaiting_peer(
        &self,
        run_id: &str,
        job_id: &str,
        peer_run_id: &str,
        cache_key: &str,
    ) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("set_awaiting_peer: begin tx")?;
        sqlx::query(
            "UPDATE runs
             SET status='awaiting_peer', job_id=$1, coalesced_peer_run_id=$2
             WHERE id=$3",
        )
        .bind(job_id)
        .bind(peer_run_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("set_awaiting_peer({run_id})"))?;
        let now = crate::util::now_ts();
        let payload = json!({
            "peer_run_id": peer_run_id,
            "cache_key": cache_key,
            "job_id": job_id,
        });
        sqlx::query(
            "INSERT INTO events (run_id, event_type, payload_json, created_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(run_id)
        .bind("stage_coalesced")
        .bind(sqlx::types::Json(&payload))
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("set_awaiting_peer: event insert")?;
        tx.commit().await.context("set_awaiting_peer: commit")?;
        Ok(())
    }

    // ---------- users / admin ----------

    /// Register a new labctl-side user row. Returns true iff a new
    /// row was inserted (false on ON CONFLICT — caller may treat that
    /// as already-registered).
    pub async fn insert_user(&self, name: &str, created_at: i64) -> Result<bool> {
        let row = sqlx::query(
            "INSERT INTO users (name, created_at, pg_role)
             VALUES ($1, $2, $1)
             ON CONFLICT (name) DO NOTHING
             RETURNING name",
        )
        .bind(name)
        .bind(created_at)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("insert_user({name})"))?;
        Ok(row.is_some())
    }

    /// Create the PG role + GRANTs to mirror the labctl-side user row.
    /// Idempotent: if the role already exists the CREATE step is
    /// skipped but the GRANTs re-apply (a no-op when already granted).
    /// Returns true iff this call actually created the role.
    ///
    /// The connecting user needs `CREATEROLE` and ownership of the
    /// schema/tables being granted — on the standard single-host lab
    /// deployment that's the user who ran `initdb`.
    ///
    /// Identifier safety: callers in `admin.rs` constrain `name` to
    /// `[A-Za-z0-9._-]+` before reaching here, so direct interpolation
    /// into `"<name>"` is sound. We still double-quote the identifier
    /// rather than relying on PG's `format('%I', ...)` because the
    /// individual statements aren't wrapped in a DO block (each one
    /// runs through the sqlx prepared-statement path with no embedded
    /// `EXECUTE format(...)` machinery).
    pub async fn ensure_pg_role(&self, name: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await.context("ensure_pg_role: begin")?;
        let already = sqlx::query("SELECT 1 FROM pg_roles WHERE rolname = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .with_context(|| format!("ensure_pg_role({name}): pre-check"))?
            .is_some();
        if !already {
            sqlx::query(&format!(r#"CREATE ROLE "{name}" WITH LOGIN"#))
                .execute(&mut *tx)
                .await
                .with_context(|| format!("ensure_pg_role({name}): CREATE ROLE"))?;
        }
        for stmt in [
            format!(r#"GRANT ALL ON SCHEMA public TO "{name}""#),
            format!(r#"GRANT ALL ON ALL TABLES IN SCHEMA public TO "{name}""#),
            format!(r#"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{name}""#),
            format!(
                r#"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{name}""#
            ),
            format!(
                r#"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{name}""#
            ),
        ] {
            sqlx::query(&stmt)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("ensure_pg_role({name}): {stmt}"))?;
        }
        tx.commit().await.context("ensure_pg_role: commit")?;
        Ok(!already)
    }

    pub async fn insert_pipeline(
        &self,
        id: &str,
        name: &str,
        pipeline_path: Option<&Path>,
        user: &str,
    ) -> Result<()> {
        let now = crate::util::now_ts();
        sqlx::query(
            "INSERT INTO pipelines (id, name, pipeline_path, \"user\", created_at)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(name)
        .bind(pipeline_path.map(|p| p.display().to_string()))
        .bind(user)
        .bind(now)
        .execute(&self.pool)
        .await
        .with_context(|| format!("insert_pipeline({id})"))?;
        Ok(())
    }

    pub async fn set_pipeline_membership(
        &self,
        run_id: &str,
        pipeline_id: &str,
        stage_name: &str,
        dependency_on: &Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE runs
             SET pipeline_id = $1, stage_name = $2, dependency_on = $3
             WHERE id = $4",
        )
        .bind(pipeline_id)
        .bind(stage_name)
        .bind(sqlx::types::Json(dependency_on))
        .bind(run_id)
        .execute(&self.pool)
        .await
        .with_context(|| format!("set_pipeline_membership({run_id})"))?;
        Ok(())
    }

    /// Atomically take the eval slot for `eval_key` on a fresh insert.
    /// Returns true iff this caller won the race. Loser callers must
    /// either `claim_eval_slot_retry` (if the snapshot indicated Retry)
    /// or surface the lost-race (orphan SLURM job risk).
    pub async fn claim_eval_slot_fresh(
        &self,
        eval_key: &str,
        checkpoint_artifact_id: &str,
        eval_recipe_hash: &str,
        policy_id: &str,
        eval_run_id: &str,
        user: &str,
    ) -> Result<bool> {
        let now = crate::util::now_ts();
        let row = sqlx::query(
            "INSERT INTO eval_requests
             (eval_key, checkpoint_artifact_id, eval_recipe_hash, policy_id,
              eval_run_id, state, attempts, \"user\", created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, 'submitted', 1, $6, $7, $7)
             ON CONFLICT (eval_key) DO NOTHING
             RETURNING eval_key",
        )
        .bind(eval_key)
        .bind(checkpoint_artifact_id)
        .bind(eval_recipe_hash)
        .bind(policy_id)
        .bind(eval_run_id)
        .bind(user)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("claim_eval_slot_fresh({eval_key})"))?;
        Ok(row.is_some())
    }

    /// Atomically advance the eval slot to a new attempt. Optimistic
    /// concurrency: the UPDATE only fires when the row still has the
    /// `expected_attempts` count and the prior `expected_run_id`
    /// reference we read in our snapshot — otherwise another caller
    /// already retried this slot and we abort. Returns true iff the
    /// update applied.
    pub async fn claim_eval_slot_retry(
        &self,
        eval_key: &str,
        expected_run_id: &str,
        expected_attempts: i64,
        new_eval_run_id: &str,
    ) -> Result<bool> {
        let now = crate::util::now_ts();
        let row = sqlx::query(
            "UPDATE eval_requests
             SET eval_run_id = $1, state = 'submitted', attempts = $2, updated_at = $3
             WHERE eval_key = $4
               AND attempts = $5
               AND eval_run_id = $6
             RETURNING eval_key",
        )
        .bind(new_eval_run_id)
        .bind(expected_attempts + 1)
        .bind(now)
        .bind(eval_key)
        .bind(expected_attempts)
        .bind(expected_run_id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("claim_eval_slot_retry({eval_key})"))?;
        Ok(row.is_some())
    }

    /// Returns the run id currently bound to `eval_key`. Used by callers
    /// that need to read the snapshot's eval_run_id for optimistic
    /// concurrency on the retry path.
    pub async fn eval_request_run_id(&self, eval_key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT eval_run_id FROM eval_requests WHERE eval_key = $1")
            .bind(eval_key)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("eval_request_run_id({eval_key})"))?;
        match row {
            None => Ok(None),
            Some(r) => Ok(r.try_get::<Option<String>, _>("eval_run_id")?),
        }
    }

    pub async fn set_tracking(
        &self,
        run_id: &str,
        entity: &str,
        project: &str,
        url: &str,
        group: Option<&str>,
        source: &str,
    ) -> Result<()> {
        let now = crate::util::now_ts();
        sqlx::query(
            "INSERT INTO tracking
                 (run_id, entity, project, url, group_name, source, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (run_id) DO UPDATE SET
                 entity = EXCLUDED.entity,
                 project = EXCLUDED.project,
                 url = EXCLUDED.url,
                 group_name = EXCLUDED.group_name,
                 source = EXCLUDED.source",
        )
        .bind(run_id)
        .bind(entity)
        .bind(project)
        .bind(url)
        .bind(group)
        .bind(source)
        .bind(now)
        .execute(&self.pool)
        .await
        .with_context(|| format!("set_tracking({run_id})"))?;
        Ok(())
    }

    // ---------- additional reads needed by the sync Store facade ----------

    pub async fn artifacts_by_kind(&self, kind: &str) -> Result<Vec<ArtifactRow>> {
        let rows = sqlx::query(&format!(
            "{ARTIFACT_SELECT_BASE} WHERE kind = $1 ORDER BY created_at ASC"
        ))
        .bind(kind)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("artifacts_by_kind({kind})"))?;
        rows.into_iter().map(row_to_artifact).collect()
    }

    /// Artifacts of a given kind whose producing run was submitted by
    /// `user`. Externally-registered artifacts (no producer run) are
    /// excluded — they have no owner attribution.
    pub async fn artifacts_by_kind_for_producer_user(
        &self,
        kind: &str,
        user: &str,
    ) -> Result<Vec<ArtifactRow>> {
        let rows = sqlx::query(
            "SELECT a.id, a.kind, a.path, a.content_hash, a.producer_run_id,
                    a.metadata_json, a.created_at
             FROM artifacts a
             JOIN runs r ON a.producer_run_id = r.id
             WHERE a.kind = $1 AND r.submitted_by = $2
             ORDER BY a.created_at ASC",
        )
        .bind(kind)
        .bind(user)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("artifacts_by_kind_for_producer_user({kind}, {user})"))?;
        rows.into_iter().map(row_to_artifact).collect()
    }

    /// Returns `(status, created_at)` tuples for the most recent `limit`
    /// runs of this recipe, oldest-first.
    pub async fn recipe_history(
        &self,
        recipe_name: &str,
        limit: i64,
    ) -> Result<Vec<(String, i64)>> {
        // Inner LIMIT picks the most recent N; outer ORDER BY ASC ships
        // them oldest-first. One query, no client-side reversal.
        let rows = sqlx::query(
            "SELECT status, created_at FROM (
                 SELECT status, created_at FROM runs
                 WHERE recipe_name = $1
                 ORDER BY created_at DESC LIMIT $2
             ) recent
             ORDER BY created_at ASC",
        )
        .bind(recipe_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("recipe_history({recipe_name})"))?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>("status")?,
                    r.try_get::<i64, _>("created_at")?,
                ))
            })
            .collect()
    }
}

fn build_connect_options(pg: &PgConfig) -> Result<PgConnectOptions> {
    // host = absolute path → Unix socket dir; else TCP. The port is
    // passed in both cases — under sockets it encodes the socket
    // filename (`<dir>/.s.PGSQL.<port>`). sqlx interprets `host`
    // starting with `/` as a socket dir, mirroring libpq behavior;
    // explicit `.socket()` calls in older sqlx APIs proved unreliable
    // across versions, so we set it via `.host()`.
    let mut opts = PgConnectOptions::new()
        .host(&pg.host)
        .port(pg.port)
        .database(&pg.database);
    let user = match pg.user.as_deref() {
        Some(u) => u.to_string(),
        None => std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .context(
                "[postgres].user not set and $USER/$USERNAME unavailable; \
                 cannot pick a PG role",
            )?,
    };
    opts = opts.username(&user);
    if let Some(var) = pg.password_env.as_deref() {
        let pw = std::env::var(var).with_context(|| {
            format!("[postgres].password_env={var} but the env var is unset")
        })?;
        opts = opts.password(&pw);
    }
    Ok(opts)
}

// Column lists kept in sync with migrations/0001_initial_schema.sql.
const RUN_SELECT_BASE: &str = "
    SELECT id, recipe_name, recipe_hash, status, job_id, run_dir, repo,
           source_path, recipe_json, context_json, created_at, finished_at,
           pipeline_id, stage_name, dependency_on, submitted_by, cache_key,
           coalesced_peer_run_id
    FROM runs
";

const RUN_SELECT_ALL: &str = "
    SELECT id, recipe_name, recipe_hash, status, job_id, run_dir, repo,
           source_path, recipe_json, context_json, created_at, finished_at,
           pipeline_id, stage_name, dependency_on, submitted_by, cache_key,
           coalesced_peer_run_id
    FROM runs
    ORDER BY created_at DESC
";

const ARTIFACT_SELECT_BASE: &str = "
    SELECT id, kind, path, content_hash, producer_run_id, metadata_json, created_at
    FROM artifacts
";

fn row_to_run(r: sqlx::postgres::PgRow) -> Result<RunRow> {
    let recipe_json: sqlx::types::Json<Value> = r.try_get("recipe_json")?;
    let context_json: sqlx::types::Json<Value> = r.try_get("context_json")?;
    let dependency_on: Option<sqlx::types::Json<Value>> = r.try_get("dependency_on")?;
    Ok(RunRow {
        id: r.try_get("id")?,
        recipe_name: r.try_get("recipe_name")?,
        recipe_hash: r.try_get("recipe_hash")?,
        status: r.try_get("status")?,
        job_id: r.try_get("job_id")?,
        run_dir: PathBuf::from(r.try_get::<String, _>("run_dir")?),
        repo: r.try_get("repo")?,
        source_path: PathBuf::from(r.try_get::<String, _>("source_path")?),
        recipe_json: recipe_json.0,
        context_json: context_json.0,
        created_at: r.try_get("created_at")?,
        finished_at: r.try_get("finished_at")?,
        pipeline_id: r.try_get("pipeline_id")?,
        stage_name: r.try_get("stage_name")?,
        dependency_on: dependency_on.map(|j| j.0),
        submitted_by: r.try_get("submitted_by")?,
        cache_key: r.try_get("cache_key")?,
        coalesced_peer_run_id: r.try_get("coalesced_peer_run_id")?,
    })
}

fn row_to_artifact(r: sqlx::postgres::PgRow) -> Result<ArtifactRow> {
    let metadata_json: sqlx::types::Json<Value> = r.try_get("metadata_json")?;
    Ok(ArtifactRow {
        id: r.try_get("id")?,
        kind: r.try_get("kind")?,
        path: PathBuf::from(r.try_get::<String, _>("path")?),
        content_hash: r.try_get("content_hash")?,
        producer_run_id: r.try_get("producer_run_id")?,
        metadata_json: metadata_json.0,
        created_at: r.try_get("created_at")?,
    })
}

#[cfg(test)]
mod tests {
    //! Tests run against the live PG instance configured in the user's
    //! cluster.toml (see docs/POSTGRES_DEPLOY.md). They're tagged
    //! `#[ignore]` so `cargo test` doesn't accidentally hit a real PG;
    //! invoke as `cargo test --test pg_smoke -- --ignored` after the
    //! instance is up and the importer has populated data.
    use super::*;
    use crate::config::ClusterConfig;
    use std::path::PathBuf;

    fn live_cluster() -> Option<ClusterConfig> {
        let p = PathBuf::from(
            std::env::var("HOME").unwrap_or_default() + "/.config/labctl/cluster.toml",
        );
        ClusterConfig::load(&p).ok()
    }

    #[tokio::test]
    #[ignore = "requires running PG with imported data; see docs/POSTGRES_DEPLOY.md"]
    async fn connect_and_count() {
        let cluster = live_cluster().expect("cluster.toml present");
        let store = PgStore::connect(&cluster).await.expect("connect");
        let runs = store.list_runs().await.expect("list_runs");
        let artifacts = store.list_artifacts().await.expect("list_artifacts");
        let pipelines = store.list_pipelines().await.expect("list_pipelines");
        eprintln!(
            "live PG smoke: runs={} artifacts={} pipelines={}",
            runs.len(),
            artifacts.len(),
            pipelines.len(),
        );
        assert!(!runs.is_empty(), "expected imported runs");
        assert!(!artifacts.is_empty(), "expected imported artifacts");
    }

    #[tokio::test]
    #[ignore = "requires running PG"]
    async fn round_trip_event() {
        let cluster = live_cluster().expect("cluster.toml present");
        let store = PgStore::connect(&cluster).await.expect("connect");
        let ts = crate::util::now_ts();
        let id = store
            .append_event(
                None,
                "pg_smoke_test",
                &serde_json::json!({"by": "pg_store::tests"}),
                ts,
            )
            .await
            .expect("append");
        let mut found = store.events_after(id - 1, 10).await.expect("events_after");
        let recovered = found.iter().find(|e| e.id == id).expect("event present");
        assert_eq!(recovered.event_type, "pg_smoke_test");
        assert_eq!(recovered.payload["by"], "pg_store::tests");
        // Clean up: delete the smoke event we just inserted.
        sqlx::query("DELETE FROM events WHERE id = $1")
            .bind(id)
            .execute(store.pool())
            .await
            .expect("cleanup");
        found.clear();
    }
}
