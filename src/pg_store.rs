//! Postgres-backed registry store. Parallel to the legacy in-memory
//! SQLite `Store`; the two coexist during the migration. Once all call
//! sites have moved to `PgStore`, `Store` and its rebuild walker get
//! deleted.
//!
//! Async-everywhere. Callers from sync contexts need to spawn a Tokio
//! runtime or use `tokio::runtime::Handle::current().block_on(...)`.
//!
//! Reuses the row types from `store.rs` so call sites don't churn:
//! `RunRow`, `ArtifactRow`, `EventRow`, `TrackingRow`, `InputResolution`,
//! `PipelineRow`. JSON columns map to `serde_json::Value` directly via
//! sqlx's `Json` wrapper.
//!
//! Currently `dead_code`-allow'd at the module level: nothing in the
//! production code paths uses `PgStore` yet — call-site migration is
//! incremental and lands in follow-up commits. The smoke tests below
//! exercise the connection + a representative read/write each.

#![allow(dead_code)]

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::config::{ClusterConfig, PgConfig};
use crate::store::{
    ArtifactRow, EventRow, InputResolution, PipelineRow, RunRow, TrackingRow,
};

pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Open a connection pool against the cluster's configured PG
    /// instance. `cluster.postgres` must be set — this is a hard
    /// requirement post-migration, no fallback.
    pub async fn connect(cluster: &ClusterConfig) -> Result<Self> {
        let pg = cluster.postgres.as_ref().with_context(|| {
            format!(
                "cluster {:?} has no [postgres] section; the PG-as-truth \
                 registry is now required — see docs/POSTGRES_DEPLOY.md",
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

    pub async fn find_artifact_by_hash(
        &self,
        kind: &str,
        content_hash: &str,
    ) -> Result<Option<ArtifactRow>> {
        let row = sqlx::query(&format!(
            "{ARTIFACT_SELECT_BASE} WHERE kind = $1 AND content_hash = $2 LIMIT 1"
        ))
        .bind(kind)
        .bind(content_hash)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("find_artifact_by_hash(kind={kind:?})"))?;
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
