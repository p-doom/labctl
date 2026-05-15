//! HTTP API + embedded SPA. Behind the `ui` feature.
//!
//! Read-only window onto the filesystem-truth registry. The CLI is the
//! only writer in the new model — every `labctl run` / `labctl
//! run-pipeline` writes sidecars directly under its own uid, never
//! through here. This server's job is to surface the in-memory cache
//! (built by the indexer at startup) over HTTP for the SPA. Bind to
//! 127.0.0.1 on a login node and reach it over an SSH tunnel; access
//! control is "who can SSH in."

use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use futures_util::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::trace::TraceLayer;

use crate::{
    config::ClusterConfig,
    store::{ArtifactRow, RunRow, Store, is_terminal},
};

#[derive(rust_embed::RustEmbed)]
#[folder = "ui/dist/"]
struct Assets;

#[derive(Clone)]
struct AppState {
    store: Arc<Mutex<Store>>,
    cluster: Arc<ClusterConfig>,
    /// Broadcast channel for SSE clients. The events-table tail task posts
    /// here; each connected client subscribes via `/api/stream`.
    events_tx: broadcast::Sender<StreamEvent>,
    /// Per-artifact dataset summary cache. Datasets are immutable post-
    /// completion so we walk once and reuse. Keyed by artifact id; entry is
    /// `None` if the artifact has no browseable per-segment layout.
    dataset_cache: Arc<Mutex<HashMap<String, Arc<Option<DatasetSummary>>>>>,
}

/// Outbound SSE message. Kept tiny — just enough for the client cache to
/// know which entry to invalidate. Detail comes from a follow-up REST call,
/// gated by the client's stale-while-revalidate logic.
#[derive(Clone, Debug, Serialize)]
struct StreamEvent {
    /// "run.created" / "run.updated" / "artifact.created"
    kind: &'static str,
    /// Entity id (run_id or artifact_id depending on kind).
    id: String,
}

pub fn serve(
    cluster: ClusterConfig,
    store: Store,
    addr: SocketAddr,
    no_dispatch: bool,
) -> Result<()> {
    // 256 is plenty — broadcast is for live deltas, not a queue. Slow
    // subscribers get lagged out and re-sync via REST on next focus.
    let (events_tx, _) = broadcast::channel::<StreamEvent>(256);
    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        cluster: Arc::new(cluster),
        events_tx: events_tx.clone(),
        dataset_cache: Arc::new(Mutex::new(HashMap::new())),
    };

    let api = Router::new()
        .route("/runs", get(list_runs))
        .route("/pipelines", get(list_pipelines))
        .route("/runs/:id", get(get_run))
        .route("/runs/:id/log", get(get_run_log))
        .route("/runs/:id/events", get(get_run_events))
        .route("/recipes/:name/history", get(get_recipe_history))
        .route("/recipes/:name", get(get_recipe))
        // Top-level so it doesn't collide with the /runs/:id route — matchit
        // disallows static + dynamic siblings on the same prefix.
        .route("/compare", get(compare_runs))
        .route("/pipelines/:id", get(get_pipeline))
        .route("/artifacts", get(list_artifacts))
        .route("/artifacts/:id", get(get_artifact))
        .route("/artifacts/:id/lineage", get(get_artifact_lineage))
        .route("/artifacts/:id/rollout", get(get_artifact_rollout))
        .route("/artifacts/:id/frames/:n", get(get_artifact_frame))
        .route("/artifacts/:id/dataset", get(get_artifact_dataset))
        .route(
            "/artifacts/:id/dataset/segments/:split/:seg",
            get(get_artifact_dataset_segment),
        )
        .route(
            "/artifacts/:id/dataset/frames/:split/:seg/:n",
            get(get_artifact_dataset_frame),
        )
        .route("/evals", get(list_evals))
        .route("/policies", get(list_policies))
        .route("/policies/:name", get(get_policy))
        .route("/cluster", get(get_cluster))
        .route("/stream", get(stream_handler));

    let tail_store = state.store.clone();
    let refresh_store = state.store.clone();
    let dispatch_cluster = state.cluster.clone();
    let dispatch_store = state.store.clone();
    let app = Router::new()
        .nest("/api", api)
        .fallback(static_handler)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    runtime.block_on(async move {
        // Background task: tail the in-process events table for SSE
        // subscribers. Since dispatch and the HTTP handlers share one
        // Store in this process, dispatch writes are immediately
        // visible — the tailer just queries the cache for new rows
        // since its last cursor.
        tokio::spawn(events_tailer(tail_store, events_tx));

        // Background task: re-walk the filesystem-truth registry on a
        // timer so the in-memory cache stays current with sidecars
        // written by out-of-process CLI invocations (`labctl run`,
        // `labctl run-pipeline`). Without this, the cache only learns
        // about runs submitted in-process and silently diverges from
        // disk between daemon restarts.
        tokio::spawn(crate::agent::periodic_refresh(refresh_store, Duration::from_secs(10)));

        let dispatch_shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
        if !no_dispatch {
            crate::agent::spawn(
                dispatch_cluster,
                dispatch_store,
                dispatch_shutdown.clone(),
            );
        } else {
            eprintln!("labctl: dispatch disabled by --no-dispatch");
        }

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;
        eprintln!("labctl listening on http://{addr}");
        let result = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("server error");
        // Notify dispatch tasks to wind down so the runtime can exit
        // cleanly. They each `select!` against this Notify.
        dispatch_shutdown.notify_waiters();
        result
    })?;

    Ok(())
}

async fn events_tailer(
    store: Arc<Mutex<Store>>,
    tx: broadcast::Sender<StreamEvent>,
) {
    // Start at the current tip so we don't replay the entire backlog
    // on every server restart. New events are appended to the cache
    // synchronously by dispatch writes (Store::event), so the tailer
    // just polls for ids strictly greater than its last cursor.
    let mut last_id: i64 = {
        let s = store.lock().unwrap();
        s.max_event_id().unwrap_or(0)
    };
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if tx.receiver_count() == 0 {
            continue;
        }
        let new_events = {
            let s = store.lock().unwrap();
            match s.events_after(last_id) {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("events_tailer: query failed: {e:#}");
                    continue;
                }
            }
        };
        for ev in new_events {
            last_id = last_id.max(ev.id);
            if let Some(out) = translate_event(&ev) {
                let _ = tx.send(out);
            }
        }
    }
}

/// Map an internal event-table row to a client-bound stream message. We
/// translate event types here so the wire format is stable even when the
/// internal `event_type` strings drift.
fn translate_event(ev: &crate::store::EventRow) -> Option<StreamEvent> {
    match ev.event_type.as_str() {
        "run_created" => ev.run_id.clone().map(|id| StreamEvent { kind: "run.created", id }),
        "run_submitted" | "run_status" => {
            ev.run_id.clone().map(|id| StreamEvent { kind: "run.updated", id })
        }
        "artifact_registered" => {
            let artifact_id = ev.payload.get("artifact_id")?.as_str()?.to_string();
            Some(StreamEvent { kind: "artifact.created", id: artifact_id })
        }
        _ => None,
    }
}

async fn stream_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|res| async move {
        // Lagged subscribers get a Lagged error from the broadcast stream.
        // Clients should resync on next focus — we just drop the lag here.
        let ev = res.ok()?;
        let payload = serde_json::to_string(&json!({ "id": ev.id })).ok()?;
        Some(Ok(Event::default().event(ev.kind).data(payload)))
    });
    // 15s keepalive is well under most idle-tunnel timeouts and below the
    // browser's EventSource default reconnect window.
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("\nshutting down");
}

// ---------- error helpers ----------

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": self.1 });
        (self.0, axum::Json(body)).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
    }
}

fn not_found(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, msg.into())
}

// ---------- run shaping ----------

/// Compact run shape for the list view. Avoids the hot fields that bloat
/// payload (recipe_json, context_json) — those load lazily on detail.
fn run_summary(r: &RunRow) -> Value {
    json!({
        "id": r.id,
        "recipe_name": r.recipe_name,
        "recipe_hash": r.recipe_hash,
        "status": r.status,
        "job_id": r.job_id,
        "run_dir": r.run_dir.display().to_string(),
        "repo": r.repo,
        "created_at": r.created_at,
        "finished_at": r.finished_at,
        "duration_secs": r.finished_at.map(|f| f.saturating_sub(r.created_at)),
        "pipeline_id": r.pipeline_id,
        "stage_name": r.stage_name,
        "submitted_by": r.submitted_by,
        "is_terminal": is_terminal(&r.status),
    })
}

fn run_full(r: &RunRow) -> Value {
    let mut v = run_summary(r);
    let map = v.as_object_mut().unwrap();
    map.insert("recipe".to_string(), r.recipe_json.clone());
    map.insert("context".to_string(), r.context_json.clone());
    map.insert(
        "dependency_on".to_string(),
        r.dependency_on.clone().unwrap_or(Value::Null),
    );
    map.insert(
        "source_path".to_string(),
        Value::String(r.source_path.display().to_string()),
    );
    v
}

fn artifact_summary(a: &ArtifactRow) -> Value {
    json!({
        "id": a.id,
        "kind": a.kind,
        "path": a.path.display().to_string(),
        "content_hash": a.content_hash,
        "producer_run_id": a.producer_run_id,
        "created_at": a.created_at,
    })
}

// ---------- handlers ----------

async fn list_runs(State(state): State<AppState>) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let runs: Vec<Value> = store.list_runs()?.iter().map(run_summary).collect();
    Ok(axum::Json(json!({ "runs": runs })))
}

async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let view = store.run_view(&id).map_err(|_| not_found(format!("run not found: {id}")))?;
    let inputs: Vec<Value> = view
        .inputs
        .iter()
        .map(|i| {
            json!({
                "role": i.role,
                "artifact_id": i.artifact_id,
                "resolved_path": i.resolved_path.display().to_string(),
            })
        })
        .collect();
    let outputs: Vec<Value> = view
        .outputs
        .iter()
        .map(|a| {
            let mut s = artifact_summary(a);
            let aliases: Vec<&String> = view
                .aliases
                .iter()
                .filter(|(_, aid)| aid == &a.id)
                .map(|(alias, _)| alias)
                .collect();
            let obj = s.as_object_mut().unwrap();
            obj.insert("aliases".into(), json!(aliases));
            // Inline `metadata.result` for eval_result outputs so the run
            // panel can render it without a follow-up artifact fetch.
            // Heavier metadata stays out of the summary path.
            if a.kind == "eval_result" {
                if let Some(result) = a.metadata_json.get("result") {
                    obj.insert("result".into(), result.clone());
                }
            }
            s
        })
        .collect();
    let tracking = match store.get_tracking(&view.run.id)? {
        Some(t) => json!({
            "wandb": {
                "entity": t.entity,
                "project": t.project,
                "url": t.url,
                "group": t.group_name,
                "source": t.source,
            }
        }),
        None => json!({ "wandb": Value::Null }),
    };
    let eval_series = build_eval_series(&store, &view.eval_requests);
    let body = json!({
        "run": run_full(&view.run),
        "inputs": inputs,
        "outputs": outputs,
        "eval_series": eval_series,
        "tracking": tracking,
    });
    Ok(axum::Json(body))
}

/// Group eval_requests by policy and shape them as a per-policy time
/// series across checkpoint step. Each point is a single eval; the series
/// is the trajectory you can chart.
///
/// Step comes from the checkpoint artifact's `metadata.step` (set by
/// `register_outputs` for `checkpoint_stream` outputs). Points without a
/// step still get included but with `step = null` — the UI sorts those to
/// the end.
fn build_eval_series(store: &Store, raw: &[Value]) -> Vec<Value> {
    use std::collections::BTreeMap;

    struct Point {
        step: Option<i64>,
        value: Option<Value>,
        metric_name: Option<String>,
        eval_run_id: Option<String>,
        checkpoint_artifact_id: String,
        state: String,
    }

    let mut by_policy: BTreeMap<String, Vec<Point>> = BTreeMap::new();

    for ev in raw {
        let policy = ev.get("policy_id").and_then(|v| v.as_str()).unwrap_or("");
        let state = ev.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let eval_run_id = ev
            .get("eval_run_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let checkpoint_artifact_id = ev
            .get("checkpoint_artifact_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Pull step out of the checkpoint artifact's metadata, when known.
        let step = store
            .get_artifact_optional(&checkpoint_artifact_id)
            .ok()
            .flatten()
            .and_then(|a| a.metadata_json.get("step").and_then(|v| v.as_i64()));

        // Pull the headline metric out of the eval run, when it has one.
        let (metric_name, value) = match eval_run_id.as_deref() {
            Some(rid) => match primary_metric_for_run(store, rid) {
                Some((m, v)) => (Some(m), Some(v)),
                None => (None, None),
            },
            None => (None, None),
        };

        by_policy.entry(policy.to_string()).or_default().push(Point {
            step,
            value,
            metric_name,
            eval_run_id,
            checkpoint_artifact_id,
            state,
        });
    }

    by_policy
        .into_iter()
        .map(|(policy, mut points)| {
            // Sort by step ascending; None goes last.
            points.sort_by(|a, b| match (a.step, b.step) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });
            // Headline metric is the most recent point's metric (last in
            // sorted order). Different points may report different metric
            // names if the eval recipe changed mid-flight; we just show
            // whichever the latest one reports.
            let latest = points.iter().rev().find(|p| p.value.is_some());
            let metric_name = latest.and_then(|p| p.metric_name.clone());
            let latest_value = latest.and_then(|p| p.value.clone());
            let latest_step = latest.and_then(|p| p.step);
            let prev_value = points
                .iter()
                .rev()
                .filter_map(|p| p.value.as_ref())
                .nth(1)
                .cloned();

            json!({
                "policy_id": policy,
                "metric_name": metric_name,
                "latest_value": latest_value,
                "latest_step": latest_step,
                "previous_value": prev_value,
                "count": points.len(),
                "points": points
                    .iter()
                    .map(|p| json!({
                        "step": p.step,
                        "value": p.value,
                        "metric_name": p.metric_name,
                        "eval_run_id": p.eval_run_id,
                        "checkpoint_artifact_id": p.checkpoint_artifact_id,
                        "state": p.state,
                    }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect()
}

/// All metric points emitted by a run's evals, flattened: one row per
/// (metric, eval, checkpoint). Drives the metric-pivot used by compare /
/// recipe views.
fn metric_points_for_run(store: &Store, run_id: &str) -> Vec<MetricPoint> {
    let raw = match store.eval_requests_for_run(run_id) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<MetricPoint> = Vec::new();
    for ev in raw {
        let policy_id = ev
            .get("policy_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let state = ev
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let eval_run_id = ev
            .get("eval_run_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let checkpoint_artifact_id = ev
            .get("checkpoint_artifact_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let step = store
            .get_artifact_optional(&checkpoint_artifact_id)
            .ok()
            .flatten()
            .and_then(|a| a.metadata_json.get("step").and_then(|v| v.as_i64()));

        // Find the eval_result artifact for this eval_run, pull all metrics.
        let metrics: Vec<(String, Value)> = match eval_run_id.as_deref() {
            Some(rid) => {
                let outputs = store.run_outputs(rid).unwrap_or_default();
                let mut m = Vec::new();
                for art in outputs {
                    if art.kind == "eval_result" {
                        if let Some(result) = art.metadata_json.get("result") {
                            m = extract_all_metrics(result);
                            break;
                        }
                    }
                }
                m
            }
            None => Vec::new(),
        };

        if metrics.is_empty() {
            // Still emit a marker row so the UI can show "eval pending"
            // for this checkpoint, keyed under a synthetic metric.
            out.push(MetricPoint {
                metric_name: String::new(), // empty = no metric yet
                step,
                value: None,
                eval_run_id: eval_run_id.clone(),
                checkpoint_artifact_id: checkpoint_artifact_id.clone(),
                state: state.clone(),
                policy_id: policy_id.clone(),
            });
        } else {
            for (name, value) in metrics {
                out.push(MetricPoint {
                    metric_name: name,
                    step,
                    value: Some(value),
                    eval_run_id: eval_run_id.clone(),
                    checkpoint_artifact_id: checkpoint_artifact_id.clone(),
                    state: state.clone(),
                    policy_id: policy_id.clone(),
                });
            }
        }
    }
    out
}

struct MetricPoint {
    metric_name: String,
    step: Option<i64>,
    value: Option<Value>,
    eval_run_id: Option<String>,
    checkpoint_artifact_id: String,
    state: String,
    policy_id: String,
}

/// Shared shape for compare and recipe views. One chart's worth of data
/// per metric; one trajectory per run inside each metric. Sort metrics
/// so the most "common" one (most runs measuring it) comes first — that
/// makes the natural default-selected metric the one users care about.
fn build_metric_pivot(store: &Store, runs: &[crate::store::RunRow]) -> Value {
    use std::collections::BTreeMap;

    // For each metric: run_id → Vec<(step, value, eval_run_id, ...)>
    type RunPoints = Vec<(Option<i64>, Value, Option<String>, String, String)>;
    let mut by_metric: BTreeMap<String, BTreeMap<String, RunPoints>> = BTreeMap::new();

    for run in runs {
        let points = metric_points_for_run(store, &run.id);
        for p in points {
            if p.metric_name.is_empty() || p.value.is_none() {
                continue; // pending or no metric yet — skip from the chart
            }
            by_metric
                .entry(p.metric_name)
                .or_default()
                .entry(run.id.clone())
                .or_default()
                .push((
                    p.step,
                    p.value.unwrap(),
                    p.eval_run_id,
                    p.state,
                    p.checkpoint_artifact_id,
                ));
        }
    }

    // Run summaries indexed by id for fast lookup as we shape the response.
    let mut run_summary_map: BTreeMap<String, Value> = BTreeMap::new();
    for r in runs {
        run_summary_map.insert(r.id.clone(), run_summary(r));
    }
    let mut run_meta_map: BTreeMap<String, (String, String, i64)> = BTreeMap::new();
    for r in runs {
        run_meta_map.insert(
            r.id.clone(),
            (r.recipe_name.clone(), r.status.clone(), r.created_at),
        );
    }

    // Reshape into the public response.
    let mut series: Vec<Value> = by_metric
        .into_iter()
        .map(|(metric, runs_map)| {
            let runs_vec: Vec<Value> = runs_map
                .into_iter()
                .map(|(run_id, mut pts)| {
                    pts.sort_by(|a, b| match (a.0, b.0) {
                        (Some(x), Some(y)) => x.cmp(&y),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    });
                    let (recipe, status, created) = run_meta_map
                        .get(&run_id)
                        .cloned()
                        .unwrap_or_else(|| (String::new(), String::new(), 0));
                    let latest = pts.last().cloned();
                    let prev = if pts.len() >= 2 {
                        pts.get(pts.len() - 2).cloned()
                    } else {
                        None
                    };
                    let count = pts.len();
                    json!({
                        "run_id": run_id,
                        "run_recipe_name": recipe,
                        "run_status": status,
                        "run_created_at": created,
                        "count": count,
                        "latest_value": latest.as_ref().map(|p| p.1.clone()).unwrap_or(Value::Null),
                        "latest_step": latest.as_ref().and_then(|p| p.0).map(|x| Value::from(x)).unwrap_or(Value::Null),
                        "previous_value": prev.as_ref().map(|p| p.1.clone()).unwrap_or(Value::Null),
                        "points": pts.into_iter().map(|(step, value, eval_run_id, state, ckpt_id)| json!({
                            "step": step,
                            "value": value,
                            "eval_run_id": eval_run_id,
                            "state": state,
                            "checkpoint_artifact_id": ckpt_id,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            json!({
                "metric_name": metric,
                "run_count": runs_vec.len(),
                "runs": runs_vec,
            })
        })
        .collect();

    // Sort metrics: most-common first (so the default chip lights up the
    // metric the user most likely wants), then preferred metric names
    // (strict_accuracy etc.), then alphabetical.
    series.sort_by(|a, b| {
        let na = a.get("run_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let nb = b.get("run_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let ma = a.get("metric_name").and_then(|v| v.as_str()).unwrap_or("");
        let mb = b.get("metric_name").and_then(|v| v.as_str()).unwrap_or("");
        nb.cmp(&na)
            .then_with(|| {
                let pa = is_preferred_metric_name(ma);
                let pb = is_preferred_metric_name(mb);
                pb.cmp(&pa) // true sorts first
            })
            .then_with(|| ma.cmp(mb))
    });

    let metrics: Vec<Value> = series
        .iter()
        .filter_map(|s| s.get("metric_name").cloned())
        .collect();
    let runs_summary: Vec<Value> = runs.iter().map(|r| run_summary(r)).collect();

    json!({
        "runs": runs_summary,
        "metrics": metrics,
        "series_by_metric": series,
    })
}

/// Look up an eval run's headline metric. Pattern-matches `metadata.result`
/// against several common eval-output shapes — anything that contains a
/// flat `{name: number}` dict in a known position is treated as metrics.
/// No coupling to a specific framework's schema; the recipe author writes
/// their natural format and labctl recognizes it.
fn primary_metric_for_run(store: &Store, run_id: &str) -> Option<(String, Value)> {
    let outputs = store.run_outputs(run_id).ok()?;
    for art in outputs {
        if art.kind != "eval_result" {
            continue;
        }
        let result = art.metadata_json.get("result")?;
        if let Some((name, value)) = first_metric(result) {
            return Some((name, value));
        }
    }
    None
}

/// Return the first metric `(name, value)` extractable from a result blob.
/// Tries (in order): an explicit `{tasks, primary}` shape, then any of the
/// common container fields (`tasks` / `scores` / `metrics` / `results`),
/// then the top-level itself if it's a flat metric dict.
fn first_metric(result: &Value) -> Option<(String, Value)> {
    let obj = result.as_object()?;

    // Explicit primary pin (the original convention — still honored).
    if let (Some(Value::String(primary)), Some(tasks)) = (obj.get("primary"), obj.get("tasks")) {
        if let Some(value) = metric_value_at(tasks, primary) {
            return Some((primary.clone(), value));
        }
    }

    // Look inside known container fields.
    for key in ["tasks", "scores", "metrics", "results"] {
        if let Some(inner) = obj.get(key) {
            if let Some(pair) = first_flat_metric(inner) {
                return Some(pair);
            }
        }
    }

    // Or the top-level itself.
    first_flat_metric(result)
}

/// Pull the numeric value at `dict[key]`. Accepts either a bare number or
/// the richer `{value: number, stderr?, n?}` form.
fn metric_value_at(dict: &Value, key: &str) -> Option<Value> {
    let entry = dict.as_object()?.get(key)?;
    if entry.is_number() {
        return Some(entry.clone());
    }
    entry.as_object()?.get("value").filter(|v| v.is_number()).cloned()
}

/// Take the headline metric from a flat dict. A "flat metric dict" is an
/// object whose values are either bare numbers or `{value: number, ...}`
/// objects. Mixed-type or nested-object dicts return None — those should
/// fall through to the JSON tree view.
///
/// Picks the most likely headline key when one matches (`strict_accuracy`,
/// `accuracy`, `acc`, `pass@1`, `exact_match`, `score`); otherwise the
/// first key. Keeps the server's inline-pill choice aligned with the
/// client's table-highlight choice.
fn first_flat_metric(value: &Value) -> Option<(String, Value)> {
    let obj = value.as_object()?;
    if obj.is_empty() {
        return None;
    }
    for v in obj.values() {
        if !is_metric_leaf(v) {
            return None;
        }
    }
    let preferred = obj.keys().find(|k| is_preferred_metric_name(k));
    let chosen_key = preferred.or_else(|| obj.keys().next())?;
    let v = obj.get(chosen_key)?;
    let num = if v.is_number() {
        v.clone()
    } else {
        v.as_object()?.get("value")?.clone()
    };
    Some((chosen_key.clone(), num))
}

/// Pull every metric out of a result blob — not just the primary. Same
/// structural rules as `first_metric` (a "metric" is a flat dict of
/// numbers or `{value: number, ...}` objects), but emits all of them so
/// the UI can offer a per-metric chip selector.
fn extract_all_metrics(result: &Value) -> Vec<(String, Value)> {
    fn flat_dict_metrics(val: &Value, out: &mut Vec<(String, Value)>) -> bool {
        let Some(obj) = val.as_object() else {
            return false;
        };
        if obj.is_empty() {
            return false;
        }
        for v in obj.values() {
            if !is_metric_leaf(v) {
                return false;
            }
        }
        for (k, v) in obj {
            let num = if v.is_number() {
                v.clone()
            } else {
                match v.as_object().and_then(|o| o.get("value")) {
                    Some(n) => n.clone(),
                    None => continue,
                }
            };
            out.push((k.clone(), num));
        }
        true
    }

    let mut out = Vec::new();
    let Some(obj) = result.as_object() else {
        return out;
    };

    // 1. Honor `{tasks, primary?}` first.
    if let Some(tasks) = obj.get("tasks") {
        if flat_dict_metrics(tasks, &mut out) {
            return out;
        }
    }
    // 2. Then common container fields, in order. First hit wins.
    for key in ["scores", "metrics", "results"] {
        if let Some(inner) = obj.get(key) {
            if flat_dict_metrics(inner, &mut out) {
                return out;
            }
        }
    }
    // 3. Or the top-level dict itself.
    flat_dict_metrics(result, &mut out);
    out
}

fn is_preferred_metric_name(key: &str) -> bool {
    // Match the segment after the last "/", case-insensitive. inspect-ai
    // emits keys like "ifeval/strict_accuracy"; the meaningful tail is
    // what we score on.
    let tail = key.rsplit('/').next().unwrap_or(key).to_ascii_lowercase();
    matches!(
        tail.as_str(),
        "strict_accuracy" | "accuracy" | "acc" | "pass@1" | "exact_match" | "score"
    )
}

fn is_metric_leaf(v: &Value) -> bool {
    if v.is_number() {
        return true;
    }
    let Some(o) = v.as_object() else { return false };
    o.get("value").is_some_and(|x| x.is_number())
}

#[derive(Deserialize)]
struct LogParams {
    /// Tail length in lines (default 200, max 5000).
    #[serde(default)]
    tail: Option<usize>,
}

async fn get_run_log(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<LogParams>,
) -> Result<axum::Json<Value>, ApiError> {
    let run = {
        let store = state.store.lock().unwrap();
        store.get_run(&id).map_err(|_| not_found(format!("run not found: {id}")))?
    };
    let tail = params.tail.unwrap_or(200).min(5000);
    let log = read_tail_log(&run.run_dir, tail);
    Ok(axum::Json(json!({
        "run_id": id,
        "lines": log.lines,
        "path": log.path.map(|p| p.display().to_string()),
        "truncated": log.truncated,
    })))
}

struct LogTail {
    lines: Vec<String>,
    path: Option<PathBuf>,
    truncated: bool,
}

fn read_tail_log(run_dir: &std::path::Path, tail: usize) -> LogTail {
    // SLURM writes to <run_dir>/.lab/<job_name>_<job_id>.log; pick the most
    // recently modified one. Falls back to .lab/status.json's stderr if no
    // log file (local scheduler / pre-submission state).
    let lab = run_dir.join(".lab");
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    if let Ok(entries) = std::fs::read_dir(&lab) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("log") {
                continue;
            }
            let mtime = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            match &newest {
                Some((best, _)) if best >= &mtime => {}
                _ => newest = Some((mtime, path)),
            }
        }
    }
    let Some((_, path)) = newest else {
        return LogTail { lines: vec![], path: None, truncated: false };
    };
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return LogTail { lines: vec![], path: Some(path), truncated: false },
    };
    let lines: Vec<&str> = content.lines().collect();
    let truncated = lines.len() > tail;
    let start = lines.len().saturating_sub(tail);
    let lines = lines[start..].iter().map(|s| s.to_string()).collect();
    LogTail { lines, path: Some(path), truncated }
}

async fn get_run_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let events = store.events_for_run(&id)?;
    Ok(axum::Json(json!({ "events": events })))
}

#[derive(Deserialize)]
struct HistoryParams {
    #[serde(default)]
    limit: Option<usize>,
}

async fn get_recipe_history(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HistoryParams>,
) -> Result<axum::Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(20).min(200);
    let store = state.store.lock().unwrap();
    let history: Vec<Value> = store
        .recipe_history(&name, limit)?
        .into_iter()
        .map(|(status, ts)| json!({ "status": status, "created_at": ts }))
        .collect();
    Ok(axum::Json(json!({ "recipe_name": name, "history": history })))
}

#[derive(Deserialize)]
struct CompareParams {
    /// Comma-separated run ids. Order is preserved in the response.
    ids: String,
}

/// Multi-run compare. Same response shape as the recipe view — same
/// frontend chart can render either. The difference: caller chooses
/// which runs to overlay (cross-recipe), instead of grouping by recipe.
async fn compare_runs(
    State(state): State<AppState>,
    Query(params): Query<CompareParams>,
) -> Result<axum::Json<Value>, ApiError> {
    let ids: Vec<String> = params
        .ids
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if ids.is_empty() {
        return Ok(axum::Json(json!({
            "runs": [],
            "metrics": [],
            "series_by_metric": [],
        })));
    }
    let store = state.store.lock().unwrap();
    let mut runs: Vec<crate::store::RunRow> = Vec::with_capacity(ids.len());
    for id in &ids {
        if let Ok(r) = store.get_run(id) {
            runs.push(r);
        }
    }
    Ok(axum::Json(build_metric_pivot(&store, &runs)))
}

/// Recipe view: every run of `name` plus eval_series transposed by policy
/// (one entry per policy, with one trajectory per run inside).
async fn get_recipe(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let runs = store.runs_by_recipe(&name)?;
    if runs.is_empty() {
        return Err(not_found(format!("recipe not found: {name}")));
    }
    let mut body = build_metric_pivot(&store, &runs);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("recipe_name".into(), Value::String(name));
    }
    Ok(axum::Json(body))
}

async fn list_pipelines(State(state): State<AppState>) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let pipelines = store.list_pipelines()?;
    let mut out = Vec::with_capacity(pipelines.len());
    for p in pipelines {
        let runs = store.list_pipeline_runs(&p.id)?;
        let stage_count = runs.len();
        let status = aggregate_pipeline_status(&runs);
        out.push(json!({
            "id": p.id,
            "name": p.name,
            "pipeline_path": p.pipeline_path.map(|p| p.display().to_string()),
            "created_at": p.created_at,
            "stage_count": stage_count,
            "status": status,
        }));
    }
    Ok(axum::Json(json!({ "pipelines": out })))
}

fn aggregate_pipeline_status(runs: &[RunRow]) -> &'static str {
    if runs.is_empty() {
        return "unknown";
    }
    if runs.iter().any(|r| r.status == "failed" || r.status == "oom" || r.status == "timeout") {
        return "failed";
    }
    if runs.iter().any(|r| !is_terminal(&r.status)) {
        return "running";
    }
    if runs.iter().all(|r| r.status == "succeeded") {
        return "succeeded";
    }
    "mixed"
}

async fn get_pipeline(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let pipeline = store
        .get_pipeline(&id)?
        .ok_or_else(|| not_found(format!("pipeline not found: {id}")))?;
    let runs = store.list_pipeline_runs(&id)?;
    let stages: Vec<Value> = runs
        .iter()
        .map(|r| {
            let mut v = run_summary(r);
            let m = v.as_object_mut().unwrap();
            m.insert(
                "dependency_on".into(),
                r.dependency_on.clone().unwrap_or(Value::Null),
            );
            v
        })
        .collect();
    Ok(axum::Json(json!({
        "pipeline": {
            "id": pipeline.id,
            "name": pipeline.name,
            "pipeline_path": pipeline.pipeline_path.map(|p| p.display().to_string()),
            "created_at": pipeline.created_at,
            "status": aggregate_pipeline_status(&runs),
        },
        "stages": stages,
    })))
}

async fn list_artifacts(State(state): State<AppState>) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let artifacts = store.list_artifacts()?;
    let mut out = Vec::with_capacity(artifacts.len());
    for a in artifacts {
        let aliases = store.aliases_for_artifact(&a.id)?;
        let mut s = artifact_summary(&a);
        s.as_object_mut().unwrap().insert("aliases".into(), json!(aliases));
        out.push(s);
    }
    Ok(axum::Json(json!({ "artifacts": out })))
}

async fn get_artifact(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let artifact = store
        .get_artifact_optional(&id)?
        .ok_or_else(|| not_found(format!("artifact not found: {id}")))?;
    let aliases = store.aliases_for_artifact(&id)?;
    let consumers = store.artifact_consumers(&id)?;
    let consumer_runs: Vec<Value> = consumers
        .into_iter()
        .filter_map(|(run_id, role)| {
            store.get_run(&run_id).ok().map(|r| {
                let mut v = run_summary(&r);
                v.as_object_mut()
                    .unwrap()
                    .insert("input_role".into(), Value::String(role));
                v
            })
        })
        .collect();
    let producer = artifact
        .producer_run_id
        .as_deref()
        .and_then(|rid| store.get_run(rid).ok())
        .map(|r| run_summary(&r));
    let mut s = artifact_summary(&artifact);
    let m = s.as_object_mut().unwrap();
    m.insert("aliases".into(), json!(aliases));
    m.insert("metadata".into(), artifact.metadata_json.clone());
    Ok(axum::Json(json!({
        "artifact": s,
        "producer": producer,
        "consumers": consumer_runs,
    })))
}

/// Bipartite lineage: artifacts and runs both as nodes, alternating.
/// Walks N hops upstream (producer chain) and downstream (consumer chain).
async fn get_artifact_lineage(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let max_hops = 6usize;
    let root = store
        .get_artifact_optional(&id)?
        .ok_or_else(|| not_found(format!("artifact not found: {id}")))?;

    let mut artifact_nodes: std::collections::BTreeMap<String, Value> = Default::default();
    let mut run_nodes: std::collections::BTreeMap<String, Value> = Default::default();
    let mut edges: Vec<Value> = Vec::new();

    let push_artifact = |a: &ArtifactRow,
                         nodes: &mut std::collections::BTreeMap<String, Value>,
                         is_root: bool| {
        let aliases = store.aliases_for_artifact(&a.id).unwrap_or_default();
        let mut s = artifact_summary(a);
        s.as_object_mut().unwrap().insert("aliases".into(), json!(aliases));
        s.as_object_mut().unwrap().insert("is_root".into(), json!(is_root));
        nodes.insert(a.id.clone(), s);
    };
    let push_run = |r: &RunRow, nodes: &mut std::collections::BTreeMap<String, Value>| {
        nodes.insert(r.id.clone(), run_summary(r));
    };

    push_artifact(&root, &mut artifact_nodes, true);

    // Upstream: artifact -> producer run -> input artifacts -> their producers ...
    let mut frontier: Vec<(String, usize)> = vec![(root.id.clone(), 0)];
    while let Some((aid, depth)) = frontier.pop() {
        if depth >= max_hops {
            continue;
        }
        let Ok(Some(a)) = store.get_artifact_optional(&aid) else { continue };
        let Some(prid) = a.producer_run_id else { continue };
        let Ok(prun) = store.get_run(&prid) else { continue };
        push_run(&prun, &mut run_nodes);
        edges.push(json!({ "from": prun.id, "to": a.id, "kind": "produces" }));
        let Ok(inputs) = store.run_inputs(&prun.id) else { continue };
        for input in inputs {
            if let Some(input_aid) = input.artifact_id {
                if let Ok(Some(input_a)) = store.get_artifact_optional(&input_aid) {
                    let new = !artifact_nodes.contains_key(&input_a.id);
                    push_artifact(&input_a, &mut artifact_nodes, false);
                    edges.push(json!({
                        "from": input_a.id,
                        "to": prun.id,
                        "kind": "consumed_by",
                        "role": input.role,
                    }));
                    if new {
                        frontier.push((input_a.id, depth + 1));
                    }
                }
            }
        }
    }

    // Downstream: artifact -> consumer runs -> their outputs -> their consumers ...
    let mut frontier: Vec<(String, usize)> = vec![(root.id.clone(), 0)];
    while let Some((aid, depth)) = frontier.pop() {
        if depth >= max_hops {
            continue;
        }
        let consumers = store.artifact_consumers(&aid).unwrap_or_default();
        for (run_id, role) in consumers {
            let Ok(crun) = store.get_run(&run_id) else { continue };
            let new_run = !run_nodes.contains_key(&crun.id);
            push_run(&crun, &mut run_nodes);
            edges.push(json!({
                "from": aid,
                "to": crun.id,
                "kind": "consumed_by",
                "role": role,
            }));
            if !new_run {
                continue;
            }
            let Ok(outputs) = store.run_outputs(&crun.id) else { continue };
            for out in outputs {
                let new = !artifact_nodes.contains_key(&out.id);
                push_artifact(&out, &mut artifact_nodes, false);
                edges.push(json!({ "from": crun.id, "to": out.id, "kind": "produces" }));
                if new {
                    frontier.push((out.id, depth + 1));
                }
            }
        }
    }

    Ok(axum::Json(json!({
        "root_id": root.id,
        "artifacts": artifact_nodes.values().collect::<Vec<_>>(),
        "runs": run_nodes.values().collect::<Vec<_>>(),
        "edges": edges,
    })))
}

async fn list_evals(State(state): State<AppState>) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let evals = store.list_eval_requests()?;
    Ok(axum::Json(json!({ "evals": evals })))
}

/// One row per distinct policy with aggregate counts and a small sparkline
/// trajectory for each of the most recent training runs that ran under
/// this policy. Sparkline points carry just (step, value) of the policy's
/// most-common metric — enough to draw the row at a glance without
/// fetching the full detail.
async fn list_policies(State(state): State<AppState>) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let summaries = store.policy_summaries()?;
    let mut out: Vec<Value> = Vec::with_capacity(summaries.len());
    for summary in summaries {
        let card = build_policy_card(&store, &summary)?;
        out.push(card);
    }
    Ok(axum::Json(json!({ "policies": out })))
}

/// Policy detail: every training run that has eval data for this policy,
/// pivoted by metric the same way the recipe/compare views are pivoted.
/// Plus the raw eval_requests list for the activity drawer at the bottom
/// of the page.
async fn get_policy(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let store = state.store.lock().unwrap();
    let requests = store.eval_requests_by_policy(&name)?;
    if requests.is_empty() {
        return Err(not_found(format!("policy not found: {name}")));
    }

    // Distinct training runs that produced the checkpoints referenced by
    // these requests. Order by created_at desc so the leaderboard and the
    // chart's legend feel consistent.
    let mut run_ids: Vec<String> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for req in &requests {
        let Some(ckpt_id) = req.get("checkpoint_artifact_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(producer) = store
            .get_artifact_optional(ckpt_id)
            .ok()
            .flatten()
            .and_then(|a| a.producer_run_id)
        else {
            continue;
        };
        if seen.insert(producer.clone()) {
            run_ids.push(producer);
        }
    }
    let mut runs: Vec<crate::store::RunRow> = Vec::with_capacity(run_ids.len());
    for id in &run_ids {
        if let Ok(r) = store.get_run(id) {
            runs.push(r);
        }
    }
    runs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let mut body = build_metric_pivot_for_policy(&store, &runs, &name);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("policy_name".into(), Value::String(name));
        obj.insert("requests".into(), Value::Array(requests));
    }
    Ok(axum::Json(body))
}

/// Header card for the policies list. Picks the most-common metric across
/// this policy's eval runs as the headline and emits up to N sparkline
/// trajectories (one per recent training run).
fn build_policy_card(
    store: &Store,
    summary: &crate::store::PolicySummaryRow,
) -> Result<Value, ApiError> {
    const SPARK_RUNS: usize = 4;

    let requests = store.eval_requests_by_policy(&summary.name)?;

    // Group eval_requests by the producer run of the checkpoint. We only
    // need recent producers; cap at SPARK_RUNS distinct most-recent ones.
    let mut producer_order: Vec<String> = Vec::new();
    let mut producer_seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for req in &requests {
        let Some(ckpt_id) = req.get("checkpoint_artifact_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(producer) = store
            .get_artifact_optional(ckpt_id)
            .ok()
            .flatten()
            .and_then(|a| a.producer_run_id)
        else {
            continue;
        };
        if producer_seen.insert(producer.clone()) {
            producer_order.push(producer);
        }
    }

    // Sort producer runs by created_at desc, keep the top N.
    let mut runs: Vec<crate::store::RunRow> = producer_order
        .iter()
        .filter_map(|id| store.get_run(id).ok())
        .collect();
    runs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    runs.truncate(SPARK_RUNS);

    // Pivot to find the policy's primary metric (most-common across recent
    // runs) — sparklines render only that metric so all rows are comparable.
    let pivot = build_metric_pivot(store, &runs);
    let primary_metric = pivot
        .get("metrics")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let primary_series = match primary_metric.as_deref() {
        Some(metric) => pivot
            .get("series_by_metric")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                arr.iter().find(|s| {
                    s.get("metric_name").and_then(|m| m.as_str()) == Some(metric)
                })
            })
            .cloned()
            .unwrap_or(Value::Null),
        None => Value::Null,
    };

    Ok(json!({
        "name": summary.name,
        "primary_metric": primary_metric,
        "total_count": summary.total,
        "failed_count": summary.failed,
        "running_count": summary.running,
        "last_fired_at": summary.last_fired_at,
        "series": primary_series,
    }))
}

/// Like `build_metric_pivot` but restricted to metric points that came
/// from eval runs whose `policy_id` matches `policy`. Cross-policy noise
/// on the same training runs is filtered out so the chart for `mmlu`
/// doesn't accidentally include points emitted by an `ifeval` policy.
fn build_metric_pivot_for_policy(
    store: &Store,
    runs: &[crate::store::RunRow],
    policy: &str,
) -> Value {
    use std::collections::BTreeMap;

    type RunPoints = Vec<(Option<i64>, Value, Option<String>, String, String)>;
    let mut by_metric: BTreeMap<String, BTreeMap<String, RunPoints>> = BTreeMap::new();

    for run in runs {
        let points = metric_points_for_run(store, &run.id);
        for p in points {
            if p.metric_name.is_empty() || p.value.is_none() {
                continue;
            }
            if p.policy_id != policy {
                continue;
            }
            by_metric
                .entry(p.metric_name)
                .or_default()
                .entry(run.id.clone())
                .or_default()
                .push((
                    p.step,
                    p.value.unwrap(),
                    p.eval_run_id,
                    p.state,
                    p.checkpoint_artifact_id,
                ));
        }
    }

    let mut run_meta_map: BTreeMap<String, (String, String, i64)> = BTreeMap::new();
    for r in runs {
        run_meta_map.insert(
            r.id.clone(),
            (r.recipe_name.clone(), r.status.clone(), r.created_at),
        );
    }

    let mut series: Vec<Value> = by_metric
        .into_iter()
        .map(|(metric, runs_map)| {
            let runs_vec: Vec<Value> = runs_map
                .into_iter()
                .map(|(run_id, mut pts)| {
                    pts.sort_by(|a, b| match (a.0, b.0) {
                        (Some(x), Some(y)) => x.cmp(&y),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    });
                    let (recipe, status, created) = run_meta_map
                        .get(&run_id)
                        .cloned()
                        .unwrap_or_else(|| (String::new(), String::new(), 0));
                    let latest = pts.last().cloned();
                    let prev = if pts.len() >= 2 {
                        pts.get(pts.len() - 2).cloned()
                    } else {
                        None
                    };
                    let count = pts.len();
                    json!({
                        "run_id": run_id,
                        "run_recipe_name": recipe,
                        "run_status": status,
                        "run_created_at": created,
                        "count": count,
                        "latest_value": latest.as_ref().map(|p| p.1.clone()).unwrap_or(Value::Null),
                        "latest_step": latest.as_ref().and_then(|p| p.0).map(Value::from).unwrap_or(Value::Null),
                        "previous_value": prev.as_ref().map(|p| p.1.clone()).unwrap_or(Value::Null),
                        "points": pts.into_iter().map(|(step, value, eval_run_id, state, ckpt_id)| json!({
                            "step": step,
                            "value": value,
                            "eval_run_id": eval_run_id,
                            "state": state,
                            "checkpoint_artifact_id": ckpt_id,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            json!({
                "metric_name": metric,
                "run_count": runs_vec.len(),
                "runs": runs_vec,
            })
        })
        .collect();

    series.sort_by(|a, b| {
        let na = a.get("run_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let nb = b.get("run_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let ma = a.get("metric_name").and_then(|v| v.as_str()).unwrap_or("");
        let mb = b.get("metric_name").and_then(|v| v.as_str()).unwrap_or("");
        nb.cmp(&na)
            .then_with(|| {
                let pa = is_preferred_metric_name(ma);
                let pb = is_preferred_metric_name(mb);
                pb.cmp(&pa)
            })
            .then_with(|| ma.cmp(mb))
    });

    let metrics: Vec<Value> = series
        .iter()
        .filter_map(|s| s.get("metric_name").cloned())
        .collect();
    let runs_summary: Vec<Value> = runs.iter().map(run_summary).collect();

    json!({
        "runs": runs_summary,
        "metrics": metrics,
        "series_by_metric": series,
    })
}

async fn get_cluster(State(state): State<AppState>) -> axum::Json<Value> {
    axum::Json(json!({
        "name": state.cluster.name,
        "runs_base": state.cluster.filesystem.runs_base.display().to_string(),
    }))
}

// ---------- rollout viewer ----------

/// Return parsed traj.jsonl + frame count for an eval_result artifact that
/// recorded a GUI rollout. The artifact's metadata.result must contain
/// `traj_path` (absolute path to traj.jsonl) written by the runner.
async fn get_artifact_rollout(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let artifact = {
        let store = state.store.lock().unwrap();
        store
            .get_artifact_optional(&id)?
            .ok_or_else(|| not_found(format!("artifact not found: {id}")))?
    };

    let traj_path_str = artifact
        .metadata_json
        .get("result")
        .and_then(|r| r.get("traj_path"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| not_found("artifact has no traj_path in result".to_string()))?
        .to_string();

    let traj_path = std::path::Path::new(&traj_path_str);
    let steps_dir = traj_path
        .parent()
        .ok_or_else(|| not_found("invalid traj_path: no parent dir".to_string()))?
        .join("steps");

    let content = std::fs::read_to_string(traj_path).map_err(|e| {
        ApiError(
            StatusCode::NOT_FOUND,
            format!("traj.jsonl not readable: {e}"),
        )
    })?;

    let steps: Vec<Value> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();

    let frame_count: u32 = std::fs::read_dir(&steps_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "png"))
                .count() as u32
        })
        .unwrap_or(0);

    Ok(axum::Json(json!({
        "steps": steps,
        "frame_count": frame_count,
    })))
}

/// Serve step_NNN.png for a GUI rollout artifact. Frame index N is
/// zero-based and must match a file written by the runner.
async fn get_artifact_frame(
    State(state): State<AppState>,
    Path((id, n)): Path<(String, u32)>,
) -> Result<Response, ApiError> {
    let artifact = {
        let store = state.store.lock().unwrap();
        store
            .get_artifact_optional(&id)?
            .ok_or_else(|| not_found(format!("artifact not found: {id}")))?
    };

    let traj_path_str = artifact
        .metadata_json
        .get("result")
        .and_then(|r| r.get("traj_path"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| not_found("artifact has no traj_path in result".to_string()))?
        .to_string();

    let steps_dir = std::path::Path::new(&traj_path_str)
        .parent()
        .ok_or_else(|| not_found("invalid traj_path: no parent dir".to_string()))?
        .join("steps");

    let frame_path = steps_dir.join(format!("step_{n:03}.png"));

    let bytes = std::fs::read(&frame_path)
        .map_err(|_| not_found(format!("frame {n} not found at {}", frame_path.display())))?;

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "image/png")
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(axum::body::Body::from(bytes))
        .unwrap())
}

// ---------- dataset explorer ----------
//
// Crowd-cast SFT datasets land on disk as
//   <artifact.path>/<split>/<segment_id>/{frames/frame_<N>.jpg, meta.json,
//                                        chat_line.json}
// for splits in {train, val, test}. Stage A and Stage B emit this shape;
// Stage C/D outputs are opaque Grain shards and are detected as
// "not browseable" by feature-detecting the per-segment meta.json files.

#[derive(Clone, Debug, Serialize)]
struct DatasetSegment {
    split: String,
    segment_id: String,
    contributor_hash: String,
    n_frames: u32,
    n_no_op: u32,
    frame_width: u32,
    frame_height: u32,
    target_fps: u32,
    /// ISO8601 UTC, or empty when meta.json had no creation_time.
    creation_time: String,
    /// Selected counts pulled out of meta.json's `stats` block. Anything
    /// the explorer's tables/sidebars use sits here so the client doesn't
    /// re-derive from the raw blob.
    stats: DatasetSegmentStats,
}

#[derive(Clone, Debug, Default, Serialize)]
struct DatasetSegmentStats {
    n_keypress: u32,
    n_keyrelease: u32,
    n_mousepress: u32,
    n_mouserelease: u32,
    n_mousemove: u32,
    n_scroll: u32,
    n_context_changed: u32,
    n_dangling_release: u32,
    n_held_at_end: u32,
    max_simultaneous_keys: u32,
}

#[derive(Clone, Debug, Serialize)]
struct DatasetSummary {
    splits: Vec<String>,
    n_segments: u32,
    n_contributors: u32,
    total_hours: f64,
    /// (earliest, latest) creation_time as ISO dates; empty strings if no
    /// segment had a parseable timestamp.
    date_range: (String, String),
    /// Flat list. The client groups by contributor/day on demand — keeps
    /// the wire format simple and lets us add more facets without a
    /// schema change.
    segments: Vec<DatasetSegment>,
}

fn parse_segment_meta(split: &str, meta_path: &std::path::Path) -> Option<DatasetSegment> {
    let raw = std::fs::read(meta_path).ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    let segment_id = v.get("segment_id")?.as_str()?.to_string();
    let contributor_hash = v
        .get("contributor_hash")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let n_frames = v.get("n_frames").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
    let n_no_op = v.get("n_no_op").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
    let frame_width = v
        .get("frame_width")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    let frame_height = v
        .get("frame_height")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    let target_fps = v
        .get("target_fps")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    let creation_time = v
        .get("creation_time")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();

    let s = v.get("stats").and_then(|x| x.as_object());
    let pick = |k: &str| -> u32 {
        s.and_then(|m| m.get(k))
            .and_then(|x| x.as_u64())
            .unwrap_or(0) as u32
    };
    let stats = DatasetSegmentStats {
        n_keypress: pick("n_keypress"),
        n_keyrelease: pick("n_keyrelease"),
        n_mousepress: pick("n_mousepress"),
        n_mouserelease: pick("n_mouserelease"),
        n_mousemove: pick("n_mousemove"),
        n_scroll: pick("n_scroll"),
        n_context_changed: pick("n_context_changed"),
        n_dangling_release: pick("n_dangling_release"),
        n_held_at_end: pick("n_held_at_end"),
        max_simultaneous_keys: pick("max_simultaneous_keys"),
    };

    Some(DatasetSegment {
        split: split.to_string(),
        segment_id,
        contributor_hash,
        n_frames,
        n_no_op,
        frame_width,
        frame_height,
        target_fps,
        creation_time,
        stats,
    })
}

/// Walk `<root>/{train,val,test}/<seg>/meta.json`. Returns `None` when no
/// meta.json is found at the expected depth — that is the signal for
/// "this dataset artifact isn't a browseable per-segment dataset" (e.g.
/// Stage C/D outputs). Bounded depth: we don't recurse into `frames/`.
fn walk_dataset(root: &std::path::Path) -> Option<DatasetSummary> {
    let mut segments: Vec<DatasetSegment> = Vec::new();
    let mut splits_present: Vec<String> = Vec::new();
    for split in &["train", "val", "test"] {
        let split_dir = root.join(split);
        if !split_dir.is_dir() {
            continue;
        }
        splits_present.push((*split).to_string());
        let entries = match std::fs::read_dir(&split_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for ent in entries.flatten() {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            let meta = p.join("meta.json");
            if !meta.is_file() {
                continue;
            }
            if let Some(seg) = parse_segment_meta(split, &meta) {
                segments.push(seg);
            }
        }
    }
    if segments.is_empty() {
        return None;
    }

    let mut contributors = std::collections::BTreeSet::new();
    let mut total_seconds: f64 = 0.0;
    let mut earliest = String::new();
    let mut latest = String::new();
    for s in &segments {
        if !s.contributor_hash.is_empty() {
            contributors.insert(s.contributor_hash.clone());
        }
        if s.target_fps > 0 {
            total_seconds += f64::from(s.n_frames) / f64::from(s.target_fps);
        }
        if !s.creation_time.is_empty() {
            if earliest.is_empty() || s.creation_time < earliest {
                earliest = s.creation_time.clone();
            }
            if s.creation_time > latest {
                latest = s.creation_time.clone();
            }
        }
    }
    let n_segments = segments.len() as u32;
    let n_contributors = contributors.len() as u32;

    Some(DatasetSummary {
        splits: splits_present,
        n_segments,
        n_contributors,
        total_hours: total_seconds / 3600.0,
        date_range: (earliest, latest),
        segments,
    })
}

/// Build (or fetch from cache) the dataset summary for an artifact, and
/// at the same time return the on-disk root path. Returns `None` if the
/// artifact exists but isn't a browseable per-segment dataset.
fn dataset_summary_for(
    state: &AppState,
    id: &str,
) -> Result<Option<(PathBuf, Arc<Option<DatasetSummary>>)>, ApiError> {
    let artifact = {
        let store = state.store.lock().unwrap();
        match store.get_artifact_optional(id)? {
            Some(a) => a,
            None => return Ok(None),
        }
    };
    if artifact.kind != "dataset" {
        return Ok(Some((artifact.path.clone(), Arc::new(None))));
    }

    {
        let cache = state.dataset_cache.lock().unwrap();
        if let Some(entry) = cache.get(id) {
            return Ok(Some((artifact.path.clone(), entry.clone())));
        }
    }
    // Walk outside the lock — a cold scan can take a few hundred ms on a
    // dataset with thousands of segments, no point blocking concurrent
    // requests for unrelated artifacts.
    let summary = walk_dataset(&artifact.path);
    let entry = Arc::new(summary);
    {
        let mut cache = state.dataset_cache.lock().unwrap();
        cache.insert(id.to_string(), entry.clone());
    }
    Ok(Some((artifact.path, entry)))
}

async fn get_artifact_dataset(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<axum::Json<Value>, ApiError> {
    let (_, entry) = dataset_summary_for(&state, &id)?
        .ok_or_else(|| not_found(format!("artifact not found: {id}")))?;
    match entry.as_ref() {
        Some(summary) => Ok(axum::Json(serde_json::to_value(summary).unwrap())),
        None => Err(not_found(
            "artifact has no browseable per-segment dataset layout".to_string(),
        )),
    }
}

async fn get_artifact_dataset_segment(
    State(state): State<AppState>,
    Path((id, split, seg)): Path<(String, String, String)>,
) -> Result<axum::Json<Value>, ApiError> {
    let (root, _) = dataset_summary_for(&state, &id)?
        .ok_or_else(|| not_found(format!("artifact not found: {id}")))?;
    let seg_dir = root.join(&split).join(&seg);
    let meta_path = seg_dir.join("meta.json");
    let chat_path = seg_dir.join("chat_line.json");

    let meta_bytes = std::fs::read(&meta_path).map_err(|e| {
        not_found(format!(
            "segment meta.json not readable: {} ({e})",
            meta_path.display()
        ))
    })?;
    let meta: Value = serde_json::from_slice(&meta_bytes)
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid meta.json: {e}")))?;

    // Extract action strings from chat_line.json. Stage A writes:
    //   {"messages": [
    //      {"role": "user",      "content": [{"type":"image","image":"<path>"}]},
    //      {"role": "assistant", "content": [{"type":"text","text":"<action>"}]},
    //      ...repeats once per frame...
    //   ]}
    // The action string lives on the assistant turn; one assistant turn
    // per frame, in source order.
    let actions: Vec<String> = std::fs::read(&chat_path)
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .map(|chat| {
            chat.get("messages")
                .and_then(|x| x.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("assistant"))
                        .map(|m| {
                            let c = m.get("content");
                            // content may be a string (post-flatten) or a
                            // block list with one or more {"type":"text",
                            // "text": "<action>"} entries.
                            if let Some(s) = c.and_then(|x| x.as_str()) {
                                return s.to_string();
                            }
                            if let Some(blocks) = c.and_then(|x| x.as_array()) {
                                for b in blocks {
                                    if b.get("type").and_then(|t| t.as_str()) == Some("text") {
                                        if let Some(t) = b.get("text").and_then(|x| x.as_str()) {
                                            return t.to_string();
                                        }
                                    }
                                }
                            }
                            String::new()
                        })
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();

    Ok(axum::Json(json!({
        "split": split,
        "segment_id": seg,
        "meta": meta,
        "actions": actions,
    })))
}

async fn get_artifact_dataset_frame(
    State(state): State<AppState>,
    Path((id, split, seg, n)): Path<(String, String, String, u32)>,
) -> Result<Response, ApiError> {
    // We resolve the artifact root via dataset_summary_for so the path
    // goes through the same artifact-exists/kind check the other dataset
    // endpoints use, and the cache walk fires once at first frame fetch
    // even before the summary endpoint is hit.
    let (root, _) = dataset_summary_for(&state, &id)?
        .ok_or_else(|| not_found(format!("artifact not found: {id}")))?;

    // Stage A writes frames as 6-digit-zero-padded JPEG filenames.
    let frame_path = root
        .join(&split)
        .join(&seg)
        .join("frames")
        .join(format!("frame_{n:06}.jpg"));
    let bytes = std::fs::read(&frame_path)
        .map_err(|_| not_found(format!("frame {n} not found at {}", frame_path.display())))?;

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "image/jpeg")
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(axum::body::Body::from(bytes))
        .unwrap())
}

// ---------- static SPA ----------

async fn static_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let candidate = if path.is_empty() { "index.html" } else { path };

    if let Some(asset) = Assets::get(candidate) {
        return serve_asset(candidate, asset);
    }

    // SPA fallback: any unmatched route returns index.html so client routing works.
    if let Some(asset) = Assets::get("index.html") {
        return serve_asset("index.html", asset);
    }
    (StatusCode::NOT_FOUND, "ui assets not embedded").into_response()
}

fn serve_asset(path: &str, asset: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let cache = if path == "index.html" {
        "no-store"
    } else {
        // Vite emits hashed filenames for everything except index.html, so
        // immutable+long is safe.
        "public, max-age=31536000, immutable"
    };
    Response::builder()
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, cache)
        .body(axum::body::Body::from(asset.data.into_owned()))
        .unwrap()
}
