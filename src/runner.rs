use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::{ClusterConfig, InputSpec, LoadedPipeline, LoadedStage, Recipe, SchedulerKind},
    fs_layout, provenance,
    store::{InputResolution, NewRun, Store, is_terminal},
    template::{RenderContext, render_value},
    util,
};

/// Fully resolved output specification: kind + rendered alias + final path.
/// Stored in the run's ``context.json`` so reconciliation/registration can
/// recover the artifact location without re-running the alias template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputResolution {
    pub role: String,
    pub kind: String,
    pub alias: String,
    pub marker: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct SubmitOverrides {
    pub input_artifacts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittedRun {
    pub run_id: String,
    pub job_id: String,
    pub run_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconcileReport {
    pub runs_reconciled: usize,
    pub artifacts_registered: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct StatusFile {
    status: String,
    #[allow(dead_code)]
    job_id: Option<String>,
    /// Wall-clock unix timestamp of the last write_status call inside the
    /// sbatch wrapper. For terminal transitions this is when the recipe
    /// command actually exited (not when reconcile observed it), so we
    /// prefer it over now_ts() when sacct doesn't return a usable End.
    updated_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct SchedulerOutcome {
    status: String,
    finished_at: Option<i64>,
}

pub fn submit_recipe(
    cluster: &ClusterConfig,
    store: &mut Store,
    recipe: &Recipe,
    overrides: Option<SubmitOverrides>,
    submitted_by: &str,
) -> Result<SubmittedRun> {
    fs_layout::validate_user(submitted_by)?;
    submit_recipe_inner(cluster, store, recipe, overrides, None, &[], None, submitted_by, None)
}

#[derive(Debug, Clone)]
pub struct StageContext<'a> {
    pub stage_name: &'a str,
    pub stage_run_ids: &'a BTreeMap<String, String>,
    /// Loaded recipes for every stage in the current pipeline. Required so
    /// downstream stages can compute the path of an upstream stage's
    /// output: ``output_roots[upstream.kind] / rendered(upstream.alias)``.
    pub stages: &'a BTreeMap<String, LoadedStage>,
}

fn submit_recipe_inner(
    cluster: &ClusterConfig,
    store: &mut Store,
    recipe: &Recipe,
    overrides: Option<SubmitOverrides>,
    stage_ctx: Option<&StageContext<'_>>,
    parent_job_ids: &[String],
    preallocated_run_id: Option<&str>,
    submitted_by: &str,
    array_sweep: Option<&ArraySweepInfo>,
) -> Result<SubmittedRun> {
    let overrides = overrides.unwrap_or_default();
    let run_id = preallocated_run_id
        .map(|s| s.to_string())
        .unwrap_or_else(|| util::new_id("run"));
    let run_dir = fs_layout::run_dir(&cluster.filesystem.runs_base, submitted_by, &run_id);
    let lab_dir = run_dir.join(fs_layout::LAB_DIRNAME);
    let source_root = run_dir.join("source");
    let source_path = source_root.join(&recipe.repo);

    fs::create_dir_all(&lab_dir)?;

    // Inputs first: alias templates may reference {inputs.X.path}.
    let inputs = resolve_inputs(cluster, store, recipe, &overrides, stage_ctx, submitted_by)?;

    // Render output aliases against a partial context (no outputs yet — they
    // are what we are computing). An alias that references {outputs.*} would
    // be a cycle and is rejected by render_value's unresolved-token check.
    let empty_outputs: BTreeMap<String, PathBuf> = BTreeMap::new();
    let alias_ctx = RenderContext {
        run_id: &run_id,
        run_dir: &run_dir,
        params: &recipe.params,
        inputs: &inputs,
        outputs: &empty_outputs,
    };
    let outputs = output_paths(cluster, recipe, &alias_ctx, submitted_by)?;

    // Materialize each output's directory so the recipe's command can write
    // into it without having to mkdir -p itself. Refuse only if the marker
    // file is already present — that's the explicit success signal of a
    // prior run. An empty or partially-populated directory (failed run, or
    // labctl-pre-created shell) is safe to reuse: re-submission of a failed
    // recipe should "just work" without forcing the operator to rm output
    // dirs by hand. For checkpoint_stream outputs, the marker lives one
    // step deeper (under <stream>/<step>/<marker>), so the top-level
    // directory existing means nothing — let it through.
    for resolution in outputs.values() {
        if resolution.kind != "checkpoint_stream" {
            let marker_path = resolution.path.join(&resolution.marker);
            if marker_path.exists() {
                bail!(
                    "output marker already present: {} (role={:?}, alias={:?}, kind={:?}). \
                     Refusing to overwrite a completed prior artifact. Delete the path \
                     explicitly or template the alias with {{run.id}} for per-submission \
                     uniqueness.",
                    marker_path.display(),
                    resolution.role,
                    resolution.alias,
                    resolution.kind,
                );
            }
        }
        fs::create_dir_all(&resolution.path).with_context(|| {
            format!(
                "failed to create output dir {} for role {:?}",
                resolution.path.display(),
                resolution.role,
            )
        })?;
    }

    // Paths-only view used for {outputs.X.path} substitution in [args] and
    // by render_script. The richer OutputResolution map is what we persist
    // in context.json for register_outputs to read at reconcile time.
    let output_paths_map: BTreeMap<String, PathBuf> = outputs
        .iter()
        .map(|(role, res)| (role.clone(), res.path.clone()))
        .collect();

    let repo_path = cluster
        .repos
        .get(&recipe.repo)
        .with_context(|| format!("cluster has no repo mapping for {:?}", recipe.repo))?;

    let recipe_hash = util::sha256_bytes(&serde_json::to_vec(recipe)?);
    let provenance_dir = lab_dir.join("provenance").join(&recipe.repo);
    let repo_provenance = provenance::capture_repo(repo_path, &provenance_dir)?;

    util::copy_dir_filtered(repo_path, &source_path).with_context(|| {
        format!(
            "failed to create execution snapshot {} from {}",
            source_path.display(),
            repo_path.display()
        )
    })?;

    let source_hash = util::dir_content_hash(&source_path)?;
    let ctx = json!({
        "schema_version": 1,
        "run_id": run_id,
        "run_dir": run_dir,
        "recipe_name": recipe.name,
        "recipe_hash": recipe_hash,
        "repo": recipe.repo,
        "source_path": source_path,
        "source_hash": source_hash,
        "inputs": inputs,
        "outputs": outputs,
        "params": recipe.params,
        "provenance": repo_provenance,
        "stage_name": stage_ctx.map(|c| c.stage_name),
        "parent_job_ids": parent_job_ids,
    });
    util::atomic_write(
        &lab_dir.join(fs_layout::CONTEXT_JSON),
        &serde_json::to_vec_pretty(&ctx)?,
    )?;

    let script = render_script(
        cluster,
        recipe,
        &run_id,
        &run_dir,
        &source_path,
        &inputs,
        &output_paths_map,
        parent_job_ids,
        array_sweep,
    )?;
    let script_path = lab_dir.join(fs_layout::SUBMIT_SH);
    util::atomic_write(&script_path, script.as_bytes())?;

    store.insert_run(
        NewRun {
            id: &run_id,
            recipe,
            recipe_hash: &recipe_hash,
            status: "created",
            run_dir: &run_dir,
            source_path: &source_path,
            context_json: &ctx,
            submitted_by: Some(submitted_by),
        },
        &inputs,
    )?;

    // Tracker row written at submission time. URL is fully derivable here
    // because we've forced WANDB_RUN_ID = labctl run id in the sbatch env.
    if let Some(wandb) = &recipe.tracking.wandb {
        let url = format!("https://wandb.ai/{}/{}/runs/{}", wandb.entity, wandb.project, run_id);
        store.set_tracking(
            &run_id,
            &wandb.entity,
            &wandb.project,
            &url,
            wandb.group.as_deref(),
            "schema",
        )?;
    }

    let job_id = submit_script(cluster, &script_path, &run_id)?;
    store.set_submitted(&run_id, &job_id)?;

    Ok(SubmittedRun {
        run_id,
        job_id,
        run_dir,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittedPipeline {
    pub pipeline_id: String,
    pub name: String,
    pub stages: Vec<SubmittedStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittedStage {
    pub stage_name: String,
    pub run_id: String,
    pub job_id: String,
    pub depends_on: Vec<String>,
}

/// Submitted result for a sweep — the single array job plus the
/// optional aggregate run (if `[sweep].aggregate` was set).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittedSweep {
    pub sweep_name: String,
    /// The single SLURM array job representing all sweep tasks.
    pub array_run: SubmittedRun,
    pub aggregate: Option<SubmittedRun>,
}

/// Context passed through `submit_recipe_inner` → `render_script` for
/// SLURM-array sweeps. When set, `render_script` emits
/// `#SBATCH --array=start-end[%throttle]` and injects the sweep arg as
/// `$SLURM_ARRAY_TASK_ID` (with an arithmetic offset when start > 0)
/// rather than a static value.
#[derive(Debug, Clone)]
struct ArraySweepInfo {
    /// The recipe `[args]` key to inject at runtime.
    arg: String,
    start: u32,
    end: u32,
    throttle: Option<u32>,
}

/// Submit the recipe as a single SLURM array job. One labctl registry
/// entry is created; SLURM spawns (end - start + 1) array tasks, each
/// receiving `$SLURM_ARRAY_TASK_ID` as the value of the sweep arg. If
/// the recipe declares `[sweep].aggregate`, a second job is submitted
/// with `--dependency=afterok:<array_job_id>` so it runs only after ALL
/// array tasks complete.
pub fn submit_sweep(
    cluster: &ClusterConfig,
    store: &mut Store,
    recipe: &Recipe,
    submitted_by: &str,
) -> Result<SubmittedSweep> {
    let sweep = recipe
        .sweep
        .as_ref()
        .context("submit_sweep called on recipe without [sweep]")?;
    fs_layout::validate_user(submitted_by)?;

    // Build a recipe clone with the sweep section stripped (it's handled
    // by render_script via ArraySweepInfo) and the sweep arg removed from
    // the static args map (it will be injected as $SLURM_ARRAY_TASK_ID).
    let mut array_recipe = recipe.clone();
    array_recipe.sweep = None;
    array_recipe.args.remove(&sweep.arg);

    let array_info = ArraySweepInfo {
        arg: sweep.arg.clone(),
        start: sweep.start,
        end: sweep.end,
        throttle: sweep.throttle,
    };

    let array_run = submit_recipe_inner(
        cluster,
        store,
        &array_recipe,
        None,
        None,
        &[],
        None,
        submitted_by,
        Some(&array_info),
    )?;

    let aggregate = if let Some(agg_path) = &sweep.aggregate {
        let agg_recipe = crate::config::Recipe::load(agg_path)
            .with_context(|| format!("failed to load aggregate recipe {}", agg_path.display()))?;
        // Depend on the array job ID — SLURM resolves afterok:<array_id>
        // as "all array elements must complete successfully".
        let submitted = submit_recipe_inner(
            cluster,
            store,
            &agg_recipe,
            None,
            None,
            &[array_run.job_id.clone()],
            None,
            submitted_by,
            None,
        )?;
        Some(submitted)
    } else {
        None
    };

    Ok(SubmittedSweep {
        sweep_name: recipe.name.clone(),
        array_run,
        aggregate,
    })
}

pub fn submit_pipeline(
    cluster: &ClusterConfig,
    store: &mut Store,
    pipeline: &LoadedPipeline,
    pipeline_path: Option<&Path>,
    submitted_by: &str,
) -> Result<SubmittedPipeline> {
    fs_layout::validate_user(submitted_by)?;
    // Allocate run_ids for every stage up-front so downstream stages can render
    // input paths that point at upstream run_dirs that haven't been created yet.
    let stage_run_ids: BTreeMap<String, String> = pipeline
        .topo_order
        .iter()
        .map(|name| (name.clone(), util::new_id("run")))
        .collect();

    let pipeline_id = util::new_id("pipeline");
    store.insert_pipeline(&pipeline_id, &pipeline.name, pipeline_path)?;

    let mut stage_job_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut submitted_stages = Vec::with_capacity(pipeline.topo_order.len());

    for stage_name in &pipeline.topo_order {
        let loaded = &pipeline.stages[stage_name];
        let parent_job_ids: Vec<String> = loaded
            .parents
            .iter()
            .map(|p| {
                stage_job_ids
                    .get(p)
                    .cloned()
                    .with_context(|| format!("missing parent job_id for {p:?}"))
            })
            .collect::<Result<Vec<_>>>()?;

        let stage_ctx = StageContext {
            stage_name,
            stage_run_ids: &stage_run_ids,
            stages: &pipeline.stages,
        };

        let preallocated = &stage_run_ids[stage_name];
        let submitted = submit_recipe_inner(
            cluster,
            store,
            &loaded.recipe,
            None,
            Some(&stage_ctx),
            &parent_job_ids,
            Some(preallocated.as_str()),
            submitted_by,
            None,
        )?;

        let dependency_on = json!({
            "afterok": loaded
                .parents
                .iter()
                .map(|p| {
                    json!({
                        "stage": p,
                        "run_id": stage_run_ids[p],
                        "job_id": stage_job_ids[p],
                    })
                })
                .collect::<Vec<_>>()
        });
        store.set_pipeline_membership(
            &submitted.run_id,
            &pipeline_id,
            stage_name,
            &dependency_on,
        )?;
        stage_job_ids.insert(stage_name.clone(), submitted.job_id.clone());
        submitted_stages.push(SubmittedStage {
            stage_name: stage_name.clone(),
            run_id: submitted.run_id,
            job_id: submitted.job_id,
            depends_on: parent_job_ids,
        });
    }

    Ok(SubmittedPipeline {
        pipeline_id,
        name: pipeline.name.clone(),
        stages: submitted_stages,
    })
}

/// Per-run reconcile step. Pulled out so the in-process dispatch loop
/// can drive reconciles with per-run mutex granularity (acquire, do one
/// run, release; UI requests interleave between iterations) instead of
/// holding a single lock for the whole pass.
pub fn reconcile_one(
    cluster: &ClusterConfig,
    store: &mut Store,
    run: &crate::store::RunRow,
) -> Result<ReconcileStep> {
    let mut step = ReconcileStep {
        status_changed: false,
        artifacts_registered: 0,
    };
    let (status, finished_at) = scheduler_outcome(cluster, run)
        .map(|o| (o.status, o.finished_at))
        .or_else(|| status_file_outcome(&run.run_dir))
        .unwrap_or_else(|| (run.status.clone(), None));
    if status != run.status {
        store.update_status(&run.id, &status, finished_at)?;
        step.status_changed = true;
    }
    let current = store.get_run(&run.id)?;
    step.artifacts_registered += register_outputs(store, &current)?;
    let _ = crate::tracking::try_populate_from_log(store, &current);
    Ok(step)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReconcileStep {
    pub status_changed: bool,
    pub artifacts_registered: usize,
}

pub fn reconcile(cluster: &ClusterConfig, store: &mut Store) -> Result<ReconcileReport> {
    let mut runs_reconciled = 0;
    let mut artifacts_registered = 0;
    // Scope to the invoking user's own runs. `labctl reconcile` is a
    // user action — folding the user's own SLURM state back into the
    // registry — and must never touch another user's run rows.
    let submitted_by = crate::store::current_user()?;
    for run in store.list_active_runs(&submitted_by)? {
        let step = reconcile_one(cluster, store, &run)?;
        if step.status_changed {
            runs_reconciled += 1;
        }
        artifacts_registered += step.artifacts_registered;
    }
    Ok(ReconcileReport {
        runs_reconciled,
        artifacts_registered,
    })
}


/// One-shot recovery for runs that hit the terminal-transition bug
/// (terminated before reconcile could register their outputs). Walks
/// terminal runs that have zero recorded `run_outputs` and calls
/// `register_outputs` for each. Idempotent — safe to re-run.
pub fn recover_outputs(_cluster: &ClusterConfig, store: &mut Store) -> Result<RecoverReport> {
    let runs = store.terminal_runs_without_outputs()?;
    let scanned = runs.len();
    let mut recovered = 0;
    let mut artifacts_registered = 0;
    for run in runs {
        // Same try-everything posture as reconcile: marker absence is
        // expected for runs that died before producing anything, and is
        // not an error.
        match register_outputs(store, &run) {
            Ok(0) => {}
            Ok(n) => {
                recovered += 1;
                artifacts_registered += n;
            }
            Err(e) => {
                eprintln!(
                    "recover_outputs: run {} failed: {e:#}",
                    run.id
                );
            }
        }
    }
    Ok(RecoverReport {
        scanned,
        recovered,
        artifacts_registered,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct RecoverReport {
    pub scanned: usize,
    pub recovered: usize,
    pub artifacts_registered: usize,
}

/// One-shot repair for runs whose ``finished_at`` was set to ``now_ts()`` at
/// reconcile time instead of the run's actual end time. Recomputes from
/// sacct's End (preferred) or status.json's updated_at (fallback). Idempotent
/// — repeated invocations converge: a run whose finished_at already matches
/// the recomputed value is left alone.
pub fn repair_finish_times(
    cluster: &ClusterConfig,
    store: &mut Store,
) -> Result<RepairReport> {
    let mut report = RepairReport::default();
    for run in store.terminal_runs()? {
        report.scanned += 1;
        let recomputed = scheduler_outcome(cluster, &run)
            .and_then(|o| o.finished_at)
            .or_else(|| status_file_outcome(&run.run_dir).and_then(|(_, ts)| ts));
        let Some(ts) = recomputed else {
            report.unresolved += 1;
            continue;
        };
        if Some(ts) == run.finished_at {
            report.unchanged += 1;
            continue;
        }
        store.set_finished_at(&run.id, ts)?;
        report.repaired += 1;
    }
    Ok(report)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RepairReport {
    pub scanned: usize,
    pub repaired: usize,
    pub unchanged: usize,
    pub unresolved: usize,
}

pub fn gc(_cluster: &ClusterConfig, store: &mut Store, terminal_snapshots: bool) -> Result<usize> {
    if !terminal_snapshots {
        return Ok(0);
    }
    gc_terminal_sources(store, 0)
}

/// Reap `source/` for terminal runs whose `finished_at` is older than
/// `min_terminal_age_secs`. `.lab/provenance/<repo>/` (git HEAD + diff +
/// uv.lock copy) is kept regardless, so reproducibility survives.
///
/// Used by the CLI (`gc()` wrapper, min_age=0) and by the agent's
/// gc_loop (min_age>0, gives reconcile time to finalize artifacts
/// before the working tree is removed).
pub fn gc_terminal_sources(store: &mut Store, min_terminal_age_secs: u64) -> Result<usize> {
    let now = util::now_ts();
    let cutoff = min_terminal_age_secs as i64;
    let mut removed = 0;
    for run in store.list_runs()? {
        if !is_terminal(&run.status) {
            continue;
        }
        if !run.source_path.exists() {
            continue;
        }
        // If finished_at is missing (rare: pre-finished_at row, or
        // sacct couldn't parse End), fall back to created_at. Either
        // way the run is already terminal, so the bound is correct.
        let stamp = run.finished_at.unwrap_or(run.created_at);
        if now.saturating_sub(stamp) < cutoff {
            continue;
        }
        fs::remove_dir_all(&run.source_path)
            .with_context(|| format!("failed to remove {}", run.source_path.display()))?;
        removed += 1;
    }
    Ok(removed)
}

fn resolve_inputs(
    cluster: &ClusterConfig,
    store: &Store,
    recipe: &Recipe,
    overrides: &SubmitOverrides,
    stage_ctx: Option<&StageContext<'_>>,
    submitted_by: &str,
) -> Result<Vec<InputResolution>> {
    let mut out = Vec::new();
    for (role, spec) in &recipe.inputs {
        let resolved = match spec {
            InputSpec::Artifact { artifact } => {
                let artifact = store.resolve_artifact_ref(artifact)?;
                InputResolution {
                    role: role.clone(),
                    artifact_id: Some(artifact.id),
                    resolved_path: artifact.path,
                }
            }
            InputSpec::External { path } => InputResolution {
                role: role.clone(),
                artifact_id: None,
                resolved_path: path
                    .canonicalize()
                    .with_context(|| format!("external input missing: {}", path.display()))?,
            },
            InputSpec::Checkpoint => {
                let artifact_ref = overrides.input_artifacts.get(role).with_context(|| {
                    format!("input {role:?} requires an artifact override from evald")
                })?;
                let artifact = store.resolve_artifact_ref(artifact_ref)?;
                InputResolution {
                    role: role.clone(),
                    artifact_id: Some(artifact.id),
                    resolved_path: artifact.path,
                }
            }
            InputSpec::Stage {
                stage,
                role: parent_role,
            } => {
                let ctx = stage_ctx.with_context(|| {
                    format!(
                        "input {role:?} is type=stage but recipe is being submitted \
                         outside of a pipeline"
                    )
                })?;
                let parent_run_id = ctx.stage_run_ids.get(stage).with_context(|| {
                    format!("input {role:?} references unknown stage {stage:?}")
                })?;
                let parent_loaded = ctx.stages.get(stage).with_context(|| {
                    format!("input {role:?} references stage {stage:?} not in pipeline")
                })?;
                let parent_spec =
                    parent_loaded.recipe.outputs.get(parent_role).with_context(|| {
                        format!(
                            "input {role:?} references {stage:?}.{parent_role:?}, but \
                             stage {stage:?} declares no such output"
                        )
                    })?;
                // Render the parent's alias against a partial context bound to
                // the parent's run_id and params. Aliases must not reference
                // {inputs.*}/{outputs.*} — render_value will bail if they do.
                // The parent stage is owned by the same submitter (one CLI
                // invocation submits the whole pipeline), so its run dir
                // and artifacts live under the same per-user prefix.
                let parent_run_dir = fs_layout::run_dir(
                    &cluster.filesystem.runs_base,
                    submitted_by,
                    parent_run_id,
                );
                let empty_inputs: Vec<InputResolution> = Vec::new();
                let empty_outputs: BTreeMap<String, PathBuf> = BTreeMap::new();
                let parent_ctx = RenderContext {
                    run_id: parent_run_id,
                    run_dir: &parent_run_dir,
                    params: &parent_loaded.recipe.params,
                    inputs: &empty_inputs,
                    outputs: &empty_outputs,
                };
                let parent_alias = render_value(&parent_spec.alias, &parent_ctx)?;
                let parent_root =
                    cluster.filesystem.output_roots.get(&parent_spec.kind).with_context(
                        || {
                            format!(
                                "stage {stage:?}.{parent_role:?} has kind {:?} which is \
                                 not configured in [filesystem.output_roots]",
                                parent_spec.kind,
                            )
                        },
                    )?;
                let resolved_path =
                    fs_layout::artifact_dir(parent_root, submitted_by, &parent_alias);
                InputResolution {
                    role: role.clone(),
                    artifact_id: None, // backfilled by Store::insert_artifact
                    resolved_path,
                }
            }
        };
        out.push(resolved);
    }
    Ok(out)
}

fn output_paths(
    cluster: &ClusterConfig,
    recipe: &Recipe,
    render_ctx: &RenderContext<'_>,
    submitted_by: &str,
) -> Result<BTreeMap<String, OutputResolution>> {
    let mut out = BTreeMap::new();
    for (role, spec) in &recipe.outputs {
        let alias = render_value(&spec.alias, render_ctx).with_context(|| {
            format!(
                "recipe {:?}: failed to render alias for output {role:?}",
                recipe.name,
            )
        })?;
        let root = cluster
            .filesystem
            .output_roots
            .get(&spec.kind)
            .with_context(|| {
                format!(
                    "recipe {:?}: output {role:?} has kind {:?} which is not configured \
                     in cluster {:?} [filesystem.output_roots]",
                    recipe.name, spec.kind, cluster.name,
                )
            })?;
        let path = fs_layout::artifact_dir(root, submitted_by, &alias);
        out.insert(
            role.clone(),
            OutputResolution {
                role: role.clone(),
                kind: spec.kind.clone(),
                alias,
                marker: spec.marker.clone(),
                path,
            },
        );
    }
    Ok(out)
}

fn render_script(
    cluster: &ClusterConfig,
    recipe: &Recipe,
    run_id: &str,
    run_dir: &Path,
    source_path: &Path,
    inputs: &[InputResolution],
    outputs: &BTreeMap<String, PathBuf>,
    parent_job_ids: &[String],
    array_sweep: Option<&ArraySweepInfo>,
) -> Result<String> {
    let ctx = RenderContext {
        run_id,
        run_dir,
        params: &recipe.params,
        inputs,
        outputs,
    };
    let mut command = recipe.command.clone();
    for (key, value) in &recipe.args {
        command.push(format!("--{}={}", key, render_value(value, &ctx)?));
    }
    // Shell-quote all static parts; the array sweep arg is appended below
    // as a raw bash expression so $SLURM_ARRAY_TASK_ID is expanded by the
    // sbatch wrapper, not treated as a literal string.
    let mut rendered_command = command
        .iter()
        .map(|part| util::shell_quote(part))
        .collect::<Vec<_>>()
        .join(" ");
    if let Some(arr) = array_sweep {
        let expr = if arr.start == 0 {
            "$SLURM_ARRAY_TASK_ID".to_string()
        } else {
            format!("$((SLURM_ARRAY_TASK_ID + {}))", arr.start)
        };
        rendered_command.push_str(&format!(" --{}={}", arr.arg, expr));
    }
    let status_path = run_dir
        .join(fs_layout::LAB_DIRNAME)
        .join(fs_layout::STATUS_JSON);

    let mut script = String::new();
    if cluster.scheduler.kind == SchedulerKind::Slurm {
        script.push_str("#!/usr/bin/env bash\n");
        script.push_str(&format!(
            "#SBATCH --job-name={}\n",
            safe_job_name(&recipe.name)
        ));
        if let Some(account) = recipe
            .resources
            .account
            .as_ref()
            .or(cluster.slurm.account.as_ref())
        {
            script.push_str(&format!("#SBATCH --account={account}\n"));
        }
        if let Some(partition) = recipe
            .resources
            .partition
            .as_ref()
            .or(cluster.slurm.partition.as_ref())
        {
            script.push_str(&format!("#SBATCH --partition={partition}\n"));
        }
        if let Some(qos) = recipe.resources.qos.as_ref().or(cluster.slurm.qos.as_ref()) {
            script.push_str(&format!("#SBATCH --qos={qos}\n"));
        }
        if let Some(exclude) = recipe.resources.exclude_nodes.as_ref() {
            script.push_str(&format!("#SBATCH --exclude={exclude}\n"));
        }
        if recipe.resources.gpus > 0 {
            let syntax = cluster
                .slurm
                .gres_gpu_syntax
                .as_deref()
                .unwrap_or("gpu:{n}")
                .replace("{n}", &recipe.resources.gpus.to_string());
            script.push_str(&format!("#SBATCH --gres={syntax}\n"));
        }
        if !parent_job_ids.is_empty() {
            script.push_str(&format!(
                "#SBATCH --dependency=afterok:{}\n",
                parent_job_ids.join(":")
            ));
            // If a parent fails, drop the queued child instead of leaving it pending forever.
            script.push_str("#SBATCH --kill-on-invalid-dep=yes\n");
        }
        script.push_str(&format!(
            "#SBATCH --cpus-per-task={}\n",
            recipe.resources.cpus
        ));
        script.push_str(&format!("#SBATCH --mem={}\n", recipe.resources.mem));
        script.push_str(&format!("#SBATCH --time={}\n", recipe.resources.time));
        script.push_str(&format!(
            "#SBATCH --output={}/.lab/%x_%j.log\n#SBATCH --error={}/.lab/%x_%j.log\n",
            run_dir.display(),
            run_dir.display()
        ));
        // Array sweep: emit --array before sbatch_extra so the user can
        // layer additional directives on top if needed.
        if let Some(arr) = array_sweep {
            let array_spec = match arr.throttle {
                Some(t) => format!("--array={}-{}%{}", arr.start, arr.end, t),
                None => format!("--array={}-{}", arr.start, arr.end),
            };
            script.push_str(&format!("#SBATCH {}\n", array_spec));
        }
        // Power-user escape hatch. Rendered last so it can layer atop the
        // typed directives, but cannot reorder them.
        for extra in &recipe.resources.sbatch_extra {
            let trimmed = extra.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Refuse comments and stray `#SBATCH` prefixes — the user
            // gives us flags, we own the formatting. Catching this at
            // submission time keeps the rendered script self-explanatory.
            if trimmed.starts_with('#') {
                bail!(
                    "resources.sbatch_extra entries must be bare flags (e.g. \"--array=0-3\"); \
                     labctl prepends the #SBATCH prefix. Got: {extra:?}"
                );
            }
            script.push_str(&format!("#SBATCH {trimmed}\n"));
        }
    } else {
        script.push_str("#!/usr/bin/env bash\n");
    }

    script.push_str(
        r#"
set -uo pipefail
job_id="${SLURM_JOB_ID:-${LABCTL_JOB_ID:-local}}"
write_status() {
  state="$1"
  rc="${2:-0}"
  tmp=""#,
    );
    script.push_str(&util::shell_quote(&format!(
        "{}.tmp",
        status_path.display()
    )));
    script.push_str(
        r#""
  dst=""#,
    );
    script.push_str(&util::shell_quote(&status_path.display().to_string()));
    script.push_str(
        r#""
  printf '{"status":"%s","job_id":"%s","rc":%s,"updated_at":%s}\n' "$state" "$job_id" "$rc" "$(date +%s)" > "$tmp"
  mv "$tmp" "$dst"
}
write_status running 0
"#,
    );

    for module in &cluster.modules {
        script.push_str(&format!("module load {}\n", util::shell_quote(module)));
    }
    for (key, value) in &cluster.env {
        script.push_str(&format!("export {key}={}\n", util::shell_quote(value)));
    }
    for (key, value) in &recipe.env {
        script.push_str(&format!("export {key}={}\n", util::shell_quote(value)));
    }
    script.push_str(&format!(
        "export LABCTL_RUN_ID={}\n",
        util::shell_quote(run_id)
    ));
    script.push_str(&format!(
        "export LABCTL_RUN_DIR={}\n",
        util::shell_quote(&run_dir.display().to_string())
    ));
    script.push_str(&format!(
        "export LABCTL_CONTEXT={}\n",
        util::shell_quote(
            &run_dir
                .join(fs_layout::LAB_DIRNAME)
                .join(fs_layout::CONTEXT_JSON)
                .display()
                .to_string(),
        )
    ));
    // Tracker env injection. Schema-declared, set after recipe.env so the
    // recipe can't accidentally clobber it. WANDB_RUN_ID = labctl run id is
    // load-bearing: it makes the W&B URL fully derivable from recipe + run id
    // without any per-run sentinel file.
    if let Some(wandb) = &recipe.tracking.wandb {
        script.push_str(&format!(
            "export WANDB_ENTITY={}\n",
            util::shell_quote(&wandb.entity)
        ));
        script.push_str(&format!(
            "export WANDB_PROJECT={}\n",
            util::shell_quote(&wandb.project)
        ));
        script.push_str(&format!(
            "export WANDB_RUN_ID={}\n",
            util::shell_quote(run_id)
        ));
        script.push_str(&format!(
            "export WANDB_NAME={}\n",
            util::shell_quote(&format!("{}-{}", recipe.name, short_run_suffix(run_id)))
        ));
        if let Some(group) = &wandb.group {
            script.push_str(&format!(
                "export WANDB_RUN_GROUP={}\n",
                util::shell_quote(group)
            ));
        }
        // resume=allow lets a re-submitted run with the same id reattach
        // instead of erroring; matches labctl's "reconcile, don't lose state"
        // model.
        script.push_str("export WANDB_RESUME=allow\n");
    }
    script.push_str(&format!(
        "cd {}\n",
        util::shell_quote(&source_path.display().to_string())
    ));
    script.push_str(&rendered_command);
    script.push('\n');
    script.push_str(
        r#"rc=$?
if [ "$rc" -eq 0 ]; then
  write_status succeeded "$rc"
else
  write_status failed "$rc"
fi
exit "$rc"
"#,
    );
    Ok(script)
}

fn submit_script(cluster: &ClusterConfig, script_path: &Path, run_id: &str) -> Result<String> {
    match cluster.scheduler.kind {
        SchedulerKind::Slurm => {
            let output = Command::new(&cluster.scheduler.sbatch)
                .arg("--parsable")
                .arg(script_path)
                .output()
                .with_context(|| format!("failed to execute {}", cluster.scheduler.sbatch))?;
            if !output.status.success() {
                bail!(
                    "sbatch failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.trim().split(';').next().unwrap_or("").to_string())
        }
        SchedulerKind::Local => {
            let job_id = format!("local-{run_id}");
            let status = Command::new("bash")
                .arg(script_path)
                .env("LABCTL_JOB_ID", &job_id)
                .status()
                .with_context(|| {
                    format!("failed to execute local script {}", script_path.display())
                })?;
            if !status.success() {
                // The run itself is allowed to fail; reconciliation records the terminal state.
            }
            Ok(job_id)
        }
    }
}

/// Query sacct for the run's State and End. We pass ``--starttime`` derived
/// from the run's ``created_at`` (minus a 1-day buffer) so that very old jobs
/// still appear, and so a recycled job_id can't return a row from a different
/// run that ran much later. ``TZ=UTC`` is forced so we can parse End without
/// ambiguity. Returns ``None`` only if sacct itself can't be invoked or
/// returns nonzero — an unparseable End is fine, we just leave finished_at
/// empty and let the caller fall back.
fn scheduler_outcome(
    cluster: &ClusterConfig,
    run: &crate::store::RunRow,
) -> Option<SchedulerOutcome> {
    if cluster.scheduler.kind == SchedulerKind::Local {
        return None;
    }
    let job_id = run.job_id.as_ref()?;
    let starttime = fmt_starttime_utc((run.created_at - 86_400).max(0));
    let output = Command::new(&cluster.scheduler.sacct)
        .env("TZ", "UTC")
        .args([
            "-j",
            job_id,
            "-P",
            "-n",
            "--format=State,End",
            "--starttime",
            &starttime,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    // Aggregate all sacct rows: handles both normal jobs (one row) and
    // array jobs (one row per element). Priority: any failure trumps all;
    // running > submitted > succeeded (so partial completion stays "running").
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut best: Option<String> = None;
    let mut latest_end: Option<i64> = None;
    for line in stdout.lines() {
        let mut parts = line.split('|');
        let Some(state_field) = parts.next() else { continue };
        let state = state_field.split_whitespace().next().unwrap_or("").trim();
        let end_field = parts.next().unwrap_or("").trim();
        let status = map_slurm_state(state);
        if let Some(ts) = parse_sacct_end_utc(end_field) {
            latest_end = Some(latest_end.map_or(ts, |prev: i64| prev.max(ts)));
        }
        best = Some(match (best.as_deref(), status.as_str()) {
            // Failure is terminal — short-circuit once seen.
            (_, "failed" | "timeout" | "oom" | "cancelled" | "unknown_terminal") => {
                return Some(SchedulerOutcome { status, finished_at: latest_end });
            }
            (None, _) => status,
            (Some("running"), _) => "running".to_string(),
            (Some("submitted"), "running") => "running".to_string(),
            (Some("submitted"), _) => best.unwrap(),
            (Some("succeeded"), "running") => "running".to_string(),
            (Some("succeeded"), "submitted") => "running".to_string(),
            _ => best.unwrap(),
        });
    }
    let status = best?;
    Some(SchedulerOutcome { status, finished_at: latest_end })
}

fn parse_sacct_end_utc(s: &str) -> Option<i64> {
    if s.is_empty() || s == "Unknown" || s == "None" {
        return None;
    }
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok()?;
    Some(dt.and_utc().timestamp())
}

fn fmt_starttime_utc(ts: i64) -> String {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_opt(ts.max(0), 0)
        .single()
        .map(|d| d.format("%Y-%m-%dT%H:%M:%S").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00".to_string())
}

fn map_slurm_state(state: &str) -> String {
    let primary = state.split_whitespace().next().unwrap_or(state);
    match primary {
        "PENDING" | "CONFIGURING" | "SUSPENDED" => "submitted",
        "RUNNING" | "COMPLETING" => "running",
        "COMPLETED" => "succeeded",
        "CANCELLED" => "cancelled",
        "TIMEOUT" => "timeout",
        "OUT_OF_MEMORY" => "oom",
        "FAILED" | "BOOT_FAIL" | "DEADLINE" | "NODE_FAIL" | "PREEMPTED" => "failed",
        _ => "unknown_terminal",
    }
    .to_string()
}

fn status_file_outcome(run_dir: &Path) -> Option<(String, Option<i64>)> {
    let path = run_dir
        .join(fs_layout::LAB_DIRNAME)
        .join(fs_layout::STATUS_JSON);
    let text = fs::read_to_string(path).ok()?;
    let status: StatusFile = serde_json::from_str(&text).ok()?;
    Some((status.status, status.updated_at))
}

fn register_outputs(store: &mut Store, run: &crate::store::RunRow) -> Result<usize> {
    let outputs_value = run
        .context_json
        .get("outputs")
        .with_context(|| format!("run {} context.json missing 'outputs' field", run.id))?
        .clone();
    let outputs: BTreeMap<String, OutputResolution> = serde_json::from_value(outputs_value)
        .with_context(|| format!("run {} context.json 'outputs' shape mismatch", run.id))?;
    let mut count = 0;
    for (role, resolution) in &outputs {
        if resolution.kind == "checkpoint_stream" {
            if !resolution.path.is_dir() {
                continue;
            }
            for entry in fs::read_dir(&resolution.path)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let step_dir = entry.path();
                let step_name = entry.file_name().to_string_lossy().to_string();
                // Accept any "<non-digit prefix><trailing digits>" dir name.
                // Covers omegalax `step9000`, slime `rollout_49`, Megatron
                // `iter_0000049`, DeepSpeed `global_step49`, HF Trainer
                // `checkpoint-49`, and bare numeric `49`. Anything else
                // (mixed-digit names, no trailing digits) is silently
                // skipped — the marker filter below catches truly-stray
                // dirs.
                let step = match parse_trailing_step(&step_name) {
                    Some(n) => n,
                    None => continue,
                };
                if !step_dir.join(&resolution.marker).exists() {
                    continue;
                }
                // Short-circuit: if we've already registered an artifact
                // for exactly this path, skip the multi-GB SHA-256 walk.
                // Each per-step checkpoint dir is path-unique (the orbax
                // ``<step>`` subdir is a content-stable artifact), so
                // path-equality is a sound dedup key here.
                if let Some(existing) = store.find_artifact_by_path("checkpoint", &step_dir)? {
                    store.link_run_output(&run.id, role, &existing.id)?;
                    continue;
                }
                // Use a path-based identity hash, not dir contents. Orbax
                // writes each step dir atomically (tmp + rename) and the
                // ``_CHECKPOINT_METADATA`` marker is the completion barrier,
                // so once the marker exists the path uniquely identifies a
                // content-stable artifact. Hashing the multi-GB tensor
                // payload added no information and was the dominant cost
                // of every reconcile pass. Same identity scheme used by
                // ``register-external``.
                let canonical = step_dir.canonicalize().unwrap_or_else(|_| step_dir.clone());
                let content_hash = util::sha256_bytes(canonical.display().to_string().as_bytes());
                let artifact = store.insert_artifact(
                    "checkpoint",
                    &step_dir,
                    &content_hash,
                    Some(&run.id),
                    &json!({
                        "role": role,
                        "step": step,
                        "marker": resolution.marker,
                        "stream_alias": resolution.alias,
                        "producer_recipe": run.recipe_name,
                    }),
                )?;
                store.link_run_output(&run.id, role, &artifact.id)?;
                count += 1;
            }
        } else {
            if !resolution.path.join(&resolution.marker).is_file() {
                continue;
            }
            let content_hash = util::dir_content_hash(&resolution.path)?;
            let mut metadata = json!({
                "role": role,
                "marker": resolution.marker,
                "producer_recipe": run.recipe_name,
            });
            if resolution.kind == "eval_result" {
                if let Ok(text) = fs::read_to_string(resolution.path.join(&resolution.marker)) {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        metadata["result"] = value;
                    }
                }
            }
            let artifact = store.insert_artifact(
                &resolution.kind,
                &resolution.path,
                &content_hash,
                Some(&run.id),
                &metadata,
            )?;
            store.link_run_output(&run.id, role, &artifact.id)?;
            store.set_alias(&resolution.alias, &artifact.id)?;
            count += 1;
        }
    }
    Ok(count)
}

/// Extract the trailing digit run from a per-step checkpoint dir name and
/// parse it as a u64. Returns None when the name has no trailing digits.
/// See the call site in `register_outputs` for accepted naming conventions.
fn parse_trailing_step(name: &str) -> Option<u64> {
    let suffix_len = name.bytes().rev().take_while(|b| b.is_ascii_digit()).count();
    if suffix_len == 0 {
        return None;
    }
    name[name.len() - suffix_len..].parse::<u64>().ok()
}

/// Last 8 chars of a labctl run id, used for short human-readable run names
/// (W&B `WANDB_NAME` etc). Run ids look like `run_<uuid_chunk>`; the
/// trailing chars are the high-entropy suffix.
fn short_run_suffix(run_id: &str) -> String {
    let n = run_id.len();
    run_id[n.saturating_sub(8)..].to_string()
}

fn safe_job_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}
