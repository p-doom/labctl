use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use tokio::process::Command;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::{ClusterConfig, InputSpec, LoadedPipeline, LoadedStage, Recipe, SchedulerKind},
    fs_layout, provenance,
    store::{ArtifactRow, InputResolution, NewRun, Store, is_terminal},
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker: Option<String>,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct SubmitOverrides {
    pub input_artifacts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmittedRun {
    pub run_id: String,
    /// Empty string when the run was a cache-hit (no SLURM job submitted).
    /// Otherwise the SLURM job id. Use ``cache_hit`` to distinguish.
    pub job_id: String,
    pub run_dir: PathBuf,
    #[serde(default)]
    pub cache_hit: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconcileReport {
    pub runs_reconciled: usize,
    pub artifacts_registered: usize,
}

#[derive(Debug, Clone)]
struct SchedulerOutcome {
    status: String,
    finished_at: Option<i64>,
}

pub async fn submit_recipe(
    cluster: &ClusterConfig,
    store: &Store,
    recipe: &Recipe,
    overrides: Option<SubmitOverrides>,
    submitted_by: &str,
) -> Result<SubmittedRun> {
    fs_layout::validate_user(submitted_by)?;
    submit_recipe_inner(cluster, store, recipe, overrides, None, &[], None, submitted_by, None).await
}

#[derive(Debug, Clone)]
pub struct StageContext<'a> {
    pub stage_name: &'a str,
    pub stage_run_ids: &'a BTreeMap<String, String>,
    /// Loaded recipes for every stage in the current pipeline. Required so
    /// downstream stages can compute the path of an upstream stage's
    /// output: ``output_roots[upstream.kind] / rendered(upstream.alias)``.
    pub stages: &'a BTreeMap<String, LoadedStage>,
    /// Outputs of the pipeline's ``from`` pin, keyed by role. Empty when
    /// the pipeline has no ``from``. Used by ``InputSpec::From`` to
    /// resolve inputs to the pinned run's artifacts.
    pub pinned_outputs: &'a BTreeMap<String, ArtifactRow>,
}

/// Stage-level cache key: a stable digest of "the exact computation this
/// stage will perform". Two submissions with the same key represent the
/// same computation and may share outputs.
///
/// Components: recipe content (recipe_hash), code state (git HEAD +
/// sha256 of uncommitted diff), runtime deps (uv.lock hash), declared
/// inputs (their artifact IDs or resolved paths), and recipe args.
/// Source-tree contents are NOT hashed — git provenance is the
/// authoritative source-state identity.
fn compute_cache_key(
    recipe_hash: &str,
    provenance: &provenance::RepoProvenance,
    inputs: &[crate::store::InputResolution],
    params: &BTreeMap<String, Value>,
) -> Result<String> {
    let diff_hash = provenance
        .git_diff_head
        .as_deref()
        .map(|s| util::sha256_bytes(s.as_bytes()))
        .unwrap_or_else(|| util::sha256_bytes(b""));
    let mut input_keys: Vec<String> = inputs
        .iter()
        .map(|i| {
            format!(
                "{}={}",
                i.role,
                i.artifact_id
                    .clone()
                    .unwrap_or_else(|| i.resolved_path.display().to_string())
            )
        })
        .collect();
    input_keys.sort();
    let params_canonical = serde_json::to_string(params)?;
    let payload = serde_json::json!({
        "recipe_hash": recipe_hash,
        "git_head": provenance.git_head,
        "diff_hash": diff_hash,
        "uv_lock_hash": provenance.uv_lock_hash,
        "inputs": input_keys,
        "params": params_canonical,
    });
    Ok(util::sha256_bytes(&serde_json::to_vec(&payload)?))
}

/// Check that every artifact in `prior_outputs` is still materialized on
/// disk with its marker file present. Cache-hit only valid when this
/// returns true.
fn cache_hit_outputs_valid(
    prior_outputs: &[crate::store::ArtifactRow],
    outputs: &BTreeMap<String, OutputResolution>,
) -> bool {
    // Build a map of role -> expected marker so we can verify each artifact
    // still has its completion marker (in case someone scrubbed the content
    // but left a stray dir).
    let role_to_marker: BTreeMap<&str, Option<&str>> = outputs
        .iter()
        .map(|(r, res)| (r.as_str(), res.marker.as_deref()))
        .collect();
    for art in prior_outputs {
        // Find the role this artifact filled. The artifact's `metadata_json`
        // includes the role under "role".
        let role = art
            .metadata_json
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let marker = match role_to_marker.get(role) {
            Some(m) => *m,
            None => return false,
        };
        if !art.path.exists() {
            return false;
        }
        if let Some(m) = marker {
            if !art.path.join(m).exists() {
                return false;
            }
        }
    }
    !prior_outputs.is_empty()
}

#[allow(clippy::too_many_arguments)]
async fn register_cache_hit(
    cluster: &ClusterConfig,
    store: &Store,
    recipe: &Recipe,
    run_id: &str,
    run_dir: &Path,
    lab_dir: &Path,
    recipe_hash: &str,
    cache_key: &str,
    prior_run_id: &str,
    inputs: &[crate::store::InputResolution],
    outputs: &BTreeMap<String, OutputResolution>,
    repo_provenance: &provenance::RepoProvenance,
    stage_ctx: Option<&StageContext<'_>>,
    parent_job_ids: &[String],
    submitted_by: &str,
) -> Result<SubmittedRun> {
    let source_root = run_dir.join("source");
    let source_path = source_root.join(&recipe.repo);
    let ctx = json!({
        "schema_version": 1,
        "run_id": run_id,
        "run_dir": run_dir,
        "recipe_name": recipe.name,
        "recipe_hash": recipe_hash,
        "repo": recipe.repo,
        // source_path is set but not materialized; cache-hit reuses prior outputs.
        "source_path": source_path,
        "source_hash": Value::Null,
        "inputs": inputs,
        "outputs": outputs,
        "params": recipe.params,
        "provenance": repo_provenance,
        "stage_name": stage_ctx.map(|c| c.stage_name),
        "parent_job_ids": parent_job_ids,
        "cache_hit": true,
        "cache_hit_source_run_id": prior_run_id,
    });
    util::atomic_write(
        &lab_dir.join(fs_layout::CONTEXT_JSON),
        &serde_json::to_vec_pretty(&ctx)?,
    )?;

    store
        .insert_run(
            crate::store::NewRun {
                id: run_id,
                recipe,
                recipe_hash,
                status: "created",
                run_dir,
                source_path: &source_path,
                context_json: &ctx,
                submitted_by: Some(submitted_by),
                cache_key: Some(cache_key),
            },
            inputs,
        )
        .await?;

    // Link the prior run's output artifacts into this new run's output set.
    store.copy_run_outputs(prior_run_id, run_id).await?;

    // Emit the explanatory event before flipping to the terminal state so
    // downstream tooling can distinguish "skipped due to cache hit" from
    // "completed normally".
    store
        .append_stage_cache_hit_event(run_id, cache_key, prior_run_id)
        .await?;
    store
        .update_status(run_id, "cache_hit", Some(util::now_ts()))
        .await?;

    // Optional: silence the unused-param warning when cluster isn't needed
    // for cache-hit. Keeping the arg in the signature mirrors the regular
    // path and leaves room for future per-cluster cache policies.
    let _ = cluster;

    Ok(SubmittedRun {
        run_id: run_id.to_string(),
        job_id: String::new(),
        run_dir: run_dir.to_path_buf(),
        cache_hit: true,
    })
}

/// Resolve a pipeline's ``from`` historical pin to a role-keyed map of
/// artifact rows. Rejects pinning to non-succeeded runs — pinning to
/// in-flight or failed runs is almost always a mistake.
async fn resolve_from(
    _cluster: &ClusterConfig,
    store: &Store,
    from_id: &str,
) -> Result<BTreeMap<String, ArtifactRow>> {
    let run = store
        .get_run(from_id)
        .await
        .with_context(|| format!("from = {from_id:?}: no such run"))?;
    match run.status.as_str() {
        "succeeded" | "cache_hit" => {}
        other => bail!(
            "from = {from_id:?}: pinned run is {other:?}; pin only to runs that \
             succeeded or cache-hit a prior succeeded run"
        ),
    }
    let outputs = store.run_outputs(from_id).await?;
    key_outputs_by_role(from_id, outputs)
}

fn key_outputs_by_role(
    from_id: &str,
    outputs: Vec<ArtifactRow>,
) -> Result<BTreeMap<String, ArtifactRow>> {
    let mut by_role = BTreeMap::new();
    for art in outputs {
        let role = art
            .metadata_json
            .get("role")
            .and_then(|v| v.as_str())
            .with_context(|| {
                format!(
                    "from = {from_id:?}: artifact {} has no `role` in metadata; \
                     pinned run's outputs cannot be addressed by role",
                    art.id
                )
            })?
            .to_string();
        by_role.insert(role, art);
    }
    Ok(by_role)
}

async fn submit_recipe_inner(
    cluster: &ClusterConfig,
    store: &Store,
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
    let inputs = resolve_inputs(cluster, store, recipe, &overrides, stage_ctx, submitted_by).await?;

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

    // recipe_hash + provenance need to land BEFORE the cache-hit / marker
    // check so we can compute a cache_key without paying the source-copy
    // cost on cache hits.
    let repo_path = cluster
        .repos
        .get(&recipe.repo)
        .with_context(|| format!("cluster has no repo mapping for {:?}", recipe.repo))?;
    let recipe_hash = util::sha256_bytes(&serde_json::to_vec(recipe)?);
    let provenance_dir = lab_dir.join("provenance").join(&recipe.repo);
    let repo_provenance = provenance::capture_repo(repo_path, &provenance_dir)?;
    let cache_key = compute_cache_key(&recipe_hash, &repo_provenance, &inputs, &recipe.params)?;

    // Cache hit: a prior run with the same cache_key still has its outputs
    // on disk. Link them and skip the entire submit / source-copy / sbatch
    // path. The recipe is unchanged; the registry IS the cache.
    if let Some(prior_run) = store.find_cache_hit_candidate(&cache_key).await? {
        let prior_outputs = store.run_outputs(&prior_run.id).await?;
        if cache_hit_outputs_valid(&prior_outputs, &outputs) {
            return register_cache_hit(
                cluster,
                store,
                recipe,
                &run_id,
                &run_dir,
                &lab_dir,
                &recipe_hash,
                &cache_key,
                &prior_run.id,
                &inputs,
                &outputs,
                &repo_provenance,
                stage_ctx,
                parent_job_ids,
                submitted_by,
            )
            .await;
        }
    }

    // Materialize each output's directory so the recipe's command can write
    // into it without having to mkdir -p itself. Refuse only if the marker
    // file is already present AND we couldn't satisfy it via cache hit —
    // that's the explicit success signal of a prior run whose cache_key
    // differs from ours (e.g. recipe or inputs changed; user must
    // explicitly delete to re-run). An empty or partially-populated
    // directory (failed run, or labctl-pre-created shell) is safe to
    // reuse: re-submission of a failed recipe should "just work" without
    // forcing the operator to rm output dirs by hand. For checkpoint_stream
    // outputs, the marker lives one step deeper (under <stream>/<step>/<marker>),
    // so the top-level directory existing means nothing — let it through.
    for resolution in outputs.values() {
        if resolution.kind != "checkpoint_stream" {
            if let Some(m) = &resolution.marker {
                let marker_path = resolution.path.join(m);
                if marker_path.exists() {
                    bail!(
                        "output marker already present: {} (role={:?}, alias={:?}, kind={:?}) and \
                         no matching cache_key in registry. The path holds a stale artifact from a \
                         different recipe/input combination. Delete the path explicitly or template \
                         the alias with {{run.id}} for per-submission uniqueness.",
                        marker_path.display(),
                        resolution.role,
                        resolution.alias,
                        resolution.kind,
                    );
                }
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

    store
        .insert_run(
            NewRun {
                id: &run_id,
                recipe,
                recipe_hash: &recipe_hash,
                status: "created",
                run_dir: &run_dir,
                source_path: &source_path,
                context_json: &ctx,
                submitted_by: Some(submitted_by),
                cache_key: Some(&cache_key),
            },
            &inputs,
        )
        .await?;

    // Tracker row written at submission time. URL is fully derivable here
    // because we've forced WANDB_RUN_ID = labctl run id in the sbatch env.
    if let Some(wandb) = &recipe.tracking.wandb {
        let url = format!("https://wandb.ai/{}/{}/runs/{}", wandb.entity, wandb.project, run_id);
        store
            .set_tracking(
                &run_id,
                &wandb.entity,
                &wandb.project,
                &url,
                wandb.group.as_deref(),
                "schema",
            )
            .await?;
    }

    let job_id = submit_script(cluster, &script_path, &run_id).await?;
    store.set_submitted(&run_id, &job_id).await?;

    Ok(SubmittedRun {
        run_id,
        job_id,
        run_dir,
        cache_hit: false,
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
pub async fn submit_sweep(
    cluster: &ClusterConfig,
    store: &Store,
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
    )
    .await?;

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
        )
        .await?;
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

pub async fn submit_pipeline(
    cluster: &ClusterConfig,
    store: &Store,
    pipeline: &LoadedPipeline,
    pipeline_path: Option<&Path>,
    submitted_by: &str,
) -> Result<SubmittedPipeline> {
    fs_layout::validate_user(submitted_by)?;
    // Resolve the `from` pin (if any). Stages whose only parents are the
    // pin (or have no parents at all) become root stages and get
    // submitted immediately. Stages with intra-pipeline parents are
    // deferred until those parents are observed terminal-succeeded by
    // the agent's reconcile loop — see `try_submit_pending_children`.
    let pinned_outputs: BTreeMap<String, ArtifactRow> = match pipeline.from.as_deref() {
        Some(from_id) => resolve_from(cluster, store, from_id).await?,
        None => BTreeMap::new(),
    };
    // Allocate run_ids up-front: pending placeholders need them, and
    // dependent stages reference parent ids in their `dependency_on`.
    let stage_run_ids: BTreeMap<String, String> = pipeline
        .topo_order
        .iter()
        .map(|name| (name.clone(), util::new_id("run")))
        .collect();

    let pipeline_id = util::new_id("pipeline");
    store
        .insert_pipeline(&pipeline_id, &pipeline.name, pipeline_path)
        .await?;

    // Stages whose outputs are materialised _right now_. Roots that get
    // cache-hit during this call land here; non-cache-hit roots and any
    // deferred stage stay out. Used to decide whether a downstream stage
    // can also be submitted synchronously (a fully-cached pipeline still
    // completes in one call) or must be deferred to the agent cascade.
    let mut materialised: BTreeSet<String> = BTreeSet::new();
    let mut submitted_stages = Vec::with_capacity(pipeline.topo_order.len());

    for stage_name in &pipeline.topo_order {
        let loaded = &pipeline.stages[stage_name];

        let dependency_on = json!({
            "afterok": loaded
                .parents
                .iter()
                .map(|p| json!({
                    "stage": p,
                    "run_id": stage_run_ids[p],
                }))
                .collect::<Vec<_>>()
        });

        let parents_ready = loaded
            .parents
            .iter()
            .all(|p| materialised.contains(p));
        let preallocated = &stage_run_ids[stage_name];

        if parents_ready {
            // Submit now. agent-driven means afterok is no longer used;
            // by construction parents either don't exist (root) or are
            // already materialised (cache-hit chain).
            let stage_ctx = StageContext {
                stage_name,
                stage_run_ids: &stage_run_ids,
                stages: &pipeline.stages,
                pinned_outputs: &pinned_outputs,
            };
            let submitted = submit_recipe_inner(
                cluster,
                store,
                &loaded.recipe,
                None,
                Some(&stage_ctx),
                &[],
                Some(preallocated.as_str()),
                submitted_by,
                None,
            )
            .await?;
            store
                .set_pipeline_membership(
                    &submitted.run_id,
                    &pipeline_id,
                    stage_name,
                    &dependency_on,
                )
                .await?;
            if submitted.cache_hit {
                materialised.insert(stage_name.clone());
            }
            submitted_stages.push(SubmittedStage {
                stage_name: stage_name.clone(),
                run_id: submitted.run_id,
                job_id: submitted.job_id,
                depends_on: Vec::new(),
            });
        } else {
            // At least one parent is still pending or in-flight. Insert
            // a placeholder; the agent's reconcile loop will pick it up
            // after the last parent reaches terminal-succeeded.
            let recipe_hash = util::sha256_bytes(&serde_json::to_vec(&loaded.recipe)?);
            let run_dir = fs_layout::run_dir(
                &cluster.filesystem.runs_base,
                submitted_by,
                preallocated,
            );
            let source_path = run_dir.join("source").join(&loaded.recipe.repo);
            store
                .insert_pending_pipeline_stage(
                    preallocated,
                    &loaded.recipe,
                    &recipe_hash,
                    &run_dir,
                    &source_path,
                    submitted_by,
                    &pipeline_id,
                    stage_name,
                    &dependency_on,
                )
                .await?;
            submitted_stages.push(SubmittedStage {
                stage_name: stage_name.clone(),
                run_id: preallocated.clone(),
                job_id: String::new(), // pending — no job_id yet
                depends_on: loaded
                    .parents
                    .iter()
                    .map(|p| stage_run_ids[p].clone())
                    .collect(),
            });
        }
    }

    Ok(SubmittedPipeline {
        pipeline_id,
        name: pipeline.name.clone(),
        stages: submitted_stages,
    })
}

/// Attempt to advance pending dependent stages now that `parent_run_id`
/// has reached a terminal state. Called from `reconcile_one` after a
/// run's status flips to a terminal value.
///
/// Behaviour:
///   - parent succeeded / cache_hit → for each pending child whose
///     full parent set is now satisfied, complete the submission
///     (resolve inputs, render submit.sh, sbatch, upsert PG row).
///     If a child's parent set isn't yet complete (multiple parents),
///     leave it pending.
///   - parent in any non-success terminal (failed / cancelled / oom /
///     timeout / unknown_terminal) → cascade-fail every pending child
///     blocked on this parent. No SLURM activity; the children are
///     marked terminal-failed in PG and downstream stages cascade in
///     turn on the next reconcile pass.
pub async fn try_submit_pending_children(
    cluster: &ClusterConfig,
    store: &Store,
    parent: &crate::store::RunRow,
) -> Result<usize> {
    if !crate::store::is_terminal(&parent.status) {
        return Ok(0);
    }
    let cascade_failure = !matches!(parent.status.as_str(), "succeeded" | "cache_hit");
    let candidates = store.pending_children_of(&parent.id).await?;
    let mut advanced = 0usize;
    for child in candidates {
        if cascade_failure {
            store.update_status(&child.id, "failed", parent.finished_at).await?;
            advanced += 1;
            continue;
        }
        // Multi-parent: check that every parent is terminal-succeeded.
        let parent_ids = child
            .dependency_on
            .as_ref()
            .and_then(|d| d.get("afterok"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e.get("run_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut all_ok = true;
        for pid in &parent_ids {
            let p = store.get_run(pid).await?;
            if !matches!(p.status.as_str(), "succeeded" | "cache_hit") {
                all_ok = false;
                break;
            }
        }
        if !all_ok {
            continue;
        }
        match complete_pending_submission(cluster, store, &child).await {
            Ok(()) => advanced += 1,
            Err(e) => {
                eprintln!(
                    "try_submit_pending_children: completing {} failed: {e:#}",
                    child.id
                );
                let now = crate::util::now_ts();
                let _ = store.update_status(&child.id, "failed", Some(now)).await;
            }
        }
    }
    Ok(advanced)
}

/// Drive a pending PG row through normal submission: deserialise its
/// recipe, reconstruct the pipeline-stage context from the recorded
/// dependency_on, and call `submit_recipe_inner` with the existing
/// run_id. The insert_run upsert path takes care of replacing the
/// placeholder.
async fn complete_pending_submission(
    cluster: &ClusterConfig,
    store: &Store,
    child: &crate::store::RunRow,
) -> Result<()> {
    let recipe: Recipe = serde_json::from_value(child.recipe_json.clone())
        .with_context(|| format!("pending run {} has unparseable recipe_json", child.id))?;
    let submitted_by = child
        .submitted_by
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    // Rebuild the stage_run_ids map from the pipeline siblings so
    // template tokens like {inputs.X.path} that reference upstream
    // outputs resolve. Pinned `from` outputs aren't available here —
    // the agent-driven path uses concrete artifact paths via PG, so
    // pinned_outputs can be empty.
    let pipeline_id = child
        .pipeline_id
        .as_deref()
        .with_context(|| format!("pending run {} has no pipeline_id", child.id))?;
    let stage_name = child
        .stage_name
        .as_deref()
        .with_context(|| format!("pending run {} has no stage_name", child.id))?;
    let siblings = store.list_pipeline_runs(pipeline_id).await?;
    let stage_run_ids: BTreeMap<String, String> = siblings
        .iter()
        .filter_map(|r| r.stage_name.clone().map(|s| (s, r.id.clone())))
        .collect();
    // Stages map: minimal — only stage_name → LoadedStage is needed for
    // resolve_inputs's chain-input lookup. We reconstruct from sibling
    // recipes. If a sibling can't be re-parsed, skip it (it won't be
    // referenced as a parent of this child).
    let mut stages: BTreeMap<String, crate::config::LoadedStage> = BTreeMap::new();
    for s in &siblings {
        let Some(name) = s.stage_name.clone() else { continue };
        let Ok(sr): std::result::Result<Recipe, _> = serde_json::from_value(s.recipe_json.clone())
        else {
            continue;
        };
        // parents list is unused inside resolve_inputs for the present
        // child; we leave it empty.
        stages.insert(
            name,
            crate::config::LoadedStage {
                recipe_path: PathBuf::new(),
                recipe: sr,
                parents: Vec::new(),
            },
        );
    }
    let pinned_outputs: BTreeMap<String, ArtifactRow> = BTreeMap::new();
    let stage_ctx = StageContext {
        stage_name,
        stage_run_ids: &stage_run_ids,
        stages: &stages,
        pinned_outputs: &pinned_outputs,
    };

    submit_recipe_inner(
        cluster,
        store,
        &recipe,
        None,
        Some(&stage_ctx),
        &[],
        Some(child.id.as_str()),
        &submitted_by,
        None,
    )
    .await?;
    Ok(())
}

/// Per-run reconcile step. Pulled out so the in-process dispatch loop
/// can drive reconciles with per-run mutex granularity (acquire, do one
/// run, release; UI requests interleave between iterations) instead of
/// holding a single lock for the whole pass.
pub async fn reconcile_one(
    cluster: &ClusterConfig,
    store: &Store,
    run: &crate::store::RunRow,
) -> Result<ReconcileStep> {
    let mut step = ReconcileStep {
        status_changed: false,
        artifacts_registered: 0,
    };
    let (status, finished_at) = scheduler_outcome(cluster, run)
        .await
        .map(|o| (o.status, o.finished_at))
        .unwrap_or_else(|| (run.status.clone(), None));
    if status != run.status {
        store.update_status(&run.id, &status, finished_at).await?;
        step.status_changed = true;
    }
    let current = store.get_run(&run.id).await?;
    step.artifacts_registered += register_outputs(store, &current).await?;
    let _ = crate::tracking::try_populate_from_log(store, &current).await;
    // Agent-driven cascade: if this run just reached a terminal state,
    // sweep its pending dependent stages and either advance (succeeded /
    // cache_hit) or cascade-fail them.
    if step.status_changed && crate::store::is_terminal(&current.status) {
        let _ = try_submit_pending_children(cluster, store, &current).await;
    }
    Ok(step)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReconcileStep {
    pub status_changed: bool,
    pub artifacts_registered: usize,
}

pub async fn reconcile(cluster: &ClusterConfig, store: &Store) -> Result<ReconcileReport> {
    let mut runs_reconciled = 0;
    let mut artifacts_registered = 0;
    // Scope to the invoking user's own runs. `labctl reconcile` is a
    // user action — folding the user's own SLURM state back into the
    // registry — and must never touch another user's run rows.
    let submitted_by = crate::store::current_user()?;
    for run in store.list_active_runs(&submitted_by).await? {
        let step = reconcile_one(cluster, store, &run).await?;
        if step.status_changed {
            runs_reconciled += 1;
        }
        artifacts_registered += step.artifacts_registered;
    }
    // try_submit_pending_children inside reconcile_one only fires when
    // this pass observed the parent's status transition. Restart between
    // transition and sweep leaves children orphaned in 'created'; idempotent
    // retroactive sweep closes the gap.
    for parent in store
        .list_terminal_runs_with_pending_children(&submitted_by)
        .await?
    {
        if let Err(e) = try_submit_pending_children(cluster, store, &parent).await {
            eprintln!("reconcile: orphan sweep for {} failed: {e:#}", parent.id);
        }
    }
    Ok(ReconcileReport {
        runs_reconciled,
        artifacts_registered,
    })
}


pub async fn gc(
    _cluster: &ClusterConfig,
    store: &Store,
    terminal_snapshots: bool,
) -> Result<usize> {
    if !terminal_snapshots {
        return Ok(0);
    }
    gc_terminal_sources(store, 0).await
}

/// Reap `<runs_base>/runs/<user>/<id>/` dirs that have no corresponding
/// row in the `runs` table. Catches the failure mode where a compute
/// job (or a labctl invocation) created the run-dir on NFS but died
/// before `insert_run` reached commit, leaving an orphan tree that no
/// dispatcher will ever touch.
///
/// Scoped to the agent's own user. Cross-user reaping would compete
/// with that user's daemon and risk racing on a freshly-created dir
/// that hasn't been inserted yet from their CLI process. `min_age` is
/// the safety cushion: any dir whose mtime is younger than this is
/// skipped — covers the brief window between mkdir and the `insert_run`
/// commit on the local user's own machine too.
pub async fn gc_orphan_run_dirs(
    cluster: &ClusterConfig,
    store: &Store,
    min_age_secs: u64,
) -> Result<usize> {
    let user = crate::store::current_user()?;
    let user_root = fs_layout::user_runs_dir(&cluster.filesystem.runs_base, &user);
    let mut entries = match tokio::fs::read_dir(&user_root).await {
        Ok(e) => e,
        // Missing dir on a fresh deployment is fine; no orphans by
        // definition.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| {
            format!("gc_orphan_run_dirs: read_dir {}", user_root.display())
        }),
    };

    let now_sys = std::time::SystemTime::now();
    let min_age = std::time::Duration::from_secs(min_age_secs);
    let mut removed = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = entry.metadata().await?;
        if !metadata.is_dir() {
            continue;
        }
        // The run-id is the directory name; if the name isn't UTF-8
        // it can't be a labctl-created run-dir, leave alone.
        let Some(run_id) = path.file_name().and_then(|s| s.to_str()).map(str::to_string) else {
            continue;
        };
        if store.get_run_optional(&run_id).await?.is_some() {
            continue;
        }
        // No PG row — but the dir might be in-flight (created by a
        // CLI process that hasn't committed yet). Require it to have
        // been quiet for at least `min_age`.
        let mtime = metadata
            .modified()
            .with_context(|| format!("mtime of {}", path.display()))?;
        let age = now_sys.duration_since(mtime).unwrap_or_default();
        if age < min_age {
            continue;
        }
        tokio::fs::remove_dir_all(&path).await.with_context(|| {
            format!("gc_orphan_run_dirs: remove_dir_all {}", path.display())
        })?;
        removed += 1;
    }
    Ok(removed)
}

/// Reap `source/` for terminal runs whose `finished_at` is older than
/// `min_terminal_age_secs`. `.lab/provenance/<repo>/` (git HEAD + diff +
/// uv.lock copy) is kept regardless, so reproducibility survives.
///
/// Used by the CLI (`gc()` wrapper, min_age=0) and by the agent's
/// gc_loop (min_age>0, gives reconcile time to finalize artifacts
/// before the working tree is removed).
pub async fn gc_terminal_sources(store: &Store, min_terminal_age_secs: u64) -> Result<usize> {
    let now = util::now_ts();
    let cutoff = min_terminal_age_secs as i64;
    let mut removed = 0;
    for run in store.list_runs().await? {
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
        tokio::fs::remove_dir_all(&run.source_path)
            .await
            .with_context(|| format!("failed to remove {}", run.source_path.display()))?;
        removed += 1;
    }
    Ok(removed)
}

async fn resolve_inputs(
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
                let artifact = store.resolve_artifact_ref(artifact).await?;
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
                let artifact = store.resolve_artifact_ref(artifact_ref).await?;
                InputResolution {
                    role: role.clone(),
                    artifact_id: Some(artifact.id),
                    resolved_path: artifact.path,
                }
            }
            InputSpec::From { role: pinned_role } => {
                let ctx = stage_ctx.with_context(|| {
                    format!(
                        "input {role:?} is type=from but recipe is being submitted \
                         outside of a pipeline"
                    )
                })?;
                let artifact = ctx.pinned_outputs.get(pinned_role).with_context(|| {
                    format!(
                        "input {role:?} is type=from role={pinned_role:?} but the \
                         pipeline's `from` run produced no output named {pinned_role:?}"
                    )
                })?;
                InputResolution {
                    role: role.clone(),
                    artifact_id: Some(artifact.id.clone()),
                    resolved_path: artifact.path.clone(),
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
                // If the upstream stage has already linked its output for
                // `parent_role` (cache-hit / cache-hit-by-coalesce satisfied
                // during the same submit_pipeline topo walk), wire the
                // artifact_id directly and pin `resolved_path` to the
                // artifact's actual on-disk location — which may live
                // under a different user's dir when the cache hit landed
                // on a peer's prior run. Otherwise leave artifact_id NULL
                // and fall back to the predicted-user path; the upstream
                // will fill both in when its output materializes (via
                // Store::insert_artifact at normal completion, or
                // Store::copy_run_outputs → backfill_stage_consumers on
                // cache-hit / coalesced-follower).
                let artifact_id =
                    store.run_output_artifact_id(parent_run_id, parent_role).await?;
                let resolved_path = match &artifact_id {
                    Some(aid) => store.get_artifact(aid).await?.path,
                    None => fs_layout::artifact_dir(parent_root, submitted_by, &parent_alias),
                };
                InputResolution {
                    role: role.clone(),
                    artifact_id,
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
        // Pipeline-stage parents do not show up here under agent-driven
        // submission — the agent only renders a child's script once
        // every parent is terminal-succeeded, so no afterok is needed.
        // What DOES still come through `parent_job_ids` is sweep-array
        // → sweep-aggregate dependency: the aggregate stage runs after
        // every array element succeeds, gated by SLURM's native
        // afterok:<array_jobid> semantics.
        if !parent_job_ids.is_empty() {
            script.push_str(&format!(
                "#SBATCH --dependency=afterok:{}\n",
                parent_job_ids.join(":")
            ));
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

    script.push_str("\nset -uo pipefail\n");

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
    script.push_str(
        "\nrc=$?\n",
    );
    script.push_str("exit \"$rc\"\n");
    // sacct is the sole source of truth for job state; bash wrapper exits
    // with the user command's rc and SLURM/sacct records the outcome.
    // Compute has no PG-bound responsibilities — outputs land at their
    // declared paths and login-side reconcile walks + registers them.
    Ok(script)
}

async fn submit_script(
    cluster: &ClusterConfig,
    script_path: &Path,
    run_id: &str,
) -> Result<String> {
    match cluster.scheduler.kind {
        SchedulerKind::Slurm => {
            let output = Command::new(&cluster.scheduler.sbatch)
                .arg("--parsable")
                .arg(script_path)
                .output()
                .await
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
                .await
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
async fn scheduler_outcome(
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
        .await
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

async fn register_outputs(store: &Store, run: &crate::store::RunRow) -> Result<usize> {
    // Pending-placeholder rows (inserted by `insert_pending_pipeline_stage`
    // for intra-pipeline children whose parents haven't materialised yet)
    // carry no `outputs` in `context_json` — the stage hasn't been rendered.
    // Nothing to register; skip cleanly so the reconcile cascade can still
    // reach `try_submit_pending_children`.
    let Some(outputs_value) = run.context_json.get("outputs").cloned() else {
        return Ok(0);
    };
    let outputs: BTreeMap<String, OutputResolution> = serde_json::from_value(outputs_value)
        .with_context(|| format!("run {} context.json 'outputs' shape mismatch", run.id))?;
    let mut count = 0;
    // `(role, artifact_id)` for non-streaming outputs we linked this pass.
    // Used after the loop to backfill downstream pipeline-stage consumers
    // structurally — mirrors the wiring `copy_run_outputs` does for
    // cache-hit / coalesced-follower. Streaming-checkpoint outputs are
    // intentionally excluded: a downstream `type=stage` input would
    // reference the stream root, not a specific step, so per-step
    // artifacts have no chain-input consumer to wire.
    let mut linked_outputs: Vec<(String, String)> = Vec::new();
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
                let marker = resolution.marker.as_deref().expect(
                    "checkpoint_stream marker is validated in OutputSpec",
                );
                if !step_dir.join(marker).exists() {
                    continue;
                }
                // Short-circuit: if we've already registered an artifact
                // for exactly this path, skip re-insert. Each per-step
                // checkpoint dir is path-unique (the orbax ``<step>``
                // subdir is a content-stable artifact), and `Store::
                // insert_artifact` derives the id from that path
                // canonically — so an existing row at this path would
                // satisfy a fresh insert too, but skipping avoids the
                // PG round-trip.
                if let Some(existing) = store.find_artifact_by_path("checkpoint", &step_dir).await? {
                    store.link_run_output(&run.id, role, &existing.id).await?;
                    continue;
                }
                let artifact = store
                    .insert_artifact(
                        "checkpoint",
                        &step_dir,
                        Some(&run.id),
                        &json!({
                            "role": role,
                            "step": step,
                            "marker": marker,
                            "stream_alias": resolution.alias,
                            "producer_recipe": run.recipe_name,
                        }),
                    )
                    .await?;
                store.link_run_output(&run.id, role, &artifact.id).await?;
                count += 1;
            }
        } else {
            let ready = match &resolution.marker {
                Some(m) => resolution.path.join(m).is_file(),
                None => run.status == "succeeded",
            };
            if !ready {
                continue;
            }
            let mut metadata = json!({
                "role": role,
                "producer_recipe": run.recipe_name,
            });
            if let Some(m) = &resolution.marker {
                metadata["marker"] = json!(m);
                if resolution.kind == "eval_result" {
                    if let Ok(text) = fs::read_to_string(resolution.path.join(m)) {
                        if let Ok(value) = serde_json::from_str::<Value>(&text) {
                            metadata["result"] = value;
                        }
                    }
                }
            }
            let artifact = store
                .insert_artifact(
                    &resolution.kind,
                    &resolution.path,
                    Some(&run.id),
                    &metadata,
                )
                .await?;
            store.link_run_output(&run.id, role, &artifact.id).await?;
            store.set_alias(&resolution.alias, &artifact.id).await?;
            linked_outputs.push((role.clone(), artifact.id.clone()));
            count += 1;
        }
    }
    if !linked_outputs.is_empty() {
        store
            .backfill_stage_consumers(&run.id, &linked_outputs)
            .await?;
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
