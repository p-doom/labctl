use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
    pub name: String,
    pub filesystem: FilesystemConfig,
    #[serde(default)]
    pub repos: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub modules: Vec<String>,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub slurm: SlurmConfig,
    /// Optional in-process dispatch loop. When present and not overridden
    /// by `--no-dispatch`, `labctl serve` runs reconcile + evald + throttle
    /// in tokio tasks. When absent, serve stays a read-only viewer.
    pub dispatch: Option<DispatchConfig>,
    /// Where the CLI sends submissions. The daemon (`labctl serve`) is the
    /// only writer to the registry, so `labctl run` and `labctl run-pipeline`
    /// POST here by default; the `--local` flag bypasses this for solo use.
    /// Defaults to ``http://127.0.0.1:8765`` to match the default `--bind`.
    #[serde(default)]
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Daemon base URL. Trailing slash is tolerated.
    pub url: String,
    /// HTTP request timeout in seconds for CLI → daemon submission. The
    /// submission path snapshots the source repo before returning, so this
    /// has to accommodate slow filesystems.
    pub timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8765".to_string(),
            timeout_secs: 120,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DispatchConfig {
    /// How often reconcile runs (s). Drives state-sync latency.
    #[serde(default = "default_reconcile_secs")]
    pub reconcile_interval_secs: u64,
    /// How often evald walks the policies dir (s). Independent of
    /// reconcile because eval submission is the load-bearing rate-limit.
    #[serde(default = "default_evald_secs")]
    pub evald_interval_secs: u64,
    /// Path to a directory of `*.toml` eval policies. Resolved relative
    /// to the cluster config file's parent dir if not absolute.
    pub policies_dir: PathBuf,
    /// Optional SLURM-side concurrency cap. Independent so users who
    /// don't want it can omit the whole block.
    pub throttle: Option<ThrottleConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ThrottleConfig {
    /// SLURM job name to count (matches `squeue %j`). Eval recipes name
    /// their jobs after the recipe — this is that name.
    pub job_name: String,
    /// Max running + actively-pending jobs of `job_name`. Excess pending
    /// jobs are held via `scontrol update Hold=yes`; held jobs are
    /// released as running slots free up.
    pub max_concurrent: usize,
}

fn default_reconcile_secs() -> u64 {
    60
}
fn default_evald_secs() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilesystemConfig {
    pub runs_base: PathBuf,
    pub registry_db: PathBuf,
    /// Per-output-kind absolute roots. Every output declared by any recipe
    /// must have its ``kind`` listed here; otherwise submission errors out.
    /// An output of role ``r``, kind ``k``, with rendered alias ``a`` lands
    /// at ``artifact_roots[k]/a/`` — not under ``runs_base``.
    pub artifact_roots: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SchedulerConfig {
    pub kind: SchedulerKind,
    pub sbatch: String,
    pub sacct: String,
    pub scancel: String,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            kind: SchedulerKind::Slurm,
            sbatch: "sbatch".to_string(),
            sacct: "sacct".to_string(),
            scancel: "scancel".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerKind {
    #[default]
    Slurm,
    Local,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SlurmConfig {
    pub partition: Option<String>,
    pub qos: Option<String>,
    pub account: Option<String>,
    pub gres_gpu_syntax: Option<String>,
}

impl ClusterConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read cluster config {}", path.display()))?;
        let mut cfg: Self = toml::from_str(&text)
            .with_context(|| format!("failed to parse cluster config {}", path.display()))?;
        if cfg.filesystem.artifact_roots.is_empty() {
            bail!(
                "cluster {:?}: [filesystem.artifact_roots] must declare at least one \
                 output kind → root mapping; recipes can no longer fall back to \
                 storing outputs under runs_base",
                cfg.name,
            );
        }
        cfg.filesystem.runs_base = absolute(&cfg.filesystem.runs_base)?;
        cfg.filesystem.registry_db = absolute(&cfg.filesystem.registry_db)?;
        for value in cfg.filesystem.artifact_roots.values_mut() {
            *value = absolute(value)?;
        }
        for value in cfg.repos.values_mut() {
            *value = absolute(value)?;
        }
        // Dispatch: resolve policies_dir relative to the cluster file's
        // parent if it's a relative path. That makes `policies_dir =
        // "policies"` work the way users expect.
        if let Some(dispatch) = cfg.dispatch.as_mut() {
            if dispatch.policies_dir.is_relative() {
                let parent = path.parent().unwrap_or_else(|| Path::new("."));
                dispatch.policies_dir = parent.join(&dispatch.policies_dir);
            }
            if !dispatch.policies_dir.is_dir() {
                bail!(
                    "cluster {:?}: [dispatch].policies_dir does not exist or is not \
                     a directory: {}",
                    cfg.name,
                    dispatch.policies_dir.display(),
                );
            }
            if dispatch.reconcile_interval_secs == 0 || dispatch.evald_interval_secs == 0 {
                bail!(
                    "cluster {:?}: [dispatch] interval seconds must be > 0",
                    cfg.name,
                );
            }
            if let Some(t) = &dispatch.throttle {
                if t.max_concurrent == 0 {
                    bail!(
                        "cluster {:?}: [dispatch.throttle].max_concurrent must be > 0",
                        cfg.name,
                    );
                }
                if t.job_name.trim().is_empty() {
                    bail!(
                        "cluster {:?}: [dispatch.throttle].job_name must not be empty",
                        cfg.name,
                    );
                }
            }
        }
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Recipe {
    pub name: String,
    pub repo: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub inputs: BTreeMap<String, InputSpec>,
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputSpec>,
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    #[serde(default)]
    pub args: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub tracking: Tracking,
}

/// Optional experiment-tracking integration. Today: just W&B. The shape is a
/// table-of-tables so the eventual MLflow/TensorBoard story can slot in
/// alongside without breaking recipes.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Tracking {
    pub wandb: Option<WandbTracking>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WandbTracking {
    pub entity: String,
    pub project: String,
    /// Optional W&B group — useful for sweeps, ablation series, etc.
    #[serde(default)]
    pub group: Option<String>,
}

impl Recipe {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read recipe {}", path.display()))?;
        let recipe: Self = toml::from_str(&text)
            .with_context(|| format!("failed to parse recipe {}", path.display()))?;
        recipe.validate()?;
        Ok(recipe)
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("recipe.name must not be empty");
        }
        if self.repo.trim().is_empty() {
            bail!("recipe.repo must not be empty");
        }
        if self.command.is_empty() {
            bail!("recipe.command must not be empty");
        }
        for (role, spec) in &self.outputs {
            if spec.kind.trim().is_empty() {
                bail!("recipe {:?}: output {role:?} has empty type", self.name);
            }
            if spec.marker.trim().is_empty() {
                bail!("recipe {:?}: output {role:?} has empty marker", self.name);
            }
            if spec.alias.trim().is_empty() {
                bail!(
                    "recipe {:?}: output {role:?} requires a non-empty alias \
                     (path resolves to artifact_roots[{:?}] / <alias>)",
                    self.name, spec.kind,
                );
            }
        }
        if let Some(w) = &self.tracking.wandb {
            if w.entity.trim().is_empty() {
                bail!("recipe {:?}: tracking.wandb.entity must not be empty", self.name);
            }
            if w.project.trim().is_empty() {
                bail!("recipe {:?}: tracking.wandb.project must not be empty", self.name);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Resources {
    pub gpus: u32,
    pub cpus: u32,
    pub mem: String,
    pub time: String,
    pub partition: Option<String>,
    pub qos: Option<String>,
    pub account: Option<String>,
    /// Comma-separated list of node names to exclude (SLURM `--exclude=`).
    /// Useful for routing around hosts with intermittent issues without
    /// waiting for SLURM's drain detection.
    pub exclude_nodes: Option<String>,
    /// Escape hatch for sbatch directives the typed schema doesn't cover
    /// (e.g. `--array`, `--nodes`, `--mail-type`, `--gpu-bind`). Each entry
    /// is rendered verbatim as a separate `#SBATCH` line, *after* the
    /// typed directives. Use this only for things labctl can't model —
    /// don't override `--cpus-per-task` or `--time` here, those have
    /// dedicated fields and overriding them confuses the dispatcher.
    /// Lines must start with the bare flag (e.g. `"--array=0-3"`); labctl
    /// prepends the `#SBATCH ` prefix.
    #[serde(default)]
    pub sbatch_extra: Vec<String>,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            gpus: 0,
            cpus: 1,
            mem: "4GB".to_string(),
            time: "00:10:00".to_string(),
            partition: None,
            qos: None,
            account: None,
            exclude_nodes: None,
            sbatch_extra: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputSpec {
    Artifact { artifact: String },
    External { path: PathBuf },
    Checkpoint,
    Stage { stage: String, role: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputSpec {
    #[serde(rename = "type")]
    pub kind: String,
    pub marker: String,
    /// Required. The rendered alias is the directory name under
    /// ``cluster.filesystem.artifact_roots[kind]`` that this output's marker
    /// must land in. Supports the same templating as ``[args]``: at minimum
    /// ``{run.id}``, ``{run.dir}``, ``{params.X}``, and ``{inputs.Y.path}``.
    pub alias: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pipeline {
    pub name: String,
    pub stages: BTreeMap<String, PipelineStage>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PipelineStage {
    pub recipe: PathBuf,
}

/// A pipeline whose stages have been parsed and topologically ordered. The
/// CLI loads this from disk (resolving relative stage paths) and sends it to
/// the daemon over HTTP, which is why it derives serde — the daemon needs to
/// reconstruct the same shape without re-reading the user's filesystem.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadedPipeline {
    pub name: String,
    pub stages: BTreeMap<String, LoadedStage>,
    pub topo_order: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadedStage {
    pub recipe_path: PathBuf,
    pub recipe: Recipe,
    pub parents: Vec<String>,
}

impl Pipeline {
    pub fn load(path: &Path) -> Result<LoadedPipeline> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read pipeline {}", path.display()))?;
        let pipeline: Self = toml::from_str(&text)
            .with_context(|| format!("failed to parse pipeline {}", path.display()))?;
        if pipeline.name.trim().is_empty() {
            bail!("pipeline.name must not be empty");
        }
        if pipeline.stages.is_empty() {
            bail!("pipeline.stages must not be empty");
        }
        let parent_dir = path.parent().unwrap_or_else(|| Path::new("."));

        let mut stages: BTreeMap<String, LoadedStage> = BTreeMap::new();
        for (stage_name, stage) in &pipeline.stages {
            let recipe_path = if stage.recipe.is_absolute() {
                stage.recipe.clone()
            } else {
                parent_dir.join(&stage.recipe)
            };
            let recipe = Recipe::load(&recipe_path).with_context(|| {
                format!("pipeline.stages.{stage_name} failed to load {}", recipe_path.display())
            })?;
            stages.insert(
                stage_name.clone(),
                LoadedStage {
                    recipe_path,
                    recipe,
                    parents: Vec::new(),
                },
            );
        }

        // Derive parent edges from inputs of type Stage.
        for (stage_name, loaded) in stages.clone() {
            for (role, spec) in &loaded.recipe.inputs {
                if let InputSpec::Stage { stage: parent, role: parent_role } = spec {
                    if !stages.contains_key(parent) {
                        bail!(
                            "pipeline.stages.{stage_name}.inputs.{role} references unknown \
                             stage {parent:?}"
                        );
                    }
                    let parent_stage = &stages[parent];
                    if !parent_stage.recipe.outputs.contains_key(parent_role) {
                        bail!(
                            "pipeline.stages.{stage_name}.inputs.{role} references stage \
                             {parent:?} role {parent_role:?} which is not an output of \
                             recipe {}",
                            parent_stage.recipe.name
                        );
                    }
                    stages.get_mut(&stage_name).unwrap().parents.push(parent.clone());
                }
            }
        }

        let topo_order = topo_sort(&stages)?;

        Ok(LoadedPipeline {
            name: pipeline.name,
            stages,
            topo_order,
        })
    }
}

fn topo_sort(stages: &BTreeMap<String, LoadedStage>) -> Result<Vec<String>> {
    let mut indegree: BTreeMap<String, usize> =
        stages.keys().map(|k| (k.clone(), 0)).collect();
    let mut adj: BTreeMap<String, Vec<String>> =
        stages.keys().map(|k| (k.clone(), Vec::new())).collect();
    for (name, stage) in stages {
        for parent in &stage.parents {
            adj.get_mut(parent).unwrap().push(name.clone());
            *indegree.get_mut(name).unwrap() += 1;
        }
    }
    let mut order = Vec::with_capacity(stages.len());
    // BTreeSet for deterministic ordering: ties broken by stage name.
    let mut frontier: std::collections::BTreeSet<String> = indegree
        .iter()
        .filter_map(|(k, &d)| (d == 0).then(|| k.clone()))
        .collect();
    while let Some(node) = frontier.iter().next().cloned() {
        frontier.remove(&node);
        order.push(node.clone());
        for child in adj.get(&node).cloned().unwrap_or_default() {
            let entry = indegree.get_mut(&child).unwrap();
            *entry -= 1;
            if *entry == 0 {
                frontier.insert(child);
            }
        }
    }
    if order.len() != stages.len() {
        let cyclic: Vec<&String> = indegree
            .iter()
            .filter(|&(_, &d)| d > 0)
            .map(|(k, _)| k)
            .collect();
        bail!("pipeline has a cycle involving stages: {cyclic:?}");
    }
    Ok(order)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvalPolicy {
    pub name: String,
    pub recipe: PathBuf,
    pub applies_to: AppliesTo,
    #[serde(default)]
    pub cadence: Cadence,
}

impl EvalPolicy {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read eval policy {}", path.display()))?;
        let mut policy: Self = toml::from_str(&text)
            .with_context(|| format!("failed to parse eval policy {}", path.display()))?;
        if policy.recipe.is_relative() {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            policy.recipe = parent.join(&policy.recipe);
        }
        Ok(policy)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppliesTo {
    #[serde(rename = "type")]
    pub kind: String,
    pub producer_recipe: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Cadence {
    pub every_n_steps: Option<u64>,
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
