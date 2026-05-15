//! `labctl init` — bootstrap a cluster.toml for a new site.
//!
//! Two-step user flow: write a config file (this module), then run
//! `labctl --cluster <file> doctor` to verify. Init never touches the
//! filesystem-truth registry or shells out to the scheduler — its only
//! side effect is writing one TOML file.
//!
//! Inputs:
//!   - `--from <other.toml>`: copy the schema from an existing cluster
//!     config (typically scp'd over from another site). All paths get
//!     surfaced to the user verbatim — they'll need to edit them — but
//!     the rest of the structure (artifact kinds, repo names, dispatch
//!     intervals, throttle, env) carries over verbatim. This is the
//!     "import the config from this cluster" workflow.
//!   - `--name`, `--runs-base`, `--artifact-root <kind=path>` (repeatable),
//!     `--repo <name=path>` (repeatable): point overrides applied on
//!     top of the base (whether `--from` or the built-in skeleton).
//!   - `--no-detect`: skip the SLURM probes.
//!
//! Auto-detect: best-effort. `sinfo`, `sacctmgr`, `scontrol show config`
//! are probed for partition / QoS / GresTypes. Any probe that fails
//! (binary missing, non-SLURM cluster, permission denied) gets noted in
//! the output but doesn't fail init — the user can still edit the
//! generated file by hand.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};

use crate::config::{
    ClusterConfig, FilesystemConfig, SchedulerConfig, SlurmConfig,
};

pub struct InitOptions {
    pub from: Option<PathBuf>,
    pub name: Option<String>,
    pub runs_base: Option<PathBuf>,
    /// (kind, path) pairs to merge into [filesystem.artifact_roots].
    pub artifact_roots: Vec<(String, PathBuf)>,
    /// (name, path) pairs to merge into [repos].
    pub repos: Vec<(String, PathBuf)>,
    /// Where to write. Default: `cluster.<name>.toml` in CWD.
    pub output: Option<PathBuf>,
    pub force: bool,
    pub no_detect: bool,
}

#[derive(Default)]
struct SlurmProbe {
    partition: Option<String>,
    qos: Option<String>,
    gres_gpu_syntax: Option<String>,
    notes: Vec<String>,
}

pub fn run(opts: InitOptions) -> Result<()> {
    let mut cfg = if let Some(from) = &opts.from {
        load_lax(from)?
    } else {
        skeleton_config(opts.name.as_deref())
    };

    if let Some(name) = &opts.name {
        cfg.name = name.clone();
    }
    if let Some(rb) = &opts.runs_base {
        cfg.filesystem.runs_base = rb.clone();
    }
    for (kind, path) in &opts.artifact_roots {
        cfg.filesystem.artifact_roots.insert(kind.clone(), path.clone());
    }
    for (name, path) in &opts.repos {
        cfg.repos.insert(name.clone(), path.clone());
    }

    let probe = if opts.no_detect {
        SlurmProbe::default()
    } else {
        slurm_probe()
    };
    // Auto-detect fills gaps only — never overwrites explicit values from
    // --from or flags. The user's existing choices win.
    if cfg.slurm.partition.is_none() {
        cfg.slurm.partition = probe.partition.clone();
    }
    if cfg.slurm.qos.is_none() {
        cfg.slurm.qos = probe.qos.clone();
    }
    if cfg.slurm.gres_gpu_syntax.is_none() {
        cfg.slurm.gres_gpu_syntax = probe.gres_gpu_syntax.clone();
    }

    let output = opts
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("cluster.{}.toml", cfg.name)));
    if output.exists() && !opts.force {
        bail!(
            "{} already exists; pass --force to overwrite",
            output.display(),
        );
    }

    let body = serialize_config(&cfg, opts.from.as_deref())?;
    std::fs::write(&output, body)
        .with_context(|| format!("failed to write {}", output.display()))?;

    print_summary(&output, &cfg, &probe);
    Ok(())
}

/// Parse a cluster.toml without running ClusterConfig::load's
/// post-deserialize validation. `--from` typically points at a foreign
/// cluster config whose paths don't exist locally — load() would bail
/// on a missing policies_dir or empty artifact_roots, but we want the
/// raw structure to copy forward, not the locally-resolved one. Path
/// canonicalization is also skipped so the foreign paths get surfaced
/// verbatim, making it obvious which ones the user needs to rewrite.
fn load_lax(path: &Path) -> Result<ClusterConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let cfg: ClusterConfig = toml::from_str(&text)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(cfg)
}

fn skeleton_config(name: Option<&str>) -> ClusterConfig {
    // Pre-seed the canonical artifact kinds so the generated file
    // round-trips through ClusterConfig::load (which bails on empty
    // artifact_roots). Users delete or rename kinds they don't need.
    let mut artifact_roots = BTreeMap::new();
    artifact_roots.insert("dataset".to_string(),     PathBuf::from("/path/to/datasets"));
    artifact_roots.insert("checkpoint".to_string(),  PathBuf::from("/path/to/checkpoints"));
    artifact_roots.insert("eval_result".to_string(), PathBuf::from("/path/to/eval_logs"));
    ClusterConfig {
        name: name.unwrap_or("untitled").to_string(),
        filesystem: FilesystemConfig {
            runs_base: PathBuf::from("/path/to/labctl_runs"),
            artifact_roots,
            output_roots: BTreeMap::new(),
        },
        repos: BTreeMap::new(),
        env: BTreeMap::new(),
        modules: Vec::new(),
        scheduler: SchedulerConfig::default(),
        slurm: SlurmConfig::default(),
        dispatch: None,
        remote: None,
    }
}

/// Probe local SLURM for partition / QoS / gres syntax. Each probe is
/// best-effort: missing binary, non-SLURM cluster, or permission-denied
/// all degrade to a note in the output rather than an error.
fn slurm_probe() -> SlurmProbe {
    let mut probe = SlurmProbe::default();

    match Command::new("sinfo").args(["-h", "-o", "%R"]).output() {
        Ok(out) if out.status.success() => {
            let parts: Vec<String> = String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !parts.is_empty() {
                probe.notes.push(format!("sinfo partitions: {}", parts.join(", ")));
                // Best-effort heuristic — first listed partition. Sites
                // with a default marker get the right answer; others
                // need a manual edit. The probe note shows the full
                // list either way.
                probe.partition = Some(parts[0].clone());
            }
        }
        Ok(_) => probe.notes.push("sinfo exited non-zero — skipped".to_string()),
        Err(_) => probe.notes.push("sinfo not on $PATH — skipped".to_string()),
    }

    match Command::new("sacctmgr")
        .args(["-nP", "list", "qos", "format=Name"])
        .output()
    {
        Ok(out) if out.status.success() => {
            let qoss: Vec<String> = String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty() && s != "normal")
                .collect();
            if !qoss.is_empty() {
                probe.notes.push(format!("sacctmgr QoS: {}", qoss.join(", ")));
                probe.qos = Some(qoss[0].clone());
            }
        }
        Ok(_) => probe.notes.push("sacctmgr exited non-zero — skipped".to_string()),
        Err(_) => probe.notes.push("sacctmgr not on $PATH — skipped".to_string()),
    }

    match Command::new("scontrol").args(["show", "config"]).output() {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("GresTypes") {
                    if let Some(eq) = rest.find('=') {
                        let types = rest[eq + 1..].trim().to_string();
                        if !types.is_empty() && types != "(null)" {
                            probe.notes.push(format!("scontrol GresTypes: {types}"));
                            if types.split(',').any(|t| t.trim() == "gpu") {
                                // Default to bare gpu:{n}; sites with
                                // typed GPUs (gpu:a100:{n}) will need a
                                // manual edit, the note above hints at it.
                                probe.gres_gpu_syntax = Some("gpu:{n}".to_string());
                            }
                        }
                    }
                    break;
                }
            }
        }
        Ok(_) => probe.notes.push("scontrol exited non-zero — skipped".to_string()),
        Err(_) => probe.notes.push("scontrol not on $PATH — skipped".to_string()),
    }

    probe
}

fn serialize_config(cfg: &ClusterConfig, copied_from: Option<&Path>) -> Result<String> {
    let from_note = match copied_from {
        Some(p) => format!(
            "# Copied from {} and adapted by `labctl init`. Edit paths as needed.\n",
            p.display(),
        ),
        None => "# Generated by `labctl init`. Edit paths as needed.\n".to_string(),
    };
    let header = format!(
        "{from_note}# Run `labctl --cluster <this-file> doctor` to verify writability,\n\
         # scheduler reachability, and (if installed) systemd unit status.\n\n"
    );
    let body = toml::to_string_pretty(cfg).context("failed to serialize cluster config")?;
    Ok(format!("{header}{body}"))
}

fn print_summary(output: &Path, cfg: &ClusterConfig, probe: &SlurmProbe) {
    println!("wrote {}", output.display());
    println!("  name:       {}", cfg.name);
    println!("  runs_base:  {}", cfg.filesystem.runs_base.display());
    if !cfg.filesystem.artifact_roots.is_empty() {
        let kinds: Vec<&str> = cfg
            .filesystem
            .artifact_roots
            .keys()
            .map(|s| s.as_str())
            .collect();
        println!("  artifacts:  {}", kinds.join(", "));
    }
    if !cfg.repos.is_empty() {
        let repos: Vec<&str> = cfg.repos.keys().map(|s| s.as_str()).collect();
        println!("  repos:      {}", repos.join(", "));
    }
    if let Some(p) = &cfg.slurm.partition {
        println!("  partition:  {p}");
    }
    if let Some(q) = &cfg.slurm.qos {
        println!("  qos:        {q}");
    }
    if let Some(g) = &cfg.slurm.gres_gpu_syntax {
        println!("  gres:       {g}");
    }
    if !probe.notes.is_empty() {
        println!();
        println!("SLURM probe:");
        for note in &probe.notes {
            println!("  {note}");
        }
    }
    println!();
    println!("Next steps:");
    println!("  1. Edit {} — fill in real paths (any /path/to/... is a placeholder).", output.display());
    println!("  2. labctl --cluster {} doctor", output.display());
}
