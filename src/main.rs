mod artifacts;
mod client;
mod config;
#[cfg(feature = "ui")]
mod dispatch;
mod evald;
mod provenance;
mod runner;
#[cfg(feature = "ui")]
mod server;
mod service;
mod store;
mod template;
mod tracking;
mod util;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

/// Compile-time version stamp. Crate version + short git SHA captured by
/// build.rs so `labctl --version` identifies the exact build.
const BUILD_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("LABCTL_GIT_SHA"),
    ")"
);

#[derive(Parser)]
#[command(name = "labctl")]
#[command(version = BUILD_VERSION)]
#[command(about = "Reproducible lab run envelope and artifact lineage control plane")]
struct Cli {
    #[arg(long, global = true, default_value = "labctl.toml")]
    cluster: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run {
        recipe: PathBuf,
    },
    RunPipeline {
        pipeline: PathBuf,
    },
    PipelineShow {
        id: String,
    },
    /// Parse and semantically validate a recipe, pipeline, or eval policy.
    /// Does not submit, does not touch the registry.
    Validate {
        path: PathBuf,
    },
    Reconcile,
    Status,
    Show {
        id: String,
    },
    Gc {
        #[arg(long)]
        terminal_snapshots: bool,
    },
    RegisterExternal {
        #[arg(long)]
        alias: String,
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value = "external")]
        kind: String,
    },
    Evald {
        #[command(subcommand)]
        command: EvaldCommand,
    },
    /// Backfill tracking rows for legacy runs by scanning their log files.
    /// Idempotent — only touches runs that don't already have a tracking row.
    BackfillTracking,
    /// Run register_outputs against terminal runs that lack any registered
    /// output rows. Recovers runs whose outputs went unregistered because
    /// of the pre-fix terminal-transition bug. Idempotent.
    RecoverOutputs,
    /// Recompute `finished_at` for terminal runs from sacct's End field
    /// (or status.json's updated_at as fallback). Use this after upgrading
    /// past the bug where finished_at was set to wall-clock time at
    /// reconcile observation rather than the actual job end time.
    RepairFinishTimes,
    /// Run a self-check: cluster config, filesystem perms, scheduler
    /// availability, and systemd unit status. Use this before reporting a
    /// problem so the report includes the environment state.
    Doctor,
    /// Manage the systemd user service that keeps `labctl serve` alive.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Serve the web UI on the given address.
    /// Bind 127.0.0.1 and reach it via SSH tunnel from your laptop.
    /// When `[dispatch]` is set in the cluster config, also runs the
    /// reconcile + evald + throttle loops as in-process tokio tasks
    /// (use --no-dispatch to suppress for an ad-hoc read-only view).
    #[cfg(feature = "ui")]
    Serve {
        #[arg(long, default_value = "127.0.0.1:8765")]
        bind: String,
        /// Skip the dispatch loop even when [dispatch] is configured.
        #[arg(long)]
        no_dispatch: bool,
    },
}

#[derive(Subcommand)]
enum EvaldCommand {
    Once { policy: PathBuf },
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// Generate a systemd user unit that runs `labctl serve` (with
    /// dispatch enabled if [dispatch] is set in the cluster config),
    /// enable it, and start it. Survives logout when linger is enabled.
    Install {
        #[arg(long, default_value = "127.0.0.1:8765")]
        bind: String,
        /// Override the unit name (default: labctl). Useful when
        /// installing units for multiple clusters on the same host.
        #[arg(long, default_value = "labctl")]
        name: String,
        /// Overwrite an existing unit file with the same name.
        #[arg(long)]
        force: bool,
    },
    /// Stop, disable, and remove the unit.
    Uninstall {
        #[arg(long, default_value = "labctl")]
        name: String,
    },
    /// Pass through to `systemctl --user status <name>`.
    Status {
        #[arg(long, default_value = "labctl")]
        name: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Validate is pure file inspection — no cluster, no registry.
    if let Command::Validate { path } = &cli.command {
        return validate_path(path);
    }
    // Service install/uninstall/status only needs the cluster path
    // (passed through into the unit's ExecStart) and the running
    // binary's absolute path. We deliberately skip cluster-config
    // validation: a user installing the service for the first time
    // shouldn't have to fix every TOML detail before they can install,
    // and the unit just delegates back to `labctl serve` which will
    // re-validate on every startup.
    // Doctor must work even when the cluster config doesn't load — that's
    // exactly the situation it's here to diagnose.
    if let Command::Doctor = &cli.command {
        return run_doctor(&cli.cluster);
    }
    if let Command::Service { command } = cli.command {
        return match command {
            ServiceCommand::Install { bind, name, force } => {
                let mut opts = service::InstallOptions::new(cli.cluster);
                opts.bind = bind;
                opts.unit_name = name;
                opts.force = force;
                service::install(opts)
            }
            ServiceCommand::Uninstall { name } => service::uninstall(&name),
            ServiceCommand::Status { name } => service::status(&name),
        };
    }
    // Run and RunPipeline are handled before opening the local registry —
    // the daemon owns every write. CLI invocations on a shared registry
    // would race on POSIX locks (broken on parallel filesystems) and
    // corrupt the DB.
    if let Command::Run { recipe } = &cli.command {
        return run_recipe_command(&cli.cluster, recipe);
    }
    if let Command::RunPipeline { pipeline } = &cli.command {
        return run_pipeline_command(&cli.cluster, pipeline);
    }

    let cluster = config::ClusterConfig::load(&cli.cluster)?;
    let mut store = store::Store::open(&cluster.filesystem.registry_db)?;

    match cli.command {
        Command::PipelineShow { id } => {
            let pipeline = store
                .get_pipeline(&id)?
                .with_context(|| format!("pipeline not found: {id}"))?;
            let runs = store.list_pipeline_runs(&id)?;
            let view = serde_json::json!({
                "pipeline": {
                    "id": pipeline.id,
                    "name": pipeline.name,
                    "pipeline_path": pipeline.pipeline_path,
                    "created_at": pipeline.created_at,
                },
                "stages": runs.iter().map(|r| serde_json::json!({
                    "stage_name": r.stage_name,
                    "run_id": r.id,
                    "job_id": r.job_id,
                    "status": r.status,
                    "dependency_on": r.dependency_on,
                    "run_dir": r.run_dir,
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        Command::Reconcile => {
            let report = runner::reconcile(&cluster, &mut store)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Status => {
            for run in store.list_runs()? {
                println!(
                    "{:<28} {:<12} {:<10} {}",
                    run.id,
                    run.status,
                    run.job_id.unwrap_or_else(|| "-".to_string()),
                    run.recipe_name
                );
            }
        }
        Command::Show { id } => {
            let view = store.run_view(&id)?;
            println!("{}", serde_json::to_string_pretty(&view)?);
        }
        Command::Gc { terminal_snapshots } => {
            let removed = runner::gc(&cluster, &mut store, terminal_snapshots)?;
            println!("removed_snapshots: {removed}");
        }
        Command::RegisterExternal { alias, path, kind } => {
            let artifact = artifacts::register_external(&mut store, &alias, &path, &kind)?;
            println!("artifact_id: {}", artifact.id);
            println!("alias: {alias}");
        }
        Command::BackfillTracking => {
            let report = tracking::backfill(&cluster, &mut store)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::RecoverOutputs => {
            let report = runner::recover_outputs(&cluster, &mut store)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::RepairFinishTimes => {
            let report = runner::repair_finish_times(&cluster, &mut store)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Evald { command } => match command {
            EvaldCommand::Once { policy } => {
                let policy = config::EvalPolicy::load(&policy)?;
                let report = evald::run_once(&cluster, &mut store, &policy)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
        #[cfg(feature = "ui")]
        Command::Serve { bind, no_dispatch } => {
            let addr: std::net::SocketAddr = bind
                .parse()
                .with_context(|| format!("invalid --bind address {bind:?}"))?;
            server::serve(cluster, store, addr, no_dispatch)?;
        }
        Command::Validate { .. }
        | Command::Service { .. }
        | Command::Doctor
        | Command::Run { .. }
        | Command::RunPipeline { .. } => {
            unreachable!("handled above")
        }
    }

    Ok(())
}

/// Self-check intended for first-time setup and bug reports. Never bails:
/// every failure becomes a "FAIL" line so the user sees the whole picture.
/// Exits with code 1 if any check failed, 0 otherwise.
fn run_doctor(cluster_path: &PathBuf) -> Result<()> {
    use std::process::Command as Cmd;
    let mut failed = 0usize;
    let mut emit = |label: &str, ok: bool, detail: &str| {
        let mark = if ok { "OK  " } else { "FAIL" };
        println!("[{mark}] {label:<32} {detail}");
        if !ok {
            failed += 1;
        }
    };

    println!("labctl {BUILD_VERSION}");
    println!("cluster config: {}", cluster_path.display());
    println!();

    let cluster = match config::ClusterConfig::load(cluster_path) {
        Ok(c) => {
            emit("cluster config", true, &format!("loaded ({} repos)", c.repos.len()));
            Some(c)
        }
        Err(e) => {
            emit("cluster config", false, &format!("{e:#}"));
            None
        }
    };

    if let Some(cluster) = &cluster {
        let runs_base = &cluster.filesystem.runs_base;
        let runs_ok = runs_base.is_dir() && writable_dir(runs_base);
        emit(
            "runs_base writable",
            runs_ok,
            &runs_base.display().to_string(),
        );

        let registry_db = &cluster.filesystem.registry_db;
        let parent_ok = registry_db
            .parent()
            .map(|p| p.is_dir() && writable_dir(p))
            .unwrap_or(false);
        emit(
            "registry_db parent dir",
            parent_ok,
            &registry_db.display().to_string(),
        );

        match store::Store::open(registry_db) {
            Ok(_) => emit("registry_db open", true, "ok"),
            Err(e) => emit("registry_db open", false, &format!("{e:#}")),
        }

        for (kind, root) in &cluster.filesystem.artifact_roots {
            let ok = root.is_dir() && writable_dir(root);
            emit(
                &format!("artifact_root[{kind}]"),
                ok,
                &root.display().to_string(),
            );
        }

        let sched_kind = format!("{:?}", cluster.scheduler.kind);
        let sacct_ok = which::which(&cluster.scheduler.sacct).is_ok();
        emit(
            "scheduler.sacct",
            sacct_ok,
            &format!("{} ({})", cluster.scheduler.sacct, sched_kind),
        );
        let sbatch_ok = which::which(&cluster.scheduler.sbatch).is_ok();
        emit(
            "scheduler.sbatch",
            sbatch_ok,
            &cluster.scheduler.sbatch.clone(),
        );

        match &cluster.dispatch {
            Some(d) => emit(
                "dispatch config",
                true,
                &format!(
                    "reconcile={}s evald={}s policies_dir={}",
                    d.reconcile_interval_secs,
                    d.evald_interval_secs,
                    d.policies_dir.display(),
                ),
            ),
            None => emit(
                "dispatch config",
                true,
                "absent (UI is read-only; reconcile + evald won't run)",
            ),
        }
    }

    let systemd_ok = service::systemd_available();
    emit(
        "systemctl --user",
        systemd_ok,
        if systemd_ok {
            "available"
        } else {
            "not available (service install/status will not work)"
        },
    );
    let unit_installed = service::is_installed("labctl");
    if systemd_ok {
        if unit_installed {
            let active = Cmd::new("systemctl")
                .args(["--user", "is-active", "labctl"])
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .unwrap_or_else(|| "unknown".into());
            emit("labctl service unit", true, &format!("installed ({active})"));
        } else {
            emit(
                "labctl service unit",
                true,
                "not installed (run `labctl service install`)",
            );
        }
    }

    println!();
    if failed == 0 {
        println!("doctor: all checks passed");
        Ok(())
    } else {
        println!("doctor: {failed} check(s) failed");
        std::process::exit(1);
    }
}

fn writable_dir(path: &std::path::Path) -> bool {
    // SAFETY: tempfile inside the dir is the only portable way to learn
    // "can I write here". W_OK from access(2) lies about ACL/quota.
    let probe = path.join(format!(".labctl-doctor-probe-{}", std::process::id()));
    let ok = std::fs::write(&probe, b"").is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

/// One-line nudge after `labctl run` succeeds, when the cluster is
/// configured for in-process dispatch but the user hasn't installed the
/// systemd unit. Suppressed by `LABCTL_NO_HINT=1` and skipped on hosts
/// without systemd-user. Designed to be ignorable — never blocks, never
/// errors.
/// Submit a recipe. Parses the TOML locally so the daemon doesn't need
/// read access to the user's filesystem, then POSTs the resolved Recipe
/// to the configured server. The daemon owns the registry write.
fn run_recipe_command(cluster_path: &PathBuf, recipe_path: &PathBuf) -> Result<()> {
    let cluster = config::ClusterConfig::load(cluster_path)?;
    let recipe = config::Recipe::load(recipe_path)?;
    let submitted = client::submit_recipe(&cluster.server, &recipe)?;
    println!("run_id: {}", submitted.run_id);
    println!("job_id: {}", submitted.job_id);
    println!("run_dir: {}", submitted.run_dir.display());
    Ok(())
}

fn run_pipeline_command(cluster_path: &PathBuf, pipeline_path: &PathBuf) -> Result<()> {
    let cluster = config::ClusterConfig::load(cluster_path)?;
    let loaded = config::Pipeline::load(pipeline_path)?;
    let submitted = client::submit_pipeline(&cluster.server, &loaded, Some(pipeline_path))?;
    println!("{}", serde_json::to_string_pretty(&submitted)?);
    Ok(())
}

fn validate_path(path: &PathBuf) -> Result<()> {
    // Heuristic: routes to the right loader by content. We try pipeline first
    // (it's the most structured); if that fails to parse as a pipeline, try
    // policy, then recipe.
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    let raw: toml::Value = toml::from_str(&text)
        .with_context(|| format!("invalid TOML in {}", path.display()))?;
    let raw = raw
        .as_table()
        .with_context(|| format!("{} is not a TOML table", path.display()))?;

    if raw.contains_key("filesystem") {
        let cluster = config::ClusterConfig::load(path)?;
        println!(
            "cluster {:?} OK ({} repo(s), runs_base={})",
            cluster.name,
            cluster.repos.len(),
            cluster.filesystem.runs_base.display(),
        );
    } else if raw.contains_key("stages") {
        let loaded = config::Pipeline::load(path)?;
        println!(
            "pipeline {:?} OK ({} stages, topo={:?})",
            loaded.name,
            loaded.stages.len(),
            loaded.topo_order,
        );
    } else if raw.contains_key("applies_to") {
        let policy = config::EvalPolicy::load(path)?;
        println!(
            "policy {:?} OK (recipe={})",
            policy.name,
            policy.recipe.display(),
        );
    } else {
        let recipe = config::Recipe::load(path)?;
        println!(
            "recipe {:?} OK (repo={}, {} inputs, {} outputs)",
            recipe.name,
            recipe.repo,
            recipe.inputs.len(),
            recipe.outputs.len(),
        );
    }
    Ok(())
}
