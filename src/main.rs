mod artifacts;
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

#[derive(Parser)]
#[command(name = "labctl")]
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

    let cluster = config::ClusterConfig::load(&cli.cluster)?;
    let mut store = store::Store::open(&cluster.filesystem.registry_db)?;

    match cli.command {
        Command::Run { recipe } => {
            let recipe = config::Recipe::load(&recipe)?;
            let submitted = runner::submit_recipe(&cluster, &mut store, &recipe, None)?;
            println!("run_id: {}", submitted.run_id);
            println!("job_id: {}", submitted.job_id);
            println!("run_dir: {}", submitted.run_dir.display());
            maybe_print_service_hint(&cluster);
        }
        Command::RunPipeline { pipeline } => {
            let loaded = config::Pipeline::load(&pipeline)?;
            let submitted = runner::submit_pipeline(
                &cluster,
                &mut store,
                &loaded,
                Some(&pipeline),
            )?;
            println!("{}", serde_json::to_string_pretty(&submitted)?);
        }
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
        Command::Validate { .. } | Command::Service { .. } => {
            unreachable!("handled above")
        }
    }

    Ok(())
}

/// One-line nudge after `labctl run` succeeds, when the cluster is
/// configured for in-process dispatch but the user hasn't installed the
/// systemd unit. Suppressed by `LABCTL_NO_HINT=1` and skipped on hosts
/// without systemd-user. Designed to be ignorable — never blocks, never
/// errors.
fn maybe_print_service_hint(cluster: &config::ClusterConfig) {
    if std::env::var_os("LABCTL_NO_HINT").is_some() {
        return;
    }
    if cluster.dispatch.is_none() {
        return;
    }
    if !service::systemd_available() {
        return;
    }
    if service::is_installed("labctl") {
        return;
    }
    eprintln!(
        "\nTip: dispatch isn't running as a service. Reconcile + evald only \
         run when `labctl serve` is up. To keep them alive across logouts:\n  \
         labctl service install\n(silence this hint with LABCTL_NO_HINT=1)"
    );
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
