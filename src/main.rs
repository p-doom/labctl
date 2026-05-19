mod agent;
mod artifacts;
mod config;
mod evald;
mod fs_layout;
mod init;
mod prompt;
mod provenance;
mod remote;
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
    /// Path to cluster.toml. Default: $LABCTL_CLUSTER, then
    /// $XDG_CONFIG_HOME/labctl/cluster.toml, then ~/.config/labctl/cluster.toml.
    #[arg(long, global = true)]
    cluster: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

fn resolve_cluster_path(arg: Option<PathBuf>) -> PathBuf {
    if let Some(p) = arg {
        return p;
    }
    if let Ok(env) = std::env::var("LABCTL_CLUSTER") {
        if !env.is_empty() {
            return PathBuf::from(env);
        }
    }
    if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
        if !x.is_empty() {
            return PathBuf::from(x).join("labctl").join("cluster.toml");
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home).join(".config/labctl/cluster.toml");
        }
    }
    PathBuf::from("labctl.toml")
}

#[derive(Subcommand)]
enum Command {
    Run {
        recipe: PathBuf,
    },
    /// Submit one independent job per sweep index. The recipe must have a
    /// [sweep] section declaring the arg to vary and the start/end range
    /// (inclusive). Each task is a normal labctl run with full provenance
    /// and status tracking. If [sweep].aggregate is set, a final job is
    /// submitted with afterok dependencies on all task jobs.
    RunSweep {
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
    /// Import a foreign cluster's artifact into the local registry.
    /// Reads the foreign alias + meta sidecars over SSH, rsyncs the
    /// bytes into the local artifact_root, and registers a local
    /// artifact row that preserves the foreign content hash (so
    /// re-importing the same bytes dedupes) and records lineage. The
    /// foreign cluster.toml must declare a [remote] section. OTP-gated
    /// hosts work transparently when ControlMaster is configured in
    /// `~/.ssh/config` — see docs/ONBOARDING.md.
    ImportFromCluster {
        /// Path to the foreign cluster's cluster.toml. Must contain a
        /// [remote] section so labctl knows how to reach it.
        #[arg(long)]
        foreign: PathBuf,
        /// Alias on the foreign cluster to resolve and import.
        #[arg(long)]
        from: String,
        /// Local alias to bind the imported artifact to. Defaults to
        /// the foreign alias name. Use this to avoid collisions or to
        /// namespace ("julich_<alias>") imports.
        #[arg(long)]
        r#as: Option<String>,
        /// Skip the rsync of the artifact bytes. Registers a
        /// metadata-only stub at the local destination. Use only if
        /// you're staging the bytes out-of-band (a shared mount,
        /// manual rsync, etc.).
        #[arg(long)]
        no_copy: bool,
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
    /// Serve the read-only web UI. HTTP-only — no dispatch loops live
    /// here; reconcile + evald + throttle run in `labctl agent` (the
    /// per-user systemd unit `labctl init` installs). Install this as a
    /// long-running unit via `labctl service install --ui`. Binds
    /// 127.0.0.1 by default; reach the UI via an SSH tunnel from your
    /// laptop.
    #[cfg(feature = "ui")]
    Serve {
        #[arg(long, default_value = "127.0.0.1:8765")]
        bind: String,
    },
    /// Run the per-user dispatch loops (reconcile + evald + periodic
    /// refresh) — no HTTP listener. Auto-installed as
    /// `labctl-agent.service` by `labctl init`; this subcommand is the
    /// unit's ExecStart and also runnable directly for debugging.
    Agent,
    /// Full bootstrap for a new-cluster setup: write/adopt a
    /// cluster.toml, pre-create per-user subdirs under runs_base +
    /// artifact_roots, install the per-user systemd agent unit, and
    /// run doctor — interactively by default, scriptable via `--yes`.
    /// Four modes, picked by mutually exclusive flag:
    ///   (default)        greenfield — write a fresh config from a
    ///                    SLURM probe + interactive prompts.
    ///   --use <path>     adopt an existing cluster.toml you wrote.
    ///   --migrate-from <path>
    ///                    copy schema from another cluster, adapt
    ///                    paths interactively.
    ///   --join <path>    join a colleague's shared registry on this
    ///                    cluster. Paths kept verbatim.
    Init {
        /// Adopt an existing cluster.toml as-is. Symlinks it into
        /// the default config location so plain `labctl <cmd>` works.
        #[arg(long, group = "init_source")]
        r#use: Option<PathBuf>,
        /// Adapt a foreign cluster's identity card to this site.
        /// Schema (kinds, repos, dispatch, throttle, env) carries
        /// over; site-local paths are surfaced for interactive edit.
        #[arg(long, group = "init_source")]
        migrate_from: Option<PathBuf>,
        /// Join a colleague's shared registry on this cluster.
        /// Paths kept verbatim — your runs land alongside theirs.
        /// Per-user agent unit and per-user subdirs are still created.
        #[arg(long, group = "init_source")]
        join: Option<PathBuf>,

        /// Cluster name. Defaults to the foreign config's name or
        /// "untitled" for greenfield.
        #[arg(long)]
        name: Option<String>,
        /// runs_base path override (interactive default if absent).
        #[arg(long)]
        runs_base: Option<PathBuf>,
        /// Artifact root override, repeatable. Format: `kind=path`,
        /// e.g. `--artifact-root checkpoint=/scratch/me/ckpts`.
        #[arg(long = "artifact-root", value_parser = parse_kv_pathbuf)]
        artifact_root: Vec<(String, PathBuf)>,
        /// Repo override, repeatable. Format: `name=path`.
        #[arg(long, value_parser = parse_kv_pathbuf)]
        repo: Vec<(String, PathBuf)>,
        /// Output path. Defaults to ~/.config/labctl/cluster.toml.
        #[arg(long)]
        output: Option<PathBuf>,

        /// Non-interactive: accept all defaults, override only via
        /// flags. Auto-enabled when stdin isn't a TTY.
        #[arg(long, short = 'y')]
        yes: bool,
        /// Overwrite an existing output file / replace an existing
        /// agent unit without prompting.
        #[arg(long)]
        force: bool,
        /// Skip the SLURM probes (CI, non-SLURM hosts, or when
        /// `--migrate-from` / `--use` / `--join` carry the values).
        #[arg(long)]
        no_detect: bool,
        /// Skip pre-creating per-user subdirs under runs_base +
        /// artifact_roots.
        #[arg(long)]
        no_create_dirs: bool,
        /// Skip installing the per-user systemd agent unit.
        #[arg(long)]
        no_agent: bool,
        /// Skip the final `labctl doctor` run.
        #[arg(long)]
        no_doctor: bool,
        /// For `--use` / `--join`: copy the source cluster.toml
        /// instead of symlinking. Decouples your local config from
        /// any later rotation of the source. Defaults off — symlink
        /// is usually what you want for a team-rotated config.
        #[arg(long)]
        copy_config: bool,
    },
}

/// `kind=path` value parser for the `--artifact-root` / `--repo` clap
/// args. Returns an error string if the value lacks an `=` separator
/// or has an empty key / path.
fn parse_kv_pathbuf(s: &str) -> Result<(String, PathBuf), String> {
    let Some(eq) = s.find('=') else {
        return Err(format!("expected `kind=path`, got {s:?}"));
    };
    let (k, v) = (&s[..eq], &s[eq + 1..]);
    if k.is_empty() {
        return Err(format!("empty key in {s:?}"));
    }
    if v.is_empty() {
        return Err(format!("empty path in {s:?}"));
    }
    Ok((k.to_string(), PathBuf::from(v)))
}

#[derive(Subcommand)]
enum EvaldCommand {
    Once { policy: PathBuf },
}

/// Which labctl unit a service subcommand is acting on. The CLI surfaces
/// this as mutually-exclusive `--agent` / `--ui` flags so users don't
/// have to remember unit-name strings.
#[derive(Debug, Clone, Copy)]
enum UnitKind {
    Agent,
    Ui,
}

impl UnitKind {
    fn unit_name(self) -> &'static str {
        match self {
            UnitKind::Agent => service::AGENT_UNIT_NAME,
            UnitKind::Ui => service::UI_UNIT_NAME,
        }
    }
}

fn pick_unit_kind(agent: bool, ui: bool) -> Result<UnitKind> {
    match (agent, ui) {
        (true, false) => Ok(UnitKind::Agent),
        (false, true) => Ok(UnitKind::Ui),
        (true, true) => unreachable!("clap's ArgGroup prevents both"),
        (false, false) => anyhow::bail!(
            "specify which unit to target: --agent or --ui"
        ),
    }
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// Install a labctl systemd user unit. Pick exactly one of `--agent`
    /// (the per-user dispatch loop; auto-installed by `labctl init`) or
    /// `--ui` (the read-only HTTP window). Survives logout when linger
    /// is enabled (`loginctl enable-linger $USER`).
    Install {
        /// Install the per-user dispatch agent (`labctl-agent.service`).
        #[arg(long, group = "kind")]
        agent: bool,
        /// Install the read-only UI (`labctl-ui.service`).
        #[arg(long, group = "kind")]
        ui: bool,
        /// Bind address for the UI unit. Ignored when `--agent`.
        #[arg(long, default_value = service::DEFAULT_BIND)]
        bind: String,
        /// Overwrite an existing unit file with the same name.
        #[arg(long)]
        force: bool,
    },
    /// Stop, disable, and remove a labctl unit. Pick exactly one of
    /// `--agent` or `--ui`.
    Uninstall {
        #[arg(long, group = "kind")]
        agent: bool,
        #[arg(long, group = "kind")]
        ui: bool,
    },
    /// Show status of both labctl units (agent + ui). Pass `--agent` or
    /// `--ui` to scope to one.
    Status {
        #[arg(long, group = "kind")]
        agent: bool,
        #[arg(long, group = "kind")]
        ui: bool,
    },
    /// Restart installed labctl units. With no flag, restarts whichever
    /// of (agent, ui) are installed — the canonical post-rebuild
    /// command. Errors if neither is installed.
    Restart {
        #[arg(long, group = "kind")]
        agent: bool,
        #[arg(long, group = "kind")]
        ui: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cluster_path = resolve_cluster_path(cli.cluster.clone());

    // Validate is pure file inspection — no cluster, no registry.
    if let Command::Validate { path } = &cli.command {
        return validate_path(path);
    }
    if let Command::Init {
        r#use,
        migrate_from,
        join,
        name,
        runs_base,
        artifact_root,
        repo,
        output,
        yes,
        force,
        no_detect,
        no_create_dirs,
        no_agent,
        no_doctor,
        copy_config,
    } = cli.command
    {
        let mode = match (r#use, migrate_from, join) {
            (Some(p), None, None) => Some(init::InitMode::Use(p)),
            (None, Some(p), None) => Some(init::InitMode::MigrateFrom(p)),
            (None, None, Some(p)) => Some(init::InitMode::Join(p)),
            (None, None, None) => None,
            _ => unreachable!("clap's mutual-exclusion group prevents this"),
        };
        return init::run(init::InitOptions {
            mode,
            yes,
            name,
            runs_base,
            artifact_roots: artifact_root,
            repos: repo,
            output,
            force,
            no_detect,
            no_create_dirs,
            no_agent,
            no_doctor,
            copy_config,
        });
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
        return run_doctor(&cluster_path);
    }
    if let Command::Service { command } = cli.command {
        return match command {
            ServiceCommand::Install { agent, ui, bind, force } => {
                let mode = pick_unit_kind(agent, ui)?;
                let mode = match mode {
                    UnitKind::Agent => service::UnitMode::Agent,
                    UnitKind::Ui => service::UnitMode::Ui { bind },
                };
                service::install(service::InstallOptions {
                    cluster_path,
                    mode,
                    force,
                })
            }
            ServiceCommand::Uninstall { agent, ui } => {
                let kind = pick_unit_kind(agent, ui)?;
                service::uninstall(kind.unit_name())
            }
            ServiceCommand::Status { agent, ui } => {
                if !agent && !ui {
                    // No filter — show both units.
                    service::status(service::AGENT_UNIT_NAME)?;
                    service::status(service::UI_UNIT_NAME)
                } else {
                    let kind = pick_unit_kind(agent, ui)?;
                    service::status(kind.unit_name())
                }
            }
            ServiceCommand::Restart { agent, ui } => {
                let targets: Vec<&str> = match (agent, ui) {
                    (true, false) => vec![service::AGENT_UNIT_NAME],
                    (false, true) => vec![service::UI_UNIT_NAME],
                    (true, true) => unreachable!("clap ArgGroup prevents both"),
                    (false, false) => [service::AGENT_UNIT_NAME, service::UI_UNIT_NAME]
                        .into_iter()
                        .filter(|n| service::is_installed(n))
                        .collect(),
                };
                if targets.is_empty() {
                    anyhow::bail!(
                        "no labctl units installed (run `labctl init` for the \
                         agent, `labctl service install --ui` for the UI)"
                    );
                }
                service::restart(&targets)
            }
        };
    }
    // Run and RunPipeline are handled before opening the local registry —
    // the daemon owns every write. CLI invocations on a shared registry
    // would race on POSIX locks (broken on parallel filesystems) and
    // corrupt the DB.
    if let Command::Run { recipe } = &cli.command {
        return run_recipe_command(&cluster_path, recipe);
    }
    if let Command::RunSweep { recipe } = &cli.command {
        return run_sweep_command(&cluster_path, recipe);
    }
    if let Command::RunPipeline { pipeline } = &cli.command {
        return run_pipeline_command(&cluster_path, pipeline);
    }

    let cluster = config::ClusterConfig::load(&cluster_path)?;
    let mut store = store::Store::open(&cluster)?;

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
        Command::ImportFromCluster {
            foreign,
            from,
            r#as,
            no_copy,
        } => {
            let foreign_cluster = config::ClusterConfig::load(&foreign)?;
            let report = artifacts::import_from_cluster(
                &cluster,
                &mut store,
                &foreign_cluster,
                &from,
                r#as.as_deref(),
                !no_copy,
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
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
        Command::Serve { bind } => {
            let addr: std::net::SocketAddr = bind
                .parse()
                .with_context(|| format!("invalid --bind address {bind:?}"))?;
            server::serve(cluster, store, addr)?;
        }
        Command::Agent => {
            agent::run_standalone(cluster, store)?;
        }
        Command::Validate { .. }
        | Command::Service { .. }
        | Command::Doctor
        | Command::Run { .. }
        | Command::RunSweep { .. }
        | Command::RunPipeline { .. }
        | Command::Init { .. } => {
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
        let user = std::env::var("USER").unwrap_or_else(|_| "<unset>".into());
        emit("submitter ($USER)", user != "<unset>", &user);

        // The CLI is the only writer in the new model — every `labctl
        // run` mkdirs into `runs/<user>/<run_id>/` and writes its own
        // sidecars under that uid. So the load-bearing question is
        // whether *this user* can write into runs_base.
        let runs_base = &cluster.filesystem.runs_base;
        let runs_ok = runs_base.is_dir() && writable_dir(runs_base);
        emit(
            "runs_base writable by $USER",
            runs_ok,
            &runs_base.display().to_string(),
        );

        // Filesystem-truth registry: the index is rebuilt in-memory on
        // every open. We surface the indexer outcome so doctor can flag a
        // partially-populated tree (typically permissions on a subdir).
        match store::Store::open(cluster) {
            Ok(_) => emit("registry index", true, "ok"),
            Err(e) => emit("registry index", false, &format!("{e:#}")),
        }

        // Each user produces artifacts under `<root>/<user>/<alias>/`.
        // The user must be able to write into <root> to create the per-
        // user subdir on first use.
        for (kind, root) in &cluster.filesystem.artifact_roots {
            let ok = root.is_dir() && writable_dir(root);
            emit(
                &format!("artifact_root[{kind}] writable"),
                ok,
                &root.display().to_string(),
            );
        }
        // Output roots are the path destinations for recipe outputs
        // (looked up by their `type =`). They may coincide with an
        // artifact_root path (e.g. a `checkpoint_stream` output writes
        // into the same tree that holds `checkpoint` artifacts) but the
        // kind is a separate namespace; check writability independently.
        for (kind, root) in &cluster.filesystem.output_roots {
            let ok = root.is_dir() && writable_dir(root);
            emit(
                &format!("output_root[{kind}] writable"),
                ok,
                &root.display().to_string(),
            );
        }

        // Detect deployment mode from the runs_base group bits. A
        // group-readable runs_base is the deliberate signal that this
        // tree is meant to be shared across a lab group; a private
        // (g===) runs_base is a single-user setup where teammate
        // access isn't a concern. We use this to decide whether the
        // multi-tenant readiness checks below should be loud or silent.
        let multi_tenant = {
            use std::os::unix::fs::MetadataExt;
            cluster
                .filesystem
                .runs_base
                .metadata()
                .map(|m| m.mode() & 0o050 == 0o050)
                .unwrap_or(false)
        };
        emit(
            "deployment mode",
            true,
            if multi_tenant {
                "multi-tenant (runs_base is group-readable)"
            } else {
                "single-user (runs_base is private; multi-tenant checks skipped)"
            },
        );

        // Multi-tenant readiness: a new <user>/ subdir created under
        // each registry root must inherit the lab-group's ownership
        // and group r+x bits so teammates can read each other's runs
        // and artifacts. group_propagation_probe is silent on
        // single-user setups (parent not group-readable → reports
        // "single-user setup" without failing), so it's safe to run
        // unconditionally; the repos check below is stricter and gated.
        let mut probed: std::collections::BTreeSet<PathBuf> =
            std::collections::BTreeSet::new();
        let mut probe_targets: Vec<(String, &PathBuf)> = Vec::new();
        probe_targets.push(("runs_base".to_string(), &cluster.filesystem.runs_base));
        for (k, p) in &cluster.filesystem.artifact_roots {
            probe_targets.push((format!("artifact_root[{k}]"), p));
        }
        for (k, p) in &cluster.filesystem.output_roots {
            probe_targets.push((format!("output_root[{k}]"), p));
        }
        for (label, path) in probe_targets {
            if !probed.insert(path.clone()) {
                continue;
            }
            let (ok, detail) = group_propagation_probe(path);
            emit(&format!("group propagation[{label}]"), ok, &detail);
        }

        // Repos: every recipe's `repo = "..."` resolves through this
        // map to a path that the submitting job will check out source
        // from. In a multi-tenant rollout that path must be reachable
        // by every teammate's uid, not just the original author's. A
        // repo nestled in a 700 homedir is the canonical silent
        // multi-tenant blocker — teammate's job fails with permission
        // denied during snapshot, not at recipe-validation time. In a
        // single-user setup nobody else is reading these paths, so
        // skip the check entirely.
        if multi_tenant {
            for (name, path) in &cluster.repos {
                let (ok, detail) = match group_traversable(path) {
                    Ok(()) => (true, format!("group-traversable: {}", path.display())),
                    Err(e) => (false, e),
                };
                emit(&format!("repos[{name}] readable by group"), ok, &detail);
            }
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
                "agent config",
                true,
                &format!(
                    "reconcile={}s evald={}s policies_dir={}",
                    d.reconcile_interval_secs,
                    d.evald_interval_secs,
                    d.policies_dir.display(),
                ),
            ),
            None => emit(
                "agent config",
                true,
                "absent ([dispatch] not set; agent is a no-op)",
            ),
        }

        // [remote] is consumed only when this config is loaded as a
        // FOREIGN cluster (cross-cluster imports). When auditing one,
        // confirm we can actually reach it from here — catches typos
        // in the ssh_alias / host field, dead jump hosts, and lapsed
        // ControlMaster sessions before the user hits them mid-import.
        if let Some(r) = &cluster.remote {
            match remote::probe_reachability(r) {
                Ok(detail) => emit("remote reachability", true, &detail),
                Err(detail) => emit("remote reachability", false, &detail),
            }
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
    // Two unit shapes labctl installs:
    //   labctl-agent   (per-user dispatch — auto-installed by `labctl init`)
    //   labctl-ui      (HTTP-only — opt-in via `labctl service install --ui`)
    // Doctor reports each independently.
    if systemd_ok {
        let candidates = [service::AGENT_UNIT_NAME, service::UI_UNIT_NAME];
        let installed: Vec<&str> = candidates
            .iter()
            .copied()
            .filter(|n| service::is_installed(n))
            .collect();
        if installed.is_empty() {
            emit(
                "labctl unit",
                true,
                "not installed (run `labctl init` for the per-user agent, \
                 `labctl service install --ui` for the read-only UI)",
            );
        } else {
            for unit in installed {
                let active = Cmd::new("systemctl")
                    .args(["--user", "is-active", unit])
                    .output()
                    .ok()
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                    .unwrap_or_else(|| "unknown".into());
                emit(&format!("{unit} unit"), true, &format!("installed ({active})"));
                if active == "failed" || active == "inactive" {
                    if let Ok(out) = Cmd::new("journalctl")
                        .args(["--user", "-u", unit, "-n", "20", "--no-pager"])
                        .output()
                    {
                        if out.status.success() {
                            let tail = String::from_utf8_lossy(&out.stdout);
                            let trimmed = tail.trim();
                            if !trimmed.is_empty() {
                                println!("       last 20 lines of `journalctl --user -u {unit}`:");
                                for line in trimmed.lines() {
                                    println!("         {line}");
                                }
                            }
                        }
                    }
                }
            }
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

/// Probe whether a newly-created subdir under `parent` inherits the
/// parent's group + group-rwx bits — the runtime check that the
/// multi-tenant FS permissions recipe (sgid + umask 002 + default ACL)
/// actually took on this filesystem. Returns `(false, why)` if either
/// the group didn't inherit (sgid missing) or the mode is too
/// restrictive (umask too tight or default ACL not in effect). Reports
/// "single-user" without failing when the parent isn't group-readable
/// to begin with — that's a valid solo setup, not a misconfiguration.
fn group_propagation_probe(parent: &std::path::Path) -> (bool, String) {
    use std::os::unix::fs::MetadataExt;
    let meta = match parent.metadata() {
        Ok(m) => m,
        Err(e) => return (false, format!("stat {}: {e}", parent.display())),
    };
    let parent_gid = meta.gid();
    let parent_mode = meta.mode();
    if parent_mode & 0o050 != 0o050 {
        return (
            true,
            format!(
                "single-user setup (parent mode {:o}; not group-readable)",
                parent_mode & 0o777
            ),
        );
    }
    let probe = parent.join(format!(".labctl-doctor-grp-probe-{}", std::process::id()));
    if let Err(e) = std::fs::create_dir(&probe) {
        return (false, format!("mkdir probe in {}: {e}", parent.display()));
    }
    let outcome: Result<String, String> = (|| {
        let pm = probe
            .metadata()
            .map_err(|e| format!("stat probe: {e}"))?;
        let probe_gid = pm.gid();
        let probe_mode = pm.mode();
        if probe_gid != parent_gid {
            return Err(format!(
                "new subdir gid={probe_gid}, parent gid={parent_gid} — \
                 sgid bit not set on parent ({})",
                parent.display()
            ));
        }
        if probe_mode & 0o050 != 0o050 {
            return Err(format!(
                "new subdir mode {:o} lacks group r+x — umask too restrictive \
                 or default ACL not applied ({})",
                probe_mode & 0o777,
                parent.display()
            ));
        }
        Ok(format!(
            "group r+x inherits (gid {probe_gid}, mode {:o})",
            probe_mode & 0o777
        ))
    })();
    let _ = std::fs::remove_dir(&probe);
    match outcome {
        Ok(d) => (true, d),
        Err(e) => (false, e),
    }
}

/// Walk path components from `path` up to `/`, verifying each
/// intermediate dir is at least group-traversable (g+x) and `path`
/// itself is group-readable + traversable (g+rx). Catches the
/// canonical multi-tenant misconfiguration: a recipe-repo path nested
/// inside a 700 homedir, which silently breaks any teammate's job
/// that tries to snapshot the source.
fn group_traversable(path: &std::path::Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let target = path.to_path_buf();
    let mut cur: Option<&std::path::Path> = Some(&target);
    while let Some(p) = cur {
        let meta = p
            .metadata()
            .map_err(|e| format!("stat {}: {e}", p.display()))?;
        let mode = meta.mode();
        let (needed, what): (u32, &str) = if p == target {
            (0o050, "g+rx")
        } else {
            (0o010, "g+x")
        };
        if mode & needed != needed {
            return Err(format!(
                "{} lacks {} (mode {:o})",
                p.display(),
                what,
                mode & 0o777
            ));
        }
        cur = p.parent();
    }
    Ok(())
}

/// One-line nudge after `labctl run` succeeds, when the cluster is
/// configured for in-process dispatch but the user hasn't installed the
/// systemd unit. Suppressed by `LABCTL_NO_HINT=1` and skipped on hosts
/// without systemd-user. Designed to be ignorable — never blocks, never
/// errors.
/// Submit a recipe. The CLI is the only writer in the new model: it
/// opens `Store` directly (against the filesystem-truth registry),
/// snapshots the source repo as the invoking user, renders the sbatch
/// script, and shells out to `sbatch` under its own uid. SLURM job
/// ownership therefore matches the OS user, and `submitted_by` in the
/// row is path-canonical.
fn run_recipe_command(cluster_path: &PathBuf, recipe_path: &PathBuf) -> Result<()> {
    let cluster = config::ClusterConfig::load(cluster_path)?;
    let recipe = config::Recipe::load(recipe_path)?;
    let submitted_by = current_user()?;
    let mut store = store::Store::open(&cluster)?;
    let submitted = runner::submit_recipe(&cluster, &mut store, &recipe, None, &submitted_by)?;
    println!("run_id: {}", submitted.run_id);
    println!("job_id: {}", submitted.job_id);
    println!("run_dir: {}", submitted.run_dir.display());
    Ok(())
}

fn run_sweep_command(cluster_path: &PathBuf, recipe_path: &PathBuf) -> Result<()> {
    let cluster = config::ClusterConfig::load(cluster_path)?;
    let recipe = config::Recipe::load(recipe_path)?;
    if recipe.sweep.is_none() {
        anyhow::bail!(
            "recipe {:?} has no [sweep] section; use `labctl run` for single-shot submission",
            recipe.name
        );
    }
    let sweep = recipe.sweep.as_ref().unwrap();
    let n = (sweep.end as usize).saturating_sub(sweep.start as usize) + 1;
    let throttle_str = sweep.throttle.map(|t| format!("%{t}")).unwrap_or_default();
    eprintln!(
        "submitting sweep: recipe={:?} arg={} range={}..={}{} ({} tasks){}",
        recipe.name,
        sweep.arg,
        sweep.start,
        sweep.end,
        throttle_str,
        n,
        if sweep.aggregate.is_some() { " + aggregate" } else { "" },
    );
    let submitted_by = current_user()?;
    let mut store = store::Store::open(&cluster)?;
    let result = runner::submit_sweep(&cluster, &mut store, &recipe, &submitted_by)?;
    println!(
        "array_run_id: {}\narray_job_id: {}\narray_run_dir: {}",
        result.array_run.run_id,
        result.array_run.job_id,
        result.array_run.run_dir.display(),
    );
    if let Some(agg) = &result.aggregate {
        println!("aggregate_run_id: {}", agg.run_id);
        println!("aggregate_job_id: {}", agg.job_id);
        println!("aggregate_run_dir: {}", agg.run_dir.display());
    }
    Ok(())
}

fn run_pipeline_command(cluster_path: &PathBuf, pipeline_path: &PathBuf) -> Result<()> {
    let cluster = config::ClusterConfig::load(cluster_path)?;
    let loaded = config::Pipeline::load(pipeline_path)?;
    let submitted_by = current_user()?;
    let mut store = store::Store::open(&cluster)?;
    let submitted = runner::submit_pipeline(
        &cluster,
        &mut store,
        &loaded,
        Some(pipeline_path),
        &submitted_by,
    )?;
    println!("{}", serde_json::to_string_pretty(&submitted)?);
    Ok(())
}

/// Resolve `$USER` once. Required everywhere the CLI writes — the path-
/// canonical layout records the invoker as a load-bearing path segment.
fn current_user() -> Result<String> {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .context("USER (or USERNAME on Windows) not set; cannot determine submitter")?;
    fs_layout::validate_user(&user)?;
    Ok(user)
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
        let from_suffix = match &loaded.from {
            Some(id) => format!(", from={id:?}"),
            None => String::new(),
        };
        println!(
            "pipeline {:?} OK ({} stages, topo={:?}{})",
            loaded.name,
            loaded.stages.len(),
            loaded.topo_order,
            from_suffix,
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
        let sweep_info = match &recipe.sweep {
            Some(s) => format!(
                ", sweep: {}={}..={} ({} tasks){}",
                s.arg,
                s.start,
                s.end,
                (s.end as usize).saturating_sub(s.start as usize) + 1,
                if s.aggregate.is_some() { " + aggregate" } else { "" },
            ),
            None => String::new(),
        };
        println!(
            "recipe {:?} OK (repo={}, {} inputs, {} outputs{})",
            recipe.name,
            recipe.repo,
            recipe.inputs.len(),
            recipe.outputs.len(),
            sweep_info,
        );
    }
    Ok(())
}
