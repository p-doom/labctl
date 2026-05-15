mod agent;
mod artifacts;
mod config;
mod evald;
mod fs_layout;
mod init;
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
    /// Serve the web UI and run the per-user dispatch loops
    /// (reconcile + evald + throttle) in one process, sharing one
    /// in-memory cache. This is the canonical long-running daemon —
    /// `labctl service install` writes a systemd user unit whose
    /// ExecStart points here. Bind 127.0.0.1 and reach the UI via an
    /// SSH tunnel from your laptop.
    #[cfg(feature = "ui")]
    Serve {
        #[arg(long, default_value = "127.0.0.1:8765")]
        bind: String,
        /// Skip the dispatch loops; the process becomes a read-only
        /// HTTP front. Useful for an extra UI replica that shouldn't
        /// also try to reconcile.
        #[arg(long)]
        no_dispatch: bool,
    },
    /// Run only the per-user dispatch loops (reconcile + evald +
    /// periodic refresh) — no HTTP listener. Pairs with one shared
    /// `labctl serve --no-dispatch` as the multi-tenant rollout model:
    /// each user runs their own tiny agent; one host runs the shared
    /// read-only UI. Install as a systemd user unit via
    /// `labctl service install --agent`. Use `labctl serve` (all-in-one)
    /// instead for single-user setups.
    Agent,
    /// Bootstrap a cluster.toml for a new site. Probes the local
    /// SLURM controller (sinfo / sacctmgr / scontrol) for partition,
    /// QoS, and GresTypes, then writes a cluster.<name>.toml in CWD.
    /// Use `--from <foreign.toml>` to copy schema from another
    /// cluster's identity card (the "import the config from this
    /// cluster" workflow); flag overrides apply on top.
    /// Always paired with a follow-up `labctl --cluster <new> doctor`.
    Init {
        /// Path to an existing cluster.toml to copy schema from.
        /// Paths in the copy are surfaced verbatim — the user must
        /// edit them. Validation of the source is skipped so foreign
        /// configs whose paths don't exist locally still work.
        #[arg(long)]
        from: Option<PathBuf>,
        /// Cluster name. Defaults to "untitled" (skeleton) or the
        /// foreign cluster's name (when `--from` is set).
        #[arg(long)]
        name: Option<String>,
        /// runs_base path override. Required if not in `--from`.
        #[arg(long)]
        runs_base: Option<PathBuf>,
        /// Artifact root override, repeatable. Format: `kind=path`,
        /// e.g. `--artifact-root checkpoint=/scratch/me/ckpts`.
        #[arg(long = "artifact-root", value_parser = parse_kv_pathbuf)]
        artifact_root: Vec<(String, PathBuf)>,
        /// Repo override, repeatable. Format: `name=path`,
        /// e.g. `--repo myrepo=/home/me/myrepo`.
        #[arg(long, value_parser = parse_kv_pathbuf)]
        repo: Vec<(String, PathBuf)>,
        /// Output path. Defaults to `cluster.<name>.toml` in CWD.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Overwrite an existing output file.
        #[arg(long)]
        force: bool,
        /// Skip the SLURM probes. Useful in CI / non-SLURM hosts.
        #[arg(long)]
        no_detect: bool,
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

#[derive(Subcommand)]
enum ServiceCommand {
    /// Generate a systemd user unit, enable it, and start it. Defaults
    /// to the all-in-one `labctl serve` (HTTP + dispatch). Pass
    /// `--agent` for the dispatch-only agent (no HTTP) — pairs with
    /// one shared `labctl serve --no-dispatch` for multi-tenant
    /// deployments. Survives logout when linger is enabled
    /// (`loginctl enable-linger $USER`).
    Install {
        #[arg(long, default_value = "127.0.0.1:8765")]
        bind: String,
        /// Install the dispatch-only agent (no HTTP listener). `--bind`
        /// is ignored. Default unit name becomes `labctl-agent`.
        /// Mutually exclusive with `--no-dispatch`.
        #[arg(long, conflicts_with = "no_dispatch")]
        agent: bool,
        /// Install the read-only shared UI (HTTP only, no reconcile or
        /// evald). The shared UI host in a multi-tenant rollout — pair
        /// with per-user `--agent` installs. Default unit name becomes
        /// `labctl-ui`. Mutually exclusive with `--agent`.
        #[arg(long)]
        no_dispatch: bool,
        /// Override the unit name. Defaults to `labctl` (all-in-one),
        /// `labctl-agent` (--agent), or `labctl-ui` (--no-dispatch).
        #[arg(long)]
        name: Option<String>,
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
    // Init writes a cluster.toml from flags + optional `--from` + SLURM
    // auto-detect. It never reads --cluster, never touches the registry.
    if let Command::Init {
        from,
        name,
        runs_base,
        artifact_root,
        repo,
        output,
        force,
        no_detect,
    } = cli.command
    {
        return init::run(init::InitOptions {
            from,
            name,
            runs_base,
            artifact_roots: artifact_root,
            repos: repo,
            output,
            force,
            no_detect,
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
        return run_doctor(&cli.cluster);
    }
    if let Command::Service { command } = cli.command {
        return match command {
            ServiceCommand::Install {
                bind,
                agent,
                no_dispatch,
                name,
                force,
            } => {
                let mut opts = service::InstallOptions::new(cli.cluster);
                opts.mode = if agent {
                    service::UnitMode::Agent
                } else {
                    service::UnitMode::Serve { bind, no_dispatch }
                };
                opts.unit_name = name.unwrap_or_else(|| {
                    if agent {
                        "labctl-agent"
                    } else if no_dispatch {
                        "labctl-ui"
                    } else {
                        "labctl"
                    }
                    .to_string()
                });
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
    if let Command::RunSweep { recipe } = &cli.command {
        return run_sweep_command(&cli.cluster, recipe);
    }
    if let Command::RunPipeline { pipeline } = &cli.command {
        return run_pipeline_command(&cli.cluster, pipeline);
    }

    let cluster = config::ClusterConfig::load(&cli.cluster)?;
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
        Command::Serve { bind, no_dispatch } => {
            let addr: std::net::SocketAddr = bind
                .parse()
                .with_context(|| format!("invalid --bind address {bind:?}"))?;
            server::serve(cluster, store, addr, no_dispatch)?;
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
    // Recognize all three unit shapes labctl knows how to install:
    //   labctl         (all-in-one: HTTP + dispatch)
    //   labctl-agent   (dispatch-only, per-user, multi-tenant rollout)
    //   labctl-ui      (HTTP-only, --no-dispatch; the shared read window)
    // Any of these can be present; doctor reports every installed one.
    if systemd_ok {
        let candidates = ["labctl", "labctl-agent", "labctl-ui"];
        let installed: Vec<&str> = candidates
            .iter()
            .copied()
            .filter(|n| service::is_installed(n))
            .collect();
        if installed.is_empty() {
            emit(
                "labctl unit",
                true,
                "not installed (run `labctl service install` for single-user, \
                 `service install --agent` for multi-tenant per-user agent, \
                 or `service install --no-dispatch` for the shared read-only UI)",
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
                // When a unit is failed or inactive, tail journalctl so
                // doctor reports the *why* in-line rather than forcing a
                // round-trip through `systemctl --user status`. Best-
                // effort: missing journalctl, no permission, or no log
                // yet all just elide the tail without failing the check.
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
