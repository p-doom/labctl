//! `labctl init` — full bootstrap for a new-cluster setup.
//!
//! Not a config-writer. End-to-end: resolve a cluster config (write
//! one, adopt an existing one, adapt a foreign one, or symlink the
//! team's), pre-create per-user subdirs, install the systemd agent
//! unit, run doctor. Leave the user with `labctl run` working.
//!
//! Four modes — pick by flag, otherwise asked interactively:
//!
//!   - `Greenfield` — write a fresh cluster.toml. SLURM probes fill
//!     partition/QoS/gres; prompts fill paths. Default for new sites
//!     with no template to crib from.
//!   - `Use(path)` — adopt an existing cluster.toml the user wrote.
//!     Don't touch the source file; symlink it into the default
//!     config location so plain `labctl <cmd>` works, then bootstrap
//!     dirs + agent + doctor around it.
//!   - `MigrateFrom(path)` — copy schema from a foreign cluster's
//!     identity card, but adapt site-local paths interactively. The
//!     output is a *new* cluster.toml at the local destination. Use
//!     this when standing labctl up at site B from site A's config.
//!   - `Join(path)` — join a colleague's shared registry on the same
//!     cluster. Keep paths verbatim (the registry IS shared);
//!     symlink the colleague's cluster.toml into the user's default
//!     location; install per-user agent; create the per-user
//!     subdirs the colleague's registry already expects. Different
//!     from Use only in messaging.
//!
//! Interactive by default. `--yes` (or non-TTY stdin) collapses to
//! "accept all defaults" — same code path, suppressed prompts. Flag
//! overrides win over interactive defaults so a partially-scripted
//! invocation works: `labctl init --yes --runs-base /scratch/foo` is
//! valid.

use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};

use crate::{
    config::{ClusterConfig, FilesystemConfig, SchedulerConfig, SlurmConfig},
    prompt,
    service,
};

/// Which of the four bootstrap stories the user is running.
#[derive(Debug, Clone)]
pub enum InitMode {
    Greenfield,
    Use(PathBuf),
    MigrateFrom(PathBuf),
    Join(PathBuf),
}

pub struct InitOptions {
    /// `None` means no mode flag was passed — init will prompt for
    /// the mode interactively, or default to Greenfield in auto mode.
    /// `Some(x)` is the explicit flag-supplied mode.
    pub mode: Option<InitMode>,
    pub yes: bool,
    pub name: Option<String>,
    pub runs_base: Option<PathBuf>,
    pub artifact_roots: Vec<(String, PathBuf)>,
    pub repos: Vec<(String, PathBuf)>,
    pub output: Option<PathBuf>,
    pub force: bool,
    pub no_detect: bool,
    pub no_create_dirs: bool,
    pub no_agent: bool,
    pub no_doctor: bool,
    pub copy_config: bool,
}

#[derive(Default)]
struct SlurmProbe {
    partition: Option<String>,
    qos: Option<String>,
    gres_gpu_syntax: Option<String>,
    notes: Vec<String>,
}

pub fn run(mut opts: InitOptions) -> Result<()> {
    let pmode = prompt::Mode::resolve(opts.yes);
    println!("labctl init — bootstrap a cluster config and per-user agent.\n");

    // Resolve mode: explicit flag wins; otherwise prompt
    // interactively (so a fresh `labctl init` walks the user
    // through "what are you doing?" instead of silently defaulting
    // to greenfield). In auto mode with no flag, fall back to
    // greenfield — the existing scripted behavior. `take()` to
    // move out without invalidating `opts` for later access.
    let init_mode = match opts.mode.take() {
        Some(m) => m,
        None => prompt_for_mode(pmode)?,
    };
    print_mode_line(&init_mode);

    // ── 1. Acquire base config ───────────────────────────────────
    let mut cfg = match &init_mode {
        InitMode::Greenfield => skeleton_config(opts.name.as_deref()),
        InitMode::Use(p) | InitMode::MigrateFrom(p) | InitMode::Join(p) => load_lax(p)
            .with_context(|| format!("failed to load source config {}", p.display()))?,
    };

    // ── 2. SLURM probe ───────────────────────────────────────────
    // Use/Join trust the source's SLURM block — don't overwrite it
    // with a local probe. Greenfield/MigrateFrom probe because the
    // user is targeting this cluster and the source (if any) is
    // from elsewhere.
    let probe = match (&init_mode, opts.no_detect) {
        (_, true) => SlurmProbe::default(),
        (InitMode::Use(_) | InitMode::Join(_), _) => SlurmProbe::default(),
        _ => slurm_probe(),
    };

    // ── 3. Apply flag overrides + interactive review ─────────────
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
    apply_probe(&mut cfg, &probe);

    match (&init_mode, pmode) {
        (InitMode::Greenfield | InitMode::MigrateFrom(_), prompt::Mode::Interactive) => {
            interactive_review(&mut cfg, &init_mode, &probe, pmode)?;
        }
        _ => {
            // In auto mode: flag overrides + probe + skeleton/foreign
            // defaults are already applied above. In Use/Join modes:
            // the source IS the truth, nothing to review.
        }
    }

    // ── 4. Decide output / link target ───────────────────────────
    let dest = pick_destination(&opts, &cfg, pmode)?;

    // ── 5. Existing-file disposition ─────────────────────────────
    if dest.exists() && !opts.force {
        let same_target = matches!(&init_mode, InitMode::Use(p) | InitMode::Join(p) if same_file(p, &dest));
        if !same_target {
            match handle_existing(&dest, pmode)? {
                ExistingAction::Keep => {
                    println!("→ keeping existing {}", dest.display());
                }
                ExistingAction::Replace => write_or_link(&cfg, &dest, &init_mode, &opts)?,
                ExistingAction::Abort => {
                    println!("aborted by user");
                    return Ok(());
                }
            }
        } else {
            println!("→ {} already points at the source; skipping write.", dest.display());
        }
    } else {
        write_or_link(&cfg, &dest, &init_mode, &opts)?;
    }

    // ── 6. Pre-create per-user subdirs ───────────────────────────
    // Always safe (the user owns their own subdir). Surfaces are
    // logged so a follow-up doctor explains any failures.
    if !opts.no_create_dirs {
        let do_create = prompt::confirm(
            "Pre-create per-user subdirectories under runs_base + artifact_roots?",
            true,
            pmode,
        )?;
        if do_create {
            create_user_dirs(&cfg)?;
        }
    }

    // ── 7. Install systemd agent unit ────────────────────────────
    if !opts.no_agent {
        if service::systemd_available() {
            let do_install = prompt::confirm(
                "Install per-user agent (systemd user unit)?",
                true,
                pmode,
            )?;
            if do_install {
                install_agent_unit(&dest, opts.force)?;
            }
        } else {
            println!(
                "→ systemd --user not available; skipping agent install. \
                 Run `labctl agent` manually or set up a service supervisor."
            );
        }
    }

    // ── 8. Run doctor ────────────────────────────────────────────
    // Invoked as a subprocess so its non-zero exit code can't kill
    // the success summary below. We still surface the exit status so
    // the user sees pass/fail.
    if !opts.no_doctor {
        println!();
        println!("Running doctor against {}...", dest.display());
        let doctor_passed = run_doctor_subprocess(&dest)?;
        println!();
        if doctor_passed {
            println!("✓ labctl is set up. Next:");
            println!("    labctl run path/to/recipe.toml");
        } else {
            println!(
                "⚠ Setup wrote {} but doctor reported failures.\n  \
                 Fix the issues above (typically: permissions or missing dirs) \
                 and re-run `labctl doctor`.",
                dest.display(),
            );
        }
    } else {
        println!("\n✓ labctl is set up (doctor skipped). Run `labctl doctor` when you're ready.");
    }

    Ok(())
}

fn print_mode_line(mode: &InitMode) {
    match mode {
        InitMode::Greenfield => println!("mode: greenfield (writing a fresh config)\n"),
        InitMode::Use(p) => println!("mode: use {} (adopting an existing config)\n", p.display()),
        InitMode::MigrateFrom(p) => {
            println!("mode: migrate-from {} (adapting to a new cluster)\n", p.display())
        }
        InitMode::Join(p) => println!(
            "mode: join {} (joining a shared registry)\n",
            p.display()
        ),
    }
}

/// Pick a mode interactively. Auto-resolves to Greenfield in non-
/// interactive mode (preserving the old scripted behavior — set the
/// mode via flag, or accept the greenfield default with `--yes`).
fn prompt_for_mode(pmode: prompt::Mode) -> Result<InitMode> {
    if pmode == prompt::Mode::Auto {
        return Ok(InitMode::Greenfield);
    }
    let options = [
        "greenfield — brand-new cluster, no template",
        "use existing — I already wrote a cluster.toml; adopt it",
        "migrate from — adapt another cluster's cluster.toml to this site",
        "join shared — a colleague already runs labctl on this cluster",
    ];
    let idx = prompt::choice("What are you doing?", &options, 0, pmode)?;
    Ok(match idx {
        0 => InitMode::Greenfield,
        1 => {
            let p = prompt::path("path to your cluster.toml", None, pmode)?;
            InitMode::Use(p)
        }
        2 => {
            let p = prompt::path(
                "path to the source cluster.toml (foreign cluster)",
                None,
                pmode,
            )?;
            InitMode::MigrateFrom(p)
        }
        3 => {
            let p = prompt::path(
                "path to the shared cluster.toml (your colleague's)",
                None,
                pmode,
            )?;
            InitMode::Join(p)
        }
        _ => unreachable!(),
    })
}

/// Probe local SLURM for partition / QoS / GresTypes. Each step is
/// best-effort: missing binary, non-SLURM cluster, or permission
/// denied each degrade to a note rather than an error.
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

fn apply_probe(cfg: &mut ClusterConfig, probe: &SlurmProbe) {
    // Only fill what's empty — the foreign --from config's explicit
    // values win.
    if cfg.slurm.partition.is_none() {
        cfg.slurm.partition = probe.partition.clone();
    }
    if cfg.slurm.qos.is_none() {
        cfg.slurm.qos = probe.qos.clone();
    }
    if cfg.slurm.gres_gpu_syntax.is_none() {
        cfg.slurm.gres_gpu_syntax = probe.gres_gpu_syntax.clone();
    }
}

/// Walk through the user-editable fields, prompting with the
/// current value as default. Greenfield and Migrate-from both flow
/// through this — the differences are in what the defaults look
/// like (skeleton placeholders vs. foreign config values).
fn interactive_review(
    cfg: &mut ClusterConfig,
    mode: &InitMode,
    probe: &SlurmProbe,
    pmode: prompt::Mode,
) -> Result<()> {
    if !probe.notes.is_empty() {
        println!("SLURM probe results:");
        for note in &probe.notes {
            println!("  - {note}");
        }
        println!();
    }

    println!("Identity:");
    cfg.name = prompt::string("cluster name", Some(&cfg.name), pmode)?;
    println!();

    println!("Filesystem paths (Enter = accept default):");
    if let InitMode::MigrateFrom(p) = mode {
        println!("  (foreign config: {})", p.display());
    }
    let rb_default = cfg.filesystem.runs_base.display().to_string();
    cfg.filesystem.runs_base = prompt::path("runs_base", Some(&rb_default), pmode)?;

    let kinds: Vec<String> = cfg.filesystem.artifact_roots.keys().cloned().collect();
    for kind in kinds {
        let cur = cfg.filesystem.artifact_roots[&kind].display().to_string();
        let new = prompt::path(&format!("artifact_root[{kind}]"), Some(&cur), pmode)?;
        cfg.filesystem.artifact_roots.insert(kind, new);
    }
    println!();

    if !cfg.repos.is_empty() {
        println!("Repos (Enter = accept, empty path = remove):");
        let names: Vec<String> = cfg.repos.keys().cloned().collect();
        for name in names {
            let cur = cfg.repos[&name].display().to_string();
            let new = prompt::string(&format!("repo[{name}]"), Some(&cur), pmode)?;
            if new.is_empty() {
                cfg.repos.remove(&name);
            } else {
                cfg.repos.insert(name, PathBuf::from(new));
            }
        }
        if prompt::confirm("Add another repo?", false, pmode)? {
            loop {
                let n = prompt::string("repo name", None, pmode)?;
                let p = prompt::path("repo path", None, pmode)?;
                cfg.repos.insert(n, p);
                if !prompt::confirm("Add another?", false, pmode)? {
                    break;
                }
            }
        }
        println!();
    }

    println!("SLURM:");
    cfg.slurm.partition = optional_string("partition", &cfg.slurm.partition, pmode)?;
    cfg.slurm.qos = optional_string("qos", &cfg.slurm.qos, pmode)?;
    cfg.slurm.gres_gpu_syntax =
        optional_string("gres_gpu_syntax", &cfg.slurm.gres_gpu_syntax, pmode)?;
    println!();

    Ok(())
}

/// Prompt for an Option<String> field. Differs from prompt::string
/// in its empty-input semantics: empty = "accept current value"
/// (which may itself be None). To clear a field that has a current
/// value, the user types `-`. Avoids the "required" re-prompt loop
/// that prompt::string falls into for None-default fields, which
/// would otherwise trap a user past the SLURM section if no probe
/// values were found and they don't want to fill it in.
fn optional_string(
    label: &str,
    current: &Option<String>,
    pmode: prompt::Mode,
) -> Result<Option<String>> {
    if pmode == prompt::Mode::Auto {
        return Ok(current.clone());
    }
    use std::io::{self, BufRead, Write};
    let suffix = match current {
        Some(c) => format!("[{c}]"),
        None => "(empty = unset, `-` clears)".to_string(),
    };
    print!("  {label} {suffix}: ");
    io::stdout().flush().ok();
    let mut line = String::new();
    io::stdin().lock().read_line(&mut line)?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        Ok(current.clone())
    } else if trimmed == "-" {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

/// Where to put the cluster.toml. In Greenfield/Migrate we WRITE
/// here; in Use/Join we SYMLINK here pointing at the source.
fn pick_destination(
    opts: &InitOptions,
    cfg: &ClusterConfig,
    mode: prompt::Mode,
) -> Result<PathBuf> {
    if let Some(p) = &opts.output {
        return Ok(p.clone());
    }
    let xdg = xdg_default();
    if mode == prompt::Mode::Auto {
        return Ok(xdg);
    }
    let options = [
        "~/.config/labctl/cluster.toml (default for all labctl commands)",
        "./cluster.<name>.toml (explicit per-project)",
        "custom",
    ];
    let idx = prompt::choice("config destination", &options, 0, mode)?;
    match idx {
        0 => Ok(xdg),
        1 => Ok(PathBuf::from(format!("cluster.{}.toml", cfg.name))),
        2 => prompt::path("custom path", None, mode),
        _ => unreachable!(),
    }
}

fn xdg_default() -> PathBuf {
    if let Ok(x) = env::var("XDG_CONFIG_HOME") {
        if !x.is_empty() {
            return PathBuf::from(x).join("labctl").join("cluster.toml");
        }
    }
    let home = env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".config/labctl/cluster.toml")
}

#[derive(Debug, Clone, Copy)]
enum ExistingAction {
    Keep,
    Replace,
    Abort,
}

fn handle_existing(dest: &Path, mode: prompt::Mode) -> Result<ExistingAction> {
    println!("→ {} already exists.", dest.display());
    let options = ["keep existing (skip write, continue to dirs/agent/doctor)", "replace", "abort"];
    let idx = prompt::choice("what now?", &options, 0, mode)?;
    Ok(match idx {
        0 => ExistingAction::Keep,
        1 => ExistingAction::Replace,
        2 => ExistingAction::Abort,
        _ => unreachable!(),
    })
}

/// Write a fresh TOML (Greenfield/MigrateFrom) OR symlink/copy the
/// source file into the destination (Use/Join). Greenfield-style
/// header carries an explanatory comment so the user knows where
/// the file came from when they grep for it later.
fn write_or_link(
    cfg: &ClusterConfig,
    dest: &Path,
    mode: &InitMode,
    opts: &InitOptions,
) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }

    match mode {
        InitMode::Use(src) | InitMode::Join(src) => {
            // For these modes the source IS the truth. The
            // destination is just a pointer so plain `labctl <cmd>`
            // (without `--cluster`) finds the right file.
            let src_abs = std::fs::canonicalize(src)
                .with_context(|| format!("canonicalize {}", src.display()))?;
            if dest.exists() || dest.is_symlink() {
                std::fs::remove_file(dest)
                    .with_context(|| format!("remove existing {}", dest.display()))?;
            }
            if opts.copy_config {
                std::fs::copy(&src_abs, dest)
                    .with_context(|| format!("copy {} → {}", src_abs.display(), dest.display()))?;
                println!("→ copied {} → {}", src_abs.display(), dest.display());
            } else {
                #[cfg(unix)]
                std::os::unix::fs::symlink(&src_abs, dest).with_context(|| {
                    format!("symlink {} → {}", dest.display(), src_abs.display())
                })?;
                #[cfg(not(unix))]
                std::fs::copy(&src_abs, dest)
                    .with_context(|| format!("copy {} → {}", src_abs.display(), dest.display()))?;
                println!("→ symlinked {} → {}", dest.display(), src_abs.display());
            }
        }
        InitMode::Greenfield | InitMode::MigrateFrom(_) => {
            let copied_from = match mode {
                InitMode::MigrateFrom(p) => Some(p.as_path()),
                _ => None,
            };
            let body = serialize_config(cfg, copied_from)?;
            std::fs::write(dest, body)
                .with_context(|| format!("write {}", dest.display()))?;
            println!("→ wrote {}", dest.display());
        }
    }
    Ok(())
}

fn create_user_dirs(cfg: &ClusterConfig) -> Result<()> {
    let user = env::var("USER").unwrap_or_else(|_| "unknown".into());
    let mut targets: Vec<PathBuf> = Vec::new();
    targets.push(cfg.filesystem.runs_base.join("runs").join(&user));
    for (_kind, root) in &cfg.filesystem.artifact_roots {
        targets.push(root.join(&user));
    }
    let mut created = 0usize;
    let mut failed: Vec<(PathBuf, String)> = Vec::new();
    for t in targets {
        match std::fs::create_dir_all(&t) {
            Ok(()) => {
                println!("  ✓ {}", t.display());
                created += 1;
            }
            Err(e) => {
                println!("  ✗ {}: {}", t.display(), e);
                failed.push((t, e.to_string()));
            }
        }
    }
    if !failed.is_empty() {
        println!(
            "  ({} created, {} failed — doctor will recheck below)",
            created,
            failed.len()
        );
    }
    Ok(())
}

fn install_agent_unit(cluster_path: &Path, force: bool) -> Result<()> {
    let mut opts = service::InstallOptions::new(cluster_path.to_path_buf());
    opts.mode = service::UnitMode::Agent;
    opts.unit_name = "labctl-agent".to_string();
    opts.force = force;
    service::install(opts).context("agent install")
}

fn run_doctor_subprocess(cluster_path: &Path) -> Result<bool> {
    let exe = env::current_exe().context("current_exe")?;
    let status = Command::new(exe)
        .arg("--cluster")
        .arg(cluster_path)
        .arg("doctor")
        .status()
        .context("invoke labctl doctor")?;
    Ok(status.success())
}

/// Best-effort same-file check: canonicalize both sides and compare.
/// Used to detect when --use/--join's source already IS the
/// destination (idempotent re-run).
fn same_file(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => false,
    }
}

fn load_lax(path: &Path) -> Result<ClusterConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let cfg: ClusterConfig = toml::from_str(&text)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(cfg)
}

fn skeleton_config(name: Option<&str>) -> ClusterConfig {
    let mut artifact_roots = BTreeMap::new();
    artifact_roots.insert("dataset".to_string(), PathBuf::from("/path/to/datasets"));
    artifact_roots.insert("checkpoint".to_string(), PathBuf::from("/path/to/checkpoints"));
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

fn serialize_config(cfg: &ClusterConfig, copied_from: Option<&Path>) -> Result<String> {
    let header = match copied_from {
        Some(p) => format!(
            "# Adapted by `labctl init --migrate-from {}` — edit paths as needed.\n\
             # `labctl doctor` to verify writability, scheduler reachability, agent status.\n\n",
            p.display(),
        ),
        None => "# Generated by `labctl init`. Edit paths as needed.\n\
                 # `labctl doctor` to verify writability, scheduler reachability, agent status.\n\n"
            .to_string(),
    };
    let body = toml::to_string_pretty(cfg).context("serialize cluster config")?;
    Ok(format!("{header}{body}"))
}
