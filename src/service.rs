//! `labctl service install / uninstall / status` — manage the systemd
//! user unit that keeps `labctl serve` alive. One unit, one process:
//! the UI and dispatch loops (reconcile + evald + throttle) share a
//! single in-memory cache and run inside the same tokio runtime.
//!
//! Why an explicit subcommand and not auto-install on first run: labctl
//! runs on HPC login nodes, dev laptops, containers, CI. systemd is one
//! of those environments and only one. Auto-installing would be a
//! surprise side-effect on the rest. Instead, every command stays
//! "what it says on the tin," and `service install` is the one place
//! that touches the host's process supervisor.
//!
//! Dependencies:
//!   - systemd 200+ (anything with `--user` support — already on RHEL 8+)
//!   - `loginctl enable-linger $USER` so the unit survives logout. We
//!     check and warn but don't auto-enable; that requires the user's
//!     own systemd user manager and the policy may forbid it on shared
//!     hosts.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};

const DEFAULT_UNIT_NAME: &str = "labctl";
const DEFAULT_BIND: &str = "127.0.0.1:8765";

/// Which long-running labctl process the systemd unit should start.
///
/// - `Serve { no_dispatch: false }` — all-in-one (HTTP + dispatch in
///   one process). The single-user default; install via
///   `labctl service install`.
/// - `Serve { no_dispatch: true }` — pure read-only HTTP window. The
///   shared UI host in a multi-tenant rollout; install via
///   `labctl service install --no-dispatch`.
/// - `Agent` — dispatch-only, no HTTP listener. Each user's own
///   reconcile + evald in the multi-tenant model; install via
///   `labctl service install --agent`.
pub enum UnitMode {
    Serve { bind: String, no_dispatch: bool },
    Agent,
}

pub struct InstallOptions {
    pub cluster_path: PathBuf,
    pub mode: UnitMode,
    pub unit_name: String,
    pub force: bool,
}

impl InstallOptions {
    pub fn new(cluster_path: PathBuf) -> Self {
        Self {
            cluster_path,
            mode: UnitMode::Serve {
                bind: DEFAULT_BIND.to_string(),
                no_dispatch: false,
            },
            unit_name: DEFAULT_UNIT_NAME.to_string(),
            force: false,
        }
    }
}

/// Render the systemd unit file as a string. Pure function — exposed for
/// tests so we can verify the unit shape without touching the filesystem.
pub fn render_unit(
    binary_path: &Path,
    cluster_path: &Path,
    mode: &UnitMode,
    unit_name: &str,
) -> String {
    let exec_args = match mode {
        UnitMode::Serve { bind, no_dispatch } => {
            let suffix = if *no_dispatch { " --no-dispatch" } else { "" };
            format!("serve --bind {bind}{suffix}")
        }
        UnitMode::Agent => "agent".to_string(),
    };
    format!(
        "[Unit]
Description=labctl ({unit_name})
After=network-online.target
StartLimitIntervalSec=120
StartLimitBurst=5

[Service]
Type=simple
ExecStart={binary} --cluster {cluster} {exec_args}
Restart=on-failure
RestartSec=5
# Capture stdout/stderr through journalctl so it's `journalctl --user -u {unit_name}`-able.
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
",
        binary = binary_path.display(),
        cluster = cluster_path.display(),
    )
}

pub fn install(opts: InstallOptions) -> Result<()> {
    require_systemd()?;
    let binary = std::env::current_exe()
        .context("could not determine the path to the running labctl binary")?;
    // We pin paths to absolutes so the unit doesn't break if launched
    // from a different cwd.
    let cluster = opts
        .cluster_path
        .canonicalize()
        .with_context(|| format!("cluster config not found: {}", opts.cluster_path.display()))?;
    let unit_path = unit_path(&opts.unit_name)?;
    if unit_path.exists() && !opts.force {
        bail!(
            "{} already exists. Re-run with --force to overwrite, or `labctl service uninstall` first.",
            unit_path.display()
        );
    }
    let unit = render_unit(&binary, &cluster, &opts.mode, &opts.unit_name);
    if let Some(parent) = unit_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&unit_path, unit)
        .with_context(|| format!("failed to write {}", unit_path.display()))?;
    eprintln!("wrote {}", unit_path.display());

    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", &opts.unit_name])?;

    if !linger_enabled() {
        eprintln!(
            "warning: linger is not enabled for $USER. The unit will stop when you log out.\n\
             Run `loginctl enable-linger $USER` (may require sudo on some sites) to keep it alive across sessions."
        );
    }

    eprintln!(
        "\nlabctl service installed and started.\n  status:  systemctl --user status {name}\n  logs:    journalctl --user -u {name} -f\n  stop:    systemctl --user stop {name}\n  remove:  labctl service uninstall",
        name = opts.unit_name,
    );
    Ok(())
}

pub fn uninstall(unit_name: &str) -> Result<()> {
    require_systemd()?;
    let unit_path = unit_path(unit_name)?;
    // `disable --now` stops the running unit and removes the symlink.
    // Errors here are non-fatal — the user might be uninstalling
    // something that's already partially gone.
    let _ = run_systemctl(&["disable", "--now", unit_name]);
    if unit_path.exists() {
        std::fs::remove_file(&unit_path)
            .with_context(|| format!("failed to remove {}", unit_path.display()))?;
        eprintln!("removed {}", unit_path.display());
    } else {
        eprintln!("no unit file at {} (already removed)", unit_path.display());
    }
    let _ = run_systemctl(&["daemon-reload"]);
    Ok(())
}

pub fn status(unit_name: &str) -> Result<()> {
    require_systemd()?;
    // Pass through to systemctl with no output redirection so the user
    // gets the familiar `systemctl --user status` formatting.
    let exit = Command::new("systemctl")
        .args(["--user", "status", unit_name])
        .status()
        .context("failed to invoke systemctl")?;
    // `systemctl status` returns nonzero when the unit is dead/inactive,
    // which is informational — not an error from our perspective.
    let _ = exit;
    Ok(())
}

/// Where labctl writes its unit file. Honors $XDG_CONFIG_HOME for
/// non-standard layouts; otherwise ~/.config/systemd/user/.
fn unit_path(unit_name: &str) -> Result<PathBuf> {
    let dir = if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(xdg).join("systemd/user")
    } else {
        let home = std::env::var("HOME").context("HOME is not set")?;
        PathBuf::from(home).join(".config/systemd/user")
    };
    Ok(dir.join(format!("{unit_name}.service")))
}

/// Refuse to run if the host doesn't have systemd-user available. Better
/// to fail with a clear message than spew opaque errors from a missing
/// `systemctl` binary.
fn require_systemd() -> Result<()> {
    if which::which("systemctl").is_err() {
        bail!(
            "systemctl not found on PATH. labctl service requires systemd \
             with --user support. On macOS, you'll need a launchd plist \
             instead — labctl doesn't generate one automatically yet."
        );
    }
    let out = Command::new("systemctl")
        .args(["--user", "--version"])
        .output()
        .context("failed to invoke systemctl --user")?;
    if !out.status.success() {
        bail!(
            "`systemctl --user` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

fn run_systemctl(args: &[&str]) -> Result<()> {
    let mut cmd = Command::new("systemctl");
    cmd.arg("--user");
    for a in args {
        cmd.arg(a);
    }
    let out = cmd
        .output()
        .with_context(|| format!("failed to invoke systemctl --user {}", args.join(" ")))?;
    if !out.status.success() {
        bail!(
            "systemctl --user {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    if !out.stdout.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&out.stdout));
    }
    Ok(())
}

fn linger_enabled() -> bool {
    let user = match std::env::var("USER") {
        Ok(u) => u,
        Err(_) => return false,
    };
    let out = Command::new("loginctl")
        .args(["show-user", &user, "-p", "Linger", "--value"])
        .output();
    match out {
        Ok(o) if o.status.success() => {
            String::from_utf8_lossy(&o.stdout).trim() == "yes"
        }
        _ => false,
    }
}

/// Returns true when a service unit is installed at the expected path.
/// Used by the first-run hint in `labctl run`.
pub fn is_installed(unit_name: &str) -> bool {
    unit_path(unit_name).map(|p| p.exists()).unwrap_or(false)
}

/// Returns true when systemd-user looks usable on this host. Mirrors the
/// gate in `require_systemd` but never errors — used to decide whether
/// to print the first-run hint.
pub fn systemd_available() -> bool {
    if which::which("systemctl").is_err() {
        return false;
    }
    Command::new("systemctl")
        .args(["--user", "--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn unit_renders_with_absolute_paths_and_correct_exec_start() {
        let unit = render_unit(
            &PathBuf::from("/usr/local/bin/labctl"),
            &PathBuf::from("/etc/labctl/cluster.toml"),
            &UnitMode::Serve {
                bind: "127.0.0.1:8765".to_string(),
                no_dispatch: false,
            },
            "labctl",
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/labctl --cluster /etc/labctl/cluster.toml serve --bind 127.0.0.1:8765"
        ));
        assert!(!unit.contains("--no-dispatch"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(!unit.contains("ExecStart=labctl"));
    }

    #[test]
    fn agent_unit_omits_bind_and_uses_agent_subcommand() {
        let unit = render_unit(
            &PathBuf::from("/usr/local/bin/labctl"),
            &PathBuf::from("/etc/labctl/cluster.toml"),
            &UnitMode::Agent,
            "labctl-agent",
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/labctl --cluster /etc/labctl/cluster.toml agent"
        ));
        assert!(!unit.contains("--bind"));
        assert!(!unit.contains("serve"));
    }

    #[test]
    fn ui_unit_appends_no_dispatch_flag() {
        let unit = render_unit(
            &PathBuf::from("/usr/local/bin/labctl"),
            &PathBuf::from("/etc/labctl/cluster.toml"),
            &UnitMode::Serve {
                bind: "127.0.0.1:8765".to_string(),
                no_dispatch: true,
            },
            "labctl-ui",
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/labctl --cluster /etc/labctl/cluster.toml serve --bind 127.0.0.1:8765 --no-dispatch"
        ));
    }

    #[test]
    fn unit_name_appears_in_description_and_log_hints() {
        let unit = render_unit(
            &PathBuf::from("/x"),
            &PathBuf::from("/y"),
            &UnitMode::Serve {
                bind: "127.0.0.1:9000".to_string(),
                no_dispatch: false,
            },
            "labctl-staging",
        );
        assert!(unit.contains("labctl (labctl-staging)"));
        assert!(unit.contains("-u labctl-staging"));
    }

    #[test]
    fn unit_path_uses_xdg_when_set() {
        let prev_xdg = std::env::var("XDG_CONFIG_HOME").ok();
        // SAFETY: tests run single-threaded under cargo test --bin labctl
        // by default; this set/restore pattern is fine.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", "/tmp/xdg") };
        let p = unit_path("labctl").unwrap();
        assert_eq!(p, PathBuf::from("/tmp/xdg/systemd/user/labctl.service"));
        match prev_xdg {
            Some(v) => unsafe { std::env::set_var("XDG_CONFIG_HOME", v) },
            None => unsafe { std::env::remove_var("XDG_CONFIG_HOME") },
        }
    }
}
