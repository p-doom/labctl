//! `labctl service install / uninstall / status` — manage the systemd
//! user units that keep labctl's two long-running processes alive.
//!
//! Two unit shapes only:
//!   - `labctl-agent.service` (auto-installed by `labctl init`) — the
//!     per-user dispatch loop (reconcile + evald + throttle).
//!   - `labctl-ui.service` (opt-in) — the read-only HTTP window.
//!
//! Per-user vs shared: the agent is always per-user. The UI is loopback-
//! by-default and either ships per-user or as one shared instance on
//! a designated login node, depending on rollout.
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

pub const DEFAULT_BIND: &str = "127.0.0.1:8765";
pub const AGENT_UNIT_NAME: &str = "labctl-agent";
pub const UI_UNIT_NAME: &str = "labctl-ui";

/// Which long-running labctl process the systemd unit should start.
///
/// - `Agent` — dispatch-only (reconcile + evald + throttle), no HTTP
///   listener. Auto-installed by `labctl init`. Unit: `labctl-agent`.
/// - `Ui { bind }` — pure read-only HTTP window. Opt-in via
///   `labctl service install --ui`. Unit: `labctl-ui`.
pub enum UnitMode {
    Agent,
    Ui { bind: String },
}

impl UnitMode {
    /// Canonical unit name for this mode. Encoding the mapping here
    /// keeps `install` and `uninstall` from disagreeing about names.
    pub fn unit_name(&self) -> &'static str {
        match self {
            UnitMode::Agent => AGENT_UNIT_NAME,
            UnitMode::Ui { .. } => UI_UNIT_NAME,
        }
    }
}

pub struct InstallOptions {
    pub cluster_path: PathBuf,
    pub mode: UnitMode,
    pub force: bool,
}

/// Render the systemd unit file as a string. Pure function — exposed for
/// tests so we can verify the unit shape without touching the filesystem.
pub fn render_unit(binary_path: &Path, cluster_path: &Path, mode: &UnitMode) -> String {
    let unit_name = mode.unit_name();
    let exec_args = match mode {
        UnitMode::Agent => "agent".to_string(),
        UnitMode::Ui { bind } => format!("serve --bind {bind}"),
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
    let unit_name = opts.mode.unit_name();
    let unit_path = unit_path(unit_name)?;
    if unit_path.exists() && !opts.force {
        bail!(
            "{} already exists. Re-run with --force to overwrite, or `labctl service uninstall` first.",
            unit_path.display()
        );
    }
    let unit = render_unit(&binary, &cluster, &opts.mode);
    if let Some(parent) = unit_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&unit_path, unit)
        .with_context(|| format!("failed to write {}", unit_path.display()))?;
    eprintln!("wrote {}", unit_path.display());

    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", unit_name])?;

    if !linger_enabled() {
        eprintln!(
            "warning: linger is not enabled for $USER. The unit will stop when you log out.\n\
             Run `loginctl enable-linger $USER` (may require sudo on some sites) to keep it alive across sessions."
        );
    }

    eprintln!(
        "\nlabctl service installed and started.\n  status:  systemctl --user status {unit_name}\n  logs:    journalctl --user -u {unit_name} -f\n  stop:    systemctl --user stop {unit_name}\n  remove:  labctl service uninstall {flag}",
        flag = match opts.mode { UnitMode::Agent => "--agent", UnitMode::Ui { .. } => "--ui" },
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

/// Restart one or more installed units. Passes every name in a single
/// `systemctl --user restart` call so the action is atomic from the
/// user's perspective. Caller is responsible for choosing the right
/// units (the CLI gates this on `is_installed` for the default-both
/// path).
pub fn restart(unit_names: &[&str]) -> Result<()> {
    require_systemd()?;
    if unit_names.is_empty() {
        bail!("no units to restart");
    }
    let mut args: Vec<&str> = vec!["restart"];
    args.extend_from_slice(unit_names);
    run_systemctl(&args)?;
    eprintln!("restarted: {}", unit_names.join(", "));
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
    fn agent_unit_omits_bind_and_uses_agent_subcommand() {
        let unit = render_unit(
            &PathBuf::from("/usr/local/bin/labctl"),
            &PathBuf::from("/etc/labctl/cluster.toml"),
            &UnitMode::Agent,
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/labctl --cluster /etc/labctl/cluster.toml agent"
        ));
        assert!(!unit.contains("--bind"));
        assert!(!unit.contains("serve"));
        assert!(unit.contains("labctl (labctl-agent)"));
        assert!(unit.contains("-u labctl-agent"));
    }

    #[test]
    fn ui_unit_serves_http_only() {
        let unit = render_unit(
            &PathBuf::from("/usr/local/bin/labctl"),
            &PathBuf::from("/etc/labctl/cluster.toml"),
            &UnitMode::Ui { bind: "127.0.0.1:8765".to_string() },
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/labctl --cluster /etc/labctl/cluster.toml serve --bind 127.0.0.1:8765"
        ));
        // `serve` is HTTP-only now; the old --no-dispatch flag is gone.
        assert!(!unit.contains("--no-dispatch"));
        assert!(unit.contains("labctl (labctl-ui)"));
        assert!(unit.contains("-u labctl-ui"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn unit_path_uses_xdg_when_set() {
        let prev_xdg = std::env::var("XDG_CONFIG_HOME").ok();
        // SAFETY: tests run single-threaded under cargo test --bin labctl
        // by default; this set/restore pattern is fine.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", "/tmp/xdg") };
        let p = unit_path("labctl-ui").unwrap();
        assert_eq!(p, PathBuf::from("/tmp/xdg/systemd/user/labctl-ui.service"));
        match prev_xdg {
            Some(v) => unsafe { std::env::set_var("XDG_CONFIG_HOME", v) },
            None => unsafe { std::env::remove_var("XDG_CONFIG_HOME") },
        }
    }
}
