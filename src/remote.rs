//! Cross-cluster I/O. Thin wrappers around `ssh` and `rsync` for
//! reading a foreign cluster's registry sidecars and pulling artifact
//! bytes into the local cluster. Authentication (keys, OTP,
//! ControlMaster multiplexing, ProxyJump) is delegated entirely to the
//! user's `~/.ssh/config` — labctl is SSH-protocol-agnostic and just
//! invokes the standard tools.
//!
//! OTP-gated clusters (Jülich, LRZ, …) work transparently: ssh writes
//! the OTP prompt directly to `/dev/tty`, so it surfaces in the user's
//! terminal regardless of what labctl does with the child's
//! stdout/stderr. Repeated prompts within one import are collapsed by
//! the user's ControlMaster setup; labctl just calls ssh and trusts
//! the multiplex socket to be there.

use std::{
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;

use crate::config::RemoteConfig;

/// Read a JSON file from the remote cluster via `ssh <target> cat
/// <path>`. stdout is captured (the JSON we want); stderr inherits the
/// parent's terminal so SSH's banners and any non-OTP errors surface
/// to the user. OTP prompts go through ssh's `/dev/tty` write, also
/// surfaced naturally.
pub fn read_json<T: DeserializeOwned>(remote: &RemoteConfig, path: &Path) -> Result<T> {
    let target = remote.ssh_target()?;
    let path_str = path.display().to_string();
    let output = Command::new("ssh")
        .arg(&target)
        // `--` prevents the remote path from being parsed as ssh flags
        // if it ever starts with a dash.
        .arg("--")
        .arg("cat")
        .arg(&path_str)
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("failed to invoke ssh {target}"))?;
    if !output.status.success() {
        bail!(
            "ssh {target} cat {path_str} failed (exit {:?})",
            output.status.code(),
        );
    }
    serde_json::from_slice::<T>(&output.stdout).with_context(|| {
        format!("failed to parse JSON from {target}:{path_str}")
    })
}

/// Pull a directory tree from the remote cluster into `local_dst`
/// using `rsync -a --partial`. stdin/stdout/stderr inherit the
/// terminal so progress, OTP prompts, and errors flow through. The
/// rsync invocation uses `ssh` as the transport, picking up the same
/// `ControlMaster` socket that `read_json` opened.
pub fn rsync_dir(remote: &RemoteConfig, remote_src: &Path, local_dst: &Path) -> Result<()> {
    if let Some(parent) = local_dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to mkdir -p {}", parent.display()))?;
    }
    let target = remote.ssh_target()?;
    // Trailing slashes matter for rsync: `src/` copies CONTENTS into
    // dst; `src` (no slash) copies dst/src. We want the latter only if
    // dst already exists and we mean "drop the dir in there"; for an
    // artifact import we want dst to BE the imported dir, so we use
    // `src` (no slash) onto a dst that doesn't exist yet.
    let remote_arg = format!("{target}:{}", remote_src.display());
    let status = Command::new("rsync")
        .arg("-a")
        .arg("--partial")
        .arg("--info=progress2")
        .arg("-e")
        .arg("ssh")
        .arg(&remote_arg)
        .arg(local_dst)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("failed to invoke rsync")?;
    if !status.success() {
        bail!("rsync from {remote_arg} → {} failed", local_dst.display());
    }
    Ok(())
}

/// Best-effort reachability probe for the doctor. Tries an existing
/// ControlMaster socket first (`ssh -O check`); on failure, falls back
/// to a non-batch reachability test via `ssh -o ConnectTimeout=5
/// BatchMode=yes <target> true`. Returns:
///   `Ok(detail)`  — host is reachable with current credentials.
///   `Err(detail)` — host is unreachable, or only via interactive auth.
/// Never blocks on a prompt — `BatchMode=yes` ensures ssh fails fast
/// rather than asking for a password / OTP from the doctor's TTY.
pub fn probe_reachability(remote: &RemoteConfig) -> Result<String, String> {
    let target = match remote.ssh_target() {
        Ok(t) => t,
        Err(e) => return Err(format!("invalid [remote]: {e}")),
    };

    // First: is there a live multiplex session? If yes, every
    // subsequent labctl op will reuse it — that's the happy path on
    // OTP-gated hosts.
    let mc = Command::new("ssh")
        .args(["-O", "check"])
        .arg(&target)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if let Ok(s) = mc {
        if s.success() {
            return Ok(format!("multiplex session active for {target}"));
        }
    }

    // Fallback: try a non-batch reachability check. Won't authenticate
    // an OTP-gated host (BatchMode forbids prompts) — that's not a
    // failure here, it just means no session is established yet.
    let bm = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
        ])
        .arg(&target)
        .arg("--")
        .arg("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match bm {
        Ok(s) if s.success() => Ok(format!("key-auth reachable: {target}")),
        Ok(_) => Err(format!(
            "{target}: no live multiplex session, key-auth fails. \
             If this host requires OTP, run `ssh {target} true` once \
             to establish ControlMaster — subsequent labctl ops will \
             reuse it."
        )),
        Err(e) => Err(format!("failed to invoke ssh: {e}")),
    }
}
