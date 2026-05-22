//! On-disk filesystem layout for the labctl run/artifact tree.
//!
//! Postgres is the source of truth for runs, artifacts, aliases, eval
//! requests, pipelines, and events; this module owns the FS bits that
//! still have to live on shared storage:
//!   - Artifact bytes under `<artifact_roots[kind]>/_objects/<prefix>/<hash>/`,
//!     plus the per-user `aliases/<user>/<alias>` symlink overlay.
//!   - The slurm-compute → login bridge: `<run_dir>/.lab/context.json`
//!     and `<run_dir>/.lab/output_hashes.json`, written by the compute
//!     job because compute nodes can't reach PG.
//!   - The `_objects/<prefix>/<hash>/.meta.json` sidecar, a human-readable
//!     projection of the corresponding `artifacts` row.
//!
//! Atomicity rules:
//!   - FS writes go tmp + rename(2) (atomic on Lustre/GPFS within the
//!     same directory).
//!   - Cross-host claims (coalesce slot, eval slot) are owned by PG, not
//!     the filesystem — `INSERT ... ON CONFLICT` is the atomic primitive.
//!
//! Identity rules:
//!   - Every per-user path has the user as a segment (`runs/<user>/...`,
//!     `<kind>/<user>/...`). The directory's owner uid is the canonical
//!     submitter; `submitted_by` / `"user"` columns in PG are the
//!     authoritative copy.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------- subdirectory names under runs_base ----------

pub const RUNS_DIR: &str = "runs";

// ---------- file names inside a run's .lab/ ----------

pub const LAB_DIRNAME: &str = ".lab";
/// Written at submit time, consumed by the compute job via
/// `$LABCTL_CONTEXT`. Carries the recipe/inputs/outputs/params the
/// compute job needs — compute nodes can't reach PG, so this stays on NFS.
pub const CONTEXT_JSON: &str = "context.json";
pub const SUBMIT_SH: &str = "submit.sh";

// ---------- sidecars elsewhere ----------

pub const ARTIFACT_META: &str = ".meta.json";

// ---------- path computations ----------

pub fn runs_root(runs_base: &Path) -> PathBuf {
    runs_base.join(RUNS_DIR)
}

pub fn user_runs_dir(runs_base: &Path, user: &str) -> PathBuf {
    runs_root(runs_base).join(user)
}

pub fn run_dir(runs_base: &Path, user: &str, run_id: &str) -> PathBuf {
    user_runs_dir(runs_base, user).join(run_id)
}

pub fn artifact_dir(artifact_root: &Path, user: &str, alias: &str) -> PathBuf {
    artifact_root.join(user).join(alias)
}

// ---------- on-disk types ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSidecar {
    pub id: String,
    pub kind: String,
    pub user: String,
    pub alias: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_run_id: Option<String>,
    pub metadata: Value,
    pub created_at: i64,
}

/// ``.target.json`` for an in-flight coalesce claim. Recorded by the
/// producer at mkdir time so subsequent followers can identify who they
/// are waiting on. The actual ``afterok:`` job id is looked up from the
/// producer's registry row — keeping the claim sidecar minimal avoids
/// a second flush when the producer's job_id eventually lands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoalesceClaimSidecar {
    pub producer_run_id: String,
    pub claimed_at: i64,
}

// ---------- atomic primitives ----------

/// Write `bytes` to `path` atomically (tmp + rename in same dir). Creates
/// missing parent directories. Pre-existing target is replaced.
pub fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    let tmp = path.with_extension(match path.extension().and_then(|s| s.to_str()) {
        Some(ext) => format!("{ext}.tmp"),
        None => "tmp".to_string(),
    });
    fs::write(&tmp, &bytes)
        .with_context(|| format!("failed to write temp file {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to atomically replace {}", path.display()))?;
    Ok(())
}

/// Outcome of an atomic-claim attempt: did we win the race? Kept for the
/// PG-backed coalesce slot, whose claim path now goes through
/// `INSERT ... ON CONFLICT DO NOTHING` rather than the legacy NFS mkdir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    Claimed,
    AlreadyExists,
}

/// Mode bits for the shared-multi-user setup: group rwx + setgid (so
/// subdirs inherit the group), no permissions for other. Applied to
/// `runs_base` and each artifact root by `labctl init` when
/// `cluster.filesystem.shared_group` is configured.
pub const SHARED_DIR_MODE: u32 = 0o2770;

/// Validate a Unix group name. Same rules as `validate_user` (no slashes,
/// not `.`/`..`, non-empty) plus a length cap matching typical
/// `getgrnam` constraints. We don't verify membership in `/etc/group`
/// here — that's a deployment-time concern caught by the doctor check.
pub fn validate_group(group: &str) -> Result<()> {
    if group.is_empty() {
        bail!("group name must not be empty");
    }
    if group.contains('/') || group.contains('\\') || group == "." || group == ".." {
        bail!("group name must not contain slashes or be . / ..: {group:?}");
    }
    if group.len() > 32 {
        bail!("group name too long (>32 chars): {group:?}");
    }
    Ok(())
}

/// Look up a group's GID by name. Returns None if the group isn't in
/// `/etc/group` (or NSS). Linux-only; on other platforms this always
/// returns None and `apply_shared_perms` becomes a no-op.
#[cfg(unix)]
pub fn gid_for_group(group: &str) -> Option<u32> {
    use std::ffi::CString;
    let c = CString::new(group).ok()?;
    // SAFETY: getgrnam returns a pointer into a static buffer; we copy
    // the gid out immediately and don't retain the pointer.
    unsafe {
        let entry = libc::getgrnam(c.as_ptr());
        if entry.is_null() {
            None
        } else {
            Some((*entry).gr_gid)
        }
    }
}

#[cfg(not(unix))]
pub fn gid_for_group(_group: &str) -> Option<u32> {
    None
}

/// Set `path` to `SHARED_DIR_MODE` and chgrp to `group`. No-op on
/// non-Unix. If the group isn't resolvable, sets the mode but leaves
/// ownership alone and returns the error to the caller — init surfaces
/// this as a hard failure (you asked for a shared group; we couldn't
/// find it), runtime callers may downgrade to a warning.
#[cfg(unix)]
pub fn apply_shared_perms(path: &Path, group: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(SHARED_DIR_MODE))
        .with_context(|| format!("failed to chmod {} to {:o}", path.display(), SHARED_DIR_MODE))?;
    let gid = gid_for_group(group).with_context(|| {
        format!(
            "shared_group {group:?} not found in /etc/group; \
             check your cluster's group database (NSS / sssd)"
        )
    })?;
    // chown(path, uid=-1 leaves owner alone, gid=<group's gid>)
    let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
        .with_context(|| format!("path contains nul byte: {}", path.display()))?;
    // SAFETY: chown is async-signal-safe and takes a stable pointer to
    // our CString plus integer args; no aliasing or lifetime hazards.
    let rc = unsafe { libc::chown(c.as_ptr(), u32::MAX, gid) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!("failed to chgrp {} to {group:?} (gid={gid})", path.display())
        });
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn apply_shared_perms(_path: &Path, _group: &str) -> Result<()> {
    Ok(())
}

/// Validate a user identifier the way every per-user path segment will be
/// checked. Reject anything that could escape the prefix or shadow a
/// reserved subdir.
pub fn validate_user(user: &str) -> Result<()> {
    if user.is_empty() {
        bail!("user identifier must not be empty");
    }
    if user.contains('/') || user.contains('\\') || user == "." || user == ".." {
        bail!("user identifier must not contain slashes or be . / ..: {user:?}");
    }
    if user == RUNS_DIR {
        bail!("user identifier collides with the reserved subdir name: {user:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn atomic_write_json_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested/run.json");
        let value = serde_json::json!({ "id": "run_abc", "n": 1 });
        atomic_write_json(&path, &value).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        let back: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn validate_user_rejects_path_separators() {
        assert!(validate_user("alice/bob").is_err());
        assert!(validate_user("..").is_err());
        assert!(validate_user("runs").is_err());
        assert!(validate_user("alice").is_ok());
    }

}
