//! On-disk schema for the labctl registry.
//!
//! The filesystem is the source of truth. Every fact about a run, artifact,
//! alias, eval request, pipeline, or event lives as a JSON sidecar at a
//! well-known path under `<runs_base>`, `<artifact_roots[kind]>`, or
//! `<output_roots[kind]>`. The in-memory SQLite cache (see `fs_store`) is
//! a derived index, rebuildable from this tree at any time.
//!
//! Atomicity rules:
//!   - Sidecars are written tmp + rename(2) (atomic on Lustre/GPFS within
//!     the same directory).
//!   - Namespace claims (alias names, eval_request keys) use mkdir(2) as
//!     the "first writer wins" primitive — also atomic on parallel FS.
//!
//! Identity rules:
//!   - Every path that records a user's intent has the user as a path
//!     segment (`runs/<user>/...`, `eval_state/<user>/...`, `<kind>/<user>/...`).
//!     The directory's owner uid is the canonical submitter; the
//!     `submitted_by` field in the JSON sidecar is a convenience copy.
//!   - The shared `aliases/` namespace has no user prefix because alias
//!     uniqueness is global by design.

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
/// Written by the sbatch wrapper at runtime. Source of truth for `status`
/// until reconcile observes terminal state via sacct. Compute → login
/// bridge for status; PG carries everything else.
pub const STATUS_JSON: &str = "status.json";
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

/// Subdir under each `artifact_roots[kind]` where bytes live, keyed on
/// content_hash. Mirrors the parallel `_provenance_objects/` convention
/// under `runs_base/`. Underscore prefix marks it as opaque internal
/// storage: humans browse via `aliases/<user>/<alias>` symlinks
/// instead.
pub const OBJECTS_DIR: &str = "_objects";

/// Subdir under each `artifact_roots[kind]` holding the human-readable
/// alias overlay: `<root>/aliases/<user>/<alias>` is a symlink to the
/// corresponding `_objects/<prefix>/<hash>/`. Multiple users can claim
/// the same content; they get separate symlinks pointing at one dir.
pub const ALIASES_USER_DIR: &str = "aliases";

/// `<artifact_root>/_objects/<hash[:2]>/<hash>/` — the canonical home
/// for an artifact's bytes under content-addressed storage. The 2-char
/// prefix bounds dir entries (otherwise `ls` of `_objects/` is
/// unbounded across thousands of artifacts).
pub fn content_addressed_dir(kind_root: &Path, content_hash: &str) -> PathBuf {
    let prefix = &content_hash[..2.min(content_hash.len())];
    kind_root.join(OBJECTS_DIR).join(prefix).join(content_hash)
}

/// `<artifact_root>/aliases/<user>/<alias>` — symlink path for the
/// per-user alias overlay. Multiple aliases per user, multiple users
/// per artifact; the only uniqueness is `(user, alias)`.
pub fn alias_symlink_path(kind_root: &Path, user: &str, alias: &str) -> PathBuf {
    kind_root.join(ALIASES_USER_DIR).join(user).join(alias)
}

/// Create or replace an alias symlink pointing at the canonical
/// `_objects/...` dir. Target stored relative to the symlink so the
/// link survives directory-tree moves (e.g. mounting the artifact
/// root at a different path on another host).
///
/// Idempotent: if the symlink already points at `target`, no-op.
/// If the symlink exists but points elsewhere, replaces it atomically
/// (write to tmp + rename).
#[cfg(unix)]
pub fn create_alias_symlink(symlink: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = symlink.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create alias parent {}", parent.display()))?;
    }
    let symlink_parent = symlink.parent().with_context(|| {
        format!("alias symlink {} has no parent", symlink.display())
    })?;
    let relative_target = pathdiff_relative(target, symlink_parent);
    if let Ok(existing) = fs::read_link(symlink) {
        if existing == relative_target {
            return Ok(());
        }
        // Replace via tmp + rename for atomicity.
    }
    let tmp = symlink.with_extension("link.tmp");
    let _ = fs::remove_file(&tmp);
    std::os::unix::fs::symlink(&relative_target, &tmp)
        .with_context(|| format!("failed to symlink {} -> {}", tmp.display(), relative_target.display()))?;
    fs::rename(&tmp, symlink)
        .with_context(|| format!("failed to rename {} -> {}", tmp.display(), symlink.display()))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn create_alias_symlink(_symlink: &Path, _target: &Path) -> Result<()> {
    bail!("alias symlinks not supported on this platform");
}

/// Compute `target` as a relative path from `from_dir`. Pure-string
/// path arithmetic; doesn't touch the filesystem. Falls back to the
/// absolute target when the paths share no prefix or one is not
/// absolute (caller's bug).
fn pathdiff_relative(target: &Path, from_dir: &Path) -> PathBuf {
    if !target.is_absolute() || !from_dir.is_absolute() {
        return target.to_path_buf();
    }
    let t: Vec<_> = target.components().collect();
    let f: Vec<_> = from_dir.components().collect();
    let mut i = 0;
    while i < t.len() && i < f.len() && t[i] == f[i] {
        i += 1;
    }
    let mut out = PathBuf::new();
    for _ in i..f.len() {
        out.push("..");
    }
    for c in &t[i..] {
        out.push(c.as_os_str());
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

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

    #[test]
    fn content_addressed_dir_uses_two_char_prefix() {
        let root = Path::new("/datasets");
        let d = content_addressed_dir(root, "abcdef1234567890");
        assert_eq!(d, Path::new("/datasets/_objects/ab/abcdef1234567890"));
    }

    #[test]
    #[cfg(unix)]
    fn create_alias_symlink_is_relative_and_idempotent() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let target = content_addressed_dir(root, "deadbeef".repeat(8).as_str());
        fs::create_dir_all(&target).unwrap();
        let link = alias_symlink_path(root, "alice", "ds_v1");

        create_alias_symlink(&link, &target).unwrap();
        let actual = fs::read_link(&link).unwrap();
        // Must be relative (so the tree can be moved/mounted elsewhere
        // without invalidating links).
        assert!(actual.is_relative(), "expected relative target, got {actual:?}");
        // Resolving through the link gives back the canonical dir.
        let resolved = link.parent().unwrap().join(&actual).canonicalize().unwrap();
        assert_eq!(resolved, target.canonicalize().unwrap());

        // Idempotent: second call doesn't error or churn.
        create_alias_symlink(&link, &target).unwrap();

        // Repointing to a different target replaces atomically.
        let other = content_addressed_dir(root, "feedface".repeat(8).as_str());
        fs::create_dir_all(&other).unwrap();
        create_alias_symlink(&link, &other).unwrap();
        let resolved2 = link.parent().unwrap().join(fs::read_link(&link).unwrap()).canonicalize().unwrap();
        assert_eq!(resolved2, other.canonicalize().unwrap());
    }
}
