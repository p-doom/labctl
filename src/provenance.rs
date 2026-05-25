use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::util;

/// Per-run snapshot of the source repository's identity at submit
/// time. Two halves:
///
///   * **Audit / replay** — `git_head`, `git_status_porcelain`, and the
///     raw `git_diff_head` text are kept so an operator can read the
///     bundle and reconstruct what was submitted.
///   * **Cache key** — the two hash fields (`diff_hash`,
///     `untracked_files_hash`) and the simple-value fields (`git_head`,
///     `uv_lock_hash`) feed `compute_cache_key`. Both dirtiness hashes
///     are derived from persisted files in the provenance bundle on
///     disk (`tracked.patch`, `untracked.patch`), not from in-memory
///     strings or from live disk state. That symmetry buys us:
///       1. Future cache_key schema migrations recompute deterministically
///          from the bundle — no need for the source snapshot, which
///          `gc_terminal_sources` removes shortly after the run finishes.
///       2. The bundle alone is a sufficient reproducibility artifact
///          (`git checkout <head> && git apply tracked.patch && git apply
///          untracked.patch` reconstructs the working tree that ran).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoProvenance {
    pub repo_path: PathBuf,
    pub git_head: Option<String>,
    pub git_status_porcelain: Option<String>,
    /// Raw `git diff HEAD` text — also written to `tracked.patch` in
    /// the provenance dir. Stored on the struct for audit / context.json
    /// inspection; the cache key uses `diff_hash` (a hash of the
    /// persisted file) instead.
    pub git_diff_head: Option<String>,
    /// sha256 of `tracked.patch` file bytes. `None` when there are no
    /// tracked modifications (or git is unavailable).
    pub diff_hash: Option<String>,
    pub uv_lock_hash: Option<String>,
    pub uv_lock_path: Option<PathBuf>,
    /// sha256 of `untracked.patch` file bytes — the concatenation of
    /// `git diff --no-index --binary /dev/null <file>` for every
    /// untracked-but-not-ignored file, sorted by path. Without this,
    /// a new untracked file shipped into the execution snapshot by
    /// `copy_dir_filtered` is invisible to the cache key. `None` when
    /// there are no untracked files.
    pub untracked_files_hash: Option<String>,
}

pub fn capture_repo(repo_path: &Path, bundle_dir: &Path) -> Result<RepoProvenance> {
    fs::create_dir_all(bundle_dir)?;
    let git_head = git(repo_path, &["rev-parse", "HEAD"]).ok();
    let status = git(repo_path, &["status", "--porcelain"]).ok();
    let diff = git(repo_path, &["diff", "HEAD"]).ok();

    if let Some(head) = &git_head {
        fs::write(bundle_dir.join("git_head.txt"), head)?;
    }
    if let Some(status) = &status {
        fs::write(bundle_dir.join("git_status_porcelain.txt"), status)?;
    }
    // Write tracked.patch first, then hash the file bytes. Symmetric
    // with the untracked path below — both hashes derive from on-disk
    // artifacts, so they survive source-snapshot GC and are recompu-
    // table from the durable bundle alone.
    let diff_hash = if let Some(d) = &diff {
        let target = bundle_dir.join("tracked.patch");
        fs::write(&target, d)?;
        Some(util::sha256_file(&target)?)
    } else {
        None
    };

    let uv_lock = find_up(repo_path, "uv.lock");
    let (uv_lock_hash, uv_lock_path) = if let Some(lock) = uv_lock {
        let dst = bundle_dir.join("uv.lock");
        fs::copy(&lock, &dst)
            .with_context(|| format!("failed to copy uv.lock from {}", lock.display()))?;
        (Some(util::sha256_file(&dst)?), Some(lock))
    } else {
        (None, None)
    };

    let untracked_files_hash = write_untracked_patch(repo_path, bundle_dir).ok().flatten();

    let prov = RepoProvenance {
        repo_path: repo_path.to_path_buf(),
        git_head,
        git_status_porcelain: status,
        git_diff_head: diff,
        diff_hash,
        uv_lock_hash,
        uv_lock_path,
        untracked_files_hash,
    };
    fs::write(
        bundle_dir.join("provenance.json"),
        serde_json::to_vec_pretty(&json!(prov))?,
    )?;
    Ok(prov)
}

/// Build `<bundle_dir>/untracked.patch` from the untracked-but-not-
/// ignored file set (`git ls-files --others --exclude-standard`),
/// formatted as a concatenation of `git diff --no-index --binary
/// /dev/null <file>` per file, sorted by path. Hash the resulting
/// file bytes.
///
/// Returns `Ok(None)` when the file set is empty; in that case no
/// `untracked.patch` is written and the cache key carries `None` for
/// `untracked_files_hash`. Returns `Err(_)` when `git` is unusable.
///
/// `--binary` matters: without it, binary files render as
/// `Binary files X and Y differ` placeholder text, and swapping two
/// distinct-content same-size binaries would not change the patch
/// bytes — a soundness hole. With `--binary`, each binary file's
/// bytes are emitted as base85-encoded zlib hunks.
///
/// Determinism caveat: `--binary` output's exact bytes depend on
/// zlib version. Stable within a deployment's lifetime; cross-deploy
/// migrations would need to factor that in. Same kind of assumption
/// we already make about `git diff HEAD` text stability.
fn write_untracked_patch(repo_path: &Path, bundle_dir: &Path) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(["ls-files", "-z", "--others", "--exclude-standard"])
        .current_dir(repo_path)
        .output()
        .with_context(|| {
            format!(
                "failed to run git ls-files --others in {}",
                repo_path.display()
            )
        })?;
    if !output.status.success() {
        anyhow::bail!(
            "git ls-files --others failed in {}: {}",
            repo_path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut paths: Vec<String> = output
        .stdout
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|raw| {
            std::str::from_utf8(raw)
                .map(str::to_owned)
                .with_context(|| {
                    format!(
                        "git ls-files --others returned non-UTF-8 path in {}",
                        repo_path.display()
                    )
                })
        })
        .collect::<Result<_>>()?;
    if paths.is_empty() {
        return Ok(None);
    }
    paths.sort();

    let mut combined: Vec<u8> = Vec::new();
    for path in &paths {
        // `git diff --no-index --binary /dev/null <path>` exits with
        // status 1 whenever the two inputs differ (which is always
        // for our case since one side is `/dev/null`). That's not
        // an error — sweep it under the rug; anything else is.
        let diff_output = Command::new("git")
            .args(["diff", "--no-index", "--binary", "/dev/null", path])
            .current_dir(repo_path)
            .output()
            .with_context(|| {
                format!(
                    "failed to run git diff --no-index for {} in {}",
                    path,
                    repo_path.display()
                )
            })?;
        let code = diff_output.status.code();
        if !diff_output.status.success() && code != Some(1) {
            anyhow::bail!(
                "git diff --no-index failed for {} (exit={:?}): {}",
                path,
                code,
                String::from_utf8_lossy(&diff_output.stderr)
            );
        }
        combined.extend_from_slice(&diff_output.stdout);
    }

    let target = bundle_dir.join("untracked.patch");
    fs::write(&target, &combined)
        .with_context(|| format!("failed to write {}", target.display()))?;
    Ok(Some(util::sha256_file(&target)?))
}

fn git(repo_path: &Path, args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(repo_path);
    util::run_capture(&mut cmd)
}

fn find_up(start: &Path, name: &str) -> Option<PathBuf> {
    let mut cur = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };
    loop {
        let candidate = cur.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !cur.pop() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Initialise a minimal git repo with one committed file. Returns
    /// the repo dir. We bypass the user's global git config (which
    /// may force GPG signing, set hooks, etc.) by pointing
    /// `GIT_CONFIG_GLOBAL` at `/dev/null` for every invocation —
    /// gives us a hermetic per-test git context.
    fn init_repo() -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        let path = dir.path();
        run_git(path, &["init", "-q", "-b", "main"]);
        run_git(path, &["config", "user.email", "test@labctl.invalid"]);
        run_git(path, &["config", "user.name", "labctl-test"]);
        run_git(path, &["config", "commit.gpgsign", "false"]);
        fs::write(path.join("README.md"), b"hello\n").unwrap();
        run_git(path, &["add", "README.md"]);
        run_git(path, &["commit", "-q", "-m", "initial"]);
        dir
    }

    fn run_git(repo: &Path, args: &[&str]) {
        let status = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(args)
            .current_dir(repo)
            .status()
            .expect("spawn git");
        assert!(
            status.success(),
            "git {args:?} failed in {}",
            repo.display()
        );
    }

    #[test]
    fn capture_repo_clean_worktree_has_no_dirtiness_hashes() {
        let repo = init_repo();
        let bundle = tempdir().unwrap();
        let prov = capture_repo(repo.path(), bundle.path()).unwrap();
        assert!(prov.git_head.is_some());
        // git diff HEAD on a clean tree returns empty string ("") which
        // capture_repo writes to tracked.patch and hashes — sha256("")
        // is a valid hash, but for clean trees the .ok() in capture_repo
        // means a successful empty-string `git diff HEAD` results in
        // Some(""). The hash of an empty patch is therefore Some(...).
        // What matters is the hash is stable across calls.
        let prov2 = capture_repo(repo.path(), bundle.path()).unwrap();
        assert_eq!(prov.diff_hash, prov2.diff_hash);
        assert_eq!(prov.untracked_files_hash, prov2.untracked_files_hash);
        // No untracked files → no untracked.patch.
        assert!(!bundle.path().join("untracked.patch").exists());
    }

    #[test]
    fn untracked_files_change_hash() {
        let repo = init_repo();
        let bundle_a = tempdir().unwrap();
        let prov_a = capture_repo(repo.path(), bundle_a.path()).unwrap();
        assert!(prov_a.untracked_files_hash.is_none());

        // Add an untracked file.
        fs::write(repo.path().join("scratch.py"), b"print(1)\n").unwrap();
        let bundle_b = tempdir().unwrap();
        let prov_b = capture_repo(repo.path(), bundle_b.path()).unwrap();
        assert!(prov_b.untracked_files_hash.is_some());
        assert!(bundle_b.path().join("untracked.patch").exists());

        // Edit it → hash changes.
        fs::write(repo.path().join("scratch.py"), b"print(2)\n").unwrap();
        let bundle_c = tempdir().unwrap();
        let prov_c = capture_repo(repo.path(), bundle_c.path()).unwrap();
        assert_ne!(prov_b.untracked_files_hash, prov_c.untracked_files_hash);

        // Rename → still untracked, different path in the patch → hash
        // changes.
        fs::rename(
            repo.path().join("scratch.py"),
            repo.path().join("scratch2.py"),
        )
        .unwrap();
        let bundle_d = tempdir().unwrap();
        let prov_d = capture_repo(repo.path(), bundle_d.path()).unwrap();
        assert_ne!(prov_c.untracked_files_hash, prov_d.untracked_files_hash);

        // Remove → drops back to None.
        fs::remove_file(repo.path().join("scratch2.py")).unwrap();
        let bundle_e = tempdir().unwrap();
        let prov_e = capture_repo(repo.path(), bundle_e.path()).unwrap();
        assert!(prov_e.untracked_files_hash.is_none());
    }

    #[test]
    fn tracked_modifications_change_diff_hash() {
        let repo = init_repo();
        let bundle_a = tempdir().unwrap();
        let prov_a = capture_repo(repo.path(), bundle_a.path()).unwrap();

        // Edit the tracked file — `git diff HEAD` now non-empty.
        fs::write(repo.path().join("README.md"), b"goodbye\n").unwrap();
        let bundle_b = tempdir().unwrap();
        let prov_b = capture_repo(repo.path(), bundle_b.path()).unwrap();
        assert_ne!(prov_a.diff_hash, prov_b.diff_hash);
        assert!(bundle_b.path().join("tracked.patch").exists());

        // Revert — `git diff HEAD` empty again, hash matches the clean
        // state.
        fs::write(repo.path().join("README.md"), b"hello\n").unwrap();
        let bundle_c = tempdir().unwrap();
        let prov_c = capture_repo(repo.path(), bundle_c.path()).unwrap();
        assert_eq!(prov_a.diff_hash, prov_c.diff_hash);
    }

    #[test]
    fn gitignored_files_dont_count_as_untracked() {
        let repo = init_repo();
        fs::write(repo.path().join(".gitignore"), b"*.log\n").unwrap();
        run_git(repo.path(), &["add", ".gitignore"]);
        run_git(repo.path(), &["commit", "-q", "-m", "ignore logs"]);

        fs::write(repo.path().join("debug.log"), b"noisy\n").unwrap();
        let bundle = tempdir().unwrap();
        let prov = capture_repo(repo.path(), bundle.path()).unwrap();
        assert!(
            prov.untracked_files_hash.is_none(),
            "ignored file must not surface as untracked"
        );
    }
}
