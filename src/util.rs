use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

pub fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to hash {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub fn dir_content_hash(path: &Path) -> Result<String> {
    let mut entries = Vec::new();
    for entry in WalkDir::new(path).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        if entry.file_type().is_file() {
            let rel = entry.path().strip_prefix(path)?;
            let hash = sha256_file(entry.path())?;
            entries.push(format!("{}\t{}", rel.display(), hash));
        }
    }
    Ok(sha256_bytes(entries.join("\n").as_bytes()))
}

pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!(
        "{}tmp",
        path.extension().and_then(|s| s.to_str()).unwrap_or("")
    ));
    fs::write(&tmp, contents)
        .with_context(|| format!("failed to write temp file {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to atomically replace {}", path.display()))?;
    Ok(())
}

pub fn run_capture(cmd: &mut Command) -> Result<String> {
    let output = cmd.output().with_context(|| "failed to execute command")?;
    if !output.status.success() {
        bail!(
            "command failed with status {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .trim_end()
        .to_string())
}

/// Snapshot `src` into `dst` using git as the authoritative filter:
/// `git ls-files -z --cached --others --exclude-standard` yields tracked
/// files + new-untracked files − ignored files, honoring nested
/// `.gitignore`, global excludes, and `.git/info/exclude`. No labctl-side
/// fallback skip list — `.lab/` lives under `runs_base`, not inside any
/// source repo, so the gitignore set is sufficient.
///
/// Symlinks are recreated as symlinks (not dereferenced). Files larger
/// than 1 GiB get a warning — single huge files in a source repo are
/// almost always a mistake.
pub fn copy_dir_filtered(src: &Path, dst: &Path) -> Result<()> {
    use std::os::unix::fs::symlink as unix_symlink;

    const LARGE_FILE_WARN: u64 = 1 << 30;

    if dst.exists() {
        fs::remove_dir_all(dst)
            .with_context(|| format!("failed to remove existing snapshot {}", dst.display()))?;
    }
    fs::create_dir_all(dst)?;

    let mut cmd = Command::new("git");
    cmd.args(["ls-files", "-z", "--cached", "--others", "--exclude-standard"])
        .current_dir(src);
    let output = cmd
        .output()
        .with_context(|| format!("failed to run git ls-files in {}", src.display()))?;
    if !output.status.success() {
        bail!(
            "git ls-files failed in {}: {}",
            src.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    for raw in output.stdout.split(|&b| b == 0) {
        if raw.is_empty() {
            continue;
        }
        let rel_str = std::str::from_utf8(raw)
            .with_context(|| format!("git ls-files returned non-UTF-8 path in {}", src.display()))?;
        let rel = Path::new(rel_str);

        let src_path = src.join(rel);
        let dst_path = dst.join(rel);

        let meta = match fs::symlink_metadata(&src_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Staged-for-deletion files appear in the index but not on disk.
                // The deletion is recorded in tracked.patch, so silent skip is correct.
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("stat {}", src_path.display())),
        };

        if meta.is_dir() {
            // Submodule gitlinks list as a single directory entry. Skipping is fine
            // for the current cluster (no submodules); revisit if that changes.
            continue;
        }

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(&src_path)
                .with_context(|| format!("read_link {}", src_path.display()))?;
            unix_symlink(&target, &dst_path).with_context(|| {
                format!("symlink {} -> {}", dst_path.display(), target.display())
            })?;
        } else if ft.is_file() {
            if meta.len() > LARGE_FILE_WARN {
                eprintln!(
                    "labctl: warning: copying {} MiB file into snapshot: {}",
                    meta.len() >> 20,
                    rel.display()
                );
            }
            fs::copy(&src_path, &dst_path).with_context(|| {
                format!(
                    "failed to copy {} to {}",
                    src_path.display(),
                    dst_path.display()
                )
            })?;
        }
    }
    Ok(())
}

pub fn shell_quote(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/' | ':' | '='))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\"'\"'"))
    }
}

