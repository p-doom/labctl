use std::{
    fs,
    io::{Read, Write},
    os::unix::{ffi::OsStrExt, fs::OpenOptionsExt, fs::PermissionsExt},
    path::Path,
    process::Command,
};

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

/// Versioned, canonical sha256 of a JSON value. Used by the
/// `recipe_hash` computation so a field reorder, a `#[serde(default)]`
/// addition, or a `#[serde(alias)]` rename in the underlying Rust
/// struct doesn't silently shift every historical hash.
///
/// Canonicalisation rules (RFC 8785-flavoured, not fully conformant —
/// we don't normalise number representations because serde already
/// emits a fixed canonical form for `serde_json::Number`):
///
///   * object members emitted in sorted key order;
///   * array order preserved (arrays are ordered by definition);
///   * no whitespace between tokens;
///   * each leaf encoded the way `serde_json::to_string` would emit it.
///
/// The version prefix lives in the *input* bytes (not the output) so
/// the hash stays a plain 64-char hex sha256 — matching the
/// `runs_recipe_hash_format` schema constraint. Bumping `version` is
/// how you intentionally invalidate every historical key (e.g. when
/// adding a new recipe field that should participate in identity).
pub fn canonical_value_hash(value: &serde_json::Value, version: &str) -> String {
    let mut buf = String::with_capacity(64);
    buf.push_str(version);
    buf.push('\n');
    canonicalize_into(value, &mut buf);
    sha256_bytes(buf.as_bytes())
}

fn canonicalize_into(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value;
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        // serde_json's `Number::to_string` matches its serialise output —
        // integers as bare digits, floats in shortest round-trippable
        // form — so this is canonical for our purposes.
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => {
            // Re-use serde's escape rules so embedded quotes, control
            // chars, and Unicode escapes match what a serialiser would
            // produce.
            out.push_str(&serde_json::to_string(s).expect("string serialise"));
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonicalize_into(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            // BTreeMap-style ordering: collect into a Vec, sort by key.
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(*k).expect("key serialise"));
                out.push(':');
                canonicalize_into(v, out);
            }
            out.push('}');
        }
    }
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to hash {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub fn dir_content_hash(path: &Path) -> Result<String> {
    tree_content_hash(path, "dir_content_hash/v2", false)
}

pub fn artifact_content_hash(path: &Path) -> Result<String> {
    tree_content_hash(path, "artifact_content_hash/v1", true)
}

pub fn seal_artifact_tree(path: &Path) -> Result<()> {
    for entry in WalkDir::new(path).follow_links(false).contents_first(true) {
        let entry = entry?;
        if entry.file_type().is_symlink() {
            bail!(
                "content-sealed artifact cannot contain symlink {}",
                entry.path().display()
            );
        }
        let metadata = entry.metadata()?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(permissions.mode() & !0o222);
        fs::set_permissions(entry.path(), permissions)
            .with_context(|| format!("failed to seal artifact path {}", entry.path().display()))?;
    }
    Ok(())
}

fn tree_content_hash(path: &Path, version: &str, exclude_artifact_sidecar: bool) -> Result<String> {
    if !path.is_dir() {
        bail!(
            "content-sealed artifact must be a directory: {}",
            path.display()
        );
    }
    let mut hasher = Sha256::new();
    hasher.update(version.as_bytes());
    hasher.update(b"\0");
    for entry in WalkDir::new(path).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        if entry.depth() == 0 {
            continue;
        }
        let rel = entry.path().strip_prefix(path)?;
        if exclude_artifact_sidecar
            && entry.depth() == 1
            && rel == Path::new(crate::fs_layout::ARTIFACT_META)
        {
            continue;
        }
        let rel_bytes = rel.as_os_str().as_bytes();
        hasher.update((rel_bytes.len() as u64).to_be_bytes());
        hasher.update(rel_bytes);
        let file_type = entry.file_type();
        if file_type.is_file() {
            hasher.update(b"f");
            let executable = entry.metadata()?.permissions().mode() & 0o111 != 0;
            hasher.update([u8::from(executable)]);
            let mut file = fs::File::open(entry.path())?;
            let mut buffer = [0_u8; 1024 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        } else if file_type.is_symlink() {
            if exclude_artifact_sidecar {
                bail!(
                    "content-sealed artifact cannot contain symlink {}",
                    entry.path().display()
                );
            }
            hasher.update(b"l");
            let target = fs::read_link(entry.path())?;
            let target_bytes = target.as_os_str().as_bytes();
            hasher.update((target_bytes.len() as u64).to_be_bytes());
            hasher.update(target_bytes);
        } else if file_type.is_dir() {
            hasher.update(b"d");
        } else {
            bail!("unsupported file type in {}", entry.path().display());
        }
    }
    Ok(hex::encode(hasher.finalize()))
}

pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|candidate| !candidate.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".labctl-{}.tmp", uuid::Uuid::now_v7().simple()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("failed to create temp file {}", temporary.display()))?;
    let write_result = file
        .write_all(contents)
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to durably write temp file {}", temporary.display()));
    drop(file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let cleanup = fs::remove_file(&temporary);
        return match cleanup {
            Ok(()) => Err(error)
                .with_context(|| format!("failed to atomically replace {}", path.display())),
            Err(cleanup_error) => Err(error).with_context(|| {
                format!(
                    "failed to atomically replace {}; failed to remove {}: {cleanup_error}",
                    path.display(),
                    temporary.display()
                )
            }),
        };
    }
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync parent directory {}", parent.display()))?;
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
    let source_root = src
        .canonicalize()
        .with_context(|| format!("failed to canonicalize source root {}", src.display()))?;

    let mut cmd = Command::new("git");
    cmd.args([
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ])
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
        let rel_str = std::str::from_utf8(raw).with_context(|| {
            format!("git ls-files returned non-UTF-8 path in {}", src.display())
        })?;
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
            bail!(
                "git submodule at {} cannot be materialized into an exact source snapshot",
                src_path.display()
            );
        }

        if let Some(parent) = dst_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(&src_path)
                .with_context(|| format!("read_link {}", src_path.display()))?;
            if target.is_absolute() {
                bail!(
                    "absolute symlink in source snapshot is not allowed: {} -> {}",
                    src_path.display(),
                    target.display()
                );
            }
            let resolved = src_path
                .parent()
                .context("source symlink has no parent")?
                .join(&target)
                .canonicalize()
                .with_context(|| {
                    format!(
                        "source symlink target is missing: {} -> {}",
                        src_path.display(),
                        target.display()
                    )
                })?;
            if !resolved.starts_with(&source_root) {
                bail!(
                    "source symlink escapes repository: {} -> {}",
                    src_path.display(),
                    target.display()
                );
            }
            unix_symlink(&target, &dst_path).with_context(|| {
                format!("symlink {} -> {}", dst_path.display(), target.display())
            })?;
        } else if ft.is_file() {
            if meta.len() > LARGE_FILE_WARN {
                tracing::info!(
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
    let snapshot_root = dst.canonicalize()?;
    for entry in WalkDir::new(dst).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_symlink() {
            let resolved = entry.path().canonicalize().with_context(|| {
                format!(
                    "source snapshot contains dangling symlink {}",
                    entry.path().display()
                )
            })?;
            if !resolved.starts_with(&snapshot_root) {
                bail!(
                    "source snapshot symlink escapes snapshot: {}",
                    entry.path().display()
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run_git(repo: &Path, args: &[&str]) {
        let status = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn init_snapshot_repo() -> tempfile::TempDir {
        let repo = tempfile::tempdir().unwrap();
        run_git(repo.path(), &["init", "-q", "-b", "main"]);
        run_git(
            repo.path(),
            &["config", "user.email", "test@labctl.invalid"],
        );
        run_git(repo.path(), &["config", "user.name", "labctl-test"]);
        fs::write(repo.path().join("tracked.txt"), b"tracked\n").unwrap();
        run_git(repo.path(), &["add", "tracked.txt"]);
        run_git(repo.path(), &["commit", "-q", "-m", "initial"]);
        repo
    }

    #[test]
    fn canonical_value_hash_is_key_order_independent() {
        // Same object, different key insertion order at construction
        // time. serde_json::Map preserves insertion order on the wire
        // when the `preserve_order` feature is on, which would make
        // naive `sha256(to_vec(value))` order-dependent — the canonical
        // hash must absorb that variance.
        let mut a = serde_json::Map::new();
        a.insert("zeta".into(), json!(1));
        a.insert("alpha".into(), json!(2));
        a.insert("mu".into(), json!(3));

        let mut b = serde_json::Map::new();
        b.insert("mu".into(), json!(3));
        b.insert("alpha".into(), json!(2));
        b.insert("zeta".into(), json!(1));

        let h_a = canonical_value_hash(&serde_json::Value::Object(a), "v1");
        let h_b = canonical_value_hash(&serde_json::Value::Object(b), "v1");
        assert_eq!(h_a, h_b, "key order must not affect canonical hash");
    }

    #[test]
    fn canonical_value_hash_recurses_into_nested_objects() {
        let outer_a = json!({
            "outer": { "z": 1, "a": 2 },
            "list": [{ "k": 1, "j": 2 }, { "b": "x", "a": "y" }],
        });
        let outer_b = json!({
            "list": [{ "j": 2, "k": 1 }, { "a": "y", "b": "x" }],
            "outer": { "a": 2, "z": 1 },
        });
        assert_eq!(
            canonical_value_hash(&outer_a, "v1"),
            canonical_value_hash(&outer_b, "v1"),
        );
    }

    #[test]
    fn canonical_value_hash_array_order_matters() {
        // Arrays are ordered — reordering them is a real semantic
        // change and the hash must reflect it.
        let a = json!([1, 2, 3]);
        let b = json!([3, 2, 1]);
        assert_ne!(
            canonical_value_hash(&a, "v1"),
            canonical_value_hash(&b, "v1"),
        );
    }

    #[test]
    fn canonical_value_hash_version_tag_separates() {
        let v = json!({ "x": 1 });
        let v1 = canonical_value_hash(&v, "recipe_hash/v1");
        let v2 = canonical_value_hash(&v, "recipe_hash/v2");
        assert_ne!(
            v1, v2,
            "bumping the version tag must change the hash even for byte-identical input"
        );
    }

    #[test]
    fn canonical_value_hash_handles_leaves() {
        // Smoke that each leaf variant terminates.
        let _ = canonical_value_hash(&json!(null), "v1");
        let _ = canonical_value_hash(&json!(true), "v1");
        let _ = canonical_value_hash(&json!(false), "v1");
        let _ = canonical_value_hash(&json!(0), "v1");
        let _ = canonical_value_hash(&json!(-1.5), "v1");
        let _ = canonical_value_hash(&json!("has \"quotes\" and \n newlines"), "v1");
    }

    #[test]
    fn atomic_write_replaces_with_private_file_and_no_temp_residue() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("context.json");
        atomic_write(&target, b"first").unwrap();
        atomic_write(&target, b"second").unwrap();

        assert_eq!(fs::read(&target).unwrap(), b"second");
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn dir_content_hash_covers_file_bytes_executable_bit_and_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("run.sh");
        fs::write(&file, b"exit 0\n").unwrap();
        let initial = dir_content_hash(dir.path()).unwrap();

        fs::write(&file, b"exit 1\n").unwrap();
        let changed_bytes = dir_content_hash(dir.path()).unwrap();
        assert_ne!(initial, changed_bytes);

        let mut permissions = fs::metadata(&file).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&file, permissions).unwrap();
        let executable = dir_content_hash(dir.path()).unwrap();
        assert_ne!(changed_bytes, executable);

        let link = dir.path().join("entrypoint");
        symlink("run.sh", &link).unwrap();
        let first_link = dir_content_hash(dir.path()).unwrap();
        fs::remove_file(&link).unwrap();
        symlink("other.sh", &link).unwrap();
        let changed_link = dir_content_hash(dir.path()).unwrap();
        assert_ne!(first_link, changed_link);
    }

    #[test]
    fn artifact_content_hash_ignores_sidecar_but_covers_payload() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("payload.bin"), b"first").unwrap();
        let initial = artifact_content_hash(dir.path()).unwrap();

        fs::write(
            dir.path().join(crate::fs_layout::ARTIFACT_META),
            b"metadata",
        )
        .unwrap();
        assert_eq!(initial, artifact_content_hash(dir.path()).unwrap());

        fs::write(dir.path().join("payload.bin"), b"second").unwrap();
        assert_ne!(initial, artifact_content_hash(dir.path()).unwrap());
    }

    #[test]
    fn artifact_content_hash_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("payload.bin"), b"payload").unwrap();
        symlink("payload.bin", dir.path().join("mutable-link")).unwrap();
        let err = artifact_content_hash(dir.path()).expect_err("symlink must be rejected");
        assert!(format!("{err:#}").contains("cannot contain symlink"));
    }

    #[test]
    fn seal_artifact_tree_removes_write_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let payload = nested.join("payload.bin");
        fs::write(&payload, b"payload").unwrap();

        seal_artifact_tree(dir.path()).unwrap();
        assert_eq!(
            fs::metadata(dir.path()).unwrap().permissions().mode() & 0o222,
            0
        );
        assert_eq!(
            fs::metadata(&nested).unwrap().permissions().mode() & 0o222,
            0
        );
        assert_eq!(
            fs::metadata(&payload).unwrap().permissions().mode() & 0o222,
            0
        );
    }

    #[test]
    fn source_snapshot_rejects_symlinks_outside_the_repo() {
        use std::os::unix::fs::symlink;

        let repo = init_snapshot_repo();
        symlink("/etc/passwd", repo.path().join("absolute-link")).unwrap();
        run_git(repo.path(), &["add", "absolute-link"]);
        let destination = tempfile::tempdir().unwrap();
        let snapshot = destination.path().join("source");
        let err = copy_dir_filtered(repo.path(), &snapshot).expect_err("absolute link must fail");
        assert!(format!("{err:#}").contains("absolute symlink"));

        fs::remove_file(repo.path().join("absolute-link")).unwrap();
        symlink("../", repo.path().join("escaping-link")).unwrap();
        run_git(repo.path(), &["add", "-A"]);
        let err = copy_dir_filtered(repo.path(), &snapshot).expect_err("escaping link must fail");
        assert!(format!("{err:#}").contains("escapes repository"));
    }

    #[test]
    fn source_snapshot_rejects_gitlinks() {
        let repo = init_snapshot_repo();
        let head = run_capture(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo.path()),
        )
        .unwrap();
        run_git(
            repo.path(),
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                "160000",
                &head,
                "vendor",
            ],
        );
        fs::create_dir(repo.path().join("vendor")).unwrap();
        let destination = tempfile::tempdir().unwrap();
        let snapshot = destination.path().join("source");
        let err = copy_dir_filtered(repo.path(), &snapshot).expect_err("gitlink must fail");
        assert!(format!("{err:#}").contains("git submodule"));
    }
}
