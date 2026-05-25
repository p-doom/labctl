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
}
