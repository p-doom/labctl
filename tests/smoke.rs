//! End-to-end smoke tests against the built binary.
//!
//! Each test invokes `target/<profile>/labctl validate <file>` on
//! the example configs that ship with the repo. The goal is to catch
//! schema drift: if someone changes the loader without updating
//! examples (or vice versa), CI fails here rather than at a user's
//! `git clone`. Tests are deliberately I/O-only — no SLURM, no
//! registry, no UI.

use std::{path::PathBuf, process::Command};

/// Locate the just-built `labctl` binary. Cargo sets CARGO_BIN_EXE_<name>
/// for binary crates' integration tests, so this is the canonical path
/// to the binary under test. Bypasses any PATH ambiguity.
fn labctl_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_labctl"))
}

fn validate(path: &str) {
    let out = Command::new(labctl_bin())
        .args(["validate", path])
        .output()
        .expect("failed to invoke labctl");
    assert!(
        out.status.success(),
        "labctl validate {path} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn example_clusters_parse() {
    for name in ["single-user", "multi-tenant", "with-remote"] {
        validate(&format!("examples/clusters/{name}.toml"));
    }
}

#[test]
fn example_recipes_parse() {
    for name in ["train", "eval", "sweep"] {
        validate(&format!("examples/recipes/{name}.toml"));
    }
}

#[test]
fn example_policy_parses() {
    validate("examples/policies/eval_per_checkpoint.toml");
}

// Note: cluster.berlin.toml at the repo root carries operational
// paths (`[dispatch].policies_dir = "policies"`) that only resolve
// in the SLURM-repo location it actually runs from. We intentionally
// don't `labctl validate` it here — that would fail on every CI
// run. Schema drift would surface immediately on the live cluster's
// next `labctl doctor` instead, which is loud enough.

/// Shared helper: run `labctl init` with the given args under a
/// tempdir, with no side effects (--no-create-dirs, --no-agent,
/// --no-doctor) so the test stays sandboxed. Returns the path to
/// the destination file.
fn run_init_in_tempdir(args: &[&str]) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("cluster.smoke.toml");
    let mut argv: Vec<String> = vec![
        "init".into(),
        "--yes".into(),
        "--no-detect".into(),
        "--no-create-dirs".into(),
        "--no-agent".into(),
        "--no-doctor".into(),
        "--output".into(),
        out.display().to_string(),
    ];
    for a in args {
        argv.push((*a).to_string());
    }
    let result = Command::new(labctl_bin())
        .args(&argv)
        .output()
        .expect("invoke labctl init");
    assert!(
        result.status.success(),
        "labctl init {argv:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
    );
    assert!(out.exists() || out.is_symlink(), "init didn't create {}", out.display());
    (tmp, out)
}

#[test]
fn labctl_init_greenfield_writes_loadable_config() {
    // `--yes --no-detect` skips both interactive prompts and the
    // SLURM probes; `--runs-base` + an `--artifact-root` flag give
    // the loader enough to pass `labctl validate`.
    let tmp = tempfile::tempdir().expect("tempdir");
    let runs_base = tmp.path().join("runs");
    let artifact_root = tmp.path().join("ck");
    let (_keep, out) = run_init_in_tempdir(&[
        "--name",
        "smoke-greenfield",
        "--runs-base",
        runs_base.to_str().unwrap(),
        "--artifact-root",
        &format!("checkpoint={}", artifact_root.display()),
    ]);
    let val = Command::new(labctl_bin())
        .args(["validate", out.to_str().unwrap()])
        .output()
        .expect("invoke labctl validate");
    assert!(
        val.status.success(),
        "validate on init output failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&val.stdout),
        String::from_utf8_lossy(&val.stderr),
    );
}

#[test]
fn labctl_init_migrate_from_carries_schema() {
    // --migrate-from copies kinds/repos/env/slurm from the source
    // and adapts a path override. Output must validate.
    let (_keep, out) = run_init_in_tempdir(&[
        "--migrate-from",
        "examples/clusters/single-user.toml",
        "--name",
        "smoke-migrate",
        "--runs-base",
        "/tmp/smoke-migrate-runs",
    ]);
    let body = std::fs::read_to_string(&out).unwrap();
    assert!(body.contains("name = \"smoke-migrate\""));
    assert!(body.contains("runs_base = \"/tmp/smoke-migrate-runs\""));
    // Kinds from the source carried over.
    assert!(body.contains("[filesystem.artifact_roots]"));
}

#[test]
fn labctl_init_join_symlinks_source() {
    // --join writes a symlink at the destination pointing at the
    // shared source. Critical for the internal-rollout flow where
    // teammates share the same cluster.toml.
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("cluster.smoke.toml");
    let result = Command::new(labctl_bin())
        .args([
            "init",
            "--yes",
            "--no-detect",
            "--no-create-dirs",
            "--no-agent",
            "--no-doctor",
            "--join",
            "examples/clusters/single-user.toml",
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .expect("invoke labctl init --join");
    assert!(
        result.status.success(),
        "labctl init --join failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
    );
    let meta = std::fs::symlink_metadata(&out).expect("symlink_metadata");
    assert!(
        meta.file_type().is_symlink(),
        "expected {} to be a symlink, got {:?}",
        out.display(),
        meta.file_type(),
    );
}

#[test]
fn labctl_init_use_copy_config_creates_regular_file() {
    // --use --copy-config produces a regular file (not a symlink),
    // decoupling local config from later source edits.
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("cluster.smoke.toml");
    let result = Command::new(labctl_bin())
        .args([
            "init",
            "--yes",
            "--no-detect",
            "--no-create-dirs",
            "--no-agent",
            "--no-doctor",
            "--use",
            "examples/clusters/single-user.toml",
            "--copy-config",
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .expect("invoke labctl init --use --copy-config");
    assert!(result.status.success(), "labctl init --use --copy-config failed");
    let meta = std::fs::symlink_metadata(&out).expect("symlink_metadata");
    assert!(
        meta.file_type().is_file() && !meta.file_type().is_symlink(),
        "expected {} to be a regular file, got {:?}",
        out.display(),
        meta.file_type(),
    );
}
