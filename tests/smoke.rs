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

#[test]
fn labctl_init_no_detect_writes_loadable_config() {
    // Tests the `labctl init` end-to-end: --no-detect skips the
    // SLURM probes (none of which work in CI), --runs-base + an
    // --artifact-root supply the minimum the loader needs, and the
    // output round-trips through `labctl validate`.
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("cluster.smoke.toml");
    let runs_base = tmp.path().join("runs");
    let artifact_root = tmp.path().join("ck");

    let init = Command::new(labctl_bin())
        .args([
            "init",
            "--name",
            "smoke",
            "--no-detect",
            "--runs-base",
            runs_base.to_str().unwrap(),
            "--artifact-root",
            &format!("checkpoint={}", artifact_root.display()),
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .expect("failed to invoke labctl init");
    assert!(
        init.status.success(),
        "labctl init failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&init.stdout),
        String::from_utf8_lossy(&init.stderr),
    );
    assert!(out.exists(), "labctl init didn't write {}", out.display());

    let val = Command::new(labctl_bin())
        .args(["validate", out.to_str().unwrap()])
        .output()
        .expect("failed to invoke labctl validate");
    assert!(
        val.status.success(),
        "validate on init output failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&val.stdout),
        String::from_utf8_lossy(&val.stderr),
    );
}
