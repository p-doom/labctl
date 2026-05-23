use std::{path::PathBuf, process::Command};

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
    for name in ["train", "eval", "sweep", "eval_from"] {
        validate(&format!("examples/recipes/{name}.toml"));
    }
}

#[test]
fn example_policy_parses() {
    validate("examples/policies/eval_per_checkpoint.toml");
}

#[test]
fn example_pipelines_parse() {
    validate("examples/pipelines/from-pinned.toml");
}

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
    assert!(
        out.exists() || out.is_symlink(),
        "init didn't create {}",
        out.display()
    );
    (tmp, out)
}

// init now bails on /path/to/... placeholders. The example cluster.toml
// fixtures intentionally use placeholders, so we synthesise a real-path
// source here for the use/migrate/join tests.
fn write_fixture_cluster(tmp: &tempfile::TempDir) -> PathBuf {
    let path = tmp.path().join("source-cluster.toml");
    let root = tmp.path().display();
    std::fs::write(
        &path,
        format!(
            r#"name = "fixture"

[filesystem]
runs_base = "{root}/runs"

[filesystem.artifact_roots]
dataset     = "{root}/datasets"
checkpoint  = "{root}/checkpoints"
eval_result = "{root}/eval_logs"

[scheduler]
kind = "slurm"
"#
        ),
    )
    .unwrap();
    path
}

#[test]
fn labctl_init_greenfield_writes_loadable_config() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let r = tmp.path().display();
    let (_keep, out) = run_init_in_tempdir(&[
        "--name",
        "smoke-greenfield",
        "--runs-base",
        &format!("{r}/runs"),
        "--artifact-root",
        &format!("checkpoint={r}/ck"),
        "--artifact-root",
        &format!("dataset={r}/ds"),
        "--artifact-root",
        &format!("eval_result={r}/er"),
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
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = write_fixture_cluster(&tmp);
    let (_keep, out) = run_init_in_tempdir(&[
        "--migrate-from",
        src.to_str().unwrap(),
        "--name",
        "smoke-migrate",
        "--runs-base",
        "/tmp/smoke-migrate-runs",
    ]);
    let body = std::fs::read_to_string(&out).unwrap();
    assert!(body.contains("name = \"smoke-migrate\""));
    assert!(body.contains("runs_base = \"/tmp/smoke-migrate-runs\""));
    assert!(body.contains("[filesystem.artifact_roots]"));
}

#[test]
fn labctl_init_join_symlinks_source() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = write_fixture_cluster(&tmp);
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
            src.to_str().unwrap(),
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
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = write_fixture_cluster(&tmp);
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
            src.to_str().unwrap(),
            "--copy-config",
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .expect("invoke labctl init --use --copy-config");
    assert!(
        result.status.success(),
        "labctl init --use --copy-config failed"
    );
    let meta = std::fs::symlink_metadata(&out).expect("symlink_metadata");
    assert!(
        meta.file_type().is_file() && !meta.file_type().is_symlink(),
        "expected {} to be a regular file, got {:?}",
        out.display(),
        meta.file_type(),
    );
}

#[test]
fn labctl_init_rejects_placeholder_paths() {
    // --yes greenfield with no --runs-base / --artifact-root flags
    // should bail rather than silently write a config full of
    // /path/to/... that the user has to fix later.
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
            "--name",
            "would-have-placeholders",
            "--output",
            out.to_str().unwrap(),
        ])
        .output()
        .expect("invoke labctl init");
    assert!(
        !result.status.success(),
        "labctl init should have failed on placeholders, but succeeded.\nstdout:\n{}",
        String::from_utf8_lossy(&result.stdout),
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("placeholders"),
        "expected stderr to mention placeholders, got:\n{stderr}",
    );
}
