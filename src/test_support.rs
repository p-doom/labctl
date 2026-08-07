#![allow(dead_code)]

//! Test-only scaffolding for tests that need a real PostgreSQL.
//!
//! # Isolation contract
//!
//! Every helper here refuses to run unless `LABCTL_TEST_PG_HOST` is
//! set, and the tests that use them are `#[ignore]`d. A bare `cargo
//! test` therefore never opens a database connection at all, and can
//! never reach the shared production registry — which is deliberate:
//! the shared instance is live and the whole team depends on it.
//!
//! To run these, stand up a throwaway cluster (own data directory, own
//! port, unix socket only) and point the env at it:
//!
//! ```text
//! export LABCTL_TEST_PG_HOST=/tmp/labctl-throwaway/sock  # socket dir
//! export LABCTL_TEST_PG_PORT=55432
//! export LABCTL_TEST_PG_USER=labctl_test
//! export LABCTL_TEST_PG_DB=labctl_test
//! cargo test -- --ignored --test-threads=1
//! ```
//!
//! `PgStore::connect` runs the embedded migrations, so the target
//! database only needs to exist and be empty.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value, json};

use crate::config::{ClusterConfig, Recipe};
use crate::store::{NewRun, Store};

/// Root for this test process's scratch trees. Node-local, never
/// `/fast`.
pub(crate) fn scratch_root() -> PathBuf {
    std::env::temp_dir().join("labctl-test-scratch")
}

/// A test cluster pointed at the throwaway PG. Returns `None` when the
/// env isn't configured, which is how the `#[ignore]`d tests stay inert
/// if someone runs them without a throwaway instance.
pub(crate) fn test_cluster(tag: &str) -> Option<ClusterConfig> {
    let host = std::env::var("LABCTL_TEST_PG_HOST").ok()?;
    let port = std::env::var("LABCTL_TEST_PG_PORT").unwrap_or_else(|_| "55432".into());
    let user = std::env::var("LABCTL_TEST_PG_USER").unwrap_or_else(|_| "labctl_test".into());
    let db = std::env::var("LABCTL_TEST_PG_DB").unwrap_or_else(|_| "labctl_test".into());

    let base = scratch_root().join(tag);
    let runs_base = base.join("runs");
    let artifact_base = base.join("artifacts");
    std::fs::create_dir_all(&runs_base).expect("mkdir runs_base");
    std::fs::create_dir_all(&artifact_base).expect("mkdir artifact_base");

    // Round-trip through the real deserializer so the test exercises
    // the same defaulting (notably the new [postgres] timeout fields)
    // as a production cluster.toml.
    let toml_text = format!(
        r#"
name = "labctl-test"

[filesystem]
runs_base = "{runs}"

[filesystem.artifact_roots]
dataset = "{art}"
checkpoint = "{art}"
eval_result = "{art}"

[scheduler]
kind = "slurm"

[postgres]
host = "{host}"
port = {port}
database = "{db}"
user = "{user}"
"#,
        runs = runs_base.display(),
        art = artifact_base.display(),
    );
    Some(toml::from_str(&toml_text).expect("test cluster.toml parses"))
}

/// The `artifact_roots` root shared by all kinds in `test_cluster`.
pub(crate) fn artifact_root(cluster: &ClusterConfig) -> PathBuf {
    cluster.filesystem.artifact_roots["dataset"].clone()
}

/// `insert_artifact` requires the staging path to be
/// `<root>/<user>/<alias>/`. Create one and return it.
pub(crate) fn staging_dir(cluster: &ClusterConfig, user: &str, alias: &str) -> PathBuf {
    let p = artifact_root(cluster).join(user).join(alias);
    std::fs::create_dir_all(&p).expect("mkdir staging");
    p
}

/// A unique suffix so parallel/repeated runs never collide.
pub(crate) fn unique_suffix() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// A minimal valid recipe. `insert_run` needs one for `recipe_json`.
pub(crate) fn dummy_recipe() -> Recipe {
    Recipe {
        name: "test_recipe".to_string(),
        repo: "labctl".to_string(),
        command: vec!["true".to_string()],
        resources: Default::default(),
        inputs: Default::default(),
        outputs: Default::default(),
        params: Default::default(),
        args: Default::default(),
        env: Default::default(),
        tracking: Default::default(),
        sweep: None,
    }
}

/// 64 hex chars — satisfies `runs_recipe_hash_format`.
pub(crate) fn dummy_recipe_hash() -> String {
    (0..64)
        .map(|i| std::char::from_digit((i % 16) as u32, 16).unwrap())
        .collect()
}

/// Insert a user and a non-terminal run owned by them. Returns
/// `(user_name, run_id)`.
pub(crate) async fn seed_run(
    store: &Store,
    cluster: &ClusterConfig,
    context_json: &Value,
    status: &str,
    job_id: Option<&str>,
) -> Result<(String, String)> {
    let suffix = unique_suffix();
    let user = format!("__test_user_{suffix}");
    let run_id = format!("run_{suffix}");
    store.insert_user(&user, crate::util::now_ts()).await?;

    let run_dir = cluster.filesystem.runs_base.join(&run_id);
    std::fs::create_dir_all(run_dir.join("source"))?;
    store
        .insert_run(
            NewRun {
                id: &run_id,
                recipe: &dummy_recipe(),
                recipe_hash: &dummy_recipe_hash(),
                status: "created",
                run_dir: &run_dir,
                source_path: &run_dir.join("source"),
                context_json,
                submitted_by: Some(&user),
                cache_key: None,
            },
            &[],
        )
        .await?;
    if let Some(j) = job_id {
        store.set_submitted(&run_id, j).await?;
    }
    if status != "created" && status != "submitted" {
        store.update_status(&run_id, status, None).await?;
    }
    Ok((user, run_id))
}

/// A `context_json` carrying exactly one non-streaming output, which is
/// what `runner::register_outputs` reads.
pub(crate) fn context_with_output(
    role: &str,
    kind: &str,
    alias: &str,
    path: &Path,
    marker: &str,
) -> Value {
    json!({
        "schema_version": 1,
        "outputs": {
            role: {
                "role": role,
                "kind": kind,
                "alias": alias,
                "marker": marker,
                "path": path,
            }
        }
    })
}

/// Write a fake `sacct` that prints `lines` verbatim and exits 0, so
/// `runner::scheduler_outcome` can be driven deterministically without
/// a scheduler. Returns the script path.
pub(crate) fn fake_sacct(dir: &Path, lines: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir).expect("mkdir fake sacct dir");
    let script = dir.join("sacct");
    std::fs::write(&script, format!("#!/bin/sh\ncat <<'EOF'\n{lines}\nEOF\n"))
        .expect("write fake sacct");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake sacct");
    script
}

/// Format a unix timestamp the way sacct's `End` column does under
/// `TZ=UTC`.
pub(crate) fn sacct_end(ts: i64) -> String {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_opt(ts, 0)
        .single()
        .expect("valid ts")
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}
