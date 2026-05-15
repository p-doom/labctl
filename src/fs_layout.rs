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
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------- subdirectory names under runs_base ----------

pub const RUNS_DIR: &str = "runs";
pub const ALIASES_DIR: &str = "aliases";
pub const EVAL_STATE_DIR: &str = "eval_state";
pub const PIPELINES_DIR: &str = "pipelines";
pub const EVENTS_DIR: &str = "events";

// ---------- file names inside a run's .lab/ ----------

pub const LAB_DIRNAME: &str = ".lab";
pub const RUN_JSON: &str = "run.json";
pub const INPUTS_JSON: &str = "inputs.json";
pub const OUTPUTS_JSON: &str = "outputs.json";
pub const TRACKING_JSON: &str = "tracking.json";
/// Existing — written at runtime by the sbatch wrapper. Source of truth
/// for `status` until reconcile observes terminal state via sacct.
pub const STATUS_JSON: &str = "status.json";
/// Existing — `register_outputs` reads this to find each output's
/// resolved path/marker/kind. Kept as part of the canonical layout.
pub const CONTEXT_JSON: &str = "context.json";
pub const SUBMIT_SH: &str = "submit.sh";

// ---------- sidecars elsewhere ----------

pub const ARTIFACT_META: &str = ".meta.json";
pub const ALIAS_TARGET: &str = ".target.json";
pub const EVAL_REQUEST_JSON: &str = "request.json";
pub const PIPELINE_JSON: &str = "pipeline.json";

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

pub fn run_lab_dir(runs_base: &Path, user: &str, run_id: &str) -> PathBuf {
    run_dir(runs_base, user, run_id).join(LAB_DIRNAME)
}

pub fn aliases_root(runs_base: &Path) -> PathBuf {
    runs_base.join(ALIASES_DIR)
}

pub fn alias_dir(runs_base: &Path, alias: &str) -> PathBuf {
    aliases_root(runs_base).join(alias)
}

pub fn alias_target(runs_base: &Path, alias: &str) -> PathBuf {
    alias_dir(runs_base, alias).join(ALIAS_TARGET)
}

pub fn eval_state_root(runs_base: &Path) -> PathBuf {
    runs_base.join(EVAL_STATE_DIR)
}

pub fn eval_request_dir(runs_base: &Path, user: &str, eval_key: &str) -> PathBuf {
    eval_state_root(runs_base).join(user).join(eval_key)
}

pub fn eval_request_path(runs_base: &Path, user: &str, eval_key: &str) -> PathBuf {
    eval_request_dir(runs_base, user, eval_key).join(EVAL_REQUEST_JSON)
}

pub fn pipelines_root(runs_base: &Path) -> PathBuf {
    runs_base.join(PIPELINES_DIR)
}

pub fn pipeline_dir(runs_base: &Path, user: &str, pipeline_id: &str) -> PathBuf {
    pipelines_root(runs_base).join(user).join(pipeline_id)
}

pub fn pipeline_path(runs_base: &Path, user: &str, pipeline_id: &str) -> PathBuf {
    pipeline_dir(runs_base, user, pipeline_id).join(PIPELINE_JSON)
}

pub fn events_root(runs_base: &Path) -> PathBuf {
    runs_base.join(EVENTS_DIR)
}

pub fn events_log_for(runs_base: &Path, ts: i64) -> PathBuf {
    use chrono::TimeZone;
    let dt = chrono::Utc.timestamp_opt(ts, 0).single().unwrap_or_default();
    events_root(runs_base).join(format!("{}.jsonl", dt.format("%Y%m%d")))
}

pub fn artifact_dir(artifact_root: &Path, user: &str, alias: &str) -> PathBuf {
    artifact_root.join(user).join(alias)
}

pub fn artifact_meta_path(artifact_root: &Path, user: &str, alias: &str) -> PathBuf {
    artifact_dir(artifact_root, user, alias).join(ARTIFACT_META)
}

// ---------- on-disk types ----------

/// Canonical run metadata sidecar. Mirrors `store::RunRow` with the
/// transient `status`/`finished_at`/`job_id` carved out: those are
/// rewritten on every state transition, so they live in a separate
/// `state.json` instead of being merged into this one. (We keep
/// `status.json` as the runtime sentinel written by the sbatch wrapper.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSidecar {
    pub id: String,
    pub recipe_name: String,
    pub recipe_hash: String,
    pub repo: String,
    pub run_dir: PathBuf,
    pub source_path: PathBuf,
    pub created_at: i64,
    pub submitted_by: String,
    pub recipe: Value,
    pub context: Value,
    /// Set when the run is part of a pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_on: Option<Value>,
    /// Most-recently-known status. Persisted here as a hint for cold
    /// reads; the live source is `status.json` (sbatch wrapper) and
    /// reconcile updates this field after a sacct observation.
    #[serde(default = "default_status")]
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
}

fn default_status() -> String {
    "created".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSidecar {
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    pub resolved_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputLink {
    pub role: String,
    pub artifact_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingSidecar {
    pub entity: String,
    pub project: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    /// "schema" (declared by recipe) or "log" (recovered by backfill).
    pub source: String,
    pub created_at: i64,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasTargetSidecar {
    pub artifact_id: String,
    pub artifact_path: PathBuf,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRequestSidecar {
    pub eval_key: String,
    pub checkpoint_artifact_id: String,
    pub eval_recipe_hash: String,
    pub policy_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_run_id: Option<String>,
    pub state: String,
    pub attempts: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSidecar {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_path: Option<PathBuf>,
    pub created_at: i64,
    pub user: String,
}

/// Wire shape of a single line in `events/<YYYYMMDD>.jsonl`. `id` is the
/// monotonic per-line ordinal within the file; the indexer assigns a
/// global id by scanning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLine {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub event_type: String,
    pub payload: Value,
    pub created_at: i64,
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

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("failed to parse {} as JSON", path.display()))
}

pub fn read_json_optional<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(
            serde_json::from_str(&text)
                .with_context(|| format!("failed to parse {} as JSON", path.display()))?,
        )),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Outcome of an atomic-claim attempt: did we win the race?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// We created the directory; we own this name.
    Claimed,
    /// Someone else got here first.
    AlreadyExists,
}

/// Atomically claim a namespace entry by creating its directory. Used for
/// alias names and eval_request keys: whoever wins the mkdir owns the
/// slot. Parent dirs are created with create_dir_all (those are not the
/// race-relevant step); only the leaf must be exclusive.
pub fn claim_dir(path: &Path) -> Result<ClaimOutcome> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    match fs::create_dir(path) {
        Ok(()) => Ok(ClaimOutcome::Claimed),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(ClaimOutcome::AlreadyExists),
        Err(e) => Err(e).with_context(|| format!("failed to create {}", path.display())),
    }
}

/// Append a JSONL line to today's events file. Open with O_APPEND so two
/// concurrent writers from different processes interleave safely on
/// POSIX-compliant filesystems (Lustre and GPFS both honor this for line-
/// sized writes well below PIPE_BUF).
pub fn append_event(runs_base: &Path, event: &EventLine) -> Result<()> {
    let path = events_log_for(runs_base, event.created_at);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut line = serde_json::to_vec(event)?;
    line.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open events log {}", path.display()))?;
    file.write_all(&line)
        .with_context(|| format!("failed to append event to {}", path.display()))?;
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
    if matches!(
        user,
        RUNS_DIR | ALIASES_DIR | EVAL_STATE_DIR | PIPELINES_DIR | EVENTS_DIR
    ) {
        bail!("user identifier collides with a reserved subdir name: {user:?}");
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
        let back: Value = read_json(&path).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn claim_dir_is_first_writer_wins() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("aliases/foo");
        assert_eq!(claim_dir(&target).unwrap(), ClaimOutcome::Claimed);
        assert_eq!(claim_dir(&target).unwrap(), ClaimOutcome::AlreadyExists);
    }

    #[test]
    fn append_event_creates_dated_file() {
        let dir = tempdir().unwrap();
        let ev = EventLine {
            run_id: Some("run_abc".into()),
            event_type: "run_created".into(),
            payload: serde_json::json!({}),
            created_at: 1_700_000_000,
        };
        append_event(dir.path(), &ev).unwrap();
        append_event(dir.path(), &ev).unwrap();
        let path = events_log_for(dir.path(), ev.created_at);
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 2);
    }

    #[test]
    fn validate_user_rejects_path_separators() {
        assert!(validate_user("alice/bob").is_err());
        assert!(validate_user("..").is_err());
        assert!(validate_user("runs").is_err());
        assert!(validate_user("alice").is_ok());
    }
}
