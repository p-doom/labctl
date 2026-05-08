//! In-process dispatch loop.
//!
//! When `[dispatch]` is set in the cluster config, `labctl serve` spawns
//! these tokio tasks as a sidecar to the HTTP server:
//!
//! - **reconcile_loop** — every `reconcile_interval_secs`, walk active
//!   runs and call `runner::reconcile_one` per run. Acquires the store
//!   mutex for one run at a time so HTTP requests interleave between
//!   iterations.
//! - **evald_loop** — every `evald_interval_secs`, walk
//!   `policies_dir/*.toml`, call `evald::run_once` for each, then
//!   enforce the optional throttle.
//!
//! Each task wraps its body in error-tolerant logging — a transient
//! `sacct` flake doesn't kill the daemon. systemd's `Restart=on-failure`
//! is the safety net for genuine panics.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokio::sync::Notify;

use crate::{
    config::{ClusterConfig, DispatchConfig, ThrottleConfig},
    evald,
    runner,
    store::Store,
    util,
};

/// Spawn the dispatch tasks. Returns immediately; the tasks live on the
/// tokio runtime until `shutdown` fires.
pub fn spawn(
    cluster: Arc<ClusterConfig>,
    store: Arc<Mutex<Store>>,
    shutdown: Arc<Notify>,
) {
    let Some(dispatch) = cluster.dispatch.clone() else {
        return;
    };
    eprintln!(
        "labctl dispatch: reconcile every {}s, evald every {}s, policies={}",
        dispatch.reconcile_interval_secs,
        dispatch.evald_interval_secs,
        dispatch.policies_dir.display(),
    );
    let cluster_r = cluster.clone();
    let store_r = store.clone();
    let shutdown_r = shutdown.clone();
    let dispatch_r = dispatch.clone();
    tokio::spawn(async move {
        reconcile_loop(cluster_r, store_r, dispatch_r, shutdown_r).await;
    });

    let cluster_e = cluster;
    let store_e = store;
    let shutdown_e = shutdown;
    tokio::spawn(async move {
        evald_loop(cluster_e, store_e, dispatch, shutdown_e).await;
    });
}

async fn reconcile_loop(
    cluster: Arc<ClusterConfig>,
    store: Arc<Mutex<Store>>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    let interval = Duration::from_secs(dispatch.reconcile_interval_secs);
    // Run once immediately on boot so the registry isn't stale waiting
    // for the first tick.
    do_reconcile(&cluster, &store);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_reconcile(&cluster, &store);
            }
            _ = shutdown.notified() => {
                eprintln!("labctl dispatch: reconcile_loop shutdown");
                return;
            }
        }
    }
}

fn do_reconcile(cluster: &ClusterConfig, store: &Arc<Mutex<Store>>) {
    // Snapshot the active-run list under one short lock, then iterate
    // outside the lock — taking the mutex per run. UI requests can
    // interleave between runs instead of waiting for the whole pass.
    let runs = match {
        let s = store.lock().unwrap();
        s.list_active_runs()
    } {
        Ok(rs) => rs,
        Err(e) => {
            eprintln!("labctl dispatch: list_active_runs failed: {e:#}");
            return;
        }
    };
    let mut runs_reconciled = 0usize;
    let mut artifacts_registered = 0usize;
    for run in runs {
        let result = {
            let mut s = store.lock().unwrap();
            runner::reconcile_one(cluster, &mut s, &run)
        };
        match result {
            Ok(step) => {
                if step.status_changed {
                    runs_reconciled += 1;
                }
                artifacts_registered += step.artifacts_registered;
            }
            Err(e) => {
                eprintln!(
                    "labctl dispatch: reconcile_one failed for {}: {e:#}",
                    run.id
                );
            }
        }
    }
    if runs_reconciled > 0 || artifacts_registered > 0 {
        eprintln!(
            "labctl dispatch: reconciled {runs_reconciled} run(s), registered {artifacts_registered} artifact(s)"
        );
    }
}

async fn evald_loop(
    cluster: Arc<ClusterConfig>,
    store: Arc<Mutex<Store>>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    let interval = Duration::from_secs(dispatch.evald_interval_secs);
    // Don't run evald on boot — let reconcile go first so any newly-
    // landed checkpoints are registered before evald looks at them.
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_evald(&cluster, &store, &dispatch);
            }
            _ = shutdown.notified() => {
                eprintln!("labctl dispatch: evald_loop shutdown");
                return;
            }
        }
    }
}

fn do_evald(
    cluster: &ClusterConfig,
    store: &Arc<Mutex<Store>>,
    dispatch: &DispatchConfig,
) {
    let policies = match list_policies(&dispatch.policies_dir) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "labctl dispatch: failed to list policies in {}: {e:#}",
                dispatch.policies_dir.display()
            );
            return;
        }
    };
    let mut total_submitted = 0usize;
    for policy_path in policies {
        let policy = match crate::config::EvalPolicy::load(&policy_path) {
            Ok(p) => p,
            Err(e) => {
                eprintln!(
                    "labctl dispatch: skipping policy {} ({e:#})",
                    policy_path.display()
                );
                continue;
            }
        };
        let result = {
            let mut s = store.lock().unwrap();
            evald::run_once(cluster, &mut s, &policy)
        };
        match result {
            Ok(report) => {
                total_submitted += report.submitted;
            }
            Err(e) => {
                eprintln!(
                    "labctl dispatch: evald failed for {}: {e:#}",
                    policy_path.display()
                );
            }
        }
    }
    if total_submitted > 0 {
        eprintln!("labctl dispatch: evald submitted {total_submitted} eval run(s)");
    }
    if let Some(throttle) = &dispatch.throttle {
        if let Err(e) = enforce_throttle(cluster, throttle) {
            eprintln!("labctl dispatch: throttle failed: {e:#}");
        }
    }
}

fn list_policies(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// ---------- throttle ----------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqueueRow {
    pub job_id: String,
    pub job_name: String,
    pub state: String,
    pub reason: String,
}

/// What to do with a single SLURM job to enforce the cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottleAction {
    Hold(String),
    Release(String),
}

/// Pure decision logic, given a list of jobs filtered by name. Excess
/// pending jobs (beyond the cap minus running) get held; previously-held
/// jobs get released as running slots free up.
///
/// Held jobs: `state == "PENDING"` and `reason == "JobHeldUser"`.
pub fn throttle_decisions(rows: &[SqueueRow], max_concurrent: usize) -> Vec<ThrottleAction> {
    let running: Vec<&SqueueRow> = rows.iter().filter(|r| r.state == "RUNNING").collect();
    let pending_active: Vec<&SqueueRow> = rows
        .iter()
        .filter(|r| r.state == "PENDING" && r.reason != "JobHeldUser")
        .collect();
    let pending_held: Vec<&SqueueRow> = rows
        .iter()
        .filter(|r| r.state == "PENDING" && r.reason == "JobHeldUser")
        .collect();

    let mut actions = Vec::new();
    let used = running.len() + pending_active.len();
    if used > max_concurrent {
        // Hold the excess, oldest-first wins fewer slots — the script's
        // existing convention; squeue ordering follows that.
        let excess = used - max_concurrent;
        for row in pending_active.iter().take(excess) {
            actions.push(ThrottleAction::Hold(row.job_id.clone()));
        }
    } else {
        let free = max_concurrent.saturating_sub(used);
        for row in pending_held.iter().take(free) {
            actions.push(ThrottleAction::Release(row.job_id.clone()));
        }
    }
    actions
}

/// Parse the output of:
///   squeue -u <user> -h -o "%i|%j|%T|%r"
/// One row per line. Pipe-delimited because `%j` (job name) can contain
/// spaces.
pub fn parse_squeue_lines(out: &str) -> Vec<SqueueRow> {
    out.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.splitn(4, '|').collect();
            if parts.len() != 4 {
                return None;
            }
            Some(SqueueRow {
                job_id: parts[0].trim().to_string(),
                job_name: parts[1].trim().to_string(),
                state: parts[2].trim().to_string(),
                reason: parts[3].trim().to_string(),
            })
        })
        .collect()
}

fn enforce_throttle(_cluster: &ClusterConfig, throttle: &ThrottleConfig) -> Result<()> {
    let user = std::env::var("USER").unwrap_or_else(|_| "labctl".to_string());
    let output = std::process::Command::new("squeue")
        .args([
            "-u",
            &user,
            "-h",
            "-o",
            "%i|%j|%T|%r",
            "--states=PENDING,RUNNING",
        ])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "squeue failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let rows: Vec<SqueueRow> = parse_squeue_lines(&stdout)
        .into_iter()
        .filter(|r| r.job_name == throttle.job_name)
        .collect();
    let actions = throttle_decisions(&rows, throttle.max_concurrent);
    for action in actions {
        let (verb, job_id, scontrol_arg) = match &action {
            ThrottleAction::Hold(id) => ("hold", id, format!("hold={id}")),
            ThrottleAction::Release(id) => ("release", id, format!("release={id}")),
        };
        let _ = scontrol_arg; // suppress warning when scontrol path differs by cluster
        let scontrol = "scontrol";
        let arg = match &action {
            ThrottleAction::Hold(id) => vec!["hold", id.as_str()],
            ThrottleAction::Release(id) => vec!["release", id.as_str()],
        };
        let result = std::process::Command::new(scontrol).args(&arg).output();
        match result {
            Ok(o) if o.status.success() => {
                eprintln!("labctl throttle: {verb} {job_id}");
            }
            Ok(o) => {
                eprintln!(
                    "labctl throttle: {verb} {job_id} failed: {}",
                    String::from_utf8_lossy(&o.stderr).trim()
                );
            }
            Err(e) => {
                eprintln!("labctl throttle: {verb} {job_id} failed: {e:#}");
            }
        }
    }
    let _ = util::now_ts(); // silence unused-import false positive
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, state: &str, reason: &str) -> SqueueRow {
        SqueueRow {
            job_id: id.to_string(),
            job_name: "eval_x".to_string(),
            state: state.to_string(),
            reason: reason.to_string(),
        }
    }

    #[test]
    fn parses_pipe_delimited_squeue_output() {
        let out = "12345|eval_x|RUNNING|None\n12346|eval_x|PENDING|Resources\n12347|eval x with spaces|RUNNING|None\n";
        let rows = parse_squeue_lines(out);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].job_id, "12345");
        assert_eq!(rows[0].state, "RUNNING");
        assert_eq!(rows[2].job_name, "eval x with spaces");
    }

    #[test]
    fn skips_malformed_lines() {
        let out = "good|x|RUNNING|None\nthis is malformed\nalso|short|RUNNING\n";
        let rows = parse_squeue_lines(out);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].job_id, "good");
    }

    #[test]
    fn no_throttle_action_when_below_cap() {
        let rows = vec![
            row("1", "RUNNING", "None"),
            row("2", "PENDING", "Resources"),
        ];
        assert!(throttle_decisions(&rows, 16).is_empty());
    }

    #[test]
    fn holds_excess_pending_jobs_when_over_cap() {
        let rows = vec![
            row("1", "RUNNING", "None"),
            row("2", "RUNNING", "None"),
            row("3", "PENDING", "Resources"),
            row("4", "PENDING", "Resources"),
            row("5", "PENDING", "Resources"),
        ];
        // cap=3, running=2, pending_active=3 → used=5, excess=2
        let actions = throttle_decisions(&rows, 3);
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0], ThrottleAction::Hold("3".to_string()));
        assert_eq!(actions[1], ThrottleAction::Hold("4".to_string()));
    }

    #[test]
    fn holds_just_one_when_one_excess() {
        let rows = vec![
            row("1", "RUNNING", "None"),
            row("2", "RUNNING", "None"),
            row("3", "PENDING", "Resources"),
            row("4", "PENDING", "Resources"),
        ];
        // cap=3, running=2, pending_active=2 → used=4, excess=1
        let actions = throttle_decisions(&rows, 3);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0], ThrottleAction::Hold("3".to_string()));
    }

    #[test]
    fn releases_held_jobs_when_capacity_frees_up() {
        let rows = vec![
            row("1", "RUNNING", "None"),
            row("2", "PENDING", "JobHeldUser"),
            row("3", "PENDING", "JobHeldUser"),
        ];
        // cap=3, running=1, pending_active=0 → 2 free, release both
        let actions = throttle_decisions(&rows, 3);
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0], ThrottleAction::Release("2".to_string()));
        assert_eq!(actions[1], ThrottleAction::Release("3".to_string()));
    }

    #[test]
    fn does_not_count_held_jobs_against_cap() {
        // Held jobs are deliberately excluded from `pending_active` — they
        // shouldn't push us over the cap and trigger more holds.
        let rows = vec![
            row("1", "RUNNING", "None"),
            row("2", "RUNNING", "None"),
            row("3", "PENDING", "JobHeldUser"),
            row("4", "PENDING", "JobHeldUser"),
        ];
        // cap=2, used=2 → no holds; free=0, no releases.
        assert!(throttle_decisions(&rows, 2).is_empty());
    }

    #[test]
    fn release_count_is_capped_by_held_pool() {
        // free=10 but only 2 held — release exactly 2.
        let rows = vec![
            row("1", "PENDING", "JobHeldUser"),
            row("2", "PENDING", "JobHeldUser"),
        ];
        let actions = throttle_decisions(&rows, 16);
        assert_eq!(actions.len(), 2);
        for a in &actions {
            assert!(matches!(a, ThrottleAction::Release(_)));
        }
    }
}
