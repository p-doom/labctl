//! Per-user dispatch loops: reconcile + evald + throttle, plus the
//! periodic filesystem→cache refresh task that keeps the in-memory
//! cache aligned with sidecars written by other processes.
//!
//! Spawned as tokio tasks either alongside the HTTP server inside
//! `server::serve` (all-in-one mode) or standalone from `labctl agent`
//! (per-user agent mode, no HTTP — pairs with one shared read-only
//! `labctl serve --no-dispatch`). Every loop operates exclusively on
//! its user's runs:
//!
//! - **reconcile_loop** — every `reconcile_interval_secs`, walks active
//!   runs and calls `runner::reconcile_one` per run. `sacct -j <jobid>`
//!   is user-agnostic, but writes only go to runs in this user's
//!   `runs/<user>/` subtree because that's the only place this `Store`
//!   has rows for.
//! - **evald_loop** — every `evald_interval_secs`, walks
//!   `policies_dir/*.toml` and submits eval recipes via the same all-CLI
//!   path the user's `labctl run` uses, so the eval job is owned by the
//!   running user in SLURM.
//! - **throttle** — `squeue -u $USER` is naturally per-user.
//!
//! Each loop body wraps in error-tolerant logging — a transient `sacct`
//! flake doesn't kill the daemon. systemd's `Restart=on-failure` is the
//! safety net for panics.

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

/// Periodically re-walk the registry from disk so the cache reflects
/// sidecars written by other processes (CLI submissions, other users'
/// daemons in a multi-tenant deployment).
///
/// The work is split into three phases so the Store's std::Mutex is
/// held only for microseconds at a time — never for the duration of
/// the slow filesystem walk:
///
///   1. **Snapshot** (brief lock): copy `runs_base` / `artifact_roots`
///      out of the Store, plus the live events table. ~ms.
///   2. **Build** (no lock): on a blocking thread, construct a fresh
///      in-memory SQLite cache from disk, then re-insert the
///      preserved events (with their original ids so the SSE tailer's
///      cursor stays valid). This is the 1-5s walk; concurrent HTTP
///      readers and dispatch writers proceed normally because no lock
///      is held during it.
///   3. **Swap** (brief lock): atomically replace the Store's cache
///      field with the new Connection. The previous Connection is
///      dropped at the end of this scope. ~microseconds.
///
/// Net effect: a /api/runs request is no longer occasionally stuck
/// behind a 1-5s refresh; reads consistently hit the cache directly.
pub async fn periodic_refresh(store: Arc<Mutex<Store>>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Drop the first tick — the indexer just ran during `Store::open`.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let store = store.clone();
        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // Phase 1 — brief lock.
            let (runs_base, artifact_roots, events) = {
                let s = store.lock().unwrap();
                let (rb, ar) = s.snapshot_paths();
                let ev = s.snapshot_events()?;
                (rb, ar, ev)
            };
            // Phase 2 — no lock; this is the 1-5s filesystem walk.
            let new_cache = Store::build_disk_snapshot(&runs_base, &artifact_roots, &events)?;
            // Phase 3 — brief lock for the swap.
            store.lock().unwrap().replace_cache(new_cache);
            Ok(())
        });
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("periodic_refresh: refresh failed: {e:#}"),
            Err(e) => eprintln!("periodic_refresh: join failed: {e}"),
        }
    }
}

/// Spawn reconcile + evald + gc tokio tasks. Returns immediately; the
/// tasks live until `shutdown` fires. With no `[dispatch]` block
/// configured, logs a notice and returns without spawning anything.
pub fn spawn(
    cluster: Arc<ClusterConfig>,
    store: Arc<Mutex<Store>>,
    shutdown: Arc<Notify>,
) {
    let Some(dispatch) = cluster.dispatch.clone() else {
        eprintln!(
            "labctl: no [dispatch] block in cluster config; reconcile + evald disabled"
        );
        return;
    };
    eprintln!(
        "labctl: dispatch — reconcile every {}s, evald every {}s, policies={}",
        dispatch.reconcile_interval_secs,
        dispatch.evald_interval_secs,
        dispatch.policies_dir.display(),
    );
    if dispatch.gc.enabled {
        eprintln!(
            "labctl: dispatch — gc every {}s (min_terminal_age={}s)",
            dispatch.gc.interval_secs, dispatch.gc.min_terminal_age_secs,
        );
    } else {
        eprintln!("labctl: dispatch — gc disabled");
    }

    let cluster_r = cluster.clone();
    let store_r = store.clone();
    let shutdown_r = shutdown.clone();
    let dispatch_r = dispatch.clone();
    tokio::spawn(async move {
        reconcile_loop(cluster_r, store_r, dispatch_r, shutdown_r).await;
    });

    let cluster_g = cluster.clone();
    let store_g = store.clone();
    let shutdown_g = shutdown.clone();
    let dispatch_g = dispatch.clone();
    tokio::spawn(async move {
        gc_loop(cluster_g, store_g, dispatch_g, shutdown_g).await;
    });

    let cluster_e = cluster;
    let store_e = store;
    let shutdown_e = shutdown;
    tokio::spawn(async move {
        evald_loop(cluster_e, store_e, dispatch, shutdown_e).await;
    });
}

/// Standalone agent entrypoint: build a tokio runtime, spawn the
/// periodic refresh task and the dispatch loops, then block on SIGINT.
/// Used by the `labctl agent` subcommand — paired with one shared
/// `labctl serve --no-dispatch` for the multi-tenant rollout model
/// described in `docs/ONBOARDING.md`. Owns no HTTP listener; this
/// process never accepts a network connection.
pub fn run_standalone(cluster: ClusterConfig, store: Store) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build tokio runtime: {e}"))?;
    runtime.block_on(async move {
        let store = Arc::new(Mutex::new(store));
        let shutdown = Arc::new(Notify::new());
        tokio::spawn(periodic_refresh(store.clone(), Duration::from_secs(10)));
        spawn(Arc::new(cluster), store.clone(), shutdown.clone());
        eprintln!("labctl agent running (no HTTP listener; ctrl-c to stop)");
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("\nshutting down");
        shutdown.notify_waiters();
    });
    Ok(())
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
    // Scope to runs this daemon's OS user submitted: in a multi-tenant
    // deployment each user runs their own daemon over a shared
    // filesystem-truth registry, and a daemon that reconciles another
    // user's runs would race with that user's daemon on every status
    // write.
    let submitted_by = match crate::store::current_user() {
        Ok(u) => u,
        Err(e) => {
            eprintln!("labctl dispatch: cannot resolve current user: {e:#}");
            return;
        }
    };
    let runs = match {
        let s = store.lock().unwrap();
        s.list_active_runs(&submitted_by)
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

/// Reap `<run_dir>/source/<repo>/` for terminal runs that have been
/// settled for at least `dispatch.gc.min_terminal_age_secs`. Skipped
/// entirely when the agent is configured with `[dispatch.gc] enabled =
/// false`. The provenance bundle under `.lab/provenance/<repo>/` is
/// independent and never touched here — losing source/ doesn't lose
/// reproducibility, just the convenience of a pre-built working tree.
async fn gc_loop(
    cluster: Arc<ClusterConfig>,
    store: Arc<Mutex<Store>>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    if !dispatch.gc.enabled {
        // Don't even tick — pin the task to shutdown so it parks
        // cleanly when the daemon stops.
        shutdown.notified().await;
        eprintln!("labctl dispatch: gc_loop shutdown (was disabled)");
        return;
    }
    let interval = Duration::from_secs(dispatch.gc.interval_secs);
    let min_age = dispatch.gc.min_terminal_age_secs;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_gc(&cluster, &store, min_age);
            }
            _ = shutdown.notified() => {
                eprintln!("labctl dispatch: gc_loop shutdown");
                return;
            }
        }
    }
}

fn do_gc(_cluster: &ClusterConfig, store: &Arc<Mutex<Store>>, min_terminal_age_secs: u64) {
    let removed = {
        let mut s = store.lock().unwrap();
        runner::gc_terminal_sources(&mut s, min_terminal_age_secs)
    };
    match removed {
        Ok(0) => {}
        Ok(n) => eprintln!("labctl dispatch: gc reaped {n} source snapshot(s)"),
        Err(e) => eprintln!("labctl dispatch: gc failed: {e:#}"),
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
