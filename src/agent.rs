//! Per-user dispatch loops: reconcile + evald + throttle.
//!
//! Run inside the standalone `labctl agent` process (no HTTP listener)
//! — auto-installed as `labctl-agent.service` by `labctl init`. The UI
//! (`labctl serve`, optionally installed as `labctl-ui.service`) is
//! read-only and never runs these loops. Every loop operates
//! exclusively on its user's runs:
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
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::sync::Notify;

use crate::{
    config::{ClusterConfig, DispatchConfig, ThrottleConfig},
    evald, runner,
    store::Store,
    util,
};

/// Upper bound on the reconcile retry interval for a persistently
/// failing run.
///
/// The trade this encodes: a run that keeps failing to reconcile is, by
/// definition, not settling — so retrying it at the base interval buys
/// nothing and costs a log line plus a round-trip every tick, per run.
/// At 15 minutes a wedged run still gets 96 attempts a day (so it
/// self-heals promptly once the underlying cause is fixed, with no
/// operator action) while a fleet-wide fault produces ~1/15th the error
/// volume of an un-backed-off loop.
///
/// The cost is bounded and known: a run that fails once transiently and
/// would have settled on the next tick settles up to this much later
/// instead. Only reached after ~5 consecutive failures at a 60s base.
const RECONCILE_BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);

/// Per-run exponential backoff for reconcile failures.
///
/// Purely in-memory and intentionally so: it is a rate limiter, not
/// state. An agent restart clearing it is the desired behaviour —
/// a restart usually means new code, which deserves a fresh attempt at
/// every run. Nothing here is a schema change.
#[derive(Debug)]
pub(crate) struct ReconcileBackoff {
    base: Duration,
    cap: Duration,
    entries: HashMap<String, BackoffEntry>,
}

#[derive(Debug)]
struct BackoffEntry {
    consecutive_failures: u32,
    retry_after: Instant,
}

impl ReconcileBackoff {
    pub(crate) fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            entries: HashMap::new(),
        }
    }

    /// True if `run_id` failed recently enough that we should skip it
    /// this pass.
    pub(crate) fn is_deferred(&self, run_id: &str, now: Instant) -> bool {
        self.entries
            .get(run_id)
            .is_some_and(|e| now < e.retry_after)
    }

    /// Record a failed reconcile. Returns the delay now in force, for
    /// logging.
    pub(crate) fn record_failure(&mut self, run_id: &str, now: Instant) -> Duration {
        let entry = self
            .entries
            .entry(run_id.to_string())
            .or_insert(BackoffEntry {
                consecutive_failures: 0,
                retry_after: now,
            });
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        // base * 2^(n-1), saturating at `cap`. `checked_mul` on the u32
        // exponent keeps a long-lived wedged run from overflowing.
        let factor = 1u32
            .checked_shl(entry.consecutive_failures - 1)
            .unwrap_or(u32::MAX);
        let delay = self
            .base
            .checked_mul(factor)
            .unwrap_or(self.cap)
            .min(self.cap);
        entry.retry_after = now + delay;
        delay
    }

    /// Clear a run's backoff after a clean pass.
    pub(crate) fn record_success(&mut self, run_id: &str) {
        self.entries.remove(run_id);
    }

    /// Drop entries for runs that are no longer active, so the map
    /// tracks the active set rather than growing without bound.
    pub(crate) fn retain_active(&mut self, active: &HashSet<String>) {
        self.entries.retain(|id, _| active.contains(id));
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.entries.len()
    }
}

/// Spawn reconcile + evald + gc tokio tasks. Returns their join
/// handles so the caller can `.await` each on shutdown — without that
/// the process exits before a mid-flight iteration finishes (in-flight
/// sqlx queries are cancellation-safe at the DB level, but the agent
/// state machine doesn't get its "final pass" before systemd reaps
/// the process). With no `[dispatch]` block configured, returns an
/// empty `Vec`.
fn spawn(
    cluster: Arc<ClusterConfig>,
    store: Arc<Store>,
    shutdown: Arc<Notify>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(dispatch) = cluster.dispatch.clone() else {
        tracing::info!("labctl: no [dispatch] block in cluster config; reconcile + evald disabled");
        return Vec::new();
    };
    tracing::info!(
        "labctl: dispatch — reconcile every {}s, evald every {}s, policies={}",
        dispatch.reconcile_interval_secs,
        dispatch.evald_interval_secs,
        dispatch.policies_dir.display(),
    );
    if dispatch.gc.enabled {
        tracing::info!(
            "labctl: dispatch — gc every {}s (min_terminal_age={}s)",
            dispatch.gc.interval_secs,
            dispatch.gc.min_terminal_age_secs,
        );
    } else {
        tracing::info!("labctl: dispatch — gc disabled");
    }

    let cluster_r = cluster.clone();
    let store_r = store.clone();
    let shutdown_r = shutdown.clone();
    let dispatch_r = dispatch.clone();
    let reconcile_h = tokio::spawn(async move {
        reconcile_loop(cluster_r, store_r, dispatch_r, shutdown_r).await;
    });

    let cluster_g = cluster.clone();
    let store_g = store.clone();
    let shutdown_g = shutdown.clone();
    let dispatch_g = dispatch.clone();
    let gc_h = tokio::spawn(async move {
        gc_loop(cluster_g, store_g, dispatch_g, shutdown_g).await;
    });

    let cluster_e = cluster;
    let store_e = store;
    let shutdown_e = shutdown;
    let evald_h = tokio::spawn(async move {
        evald_loop(cluster_e, store_e, dispatch, shutdown_e).await;
    });

    vec![reconcile_h, gc_h, evald_h]
}

/// Standalone agent entrypoint: build a tokio runtime, spawn the
/// periodic refresh task and the dispatch loops, then block on SIGINT.
/// Used by the `labctl agent` subcommand. Auto-installed by
/// `labctl init` as a per-user systemd unit (`labctl-agent.service`).
/// Owns no HTTP listener; this process never accepts a network
/// connection. Pair with `labctl serve` (HTTP-only) running on the same
/// or another host for the UI.
pub async fn run_standalone(cluster: ClusterConfig) -> Result<()> {
    let store = Arc::new(Store::connect(&cluster).await?);
    let shutdown = Arc::new(Notify::new());
    let handles = spawn(Arc::new(cluster), store, shutdown.clone());
    tracing::info!("labctl agent running (no HTTP listener; ctrl-c to stop)");
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down — waiting for in-flight iterations to finish");
    shutdown.notify_waiters();
    // Drain: each loop's `select!` exits on shutdown but only after the
    // current iteration's body returns (sleep is the only race target).
    // Awaiting the handles guarantees we don't return to systemd before
    // every spawned task has observed the shutdown and exited cleanly.
    for h in handles {
        if let Err(e) = h.await {
            tracing::error!("labctl agent: task panicked during drain: {e:#}");
        }
    }
    tracing::info!("labctl agent: clean shutdown");
    Ok(())
}

async fn reconcile_loop(
    cluster: Arc<ClusterConfig>,
    store: Arc<Store>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    let interval = Duration::from_secs(dispatch.reconcile_interval_secs);
    // Per-run failure backoff, owned by the loop so it persists across
    // passes (and dies with the process — see `ReconcileBackoff`).
    let mut backoff = ReconcileBackoff::new(interval, RECONCILE_BACKOFF_CAP);
    // Run once immediately on boot so the registry isn't stale waiting
    // for the first tick.
    do_reconcile(&cluster, &store, &mut backoff).await;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_reconcile(&cluster, &store, &mut backoff).await;
            }
            _ = shutdown.notified() => {
                tracing::info!("labctl dispatch: reconcile_loop shutdown");
                return;
            }
        }
    }
}

async fn do_reconcile(cluster: &ClusterConfig, store: &Arc<Store>, backoff: &mut ReconcileBackoff) {
    // Scope to runs this daemon's OS user submitted: in a multi-tenant
    // deployment each user runs their own daemon over a shared
    // filesystem-truth registry, and a daemon that reconciles another
    // user's runs would race with that user's daemon on every status
    // write.
    let submitted_by = match crate::store::current_user() {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("labctl dispatch: cannot resolve current user: {e:#}");
            return;
        }
    };
    let runs = match store.list_active_runs(&submitted_by).await {
        Ok(rs) => rs,
        Err(e) => {
            tracing::error!("labctl dispatch: list_active_runs failed: {e:#}");
            return;
        }
    };
    // Forget runs that have left the active set; keeps the backoff map
    // sized to the active set rather than to all runs ever seen.
    backoff.retain_active(&runs.iter().map(|r| r.id.clone()).collect());

    let mut runs_reconciled = 0usize;
    let mut artifacts_registered = 0usize;
    let mut deferred = 0usize;
    for run in runs {
        let now = Instant::now();
        if backoff.is_deferred(&run.id, now) {
            deferred += 1;
            continue;
        }
        match runner::reconcile_one(cluster, store, &run).await {
            Ok(step) => {
                backoff.record_success(&run.id);
                if step.status_changed {
                    runs_reconciled += 1;
                }
                artifacts_registered += step.artifacts_registered;
            }
            Err(e) => {
                // `reconcile_one` isolates its own steps, so by the time
                // it returns Err every step has been attempted and any
                // that could succeed has. The error is a signal to slow
                // this run down, not a reason to abandon the pass.
                let delay = backoff.record_failure(&run.id, now);
                tracing::error!(
                    run_id = %run.id,
                    retry_in_secs = delay.as_secs(),
                    "labctl dispatch: reconcile_one failed for {}: {e:#}",
                    run.id
                );
            }
        }
    }
    if deferred > 0 {
        tracing::debug!("labctl dispatch: {deferred} run(s) deferred by reconcile backoff");
    }
    // Retroactive child-advance sweep — covers the gap where the agent
    // restarted between a parent's terminal transition and the in-pass
    // try_submit_pending_children call. Idempotent: already-advanced
    // children no longer appear in list_terminal_runs_with_pending_children.
    match store
        .list_terminal_runs_with_pending_children(&submitted_by)
        .await
    {
        Ok(parents) => {
            for parent in parents {
                if let Err(e) = runner::try_submit_pending_children(cluster, store, &parent).await {
                    tracing::error!(
                        "labctl dispatch: orphan sweep for {} failed: {e:#}",
                        parent.id
                    );
                }
            }
        }
        Err(e) => {
            tracing::error!(
                "labctl dispatch: list_terminal_runs_with_pending_children failed: {e:#}"
            );
        }
    }
    if runs_reconciled > 0 || artifacts_registered > 0 {
        tracing::info!(
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
    store: Arc<Store>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    if !dispatch.gc.enabled {
        // Don't even tick — pin the task to shutdown so it parks
        // cleanly when the daemon stops.
        shutdown.notified().await;
        tracing::info!("labctl dispatch: gc_loop shutdown (was disabled)");
        return;
    }
    let interval = Duration::from_secs(dispatch.gc.interval_secs);
    let min_age = dispatch.gc.min_terminal_age_secs;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_gc(&cluster, &store, min_age).await;
            }
            _ = shutdown.notified() => {
                tracing::info!("labctl dispatch: gc_loop shutdown");
                return;
            }
        }
    }
}

async fn do_gc(cluster: &ClusterConfig, store: &Arc<Store>, min_terminal_age_secs: u64) {
    match runner::gc_terminal_sources(store, min_terminal_age_secs).await {
        Ok(0) => {}
        Ok(n) => tracing::info!("labctl dispatch: gc reaped {n} source snapshot(s)"),
        Err(e) => tracing::error!("labctl dispatch: gc failed: {e:#}"),
    }
    // Reap orphan run-dirs: <runs_base>/runs/<user>/<id>/ with no PG row.
    // Cushion the age so we don't race against an in-flight CLI submit
    // whose insert_run hasn't committed yet; the terminal-source cutoff
    // is a sensible minimum (it's already the agent operator's "this
    // run is settled" threshold).
    let orphan_min_age = min_terminal_age_secs.max(3600);
    match runner::gc_orphan_run_dirs(cluster, store, orphan_min_age).await {
        Ok(0) => {}
        Ok(n) => tracing::warn!("labctl dispatch: gc reaped {n} orphan run-dir(s)"),
        Err(e) => tracing::error!("labctl dispatch: orphan-dir gc failed: {e:#}"),
    }
}

async fn evald_loop(
    cluster: Arc<ClusterConfig>,
    store: Arc<Store>,
    dispatch: DispatchConfig,
    shutdown: Arc<Notify>,
) {
    let interval = Duration::from_secs(dispatch.evald_interval_secs);
    // Don't run evald on boot — let reconcile go first so any newly-
    // landed checkpoints are registered before evald looks at them.
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                do_evald(&cluster, &store, &dispatch).await;
            }
            _ = shutdown.notified() => {
                tracing::info!("labctl dispatch: evald_loop shutdown");
                return;
            }
        }
    }
}

async fn do_evald(cluster: &ClusterConfig, store: &Arc<Store>, dispatch: &DispatchConfig) {
    let policies = match list_policies(&dispatch.policies_dir) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
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
                tracing::warn!(
                    "labctl dispatch: skipping policy {} ({e:#})",
                    policy_path.display()
                );
                continue;
            }
        };
        match evald::run_once(cluster, store, &policy).await {
            Ok(report) => {
                total_submitted += report.submitted;
            }
            Err(e) => {
                tracing::error!(
                    "labctl dispatch: evald failed for {}: {e:#}",
                    policy_path.display()
                );
            }
        }
    }
    if total_submitted > 0 {
        tracing::info!("labctl dispatch: evald submitted {total_submitted} eval run(s)");
    }
    if let Some(throttle) = &dispatch.throttle
        && let Err(e) = enforce_throttle(cluster, throttle).await
    {
        tracing::error!("labctl dispatch: throttle failed: {e:#}");
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

async fn enforce_throttle(_cluster: &ClusterConfig, throttle: &ThrottleConfig) -> Result<()> {
    let user = crate::store::current_user()?;
    let output = tokio::process::Command::new("squeue")
        .args([
            "-u",
            &user,
            "-h",
            "-o",
            "%i|%j|%T|%r",
            "--states=PENDING,RUNNING",
        ])
        .output()
        .await?;
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
        let result = tokio::process::Command::new(scontrol)
            .args(&arg)
            .output()
            .await;
        match result {
            Ok(o) if o.status.success() => {
                tracing::info!("labctl throttle: {verb} {job_id}");
            }
            Ok(o) => {
                tracing::error!(
                    "labctl throttle: {verb} {job_id} failed: {}",
                    String::from_utf8_lossy(&o.stderr).trim()
                );
            }
            Err(e) => {
                tracing::error!("labctl throttle: {verb} {job_id} failed: {e:#}");
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

#[cfg(test)]
mod backoff_tests {
    //! Pure unit tests for `ReconcileBackoff` — no DB, no clock sleeps.
    //! `Instant` is passed in so time can be advanced synthetically.

    use super::*;

    const BASE: Duration = Duration::from_secs(60);
    const CAP: Duration = Duration::from_secs(900);

    #[test]
    fn unknown_run_is_never_deferred() {
        let b = ReconcileBackoff::new(BASE, CAP);
        assert!(!b.is_deferred("run_x", Instant::now()));
    }

    /// The livelock's rate-limiting half: a run that keeps failing must
    /// not be retried every tick forever.
    #[test]
    fn failure_defers_the_next_attempt() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t0 = Instant::now();
        let delay = b.record_failure("run_x", t0);
        assert_eq!(delay, BASE, "first failure waits one base interval");
        // Still inside the window — the next tick must skip this run.
        assert!(b.is_deferred("run_x", t0 + Duration::from_secs(59)));
        // Window elapsed — eligible again.
        assert!(!b.is_deferred("run_x", t0 + Duration::from_secs(61)));
    }

    #[test]
    fn consecutive_failures_back_off_exponentially() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t = Instant::now();
        assert_eq!(b.record_failure("run_x", t), Duration::from_secs(60));
        assert_eq!(b.record_failure("run_x", t), Duration::from_secs(120));
        assert_eq!(b.record_failure("run_x", t), Duration::from_secs(240));
        assert_eq!(b.record_failure("run_x", t), Duration::from_secs(480));
    }

    /// A permanently-failing run must plateau, not grow without bound
    /// (and must not overflow the shift).
    #[test]
    fn backoff_saturates_at_the_cap() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t = Instant::now();
        for _ in 0..200 {
            let d = b.record_failure("run_x", t);
            assert!(d <= CAP, "delay {d:?} exceeded cap {CAP:?}");
        }
        assert_eq!(
            b.record_failure("run_x", t),
            CAP,
            "a wedged run settles at the cap, still retrying ~96x/day",
        );
    }

    /// Recovery must be immediate: one clean pass clears the penalty,
    /// so a fixed run isn't held back by its failure history.
    #[test]
    fn success_clears_the_backoff() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t = Instant::now();
        b.record_failure("run_x", t);
        b.record_failure("run_x", t);
        assert!(b.is_deferred("run_x", t + Duration::from_secs(1)));
        b.record_success("run_x");
        assert!(!b.is_deferred("run_x", t + Duration::from_secs(1)));
        // And the exponent resets, rather than resuming where it left off.
        assert_eq!(b.record_failure("run_x", t), BASE);
    }

    #[test]
    fn failures_are_tracked_per_run() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t = Instant::now();
        b.record_failure("run_a", t);
        assert!(b.is_deferred("run_a", t + Duration::from_secs(1)));
        assert!(
            !b.is_deferred("run_b", t + Duration::from_secs(1)),
            "one bad run must not throttle any other run",
        );
    }

    /// The map tracks the active set, so a long-lived agent doesn't
    /// accumulate an entry for every run it has ever seen.
    #[test]
    fn retain_active_prunes_departed_runs() {
        let mut b = ReconcileBackoff::new(BASE, CAP);
        let t = Instant::now();
        b.record_failure("run_a", t);
        b.record_failure("run_b", t);
        assert_eq!(b.tracked(), 2);
        let active: HashSet<String> = ["run_b".to_string()].into_iter().collect();
        b.retain_active(&active);
        assert_eq!(b.tracked(), 1);
        assert!(!b.is_deferred("run_a", t + Duration::from_secs(1)));
        assert!(b.is_deferred("run_b", t + Duration::from_secs(1)));
    }
}
