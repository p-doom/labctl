use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::{
    config::{ClusterConfig, EvalPolicy, Recipe},
    fs_layout,
    runner::{self, SubmitOverrides},
    store::{ArtifactRow, EvalRequestSlot, Store},
    util,
};

/// Maximum number of times the dispatcher will re-fire an eval request
/// for the same (checkpoint, policy) pair before giving up. After the
/// counter reaches this value the row stays terminally failed and is
/// skipped silently (with an info log) until manually cleared.
///
/// Three was chosen because the historical failure modes that justify
/// auto-retry — node hiccups, cold flashinfer JIT timeouts, sglang
/// startup races — almost always succeed on the second attempt. Three
/// gives one bonus retry while still capping deterministic-failure
/// storms (e.g. ``uv: command not found``) at a tiny number of wasted
/// SLURM accounting entries.
pub const MAX_EVAL_ATTEMPTS: i64 = 3;

#[derive(Debug, Clone, Serialize)]
pub struct EvaldReport {
    pub considered: usize,
    pub submitted: usize,
    pub retried: usize,
    pub skipped_existing: usize,
    pub skipped_ineligible: usize,
    pub skipped_exhausted: usize,
}

pub fn run_once(
    cluster: &ClusterConfig,
    store: &Store,
    policy: &EvalPolicy,
) -> Result<EvaldReport> {
    let recipe = Recipe::load(&policy.recipe)?;
    let recipe_hash = util::sha256_bytes(&serde_json::to_vec(&recipe)?);
    // Eval submissions are owned by the OS user the agent runs as. The
    // path-canonical layout requires a real `$USER`; a sentinel like
    // "evald" would not validate.
    let submitted_by = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .map_err(|_| anyhow::anyhow!("USER not set; cannot run evald"))?;
    fs_layout::validate_user(&submitted_by)?;
    // Scope candidates to checkpoints produced by this daemon's own
    // user. In a multi-tenant deployment (one daemon per user, shared
    // filesystem-truth registry) each evald must only dispatch evals
    // for checkpoints it owns — otherwise user A's daemon would submit
    // evals against user B's checkpoints, charging A's SLURM account
    // and double-dispatching against B's daemon's work.
    let checkpoints = store
        .artifacts_by_kind_for_producer_user(&policy.applies_to.kind, &submitted_by)?;
    let mut report = EvaldReport {
        considered: 0,
        submitted: 0,
        retried: 0,
        skipped_existing: 0,
        skipped_ineligible: 0,
        skipped_exhausted: 0,
    };

    for checkpoint in checkpoints {
        report.considered += 1;
        if !eligible(policy, &checkpoint) {
            report.skipped_ineligible += 1;
            continue;
        }

        let eval_key = util::sha256_bytes(
            format!("{}:{}:{}", checkpoint.id, recipe_hash, policy.name).as_bytes(),
        );
        let slot = store.eval_request_status(&eval_key, MAX_EVAL_ATTEMPTS)?;
        match slot {
            EvalRequestSlot::Active => {
                report.skipped_existing += 1;
                continue;
            }
            EvalRequestSlot::Exhausted { attempts } => {
                eprintln!(
                    "[evald] policy={} checkpoint={} retries exhausted ({}/{}), \
                     skipping. Clear the eval_request row to retry manually.",
                    policy.name, checkpoint.id, attempts, MAX_EVAL_ATTEMPTS,
                );
                report.skipped_exhausted += 1;
                continue;
            }
            EvalRequestSlot::Fresh | EvalRequestSlot::Retry { .. } => {}
        }

        // Capture the prior eval_run_id under the same snapshot so the
        // Retry-path UPDATE has its optimistic-concurrency witness. For
        // the Fresh path there is no prior id.
        let prior_run_id = match &slot {
            EvalRequestSlot::Retry { .. } => store.eval_request_run_id(&eval_key)?,
            _ => None,
        };

        let mut overrides = SubmitOverrides::default();
        overrides
            .input_artifacts
            .insert("checkpoint".to_string(), checkpoint.id.clone());
        let submitted = runner::submit_recipe(
            cluster,
            store,
            &recipe,
            Some(overrides),
            &submitted_by,
        )?;
        let claimed = match &slot {
            EvalRequestSlot::Fresh => store.claim_eval_slot_fresh(
                &eval_key,
                &checkpoint.id,
                &recipe_hash,
                &policy.name,
                &submitted.run_id,
            )?,
            EvalRequestSlot::Retry { previous_attempts } => {
                let prior = prior_run_id.as_deref().with_context(|| {
                    format!(
                        "[evald] internal: eval_key {eval_key} reported Retry \
                         but eval_request_run_id was NULL; \
                         eval_request_status / eval_request_run_id disagree"
                    )
                })?;
                store.claim_eval_slot_retry(
                    &eval_key,
                    prior,
                    *previous_attempts,
                    &submitted.run_id,
                )?
            }
            EvalRequestSlot::Active | EvalRequestSlot::Exhausted { .. } => unreachable!(),
        };
        if !claimed {
            // Atomic claim lost the race against another dispatcher.
            // The SLURM job we just submitted is now an orphan — log
            // loudly so the operator can scancel it. With one daemon
            // per user this is a developer-error path, not a normal
            // production occurrence.
            anyhow::bail!(
                "[evald] policy={} checkpoint={} lost the atomic eval-slot claim \
                 after submitting run {} — that SLURM job is now orphan, \
                 please scancel it. This means another writer raced us on \
                 eval_key={}; check for a stray second agent.",
                policy.name,
                checkpoint.id,
                submitted.run_id,
                eval_key,
            );
        }
        match slot {
            EvalRequestSlot::Fresh => report.submitted += 1,
            EvalRequestSlot::Retry { .. } => report.retried += 1,
            EvalRequestSlot::Active | EvalRequestSlot::Exhausted { .. } => unreachable!(),
        }
    }

    Ok(report)
}

fn eligible(policy: &EvalPolicy, artifact: &ArtifactRow) -> bool {
    if let Some(expected_recipe) = &policy.applies_to.producer_recipe {
        let producer = artifact
            .metadata_json
            .get("producer_recipe")
            .and_then(Value::as_str);
        if producer != Some(expected_recipe.as_str()) {
            return false;
        }
    }
    if let Some(every) = policy.cadence.every_n_steps {
        let step = artifact.metadata_json.get("step").and_then(Value::as_u64);
        return step.is_some_and(|step| step % every == 0);
    }
    true
}
