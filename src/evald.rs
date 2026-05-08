use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::{
    config::{ClusterConfig, EvalPolicy, Recipe},
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
    store: &mut Store,
    policy: &EvalPolicy,
) -> Result<EvaldReport> {
    let recipe = Recipe::load(&policy.recipe)?;
    let recipe_hash = util::sha256_bytes(&serde_json::to_vec(&recipe)?);
    let checkpoints = store.artifacts_by_kind(&policy.applies_to.kind)?;
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

        let mut overrides = SubmitOverrides::default();
        overrides
            .input_artifacts
            .insert("checkpoint".to_string(), checkpoint.id.clone());
        let submitted = runner::submit_recipe(cluster, store, &recipe, Some(overrides))?;
        match slot {
            EvalRequestSlot::Fresh => {
                store.insert_eval_request(
                    &eval_key,
                    &checkpoint.id,
                    &recipe_hash,
                    &policy.name,
                    &submitted.run_id,
                )?;
                report.submitted += 1;
            }
            EvalRequestSlot::Retry { previous_attempts } => {
                store.retry_eval_request(
                    &eval_key,
                    &submitted.run_id,
                    previous_attempts + 1,
                )?;
                report.retried += 1;
            }
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
