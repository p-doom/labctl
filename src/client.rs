//! Synchronous HTTP client for `labctl run` and `labctl run-pipeline`. The
//! daemon (`labctl serve`) is the only writer to the registry; this module
//! POSTs the parsed Recipe / LoadedPipeline to it and returns the daemon's
//! response.
//!
//! Uses ureq instead of reqwest+tokio because the CLI is synchronous and we
//! want a small dep footprint. ureq surfaces every non-2xx as
//! ``ureq::Error::Status(code, response)``; the client layer flattens that
//! into anyhow with the daemon's error body inlined so users see the actual
//! cause (e.g. "output marker already present"), not just an HTTP code.

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde_json::Value;

use crate::{
    config::{LoadedPipeline, Recipe, ServerConfig},
    runner::{SubmittedPipeline, SubmittedRun},
};

pub fn submit_recipe(server: &ServerConfig, recipe: &Recipe) -> Result<SubmittedRun> {
    let url = endpoint(&server.url, "/api/runs");
    let resp: Value = post_json(&url, server.timeout_secs, recipe)?;
    // Daemon returns a flat ``{run_id, job_id, run_dir}``; deserialize via
    // serde_json::from_value rather than constructing manually so the wire
    // type can grow fields without breaking the client.
    serde_json::from_value(resp).context("daemon returned an unexpected /api/runs response shape")
}

#[derive(Serialize)]
struct SubmitPipelineBody<'a> {
    pipeline: &'a LoadedPipeline,
    pipeline_path: Option<String>,
}

pub fn submit_pipeline(
    server: &ServerConfig,
    pipeline: &LoadedPipeline,
    pipeline_path: Option<&std::path::Path>,
) -> Result<SubmittedPipeline> {
    let url = endpoint(&server.url, "/api/pipelines");
    let body = SubmitPipelineBody {
        pipeline,
        pipeline_path: pipeline_path.map(|p| p.display().to_string()),
    };
    let resp: Value = post_json(&url, server.timeout_secs, &body)?;
    serde_json::from_value(resp)
        .context("daemon returned an unexpected /api/pipelines response shape")
}

fn endpoint(base: &str, path: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    format!("{trimmed}{path}")
}

fn post_json<T: Serialize>(url: &str, timeout_secs: u64, body: &T) -> Result<Value> {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(timeout_secs))
        .build();
    match agent.post(url).send_json(body) {
        Ok(resp) => resp
            .into_json::<Value>()
            .with_context(|| format!("daemon at {url} returned non-JSON")),
        Err(ureq::Error::Status(code, resp)) => {
            // Surface the daemon's own error message; the read path uses
            // ApiError which serializes ``{"error": "..."}``. Fall back to
            // the raw body if the shape is unexpected.
            let body = resp.into_string().unwrap_or_default();
            let inner = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or(body);
            bail!("daemon rejected request ({code}): {inner}")
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!(
            "cannot reach labctl daemon at {url}: {t}\n\
             Hint: start the daemon with `labctl service install` (or `labctl serve`),\n\
             or pass --local to write directly to the registry (single-user only)."
        )),
    }
}
