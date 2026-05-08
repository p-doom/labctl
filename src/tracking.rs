//! Backfill tracking rows for legacy runs whose recipe predates
//! `[tracking.wandb]`. Walks every run missing a `tracking` row, scans the
//! head of its log file for the URL `wandb.init` prints, and writes a `log`
//! tracking row.
//!
//! Idempotent — safe to re-run; only touches runs that don't already have a
//! tracking row, and `set_tracking` upserts.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Result;
use serde::Serialize;

use crate::{
    config::ClusterConfig,
    store::{RunRow, Store},
};

#[derive(Debug, Serialize)]
pub struct BackfillReport {
    pub scanned: usize,
    pub matched: usize,
    pub no_url: usize,
    pub no_log: usize,
}

pub fn backfill(_cluster: &ClusterConfig, store: &mut Store) -> Result<BackfillReport> {
    let runs = store.runs_missing_tracking()?;
    let mut report = BackfillReport {
        scanned: runs.len(),
        matched: 0,
        no_url: 0,
        no_log: 0,
    };
    for run in runs {
        match try_populate_from_log(store, &run)? {
            PopulateResult::Matched => report.matched += 1,
            PopulateResult::NoUrl => report.no_url += 1,
            PopulateResult::NoLog => report.no_log += 1,
            PopulateResult::AlreadyTracked => {}
        }
    }
    Ok(report)
}

/// Outcome of a single tracking-from-log attempt. Reported for visibility
/// in `backfill-tracking`; ignored by `reconcile`.
pub enum PopulateResult {
    Matched,
    NoUrl,
    NoLog,
    AlreadyTracked,
}

/// Per-run tracking population. Idempotent: returns `AlreadyTracked`
/// without doing any I/O if a row exists. Suitable to call on every run
/// from inside `reconcile` — the only cost when nothing changes is one
/// indexed SQL lookup per run.
pub fn try_populate_from_log(store: &mut Store, run: &RunRow) -> Result<PopulateResult> {
    if store.get_tracking(&run.id)?.is_some() {
        return Ok(PopulateResult::AlreadyTracked);
    }
    let Some((log_path, _)) = newest_log(&run.run_dir) else {
        return Ok(PopulateResult::NoLog);
    };
    let Some(url) = scan_log_for_wandb_url(&log_path) else {
        return Ok(PopulateResult::NoUrl);
    };
    let Some((entity, project)) = parse_wandb_url(&url) else {
        return Ok(PopulateResult::NoUrl);
    };
    store.set_tracking(&run.id, &entity, &project, &url, None, "log")?;
    Ok(PopulateResult::Matched)
}

fn newest_log(run_dir: &Path) -> Option<(PathBuf, SystemTime)> {
    let entries = std::fs::read_dir(run_dir.join(".lab")).ok()?;
    let mut best: Option<(SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("log") {
            continue;
        }
        let mtime = entry.metadata().and_then(|m| m.modified()).ok()?;
        match &best {
            Some((b, _)) if b >= &mtime => {}
            _ => best = Some((mtime, path)),
        }
    }
    best.map(|(m, p)| (p, m))
}

/// Scan the head of a log for the wandb.init URL banner. Caps the read at
/// 256KB since wandb prints it within the first few hundred bytes.
fn scan_log_for_wandb_url(log_path: &Path) -> Option<String> {
    const HEAD_BYTES: usize = 256 * 1024;
    let mut file = File::open(log_path).ok()?;
    let mut buf = vec![0u8; HEAD_BYTES];
    let n = file.read(&mut buf).ok()?;
    let head = std::str::from_utf8(&buf[..n]).ok()?;

    // Find the first occurrence of an actual run URL (must contain /runs/).
    // wandb.init also prints a project URL without /runs/, which we skip.
    let mut search = head;
    while let Some(start) = search.find("https://wandb.ai/") {
        let after = &search[start..];
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ')' | '<'))
            .unwrap_or(after.len());
        let url = after[..end].trim_end_matches(|c: char| matches!(c, '.' | ',' | ';' | ':' | ']'));
        if url.contains("/runs/") {
            return Some(url.to_string());
        }
        search = &after[end..];
    }
    None
}

/// Pull `(entity, project)` out of `https://wandb.ai/{entity}/{project}/runs/{id}`.
fn parse_wandb_url(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("https://wandb.ai/")?;
    let mut parts = rest.splitn(4, '/');
    let entity = parts.next()?.to_string();
    let project = parts.next()?.to_string();
    let runs_marker = parts.next()?;
    if runs_marker != "runs" {
        return None;
    }
    Some((entity, project))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_wandb_url() {
        let (e, p) = parse_wandb_url("https://wandb.ai/labctl-demo/lm_v3/runs/abc123").unwrap();
        assert_eq!(e, "labctl-demo");
        assert_eq!(p, "lm_v3");
    }

    #[test]
    fn rejects_project_url_without_runs_segment() {
        assert!(parse_wandb_url("https://wandb.ai/labctl-demo/lm_v3").is_none());
    }

    #[test]
    fn extracts_url_from_banner() {
        let log = "wandb: \u{1f680} View run at https://wandb.ai/foo/bar/runs/xyz789\nrest of log\n";
        // round-trip via a temp file
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), log).unwrap();
        let url = scan_log_for_wandb_url(tmp.path()).unwrap();
        assert_eq!(url, "https://wandb.ai/foo/bar/runs/xyz789");
    }

    #[test]
    fn ignores_project_only_url() {
        let log = "wandb: \u{2b50}\u{fe0f} View project at https://wandb.ai/foo/bar\n";
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), log).unwrap();
        assert!(scan_log_for_wandb_url(tmp.path()).is_none());
    }
}
