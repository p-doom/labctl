use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::util;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoProvenance {
    pub repo_path: PathBuf,
    pub git_head: Option<String>,
    pub git_status_porcelain: Option<String>,
    pub git_diff_head: Option<String>,
    pub uv_lock_hash: Option<String>,
    pub uv_lock_path: Option<PathBuf>,
}

pub fn capture_repo(repo_path: &Path, bundle_dir: &Path) -> Result<RepoProvenance> {
    fs::create_dir_all(bundle_dir)?;
    let git_head = git(repo_path, &["rev-parse", "HEAD"]).ok();
    let status = git(repo_path, &["status", "--porcelain"]).ok();
    let diff = git(repo_path, &["diff", "HEAD"]).ok();

    if let Some(head) = &git_head {
        fs::write(bundle_dir.join("git_head.txt"), head)?;
    }
    if let Some(status) = &status {
        fs::write(bundle_dir.join("git_status_porcelain.txt"), status)?;
    }
    if let Some(diff) = &diff {
        fs::write(bundle_dir.join("tracked.patch"), diff)?;
    }

    let uv_lock = find_up(repo_path, "uv.lock");
    let (uv_lock_hash, uv_lock_path) = if let Some(lock) = uv_lock {
        let dst = bundle_dir.join("uv.lock");
        fs::copy(&lock, &dst)
            .with_context(|| format!("failed to copy uv.lock from {}", lock.display()))?;
        (Some(util::sha256_file(&dst)?), Some(lock))
    } else {
        (None, None)
    };

    let prov = RepoProvenance {
        repo_path: repo_path.to_path_buf(),
        git_head,
        git_status_porcelain: status,
        git_diff_head: diff,
        uv_lock_hash,
        uv_lock_path,
    };
    fs::write(
        bundle_dir.join("provenance.json"),
        serde_json::to_vec_pretty(&json!(prov))?,
    )?;
    Ok(prov)
}

fn git(repo_path: &Path, args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(repo_path);
    util::run_capture(&mut cmd)
}

fn find_up(start: &Path, name: &str) -> Option<PathBuf> {
    let mut cur = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };
    loop {
        let candidate = cur.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !cur.pop() {
            return None;
        }
    }
}
