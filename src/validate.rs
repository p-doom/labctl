use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Result, bail};

use crate::config::{ClusterConfig, Recipe};

const TRAINER_SCRIPTS: &[&str] = &["scripts/train_vlm_sft.py", "scripts/train_text_sft.py"];

#[derive(Debug, PartialEq, Eq)]
enum Level {
    Note,
    Warn,
    Error,
}

#[derive(Debug)]
pub(crate) struct Finding {
    level: Level,
    message: String,
}

impl Finding {
    fn note(message: String) -> Self {
        Self {
            level: Level::Note,
            message,
        }
    }
    fn warn(message: String) -> Self {
        Self {
            level: Level::Warn,
            message,
        }
    }
    fn error(message: String) -> Self {
        Self {
            level: Level::Error,
            message,
        }
    }
}

pub(crate) fn report(findings: &[Finding], strict: bool) -> Result<()> {
    for f in findings {
        let tag = match f.level {
            Level::Note => "note",
            Level::Warn => "warn",
            Level::Error => "error",
        };
        println!("  {tag}: {}", f.message);
    }
    let errors = findings.iter().filter(|f| f.level == Level::Error).count();
    let warnings = findings.iter().filter(|f| f.level == Level::Warn).count();
    if errors > 0 {
        bail!("{errors} contract error(s)");
    }
    if strict && warnings > 0 {
        bail!("{warnings} contract warning(s) (--strict)");
    }
    Ok(())
}

pub(crate) fn repo_pins(repos: &BTreeMap<String, PathBuf>) -> Vec<Finding> {
    repos
        .iter()
        .filter_map(|(name, path)| repo_pin(name, path))
        .collect()
}

fn repo_pin(name: &str, path: &Path) -> Option<Finding> {
    if !path.exists() {
        return Some(Finding::error(format!(
            "repos[{name}]: pinned path does not exist: {}",
            path.display()
        )));
    }
    let out = match Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["status", "--porcelain"])
        .output()
    {
        Ok(out) => out,
        Err(e) => {
            return Some(Finding::warn(format!(
                "repos[{name}]: cannot run git on {}: {e}",
                path.display()
            )));
        }
    };
    if !out.status.success() {
        return Some(Finding::warn(format!(
            "repos[{name}]: not a git worktree ({}): {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr)
                .trim()
                .replace('\n', "; "),
        )));
    }
    let porcelain = String::from_utf8_lossy(&out.stdout);
    let (tracked, untracked) =
        porcelain
            .lines()
            .filter(|l| !l.trim().is_empty())
            .fold((0usize, 0usize), |(t, u), l| {
                if l.starts_with("??") {
                    (t, u + 1)
                } else {
                    (t + 1, u)
                }
            });
    if tracked == 0 && untracked == 0 {
        return None;
    }
    Some(Finding::warn(format!(
        "repos[{name}]: dirty worktree — {tracked} tracked change(s), {untracked} untracked file(s) \
         at {}; provenance would pin a HEAD that does not describe what runs",
        path.display(),
    )))
}

pub(crate) fn recipe_contracts(recipe: &Recipe, cluster_path: &Path) -> Vec<Finding> {
    let cluster = match ClusterConfig::load(cluster_path) {
        Ok(c) => c,
        Err(e) => {
            return vec![Finding::warn(format!(
                "repo + flag contracts not checked: cannot load cluster config {}: {e}",
                cluster_path.display(),
            ))];
        }
    };
    let Some(repo_path) = cluster.repos.get(&recipe.repo) else {
        return vec![Finding::error(format!(
            "repo {:?} has no [repos] entry in cluster {:?} ({}); known keys: {}",
            recipe.repo,
            cluster.name,
            cluster_path.display(),
            cluster
                .repos
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ))];
    };

    let mut findings: Vec<Finding> = repo_pin(&recipe.repo, repo_path).into_iter().collect();
    let command = recipe.command.join(" ");
    for script in TRAINER_SCRIPTS {
        if !command.contains(script) {
            continue;
        }
        findings.push(flag_contract(recipe, &command, repo_path, script));
    }
    findings
}

fn flag_contract(recipe: &Recipe, command: &str, repo_path: &Path, script: &str) -> Finding {
    let script_path = repo_path.join(script);
    let source = match fs::read_to_string(&script_path) {
        Ok(s) => s,
        Err(e) => {
            return Finding::warn(format!(
                "flag contract for {script} not checked: cannot read {}: {e}",
                script_path.display(),
            ));
        }
    };
    let required = match required_flags(&source) {
        Ok(required) => required,
        Err(reason) => {
            return Finding::warn(format!(
                "flag contract for {script} not checked: {reason} in {}",
                script_path.display(),
            ));
        }
    };
    let declared = declared_flags(recipe, command, script);
    let missing: Vec<&String> = required.iter().filter(|f| !declared.contains(*f)).collect();
    if missing.is_empty() {
        return Finding::note(format!(
            "flag contract for {script}: all {} required flag(s) present ({})",
            required.len(),
            script_path.display(),
        ));
    }
    Finding::error(format!(
        "flag contract for {script}: {} required flag(s) missing from [args]/command — {}; \
         declared in {} and enforced by _validate_flags() at startup",
        missing.len(),
        missing
            .iter()
            .map(|f| f.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        script_path.display(),
    ))
}

fn declared_flags(recipe: &Recipe, command: &str, script: &str) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = recipe.args.keys().cloned().collect();
    if let Some(sweep) = &recipe.sweep {
        out.insert(sweep.arg.clone());
    }
    let Some(pos) = command.find(script) else {
        return out;
    };
    let tail = &command[pos + script.len()..];
    let bytes = tail.as_bytes();
    let mut i = 0usize;
    while i + 2 < bytes.len() {
        let boundary = i == 0 || !is_flag_char(bytes[i - 1]);
        if bytes[i] == b'-' && bytes[i + 1] == b'-' && boundary {
            let mut j = i + 2;
            while j < bytes.len() && is_flag_char(bytes[j]) {
                j += 1;
            }
            if j > i + 2 {
                out.insert(tail[i + 2..j].to_string());
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

fn is_flag_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

fn required_flags(source: &str) -> std::result::Result<Vec<String>, String> {
    let mut lines = source.lines();
    let Some(mut literal) = lines.find_map(|line| {
        line.trim_start()
            .strip_prefix("_REQUIRED")?
            .trim_start()
            .strip_prefix('=')
            .map(str::trim_start)
            .map(str::to_owned)
    }) else {
        return Err("no _REQUIRED list found".into());
    };
    if !literal.starts_with('[') {
        return Err("_REQUIRED is not a list literal".into());
    }
    while !literal.contains(']') {
        let Some(line) = lines.next() else {
            return Err("_REQUIRED list is not closed".into());
        };
        literal.push('\n');
        literal.push_str(line);
    }
    literal.truncate(literal.find(']').unwrap() + 1);
    let parsed: toml::Value = toml::from_str(&format!("required = {literal}"))
        .map_err(|e| format!("cannot parse _REQUIRED list: {e}"))?;
    let required = parsed["required"]
        .as_array()
        .ok_or_else(|| "_REQUIRED is not a list literal".to_string())?;
    required
        .iter()
        .map(|value| {
            let flag = value
                .as_str()
                .ok_or_else(|| "_REQUIRED contains a non-string entry".to_string())?;
            if flag.is_empty() || !flag.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return Err("_REQUIRED contains an invalid flag name".into());
            }
            Ok(flag.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_list_ignoring_comments_and_prose() {
        let src = r#"
"""Docstring mentioning _REQUIRED = ["bogus"] in prose."""
_OPTIONAL = ["ignored"]
_REQUIRED = [
    "model_id",   # inline comment "not_a_flag"
    "seed",
    'keep_period',
]
"#;
        let got = required_flags(src).unwrap();
        assert_eq!(got, vec!["model_id", "seed", "keep_period"]);
    }

    #[test]
    fn parses_single_line_list() {
        let got = required_flags("_REQUIRED = [\"a\", \"b\"]\n").unwrap();
        assert_eq!(got, vec!["a", "b"]);
    }

    #[test]
    fn rejects_missing_or_malformed_required_list() {
        assert_eq!(
            required_flags("flags.DEFINE_string('model_id', None)\n"),
            Err("no _REQUIRED list found".into())
        );
        assert_eq!(
            required_flags("_REQUIRED = [\"model_id\", 7]\n"),
            Err("_REQUIRED contains a non-string entry".into())
        );
        assert!(
            required_flags("_REQUIRED = [\"model_id\" \"seed\"]\n")
                .unwrap_err()
                .starts_with("cannot parse _REQUIRED list:")
        );
    }

    #[test]
    fn declared_flags_reads_command_tail_args_and_sweep() {
        let recipe: Recipe = toml::from_str(
            r#"
name = "r"
repo = "omegalax"
command = ["bash", "-c", "python --before=1 scripts/train_vlm_sft.py --jax_cache_dir=$d --wandb_tags a b \"$@\"", "--"]
[args]
model_id = "x"
seed = "0"
[sweep]
arg = "shard"
start = 0
end = 3
"#,
        )
        .expect("recipe parses");
        let command = recipe.command.join(" ");
        let got = declared_flags(&recipe, &command, "scripts/train_vlm_sft.py");
        assert!(got.contains("jax_cache_dir"));
        assert!(got.contains("wandb_tags"));
        assert!(got.contains("model_id"));
        assert!(got.contains("shard"));
        assert!(
            !got.contains("before"),
            "flags before the script belong to the wrapper, not the trainer"
        );
    }
}
