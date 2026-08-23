use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn labctl_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_labctl"))
}

struct Out {
    ok: bool,
    text: String,
}

fn validate(cluster: &Path, target: &Path, strict: bool) -> Out {
    let mut cmd = Command::new(labctl_bin());
    cmd.args(["--cluster", &cluster.to_string_lossy()]);
    cmd.args(["validate", &target.to_string_lossy()]);
    if strict {
        cmd.arg("--strict");
    }
    let out = cmd.output().expect("failed to invoke labctl");
    Out {
        ok: out.status.success(),
        text: format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ),
    }
}

fn run_git(repo: &Path, args: &[&str]) {
    let status = Command::new("git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(args)
        .current_dir(repo)
        .status()
        .expect("spawn git");
    assert!(
        status.success(),
        "git {args:?} failed in {}",
        repo.display()
    );
}

fn fake_trainer_repo(root: &Path) -> PathBuf {
    let repo = root.join("omegalax");
    std::fs::create_dir_all(repo.join("scripts")).unwrap();
    std::fs::write(
        repo.join("scripts/train_vlm_sft.py"),
        r#"from absl import flags

FLAGS = flags.FLAGS

_REQUIRED = [
    "model_id",
    "num_steps",
    "keep_period",
    "keep_latest",
    "log_memory",
    "resume",
    "num_loss_tiles",
]


def _validate_flags() -> None:
    problems = [n for n in _REQUIRED if FLAGS[n].value is None]
    if problems:
        raise SystemExit(problems)
"#,
    )
    .unwrap();
    run_git(&repo, &["init", "-q", "-b", "main"]);
    run_git(&repo, &["config", "user.email", "test@labctl.invalid"]);
    run_git(&repo, &["config", "user.name", "labctl-test"]);
    run_git(&repo, &["config", "commit.gpgsign", "false"]);
    run_git(&repo, &["add", "-A"]);
    run_git(&repo, &["commit", "-q", "-m", "initial"]);
    repo
}

fn write_cluster(root: &Path, repos: &str) -> PathBuf {
    let path = root.join("cluster.toml");
    let r = root.display();
    std::fs::write(
        &path,
        format!(
            r#"name = "fixture"

[filesystem]
runs_base = "{r}/runs"

[filesystem.artifact_roots]
checkpoint = "{r}/checkpoints"

[filesystem.output_roots]
checkpoint_stream = "{r}/checkpoints"

[repos]
{repos}
"#
        ),
    )
    .unwrap();
    path
}

fn write_recipe(root: &Path, name: &str, repo: &str, args: &str) -> PathBuf {
    write_recipe_for_script(root, name, repo, "scripts/train_vlm_sft.py", args)
}

fn write_recipe_for_script(
    root: &Path,
    name: &str,
    repo: &str,
    script: &str,
    args: &str,
) -> PathBuf {
    let path = root.join(format!("{name}.toml"));
    std::fs::write(
        &path,
        format!(
            r#"name = "{name}"
repo = "{repo}"
command = ["bash", "-c", '''
exec uv run -- srun python {script} \
    --resume=false \
    "$@"
''', "--"]

[outputs.checkpoint]
type   = "checkpoint_stream"
marker = "_CHECKPOINT_METADATA"
alias  = "{name}"

[args]
{args}
"#
        ),
    )
    .unwrap();
    path
}

const COMPLETE_ARGS: &str = r#"model_id = "Qwen/Qwen3-VL-2B-Instruct"
num_steps = "13500"
keep_period = "4500"
keep_latest = "2"
log_memory = "true"
num_loss_tiles = "4"
"#;

const INCIDENT_ARGS: &str = r#"model_id = "Qwen/Qwen3-VL-2B-Instruct"
num_steps = "13500"
"#;

#[test]
fn complete_recipe_passes_and_names_the_contract_it_checked() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe(tmp.path(), "good", "omegalax", COMPLETE_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(out.ok, "expected pass, got:\n{}", out.text);
    assert!(
        out.text.contains("all 7 required flag(s) present"),
        "pass must name the contract it checked:\n{}",
        out.text
    );
    assert!(!out.text.contains("resume"), "{}", out.text);
}

#[test]
fn missing_required_flags_fail_and_are_all_listed() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe(tmp.path(), "incident", "omegalax", INCIDENT_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(!out.ok, "expected failure, got:\n{}", out.text);
    for flag in ["keep_period", "keep_latest", "log_memory", "num_loss_tiles"] {
        assert!(
            out.text.contains(flag),
            "{flag} not reported:\n{}",
            out.text
        );
    }
    assert!(
        !out.text.contains("recipe \"incident\" OK"),
        "a failing contract must not also print the OK summary:\n{}",
        out.text
    );
}

#[test]
fn unreadable_script_warns_and_never_reads_as_a_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    std::fs::remove_file(repo.join("scripts/train_vlm_sft.py")).unwrap();
    run_git(&repo, &["commit", "-q", "-a", "-m", "drop trainer"]);
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe(tmp.path(), "unreadable", "omegalax", INCIDENT_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(
        out.text
            .contains("flag contract for scripts/train_vlm_sft.py not checked"),
        "degradation must name itself as not-checked:\n{}",
        out.text
    );
    assert!(
        !out.text.contains("required flag(s) present"),
        "a check that could not run must not claim a pass:\n{}",
        out.text
    );
    assert!(out.ok, "{}", out.text);
    assert!(
        !validate(&cluster, &recipe, true).ok,
        "--strict must promote the warning to a failure"
    );
}

#[test]
fn trainer_without_required_list_warns_and_fails_under_strict() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    std::fs::write(
        repo.join("scripts/train_text_sft.py"),
        "flags.DEFINE_string(\"model_id\", None, required=True)\n",
    )
    .unwrap();
    run_git(&repo, &["add", "scripts/train_text_sft.py"]);
    run_git(&repo, &["commit", "-q", "-m", "add text trainer"]);
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe_for_script(
        tmp.path(),
        "text",
        "omegalax",
        "scripts/train_text_sft.py",
        COMPLETE_ARGS,
    );

    let out = validate(&cluster, &recipe, false);
    assert!(out.ok, "{}", out.text);
    assert!(
        out.text.contains(
            "flag contract for scripts/train_text_sft.py not checked: no _REQUIRED list found"
        ),
        "{}",
        out.text
    );
    assert!(!validate(&cluster, &recipe, true).ok);
}

#[test]
fn unmapped_repo_key_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe(tmp.path(), "otherrepo", "not_in_cluster", COMPLETE_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(!out.ok, "expected failure, got:\n{}", out.text);
    assert!(
        out.text
            .contains("repo \"not_in_cluster\" has no [repos] entry"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("known keys: omegalax"),
        "the error must name what is available:\n{}",
        out.text
    );
}

#[test]
fn unloadable_cluster_warns_rather_than_passing_or_erroring() {
    let tmp = tempfile::tempdir().unwrap();
    let recipe = write_recipe(tmp.path(), "nocluster", "omegalax", COMPLETE_ARGS);
    let missing = tmp.path().join("no-such-cluster.toml");

    let out = validate(&missing, &recipe, false);
    assert!(
        out.text.contains("repo + flag contracts not checked"),
        "{}",
        out.text
    );
    assert!(
        !out.text.contains("required flag(s) present"),
        "an unrunnable check must not claim a pass:\n{}",
        out.text
    );
    assert!(out.ok, "{}", out.text);
    assert!(
        !validate(&missing, &recipe, true).ok,
        "--strict must promote the warning to a failure"
    );
}

#[test]
fn dirty_pin_warns_with_counts_and_fails_under_strict() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = fake_trainer_repo(tmp.path());
    std::fs::write(repo.join("scripts/train_vlm_sft.py.bak"), "junk\n").unwrap();
    std::fs::write(repo.join("dirty.txt"), "uncommitted\n").unwrap();
    std::fs::write(
        repo.join("scripts/train_vlm_sft.py"),
        std::fs::read_to_string(repo.join("scripts/train_vlm_sft.py")).unwrap() + "\n# edited\n",
    )
    .unwrap();
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", repo.display()));
    let recipe = write_recipe(tmp.path(), "dirtypin", "omegalax", COMPLETE_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(
        out.text
            .contains("dirty worktree — 1 tracked change(s), 2 untracked file(s)"),
        "dirty pin must report counts:\n{}",
        out.text
    );
    assert!(out.ok, "dirty is a warning by default:\n{}", out.text);
    assert!(
        !validate(&cluster, &recipe, true).ok,
        "--strict must promote the dirty pin to a failure"
    );
}

#[test]
fn non_git_pin_warns_rather_than_claiming_clean() {
    let tmp = tempfile::tempdir().unwrap();
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let cluster = write_cluster(tmp.path(), &format!("omegalax = \"{}\"", plain.display()));
    let recipe = write_recipe(tmp.path(), "nongit", "omegalax", COMPLETE_ARGS);

    let out = validate(&cluster, &recipe, false);
    assert!(out.text.contains("not a git worktree"), "{}", out.text);
    assert!(out.ok, "{}", out.text);
}

#[test]
fn cluster_validate_reports_every_bad_pin() {
    let tmp = tempfile::tempdir().unwrap();
    let clean = fake_trainer_repo(tmp.path());
    let dirty = fake_trainer_repo(&tmp.path().join("second"));
    std::fs::write(dirty.join("scratch.txt"), "x\n").unwrap();
    let cluster = write_cluster(
        tmp.path(),
        &format!(
            "a_clean = \"{}\"\nb_dirty = \"{}\"\nc_gone = \"{}/nope\"\n",
            clean.display(),
            dirty.display(),
            tmp.path().display(),
        ),
    );

    let out = validate(&cluster, &cluster, false);
    assert!(!out.ok, "a missing pin must fail:\n{}", out.text);
    assert!(
        out.text.contains("repos[b_dirty]: dirty worktree"),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("repos[c_gone]: pinned path does not exist"),
        "{}",
        out.text
    );
    assert!(
        !out.text.contains("repos[a_clean]"),
        "clean pins must stay silent:\n{}",
        out.text
    );
}
