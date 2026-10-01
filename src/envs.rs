//! RL environment browser: crowd-cast task packages read out of git.
//!
//! Every read goes through git objects at a resolved commit — `ls-tree`
//! for the layout, one `cat-file --batch` for contents — so the work tree
//! is never touched and a commit's view is immutable. That is what lets
//! the server cache one snapshot per sha, and what lets a run later link
//! to "the envs as this run saw them" by commit.
//!
//! A task is a directory under `[envs].tasks_dir` with a `task.json` at
//! its root (RL-Env-Contract 0.4). `scoring/` and `reference/` are the
//! grader side: hidden from the policy, and only returned here when the
//! caller asks for them. The package-level `provenance.json` (published
//! id → the id a task was built under) is never read.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;

/// Largest blob returned as text. Bigger files come back truncated.
pub const MAX_TEXT_BYTES: u64 = 1024 * 1024;

/// `git log` / `for-each-ref` field and record separators.
const FS: char = '\x1f';
const RS: char = '\x1e';
const LOG_FORMAT: &str = "--format=%H%x1f%h%x1f%cI%x1f%an%x1f%s%x1e";

#[derive(Debug, Clone, Serialize)]
pub struct Commit {
    pub sha: String,
    pub short: String,
    /// Committer date, ISO 8601.
    pub date: String,
    pub author: String,
    pub subject: String,
}

/// A commit in a task's history, and whether the task has a `task.json`
/// there. It doesn't before the repo adopted the contract layout, or
/// after the commit that deleted it; those can't be browsed.
#[derive(Debug, Clone, Serialize)]
pub struct HistoryCommit {
    #[serde(flatten)]
    pub commit: Commit,
    pub has_manifest: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RefInfo {
    /// `main`, `origin/main`, `v0.4` — what a user would type as `?rev=`.
    pub name: String,
    /// "branch" | "remote" | "tag"
    pub kind: &'static str,
    pub sha: String,
    pub date: String,
}

/// Which part of a task package a file belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Area {
    /// `task.json`, `prompt.md` and anything else at the package root.
    Package,
    /// What gets copied into the agent's runtime.
    Workspace,
    Scoring,
    Reference,
}

impl Area {
    fn of(rel: &str) -> Area {
        match rel.split('/').next() {
            Some("workspace") => Area::Workspace,
            Some("scoring") => Area::Scoring,
            Some("reference") => Area::Reference,
            _ => Area::Package,
        }
    }

    pub fn is_grader(self) -> bool {
        matches!(self, Area::Scoring | Area::Reference)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    /// Relative to the task directory, e.g. `workspace/README.md`.
    pub path: String,
    pub size: u64,
    pub area: Area,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub executable: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub symlink: bool,
    #[serde(skip)]
    oid: String,
}

/// One commit's view of every task package. Immutable, so the server
/// caches it by sha.
#[derive(Debug)]
pub struct Snapshot {
    pub commit: Commit,
    pub tasks: BTreeMap<String, TaskPackage>,
    /// Directories under `tasks_dir` with no `task.json` — e.g. commits
    /// from before the RL-Env-Contract layout. Reported so the UI can
    /// say why a revision shows fewer tasks than it has directories.
    pub skipped: Vec<String>,
}

#[derive(Debug)]
pub struct TaskPackage {
    pub key: String,
    /// Sorted by path; includes grader-side files.
    pub files: Vec<FileEntry>,
    /// Parsed `task.json`, or `Null` when it doesn't parse.
    pub task: Value,
    pub task_error: Option<String>,
    pub prompt: String,
}

#[derive(Debug, Serialize)]
pub struct TaskSummary {
    pub key: String,
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub difficulty: Option<String>,
    pub authors: Vec<String>,
    pub runtime: Option<String>,
    pub agent_timeout_secs: Option<u64>,
    pub workspace_files: usize,
    pub workspace_bytes: u64,
    /// Full prompt text; the list searches it client-side.
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TaskDetail {
    pub key: String,
    pub task: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_error: Option<String>,
    pub prompt: String,
    /// Agent-side files always; grader-side files only with `grader`.
    pub files: Vec<FileEntry>,
    /// Commits that touched this task, newest first, reachable from the
    /// requested revision.
    pub history: Vec<HistoryCommit>,
    pub grader: Option<GraderSide>,
}

/// The parts of `scoring/` and `reference/` with known shapes, decoded
/// for the task page. Everything else is reachable through the file list.
#[derive(Debug, Default, Serialize)]
pub struct GraderSide {
    pub rubrics: Option<Value>,
    pub findings: Option<Value>,
    pub checks: Option<Value>,
    pub prediction: Option<Value>,
    pub provenance: Option<Value>,
    pub notes: Option<String>,
    pub reproduce: Option<String>,
    /// The `SYSTEM` prompt from the package's `scaffold/judge.py` at the
    /// same commit — what every judge-scored criterion is asked under.
    pub judge_system_prompt: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FileContent {
    pub path: String,
    pub size: u64,
    pub area: Area,
    pub binary: bool,
    pub truncated: bool,
    /// UTF-8 text, or `None` for binary files.
    pub text: Option<String>,
}

#[derive(Debug)]
pub enum FileError {
    NotFound,
    /// A `scoring/` or `reference/` path requested without the grader side.
    GraderOnly,
    Git(anyhow::Error),
}

/// A git checkout, read only through plumbing commands.
pub struct Repo {
    path: PathBuf,
}

impl Repo {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Repo { path: path.into() }
    }

    fn command(&self) -> Command {
        let mut c = Command::new("git");
        c.arg("-C")
            .arg(&self.path)
            // Never block on a credential prompt, never take the index
            // lock: this process only reads.
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .stdin(Stdio::null());
        c
    }

    fn run(&self, args: &[&str]) -> Result<Vec<u8>> {
        let out = self
            .command()
            .args(args)
            .output()
            .with_context(|| format!("failed to run git in {}", self.path.display()))?;
        if !out.status.success() {
            bail!(
                "git {} failed in {}: {}",
                args.first().copied().unwrap_or(""),
                self.path.display(),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(out.stdout)
    }

    /// Resolve a user-supplied revision to a commit. `Ok(None)` when it
    /// names nothing in this repo.
    pub fn resolve(&self, rev: &str) -> Result<Option<Commit>> {
        validate_rev(rev)?;
        let spec = format!("{rev}^{{commit}}");
        let out = self
            .command()
            .args(["rev-parse", "--verify", "--quiet", &spec])
            .output()
            .with_context(|| format!("failed to run git in {}", self.path.display()))?;
        if !out.status.success() {
            // --quiet: exit 1 with empty stderr means "no such commit".
            // Anything on stderr is a real failure (not a repo, ownership).
            let err = String::from_utf8_lossy(&out.stderr);
            if err.trim().is_empty() {
                return Ok(None);
            }
            bail!(
                "git rev-parse failed in {}: {}",
                self.path.display(),
                err.trim()
            );
        }
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let mut commits =
            parse_log(&self.run(&["log", "-1", "--no-show-signature", LOG_FORMAT, &sha])?);
        Ok(commits.pop())
    }

    /// Commits that touched `path`, newest first, reachable from `sha`.
    pub fn log(&self, sha: &str, path: &str, limit: usize) -> Result<Vec<Commit>> {
        let n = format!("-n{limit}");
        Ok(parse_log(&self.run(&[
            "log",
            &n,
            "--no-show-signature",
            LOG_FORMAT,
            sha,
            "--",
            path,
        ])?))
    }

    /// Branches, remote branches and tags, most recent first.
    pub fn refs(&self, limit: usize) -> Result<Vec<RefInfo>> {
        let out = self.run(&[
            "for-each-ref",
            "--sort=-creatordate",
            "--format=%(refname)%1f%(objectname)%1f%(*objectname)%1f%(creatordate:iso-strict)%1e",
            "refs/heads",
            "refs/remotes",
            "refs/tags",
        ])?;
        let text = String::from_utf8_lossy(&out);
        let mut refs = Vec::new();
        for rec in text.split(RS) {
            let fields: Vec<&str> = rec.trim_start_matches('\n').split(FS).collect();
            let [full, obj, peeled, date] = fields[..] else {
                continue;
            };
            // refs/remotes/origin/HEAD is a pointer, not a branch.
            if full.ends_with("/HEAD") {
                continue;
            }
            let (kind, name) = if let Some(n) = full.strip_prefix("refs/heads/") {
                ("branch", n)
            } else if let Some(n) = full.strip_prefix("refs/remotes/") {
                ("remote", n)
            } else if let Some(n) = full.strip_prefix("refs/tags/") {
                ("tag", n)
            } else {
                continue;
            };
            // Annotated tags point at a tag object; the peeled id is the commit.
            let sha = if peeled.is_empty() { obj } else { peeled };
            refs.push(RefInfo {
                name: name.to_string(),
                kind,
                sha: sha.to_string(),
                date: date.to_string(),
            });
            if refs.len() >= limit {
                break;
            }
        }
        Ok(refs)
    }

    /// Every blob under `dir/` at `sha`, with sizes. Paths are repo-relative.
    fn ls_tree(&self, sha: &str, dir: &str) -> Result<Vec<TreeEntry>> {
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        let out = self.run(&[
            "ls-tree",
            "-r",
            "-z",
            "--long",
            "--full-tree",
            sha,
            "--",
            &prefix,
        ])?;
        let mut entries = Vec::new();
        for rec in out.split(|b| *b == 0) {
            if rec.is_empty() {
                continue;
            }
            // "<mode> <type> <oid> <padded size>\t<path>"
            let Some(tab) = rec.iter().position(|b| *b == b'\t') else {
                continue;
            };
            let meta = String::from_utf8_lossy(&rec[..tab]);
            let path = String::from_utf8_lossy(&rec[tab + 1..]).to_string();
            let mut parts = meta.split_whitespace();
            let (Some(mode), Some(kind), Some(oid), Some(size)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            // Submodules show up as `commit` entries with no content here.
            if kind != "blob" {
                continue;
            }
            entries.push(TreeEntry {
                mode: mode.to_string(),
                oid: oid.to_string(),
                size: size.parse().unwrap_or(0),
                path,
            });
        }
        Ok(entries)
    }

    /// Read many blobs in one `git cat-file --batch`. Results are in the
    /// order asked; a missing object is an error.
    fn read_blobs(&self, oids: &[&str]) -> Result<Vec<Vec<u8>>> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }
        let out = self.batch("--batch", oids)?;
        parse_batch(&out, oids.len())
    }

    /// Which of `specs` (`<rev>:<path>`) name an existing object, in one
    /// `git cat-file --batch-check`.
    fn exists(&self, specs: &[String]) -> Result<Vec<bool>> {
        if specs.is_empty() {
            return Ok(Vec::new());
        }
        let specs: Vec<&str> = specs.iter().map(String::as_str).collect();
        let out = self.batch("--batch-check", &specs)?;
        let text = String::from_utf8_lossy(&out);
        let found: Vec<bool> = text.lines().map(|l| !l.ends_with(" missing")).collect();
        if found.len() != specs.len() {
            bail!(
                "git cat-file --batch-check answered {} of {} requests",
                found.len(),
                specs.len()
            );
        }
        Ok(found)
    }

    /// Run `git cat-file <mode>` with one request per line on stdin.
    fn batch(&self, mode: &str, requests: &[&str]) -> Result<Vec<u8>> {
        let mut child = self
            .command()
            .args(["cat-file", mode])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to run git in {}", self.path.display()))?;
        let mut stdin = child.stdin.take().expect("piped stdin");
        let input: Vec<u8> = requests
            .iter()
            .flat_map(|o| format!("{o}\n").into_bytes())
            .collect();
        // Write from a thread: git starts answering before it has read
        // every request, and both pipes have finite buffers.
        let writer = std::thread::spawn(move || stdin.write_all(&input));
        let mut out = Vec::new();
        child
            .stdout
            .take()
            .expect("piped stdout")
            .read_to_end(&mut out)
            .context("reading git cat-file output")?;
        let status = child.wait().context("waiting for git cat-file")?;
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("git cat-file writer panicked"))?
            .context("writing to git cat-file")?;
        if !status.success() {
            bail!("git cat-file {mode} failed in {}", self.path.display());
        }
        Ok(out)
    }

    /// The first `max` bytes of a blob, without reading the rest.
    fn read_blob_prefix(&self, oid: &str, max: u64) -> Result<Vec<u8>> {
        let mut child = self
            .command()
            .args(["cat-file", "blob", oid])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to run git in {}", self.path.display()))?;
        let mut buf = Vec::new();
        child
            .stdout
            .take()
            .expect("piped stdout")
            .take(max)
            .read_to_end(&mut buf)
            .context("reading git cat-file blob")?;
        let _ = child.kill();
        let _ = child.wait();
        Ok(buf)
    }

    /// A single file at `sha:path`, if it exists.
    fn read_path(&self, sha: &str, path: &str) -> Option<Vec<u8>> {
        self.run(&["cat-file", "blob", &format!("{sha}:{path}")])
            .ok()
    }
}

#[derive(Debug)]
struct TreeEntry {
    mode: String,
    oid: String,
    size: u64,
    path: String,
}

/// Revisions come from the URL. Allow what people actually type (`main`,
/// `origin/main`, a sha, `HEAD~3`, `v1^`) and refuse anything that could
/// be read as an option, a range or a `rev:path` lookup.
pub fn validate_rev(rev: &str) -> Result<()> {
    let ok = !rev.is_empty()
        && rev.len() <= 200
        && !rev.starts_with('-')
        && !rev.contains("..")
        && rev
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./~^@{}".contains(c));
    if !ok {
        bail!("invalid revision {rev:?}");
    }
    Ok(())
}

fn parse_log(out: &[u8]) -> Vec<Commit> {
    let text = String::from_utf8_lossy(out);
    text.split(RS)
        .filter_map(|rec| {
            let fields: Vec<&str> = rec.trim_start_matches('\n').split(FS).collect();
            let [sha, short, date, author, subject] = fields[..] else {
                return None;
            };
            Some(Commit {
                sha: sha.to_string(),
                short: short.to_string(),
                date: date.to_string(),
                author: author.to_string(),
                subject: subject.to_string(),
            })
        })
        .collect()
}

/// Parse `cat-file --batch` output: per object a `<oid> <type> <size>`
/// header line, `size` bytes of content, then a newline.
fn parse_batch(out: &[u8], expected: usize) -> Result<Vec<Vec<u8>>> {
    let mut blobs = Vec::with_capacity(expected);
    let mut pos = 0;
    while blobs.len() < expected {
        let Some(nl) = out[pos..].iter().position(|b| *b == b'\n') else {
            bail!("git cat-file output ended early");
        };
        let header = String::from_utf8_lossy(&out[pos..pos + nl]).to_string();
        pos += nl + 1;
        let mut parts = header.split(' ');
        let (Some(oid), Some(kind)) = (parts.next(), parts.next()) else {
            bail!("unexpected git cat-file header {header:?}");
        };
        if kind == "missing" {
            bail!("git object {oid} is missing");
        }
        let size: usize = parts
            .next()
            .and_then(|s| s.parse().ok())
            .with_context(|| format!("unexpected git cat-file header {header:?}"))?;
        if pos + size > out.len() {
            bail!("git cat-file output truncated");
        }
        blobs.push(out[pos..pos + size].to_vec());
        pos += size + 1;
    }
    Ok(blobs)
}

/// Read every task package at `commit`.
pub fn load_snapshot(repo: &Repo, commit: Commit, tasks_dir: &str) -> Result<Snapshot> {
    let tasks_dir = tasks_dir.trim_end_matches('/');
    let prefix = format!("{tasks_dir}/");
    let mut grouped: BTreeMap<String, Vec<FileEntry>> = BTreeMap::new();
    for e in repo.ls_tree(&commit.sha, tasks_dir)? {
        let Some(rel) = e.path.strip_prefix(&prefix) else {
            continue;
        };
        // Files directly under tasks_dir aren't part of any task.
        let Some((key, path)) = rel.split_once('/') else {
            continue;
        };
        grouped.entry(key.to_string()).or_default().push(FileEntry {
            area: Area::of(path),
            executable: e.mode == "100755",
            symlink: e.mode == "120000",
            path: path.to_string(),
            size: e.size,
            oid: e.oid,
        });
    }
    // A directory is a task only if it has a manifest.
    let skipped: Vec<String> = grouped
        .iter()
        .filter(|(_, files)| !files.iter().any(|f| f.path == "task.json"))
        .map(|(key, _)| key.clone())
        .collect();
    grouped.retain(|key, _| !skipped.contains(key));

    // One batch for every manifest and prompt.
    let mut wanted: Vec<(String, &'static str, String)> = Vec::new();
    for (key, files) in &grouped {
        for name in ["task.json", "prompt.md"] {
            if let Some(f) = files.iter().find(|f| f.path == name) {
                wanted.push((key.clone(), name, f.oid.clone()));
            }
        }
    }
    let oids: Vec<&str> = wanted.iter().map(|(_, _, o)| o.as_str()).collect();
    let blobs = repo.read_blobs(&oids)?;
    let mut contents: BTreeMap<(String, &'static str), Vec<u8>> = BTreeMap::new();
    for ((key, name, _), blob) in wanted.into_iter().zip(blobs) {
        contents.insert((key, name), blob);
    }

    let mut tasks = BTreeMap::new();
    for (key, mut files) in grouped {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let raw = contents
            .remove(&(key.clone(), "task.json"))
            .unwrap_or_default();
        let (task, task_error) = match serde_json::from_slice::<Value>(&raw) {
            Ok(v) => (v, None),
            Err(e) => (Value::Null, Some(format!("task.json does not parse: {e}"))),
        };
        let prompt = contents
            .remove(&(key.clone(), "prompt.md"))
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        tasks.insert(
            key.clone(),
            TaskPackage {
                key,
                files,
                task,
                task_error,
                prompt,
            },
        );
    }
    Ok(Snapshot {
        commit,
        tasks,
        skipped,
    })
}

impl TaskPackage {
    pub fn summary(&self) -> TaskSummary {
        let meta = &self.task["metadata"];
        let workspace: Vec<&FileEntry> = self
            .files
            .iter()
            .filter(|f| f.area == Area::Workspace)
            .collect();
        TaskSummary {
            key: self.key.clone(),
            title: meta["name"].as_str().map(str::to_string),
            tags: strings(&meta["tags"]),
            difficulty: meta["difficulty"].as_str().map(str::to_string),
            authors: authors(&meta["authors"]),
            runtime: self.task["runtime"].as_str().map(str::to_string),
            agent_timeout_secs: self.task["timeout"]["agent"].as_u64(),
            workspace_files: workspace.len(),
            workspace_bytes: workspace.iter().map(|f| f.size).sum(),
            prompt: self.prompt.clone(),
            error: self.task_error.clone(),
        }
    }

    fn file(&self, path: &str) -> Option<&FileEntry> {
        self.files.iter().find(|f| f.path == path)
    }

    /// The task page. Grader-side files and decoded grader data only when
    /// `grader` is set; `history_limit` bounds the commit list.
    pub fn detail(
        &self,
        repo: &Repo,
        snapshot: &Snapshot,
        tasks_dir: &str,
        grader: bool,
        history_limit: usize,
    ) -> Result<TaskDetail> {
        let task_path = format!("{}/{}", tasks_dir.trim_end_matches('/'), self.key);
        let commits = repo.log(&snapshot.commit.sha, &task_path, history_limit)?;
        let specs: Vec<String> = commits
            .iter()
            .map(|c| format!("{}:{task_path}/task.json", c.sha))
            .collect();
        let history = commits
            .into_iter()
            .zip(repo.exists(&specs)?)
            .map(|(commit, has_manifest)| HistoryCommit {
                commit,
                has_manifest,
            })
            .collect();
        let files = self
            .files
            .iter()
            .filter(|f| grader || !f.area.is_grader())
            .cloned()
            .collect();
        let grader = if grader {
            Some(self.grader_side(repo, snapshot, tasks_dir)?)
        } else {
            None
        };
        Ok(TaskDetail {
            key: self.key.clone(),
            task: self.task.clone(),
            task_error: self.task_error.clone(),
            prompt: self.prompt.clone(),
            files,
            history,
            grader,
        })
    }

    fn grader_side(&self, repo: &Repo, snapshot: &Snapshot, tasks_dir: &str) -> Result<GraderSide> {
        const KNOWN: [&str; 7] = [
            "scoring/rubrics.json",
            "reference/FINDINGS.json",
            "reference/CHECKS.json",
            "reference/PREDICTION.json",
            "reference/provenance.json",
            "reference/NOTES.md",
            "reference/REPRODUCE.txt",
        ];
        let present: Vec<&FileEntry> = KNOWN.iter().filter_map(|p| self.file(p)).collect();
        let oids: Vec<&str> = present.iter().map(|f| f.oid.as_str()).collect();
        let mut by_path: BTreeMap<&str, Vec<u8>> = BTreeMap::new();
        for (f, blob) in present.iter().zip(repo.read_blobs(&oids)?) {
            by_path.insert(f.path.as_str(), blob);
        }
        let json = |p: &str| {
            by_path
                .get(p)
                .and_then(|b| serde_json::from_slice::<Value>(b).ok())
        };
        let text = |p: &str| {
            by_path
                .get(p)
                .map(|b| String::from_utf8_lossy(b).into_owned())
        };

        // The scaffold sits next to tasks/ in the package.
        let judge_path = Path::new(tasks_dir)
            .parent()
            .map(|p| p.join("scaffold/judge.py").to_string_lossy().into_owned());
        let judge_system_prompt = judge_path
            .and_then(|p| repo.read_path(&snapshot.commit.sha, &p))
            .and_then(|src| extract_judge_system(&String::from_utf8_lossy(&src)));

        Ok(GraderSide {
            rubrics: json("scoring/rubrics.json"),
            findings: json("reference/FINDINGS.json"),
            checks: json("reference/CHECKS.json"),
            prediction: json("reference/PREDICTION.json"),
            provenance: json("reference/provenance.json"),
            notes: text("reference/NOTES.md"),
            reproduce: text("reference/REPRODUCE.txt"),
            judge_system_prompt,
        })
    }

    /// One file of the package. Grader-side paths need `grader`.
    pub fn read_file(
        &self,
        repo: &Repo,
        path: &str,
        grader: bool,
    ) -> Result<FileContent, FileError> {
        let f = self.file(path).ok_or(FileError::NotFound)?;
        if f.area.is_grader() && !grader {
            return Err(FileError::GraderOnly);
        }
        let truncated = f.size > MAX_TEXT_BYTES;
        let bytes = if truncated {
            repo.read_blob_prefix(&f.oid, MAX_TEXT_BYTES)
        } else {
            repo.read_blobs(&[f.oid.as_str()])
                .map(|mut v| v.pop().unwrap_or_default())
        }
        .map_err(FileError::Git)?;
        let sniff = &bytes[..bytes.len().min(8192)];
        let text = if sniff.contains(&0) {
            None
        } else if truncated {
            // A cut can land inside a multi-byte character.
            Some(String::from_utf8_lossy(&bytes).into_owned())
        } else {
            String::from_utf8(bytes).ok()
        };
        Ok(FileContent {
            path: f.path.clone(),
            size: f.size,
            area: f.area,
            binary: text.is_none(),
            truncated,
            text,
        })
    }
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The contract's authors are `{ "name": ... }` objects; accept bare strings too.
fn authors(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    x["name"]
                        .as_str()
                        .or_else(|| x.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pull `SYSTEM = """\ ... """` out of judge.py without running it.
fn extract_judge_system(src: &str) -> Option<String> {
    let start = src.find("SYSTEM = \"\"\"")? + "SYSTEM = \"\"\"".len();
    let rest = &src[start..];
    let end = rest.find("\"\"\"")?;
    let body = &rest[..end];
    // A backslash right after the opening quotes, or before the closing
    // ones, is a line continuation, not part of the prompt.
    let body = body.strip_prefix("\\\n").unwrap_or(body);
    let body = body
        .strip_suffix("\\\n")
        .or_else(|| body.strip_suffix('\\'))
        .unwrap_or(body);
    Some(body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const TASKS: &str = "environments/pkg/pkg/tasks";

    fn git(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn write(root: &Path, rel: &str, body: &str) {
        let p = root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, body).unwrap();
    }

    fn commit(root: &Path, msg: &str) {
        git(root, &["add", "-A"]);
        git(root, &["commit", "-q", "-m", msg]);
    }

    /// A repo with two tasks, a directory without a manifest, and a
    /// package-level provenance.json that must never surface.
    fn fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git(root, &["init", "-q", "-b", "main"]);
        git(root, &["config", "user.email", "t@example.com"]);
        git(root, &["config", "user.name", "Tester"]);
        git(root, &["config", "commit.gpgsign", "false"]);
        write(
            root,
            "environments/pkg/pkg/provenance.json",
            r#"{"alpha": {"built_from": "someone-alpha"}}"#,
        );
        write(
            root,
            "environments/pkg/pkg/scaffold/judge.py",
            "SYSTEM = \"\"\"\\\nJudge strictly.\nReply with JSON.\\\n\"\"\"\n",
        );
        write(
            root,
            &format!("{TASKS}/alpha/task.json"),
            r#"{"key":"alpha","prompt":"prompt.md","runtime":"terminal-standard","timeout":{"agent":900},
                "metadata":{"name":"Fix alpha","tags":["python","debugging"],"difficulty":"hard","authors":[{"name":"A. Person"}]}}"#,
        );
        write(
            root,
            &format!("{TASKS}/alpha/prompt.md"),
            "Make alpha work.\n",
        );
        write(
            root,
            &format!("{TASKS}/alpha/workspace/README.md"),
            "# alpha\n",
        );
        write(
            root,
            &format!("{TASKS}/alpha/workspace/data.bin"),
            "\0\x01\x02",
        );
        write(
            root,
            &format!("{TASKS}/alpha/scoring/rubrics.json"),
            r#"{"schema_version":"0.4","rubrics":[]}"#,
        );
        write(
            root,
            &format!("{TASKS}/alpha/reference/NOTES.md"),
            "secret notes\n",
        );
        write(
            root,
            &format!("{TASKS}/alpha/reference/PREDICTION.json"),
            r#"{"expected_low":1,"expected_high":3,"why":"x"}"#,
        );
        write(root, &format!("{TASKS}/beta/task.json"), "not json");
        write(root, &format!("{TASKS}/beta/prompt.md"), "Beta.\n");
        write(
            root,
            &format!("{TASKS}/notatask/README.md"),
            "no manifest here\n",
        );
        commit(root, "add tasks");
        write(
            root,
            &format!("{TASKS}/alpha/prompt.md"),
            "Make alpha work, properly.\n",
        );
        commit(root, "reword alpha");
        dir
    }

    fn snapshot(repo: &Repo, rev: &str) -> Snapshot {
        let commit = repo.resolve(rev).unwrap().expect("rev resolves");
        load_snapshot(repo, commit, TASKS).unwrap()
    }

    #[test]
    fn rev_validation_refuses_options_ranges_and_paths() {
        for ok in [
            "main",
            "origin/main",
            "HEAD~3",
            "v1^",
            "deadbeef",
            "main@{1}",
        ] {
            assert!(validate_rev(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "-n1",
            "--all",
            "main..dev",
            "HEAD:secret",
            "a b",
            "$(x)",
        ] {
            assert!(validate_rev(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn unknown_rev_is_none_not_error() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        assert!(repo.resolve("no-such-branch").unwrap().is_none());
    }

    #[test]
    fn snapshot_finds_tasks_with_manifests_only() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main");
        assert_eq!(snap.tasks.keys().collect::<Vec<_>>(), ["alpha", "beta"]);
        assert_eq!(snap.skipped, ["notatask"]);
        assert_eq!(snap.commit.subject, "reword alpha");

        let alpha = snap.tasks["alpha"].summary();
        assert_eq!(alpha.title.as_deref(), Some("Fix alpha"));
        assert_eq!(alpha.tags, ["python", "debugging"]);
        assert_eq!(alpha.authors, ["A. Person"]);
        assert_eq!(alpha.agent_timeout_secs, Some(900));
        assert_eq!(alpha.workspace_files, 2);
        assert_eq!(alpha.prompt, "Make alpha work, properly.\n");

        let beta = snap.tasks["beta"].summary();
        assert!(
            beta.error.is_some(),
            "unparsable task.json is reported, not fatal"
        );
    }

    #[test]
    fn older_revision_shows_older_content() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main~1");
        assert_eq!(snap.tasks["alpha"].prompt, "Make alpha work.\n");
    }

    #[test]
    fn grader_side_is_opt_in() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main");
        let alpha = &snap.tasks["alpha"];

        let agent = alpha.detail(&repo, &snap, TASKS, false, 10).unwrap();
        assert!(agent.grader.is_none());
        assert!(agent.files.iter().all(|f| !f.area.is_grader()));
        assert!(matches!(
            alpha.read_file(&repo, "reference/NOTES.md", false),
            Err(FileError::GraderOnly)
        ));

        let full = alpha.detail(&repo, &snap, TASKS, true, 10).unwrap();
        let g = full.grader.unwrap();
        assert_eq!(g.notes.as_deref(), Some("secret notes\n"));
        assert_eq!(g.prediction.unwrap()["expected_high"], 3);
        assert_eq!(
            g.judge_system_prompt.as_deref(),
            Some("Judge strictly.\nReply with JSON.")
        );
        assert!(full.files.iter().any(|f| f.path == "scoring/rubrics.json"));
    }

    #[test]
    fn package_provenance_never_surfaces() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main");
        for t in snap.tasks.values() {
            let d = t.detail(&repo, &snap, TASKS, true, 10).unwrap();
            let body = serde_json::to_string(&d).unwrap();
            assert!(!body.contains("someone-alpha"), "{body}");
        }
    }

    #[test]
    fn history_lists_commits_touching_the_task() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main");
        let alpha = snap.tasks["alpha"]
            .detail(&repo, &snap, TASKS, false, 10)
            .unwrap();
        let subjects: Vec<&str> = alpha
            .history
            .iter()
            .map(|c| c.commit.subject.as_str())
            .collect();
        assert_eq!(subjects, ["reword alpha", "add tasks"]);
        assert!(alpha.history.iter().all(|c| c.has_manifest));
        let beta = snap.tasks["beta"]
            .detail(&repo, &snap, TASKS, false, 10)
            .unwrap();
        assert_eq!(beta.history.len(), 1);
    }

    #[test]
    fn history_marks_commits_before_the_manifest() {
        // A task that predates the contract layout, then adopts it.
        let dir = fixture();
        let root = dir.path();
        write(
            root,
            &format!("{TASKS}/notatask/task.json"),
            r#"{"key":"notatask","prompt":"prompt.md"}"#,
        );
        write(
            root,
            &format!("{TASKS}/notatask/prompt.md"),
            "Now a task.\n",
        );
        commit(root, "adopt contract");
        let repo = Repo::new(root);
        let snap = snapshot(&repo, "main");
        let d = snap.tasks["notatask"]
            .detail(&repo, &snap, TASKS, false, 10)
            .unwrap();
        let marks: Vec<(&str, bool)> = d
            .history
            .iter()
            .map(|c| (c.commit.subject.as_str(), c.has_manifest))
            .collect();
        assert_eq!(marks, [("adopt contract", true), ("add tasks", false)]);
    }

    #[test]
    fn files_read_as_text_or_binary() {
        let dir = fixture();
        let repo = Repo::new(dir.path());
        let snap = snapshot(&repo, "main");
        let alpha = &snap.tasks["alpha"];
        let readme = alpha
            .read_file(&repo, "workspace/README.md", false)
            .unwrap();
        assert_eq!(readme.text.as_deref(), Some("# alpha\n"));
        assert!(!readme.binary && !readme.truncated);
        let bin = alpha.read_file(&repo, "workspace/data.bin", false).unwrap();
        assert!(bin.binary && bin.text.is_none());
        assert!(matches!(
            alpha.read_file(&repo, "workspace/nope", true),
            Err(FileError::NotFound)
        ));
        assert!(matches!(
            alpha.read_file(&repo, "../beta/prompt.md", true),
            Err(FileError::NotFound)
        ));
    }

    #[test]
    fn large_files_are_truncated() {
        let dir = fixture();
        let root = dir.path();
        let big = "x".repeat(MAX_TEXT_BYTES as usize + 10);
        write(root, &format!("{TASKS}/alpha/workspace/big.txt"), &big);
        commit(root, "big file");
        let repo = Repo::new(root);
        let snap = snapshot(&repo, "main");
        let f = snap.tasks["alpha"]
            .read_file(&repo, "workspace/big.txt", false)
            .unwrap();
        assert!(f.truncated);
        assert_eq!(f.text.unwrap().len() as u64, MAX_TEXT_BYTES);
        assert_eq!(f.size, MAX_TEXT_BYTES + 10);
    }

    #[test]
    fn refs_skip_symbolic_head() {
        let dir = fixture();
        let root = dir.path();
        git(root, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
        git(
            root,
            &[
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/main",
            ],
        );
        git(root, &["tag", "-a", "v1", "-m", "v1"]);
        let refs = Repo::new(root).refs(50).unwrap();
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"main") && names.contains(&"origin/main") && names.contains(&"v1"));
        assert!(!names.iter().any(|n| n.ends_with("HEAD")));
        let head = Repo::new(root).resolve("main").unwrap().unwrap().sha;
        let tag = refs.iter().find(|r| r.name == "v1").unwrap();
        assert_eq!(tag.sha, head, "annotated tag is peeled to its commit");
    }
}
