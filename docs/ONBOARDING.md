# Onboarding

A 10-minute walkthrough that gets you from `git clone` to a running web
UI displaying a real recipe submission. By the end you'll be able to
point labctl at a real cluster.

## Mental model first

The filesystem is the registry. Your CLI writes JSON sidecars under
your own uid; SLURM jobs are owned by you; the read-only UI server is
just an HTTP window onto an in-memory SQLite cache that's rebuilt from
the tree on startup. There is no privileged daemon and no shared
service account.

```
runs_base/
  runs/<user>/<run_id>/.lab/         per-run sidecars + source snapshot
  aliases/<name>/.target.json        global alias namespace
  eval_state/<user>/<key>/           per-user eval dedup
  pipelines/<user>/<id>/
  events/<YYYYMMDD>.jsonl            append-only event log

<artifact_root>/<kind>/<user>/<alias>/   the artifact + .meta.json sidecar
```

## 0. Install

```bash
git clone <this-repo> labctl && cd labctl
./scripts/install.sh
```

`scripts/install.sh` does three things: builds the embedded
frontend (`ui/dist/`), `cargo install --path . --features ui` so
`labctl` ends up in `~/.cargo/bin/` (on PATH for any normal Rust
setup), and points `git config core.hooksPath` at `scripts/hooks/`
so `cargo test --all-features` runs before every `git push`. Bypass
with `git push --no-verify` if you're pushing a deliberate WIP branch.

Re-run `./scripts/install.sh` after `git pull` to refresh the
installed binary.

## 1. Sanity check with the demo seed

The demo seed writes a self-contained filesystem-truth registry to
`/tmp/labctl-demo/` so you can poke at the UI without touching real
data:

```bash
python3 scripts/seed-demo.py
labctl --cluster /tmp/labctl-demo/labctl.toml serve
```

Open `http://127.0.0.1:8765`. You should see four fake runs, an
artifact + alias, and the runs/artifacts views populated. `labctl
--cluster /tmp/labctl-demo/labctl.toml doctor` against the seed
returns "all checks passed" — a useful end-to-end smoke check before
you point labctl at a real cluster.

## 2. Set up your cluster

`labctl init` is a full setup wizard, not just a config writer. It
picks one of four modes — interactively or via flag — and does
everything needed to leave you with a working setup: writes (or
adopts) a cluster.toml, pre-creates your per-user subdirs, installs
the systemd agent unit, and runs doctor.

```bash
labctl init
```

This drops you into an interactive walkthrough that probes local
SLURM, asks for paths and repos with smart defaults, and at the end
runs `labctl doctor` for you. Pass `--yes` (or pipe stdin from a
script) to accept defaults non-interactively.

### Pick the right mode

| Situation | Mode | What it does |
| --- | --- | --- |
| Brand-new cluster, no template | `labctl init` (default) | SLURM probe → fresh cluster.toml → per-user dirs → agent → doctor. |
| You already wrote a cluster.toml | `labctl init --use ~/cluster.toml` | Symlinks your file to the default location → dirs → agent → doctor. No edits to your source. |
| Standing labctl up at a new site, had it at another | `labctl init --migrate-from /path/to/cluster.berlin.toml` | Carries schema (kinds, repos, dispatch, env, throttle) from the foreign config; reviews site-local paths interactively. |
| Joining a colleague's shared registry on this cluster | `labctl init --join /shared/cluster.berlin.toml` | Symlinks the team config in. **Paths kept verbatim** — your runs land in the same registry as your colleague's. Per-user agent + per-user subdirs only. |

The default destination is `~/.config/labctl/cluster.toml`, which is
also the default for the `--cluster` global flag, so plain `labctl
<cmd>` works after setup. Override with `--output <path>` at init
time or `--cluster <path>` (or `$LABCTL_CLUSTER`) per command.

### What the wizard does in greenfield mode

1. **SLURM probe** — `sinfo`, `sacctmgr`, and `scontrol show config`
   suggest partition / QoS / GresTypes. Best-effort; missing binary
   degrades to a note, not an error.
2. **Interactive review** — name, runs_base, each artifact_root,
   repos, slurm fields. Defaults are pre-filled from the probe.
3. **Pre-create per-user subdirs** — `mkdir -p
   runs_base/runs/$USER`, one per artifact_root for `$USER`. Failed
   mkdirs (permissions) get surfaced but don't abort.
4. **Install per-user agent** — `~/.config/systemd/user/labctl-agent.service`
   runs `labctl agent` as you (reconcile + evald + throttle).
   Enables linger if you want it to survive logout.
5. **Run doctor** — full self-check against the new config.

### Skipping steps

Each step has an off switch for scripted / cautious runs:

- `--no-detect` — skip the SLURM probes
- `--no-create-dirs` — don't `mkdir -p` per-user subdirs
- `--no-agent` — don't install the systemd unit
- `--no-doctor` — skip the final self-check

### When you'd hand-write the file instead

If you want full control, write a `cluster.toml` directly (minimal
shape below), then run `labctl init --use <that-file>` to bootstrap
dirs + agent + doctor around it. `examples/clusters/` has
`single-user.toml`, `multi-tenant.toml`, and `with-remote.toml`
templates to crib from.

```toml
name = "berlin"

[filesystem]
# Filesystem-truth registry root.
runs_base = "/fast/.../labctl_runs"

[filesystem.artifact_roots]
# Artifact kinds — what shows up as rows in the `artifacts` table.
dataset     = "/fast/.../datasets"
checkpoint  = "/fast/.../checkpoints"
eval_result = "/fast/.../eval_logs"

[filesystem.output_roots]
# Recipe-output resolution kinds — what `[outputs.X] type = ...` looks
# up for path templating. Distinct namespace from artifact_roots: a
# `checkpoint_stream` output is a directory of orbax step subdirs that
# `register_outputs` walks, registering each step as a `checkpoint`
# artifact under the same physical root.
checkpoint_stream = "/fast/.../checkpoints"

[repos]
# Logical name → on-disk path.
omegalax = "/fast/home/<you>/omegalax"

[scheduler]
kind = "slurm"

[slurm]
qos = "low"
gres_gpu_syntax = "gpu:{n}"

# Optional. Without [dispatch], `labctl agent` is a no-op.
[dispatch]
reconcile_interval_secs = 60
evald_interval_secs     = 300
policies_dir            = "policies"
```

### Doctor anytime

```bash
labctl doctor
```

Walks the cluster config, checks every directory is writable by
`$USER`, validates `sacct`/`sbatch` on `$PATH`, reports the agent
unit's status, and when a unit is failed/inactive tails the last 20
lines of `journalctl --user -u <unit>` so you don't have to
round-trip.

## 3. Submit your first recipe

Write a recipe that exercises one input and one output. The
[recipe contract](RECIPE_CONTRACT.md) covers what labctl gives your job
and what it expects in return. A minimal example:

```toml
name    = "hello_labctl"
repo    = "omegalax"
command = ["python", "-c", "import json,os,pathlib;p=pathlib.Path(os.environ['LABCTL_RUN_DIR'])/'.lab'/'hello.json';p.write_text(json.dumps({'hi':1}))"]

[resources]
gpus = 0
cpus = 1
mem  = "2GB"
time = "00:05:00"

[outputs.greeting]
type   = "eval_result"
marker = "hello.json"
alias  = "hello_{run.id}"
```

Submit it:

```bash
labctl run hello.toml
```

The CLI runs everything under your uid: it opens the filesystem-truth
registry, snapshots the source repo, writes `runs/<your_user>/<run_id>/`,
renders the sbatch script, calls `sbatch`. The SLURM job is owned by
you; `scancel` works without ceremony.

`labctl` reads `~/.config/labctl/cluster.toml` by default — written
there by `labctl init`. Override per command with `--cluster <path>`
or `$LABCTL_CLUSTER` for multi-cluster work.

## 4. Manage the agent

`labctl init` already installed the per-user agent for you. Inspect
or control it:

```bash
labctl service status                            # both units (agent + ui)
labctl service status --agent                    # just the dispatch agent
systemctl --user restart labctl-agent
loginctl enable-linger $USER                     # survive logout
```

If you skipped `--no-agent` during init, install later:

```bash
labctl service install --agent
```

The unit runs `labctl agent` as you, reconciles only your runs, and
submits eval recipes under your uid.

## 5. Reach the UI from your laptop

The UI is a separate `labctl serve` process — read-only HTTP, no
dispatch loops. Either run it ad-hoc:

```bash
labctl serve --bind 127.0.0.1:8765
```

…or install it as a long-running unit (`labctl-ui.service`):

```bash
labctl service install --ui
```

Tunnel from your laptop:

```bash
ssh -L 8765:127.0.0.1:8765 <login-node>
```

Then open `http://127.0.0.1:8765`.

## 6. Sharing the registry with your team

labctl is built for multi-tenant use over a single shared
filesystem-truth registry. Two unit shapes:

- **Per-user dispatch agent**: `labctl-agent.service`. Each user runs
  their own (auto-installed by `labctl init`). No HTTP listener, just
  the reconcile + evald + periodic-refresh loops, scoped to their own
  runs. Each writes only its own subtree (`runs/<user>/`,
  `eval_state/<user>/`, `pipelines/<user>/`), and the OS enforces
  isolation — there is no RPC layer, no shared service account, no
  impersonation.
- **Read-only UI**: `labctl-ui.service`. Pure HTTP read window over the
  shared registry; no auth needed (still SSH-gated). Run one shared
  instance on a designated login node, or run one per teammate — both
  work because writes never go through the UI.

Aliases and artifact metadata are global so teammates can reference
each other's work directly via `[inputs.X] type = "artifact" artifact =
"<their_alias>"`; writes are partitioned by uid.

Each teammate joining the cluster runs one command:

```bash
labctl init --join /shared/cluster.berlin.toml
```

This symlinks the shared config into their default config location,
creates their per-user subdirs under the existing registry roots,
installs the `labctl-agent` systemd user unit, and runs doctor. The
team-shared cluster.toml stays the canonical source — if it gets
rotated (new path, additional kind), each teammate just re-runs
`labctl init --join ...` to pick up the changes.

One person, once — install the shared read-only UI:

```bash
labctl service install --ui
```

This writes `~/.config/systemd/user/labctl-ui.service` whose
`ExecStart` is `labctl serve --bind 127.0.0.1:8765`. `labctl serve` is
HTTP-only — no reconcile, no evald (those happen in each user's own
agent). Pick one teammate to host it (or run it under a service
account); everyone else SSH-tunnels to `127.0.0.1:8765` on that host.
Linger (`loginctl enable-linger $USER`) keeps it up across logouts.

Three operational details when onboarding a second user:

### Filesystem permissions on `runs_base`

Every user must be able to traverse + read the whole tree but only
write under their own subdir. The simplest setup is a shared POSIX
group + sgid + a permissive default ACL. Once, as anyone with `chgrp`
rights on the registry tree:

```bash
GROUP=<your-lab-group>
ROOTS=( /fast/.../labctl /fast/.../datasets /fast/.../checkpoints /fast/.../eval_logs )

for r in "${ROOTS[@]}"; do
  chgrp -R "$GROUP" "$r"
  chmod -R g+rwX "$r"
  chmod g+s "$r"                 # new entries inherit the group
  setfacl -d -m g::rwX "$r"      # new dirs default to group-rwx
done
```

Each user, once in their shell init:

```bash
umask 002      # files you create are group-writable
```

Verify per user: `labctl doctor` reports every `artifact_root[k]`
and `output_root[k]` as writable.

### Recipe repos must be reachable by every user

Every recipe declares `repo = "<name>"`, which resolves through the
cluster config's `[repos]` map to a path that the submitting job
checks out from. In a multi-tenant rollout that path must be readable
by every teammate's uid, not just the original author's. A repo in a
`700` homedir is the canonical silent multi-tenant blocker —
teammate's `labctl run` fails with permission denied during source
snapshot, not at recipe-validation time.

Two patterns work:

- **Shared group-readable checkouts** (recommended for canonical
  recipes): clone the repo into a group-readable location (e.g. a
  project directory under `/fast/project/<group>/repos/<name>`), point
  the cluster's `[repos]` map at it, and have everyone use that path.
- **Per-user clones with permissions opened up**: keep the repo in
  your homedir but ensure the path is group-traversable end-to-end
  (`chmod g+rx` on each component up to and including the repo). The
  homedir itself must allow group execute (`chmod g+x ~`).

`labctl doctor` actively checks this: every `[repos]` entry runs
through `group_traversable`, which walks parent directories up to `/`
and fails the check if any link in the chain lacks `g+x` (or, for the
repo dir itself, `g+rx`). Run it as the *non-author* user to verify.

### Aliases are a global namespace — by design

`aliases/<name>/.target.json` is one flat namespace across all users.
That's a feature: a teammate can write `[inputs.X] type = "artifact"
artifact = "your_alias_name"` and resolve straight to your artifact
with no path-sharing required.

Collisions fail safely — `claim_dir` uses `mkdir(2)` as a first-writer-wins
primitive, so the second submission errors loudly instead of overwriting.
Convention: prefix aliases you don't intend to share (`<user>_…` or
`<project>_…`); reserve clean names for team-shared canonical artifacts
(e.g. `qwen3vl2b_pretrained`).

### Events log is one append-only JSONL per day

Every daemon appends `run_created` / `status_changed` /
`artifact_registered` events to `events/YYYYMMDD.jsonl`. The file is
read at indexer startup and tailed by the SSE stream. `O_APPEND` writes
≤ PIPE_BUF (4 KiB) are atomic on Lustre/GPFS and on the local FS, and
each event line is well under that, so concurrent appends from multiple
per-user daemons don't corrupt the file. If you ever observe truncated
lines, partition to `events/<user>/<date>.jsonl` — small indexer change,
no schema change.

## 7. Working across clusters

When you have multiple clusters that don't share storage (e.g. HMGU
and Jülich), each gets its own filesystem-truth registry. There is no
unified daemon or sync layer — the right model is **one registry per
cluster, full stop.** Visit each cluster's UI separately when you
need its history; archive a decommissioned cluster's registry tree
with `rsync` and serve it later via `labctl serve --cluster
<archived.toml>`.

The one workflow worth codifying is **importing an artifact** from
another cluster into the local one — typically "I want to fine-tune
on cluster B from a checkpoint that lives on cluster A." The
`import-from-cluster` command reads the foreign alias's sidecars over
SSH, rsyncs the artifact directory into the local artifact root, and
registers it in the local registry with the foreign content hash
preserved (so re-importing the same bytes dedupes) and import
provenance recorded in metadata.

### Tag each cluster.toml with a `[remote]` section

Cluster.toml becomes the cluster's complete identity card —
filesystem layout + scheduler + reachability — committed to git and
shared across the team. Add the reachability bits:

```toml
# julich.toml
name = "julich"

[remote]
# Recommended: a `~/.ssh/config` alias. Putting host details there
# (plus ControlMaster, ProxyJump, etc.) gives labctl one stable
# handle and the rest of your shell the same.
ssh_alias = "julich-login"

# Fallback for hosts that don't need anything fancy:
# host     = "julich.fz-juelich.de"
# ssh_user = "fsra123"

[filesystem]
runs_base = "/scratch/fsra123/labctl"
# ...
```

`[remote]` is consumed only when this file is loaded as a *foreign*
cluster (the `--foreign` arg to `import-from-cluster`, or the
reachability check in `labctl doctor`). When `--cluster julich.toml`
is the *current* cluster, the section is ignored.

### OTP-gated clusters (Jülich, LRZ, …)

OTP prompts already pass through transparently — ssh writes them to
`/dev/tty` regardless of what labctl does with stdio, so you'll see
them in your shell when you invoke `import-from-cluster`. The wrinkle
is **repetition**: each `import-from-cluster` makes ~3 SSH/rsync
calls, which without multiplexing is 3 OTP prompts. SSH ControlMaster
collapses them to one OTP per session lifetime. Add to
`~/.ssh/config`:

```
Host julich-login
    HostName        julich.fz-juelich.de
    User            fsra123
    ControlMaster   auto
    ControlPath     ~/.ssh/cm-%r@%h:%p
    ControlPersist  8h
```

First `import-from-cluster` (or any `ssh julich-login true`) prompts
for OTP and establishes the multiplex socket; subsequent ones reuse
it silently until expiry. labctl never touches the OTP itself — your
SSH config carries all the auth complexity.

`labctl doctor` (when `--cluster julich.toml` has `[remote]` set)
reports `remote reachability` based on `ssh -O check`: "multiplex
session active" means the next import will go through silently; "no
live multiplex session" is not a failure, just a heads-up that the
first call will prompt.

### Importing an artifact

```bash
# Inside the local (berlin) cluster's environment:
labctl --cluster ./berlin.toml import-from-cluster \
    --foreign ./julich.toml \
    --from   ifeval_qwen3vl2b_step9000 \
    --as     julich_ifeval_qwen_v1
```

Outputs JSON with the imported artifact id, local path, content hash,
and a `dedup_hit: true` field if the same content was already in the
local registry (in which case no bytes are copied — just the alias is
bound). Use `--no-copy` for a metadata-only stub when staging bytes
out of band.

Cross-cluster lineage chains are not maintained automatically:
`import-from-cluster` records the foreign cluster/run/alias in
`metadata.imported_from`, but the foreign run row itself is not
copied. That's intentional — federating the run-level data is what
the "registry per cluster" boundary buys you simplicity for. If you
need the foreign run details, open the foreign cluster's UI directly.

## What's next

- **Recipes**: see `docs/RECIPE_CONTRACT.md` for the full contract.
- **Pipelines**: a TOML file listing stages, each pointing at a recipe.
  Inputs of `type = "stage"` chain stages into a DAG that labctl
  submits with `--dependency=afterok:` between SLURM jobs. The pipeline
  can also pin its upstream to a specific historical run with
  `from = "<run_id>"`; stages whose inputs are `type = "from"` resolve
  to that pinned run's outputs (frozen provenance, no re-validation).
  Submit the same pipeline twice and the second submission cache-hits
  the first; submit six in parallel and they coalesce onto one SLURM
  job. Result: fan out experiments cheaply without duplicating work,
  and pin them to frozen upstream state without freezing the registry.
  See `examples/pipelines/from-pinned.toml`.
- **Eval policies**: declarative auto-dispatch — see `examples/policies/`.
- **CLI surface**: `labctl --help` lists everything, including
  `validate`, `show <run>`, `recover-outputs`, and
  `repair-finish-times` for after-the-fact registry repair.
