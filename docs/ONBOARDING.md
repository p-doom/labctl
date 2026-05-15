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

## 0. Build the binary

```bash
git clone <this-repo> labctl && cd labctl

# Frontend (the SPA is baked into the binary via rust-embed)
cd ui && npm ci && npm run build && cd ..

# Release binary with the UI feature on
cargo build --release --features ui
```

You now have `target/release/labctl`. Put it on your `$PATH` or symlink
it into `~/.local/bin/`.

## 1. Sanity check with the demo seed

The demo seed writes a self-contained filesystem-truth registry to
`/tmp/labctl-demo/` so you can poke at the UI without touching real
data:

```bash
python3 scripts/seed-demo.py
target/release/labctl --cluster /tmp/labctl-demo/labctl.toml serve
```

Open `http://127.0.0.1:8765`. You should see four fake runs, an
artifact + alias, and the runs/artifacts views populated. `labctl
--cluster /tmp/labctl-demo/labctl.toml doctor` against the seed
returns "all checks passed" — a useful end-to-end smoke check before
you point labctl at a real cluster.

## 2. Point labctl at your cluster

The fastest path is `labctl init`. It probes the local SLURM
controller (`sinfo`, `sacctmgr`, `scontrol show config`) for
partition, QoS, and `GresTypes`, then writes a templated
`cluster.<name>.toml` in CWD:

```bash
labctl init \
    --name berlin \
    --runs-base /fast/.../labctl_runs \
    --artifact-root dataset=/fast/.../datasets \
    --artifact-root checkpoint=/fast/.../checkpoints \
    --artifact-root eval_result=/fast/.../eval_logs \
    --repo omegalax=/fast/home/<you>/omegalax
```

On a second cluster (HMGU → Jülich, say), copy the schema from your
first cluster's identity card and only override site-local paths:

```bash
labctl init --from cluster.berlin.toml --name julich \
    --runs-base /scratch/<you>/labctl_runs
```

`--from` carries artifact kinds, output kinds, repo names, dispatch
intervals, throttle, and env across verbatim — the foreign paths are
surfaced so you can see them and rewrite. Flag overrides
(`--runs-base`, `--artifact-root`, `--repo`) win over both `--from`
values and the SLURM auto-detect.

The three SLURM probes `labctl init` runs (skippable via
`--no-detect`):

| Probe                                 | Fills                       |
| ------------------------------------- | --------------------------- |
| `sinfo -h -o '%R'`                    | `[slurm].partition`         |
| `sacctmgr -nP list qos format=Name`   | `[slurm].qos`               |
| `scontrol show config` → `GresTypes`  | `[slurm].gres_gpu_syntax`   |

Each is best-effort: missing binary, non-SLURM cluster, or
permission-denied all degrade to a note in the output rather than an
error. The full list of detected partitions / QoS values is printed,
so even if init picks the wrong default you can see your options.

What `labctl init` never does: touch the registry (`runs_base/` and
the artifact roots are not created — that's the first `labctl run`'s
job, plus your filesystem permissions setup for multi-tenant
rollouts), run the scheduler (no `sbatch`, no `scancel`), or
overwrite an existing file (use `--force` to allow it). The only
side effect is writing one TOML file.

See `examples/clusters/` for templates (`single-user.toml`,
`multi-tenant.toml`, `with-remote.toml`) and `labctl init --help`
for the flag reference.

If you'd rather hand-write the config, the minimal shape is:

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

The committed `cluster.berlin.toml` at the repo root is a real
populated example of this shape — the identity card the HMGU/berlin
cluster's systemd units point at. See `examples/clusters/` for
`single-user.toml`, `multi-tenant.toml`, and `with-remote.toml`
variants.

Verify whichever route you took:

```bash
labctl --cluster cluster.berlin.toml doctor
```

The doctor walks the cluster config, checks every directory is writable
by `$USER` (since `labctl run` creates `runs/<user>/...` and
`<artifact_root>/<user>/...` under your own uid), checks
`sacct`/`sbatch` are on `$PATH`, reports the agent unit's status, and
when a unit is failed or inactive tails the last 20 lines of
`journalctl --user -u <unit>` so you don't have to round-trip.

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
labctl --cluster cluster.toml run hello.toml
```

The CLI runs everything under your uid: it opens the filesystem-truth
registry, snapshots the source repo, writes `runs/<your_user>/<run_id>/`,
renders the sbatch script, calls `sbatch`. The SLURM job is owned by
you; `scancel` works without ceremony.

## 4. Install the agent

If you have eval policies or want SLURM status to update without a
manual `labctl reconcile`, install the per-user agent:

```bash
labctl --cluster cluster.toml service install
labctl --cluster cluster.toml service status
```

The unit is per-user: it runs `labctl agent` as you, reconciles only
your runs, and submits eval recipes under your uid. The unit survives
logout once your account has linger enabled (`loginctl enable-linger
$USER`).

## 5. Reach the UI from your laptop

The UI is a separate `labctl serve` process. Anyone can run one — it's
read-only and stateless. Bind it to loopback:

```bash
labctl --cluster cluster.toml serve --bind 127.0.0.1:8765
```

Tunnel from your laptop:

```bash
ssh -L 8765:127.0.0.1:8765 <login-node>
```

Then open `http://127.0.0.1:8765`.

## 6. Sharing the registry with your team

labctl is built for multi-tenant use over a single shared
filesystem-truth registry. The model splits the two concerns of the
single-user setup:

- **One shared read-only UI**: `labctl serve --no-dispatch`. Runs on
  one host, anyone tunnels to it. Pure HTTP read window over the
  shared registry; no auth needed (still SSH-gated).
- **Per-user dispatch agents**: `labctl agent`. Each user runs their
  own. No HTTP listener, just the reconcile + evald + periodic-refresh
  loops, scoped to their own runs. Each writes only its own subtree
  (`runs/<user>/`, `eval_state/<user>/`, `pipelines/<user>/`), and the
  OS enforces isolation — there is no RPC layer, no shared service
  account, no impersonation.

Aliases and artifact metadata are global so teammates can reference
each other's work directly via `[inputs.X] type = "artifact" artifact =
"<their_alias>"`; writes are partitioned by uid.

Each user, once — install their agent unit:

```bash
labctl --cluster cluster.toml service install --agent
```

This writes `~/.config/systemd/user/labctl-agent.service` whose
`ExecStart` is `labctl agent` (no HTTP, no port). The unit name defaults
to `labctl-agent` so it doesn't collide with a single-user `labctl`
unit that might exist from a previous install.

One person, once — install the shared read-only UI:

```bash
labctl --cluster cluster.toml service install --no-dispatch
```

This writes `~/.config/systemd/user/labctl-ui.service` whose
`ExecStart` is `labctl serve --bind 127.0.0.1:8765 --no-dispatch`.
`--no-dispatch` is the key bit: this process is pure HTTP read-only,
no reconcile, no evald — those happen in each user's own agent. Pick
one teammate to host it (or run it under a service account); everyone
else SSH-tunnels to `127.0.0.1:8765` on that host. Linger
(`loginctl enable-linger $USER`) keeps it up across logouts.

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

Verify per user: `labctl --cluster cluster.toml doctor` reports every
`artifact_root[k]` and `output_root[k]` as writable.

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
with `rsync` and serve it later via `labctl serve --no-dispatch
--cluster <archived.toml>`.

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
- **Pipelines**: TOML stages with explicit inter-stage `[stages.X.depends_on]`.
- **Eval policies**: declarative auto-dispatch — see `examples/policies/`.
- **CLI surface**: `labctl --help` lists everything, including
  `validate`, `show <run>`, `recover-outputs`, and
  `repair-finish-times` for after-the-fact registry repair.
