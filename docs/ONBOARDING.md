# Onboarding

A 10-minute walkthrough that gets you from `git clone` to a running web UI
displaying a real recipe submission. By the end of this you should be able
to point labctl at a real cluster.

## 0. Build the binary

```bash
git clone <this-repo> labctl && cd labctl

# Frontend (the SPA is baked into the binary via rust-embed).
cd ui && npm ci && npm run build && cd ..

# Release binary with the UI feature on.
cargo build --release --features ui
```

You now have `target/release/labctl`. Put it on your `$PATH` or symlink it
into `~/.local/bin/`.

## 1. Sanity check with the demo seed

The demo seed writes a self-contained registry to `/tmp/labctl-demo/` so
you can poke at the UI without touching real data:

```bash
python3 scripts/seed-demo.py
target/release/labctl --cluster /tmp/labctl-demo/labctl.toml serve
```

Open `http://127.0.0.1:8765`. You should see a list of fake runs, an
artifacts view, lineage graph, and pipeline DAG.

## 2. Point labctl at your cluster

Write a `cluster.toml` in your home repo. Minimal version for SLURM:

```toml
name = "berlin"

[filesystem]
# Persistent shared storage for run snapshots and the SQLite registry.
runs_base   = "/fast/.../labctl_runs"
registry_db = "/fast/.../labctl_runs/registry.db"

[filesystem.artifact_roots]
# Every output kind your recipes declare must be listed here.
dataset           = "/fast/.../datasets"
checkpoint_stream = "/fast/.../checkpoints"
eval_result       = "/fast/.../eval_logs"

[repos]
# Logical name -> on-disk path. Recipes refer to repos by logical name.
omegalax = "/fast/home/<you>/omegalax"

[scheduler]
kind = "slurm"

[slurm]
qos = "low"
gres_gpu_syntax = "gpu:{n}"

# Optional. Without [dispatch], `labctl serve` only runs the UI; reconcile
# + evald + throttle won't fire. Add it as soon as you have eval policies.
[dispatch]
reconcile_interval_secs = 60
evald_interval_secs     = 300
policies_dir            = "policies"
```

Verify the config is sane:

```bash
labctl --cluster cluster.toml doctor
```

The doctor walks the cluster config, tries to write to every directory,
checks `sacct`/`sbatch` are on `$PATH`, and reports the systemd unit's
status.

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

You'll see `run_id`, `job_id`, and a hint to install the systemd unit.

## 4. Keep dispatch alive

If you have eval policies or want SLURM status to update without a manual
`labctl reconcile`, install the systemd user service. It calls
`labctl serve` under the hood, which runs reconcile + evald + throttle as
in-process tokio tasks while serving the UI.

```bash
labctl --cluster cluster.toml service install
labctl --cluster cluster.toml service status
```

The unit survives logout once your account has linger enabled
(`loginctl enable-linger $USER`).

## 5. Reach the UI from your laptop

The service binds `127.0.0.1:8765` by default. Tunnel it from your laptop:

```bash
ssh -L 8765:127.0.0.1:8765 <login-node>
```

Then open `http://127.0.0.1:8765` locally.

## What's next

- **Recipes**: see `docs/RECIPE_CONTRACT.md` for the full contract.
- **Pipelines**: TOML stages with explicit inter-stage `[stages.X.depends_on]`.
- **Eval policies**: declarative auto-dispatch — see `examples/policies/`.
- **CLI surface**: `labctl --help` lists everything, including
  `validate`, `show <run>`, `recover-outputs`, and `repair-finish-times`
  for after-the-fact registry repair.
