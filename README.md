# labctl

Reproducible lab run envelope, artifact lineage, and async eval control
plane for ML workflows on a SLURM cluster.

`labctl` wraps recipe TOML files into versioned SLURM jobs, captures their
inputs/outputs in a filesystem-truth registry, and provides a small
read-only web UI for monitoring runs, comparing metrics, and tracing
artifact lineage. It is multi-user by design: every action runs under
the invoking user's own uid, and SLURM job ownership matches.

## Architecture in one paragraph

The filesystem under `runs_base/` is the source of truth. Each user's
runs land at `runs_base/runs/<user>/<run_id>/.lab/`; artifacts at
`<artifact_root>/<kind>/<user>/<alias>/`; aliases, eval-request
dedup, pipelines and events live in their own subdirs. The CLI is the
only writer: `labctl run` opens the registry directly, snapshots the
source repo, renders the sbatch script, and shells out to `sbatch`
under its own uid. A per-user `labctl agent` runs reconcile + evald +
throttle as a systemd unit. `labctl serve` is a read-only HTTP server
that anyone can run; it builds an in-memory SQLite cache from the tree
on startup.

## Build

```bash
# Frontend (the SPA is baked into the binary via rust-embed)
cd ui && npm ci && npm run build && cd ..

# Release binary with the UI feature on
cargo build --release --features ui
```

Resulting binary: `target/release/labctl`.

## Quick start

1. Bootstrap a `cluster.toml`. On a new cluster, the fastest path is
   `labctl init` — it probes local SLURM for partitions / QoS / GPU
   gres syntax and writes a templated `cluster.<name>.toml`:

   ```bash
   labctl init \
       --name berlin \
       --runs-base /fast/.../labctl_runs \
       --artifact-root checkpoint=/fast/.../checkpoints \
       --artifact-root dataset=/fast/.../datasets \
       --artifact-root eval_result=/fast/.../eval_logs \
       --repo omegalax=/fast/home/<you>/omegalax
   ```

   On a second cluster, copy the schema from your first cluster's
   identity card and only override the site-local paths:

   ```bash
   labctl init --from cluster.berlin.toml --name julich \
       --runs-base /scratch/<you>/labctl_runs
   ```

   See `examples/clusters/` for templates and `cluster.berlin.toml`
   (the in-repo identity card for the HMGU/berlin cluster) for what
   a populated config looks like in practice. Verify the result:

   ```bash
   labctl --cluster cluster.berlin.toml doctor
   ```

2. Submit a recipe under your own uid:

   ```bash
   labctl --cluster cluster.berlin.toml run my_recipe.toml
   ```

3. Install the per-user agent (reconcile + evald + throttle):

   ```bash
   labctl --cluster cluster.berlin.toml service install --agent
   ```

4. Run the UI ad-hoc when you want to look at runs:

   ```bash
   labctl --cluster cluster.berlin.toml serve --bind 127.0.0.1:8765
   ```

   Tunnel from your laptop: `ssh -L 8765:127.0.0.1:8765 <login-node>`,
   then open `http://127.0.0.1:8765`.

See `docs/ONBOARDING.md` for the full walkthrough and
`docs/RECIPE_CONTRACT.md` for the contract between labctl and your
recipes.

## Status

Pre-1.0. Multi-user from day one of the rewrite, single trust domain
(loopback + uid).
