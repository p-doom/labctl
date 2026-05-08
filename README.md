# labctl

Reproducible lab run envelope, artifact lineage, and async eval control plane
for ML workflows on a SLURM cluster.

`labctl` wraps recipe TOML files into versioned SLURM jobs, captures their
inputs/outputs in a single SQLite registry, and provides a small read-only web
UI for monitoring runs, comparing metrics, and tracing artifact lineage.

## What it does

- **Recipes**: declarative TOML describing a job (repo, command, inputs,
  outputs, resources). `labctl run recipe.toml` snapshots the source repo,
  renders an sbatch script, and submits it.
- **Registry**: a single SQLite DB tracks every run, its inputs/outputs, and
  artifact lineage across runs.
- **Pipelines**: TOML describes a DAG of stages whose outputs flow into
  downstream inputs. Submitted as one unit; SLURM dependencies enforce order.
- **Eval policies**: declarative rules that auto-submit eval recipes when new
  artifacts of a given kind appear (e.g. every checkpoint at step % 5000 == 0).
- **Web UI**: `labctl serve` bundles a Svelte SPA + axum API. Runs the
  reconcile + evald + throttle loops as in-process tokio tasks when
  `[dispatch]` is set in the cluster config.

## Build

```bash
# Rust binary (UI feature bakes the SPA into the binary via rust-embed)
cd ui && npm ci && npm run build && cd ..
cargo build --release --features ui
```

Resulting binary: `target/release/labctl`.

## Quick start

1. Write a `cluster.toml` describing your filesystem layout, repos, and SLURM
   defaults. See `examples/clusters/` for templates.
2. Write a recipe (`examples/recipes/...`) and submit it:

   ```bash
   labctl --cluster cluster.toml run my_recipe.toml
   ```
3. Start the UI (and dispatch loop) as a systemd user service:

   ```bash
   labctl --cluster cluster.toml service install
   ```

   Reach the UI by tunneling `127.0.0.1:8765` from the cluster login node.

See `docs/ONBOARDING.md` for a full walkthrough and `docs/RECIPE_CONTRACT.md`
for the contract between labctl and your recipes.

## Status

Pre-1.0. Single-binary, single-user-per-cluster-config in production.
