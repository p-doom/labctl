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

## Workflow philosophy

**Stable artifacts, composable pipelines, ephemeral runs.** Data
preparation lives in long-running pipelines that produce named
artifacts; experiments are small pipeline files that extend a specific
historical run via `from = "<run_id>"`. Stage-level cache-hit
short-circuits any stage whose key already has a succeeded run on disk;
in-flight coalescing routes parallel submissions that share an upstream
key onto a single SLURM job instead of duplicating it. The net effect:
fan out experiments without duplicating work, and pin them to frozen
upstream state without freezing the registry. See
`examples/pipelines/from-pinned.toml`.

## Install

```bash
./scripts/install.sh
```

Builds the embedded frontend, runs `cargo install --path . --features ui`
(so `labctl` lands in `~/.cargo/bin/` and is on PATH for any normal
Rust setup), and points git at `scripts/hooks/` so `cargo test
--all-features` runs before every push.

Re-run after `git pull` to refresh the installed binary.

## Quick start

```bash
./scripts/install.sh   # cargo install + git hook setup
labctl init            # interactive bootstrap — config, dirs, agent, doctor
labctl run path/to/recipe.toml
```

`labctl init` is a full setup wizard, not just a config writer. It
picks one of four modes — interactively or via flag — and does
*everything* needed to leave you with a working setup:

| Situation | Command | What it does |
| --- | --- | --- |
| Brand-new cluster, no template | `labctl init` | Greenfield: SLURM probe + prompts → fresh cluster.toml, per-user dirs, agent unit, doctor. |
| You already wrote a cluster.toml | `labctl init --use ~/cluster.toml` | Symlinks it into the default config location; creates dirs; installs agent; doctor. |
| Standing labctl up at a new site (had one at site A, now at site B) | `labctl init --migrate-from /path/to/cluster.berlin.toml` | Schema carries over; site-local paths reviewed interactively. |
| Joining a colleague's shared registry on this cluster | `labctl init --join /shared/cluster.berlin.toml` | Paths kept verbatim; per-user agent + per-user subdirs only. |

The config is written to `~/.config/labctl/cluster.toml` by default,
so all later `labctl <cmd>` invocations work without `--cluster`.
Override with `--cluster <path>` or `$LABCTL_CLUSTER`.

Once set up:

```bash
labctl run path/to/recipe.toml                   # submit
labctl serve --bind 127.0.0.1:8765               # UI (ssh -L from your laptop)
labctl doctor                                    # re-verify anytime
```

See `docs/ONBOARDING.md` for the full walkthrough,
`docs/RECIPE_CONTRACT.md` for the contract between labctl and your
recipes, and `examples/` for cluster / recipe / policy templates.

## Status

Pre-1.0. Multi-user from day one of the rewrite, single trust domain
(loopback + uid).
