# `labctl init` — bootstrap a cluster.toml

The fastest path from "I have a labctl binary on a new cluster" to
"I have a working cluster.toml". Run once on the new cluster, edit a
handful of paths, then `labctl doctor` it.

## Two flows

### Fresh

Use when you're standing up labctl on a brand-new cluster with no
template to crib from. The local SLURM controller is probed for
partition, QoS, and GresTypes; everything else is a `/path/to/...`
placeholder you'll edit.

```bash
labctl init \
    --name julich \
    --runs-base /scratch/$USER/labctl_runs \
    --artifact-root dataset=/scratch/$USER/datasets \
    --artifact-root checkpoint=/scratch/$USER/checkpoints \
    --artifact-root eval_result=/scratch/$USER/eval_logs \
    --repo myrepo=/home/$USER/repos/myrepo
```

Writes `cluster.julich.toml` in CWD. Edit any remaining placeholders
and validate:

```bash
labctl --cluster cluster.julich.toml doctor
```

### From an existing cluster.toml

Use when you're rolling labctl out to a second site and you already
have a working cluster.toml on the first. This is the workflow
behind "import the config from this cluster".

```bash
# scp the foreign config locally, then:
labctl init \
    --from ./cluster.berlin.toml \
    --name julich \
    --runs-base /scratch/$USER/labctl_runs
```

The schema (artifact kinds, output kinds, repo names, dispatch
intervals, throttle, env, SLURM hints) is copied verbatim from
`--from`. Paths inside it are surfaced unchanged — they almost
certainly need to be rewritten for the new cluster, and surfacing
them rather than nulling them lets you see what the original
cluster's layout looked like as a starting point.

Flag overrides (`--runs-base`, `--artifact-root`, `--repo`) win over
both the `--from` values and the SLURM auto-detect.

## SLURM auto-detect

`labctl init` (without `--no-detect`) runs three best-effort probes:

| Probe                                 | Fills                       |
| ------------------------------------- | --------------------------- |
| `sinfo -h -o '%R'`                    | `[slurm].partition`         |
| `sacctmgr -nP list qos format=Name`   | `[slurm].qos`               |
| `scontrol show config` → `GresTypes`  | `[slurm].gres_gpu_syntax`   |

Each is best-effort. Missing binary, non-SLURM cluster, or
permission-denied all degrade to a note in the output instead of an
error — you can still edit the generated TOML by hand. The full
list of partitions / QoS values is printed so you can see what was
available even if init picked the wrong default.

Pass `--no-detect` to skip the probes entirely (CI, non-SLURM, or
when you want only the schema from `--from`).

## What it never does

- Touch the registry. `runs_base/` and the artifact roots are not
  created — that's the job of the first `labctl run` (or your
  filesystem permissions setup for multi-tenant rollouts).
- Run the scheduler. No `sbatch`, no `scancel`.
- Overwrite an existing file. Use `--force` to allow it.

The only side effect is writing one TOML file.
