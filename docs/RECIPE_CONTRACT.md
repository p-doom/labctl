# Recipe contract

The contract between `labctl` and your recipe is small enough to fit on one
page. It has two halves: what labctl gives your job at runtime, and what your
job must produce for labctl to register its outputs.

## What labctl gives your recipe

### Working directory
Before your `command` runs, the shell `cd`s into the per-run snapshot of your
repo at `<run_dir>/source/`. This is a clean checkout of the commit at
submission time — your job sees the exact code labctl recorded.

### Environment variables
Always injected:

| Var               | Value                                             |
|-------------------|---------------------------------------------------|
| `LABCTL_RUN_ID`   | The run's id (UUIDv7).                            |
| `LABCTL_RUN_DIR`  | Absolute path to the run dir.                     |
| `LABCTL_CONTEXT`  | Path to `<run_dir>/.lab/context.json` (alias map).|
| `SLURM_JOB_ID`    | Set by SLURM; `LABCTL_JOB_ID` is the local fallback. |

Plus everything from `[env]` in the cluster config, then everything from
`[env]` in the recipe (recipe wins on conflict, except for `WANDB_*` and
`LABCTL_*` which are set last and cannot be clobbered).

### W&B integration
If your recipe declares `[tracking.wandb]`, labctl additionally exports
`WANDB_ENTITY`, `WANDB_PROJECT`, `WANDB_RUN_ID` (= `LABCTL_RUN_ID`),
`WANDB_NAME`, `WANDB_RESUME=allow`, and optionally `WANDB_RUN_GROUP`.
This makes the W&B URL fully derivable from `(entity, project, run_id)` —
no per-run sentinel file required.

### Templated args
Anything in `[args]`, `[outputs.<role>.alias]`, or values in `[env]` may
reference these tokens; labctl substitutes them before rendering the
sbatch script:

| Token                    | Meaning                                       |
|--------------------------|-----------------------------------------------|
| `{run.id}`               | The run's id.                                 |
| `{run.dir}`              | The run dir.                                  |
| `{params.<key>}`         | Value from `[params]` in the recipe.          |
| `{inputs.<role>.path}`   | Resolved absolute path of input artifact.     |
| `{inputs.<role>.id}`     | Artifact id of the input.                     |
| `{outputs.<role>.path}`  | Absolute path where the output is expected.   |

Any leftover `{...}` token at submission time is a hard error — labctl will
not submit a recipe with unresolved templates.

### Status writing
labctl wraps your command with a `write_status` helper that emits
`<run_dir>/.lab/status.json` atomically. You don't need to call it; the
wrapper writes `running` before your command and `succeeded` / `failed`
based on its exit code.

## What labctl demands from your recipe

### One marker file per output
Each `[outputs.<role>]` declares a `marker` filename. Your job must write a
file with that name into the resolved output path. Without the marker,
labctl will not register the output as an artifact.

For most outputs the marker lives at `<output_path>/<marker>`. For
`type = "checkpoint_stream"` outputs, the marker lives one step deeper at
`<output_path>/<step>/<marker>` (one marker per step), so the stream
appears as a sequence of artifacts in the registry.

### Output paths are computed, not chosen
Output paths are resolved by labctl as
`<cluster.filesystem.artifact_roots[<type>]>/<rendered_alias>/`. Use
`{outputs.<role>.path}` to reference them in `[args]`. Do not try to write
to a hand-picked path; the artifact root must match the registered type so
lineage queries work.

If the output marker already exists at submission time, labctl refuses to
submit. Bind the alias to a unique key (e.g. `{run.id}`,
`{inputs.checkpoint.id}`) for per-submission outputs.

### Exit code semantics
- exit `0` → status `succeeded`, outputs scanned and registered.
- nonzero → status `failed`, no outputs registered (markers may exist
  partially; labctl skips them).

## Minimal example

```toml
name = "fit_qwen3vl"
repo = "omegalax"
command = ["uv", "run", "python", "fit.py"]

[resources]
gpus = 8
cpus = 32
mem  = "256GB"
time = "08:00:00"

[inputs.dataset]
type = "dataset"
alias = "ifeval_replay_v1"

[outputs.checkpoint]
type   = "checkpoint_stream"
marker = "_CHECKPOINT_METADATA"
alias  = "fit_qwen3vl_{run.id}"

[args]
data_dir   = "{inputs.dataset.path}"
output_dir = "{outputs.checkpoint.path}"
lr         = "{params.lr}"

[params]
lr = "3e-5"

[tracking.wandb]
entity  = "p-doom"
project = "fit"
group   = "qwen3vl-replay"
```

## Escape hatches

- **Custom env**: anything not covered by `[tracking.*]` belongs in `[env]`.
- **Arbitrary shell**: a recipe whose `command` is `["bash", "-c", "..."]`
  can do anything inside the job body.
- **Custom sbatch directives**: anything `[resources]` doesn't model goes
  in `resources.sbatch_extra` as a list of flag strings — labctl prepends
  the `#SBATCH ` prefix and inserts each line after the typed directives:

  ```toml
  [resources]
  gpus = 4
  cpus = 16
  mem  = "64GB"
  time = "12:00:00"
  sbatch_extra = [
    "--array=0-3",
    "--mail-type=END,FAIL",
    "--gpu-bind=closest",
  ]
  ```

  Don't override flags `[resources]` already manages (`--cpus-per-task`,
  `--mem`, `--time`, `--gres`, etc.) — labctl won't stop you, but
  duplicate `#SBATCH` lines confuse SLURM and the dispatcher.
- **Pre-existing artifacts**: register externally-produced data with
  `labctl register-external --alias <name> --path <abs> --kind <type>`,
  then reference it as an input by alias.
