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
`command`, anything in `[args]`, `[outputs.<role>.alias]`, and values in
`[env]` may reference these tokens; labctl substitutes them before
rendering the sbatch script:

| Token                    | Meaning                                       |
|--------------------------|-----------------------------------------------|
| `{run.id}`               | The run's id.                                 |
| `{run.dir}`              | The run dir.                                  |
| `{params.<key>}`         | Value from `[params]` in the recipe.          |
| `{inputs.<role>.path}`   | Resolved absolute path of input artifact.     |
| `{inputs.<role>.id}`     | Artifact id of the input.                     |
| `{outputs.<role>.path}`  | Absolute path where the output is expected.   |

Any leftover `{...}` token at submission time is a hard error — labctl will
not submit a recipe with unresolved templates. Only the labctl token shape
(`{word(.word)*}`) is treated as a token, so inline JSON in a `bash -c`
command passes through untouched.

### Status writing
labctl wraps your command with a `write_status` helper that emits
`<run_dir>/.lab/status.json` atomically. You don't need to call it; the
wrapper writes `running` before your command and `succeeded` / `failed`
based on its exit code.

### Input types

The `type` field on each `[inputs.<role>]` chooses how labctl resolves
the artifact at submit time:

| type         | resolves via                                                          |
|--------------|-----------------------------------------------------------------------|
| `artifact`   | named registry alias (`artifact = "alias_name"`)                      |
| `external`   | absolute filesystem path (`path = "/abs/..."`)                        |
| `stage`      | another stage in the same pipeline (`stage = "X", role = "..."`)      |
| `from`       | the pipeline's `from`-pinned run (`role = "..."`)                     |
| `checkpoint` | injected by an eval policy at dispatch (per-checkpoint eval recipes)  |

`stage` and `from` are pipeline-scoped: `stage` pulls from an
intra-pipeline parent, `from` pulls from the pipeline's historical
`from = "<run_id>"` pin. Submitting a recipe outside a pipeline that
declares either is a hard error.

## What labctl demands from your recipe

### Inputs are read-only
`{inputs.<role>.path}` points at the artifact's actual on-disk directory —
which on a cross-user cache hit will live under the *producer's* user
prefix, not yours (e.g. you submitted as `bob`, the cache hit reused
`alice`'s prior run, so `{inputs.X.path}` resolves to
`<artifact_root>/alice/<alias>`). With shared-group + setgid on the
artifact roots (the standard multi-user setup), your job has read +
write access to that directory.

Do not write into it. Treat `{inputs.<role>.path}` as strictly
read-only. A stray write — a tempfile, a `.lock`, an in-place edit —
lands inside another user's registered artifact and silently corrupts
shared state with no error from labctl or your filesystem.

Recipes should write only under `{outputs.<role>.path}` and `{run.dir}`.

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

### Browsable rollouts

The UI's rollout viewer replays GUI trajectories straight off disk, but only
for an output declared `type = "eval_result"` **with** a `marker`. labctl
parses that marker file as JSON and stores it verbatim as the artifact's
`metadata.result`; everything the viewer knows comes from that blob. No
marker, no `result`, no rollout.

A result counts as browsable when it carries `traj_path`, or `gif_path`, or a
non-empty `runs` array. Two layouts resolve to actual files:

- **Multi** — `result.runs` is an array of objects, one per episode, each
  with a `subdir` relative to the artifact path. Episode *i* is read from
  `<artifact_path>/<runs[i].subdir>/trajectory.jsonl` and
  `<artifact_path>/<runs[i].subdir>/steps/`. Each entry labels its tab with
  `instruction`, falling back to `slug`; keep `index` equal to the entry's
  position in the array, since the tab sends `index` and the server resolves
  by position.
- **Single** — `result.traj_path` is an *absolute* path to the trajectory
  file; frames are read from a `steps/` directory beside it. `task` is
  ignored. (`gif_path` alone marks a point browsable but resolves to
  nothing — pair it with `traj_path`.)

Two filenames are hardcoded, and they are the easy mistake:

- In the multi shape the trajectory must be named exactly
  **`trajectory.jsonl`**. Nothing else is looked for. (The single shape may
  name it anything, because you hand over the full path.)
- Frames must be named **`step_%03d.png`** — `step_000.png`, `step_001.png`,
  … The viewer builds the path from the frame index rather than listing or
  sorting the directory, so any other name is unreachable and a gap in the
  numbering is a hole it cannot skip past.

`frame_count`, by contrast, is a count of *every* `*.png` under `steps/`, so
it matches the fetchable index space only when `steps/` holds exactly one PNG
per step and nothing else. A job that also writes `step_000_after.png` (as
today's OSWorld eval does) reports roughly double its real step count, and
the back half of the scrubber 404s.

The trajectory is JSON-lines — one object per step, in frame order (blank
lines are skipped, unparseable ones silently dropped). Four fields are read:

| Field      | Meaning                                                       |
|------------|---------------------------------------------------------------|
| `step_num` | Frame this row jumps to when clicked; keep it = line position. |
| `action`   | Action string, shown per-frame and in the trajectory table.    |
| `response` | Model output; the literal `"<reset>"` renders as a dash.       |
| `reward`   | Number, colour-coded zero / partial / full.                    |

A multi-shape example in production — the OSWorld CUA eval, whose
`eval_result` artifact carries `marker = "completed.json"`:

```
<artifact_path>/completed.json     # marker; its JSON becomes metadata.result
<artifact_path>/task_000_multi.chrome.wikipedia_transformers_article/
    trajectory.jsonl               # 65 lines, step_num 0…64
    steps/step_000.png … step_064.png   # + step_NNN_after.png, hence the
                                        #   inflated frame_count above
```

with `result.runs[0] = {"index": 0, "subdir":
"task_000_multi.chrome.wikipedia_transformers_article", "slug": <same>,
"instruction": "Open Chrome and search for the Wikipedia article on
transformers, …"}` — `subdir` is the only field the server needs; `slug` and
`instruction` are the tab label.

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
type     = "artifact"
artifact = "ifeval_replay_v1"

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

## Heterogeneous jobs

Some work needs two machine shapes at once — e.g. a CPU node serving
environment VMs alongside the GPU node training against them. Declaring the
second shape as a *separate* run costs you the two things that make the
pairing work: co-scheduling (the GPUs would idle while the CPU side queues)
and coupled teardown (a crashed trainer would leave the fleet holding a node).

`[[resources.components]]` allocates them as one SLURM heterogeneous job:

```toml
[resources]          # component 0
gpus = 0
cpus = 48
mem  = "256GB"
time = "12:00:00"

[[resources.components]]   # component 1
gpus  = 4
cpus  = 16
mem   = "128GB"
nodes = 1
```

Each entry emits a `#SBATCH hetjob` separator and its own directive block.
Job-wide settings — `time`, `qos`, `account`, dependencies and log paths —
are taken from component 0 and deliberately not repeated, per SLURM's
semantics.

Note what SLURM does with the batch script: **the body runs on component 0
only**. Your `command` is therefore responsible for placing work on the other
components with `srun --het-group=N`:

```toml
command = [
  "bash", "-c",
  "srun --het-group=0 python scripts/fleet.py & srun --het-group=1 python scripts/train.py; wait",
]
```

Run state needs no special handling: `sacct -j <job_id>` reports one row per
component, which labctl aggregates the same way it already aggregates array
elements (any failure trumps; running outranks succeeded), and
`scancel <job_id>` cancels every component.

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
