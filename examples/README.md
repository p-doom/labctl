# examples/

Working templates for cluster, recipe, and policy files. Every file
here parses against the current schema — `cargo test --test smoke`
fails CI if one drifts.

## clusters/

- **single-user.toml** — solo dev on a SLURM cluster, no shared
  registry concerns.
- **multi-tenant.toml** — group-shared registry with `[dispatch]`
  enabled and the multi-tenant filesystem expectations called out.
- **with-remote.toml** — adds a `[remote]` section, the form a
  cluster.toml takes when it's the FOREIGN end of a
  `labctl import-from-cluster` invocation.

Pick the closest match, copy it, edit the paths. Or, on a new
cluster, use [`labctl init`](../docs/INIT.md) (with `--from
examples/clusters/<x>.toml`) to get an auto-edited copy.

## recipes/

- **train.toml** — minimal training recipe emitting a
  `checkpoint_stream` output.
- **eval.toml** — eval recipe consuming a checkpoint artifact and
  emitting an `eval_result`.
- **sweep.toml** — sweep recipe using `[sweep]` to fan out a SLURM
  array job.

See [`docs/RECIPE_CONTRACT.md`](../docs/RECIPE_CONTRACT.md) for the
full contract.

## policies/

- **eval_per_checkpoint.toml** — auto-dispatches `eval.toml`
  whenever a new `checkpoint` artifact from `train_example` is
  registered. Pair with `[dispatch].policies_dir` in a
  cluster.toml; `labctl agent` evaluates it on every
  `evald_interval_secs` tick.
