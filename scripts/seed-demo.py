#!/usr/bin/env python3
"""Seed a labctl registry with realistic fake data for UI development.

Writes:
  /tmp/labctl-demo/labctl.toml          cluster config
  /tmp/labctl-demo/registry.db          SQLite registry
  /tmp/labctl-demo/runs/<run_id>/.lab/  log + status fixtures

Run via:
  python3 scripts/seed-demo.py
  cargo run --features ui -- --cluster /tmp/labctl-demo/labctl.toml serve
"""
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

BASE = Path("/tmp/labctl-demo")
RUNS_BASE = BASE / "runs"
ART_BASE = BASE / "artifacts"
REGISTRY = BASE / "registry.db"

NOW = int(time.time())


def fresh():
    if REGISTRY.exists():
        REGISTRY.unlink()
    BASE.mkdir(parents=True, exist_ok=True)
    RUNS_BASE.mkdir(parents=True, exist_ok=True)
    ART_BASE.mkdir(parents=True, exist_ok=True)


def schema(c):
    c.executescript("""
    CREATE TABLE runs (
      id TEXT PRIMARY KEY, recipe_name TEXT NOT NULL, recipe_hash TEXT NOT NULL,
      status TEXT NOT NULL, job_id TEXT, run_dir TEXT NOT NULL, repo TEXT NOT NULL,
      source_path TEXT NOT NULL, recipe_json TEXT NOT NULL, context_json TEXT NOT NULL,
      created_at INTEGER NOT NULL, finished_at INTEGER,
      pipeline_id TEXT, dependency_on TEXT, stage_name TEXT,
      submitted_by TEXT
    );
    CREATE TABLE pipelines (
      id TEXT PRIMARY KEY, name TEXT NOT NULL,
      pipeline_path TEXT, created_at INTEGER NOT NULL
    );
    CREATE TABLE artifacts (
      id TEXT PRIMARY KEY, kind TEXT NOT NULL, path TEXT NOT NULL,
      content_hash TEXT NOT NULL, producer_run_id TEXT, metadata_json TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE artifact_aliases (
      alias TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, created_at INTEGER NOT NULL
    );
    CREATE TABLE run_inputs (
      run_id TEXT NOT NULL, role TEXT NOT NULL, artifact_id TEXT,
      resolved_path TEXT NOT NULL, PRIMARY KEY (run_id, role)
    );
    CREATE TABLE run_outputs (
      run_id TEXT NOT NULL, role TEXT NOT NULL, artifact_id TEXT NOT NULL,
      PRIMARY KEY (run_id, role, artifact_id)
    );
    CREATE TABLE eval_requests (
      eval_key TEXT PRIMARY KEY, checkpoint_artifact_id TEXT NOT NULL,
      eval_recipe_hash TEXT NOT NULL, policy_id TEXT NOT NULL,
      eval_run_id TEXT, state TEXT NOT NULL,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE TABLE events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, event_type TEXT NOT NULL,
      payload_json TEXT NOT NULL, created_at INTEGER NOT NULL
    );
    CREATE TABLE tracking (
      run_id TEXT PRIMARY KEY, entity TEXT NOT NULL, project TEXT NOT NULL,
      url TEXT NOT NULL, group_name TEXT, source TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    """)


def add_run(
    c, *, run_id, recipe, status, repo, created_offset, duration=None,
    job_id=None, pipeline_id=None, stage_name=None, dependency_on=None,
    inputs=None, outputs=None, context=None,
):
    created = NOW - created_offset
    finished = (created + duration) if duration is not None else None
    is_terminal = status in ("succeeded", "failed", "cancelled", "timeout", "oom", "unknown_terminal")
    if not is_terminal:
        finished = None
    run_dir = str(RUNS_BASE / run_id)
    Path(run_dir, ".lab").mkdir(parents=True, exist_ok=True)
    log = f"""[{time.strftime('%H:%M:%S', time.gmtime(created))}] starting {recipe['name']}
[{time.strftime('%H:%M:%S', time.gmtime(created+10))}] loaded checkpoint, batch=128 lr=3.0e-4
[{time.strftime('%H:%M:%S', time.gmtime(created+30))}] step 100 loss=2.847 throughput=14250 tok/s
[{time.strftime('%H:%M:%S', time.gmtime(created+60))}] step 1000 loss=2.342 throughput=14310 tok/s
[{time.strftime('%H:%M:%S', time.gmtime(created+120))}] step 5000 loss=1.987 throughput=14220 tok/s
"""
    # Plant a wandb-init banner on legacy runs that had W&B integration before
    # the [tracking.wandb] schema existed. Backfill picks these up.
    if recipe.get("_legacy_wandb"):
        wb_entity, wb_project, wb_id = recipe["_legacy_wandb"]
        banner = (
            f"wandb: Tracking run with wandb version 0.18.5\n"
            f"wandb: Run data is saved locally in /scratch/wandb/run-20260504_000000-{wb_id}\n"
            f"wandb: Syncing run {recipe['name']}-{wb_id}\n"
            f"wandb: ⭐️ View project at https://wandb.ai/{wb_entity}/{wb_project}\n"
            f"wandb: 🚀 View run at https://wandb.ai/{wb_entity}/{wb_project}/runs/{wb_id}\n"
        )
        log = banner + log
    if status == "oom":
        log += "[ERROR] CUDA out of memory while allocating 24.50 GiB on rank 3\nRuntimeError: CUDA out of memory at line 412 in train.py\nslurmstepd: error: Detected 1 oom-kill event(s) in StepId=2149098.batch\n"
    elif status == "failed":
        log += "[ERROR] AssertionError: validation loss diverged at step 4923\nTraceback (most recent call last):\n  File \"scripts/train.py\", line 287, in main\n    raise AssertionError(f\"validation loss diverged at step {step}\")\n"
    elif status == "succeeded":
        log += f"[{time.strftime('%H:%M:%S', time.gmtime((finished or created+200)))}] training complete, saved checkpoint\n"
    Path(run_dir, ".lab", f"{recipe['name']}_{job_id or 'NA'}.log").write_text(log)
    persisted_recipe = {k: v for k, v in recipe.items() if not k.startswith("_")}
    c.execute(
        "INSERT INTO runs(id, recipe_name, recipe_hash, status, job_id, run_dir, repo, "
        "source_path, recipe_json, context_json, created_at, finished_at, "
        "pipeline_id, dependency_on, stage_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, recipe["name"], "sha256:abcdef0123456789" + run_id[-4:],
         status, job_id, run_dir, repo, "/repos/labctl-demo/recipes/" + recipe["name"] + ".toml",
         json.dumps(persisted_recipe), json.dumps(context or {"seed": 42}),
         created, finished, pipeline_id,
         json.dumps(dependency_on) if dependency_on else None, stage_name)
    )
    # Mirror the production submission flow: when the recipe declares
    # [tracking.wandb], a tracking row is written at submit time.
    wandb_cfg = persisted_recipe.get("tracking", {}).get("wandb")
    if wandb_cfg:
        c.execute(
            "INSERT INTO tracking(run_id, entity, project, url, group_name, source, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (run_id, wandb_cfg["entity"], wandb_cfg["project"],
             f"https://wandb.ai/{wandb_cfg['entity']}/{wandb_cfg['project']}/runs/{run_id}",
             wandb_cfg.get("group"), "schema", created)
        )
    if inputs:
        for role, art_id, p in inputs:
            c.execute("INSERT INTO run_inputs VALUES (?,?,?,?)", (run_id, role, art_id, p))
    if outputs:
        for role, art_id in outputs:
            c.execute("INSERT INTO run_outputs VALUES (?,?,?)", (run_id, role, art_id))
    c.execute("INSERT INTO events(run_id, event_type, payload_json, created_at) VALUES (?,?,?,?)",
              (run_id, "run_created", json.dumps({}), created))
    if job_id:
        c.execute("INSERT INTO events(run_id, event_type, payload_json, created_at) VALUES (?,?,?,?)",
                  (run_id, "run_submitted", json.dumps({"job_id": job_id}), created + 1))
    if status not in ("created", "submitted"):
        c.execute("INSERT INTO events(run_id, event_type, payload_json, created_at) VALUES (?,?,?,?)",
                  (run_id, "run_status", json.dumps({"status": "running"}), created + 5))
    if is_terminal:
        c.execute("INSERT INTO events(run_id, event_type, payload_json, created_at) VALUES (?,?,?,?)",
                  (run_id, "run_status", json.dumps({"status": status}), finished))


def add_artifact(c, *, art_id, kind, path, hash_, producer, alias=None, created_offset=0, metadata=None):
    p = ART_BASE / kind / Path(path).name
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.write_text("(fake content)")
    c.execute(
        "INSERT INTO artifacts(id, kind, path, content_hash, producer_run_id, metadata_json, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (art_id, kind, str(p), hash_, producer, json.dumps(metadata or {}), NOW - created_offset),
    )
    if alias:
        c.execute("INSERT INTO artifact_aliases VALUES (?,?,?)", (alias, art_id, NOW - created_offset))


def main():
    fresh()
    conn = sqlite3.connect(REGISTRY)
    conn.execute("PRAGMA foreign_keys=ON")
    schema(conn)

    # ----- Pipeline: data → train → eval -----
    pipe_id = "pipe_v3_main"
    conn.execute("INSERT INTO pipelines VALUES (?,?,?,?)",
                 (pipe_id, "lm_v3", "/repos/labctl-demo/pipelines/lm_v3.toml", NOW - 86400))

    # Stage 1: data prep (succeeded)
    recipe_data = {
        "name": "tokenize_c4",
        "repo": "labctl-demo",
        "command": ["python", "scripts/tokenize.py", "--shard", "{shard_index}"],
        "resources": {"gpus": 0, "cpus": 32, "mem": "120GB", "time": "06:00:00"},
        "params": {"shard_index": 0, "vocab_size": 50_000, "tokenizer": "bpe"},
    }
    add_artifact(conn, art_id="artifact_tok_001", kind="dataset",
                 path="/data/c4_tokenized_v3", hash_="sha256:dead0001beef0001cafe0001",
                 producer="run_aaaa1111data00", alias="c4_tokenized@v3", created_offset=86000)

    add_run(conn, run_id="run_aaaa1111data00", recipe=recipe_data, status="succeeded",
            repo="labctl-demo", created_offset=86400, duration=4200, job_id="2148901",
            pipeline_id=pipe_id, stage_name="tokenize",
            outputs=[("dataset", "artifact_tok_001")],
            context={"shard_index": 0, "host": "compute-001"})

    # Stage 2: train (running, mid-flight)
    recipe_train = {
        "name": "train_lm_v3",
        "repo": "labctl-demo",
        "command": ["python", "scripts/train.py", "--config", "{params.config}"],
        "resources": {"gpus": 8, "cpus": 96, "mem": "512GB", "time": "48:00:00",
                      "partition": "gpu", "qos": "high"},
        "params": {"config": "configs/lm_8b.toml", "lr": 3.0e-4, "batch_size": 128,
                   "warmup_steps": 2000, "max_steps": 100000},
        "inputs": {"data": {"type": "stage", "stage": "tokenize", "role": "dataset"}},
        "outputs": {"checkpoint": {"type": "checkpoint", "marker": "step_final/",
                                   "alias": "lm_v3_8b_step{step}"}},
        "tracking": {"wandb": {"entity": "labctl-demo", "project": "lm_v3",
                               "group": "main"}},
    }
    add_run(conn, run_id="run_bbbb2222train0", recipe=recipe_train, status="running",
            repo="labctl-demo", created_offset=21600, job_id="2149045",
            pipeline_id=pipe_id, stage_name="train",
            dependency_on={"tokenize": "run_aaaa1111data00"},
            inputs=[("data", "artifact_tok_001", "/data/c4_tokenized_v3")],
            context={"world_size": 8, "host_master": "gpu-node-014",
                     "wandb_url": "https://wandb.ai/labctl-demo/lm_v3/runs/abc123"})

    # Stage 3: eval recipe + per-checkpoint series.
    recipe_eval = {
        "name": "eval_mmlu",
        "repo": "labctl-demo",
        "command": ["python", "scripts/eval.py", "--ckpt", "{inputs.ckpt.path}"],
        "resources": {"gpus": 1, "cpus": 16, "mem": "64GB", "time": "01:00:00"},
        "inputs": {"ckpt": {"type": "checkpoint"}},
        "params": {"task": "mmlu", "shots": 5},
    }

    # Eval trajectory: 5 checkpoints at increasing steps; eval finishes for
    # the older ones, latest is still running. Drives the per-policy
    # series chart in the run panel.
    checkpoint_series = [
        # (step, checkpoint_offset_secs, eval_offset_secs, eval_status, strict, loose, prompt_strict, prompt_loose)
        (10000, 21600, 18000, "succeeded", 0.4821, 0.5612, 0.4012, 0.4801),
        (20000, 18000, 14400, "succeeded", 0.5412, 0.6210, 0.4581, 0.5320),
        (30000, 14400, 10800, "succeeded", 0.5895, 0.6712, 0.5102, 0.5780),
        (40000, 10800,  7200, "succeeded", 0.6231, 0.7012, 0.5481, 0.6120),
        (50000,  3600,  1800, "running",   None,   None,   None,   None),
    ]
    for step, ck_off, ev_off, ev_status, strict, loose, prompt_s, prompt_l in checkpoint_series:
        ck_id = f"artifact_ckpt_step{step}"
        add_artifact(conn, art_id=ck_id, kind="checkpoint",
                     path=f"/checkpoints/lm_v3_8b_step{step}",
                     hash_=f"sha256:ckpt{step:08d}",
                     producer="run_bbbb2222train0",
                     alias=f"lm_v3_8b_step{step}",
                     created_offset=ck_off,
                     metadata={"step": step, "role": "checkpoint",
                               "marker": "_CHECKPOINT_METADATA",
                               "stream_alias": "lm_v3_8b_step{step}",
                               "producer_recipe": "train_lm_v3"})
        conn.execute("INSERT INTO run_outputs VALUES (?,?,?)",
                     ("run_bbbb2222train0", "checkpoint", ck_id))

        eval_run_id = f"run_eval_step{step}"
        eval_dur = None if ev_status == "running" else 1500
        # Each eval run *consumes* the checkpoint and *produces* a result.
        eval_inputs = [("ckpt", ck_id, f"/checkpoints/lm_v3_8b_step{step}")]
        eval_outputs = []
        if ev_status == "succeeded":
            res_id = f"artifact_evalres_step{step}"
            res_meta = {
                "role": "result",
                "marker": "result.json",
                "producer_recipe": "eval_mmlu",
                "result": {
                    "schema_version": 1,
                    "task": "inspect_evals/ifeval",
                    "scores": {
                        "ifeval/strict_accuracy": strict,
                        "ifeval/loose_accuracy": loose,
                        "ifeval/prompt_strict_accuracy": prompt_s,
                        "ifeval/prompt_loose_accuracy": prompt_l,
                    },
                    "params": {"temperature": 0.0, "max_tokens": 1280, "seed": 0},
                    "n_samples": 542,
                },
            }
            add_artifact(conn, art_id=res_id, kind="eval_result",
                         path=f"/eval_logs/run_eval_step{step}",
                         hash_=f"sha256:evalres{step:08d}",
                         producer=eval_run_id,
                         alias=f"lm_v3_8b_step{step}_mmlu",
                         created_offset=ev_off - 60, metadata=res_meta)
            eval_outputs.append(("result", res_id))

        add_run(conn, run_id=eval_run_id, recipe=recipe_eval, status=ev_status,
                repo="labctl-demo", created_offset=ev_off, duration=eval_dur,
                job_id=str(2150000 + step // 1000),
                inputs=eval_inputs, outputs=eval_outputs)

        conn.execute(
            "INSERT INTO eval_requests VALUES (?,?,?,?,?,?,?,?)",
            (f"eval_mmlu_step{step}", ck_id, "sha256:eval01",
             "ifeval_per_ckpt_5fps_v1", eval_run_id, ev_status,
             NOW - ev_off, NOW - max(ev_off - (eval_dur or 0), 0)),
        )

    # Latest checkpoint stays linked to a friendly alias so existing
    # references in the demo (artifact panel, etc.) keep resolving.
    conn.execute("UPDATE artifact_aliases SET artifact_id = ? WHERE alias = ?",
                 ("artifact_ckpt_step50000", "lm_v3_8b_step50000"))

    # ----- Standalone runs -----
    recipe_quick = {
        "name": "ablation_dropout",
        "repo": "research-side",
        "command": ["python", "scripts/ablation.py"],
        "resources": {"gpus": 1, "cpus": 8, "mem": "32GB", "time": "02:00:00"},
        "params": {"dropout": 0.1, "lr": 1.0e-4},
    }
    # A history of recent runs for the same recipe — drives sparkline
    for i, (off, status, dur) in enumerate([
        (3600 * 30, "succeeded", 1800),
        (3600 * 24, "succeeded", 1750),
        (3600 * 18, "failed", 240),
        (3600 * 12, "succeeded", 1810),
        (3600 * 8, "failed", 90),
        (3600 * 4, "succeeded", 1700),
        (3600 * 2, "running", None),
    ]):
        add_run(conn, run_id=f"run_dddd{i:04d}ablate", recipe={**recipe_quick, "params": {**recipe_quick["params"], "dropout": 0.05 + i * 0.05}},
                status=status, repo="research-side", created_offset=off, duration=dur,
                job_id=str(2148000 + i))

    # An OOM failure to show the failed pill clearly
    recipe_big = {
        "name": "scaling_search",
        "repo": "research-side",
        "command": ["python", "scripts/search.py"],
        "resources": {"gpus": 8, "cpus": 64, "mem": "1024GB", "time": "12:00:00"},
        "params": {"model_size": "70B"},
        # Legacy: this recipe predates [tracking.wandb], but the training
        # script DID call wandb.init at runtime, so the URL ends up in the
        # log file. `labctl backfill-tracking` should resolve it.
        "_legacy_wandb": ("research-side", "scaling-sweep", "xtq9k7ab"),
    }
    add_run(conn, run_id="run_eeee0001oom00", recipe=recipe_big, status="oom",
            repo="research-side", created_offset=600, duration=180, job_id="2149098")

    # An old eval run that succeeded
    add_artifact(conn, art_id="artifact_oldck_001", kind="checkpoint",
                 path="/checkpoints/lm_v2_8b_final",
                 hash_="sha256:99887766554433221100",
                 producer=None, alias="lm_v2_final", created_offset=86400 * 12)
    conn.execute(
        "INSERT INTO eval_requests VALUES (?,?,?,?,?,?,?,?)",
        ("eval_mmlu_v2", "artifact_oldck_001", "sha256:eval01",
         "mmlu_5shot", "run_eval_v2done", "succeeded",
         NOW - 86400 * 5, NOW - 86400 * 5),
    )
    # Eval result in the shape `_result.py` actually writes today (flat
    # `scores` dict). No `{tasks, primary}` — labctl's smart extractor
    # recognizes this structurally without any wrapper code.
    eval_result_meta = {
        "role": "result",
        "marker": "result.json",
        "producer_recipe": "eval_mmlu",
        "result": {
            "schema_version": 1,
            "task": "inspect_evals/ifeval",
            "scores": {
                "ifeval/strict_accuracy": 0.6543,
                "ifeval/loose_accuracy": 0.7218,
                "ifeval/prompt_strict_accuracy": 0.5912,
                "ifeval/prompt_loose_accuracy": 0.6480,
            },
            "params": {"temperature": 0.0, "max_tokens": 1280, "seed": 0},
            "n_samples": 542,
            "elapsed_s": 2400,
        },
    }
    add_artifact(conn, art_id="artifact_evalres_v2", kind="eval_result",
                 path="/eval_logs/run_eval_v2done",
                 hash_="sha256:evalres0001",
                 producer="run_eval_v2done", alias="lm_v2_mmlu_result",
                 created_offset=86400 * 5, metadata=eval_result_meta)
    add_run(conn, run_id="run_eval_v2done", recipe=recipe_eval, status="succeeded",
            repo="labctl-demo", created_offset=86400 * 5, duration=2400,
            job_id="2138001",
            inputs=[("ckpt", "artifact_oldck_001", "/checkpoints/lm_v2_8b_final")],
            outputs=[("result", "artifact_evalres_v2")])

    conn.commit()
    conn.close()

    # Cluster config
    cluster = f"""name = "demo"

[filesystem]
runs_base = "{RUNS_BASE}"
registry_db = "{REGISTRY}"

[filesystem.artifact_roots]
checkpoint = "{ART_BASE}/checkpoints"
dataset = "{ART_BASE}/datasets"
eval_result = "{ART_BASE}/eval_logs"

[scheduler]
kind = "slurm"

[dispatch]
reconcile_interval_secs = 60
evald_interval_secs = 300
policies_dir = "policies"
"""
    (BASE / "labctl.toml").write_text(cluster)

    print(f"seeded → {REGISTRY}")
    print(f"  runs:      {sqlite3.connect(REGISTRY).execute('SELECT count(*) FROM runs').fetchone()[0]}")
    print(f"  artifacts: {sqlite3.connect(REGISTRY).execute('SELECT count(*) FROM artifacts').fetchone()[0]}")
    print(f"  pipelines: {sqlite3.connect(REGISTRY).execute('SELECT count(*) FROM pipelines').fetchone()[0]}")
    print(f"  evals:     {sqlite3.connect(REGISTRY).execute('SELECT count(*) FROM eval_requests').fetchone()[0]}")
    print()
    print(f"  cargo run --features ui --release -- --cluster {BASE}/labctl.toml serve")


if __name__ == "__main__":
    sys.exit(main())
