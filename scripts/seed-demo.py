#!/usr/bin/env python3
"""Seed a filesystem-truth labctl registry for UI smoke testing.

Writes a self-contained registry under /tmp/labctl-demo/ with a
handful of runs, one artifact, and an alias. `labctl serve` walks
this tree at startup and builds an in-memory index from it.

Run:
  python3 scripts/seed-demo.py
  target/release/labctl --cluster /tmp/labctl-demo/labctl.toml serve

Then SSH-tunnel to 127.0.0.1:8765 and open the UI.

This is not a fidelity demo (no W&B banners, no eval requests, no
pipelines). It's a five-fixture smoke test: enough to verify the
server boots, parses the tree, and renders the runs/artifacts views.
"""
import json
import shutil
import sys
import time
import uuid
from pathlib import Path

BASE = Path("/tmp/labctl-demo")
USER = "demo"
NOW = int(time.time())


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def run_sidecar(run_id, recipe_name, status, created_offset, duration):
    created = NOW - created_offset
    finished = (created + duration) if status in ("succeeded", "failed") else None
    run_dir = BASE / "runs" / USER / run_id
    return {
        "id": run_id,
        "recipe_name": recipe_name,
        "recipe_hash": "deadbeef" + run_id[:8],
        "repo": "myrepo",
        "run_dir": str(run_dir),
        "source_path": str(run_dir / "source" / "myrepo"),
        "created_at": created,
        "submitted_by": USER,
        "recipe": {
            "name": recipe_name,
            "repo": "myrepo",
            "command": ["python", "scripts/train.py"],
        },
        "context": {"cluster": "demo", "host": "demo-login"},
        "status": status,
        "job_id": f"100{run_id[-4:]}",
        "finished_at": finished,
    }


def seed():
    if BASE.exists():
        shutil.rmtree(BASE)
    BASE.mkdir(parents=True)

    # Three runs across statuses.
    runs = [
        ("run_aaaaaaaa", "train_example", "succeeded", 3600, 1800),
        ("run_bbbbbbbb", "eval_example",  "succeeded", 1800, 600),
        ("run_cccccccc", "train_example", "failed",   2400, 900),
        ("run_dddddddd", "train_example", "running",  300,  0),
    ]
    for run_id, recipe, status, off, dur in runs:
        sidecar = run_sidecar(run_id, recipe, status, off, dur)
        write_json(BASE / "runs" / USER / run_id / ".lab" / "run.json", sidecar)
        # Plant a small log so the UI's log view has something to show.
        log_path = BASE / "runs" / USER / run_id / ".lab" / f"{recipe}.log"
        log_path.write_text(
            f"[{time.strftime('%H:%M:%S')}] starting {recipe}\n"
            f"[{time.strftime('%H:%M:%S')}] status: {status}\n"
        )

    # Pre-create the artifact root subdirs so `labctl doctor` reports
    # them as writable. Without this, doctor flags missing roots.
    for kind in ("checkpoint", "dataset", "eval_logs"):
        (BASE / "artifacts" / kind).mkdir(parents=True, exist_ok=True)

    # One artifact + alias, produced by the first succeeded run.
    artifact_id = str(uuid.uuid4())
    artifact_alias = "demo_checkpoint_v1"
    artifact_dir = BASE / "artifacts" / "checkpoint" / USER / artifact_alias
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "checkpoint.bin").write_bytes(b"fake-checkpoint-bytes")
    write_json(artifact_dir / ".meta.json", {
        "id":              artifact_id,
        "kind":            "checkpoint",
        "user":            USER,
        "alias":           artifact_alias,
        "content_hash":    "sha256:" + "a" * 64,
        "producer_run_id": "run_aaaaaaaa",
        "metadata":        {"step": 1000},
        "created_at":      NOW - 1800,
    })
    write_json(BASE / "aliases" / artifact_alias / ".target.json", {
        "artifact_id":   artifact_id,
        "artifact_path": str(artifact_dir),
        "created_at":    NOW - 1800,
    })

    # cluster.toml the labctl binary will load.
    (BASE / "labctl.toml").write_text(f"""\
name = "demo"

[filesystem]
runs_base = "{BASE}"

[filesystem.artifact_roots]
checkpoint = "{BASE}/artifacts/checkpoint"
dataset = "{BASE}/artifacts/dataset"
eval_result = "{BASE}/artifacts/eval_logs"

[filesystem.output_roots]
checkpoint_stream = "{BASE}/artifacts/checkpoint"
eval_result = "{BASE}/artifacts/eval_logs"

[scheduler]
kind = "local"
""")

    print(f"seeded → {BASE}")
    print(f"  runs: {len(runs)} ({USER}/{', '.join(r[0] for r in runs)})")
    print(f"  artifacts: 1 (alias: {artifact_alias})")
    print()
    print("Boot the UI:")
    print(f"  target/release/labctl --cluster {BASE}/labctl.toml serve")


if __name__ == "__main__":
    sys.exit(seed())
