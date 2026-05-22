#!/usr/bin/env python3
"""
One-shot importer: walks labctl's existing FS sidecars and loads the data
into a Postgres database created by migrations/0001_initial_schema.sql.

Output strategy: emits per-table TSV files into a working dir, then runs
`psql \\copy table FROM 'file.tsv'` for each. Bulk loading is meaningfully
faster than per-row INSERTs at the events-table scale (~19k rows).

Idempotent re-run by design: the script TRUNCATEs each table before
loading. This is intended only for the one-shot cutover, not for
incremental sync.

Usage:
    python3 scripts/import-to-pg.py \\
        --cluster ~/.config/labctl/cluster.toml \\
        --pg-socket /fast/project/.../postgres/run \\
        --pg-db labctl \\
        [--workdir /tmp/labctl-import] \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore


LAB_DIRNAME = ".lab"
RUN_JSON = "run.json"
INPUTS_JSON = "inputs.json"
OUTPUTS_JSON = "outputs.json"
TRACKING_JSON = "tracking.json"
PIPELINE_JSON = "pipeline.json"
EVAL_REQUEST_JSON = "request.json"
ALIAS_TARGET = ".target.json"
META_FILENAME = ".meta.json"
OBJECTS_DIR = "_objects"


# Per-table TSV writers. We sidestep PG's default DELIMITER vs. JSON
# field issues by using TSV with explicit field escapes — JSON columns
# go in as TEXT and get cast on the PG side via JSONB.
PG_NULL = "\\N"


def tsv_escape(v: Any) -> str:
    """Escape a value for PG \\copy TSV format."""
    if v is None:
        return PG_NULL
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    return (
        s.replace("\\", "\\\\")
         .replace("\t", "\\t")
         .replace("\n", "\\n")
         .replace("\r", "\\r")
    )


def json_field(v: Any) -> str:
    """A value destined for a JSONB column: serialize to JSON, then escape."""
    return tsv_escape(json.dumps(v, separators=(",", ":")))


@dataclass
class Counts:
    users: int = 0
    runs: int = 0
    pipelines: int = 0
    artifacts: int = 0
    artifact_aliases: int = 0
    run_inputs: int = 0
    run_outputs: int = 0
    eval_requests: int = 0
    tracking: int = 0
    events: int = 0
    # name -> earliest created_at observed. Populated by every emit_*
    # that touches a user-bearing sidecar. Written out as users.tsv
    # before the COPY phase so the FKs added in 0003 resolve.
    user_first_seen: dict[str, int] = field(default_factory=dict)


def observe_user(counts: Counts, name: str | None, ts: int | None) -> None:
    if not name:
        return
    cur = counts.user_first_seen.get(name)
    if ts is None:
        # Anchor to a sentinel that loses every MIN; gets clobbered by
        # any real timestamp on a subsequent observation.
        ts = 1 << 62
    if cur is None or ts < cur:
        counts.user_first_seen[name] = ts


def load_cluster(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def read_json_optional(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ! cannot read {path}: {e}", file=sys.stderr)
        return None


# ---------- per-table extractors ----------


def emit_runs_and_associated(
    runs_base: Path, w_runs, w_inputs, w_outputs, w_tracking, counts: Counts
):
    runs_root = runs_base / "runs"
    if not runs_root.is_dir():
        return
    for user_dir in sorted(runs_root.iterdir()):
        if not user_dir.is_dir():
            continue
        for run_dir in sorted(user_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            lab = run_dir / LAB_DIRNAME
            sidecar = read_json_optional(lab / RUN_JSON)
            if sidecar is None:
                continue
            # runs row
            w_runs.writerow([
                tsv_escape(sidecar["id"]),
                tsv_escape(sidecar["recipe_name"]),
                tsv_escape(sidecar["recipe_hash"]),
                tsv_escape(sidecar.get("status", "created")),
                tsv_escape(sidecar.get("job_id")),
                tsv_escape(sidecar["run_dir"]),
                tsv_escape(sidecar["repo"]),
                tsv_escape(sidecar["source_path"]),
                json_field(sidecar["recipe"]),
                json_field(sidecar["context"]),
                tsv_escape(sidecar["created_at"]),
                tsv_escape(sidecar.get("finished_at")),
                tsv_escape(sidecar.get("pipeline_id")),
                tsv_escape(
                    json.dumps(sidecar["dependency_on"])
                    if sidecar.get("dependency_on") is not None
                    else None
                ),
                tsv_escape(sidecar.get("stage_name")),
                tsv_escape(sidecar["submitted_by"]),
                tsv_escape(sidecar.get("cache_key")),
            ])
            counts.runs += 1
            observe_user(counts, sidecar.get("submitted_by"), sidecar.get("created_at"))

            # inputs.json
            inputs = read_json_optional(lab / INPUTS_JSON) or []
            for inp in inputs:
                w_inputs.writerow([
                    tsv_escape(sidecar["id"]),
                    tsv_escape(inp["role"]),
                    tsv_escape(inp.get("artifact_id")),
                    tsv_escape(inp["resolved_path"]),
                ])
                counts.run_inputs += 1

            # outputs.json
            outputs = read_json_optional(lab / OUTPUTS_JSON) or []
            for out in outputs:
                w_outputs.writerow([
                    tsv_escape(sidecar["id"]),
                    tsv_escape(out["role"]),
                    tsv_escape(out["artifact_id"]),
                ])
                counts.run_outputs += 1

            # tracking.json
            tracking = read_json_optional(lab / TRACKING_JSON)
            if tracking is not None:
                w_tracking.writerow([
                    tsv_escape(sidecar["id"]),
                    tsv_escape(tracking["entity"]),
                    tsv_escape(tracking["project"]),
                    tsv_escape(tracking["url"]),
                    tsv_escape(tracking.get("group_name")),
                    tsv_escape(tracking["source"]),
                    tsv_escape(tracking["created_at"]),
                ])
                counts.tracking += 1


def emit_pipelines(runs_base: Path, w_pipelines, counts: Counts):
    root = runs_base / "pipelines"
    if not root.is_dir():
        return
    for user_dir in sorted(root.iterdir()):
        if not user_dir.is_dir():
            continue
        for pipeline_dir in sorted(user_dir.iterdir()):
            if not pipeline_dir.is_dir():
                continue
            sidecar = read_json_optional(pipeline_dir / PIPELINE_JSON)
            if sidecar is None:
                continue
            w_pipelines.writerow([
                tsv_escape(sidecar["id"]),
                tsv_escape(sidecar["name"]),
                tsv_escape(sidecar.get("pipeline_path")),
                tsv_escape(sidecar["user"]),
                tsv_escape(sidecar["created_at"]),
            ])
            observe_user(counts, sidecar.get("user"), sidecar.get("created_at"))
            counts.pipelines += 1


def emit_artifacts(
    artifact_roots: dict[str, Path], w_artifacts, counts: Counts
):
    """Walk the legacy `_objects/<prefix>/<hash>/` trees populated by the
    pre-c1d31e8 content-addressed code and emit one `artifacts` row per
    sidecar. `<root>/<user>/<alias>/` (the post-c1d31e8 layout) is walked
    via the run's `outputs.json` sidecars at run-import time, not here —
    the producer's own bookkeeping is the canonical source for that
    layout. The legacy sidecar's `content_hash` / `alias` fields are
    dropped on the floor; migration 0006 drops those columns.
    """
    seen_roots = set()
    for _kind, root in artifact_roots.items():
        if root in seen_roots:
            continue
        seen_roots.add(root)

        objects_root = root / OBJECTS_DIR
        if not objects_root.is_dir():
            continue
        for prefix_dir in sorted(objects_root.iterdir()):
            if not prefix_dir.is_dir():
                continue
            for hash_dir in sorted(prefix_dir.iterdir()):
                if not hash_dir.is_dir():
                    continue
                sidecar = read_json_optional(hash_dir / META_FILENAME)
                if sidecar is None:
                    continue
                w_artifacts.writerow([
                    tsv_escape(sidecar["id"]),
                    tsv_escape(sidecar["kind"]),
                    tsv_escape(str(hash_dir)),
                    tsv_escape(sidecar.get("producer_run_id")),
                    json_field(sidecar["metadata"]),
                    tsv_escape(sidecar["created_at"]),
                    tsv_escape(sidecar["user"]),
                ])
                counts.artifacts += 1
                observe_user(counts, sidecar.get("user"), sidecar.get("created_at"))


def emit_global_aliases(runs_base: Path, w_aliases, counts: Counts):
    root = runs_base / "aliases"
    if not root.is_dir():
        return
    for alias_dir in sorted(root.iterdir()):
        if not alias_dir.is_dir():
            continue
        sidecar = read_json_optional(alias_dir / ALIAS_TARGET)
        if sidecar is None:
            continue
        w_aliases.writerow([
            tsv_escape(alias_dir.name),
            tsv_escape(sidecar["artifact_id"]),
            tsv_escape(sidecar["created_at"]),
        ])
        counts.artifact_aliases += 1


def emit_eval_requests(runs_base: Path, w_eval, counts: Counts):
    root = runs_base / "eval_state"
    if not root.is_dir():
        return
    for user_dir in sorted(root.iterdir()):
        if not user_dir.is_dir():
            continue
        for key_dir in sorted(user_dir.iterdir()):
            if not key_dir.is_dir():
                continue
            sidecar = read_json_optional(key_dir / EVAL_REQUEST_JSON)
            if sidecar is None:
                continue
            w_eval.writerow([
                tsv_escape(sidecar["eval_key"]),
                tsv_escape(sidecar["checkpoint_artifact_id"]),
                tsv_escape(sidecar["eval_recipe_hash"]),
                tsv_escape(sidecar["policy_id"]),
                tsv_escape(sidecar.get("eval_run_id")),
                tsv_escape(sidecar["state"]),
                tsv_escape(sidecar["attempts"]),
                tsv_escape(user_dir.name),
                tsv_escape(sidecar["created_at"]),
                tsv_escape(sidecar["updated_at"]),
            ])
            counts.eval_requests += 1
            observe_user(counts, user_dir.name, sidecar.get("created_at"))


def emit_events(runs_base: Path, w_events, counts: Counts):
    root = runs_base / "events"
    if not root.is_dir():
        return
    files = sorted(p for p in root.iterdir() if p.suffix == ".jsonl")
    for path in files:
        try:
            with path.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    w_events.writerow([
                        tsv_escape(ev.get("run_id")),
                        tsv_escape(ev["event_type"]),
                        json_field(ev.get("payload", {})),
                        tsv_escape(ev["created_at"]),
                    ])
                    counts.events += 1
        except OSError as e:
            print(f"  ! cannot read events file {path}: {e}", file=sys.stderr)


# ---------- driver ----------


@dataclass
class TableSpec:
    name: str
    columns: list[str]
    file: Path = field(default=Path())


TABLES = [
    # Users must be loaded FIRST: migration 0003 added FKs from
    # runs.submitted_by, pipelines."user", artifacts."user", and
    # eval_requests."user" to users.name. Every downstream COPY
    # validates against rows already present here.
    TableSpec("users", ["name", "created_at", "pg_role"]),
    TableSpec(
        "runs",
        [
            "id", "recipe_name", "recipe_hash", "status", "job_id",
            "run_dir", "repo", "source_path", "recipe_json", "context_json",
            "created_at", "finished_at", "pipeline_id", "dependency_on",
            "stage_name", "submitted_by", "cache_key",
        ],
    ),
    TableSpec("pipelines", ["id", "name", "pipeline_path", '"user"', "created_at"]),
    TableSpec(
        "artifacts",
        ["id", "kind", "path", "producer_run_id",
         "metadata_json", "created_at", '"user"'],
    ),
    TableSpec("artifact_aliases", ["alias", "artifact_id", "created_at"]),
    TableSpec("run_inputs", ["run_id", "role", "artifact_id", "resolved_path"]),
    TableSpec("run_outputs", ["run_id", "role", "artifact_id"]),
    TableSpec(
        "eval_requests",
        ["eval_key", "checkpoint_artifact_id", "eval_recipe_hash", "policy_id",
         "eval_run_id", "state", "attempts", '"user"', "created_at", "updated_at"],
    ),
    TableSpec(
        "tracking",
        ["run_id", "entity", "project", "url", "group_name", "source", "created_at"],
    ),
    TableSpec(
        "events",
        ["run_id", "event_type", "payload_json", "created_at"],
    ),
]


def run_psql(args: list[str], stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["psql", *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cluster", required=True, type=Path)
    p.add_argument(
        "--pg-socket",
        default="/fast/project/HFMI_SynergyUnit/p-doom_shared/labctl/postgres/run",
    )
    p.add_argument("--pg-db", default="labctl")
    p.add_argument(
        "--workdir",
        type=Path,
        default=Path("/tmp/labctl-import"),
        help="Per-table TSV scratch files land here. Removed on success.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Emit TSV files; don't run \\copy.")
    args = p.parse_args()

    cfg = load_cluster(args.cluster.expanduser())
    runs_base = Path(cfg["filesystem"]["runs_base"])
    artifact_roots = {
        kind: Path(p) for kind, p in cfg["filesystem"]["artifact_roots"].items()
    }

    args.workdir.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        t.file = args.workdir / f"{t.name}.tsv"

    counts = Counts()
    handles = {t.name: t.file.open("w", newline="") for t in TABLES}

    class TsvWriter:
        """Minimal TSV writer: row -> '\\t'-joined + '\\n'. All
        per-cell escaping is the caller's responsibility (see
        `tsv_escape` / `json_field`). This sidesteps the stdlib
        csv module's reluctance to write rows containing the
        delimiter even when we've already escaped them."""

        def __init__(self, handle):
            self.h = handle

        def writerow(self, row):
            self.h.write("\t".join(row))
            self.h.write("\n")

    writers = {n: TsvWriter(h) for n, h in handles.items()}
    try:
        emit_runs_and_associated(
            runs_base,
            writers["runs"],
            writers["run_inputs"],
            writers["run_outputs"],
            writers["tracking"],
            counts,
        )
        emit_pipelines(runs_base, writers["pipelines"], counts)
        emit_artifacts(
            artifact_roots,
            writers["artifacts"],
            counts,
        )
        emit_global_aliases(runs_base, writers["artifact_aliases"], counts)
        emit_eval_requests(runs_base, writers["eval_requests"], counts)
        emit_events(runs_base, writers["events"], counts)
        # Users get written last in code but COPYed first at load time;
        # they're derived from the unions of `submitted_by` / `"user"`
        # observed during the emits above.
        for name, first_seen in sorted(counts.user_first_seen.items()):
            # The sentinel (1<<62) only survives if we never saw a real
            # timestamp for this user — fall back to 0 so the row still
            # loads. (PG stores BIGINT; 0 == 1970-01-01 is fine for the
            # informational column.)
            ts = 0 if first_seen >= (1 << 60) else first_seen
            writers["users"].writerow([
                tsv_escape(name),
                tsv_escape(ts),
                tsv_escape(name),  # pg_role mirrors name on import.
            ])
            counts.users += 1
    finally:
        for h in handles.values():
            h.close()

    print(f"emitted: users={counts.users} runs={counts.runs} pipelines={counts.pipelines} "
          f"artifacts={counts.artifacts} artifact_aliases={counts.artifact_aliases} "
          f"run_inputs={counts.run_inputs} run_outputs={counts.run_outputs} "
          f"eval_requests={counts.eval_requests} tracking={counts.tracking} "
          f"events={counts.events}")

    if args.dry_run:
        print(f"(dry run — TSV files in {args.workdir}; no \\copy executed)")
        return 0

    # Single-transaction load: TRUNCATE CASCADE + \copy + setval all
    # run inside one BEGIN/COMMIT so the DEFERRABLE FKs added in 0002
    # (runs.pipeline_id, artifacts.producer_run_id, eval_requests.eval_run_id)
    # are validated only at COMMIT — i.e. after the entire graph is loaded.
    # Per-row checking would reject intra-batch cross-references during the
    # COPY of `runs`.
    #
    # CASCADE on TRUNCATE handles the runs<->pipelines cycle.
    pg_args = ["-h", args.pg_socket, "-d", args.pg_db, "-v", "ON_ERROR_STOP=1"]
    sql_path = args.workdir / "load.sql"
    with sql_path.open("w") as f:
        f.write("BEGIN;\n")
        f.write("TRUNCATE " + ", ".join(t.name for t in TABLES)
                + " RESTART IDENTITY CASCADE;\n")
        for t in TABLES:
            cols = ", ".join(t.columns)
            f.write(f"\\copy {t.name} ({cols}) FROM '{t.file}'\n")
        # Bump the events seq to max(id)+1 (so future inserts don't
        # collide with imported rows).
        f.write("SELECT setval('events_id_seq', "
                "COALESCE((SELECT MAX(id) FROM events), 1));\n")
        f.write("COMMIT;\n")
    r = run_psql(pg_args + ["-f", str(sql_path)])
    if r.returncode != 0:
        print(f"transactional load failed: {r.stderr}", file=sys.stderr)
        print(r.stdout, file=sys.stderr)
        return 1
    print(r.stdout)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
