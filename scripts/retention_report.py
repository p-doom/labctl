#!/usr/bin/python3
"""Propose checkpoint retention decisions for the labctl artifact estate.

Writes a markdown report and nothing else. There is no delete path, no --apply,
no --force: the tool proposes, a human disposes. The only bytes it writes are
its own report.

Every unit starts at KEEP and only a rule that fires moves it to a candidate,
so anything the lineage walk cannot resolve stays KEEP and is counted in the
report's unknown bucket.

Login-node only, for the same reason as alert_on_failures.py: the labctl
Postgres listens on a Unix socket that is node-local to hai-login2. Sizing
(du) and the reference greps would work anywhere, but the registry half would
not, and a retention report built without lineage is worse than no report.
"""

import argparse
import csv
import io
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from datetime import datetime, timezone

Q_ARTIFACTS = """
SELECT path, coalesce(producer_run_id, ''), kind FROM artifacts
"""

Q_RUNS = """
SELECT id, recipe_name, submitted_by, status, run_dir FROM runs
"""

Q_INPUTS = """
SELECT run_id, role, resolved_path FROM run_inputs
"""

Q_ARG_PATHS = """
SELECT r.id, args.key, args.value
FROM runs r
CROSS JOIN LATERAL jsonb_each_text(coalesce(r.recipe_json -> 'args', '{}'::jsonb)) AS args
WHERE args.key IN ('model_id', 'data_path', 'val_data_path', 'checkpoint_path',
                   'base_snapshot', 'recovery_from', 'source_checkpoint_root')
  AND args.value LIKE '/fast/%'
"""

Q_OUTPUTS = """
SELECT ro.run_id, ro.role, a.path
FROM run_outputs ro JOIN artifacts a ON a.id = ro.artifact_id
"""

Q_EVAL_RUNS = """
SELECT DISTINCT ro.run_id
FROM run_outputs ro JOIN artifacts a ON a.id = ro.artifact_id
WHERE a.kind = 'eval_result'
"""

Q_EVAL_REQUESTS = """
SELECT a.path, er.policy_id, coalesce(er.eval_run_id, '')
FROM eval_requests er JOIN artifacts a ON a.id = er.checkpoint_artifact_id
"""

Q_ALIASES = """
SELECT al.alias, a.path FROM artifact_aliases al JOIN artifacts a ON a.id = al.artifact_id
"""

# Rows printed per KEEP section. Candidate sections are never truncated --
# a proposal a human cannot see is a proposal they cannot reject.
KEEP_ROWS = 40

CROWDCAST_PURE = "crowd-cast (pure)"
CROWDCAST_MIXTURE = "crowd-cast mixture"
NOT_CROWDCAST = "not crowd-cast"
UNKNOWN = "unresolved"

RUN_SUFFIX = re.compile(r"_(run_[0-9a-f]{32})$")
REPORT_HEADER = "# Checkpoint retention report -- proposals only"


def run_command(label, argv, ok=(0,), **kwargs):
    try:
        out = subprocess.run(argv, **kwargs)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("%s failed: %s" % (label, exc)) from exc
    if out.returncode not in ok:
        detail = out.stderr.decode(errors="replace").strip() if out.stderr else ""
        raise RuntimeError("%s failed (rc=%d)%s" % (
            label, out.returncode, ": " + detail if detail else ""))
    return out


class Unit:
    def __init__(self, path, user, name, nbytes, mtime):
        self.path = path
        self.user = user
        self.name = name
        self.nbytes = nbytes
        self.mtime = mtime
        self.producers = set()
        self.leaves = set()
        self.unmatched = []
        self.lineage_class = UNKNOWN
        self.lineage_note = ""
        self.refs = []
        self.decision = "KEEP"
        self.rule = "default"


def query(cfg, sql):
    env = dict(os.environ)
    env.update(
        PGHOST=cfg["pghost"],
        PGPORT=cfg["pgport"],
        PGDATABASE=cfg["pgdatabase"],
        PGOPTIONS="-c default_transaction_read_only=on",
        PGCONNECT_TIMEOUT="10",
    )
    out = run_command(
        "psql query",
        [cfg["psql"], "-X", "-q", "-t", "--csv", "-v", "ON_ERROR_STOP=1", "-f", "-"],
        env=env, input=sql.encode(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600,
    )
    try:
        return list(csv.reader(io.StringIO(out.stdout.decode())))
    except UnicodeDecodeError as exc:
        raise RuntimeError("psql returned non-UTF-8 output") from exc


def du(paths):
    """Apparent bytes and newest mtime in one stat pass per tree."""
    sizes = {}
    for chunk in [paths[i:i + 400] for i in range(0, len(paths), 400)]:
        out = run_command(
            "du sizing",
            ["du", "-sb", "--time", "--time-style=+%s", "--"] + chunk,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3600,
        )
        for line in out.stdout.decode(errors="replace").splitlines():
            try:
                nbytes, mtime, path = line.split("\t", 2)
                value = (int(nbytes), int(mtime))
            except ValueError as exc:
                raise RuntimeError("du returned an invalid row: %r" % line) from exc
            if path in sizes:
                raise RuntimeError("du returned duplicate rows for %s" % path)
            sizes[path] = value
    missing = sorted(set(paths) - set(sizes))
    unexpected = sorted(set(sizes) - set(paths))
    if missing or unexpected:
        raise RuntimeError("du coverage mismatch: %d missing, %d unexpected" % (
            len(missing), len(unexpected)))
    return sizes


def scan_units(scan):
    """Every <root>/<user>/<name> and <extra>/<name> directory that exists."""
    found = []
    namespaces = {scan["owner"], *scan["colleagues"], *scan["skip_namespaces"]}
    for root in scan["checkpoint_roots"]:
        entries = sorted(os.scandir(root), key=lambda entry: entry.name)
        unknown = [entry.name for entry in entries
                   if entry.is_dir(follow_symlinks=False) and entry.name not in namespaces]
        if unknown:
            raise RuntimeError("unconfigured checkpoint namespace(s) under %s: %s" % (
                root, ", ".join(unknown)))
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            user = entry.name
            if user in scan["skip_namespaces"]:
                continue
            for child in sorted(os.scandir(entry.path), key=lambda item: item.name):
                if child.is_dir(follow_symlinks=False):
                    found.append((child.path, user, child.name))
    for extra in scan["extra_group_dirs"]:
        for child in sorted(os.scandir(extra), key=lambda item: item.name):
            if child.is_dir(follow_symlinks=False):
                found.append((child.path, scan["owner"], child.name))

    bad_paths = [path for path, _, _ in found if "\n" in path or "\t" in path]
    if bad_paths:
        raise RuntimeError("checkpoint paths containing tabs or newlines are unsupported")
    if not found:
        raise RuntimeError("checkpoint scan found no units")
    counts = {}
    for _, _, name in found:
        counts[name] = counts.get(name, 0) + 1
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        raise RuntimeError("checkpoint unit names must be globally unique: %s" % ", ".join(duplicates))

    sizes = du([p for p, _, _ in found])
    return [Unit(p, u, n, sizes[p][0], sizes[p][1]) for p, u, n in found]


def index_by_prefix(by_path):
    """Longest-prefix lookup: a step subdir resolves to the tree that holds it."""
    def lookup(path):
        cur = path.rstrip("/")
        while len(cur) > 1:
            if cur in by_path:
                return by_path[cur]
            cur = os.path.dirname(cur)
        return None

    return lookup


def grep_fixed(patterns, files, chunk=500):
    """One serial `grep -F -o -H` pass over each file chunk."""
    if not patterns:
        raise RuntimeError("reference grep has no patterns")
    pattern_bytes = ("\n".join(patterns) + "\n").encode()
    hits = []
    for i in range(0, len(files), chunk):
        out = run_command(
            "reference grep",
            ["grep", "-a", "-F", "-f", "-", "-o", "-H", "-Z", "--"] + files[i:i + chunk],
            ok=(0, 1), input=pattern_bytes,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=3600,
        )
        payload = out.stdout
        while payload:
            raw_path, separator, remainder = payload.partition(b"\0")
            raw_match, newline, payload = remainder.partition(b"\n")
            if not separator or not newline or not raw_path or not raw_match:
                raise RuntimeError("reference grep returned an invalid row")
            try:
                match = raw_match.decode()
            except UnicodeDecodeError as exc:
                raise RuntimeError("reference grep returned a non-UTF-8 match") from exc
            hits.append((os.fsdecode(raw_path), match))
    return hits


def collect_files(roots, suffixes):
    files = []

    def walk_error(exc):
        raise exc

    for root in roots:
        for dirpath, dirs, names in os.walk(root, onerror=walk_error):
            dirs.sort()
            for name in sorted(names):
                if not name.endswith(tuple(suffixes)):
                    continue
                path = os.path.join(dirpath, name)
                os.stat(path)
                files.append(path)
    return files


def classify_leaf(path, lineage):
    if any(m in path for m in lineage["no_signal_markers"]):
        return "base"
    cc = any(m in path for m in lineage["crowdcast_markers"])
    nc = any(m in path for m in lineage["not_crowdcast_markers"])
    if cc and nc:
        return "conflict"
    if cc:
        return "crowdcast"
    if nc:
        return "not"
    return "unknown"


def resolve_lineage(unit, consumed, producer_of, lineage):
    """Walk producer->input edges until every branch reaches something nothing
    in the registry produced, then classify that leaf set.

    Recursion follows any produced path, not just checkpoints: a training set is
    itself a stage-06 run's output over a stage-04 run's output, and stopping at
    the dataset dir would classify `e2e2_..._stage_06_records_eov3_32k_split` by
    its name instead of by the corpus underneath it."""
    leaves = set()
    seen_runs = set()
    frontier = [(r, 0) for r in unit.producers]
    truncated = False
    while frontier:
        run_id, depth = frontier.pop()
        if run_id in seen_runs:
            continue
        seen_runs.add(run_id)
        if depth >= lineage["max_depth"]:
            truncated = True
            continue
        for path in consumed.get(run_id, ()):
            producers = producer_of(path)
            if producers:
                frontier.extend((r, depth + 1) for r in producers if r not in seen_runs)
            else:
                leaves.add(path)

    unit.leaves = leaves
    kinds = {classify_leaf(p, lineage) for p in leaves}
    kinds.discard("base")
    unit.unmatched = sorted(p for p in leaves if classify_leaf(p, lineage) == "unknown")
    if "conflict" in kinds:
        unit.lineage_class = UNKNOWN
        unit.lineage_note = "a leaf matches both the crowd-cast and not-crowd-cast marker lists"
        return
    if not unit.producers:
        unit.lineage_class = UNKNOWN
        unit.lineage_note = "no producing run in the registry"
        return
    if truncated:
        unit.lineage_class = UNKNOWN
        unit.lineage_note = "lineage deeper than max_depth"
        return
    if not kinds:
        unit.lineage_class = UNKNOWN
        unit.lineage_note = "producing run declares no input carrying a data signal"
        return
    # Mixture is monotone in the leaf set: once one crowd-cast and one
    # non-crowd-cast leaf are both proven, no leaf that is still unclassified
    # can un-mix the lineage. Purity and not-crowd-cast are the opposite --
    # both are claims about *every* leaf -- so a single unknown sinks them.
    if "crowdcast" in kinds and "not" in kinds:
        unit.lineage_class = CROWDCAST_MIXTURE
        if unit.unmatched:
            unit.lineage_note = "mixture proven; %d further leaf/leaves unclassified" % len(
                unit.unmatched)
        return
    if "unknown" in kinds:
        unit.lineage_class = UNKNOWN
        unit.lineage_note = "%d of %d leaves match no marker list" % (
            len(unit.unmatched), len(leaves))
        return
    unit.lineage_class = CROWDCAST_PURE if "crowdcast" in kinds else NOT_CROWDCAST


def rule_not_ours(unit, ctx):
    return unit.user in ctx["colleagues"]


def rule_recent(unit, ctx):
    return unit.mtime >= ctx["now"] - ctx["recent_days"] * 86400


def rule_referenced(unit, ctx):
    return bool(unit.refs)


def rule_crowdcast_mixture(unit, ctx):
    return unit.lineage_class == CROWDCAST_MIXTURE


def rule_name_only_crowdcast(unit, ctx):
    return unit.lineage_class == NOT_CROWDCAST and ctx["name_re"].search(unit.name) is not None


def rule_crowdcast_pure_unreferenced(unit, ctx):
    return unit.lineage_class == CROWDCAST_PURE


RULES = {
    "not_ours": (rule_not_ours, "NOT_OURS",
                 "under a colleague's namespace -- listed for completeness, never a candidate"),
    "recent": (rule_recent, "KEEP", "modified within the retention window"),
    "referenced": (rule_referenced, "KEEP", "at least one reference found by methods a-c"),
    "crowdcast_mixture": (rule_crowdcast_mixture, "DELETE",
                          "lineage mixes crowd-cast with non-crowd-cast data, and nothing references it"),
    "name_only_crowdcast": (rule_name_only_crowdcast, "DELETE",
                            "name says crowd-cast but the lineage does not, and nothing references it"),
    "crowdcast_pure_unreferenced": (rule_crowdcast_pure_unreferenced, "DELETE",
                                    "pure crowd-cast lineage with zero inbound references"),
}


# evald fans an eval policy out over every registered checkpoint step without a
# human ever naming it, so an eval reference is much weaker evidence of "someone
# wants this" than a recipe, a doc, or another training run warm-starting from
# it. The report splits them rather than reporting one undifferentiated count.
REF_STRENGTH = (
    ("named by a handoff/artifact doc", lambda r: r.startswith("(c) doc")),
    ("named by a recipe TOML", lambda r: r.startswith("(b) recipe")),
    ("has a registry alias", lambda r: r.startswith("(a) alias")),
    ("consumed by a non-eval run (warm start or export)",
     lambda r: r.startswith("(a) ") and " run `" in r and not r.startswith("(a) eval run")),
    ("consumed by an eval run", lambda r: r.startswith("(a) eval run")),
    ("queued by an evald policy", lambda r: r.startswith("(a) eval_requests")),
)


def strongest_ref(unit):
    for label, pred in REF_STRENGTH:
        if any(pred(r) for r in unit.refs):
            return label
    return "-"


def human(nbytes):
    for unit, div in (("TiB", 1 << 40), ("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if nbytes >= div:
            return "%.2f %s" % (nbytes / div, unit)
    return "%d B" % nbytes


def stamp(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def write_report(path, units, order, meta):
    by_rule = {}
    for u in units:
        by_rule.setdefault(u.rule, []).append(u)

    candidates = [u for u in units if u.decision == "DELETE"]
    not_ours = [u for u in units if u.decision == "NOT_OURS"]
    ours = [u for u in units if u.decision != "NOT_OURS"]
    unknown = [u for u in ours if u.lineage_class == UNKNOWN]

    out = []
    w = out.append
    w(REPORT_HEADER + "\n")
    w("Generated %s by `%s`.\n" % (meta["generated"], meta["tool"]))
    w("**Nothing has been deleted, moved, or modified.** This tool has no delete "
      "path. Every line below is a proposal for a human to accept or reject.\n")

    w("## Headline\n")
    w("| | Units | Apparent bytes | |")
    w("|---|---:|---:|---|")
    w("| Scanned | %d | %d | %s |" % (len(units), sum(u.nbytes for u in units),
                                      human(sum(u.nbytes for u in units))))
    w("| Ours (%s) | %d | %d | %s |" % (meta["owner"], len(ours), sum(u.nbytes for u in ours),
                                        human(sum(u.nbytes for u in ours))))
    w("| Not ours to judge | %d | %d | %s |" % (len(not_ours), sum(u.nbytes for u in not_ours),
                                                human(sum(u.nbytes for u in not_ours))))
    w("| **Delete candidates** | **%d** | **%d** | **%s** |" % (
        len(candidates), sum(u.nbytes for u in candidates), human(sum(u.nbytes for u in candidates))))
    w("| Kept, lineage unresolved | %d | %d | %s |" % (
        len(unknown), sum(u.nbytes for u in unknown), human(sum(u.nbytes for u in unknown))))
    w("")
    w("**Grand total reclaimable if every candidate below is accepted: %s** "
      "(%d bytes across %d units).\n" % (human(sum(u.nbytes for u in candidates)),
                                         sum(u.nbytes for u in candidates), len(candidates)))

    w("## How a unit was judged\n")
    w("Rules are applied in the configured order and the first match wins; a unit "
      "matching no rule stays KEEP. Protective rules run before deletion rules, so a "
      "reference or a recent mtime outranks any lineage verdict.\n")
    w("| # | Rule | Verdict | Meaning | Units | Bytes |")
    w("|---:|---|---|---|---:|---:|")
    for i, name in enumerate(order, 1):
        fn, verdict, desc = RULES[name]
        hit = by_rule.get(name, [])
        w("| %d | `%s` | %s | %s | %d | %s |" % (i, name, verdict, desc, len(hit),
                                                 human(sum(u.nbytes for u in hit))))
    default = by_rule.get("default", [])
    w("| - | `default` | KEEP | matched no rule | %d | %s |" % (
        len(default), human(sum(u.nbytes for u in default))))
    w("")
    w("Reference methods, all three run against every unit:\n")
    w("- **(a) registry** -- `run_inputs.resolved_path`, stored warm-start/data recipe args, "
      "`eval_requests.checkpoint_artifact_id`, `artifact_aliases`, and a fixed-string scan "
      "of generated `.lab/submit.sh` files (%d readable; %d absent). The producing run's "
      "own rows and its own `submit.sh` are excluded, "
      "otherwise every unit would reference itself." % (
          meta["submit_scanned"], meta["submit_missing"]))
    w("- **(b) recipes** -- fixed-string scan of %d recipe TOMLs under %s. The producing "
      "recipe's own file is excluded for the same reason." % (meta["recipe_files"],
                                                              ", ".join(meta["recipe_roots"])))
    w("- **(c) docs** -- fixed-string scan of all %d matching files under the configured "
      "handoff/artifact doc roots.\n" % meta["doc_files"])

    w("## Known limits\n")
    w("- Filesystem discovery is limited to the configured checkpoint roots and immediate "
      "children of the configured extra group directories: %s." % ", ".join(meta["scan_roots"]))
    w("- %d registry runs have no readable generated `submit.sh`. Their registered inputs and "
      "stored warm-start/data recipe args were still checked, but a path introduced only by a "
      "now-absent generated script cannot be recovered." % meta["submit_missing"])
    w("- The document scan covers only these configured text suffixes: %s. It has no size cap; "
      "every matching file was read." % ", ".join(meta["doc_suffixes"]))
    w("- %d owned units (%s) remain lineage-unresolved and therefore KEEP. Sizes are GNU `du` "
      "apparent bytes, not allocated-block usage.\n" % (
          len(unknown), human(sum(u.nbytes for u in unknown))))

    w("## Delete candidates, grouped by rule\n")
    if not candidates:
        w("None.\n")
    for name in order:
        fn, verdict, desc = RULES[name]
        if verdict != "DELETE":
            continue
        hit = sorted(by_rule.get(name, []), key=lambda u: -u.nbytes)
        w("### `%s` -- %d units, %s\n" % (name, len(hit), human(sum(u.nbytes for u in hit))))
        w("%s\n" % desc.capitalize())
        if not hit:
            w("No units matched.\n")
            continue
        w("| Unit | Size | Newest mtime | Lineage | Evidence |")
        w("|---|---:|---|---|---|")
        for u in hit:
            w("| `%s` | %s | %s | %s | %s |" % (
                u.path, human(u.nbytes), stamp(u.mtime), u.lineage_class, evidence(u)))
        w("")

    w("## Kept\n")
    for name in order:
        fn, verdict, desc = RULES[name]
        if verdict != "KEEP":
            continue
        hit = sorted(by_rule.get(name, []), key=lambda u: -u.nbytes)
        w("### `%s` -- %d units, %s\n" % (name, len(hit), human(sum(u.nbytes for u in hit))))
        if not hit:
            w("No units matched.\n")
            continue
        if name == "referenced":
            w("Split by the strongest reference found, because the kinds are not equally "
              "strong evidence: a doc or a recipe means a human named this checkpoint, "
              "while an evald policy fans out over every registered step on its own. The "
              "bottom rows are where a human looking to reclaim space should look first.\n")
            w("| Strongest reference | Units | Bytes |")
            w("|---|---:|---:|")
            strength = {}
            for u in hit:
                strength.setdefault(strongest_ref(u), []).append(u)
            for label, _ in REF_STRENGTH:
                rows = strength.get(label, [])
                if rows:
                    w("| %s | %d | %s |" % (label, len(rows), human(sum(x.nbytes for x in rows))))
            w("")
        w("| Unit | Size | Newest mtime | Lineage | Why kept |")
        w("|---|---:|---|---|---|")
        for u in hit[:KEEP_ROWS]:
            w("| `%s` | %s | %s | %s | %s |" % (
                u.path, human(u.nbytes), stamp(u.mtime), u.lineage_class, evidence(u)))
        if len(hit) > KEEP_ROWS:
            w("| ... %d more | %s | | | |" % (len(hit) - KEEP_ROWS,
                                              human(sum(u.nbytes for u in hit[KEEP_ROWS:]))))
        w("")

    w("### `default` -- matched no rule, %d units, %s\n" % (
        len(default), human(sum(u.nbytes for u in default))))
    w("Unreferenced and not recent, but no DELETE rule claimed them. Two different "
      "situations share this bucket, and they call for different follow-ups:\n")
    w("| Lineage | Units | Bytes | Why no rule fired |")
    w("|---|---:|---:|---|")
    for cls, why in ((NOT_CROWDCAST, "resolved, and the seeded policy only targets crowd-cast lineages -- "
                                     "reclaimable in principle, but not under a rule franz has stated"),
                     (CROWDCAST_PURE, "pure crowd-cast, so `crowdcast_pure_unreferenced` should have "
                                      "claimed it; if this row is non-empty that rule is disabled"),
                     (CROWDCAST_MIXTURE, "mixture, so `crowdcast_mixture` should have claimed it; if this "
                                         "row is non-empty that rule is disabled"),
                     (UNKNOWN, "the lineage walk could not classify it -- KEEP because the tool does "
                               "not know, not because it knows the checkpoint is wanted")):
        rows = [u for u in default if u.lineage_class == cls]
        if rows:
            w("| %s | %d | %s | %s |" % (cls, len(rows), human(sum(u.nbytes for u in rows)), why))
    w("")
    if default:
        w("| Unit | Size | Newest mtime | Lineage | Note |")
        w("|---|---:|---|---|---|")
        for u in sorted(default, key=lambda u: -u.nbytes)[:KEEP_ROWS]:
            w("| `%s` | %s | %s | %s | %s |" % (
                u.path, human(u.nbytes), stamp(u.mtime), u.lineage_class,
                u.lineage_note or "-"))
        if len(default) > KEEP_ROWS:
            w("| ... %d more | %s | | | |" % (len(default) - KEEP_ROWS,
                                              human(sum(u.nbytes for u in default[KEEP_ROWS:]))))
        w("")

    w("## Unknown bucket\n")
    w("%d of %d units we own (%s of %s) have an unresolved lineage. Every one is "
      "KEEP. Breakdown by why:\n" % (len(unknown), len(ours),
                                     human(sum(u.nbytes for u in unknown)),
                                     human(sum(u.nbytes for u in ours))))
    reasons = {}
    for u in unknown:
        reasons.setdefault(u.lineage_note or "-", []).append(u)
    w("| Reason | Units | Bytes |")
    w("|---|---:|---:|")
    for reason, hit in sorted(reasons.items(), key=lambda kv: -sum(u.nbytes for u in kv[1])):
        w("| %s | %d | %s |" % (reason, len(hit), human(sum(u.nbytes for u in hit))))
    w("")
    blocking = {}
    for u in unknown:
        for leaf in u.unmatched:
            blocking.setdefault(leaf, []).append(u)
    if blocking:
        w("The unresolved lineages are blocked by %d distinct leaf paths that match no "
          "marker list. Classifying these in `[lineage]` of the rules file is the single "
          "lever that shrinks this bucket; the tool will not guess them.\n" % len(blocking))
        w("| Leaf path matching no marker | Units blocked | Bytes blocked |")
        w("|---|---:|---:|")
        for leaf, hit in sorted(blocking.items(), key=lambda kv: -sum(u.nbytes for u in kv[1]))[:40]:
            w("| `%s` | %d | %s |" % (leaf, len(hit), human(sum(u.nbytes for u in hit))))
        w("")

    w("## Not ours to judge\n")
    w("%d units under colleague namespaces (%s), %s. Listed so the estate total is "
      "honest; never candidates regardless of lineage or references.\n" % (
          len(not_ours), ", ".join(meta["colleagues"]), human(sum(u.nbytes for u in not_ours))))
    w("| Namespace | Units | Bytes |")
    w("|---|---:|---:|")
    per_user = {}
    for u in not_ours:
        per_user.setdefault(u.user, []).append(u)
    for user, hit in sorted(per_user.items(), key=lambda kv: -sum(u.nbytes for u in kv[1])):
        w("| `%s` | %d | %s |" % (user, len(hit), human(sum(u.nbytes for u in hit))))
    w("")
    w("<details><summary>Full list</summary>\n")
    w("| Unit | Size | Newest mtime |")
    w("|---|---:|---|")
    for u in sorted(not_ours, key=lambda u: -u.nbytes):
        w("| `%s` | %s | %s |" % (u.path, human(u.nbytes), stamp(u.mtime)))
    w("\n</details>\n")

    os.umask(0o007)
    parent = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix=".retention_report.", dir=parent, text=True)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write("\n".join(out) + "\n")
        os.chmod(tmp, 0o660)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def evidence(unit):
    if not unit.refs:
        return "none found by methods a-c"
    shown = unit.refs[:4]
    text = "; ".join(shown)
    if len(unit.refs) > len(shown):
        text += "; +%d more" % (len(unit.refs) - len(shown))
    return text.replace("|", "\\|")


def validate_config(rules, out_path):
    sections = {"scan", "registry", "references", "lineage", "policy"}
    if set(rules) != sections:
        raise ValueError("rules must contain exactly these sections: %s" % ", ".join(sorted(sections)))

    schemas = {
        "scan": {"checkpoint_roots", "extra_group_dirs", "owner", "colleagues", "skip_namespaces"},
        "registry": {"psql", "pghost", "pgport", "pgdatabase"},
        "references": {"run_dir_marker", "recipe_roots", "recipe_suffixes", "doc_roots",
                       "doc_suffixes"},
        "lineage": {"crowdcast_markers", "not_crowdcast_markers", "no_signal_markers",
                    "max_depth"},
        "policy": {"recent_days", "name_crowdcast_pattern", "enabled_rules"},
    }
    for section, keys in schemas.items():
        actual = set(rules[section])
        if actual != keys:
            raise ValueError("[%s] must contain exactly: %s" % (section, ", ".join(sorted(keys))))

    def string_list(section, key, allow_empty=False):
        value = rules[section][key]
        if (not isinstance(value, list) or (not value and not allow_empty)
                or any(not isinstance(item, str) or not item for item in value)):
            raise ValueError("[%s].%s must be %sa list of non-empty strings" % (
                section, key, "empty or " if allow_empty else ""))
        if len(value) != len(set(value)):
            raise ValueError("[%s].%s contains duplicates" % (section, key))
        return value

    scan = rules["scan"]
    owner = scan["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("[scan].owner must be a non-empty string")
    colleagues = string_list("scan", "colleagues")
    skip_namespaces = string_list("scan", "skip_namespaces", allow_empty=True)
    if owner in colleagues or owner in skip_namespaces or set(colleagues) & set(skip_namespaces):
        raise ValueError("owner, colleagues, and skipped namespaces must be disjoint")

    directory_keys = (("scan", "checkpoint_roots"), ("scan", "extra_group_dirs"),
                      ("references", "recipe_roots"), ("references", "doc_roots"))
    configured_dirs = []
    for section, key in directory_keys:
        paths = string_list(section, key, allow_empty=(key == "extra_group_dirs"))
        for path in paths:
            if not os.path.isabs(path):
                raise ValueError("[%s].%s path is not absolute: %s" % (section, key, path))
            if not os.path.isdir(path):
                raise ValueError("[%s].%s directory does not exist: %s" % (section, key, path))
            configured_dirs.append(os.path.realpath(path))
    if len(configured_dirs) != len(set(configured_dirs)):
        raise ValueError("configured directory roots must be distinct")

    refs = rules["references"]
    marker = refs["run_dir_marker"]
    if (not isinstance(marker, str) or not marker or os.path.isabs(marker)
            or os.path.normpath(marker).startswith("..")):
        raise ValueError("[references].run_dir_marker must be a relative path within each run dir")
    for key in ("recipe_suffixes", "doc_suffixes"):
        suffixes = string_list("references", key)
        if any(not suffix.startswith(".") for suffix in suffixes):
            raise ValueError("[references].%s entries must start with a dot" % key)

    registry = rules["registry"]
    for key in ("psql", "pghost", "pgport", "pgdatabase"):
        if not isinstance(registry[key], str) or not registry[key]:
            raise ValueError("[registry].%s must be a non-empty string" % key)
    if not os.path.isabs(registry["psql"]) or not os.access(registry["psql"], os.X_OK):
        raise ValueError("[registry].psql is not an executable absolute path")
    if not os.path.isabs(registry["pghost"]):
        raise ValueError("[registry].pghost must be absolute")

    lineage = rules["lineage"]
    marker_lists = [string_list("lineage", key) for key in (
        "crowdcast_markers", "not_crowdcast_markers", "no_signal_markers")]
    if any(set(left) & set(right) for i, left in enumerate(marker_lists)
           for right in marker_lists[i + 1:]):
        raise ValueError("lineage marker lists must be disjoint")
    if (not isinstance(lineage["max_depth"], int) or isinstance(lineage["max_depth"], bool)
            or lineage["max_depth"] <= 0):
        raise ValueError("[lineage].max_depth must be a positive integer")

    policy = rules["policy"]
    if (not isinstance(policy["recent_days"], int) or isinstance(policy["recent_days"], bool)
            or policy["recent_days"] <= 0):
        raise ValueError("[policy].recent_days must be a positive integer")
    if not isinstance(policy["name_crowdcast_pattern"], str):
        raise ValueError("[policy].name_crowdcast_pattern must be a string")
    try:
        re.compile(policy["name_crowdcast_pattern"])
    except re.error as exc:
        raise ValueError("invalid [policy].name_crowdcast_pattern: %s" % exc) from exc
    order = string_list("policy", "enabled_rules")
    unknown_rules = [name for name in order if name not in RULES]
    if unknown_rules:
        raise ValueError("unknown enabled rule(s): %s" % ", ".join(unknown_rules))
    protective = ("not_ours", "recent", "referenced")
    if any(name not in order for name in protective):
        raise ValueError("not_ours, recent, and referenced are mandatory protective rules")
    deletes = [name for name in order if RULES[name][1] == "DELETE"]
    if order[0] != "not_ours" or any(order.index(name) > order.index(delete)
                                     for name in protective for delete in deletes):
        raise ValueError("protective rules must precede every DELETE rule, with not_ours first")

    if not os.path.isabs(out_path) or not out_path.endswith(".md"):
        raise ValueError("--out must be an absolute .md path")
    parent = os.path.dirname(out_path)
    if not os.path.isdir(parent):
        raise ValueError("--out parent directory does not exist: %s" % parent)
    if os.path.islink(out_path):
        raise ValueError("--out must not be a symlink")
    real_out = os.path.realpath(out_path)
    for root in configured_dirs:
        if os.path.commonpath((real_out, root)) == root:
            raise ValueError("--out must not be inside a configured estate or reference root")
    if os.path.exists(out_path):
        if not os.path.isfile(out_path):
            raise ValueError("--out exists and is not a regular file")
        with open(out_path) as fh:
            if fh.readline().rstrip("\n") != REPORT_HEADER:
                raise ValueError("refusing to overwrite a file that is not a retention report")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rules", required=True, help="path to retention_rules.toml")
    ap.add_argument("--out", required=True, help="path to write the markdown report")
    args = ap.parse_args()

    with open(args.rules, "rb") as fh:
        rules = tomllib.load(fh)
    validate_config(rules, args.out)
    order = rules["policy"]["enabled_rules"]

    started = time.time()
    scan = rules["scan"]
    lineage = rules["lineage"]
    refs_cfg = rules["references"]

    def log(msg):
        sys.stderr.write("[%6.1fs] %s\n" % (time.time() - started, msg))

    log("reading registry")
    reg_cfg = rules["registry"]
    runs = {r[0]: r for r in query(reg_cfg, Q_RUNS)}
    eval_runs = {r[0] for r in query(reg_cfg, Q_EVAL_RUNS)}

    log("scanning checkpoint trees")
    units = scan_units(scan)
    log("%d units, %s apparent" % (len(units), human(sum(u.nbytes for u in units))))
    lookup = index_by_prefix({u.path: u for u in units})

    produced = {}
    for path, producer, _ in query(reg_cfg, Q_ARTIFACTS):
        if producer:
            produced.setdefault(path, set()).add(producer)
    for run_id, role, path in query(reg_cfg, Q_OUTPUTS):
        produced.setdefault(path, set()).add(run_id)
    # A run's own directory is its output too: warm starts that name
    # <run_dir>/orbax reach the producing run by no other route.
    for run in runs.values():
        produced.setdefault(run[4], set()).add(run[0])
    producer_of = index_by_prefix(produced)

    for path in list(produced):
        u = lookup(path)
        if u is not None:
            u.producers |= produced[path]
    # A checkpoint tree is named <recipe>_run_<run id>. Where the artifacts
    # table has no row -- the run died before registering, or the tree predates
    # the artifact -- that suffix is still an exact producer edge, so validate
    # it against the runs table rather than discarding the unit as unresolvable.
    recovered = 0
    for u in units:
        if u.producers:
            continue
        m = RUN_SUFFIX.search(u.name)
        if m and m.group(1) in runs:
            u.producers.add(m.group(1))
            recovered += 1
    # producer_of closes over this dict, so every producer edge must land in it:
    # a unit whose producer is only known from its name is otherwise a dead end
    # when some *other* unit warm-started from it, and that other unit's lineage
    # stops at a checkpoint path instead of reaching the corpus underneath.
    for u in units:
        if u.producers:
            produced.setdefault(u.path, set()).update(u.producers)
    log("%d runs, %d eval runs, %d units with a producer (%d recovered from the name suffix)"
        % (len(runs), len(eval_runs), sum(1 for u in units if u.producers), recovered))

    consumed = {}
    for run_id, role, path in query(reg_cfg, Q_INPUTS):
        consumed.setdefault(run_id, set()).add(path)
    for run_id, key, path in query(reg_cfg, Q_ARG_PATHS):
        consumed.setdefault(run_id, set()).add(path)

    log("scanning %d run submit.sh files" % len(runs))
    names = sorted({u.name for u in units})
    submit_paths = [os.path.join(r[4], refs_cfg["run_dir_marker"]) for r in runs.values()]
    submits = []
    for path in submit_paths:
        try:
            mode = os.stat(path).st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(mode):
            raise RuntimeError("run submit marker is not a regular file: %s" % path)
        submits.append(path)
    submit_hits = grep_fixed(names, submits)
    log("%d submit.sh readable, %d name hits" % (len(submits), len(submit_hits)))

    by_name = {}
    for u in units:
        by_name.setdefault(u.name, []).append(u)
    run_of_dir = {r[4]: r[0] for r in runs.values()}

    for path, name in submit_hits:
        run_id = run_of_dir.get(os.path.dirname(os.path.dirname(path)))
        if run_id is None:
            continue
        for u in by_name.get(name, ()):
            consumed.setdefault(run_id, set()).add(u.path)

    log("resolving lineage")
    for u in units:
        resolve_lineage(u, consumed, producer_of, lineage)

    log("collecting references")
    for run_id, paths in consumed.items():
        run = runs.get(run_id)
        if run is None:
            continue
        for path in paths:
            u = lookup(path)
            if u is None or run_id in u.producers:
                continue
            kind = "eval run" if run_id in eval_runs else "%s run" % run[3]
            u.refs.append("(a) %s `%s` (%s)" % (kind, run[1], run_id))
    for path, policy_id, eval_run_id in query(reg_cfg, Q_EVAL_REQUESTS):
        u = lookup(path)
        if u is not None:
            u.refs.append("(a) eval_requests policy `%s`" % policy_id)
    for alias, path in query(reg_cfg, Q_ALIASES):
        u = lookup(path)
        if u is not None:
            u.refs.append("(a) alias `%s`" % alias)

    producer_recipes = {}
    for u in units:
        for run_id in u.producers:
            run = runs.get(run_id)
            if run is not None:
                producer_recipes.setdefault(u.name, set()).add(run[1])

    recipe_files = collect_files(refs_cfg["recipe_roots"], refs_cfg["recipe_suffixes"])
    if not recipe_files:
        raise RuntimeError("recipe scan found no matching files")
    log("scanning %d recipe files" % len(recipe_files))
    for path, name in grep_fixed(names, recipe_files):
        stem = os.path.splitext(os.path.basename(path))[0]
        if stem in producer_recipes.get(name, ()):
            continue
        for u in by_name.get(name, ()):
            u.refs.append("(b) recipe `%s`" % path)

    doc_files = collect_files(refs_cfg["doc_roots"], refs_cfg["doc_suffixes"])
    if not doc_files:
        raise RuntimeError("document scan found no matching files")
    log("scanning %d doc files" % len(doc_files))
    for path, name in grep_fixed(names, doc_files):
        for u in by_name.get(name, ()):
            u.refs.append("(c) doc `%s`" % path)

    for u in units:
        u.refs = sorted(set(u.refs))

    ctx = {
        "colleagues": set(scan["colleagues"]),
        "recent_days": rules["policy"]["recent_days"],
        "name_re": re.compile(rules["policy"]["name_crowdcast_pattern"]),
        "now": int(started),
    }
    for u in units:
        for name in order:
            fn, verdict, _ = RULES[name]
            if fn(u, ctx):
                u.decision = verdict
                u.rule = name
                break

    write_report(args.out, units, order, {
        "generated": datetime.fromtimestamp(started, timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "tool": os.path.abspath(__file__),
        "owner": scan["owner"],
        "colleagues": scan["colleagues"],
        "scan_roots": scan["checkpoint_roots"] + scan["extra_group_dirs"],
        "recipe_roots": refs_cfg["recipe_roots"],
        "submit_scanned": len(submits),
        "submit_missing": len(runs) - len(submits),
        "recipe_files": len(recipe_files),
        "doc_files": len(doc_files),
        "doc_suffixes": refs_cfg["doc_suffixes"],
    })
    log("wrote %s" % args.out)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, tomllib.TOMLDecodeError) as exc:
        sys.exit("retention_report: %s" % exc)
