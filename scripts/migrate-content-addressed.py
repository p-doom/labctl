#!/usr/bin/env python3
"""
M2 migration: move existing artifacts from <root>/<user>/<alias>/ to
<root>/_objects/<prefix>/<hash>/ + create per-user alias symlinks.

Idempotent and crash-safe via os.rename(2) (atomic on same FS).
Does NOT touch the labctl SQLite cache — that gets rebuilt from disk
on the next `Store::open` / `refresh_from_disk` pass. Stop the labctl
agent + serve units before running to avoid racing the rebuild.

Usage:
    python3 scripts/migrate-content-addressed.py \\
        --cluster ~/.config/labctl/cluster.toml \\
        [--dry-run]

Failure modes handled:
  - Target by-hash dir already exists (re-run after partial migration):
    treats the existing target as canonical, removes the legacy dir if
    it's safely identical, otherwise warns and skips.
  - Legacy dir has no .meta.json: skip (not a labctl artifact).
  - Sidecar lacks content_hash field: skip with warning.
  - Cross-filesystem source/target: skip with loud error (rename(2)
    returns EXDEV; would need a slow copy+verify+rm, which is out of
    scope for this script — caller is expected to keep artifact roots
    on a single filesystem).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python <3.11
    import tomli as tomllib  # type: ignore


META_FILENAME = ".meta.json"
OBJECTS_DIR = "_objects"
ALIASES_USER_DIR = "aliases"


@dataclass
class Stats:
    scanned: int = 0
    migrated: int = 0
    already_at_target: int = 0
    skipped: int = 0
    errors: int = 0


def load_cluster(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def by_hash_dir(root: Path, content_hash: str) -> Path:
    return root / OBJECTS_DIR / content_hash[:2] / content_hash


def alias_symlink(root: Path, user: str, alias: str) -> Path:
    return root / ALIASES_USER_DIR / user / alias


def relative_target(target: Path, from_dir: Path) -> Path:
    target = target.resolve()
    from_dir = from_dir.resolve()
    return Path(os.path.relpath(target, from_dir))


def migrate_one(legacy_dir: Path, root: Path, dry_run: bool, stats: Stats) -> None:
    """Migrate a single artifact dir. Idempotent."""
    sidecar_path = legacy_dir / META_FILENAME
    if not sidecar_path.is_file():
        return  # not an artifact

    stats.scanned += 1
    try:
        sidecar = json.loads(sidecar_path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ✗ {legacy_dir}: cannot read sidecar: {e}", file=sys.stderr)
        stats.errors += 1
        return

    content_hash = sidecar.get("content_hash")
    if not content_hash or len(content_hash) < 2:
        print(f"  ✗ {legacy_dir}: sidecar lacks content_hash; skipping", file=sys.stderr)
        stats.errors += 1
        return

    user = sidecar.get("user")
    alias = sidecar.get("alias")
    if not user or not alias:
        print(f"  ✗ {legacy_dir}: sidecar lacks user/alias; skipping", file=sys.stderr)
        stats.errors += 1
        return

    target = by_hash_dir(root, content_hash)

    if target == legacy_dir:
        # Already in canonical form (shouldn't happen but harmless).
        stats.already_at_target += 1
        return

    if target.exists():
        # Idempotent re-run: the bytes are already at the by-hash slot
        # because a prior migration pass already moved them. Just need
        # to make sure the alias symlink exists, and the legacy dir is
        # gone if it's a duplicate copy.
        if legacy_dir.is_dir() and not legacy_dir.is_symlink():
            print(
                f"  ! {legacy_dir}: target {target} already exists; "
                f"legacy dir was not removed by prior pass — leaving it for manual review"
            )
        ensure_alias_symlink(target, root, user, alias, dry_run)
        stats.already_at_target += 1
        return

    # Cross-filesystem check (rename(2) returns EXDEV otherwise).
    if legacy_dir.stat().st_dev != root.stat().st_dev:
        print(
            f"  ✗ {legacy_dir}: on a different filesystem from {root}; "
            f"rename(2) won't work and a slow copy is out of scope. Move "
            f"the dir tree to the artifact root's filesystem and re-run.",
            file=sys.stderr,
        )
        stats.errors += 1
        return

    target.parent.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print(f"  [dry] mv {legacy_dir} -> {target}")
        print(f"  [dry] ln -s {target} {alias_symlink(root, user, alias)}")
        stats.migrated += 1
        return

    try:
        os.rename(legacy_dir, target)
    except OSError as e:
        print(f"  ✗ rename {legacy_dir} -> {target}: {e}", file=sys.stderr)
        stats.errors += 1
        return

    ensure_alias_symlink(target, root, user, alias, dry_run=False)

    # Best-effort: clean up the now-empty user dir if it has no other
    # aliases. rmdir(2) is a no-op if non-empty.
    parent = legacy_dir.parent
    try:
        parent.rmdir()
    except OSError:
        pass

    stats.migrated += 1
    print(f"  ✓ {legacy_dir} -> {target}")


def ensure_alias_symlink(
    target: Path, root: Path, user: str, alias: str, dry_run: bool
) -> None:
    """Create or update the per-user alias symlink. Idempotent."""
    link = alias_symlink(root, user, alias)
    rel = relative_target(target, link.parent)
    if link.is_symlink():
        current = os.readlink(link)
        if Path(current) == rel:
            return
        if dry_run:
            print(f"  [dry] ln -sf {rel} {link}  # was {current}")
            return
        link.unlink()
    elif link.exists():
        # A non-symlink object is at the alias path. Surprising; leave it.
        print(
            f"  ! {link}: non-symlink object at alias path; not overwriting",
            file=sys.stderr,
        )
        return
    if dry_run:
        print(f"  [dry] ln -s {rel} {link}")
        return
    link.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(rel, link)


def migrate_root(root: Path, dry_run: bool) -> Stats:
    stats = Stats()
    if not root.is_dir():
        print(f"  ! {root}: artifact root not found; skipping")
        return stats

    print(f"Migrating {root}")
    for user_entry in sorted(root.iterdir()):
        if not user_entry.is_dir():
            continue
        if user_entry.name in (OBJECTS_DIR, ALIASES_USER_DIR):
            continue
        for alias_entry in sorted(user_entry.iterdir()):
            if alias_entry.is_symlink() or not alias_entry.is_dir():
                continue
            # Two mutually-exclusive cases: a direct artifact at
            # <root>/<user>/<alias>/.meta.json, or a streaming-
            # checkpoint root at <root>/<user>/<alias>/<step>/.meta.json
            # (the parent has no sidecar — only the per-step subdirs do).
            # If we migrate the alias_entry, it's gone afterwards; skip
            # the inner descent.
            if (alias_entry / META_FILENAME).is_file():
                migrate_one(alias_entry, root, dry_run, stats)
                continue
            if not alias_entry.is_dir():
                continue
            for step_entry in sorted(alias_entry.iterdir()):
                if step_entry.is_symlink() or not step_entry.is_dir():
                    continue
                if (step_entry / META_FILENAME).is_file():
                    migrate_one(step_entry, root, dry_run, stats)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cluster",
        required=True,
        type=Path,
        help="Path to cluster.toml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done; don't touch the filesystem.",
    )
    args = parser.parse_args()

    cfg = load_cluster(args.cluster.expanduser())
    artifact_roots = cfg.get("filesystem", {}).get("artifact_roots", {})
    if not artifact_roots:
        print("cluster.toml has no [filesystem.artifact_roots]", file=sys.stderr)
        return 1

    seen_roots: set[Path] = set()
    total = Stats()
    for kind, root_str in artifact_roots.items():
        root = Path(root_str)
        if root in seen_roots:
            continue
        seen_roots.add(root)
        stats = migrate_root(root, args.dry_run)
        total.scanned += stats.scanned
        total.migrated += stats.migrated
        total.already_at_target += stats.already_at_target
        total.skipped += stats.skipped
        total.errors += stats.errors

    print()
    print(f"scanned={total.scanned}  migrated={total.migrated}  "
          f"already_at_target={total.already_at_target}  errors={total.errors}")
    if total.errors:
        return 1
    if args.dry_run:
        print("(dry run — no changes written)")
    else:
        print("Done. Restart `labctl serve` / agent to pick up the new layout.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
