#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || "$1" != /* ]]; then
  echo "usage: $0 /absolute/path/to/deployed/tooling" >&2
  exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
deployed_dir="$1"

if [[ ! -d "$deployed_dir" || -L "$deployed_dir" ]]; then
  echo "retention deployment directory must be a real directory: $deployed_dir" >&2
  exit 1
fi

for name in retention_report.py retention_rules.toml; do
  deployed="$deployed_dir/$name"
  if [[ ! -f "$deployed" || -L "$deployed" ]]; then
    echo "retention deployment is missing a regular $name: $deployed" >&2
    exit 1
  fi
  if ! cmp -s -- "$repo_dir/$name" "$deployed"; then
    echo "retention deployment differs from the repository: $deployed" >&2
    sha256sum -- "$repo_dir/$name" "$deployed" >&2
    exit 1
  fi
done

sha256sum -- "$repo_dir/retention_report.py" "$repo_dir/retention_rules.toml"
