#!/usr/bin/env bash
# Install labctl: build the embedded frontend, cargo-install the
# binary, and wire up the local git hooks. Idempotent — re-run after
# pulling to refresh the installed binary.
set -euo pipefail

cd "$(dirname "$0")/.."

# Frontend: rust-embed bakes ui/dist/ into the binary at compile
# time, so the dir must exist. If npm is on PATH, build it fresh
# every time (safe — npm ci's lockfile-pin keeps it deterministic).
# If npm isn't available but ui/dist/ exists, skip — this lets users
# rebuild the binary on a host without node (login nodes, CI boxes)
# as long as the frontend was built once.
if command -v npm >/dev/null 2>&1; then
  echo "→ Building frontend (ui/dist/ is embedded into the binary)..."
  (cd ui && npm ci && npm run build)
elif [ -d ui/dist ]; then
  echo "→ npm not on PATH but ui/dist/ exists — using cached frontend."
else
  echo "✗ npm not on PATH and ui/dist/ doesn't exist." >&2
  echo "  Either:" >&2
  echo "    - activate nvm:    \`. ~/.nvm/nvm.sh && nvm use --lts\`" >&2
  echo "    - load module:     \`module load nodejs\`" >&2
  echo "    - install node:    https://nodejs.org/ or your package manager" >&2
  echo "  then re-run scripts/install.sh." >&2
  exit 1
fi

echo "→ Installing labctl via cargo install..."
# `cargo install --path .` copies the binary to ~/.cargo/bin/, which
# is on PATH by default for any user with a normal Rust install.
# `--features ui` turns on the embedded UI and `serve` subcommand.
cargo install --path . --features ui --locked

echo "→ Configuring git hooks (scripts/hooks/*)..."
# Point git at the in-repo hooks dir so a fresh clone gets the same
# pre-push test run as everyone else. One-time per checkout.
git config core.hooksPath scripts/hooks

# Sanity: PATH check. cargo install writes to $CARGO_HOME/bin
# (defaults to ~/.cargo/bin), which a normal `rustup` install puts
# on PATH via the rustup-managed shell profile. Catch the case where
# someone built rust differently and the binary isn't reachable.
CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
case ":$PATH:" in
  *":$CARGO_BIN:"*) echo "✓ $CARGO_BIN on PATH" ;;
  *)
    echo
    echo "⚠  $CARGO_BIN is not on PATH. Add to your shell init:"
    echo "       export PATH=\"$CARGO_BIN:\$PATH\""
    ;;
esac

echo
INSTALLED="$(command -v labctl || echo "$CARGO_BIN/labctl")"
echo "✓ labctl installed at $INSTALLED"
echo "  $($INSTALLED --version 2>/dev/null || echo "(re-open your shell to pick up PATH)")"
echo "✓ Pre-push hook will run \`cargo test --all-features\` before any push."
echo
echo "Next: write a cluster.toml (\`labctl init --help\`) and run \`labctl doctor\`."
