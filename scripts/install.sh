#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Frontend is required: rust-embed bakes ui/dist/ into the binary at compile time.
# Skip the rebuild if npm isn't on PATH but dist/ already exists, so the binary
# can be reinstalled on node-less hosts (login nodes, ...).
if command -v npm >/dev/null 2>&1; then
  echo "→ Building frontend..."
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
cargo install --path . --features ui --locked

echo "→ Configuring git hooks (scripts/hooks/*)..."
git config core.hooksPath scripts/hooks

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
echo "Next: \`labctl init\` to bootstrap a cluster config."
