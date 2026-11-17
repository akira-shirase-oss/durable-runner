#!/usr/bin/env bash
# The gate: everything CI runs, runnable locally. Exits non-zero on the
# first failure.
#
#   scripts/check.sh            # the whole gate
#   SKIP_PILOT=1 scripts/check.sh
#   SKIP_NPM=1 scripts/check.sh  # no node on this machine
#
# Needs: the toolchain from rust-toolchain.toml, cargo-deny, python3 and
# curl (the pilot), node + npm (the inspector).
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v brew >/dev/null 2>&1 && brew --prefix rustup >/dev/null 2>&1; then
  # Homebrew's own cargo ignores rust-toolchain.toml; rustup's honours it.
  export PATH="$(brew --prefix rustup)/bin:$PATH"
fi

step() { printf '\n== %s\n' "$1"; }

step "toolchain"
cargo --version
rustc --version

step "fmt"
cargo fmt --all -- --check

step "clippy (-D warnings)"
cargo clippy --workspace --all-targets --locked -- -D warnings

step "tests"
cargo test --workspace --locked

step "architecture: the domain crate does no I/O"
if grep -nE 'std::(fs|net|process|io|thread|env)|tokio|reqwest' crates/durable-domain/src/*.rs; then
  echo "durable-domain must stay pure (AGENTS.md non-negotiable 5)" >&2
  exit 1
fi

step "architecture: nothing outside tests spawns processes"
if grep -rnE 'std::process::Command|process::Command|Command::new' crates/*/src; then
  echo "no process spawning in crates/*/src (handlers are HTTP only)" >&2
  exit 1
fi

step "architecture: fault injection is absent from the shipped binaries"
for bin in durable-server durable-cli; do
  if cargo tree --locked -p "$bin" -e features,normal 2>/dev/null | grep -q 'fault-injection'; then
    echo "$bin's normal build enables durable-journal/fault-injection" >&2
    exit 1
  fi
done

step "cargo deny (advisories, licenses, bans, sources)"
cargo deny --locked check

if [ "${SKIP_NPM:-0}" != 1 ]; then
  step "inspector: npm ci, audit, type-check, build"
  (
    cd apps/inspector
    npm ci --no-audit --no-fund
    npm audit --omit=dev --audit-level=moderate
    npm run check
    npm run build
  )
fi

if [ "${SKIP_PILOT:-0}" != 1 ]; then
  step "pilot (release build)"
  examples/pilot/run.sh
fi

step "gate passed"
