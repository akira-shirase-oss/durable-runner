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

step "no mutation markers in committed source"
# A mutation marker in a tracked file means a mutation under test was
# never reverted — it shipped. Mutations happen only on the scratch copy
# in tests/attacks/prove-attacks.sh, so no marker may ever appear in a
# tracked file. (The pattern is built by concatenation so this script
# does not match itself.)
marker='MUT''ANT\|MUT''ATION-MARK\|MUT''ATE-HERE'
if git grep -nE "$marker" -- . >"${TMPDIR:-/tmp}/durable-mutants.$$"; then
  cat "${TMPDIR:-/tmp}/durable-mutants.$$"
  echo "a mutation marker is committed — revert it before committing" >&2
  exit 1
fi

step "toolchain"
cargo --version
rustc --version

step "fmt"
cargo fmt --all -- --check

step "clippy (-D warnings)"
cargo clippy --workspace --all-targets --locked -- -D warnings

step "tests"
cargo test --workspace --locked

step "fuzz smoke (never-panics decoders)"
# Every outside-input parser/decoder has a fuzz_* proptest target with a
# seed corpus in fuzz/corpus/. The suite runs each at the default count;
# this pass runs them deeper — fixed iterations, not wall-clock time.
PROPTEST_CASES="${FUZZ_CASES:-2048}" cargo test --workspace --locked fuzz

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
    npm test
    npm run build
  )
fi

step "perf: clean-imports gate (+ known-bad proof)"
perf/check-clean-imports.sh

step "migrations frozen (checksum manifest + known-bad proof)"
scripts/check-migrations.sh

step "deploy format guard (D-056)"
deploy/test-format-guard.sh

if [ "${SKIP_PILOT:-0}" != 1 ]; then
  step "pilot (release build)"
  examples/pilot/run.sh
fi

step "gate passed"
