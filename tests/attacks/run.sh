#!/usr/bin/env bash
# The dynamic pentest suite: build the real binary, then send real attack
# requests at running instances of it.
#   tests/attacks/run.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
cargo build -p durable-server --locked
cargo test -p durable-attacks --locked
