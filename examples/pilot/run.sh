#!/usr/bin/env bash
# Pilot walkthrough — run, inspect, kill, back up, and restore one workflow.
# The whole second-developer path on a fresh checkout:
#   cargo build --release && examples/pilot/run.sh
# Requires: python3, curl. Cleans up its child processes on exit.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$(pwd)
WORK=$(mktemp -d)
SRV_PORT=58330; HND_PORT=58320; RSRV_PORT=58331
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

step() { printf '\n== %s\n' "$1"; }

step "build"
export PATH="$(brew --prefix rustup 2>/dev/null)/bin:$PATH"
cargo build --release -p durable-server -p durable-cli 2>&1 | tail -1
SRV=$ROOT/target/release/durable-server
CLI="$ROOT/target/release/durable-cli --server http://127.0.0.1:$SRV_PORT"

step "start the pilot handler + the server (UI mounted at /inspector)"
python3 examples/pilot/handler.py &
"$SRV" --journal "$WORK/journal" --listen 127.0.0.1:$SRV_PORT \
  --ui-dir "$ROOT/apps/inspector/build" \
  --handler step=http://127.0.0.1:$HND_PORT/step >"$WORK/server.log" 2>&1 &
sleep 1.2

step "submit via the CLI (op_key dedupes)"
OUT=$($CLI submit --op-key pilot/onboarding-email \
      --step prepare=step --step deliver=step \
      --input '{"user":"alice","template":"welcome"}')
echo "submit -> $OUT"
ID=$(echo "$OUT" | grep -o '[0-9]*')

step "wait for done"
for _ in $(seq 1 60); do
  S=$($CLI inspect --run "$ID")
  echo "$S" | grep -q '"done"' && break
  sleep 0.25
done
echo "inspect -> $S"

step "list + inspector page"
curl -s "http://127.0.0.1:$SRV_PORT/jobs"; echo
echo "inspector -> http://127.0.0.1:$SRV_PORT/inspector/ ($(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$SRV_PORT/inspector/))"

step "kill -9 the server mid-life, restart against the same journal"
kill -9 "$(lsof -ti :$SRV_PORT)" || true
sleep 0.4
"$SRV" --journal "$WORK/journal" --listen 127.0.0.1:$SRV_PORT \
  --handler step=http://127.0.0.1:$HND_PORT/step >>"$WORK/server.log" 2>&1 &
sleep 1.2
echo "after replay -> $($CLI inspect --run "$ID")"

step "compact + backup"
$CLI compact
$CLI backup --to "$WORK/backup"

step "restore on a second server instance (different port + journal dir)"
kill -9 "$(lsof -ti :$SRV_PORT)" || true
sleep 0.4
"$SRV" --journal "$WORK/backup" --listen 127.0.0.1:$RSRV_PORT \
  --handler step=http://127.0.0.1:$HND_PORT/step >"$WORK/restored.log" 2>&1 &
sleep 1.2
RCLI="$ROOT/target/release/durable-cli --server http://127.0.0.1:$RSRV_PORT"
echo "restored inspect -> $($RCLI inspect --run "$ID")"

step "pilot complete — run survived kill -9, compaction, and a restore"
