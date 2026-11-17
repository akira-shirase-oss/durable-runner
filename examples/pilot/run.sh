#!/usr/bin/env bash
# Pilot walkthrough — run, inspect, kill, back up, restore and stop one
# workflow. The whole second-developer path on a fresh checkout:
#   cargo build --release && examples/pilot/run.sh
# Requires: python3, curl. Every step is checked; the script exits non-zero
# on the first one that does not hold. Cleans up its child processes on exit.
#
# Ports: DURABLE_PILOT_PORT (default 58330) for the server, that + 1 for the
# restored server, that - 10 for the pilot handler.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$(pwd)
WORK=$(mktemp -d)
SRV_PORT=${DURABLE_PILOT_PORT:-58330}
RSRV_PORT=$((SRV_PORT + 1))
HND_PORT=$((SRV_PORT - 10))
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'PILOT FAILED: %s\n' "$1" >&2
  for log in "$WORK"/*.log; do [ -f "$log" ] && { echo "--- $log" >&2; tail -20 "$log" >&2; }; done
  exit 1
}
# Wait until a server answers /health (or fail after ~15 s).
healthy() {
  for _ in $(seq 1 150); do
    curl -sf "http://127.0.0.1:$1/health" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  fail "server on port $1 never became healthy"
}

# The API key: every route but /health, /ready and the inspector's files
# needs it. The server and the CLI both read DURABLE_API_KEY from the
# environment (never from argv). A fresh random key per pilot run; in
# production, keep it in your secret store and export it into the server's
# environment.
DURABLE_API_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
export DURABLE_API_KEY

step "build"
if command -v brew >/dev/null 2>&1; then
  export PATH="$(brew --prefix rustup)/bin:$PATH" # the pinned toolchain on macOS
fi
# The binaries live wherever cargo builds them: honour CARGO_TARGET_DIR and
# .cargo/config (per-worktree target dirs), never assume $ROOT/target.
if ! BUILD=$(cargo build --release --locked -p durable-server -p durable-cli 2>&1); then
  printf '%s\n' "$BUILD" >&2
  fail "the release build failed (full output above)"
fi
printf '%s\n' "$BUILD" | tail -1
TARGET=$(cargo metadata --format-version 1 --no-deps --locked \
  | python3 -I -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
BIN=$TARGET/release
[ -x "$BIN/durable-server" ] && [ -x "$BIN/durable-cli" ] || fail "no release binaries in $BIN"
SRV=$BIN/durable-server
CLI="$BIN/durable-cli --server http://127.0.0.1:$SRV_PORT"
SERVE=("$SRV" --journal "$WORK/journal" --listen "127.0.0.1:$SRV_PORT"
  --backup-root "$WORK/backups" --handler "step=http://127.0.0.1:$HND_PORT/step")

step "start the pilot handler + the server (UI mounted at /inspector)"
python3 examples/pilot/handler.py "$HND_PORT" &
"${SERVE[@]}" --ui-dir "$ROOT/apps/inspector/build" >"$WORK/server.log" 2>&1 &
SRV_PID=$!
healthy "$SRV_PORT"

step "submit via the CLI (op_key dedupes)"
OUT=$($CLI submit --op-key pilot/onboarding-email \
      --step prepare=step --step deliver=step \
      --input '{"user":"alice","template":"welcome"}')
echo "submit -> $OUT"
ID=$(echo "$OUT" | grep -o '[0-9]*')
AGAIN=$($CLI submit --op-key pilot/onboarding-email --step prepare=step --step deliver=step \
      --input '{"user":"alice","template":"welcome"}')
[ "$AGAIN" = "$OUT" ] || fail "resubmitting the op_key gave $AGAIN, not $OUT"

step "wait for done"
S=""
for _ in $(seq 1 120); do
  S=$($CLI inspect --run "$ID")
  echo "$S" | grep -q '"done"' && break
  sleep 0.25
done
echo "inspect -> $S"
echo "$S" | grep -q '"done"' || fail "run $ID never reached done"
FINISHED=$(echo "$S" | grep -o '"finished_at":[0-9]*')

step "list + inspector page (reads need the key)"
curl -s -H "Authorization: Bearer $DURABLE_API_KEY" "http://127.0.0.1:$SRV_PORT/jobs"; echo
CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$SRV_PORT/jobs")
[ "$CODE" = 401 ] || fail "an unauthenticated read answered $CODE, not 401"
echo "inspector -> http://127.0.0.1:$SRV_PORT/inspector/ ($(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$SRV_PORT/inspector/"))"

step "kill -9 the server mid-life, restart against the same journal"
kill -9 "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
"${SERVE[@]}" >>"$WORK/server.log" 2>&1 &
SRV_PID=$!
healthy "$SRV_PORT"
S=$($CLI inspect --run "$ID")
echo "after replay -> $S"
echo "$S" | grep -q '"done"' || fail "run $ID is not done after kill -9 and replay"

step "compact + backup"
$CLI compact
$CLI backup --name pilot          # lands in $WORK/backups/pilot, nowhere else

step "restore on a second server instance (different port + journal dir)"
"$BIN/durable-cli" restore --from "$WORK/backups/pilot" --to "$WORK/journal" \
  && fail "restore into the live journal was not refused"
kill -9 "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
"$BIN/durable-cli" restore --from "$WORK/backups/pilot" --to "$WORK/restored"
"$SRV" --journal "$WORK/restored" --listen "127.0.0.1:$RSRV_PORT" \
  --handler "step=http://127.0.0.1:$HND_PORT/step" >"$WORK/restored.log" 2>&1 &
RSRV_PID=$!
healthy "$RSRV_PORT"
RCLI="$BIN/durable-cli --server http://127.0.0.1:$RSRV_PORT"
R=$($RCLI inspect --run "$ID")
echo "restored inspect -> $R"
echo "$R" | grep -q '"done"' || fail "the restored run is not done"
echo "$R" | grep -q "$FINISHED" || fail "the restored run's finished_at changed ($FINISHED)"

step "stop the restored server with SIGTERM: it drains and exits 0"
kill -TERM "$RSRV_PID"
RC=0
wait "$RSRV_PID" || RC=$?
[ "$RC" = 0 ] || fail "SIGTERM exit status $RC, not 0"
grep -q "drained" "$WORK/restored.log" || fail "the server did not report a drain"

step "pilot complete — run survived kill -9, compaction, a restore and a drain"
