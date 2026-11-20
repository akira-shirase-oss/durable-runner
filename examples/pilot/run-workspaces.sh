#!/usr/bin/env bash
# Workspace crash drill — two workspaces submit through the memory account
# backend, the server is kill -9'd, restarted on the same journal, and
# isolation + the handlers.json snapshot must hold across the replay:
#   cargo build --release -p durable-server && examples/pilot/run-workspaces.sh
# Requires: python3, curl. Exits non-zero on the first check that fails.
#
# Port: DURABLE_PILOT_PORT (default 58340) for the server.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$(pwd)
WORK=$(mktemp -d)
SRV_PORT=${DURABLE_PILOT_PORT:-58340}
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'DRILL FAILED: %s\n' "$1" >&2
  for log in "$WORK"/*.log; do [ -f "$log" ] && { echo "--- $log" >&2; tail -20 "$log" >&2; }; done
  exit 1
}
healthy() {
  for _ in $(seq 1 150); do
    curl -sf "http://127.0.0.1:$SRV_PORT/health" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  fail "server never became healthy"
}

step "build"
if command -v brew >/dev/null 2>&1; then
  export PATH="$(brew --prefix rustup)/bin:$PATH" # the pinned toolchain on macOS
fi
if ! BUILD=$(cargo build --release --locked -p durable-server 2>&1); then
  printf '%s\n' "$BUILD" >&2
  fail "the release build failed (full output above)"
fi
TARGET=$(cargo metadata --format-version 1 --no-deps --locked \
  | python3 -I -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
SRV=$TARGET/release/durable-server
[ -x "$SRV" ] || fail "no release binary in $TARGET/release"

# Ops key = workspace 0 admin. Memory backend: no Postgres needed.
DURABLE_API_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
export DURABLE_API_KEY DURABLE_ACCOUNTS_BACKEND=memory
API="http://127.0.0.1:$SRV_PORT"
AUTH="authorization: Bearer $DURABLE_API_KEY"
JQ() { python3 -I -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

start() {
  "$SRV" --journal "$WORK/journal" --listen "127.0.0.1:$SRV_PORT" \
    --allow-local-handlers --drain-timeout-secs 2 \
    >"$WORK/server-$1.log" 2>&1 &
  healthy
}

step "start (memory backend)"
start a

step "create two workspaces + one admin key each"
WS_A=$(curl -sf -X POST "$API/admin/workspaces" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"alpha"}') || fail "create workspace A"
WS_B=$(curl -sf -X POST "$API/admin/workspaces" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"bravo"}') || fail "create workspace B"
PA=$(echo "$WS_A" | JQ 'd["public_id"]')
PB=$(echo "$WS_B" | JQ 'd["public_id"]')
[ "$(echo "$WS_A" | JQ 'len(d.get("handler_secret",""))')" = 64 ] || fail "workspace A secret not returned once: $WS_A"
echo "A=$PA B=$PB (secrets returned once, 64 hex chars)"

KA=$(curl -sf -X POST "$API/admin/workspaces/$PA/keys" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"k","permission":"admin"}' | JQ 'd["key"]')
KB=$(curl -sf -X POST "$API/admin/workspaces/$PB/keys" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"k","permission":"admin"}' | JQ 'd["key"]')
AUTH_A="authorization: Bearer $KA"
AUTH_B="authorization: Bearer $KB"

step "register a signed handler per workspace + submit runs"
for K in "$AUTH_A" "$AUTH_B"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$API/handlers/step" \
    -H "$K" -H 'content-type: application/json' \
    -d '{"url":"http://127.0.0.1:9/step"}')
  [ "$code" = 200 ] || fail "PUT /handlers/step -> $code"
done
# Rotate A's secret once — must return the new one and keep working.
ROT=$(curl -sf -X POST "$API/handler-secret" -H "$AUTH_A") || fail "rotate A secret"
[ "$(echo "$ROT" | JQ 'len(d.get("secret",""))')" = 64 ] || fail "rotation did not return a secret: $ROT"
for i in 1 2 3; do
  curl -sf -X POST "$API/jobs" -H "$AUTH_A" -H 'content-type: application/json' \
    -d "{\"op_key\":\"drill-a-$i\",\"spec\":{\"version\":1,\"steps\":[{\"name\":\"s\",\"handler\":\"step\"}],\"input_b64\":\"aGk=\"}}" >/dev/null \
    || fail "submit A$i"
  curl -sf -X POST "$API/jobs" -H "$AUTH_B" -H 'content-type: application/json' \
    -d "{\"op_key\":\"drill-b-$i\",\"spec\":{\"version\":1,\"steps\":[{\"name\":\"s\",\"handler\":\"step\"}],\"input_b64\":\"aGk=\"}}" >/dev/null \
    || fail "submit B$i"
done

step "isolation before the crash"
[ "$(curl -sf "$API/jobs?limit=50" -H "$AUTH_A" | JQ 'len(d["runs"])')" = 3 ] || fail "A list != 3"
[ "$(curl -sf "$API/jobs?limit=50" -H "$AUTH_B" | JQ 'len(d["runs"])')" = 3 ] || fail "B list != 3"
IDA=$(curl -sf "$API/jobs?limit=1" -H "$AUTH_A" | JQ 'd["runs"][0]["id"]')
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/jobs/$IDA" -H "$AUTH_B")
[ "$code" = 404 ] || fail "B read A's run -> $code (want 404)"

step "kill -9 and restart on the same journal"
kill -9 "$(jobs -p | tail -1)" 2>/dev/null || fail "no server to kill"
sleep 0.3
start b

step "recreate the account graph (memory store restarts empty)"
# The journal's authority is the internal u64 ws id — the memory store
# re-derives the same ids in creation order (alpha -> 1, bravo -> 2), so
# replayed runs reattach to the recreated workspaces. On Postgres this
# step does not exist: the graph survives independently of the process.
WS_A2=$(curl -sf -X POST "$API/admin/workspaces" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"alpha"}') || fail "recreate workspace A"
WS_B2=$(curl -sf -X POST "$API/admin/workspaces" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"bravo"}') || fail "recreate workspace B"
PA2=$(echo "$WS_A2" | JQ 'd["public_id"]')
PB2=$(echo "$WS_B2" | JQ 'd["public_id"]')
KA=$(curl -sf -X POST "$API/admin/workspaces/$PA2/keys" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"k","permission":"admin"}' | JQ 'd["key"]')
KB=$(curl -sf -X POST "$API/admin/workspaces/$PB2/keys" -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"k","permission":"admin"}' | JQ 'd["key"]')
AUTH_A="authorization: Bearer $KA"
AUTH_B="authorization: Bearer $KB"

step "after replay: ws tags + scoping + snapshot registry all intact"
[ "$(curl -sf "$API/jobs?limit=50" -H "$AUTH_A" | JQ 'len(d["runs"])')" = 3 ] || fail "A list after restart != 3"
[ "$(curl -sf "$API/jobs?limit=50" -H "$AUTH_B" | JQ 'len(d["runs"])')" = 3 ] || fail "B list after restart != 3"
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/jobs/$IDA" -H "$AUTH_B")
[ "$code" = 404 ] || fail "B read A's run after restart -> $code (want 404)"
# The handlers.json snapshot must re-arm the registry: a fresh submit
# under B's key still admits `step` without any store round-trip needed.
curl -sf -X POST "$API/jobs" -H "$AUTH_B" -H 'content-type: application/json' \
  -d '{"op_key":"drill-b-post","spec":{"version":1,"steps":[{"name":"s","handler":"step"}],"input_b64":"aGk="}}' >/dev/null \
  || fail "post-restart submit under B refused — snapshot registry lost"
[ "$(curl -sf "$API/jobs?limit=50" -H "$AUTH_B" | JQ 'len(d["runs"])')" = 4 ] || fail "B list after post-restart submit != 4"

printf '\n== workspace drill complete — 2 workspaces, kill -9 replay kept scoping,\n'
printf '   rotated secret returned once, snapshot registry admitted post-restart\n'
