#!/usr/bin/env bash
# shots.sh — produce the inspector's before/after screenshot sets
# (1440 and 390, light and dark, runs/detail/submit/admin) into
# perf/ui/{before,after}. Run outside the sandbox:
#
#   apps/inspector/shots.sh [before-ref]
#
# before-ref defaults to a6fa947 (the last commit before the D-053
# rework). Ports override with PORT/HPORT if the defaults are busy.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BEFORE_REF="${1:-a6fa947}"
PORT="${PORT:-53344}"
HPORT="${HPORT:-53345}"
OUT="$ROOT/perf/ui"
BEFORE_DIR="$(mktemp -d)/dr-ui-before"

cd "$ROOT/apps/inspector"
npm ci --no-fund --no-audit
npx playwright install chromium
npm run build
node capture.mjs --out "$OUT/after" --port="$PORT" --hport="$HPORT"

mkdir -p "$BEFORE_DIR"
git -C "$ROOT" archive "$BEFORE_REF" apps/inspector | tar -x -C "$BEFORE_DIR"
cd "$BEFORE_DIR/apps/inspector"
npm ci --no-fund --no-audit
npm run build
node "$ROOT/apps/inspector/capture.mjs" \
  --ui-dir "$BEFORE_DIR/apps/inspector/build" \
  --out "$OUT/before" --port="$PORT" --hport="$HPORT"

echo "done: $OUT/after $OUT/before"
