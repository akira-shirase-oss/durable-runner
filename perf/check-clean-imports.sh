#!/usr/bin/env bash
# The clean-checkout import gate: a bare specifier that only resolves
# because a local node_modules happened to exist is caught here, not on
# CI (undici-as-import killed the whole 44894875049 battery that way).
#
# Archives HEAD — no node_modules, nothing installed — and imports
# load.mjs. ESM resolves the static graph before evaluating it, so a
# missing package rejects as ERR_MODULE_NOT_FOUND (exit 9) BEFORE any of
# the harness runs; a resolved graph evaluates and exits on the missing
# scenario arg (2) — anything but 9 is a pass.
set -euo pipefail
cd "$(dirname "$0")/.."
TMP=$(mktemp -d "${TMPDIR:-/tmp}/dr-clean-imports.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
git archive HEAD | tar -x -C "$TMP"
cd "$TMP"
set +e
node --input-type=module -e "
import('./perf/load.mjs').catch((e) => {
  console.error(e);
  process.exit(e.code === 'ERR_MODULE_NOT_FOUND' || e.code === 'ERR_MODULE_SPECIFIER' ? 9 : 0);
});
"
rc=$?
set -e
if [ "$rc" -eq 9 ]; then
  echo "check-clean-imports: perf/load.mjs imports something absent from a clean checkout" >&2
  exit 1
fi
echo "check-clean-imports: perf/load.mjs resolves on a clean checkout (exit $rc)"
