#!/usr/bin/env bash
# The heavy slot: ONE load/battery run per machine. Anything that can
# exhaust ephemeral ports, fds, or CPU runs serialized under this — an
# unserialized flood once took the whole box's outbound TCP down with it.
#
#   perf/heavy.sh node perf/load.mjs workspaces --workspaces=4
#   perf/heavy.sh perf/battery.sh --full sustained
#
# The slot refuses a second run outright (exit 3); it does not queue —
# queueing hides the collision the way three concurrent floods hid theirs.
set -euo pipefail
cd "$(dirname "$0")/.."
SLOT=perf/.heavy.slot
if ! mkdir "$SLOT" 2>/dev/null; then
  holder=$(cat "$SLOT/cmd" 2>/dev/null || echo unknown)
  echo "heavy.sh: the heavy slot is held by: ${holder:-unknown}" >&2
  echo "heavy.sh: a load run already owns this machine — wait or stop it; refusing to race" >&2
  exit 3
fi
cleanup() { rm -rf "$SLOT"; }
trap cleanup EXIT
printf '%s\n' "$$ $*" > "$SLOT/cmd"
exec "$@"
