#!/usr/bin/env bash
# The heavy slot: ONE load/battery run per machine. Anything that can
# exhaust ephemeral ports, fds, or CPU runs serialized under this — an
# unserialized flood once took the whole box's outbound TCP down with it.
#
#   perf/heavy.sh node perf/load.mjs workspaces --workspaces=4
#   perf/heavy.sh perf/battery.sh --full sustained
#
# The slot is MACHINE-wide — one path under TMPDIR that every worktree,
# checkout and CI job resolves to the same place — and it names its holder
# pid. A slot whose pid is dead is stale and gets swept; a live one refuses
# (exit 3). It never queues: queueing hides the collision the way three
# concurrent floods hid theirs.
set -euo pipefail
cd "$(dirname "$0")/.."
SLOT="${TMPDIR:-/tmp}/durable-runner-heavy.slot"

tries=0
until mkdir "$SLOT" 2>/dev/null; do
  tries=$((tries + 1))
  holder_pid=""
  if [ -r "$SLOT/holder" ]; then
    read -r holder_pid _ < "$SLOT/holder" || true
  fi
  # Liveness, fail-closed: only a pid PROVEN dead makes the slot stale.
  # kill -0 says "No such process" for dead and "Operation not permitted"
  # for alive-but-not-ours — only the former permits the sweep; anything
  # unanswerable (a denied kill, a young unwritten holder) refuses.
  stale=0
  if [ -n "$holder_pid" ]; then
    err=$(kill -0 "$holder_pid" 2>&1 >/dev/null || true)
    case "$err" in
      *"No such process"*) stale=1 ;;
    esac
  fi
  if [ "$stale" -eq 1 ]; then
    rm -rf "$SLOT"
    continue
  fi
  if [ "$tries" -lt 3 ] && [ -z "$holder_pid" ]; then
    sleep 0.2 2>/dev/null || sleep 1
    continue
  fi
  holder=$(cat "$SLOT/holder" 2>/dev/null || echo unknown)
  echo "heavy.sh: the heavy slot is held by: ${holder:-unknown}" >&2
  echo "heavy.sh: a load run already owns this machine — wait or stop it; refusing to race" >&2
  exit 3
done

cleanup() { rm -rf "$SLOT"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$$ $*" > "$SLOT/holder"
# Children under this slot (battery legs, a nested load.mjs) inherit the
# mark and skip their own acquire — the slot is per-run, not per-process.
export DR_HEAVY_HELD=$$

set +e
"$@"
status=$?
set -e
exit $status
