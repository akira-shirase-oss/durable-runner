#!/usr/bin/env bash
# The heavy slot: ONE load/battery run per machine. Anything that can
# exhaust ephemeral ports, fds, or CPU runs serialized under this — an
# unserialized flood once took the whole box's outbound TCP down with it.
#
#   perf/heavy.sh node perf/load.mjs workspaces --workspaces=4
#   perf/heavy.sh perf/battery.sh --full sustained
#
# The slot is an flock(2) (fcntl.flock) on ONE FIXED PATH — the git
# common dir's durable-runner-heavy.lock, the one piece of storage every
# worktree and checkout of this clone resolves to identically (S2
# re-review F4: $TMPDIR is per-user and per-environment, so a slot under
# it was never machine-wide). DR_HEAVY_LOCK overrides the path for a
# cross-clone machine path. The lock is kernel state on an open file
# description, not a file's existence: the holder dying releases it, so
# there is no stale sweep, no pid liveness to get wrong, no pid to
# reuse, and no wedge between a mkdir and a holder write.
#
# fd 9 stays open in this process and is INHERITED by the command's whole
# process tree — a SIGKILL here cannot orphan a running child into an
# unswept slot, because the child still holds the lock's fd. Children
# prove they're under the slot by fd 9 being THIS file (inode compare),
# never by trusting DR_HEAVY_HELD — the env var is informational only.
set -euo pipefail
cd "$(dirname "$0")/.."
LOCK="${DR_HEAVY_LOCK:-$(git rev-parse --git-common-dir)/durable-runner-heavy.lock}"
HOLDER="$LOCK.holder"

# O_WRONLY|O_APPEND|O_CREAT on fd 9 — LOCK_EX wants a writable fd on
# BSD. A foreign-owned file we can't open refuses the run (loud, not
# silent).
exec 9>>"$LOCK"
chmod 0666 "$LOCK" 2>/dev/null || true # keep the file joinable

held=0
for _ in 1 2 3 4; do
  if python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)' 9>&9 2>/dev/null; then
    held=1
    break
  fi
  sleep 1
done
if [ "$held" -ne 1 ]; then
  echo "heavy.sh: the heavy slot is held ($LOCK):" >&2
  sed 's/^/heavy.sh:   /' "$HOLDER" 2>/dev/null >&2 || true
  echo "heavy.sh: a load run already owns this machine — wait or stop it; refusing to race" >&2
  exit 3
fi

# Advisory holder note for the refused message above — never read for
# liveness (the kernel answers that). Best-effort: a locked file we
# couldn't write the note next to still serializes correctly.
printf '%s %s\n' "$$" "$*" > "$HOLDER" 2>/dev/null || true
# Children under this slot (battery legs, a nested load.mjs) inherit
# fd 9 — proof they may run — and the mark for humans/debugging.
export DR_HEAVY_HELD=$$
export DR_HEAVY_LOCK="$LOCK"

set +e
"$@"
status=$?
set -e
exit $status
