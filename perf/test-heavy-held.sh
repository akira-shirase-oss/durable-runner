#!/usr/bin/env bash
# perf/heavy-held.sh proves the heavy slot is held BY THE INHERITED fd 9,
# not merely that fd 9 is open on the lock file (re-review #12). Four
# cases, each with the answer fixed before the run:
#   no fd 9                                   → 1
#   fd 9 open on the file, nobody holds it    → 1   (the old check said 0)
#   fd 9 open, unlocked, another holds it     → 1
#   fd 9 is the holding description           → 0
# Plus the known-bad proof: the old single-flock check run on case 2
# must answer 0, or this test could not tell the two apart.
set -uo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-heavyheld.XXXXXX")"
lockfile="$work/heavy.lock"
: >"$lockfile"
fails=0
holder_pid=""
cleanup() { [[ -n "$holder_pid" ]] && kill "$holder_pid" 2>/dev/null; rm -rf "$work"; }
trap cleanup EXIT
export DR_HEAVY_LOCK="$lockfile"

expect() { # <want> <desc> <cmd...>
  local want=$1 desc=$2; shift 2
  "$@"; local got=$?
  if [[ "$got" != "$want" ]]; then
    echo "FAIL: $desc: exit $got, want $want"; fails=1
  else
    echo "ok: $desc"
  fi
}

expect 1 "no fd 9" bash -c 'exec 9>&-; perf/heavy-held.sh'
expect 1 "fd 9 open, nobody holds the lock" bash -c 'exec 9>>"$DR_HEAVY_LOCK"; perf/heavy-held.sh'

# Another process holds the lock for the next case.
python3 -I -c '
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_APPEND)
fcntl.flock(fd, fcntl.LOCK_EX)
print("held", flush=True)
time.sleep(60)
' "$lockfile" >"$work/holder.out" &
holder_pid=$!
for _ in $(seq 1 100); do grep -q held "$work/holder.out" 2>/dev/null && break; sleep 0.05; done
expect 1 "fd 9 open, unlocked, another process holds it" bash -c 'exec 9>>"$DR_HEAVY_LOCK"; perf/heavy-held.sh'
kill "$holder_pid" 2>/dev/null; wait "$holder_pid" 2>/dev/null; holder_pid=""

expect 0 "fd 9 is the holding description" bash -c '
  exec 9>>"$DR_HEAVY_LOCK"
  python3 -I -c "import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)" 9>&9
  perf/heavy-held.sh'

# Known-bad proof: the pre-fix check (fstat + flock(9) only) on case 2.
cat >"$work/old-held.sh" <<'OLD'
#!/usr/bin/env bash
LOCK="$DR_HEAVY_LOCK" python3 -I - <<'PY' 9>&9
import os, sys, fcntl
try:
    held = os.fstat(9); lock = os.stat(os.environ['LOCK'])
    if (held.st_dev, held.st_ino) == (lock.st_dev, lock.st_ino):
        fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB); sys.exit(0)
except OSError:
    pass
sys.exit(1)
PY
OLD
chmod +x "$work/old-held.sh"
expect 0 "known-bad: the old check is fooled by an unlocked fd 9" bash -c "exec 9>>\"\$DR_HEAVY_LOCK\"; $work/old-held.sh"

if [[ "$fails" -ne 0 ]]; then
  echo "test-heavy-held: FAILED" >&2
  exit 1
fi
echo "test-heavy-held: fd 9 must be the holding description (and the old check could not tell)"
