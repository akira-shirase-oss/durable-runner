#!/usr/bin/env bash
# Am I running under the machine-wide heavy slot? Exit 0 iff fd 9 is an
# open descriptor on the lock file AND flock(9, EX|NB) succeeds — the
# inherited description holds the lock, so a relock on it never
# contends; an fd merely open on a contended lock fails (S2 re-review
# F4 + N5: the fd IS the proof, and the proof is the lock — an open
# descriptor on a 0666 file anyone can open proves nothing alone).
# DR_HEAVY_HELD is informational only.
#
#   perf/heavy-held.sh && echo under-the-slot
set -euo pipefail
cd "$(dirname "$0")/.."
LOCK="${DR_HEAVY_LOCK:-$(git rev-parse --git-common-dir)/durable-runner-heavy.lock}"
LOCK="$LOCK" python3 - <<'PY' 9>&9
import os, sys, fcntl
try:
    held = os.fstat(9)
    lock = os.stat(os.environ['LOCK'])
    same = (held.st_dev, held.st_ino) == (lock.st_dev, lock.st_ino)
    if same:
        fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sys.exit(0)
except OSError:
    pass
sys.exit(1)
PY
