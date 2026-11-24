#!/usr/bin/env bash
# Am I running under the machine-wide heavy slot? Exit 0 iff the lock is
# HELD and fd 9 — the descriptor this process inherited — is the
# description holding it (re-review #12: flock(9, EX|NB) alone succeeds
# on an open-but-UNLOCKED fd by taking the lock, which proves nothing):
#   1. a FRESH open of the lock file must fail flock(EX|NB) — someone
#      holds the lock (if it succeeds, nobody did: release, refuse);
#   2. flock(9, EX|NB) must then succeed — a relock on the very
#      description that holds it never contends; any other description
#      contends with the holder and fails.
# DR_HEAVY_HELD is informational only.
#
#   perf/heavy-held.sh && echo under-the-slot
set -euo pipefail
cd "$(dirname "$0")/.."
LOCK="${DR_HEAVY_LOCK:-$(git rev-parse --git-common-dir)/durable-runner-heavy.lock}"
LOCK="$LOCK" python3 -I - <<'PY' 9>&9
import os, sys, fcntl
try:
    held = os.fstat(9)
    lock = os.stat(os.environ['LOCK'])
    if (held.st_dev, held.st_ino) != (lock.st_dev, lock.st_ino):
        sys.exit(1)
    probe = os.open(os.environ['LOCK'], os.O_WRONLY | os.O_APPEND)
    try:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        pass  # held by someone: now prove it is fd 9's description
    else:
        os.close(probe)  # nobody held it; drop what the probe just took
        sys.exit(1)
    os.close(probe)
    fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
    sys.exit(0)
except OSError:
    pass
sys.exit(1)
PY
