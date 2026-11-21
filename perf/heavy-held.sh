#!/usr/bin/env bash
# Am I running under the machine-wide heavy slot? Exit 0 iff fd 9 is an
# open descriptor on the lock file — i.e. this process inherited
# heavy.sh's locked file description. The fd IS the proof (S2 re-review
# F4): DR_HEAVY_HELD is informational and can be set by anyone; only a
# child of the lock holder has the description.
#
#   perf/heavy-held.sh && echo under-the-slot
set -euo pipefail
cd "$(dirname "$0")/.."
LOCK="${DR_HEAVY_LOCK:-$(git rev-parse --git-common-dir)/durable-runner-heavy.lock}"
LOCK="$LOCK" python3 - <<'PY' 9>&9
import os, sys
try:
    held = os.fstat(9)
    lock = os.stat(os.environ['LOCK'])
except OSError:
    sys.exit(1)
sys.exit(0 if (held.st_dev, held.st_ino) == (lock.st_dev, lock.st_ino) else 1)
PY
