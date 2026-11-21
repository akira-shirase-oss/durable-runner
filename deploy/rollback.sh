#!/usr/bin/env bash
# rollback.sh — restore the binary upgrade.sh saved and restart, < 1 min.
# The journal is forward-compatible within v1 (D-054's reader accepts every
# shipped format), so a rollback replays the same journal the new binary wrote.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
[[ -f /usr/local/bin/durable-server.prev ]] || { echo "no .prev binary — nothing to roll back to" >&2; exit 1; }
# D-056: the saved binary must still read what the journal now holds.
# The journal must be QUIESCENT when judged — a live server can still
# commit tenant records or finish a compaction between the guard's scan
# and the stop, and a drain flushes queued writes on SIGTERM. Stop first,
# then check, then swap.
if ! systemctl stop durable-server; then
  # A failed stop with the unit still active means the journal is NOT
  # quiescent — judging it would race live commits. Refuse loudly; if
  # the unit actually went down, the stop did its job and we continue.
  if systemctl is-active --quiet durable-server; then
    echo "rollback: stop failed and the unit is still active — journal not quiescent" >&2
    exit 1
  fi
  echo "rollback: stop reported an error but the unit is down — continuing" >&2
fi
# Files-only: the unit is down, so anything answering the port is a
# squatter — the diagnostics path would leak the admin key and could
# feed arithmetic-injection JSON into the guard's merges.
if ! FORMAT_GUARD_FILES_ONLY=1 deploy/format-guard.sh /usr/local/bin/durable-server.prev; then
  if ! systemctl start durable-server; then
    echo "rollback refused AND the restart failed — the service is DOWN" >&2
    exit 1
  fi
  echo "rollback refused — the running binary was restarted" >&2
  exit 1
fi
# cp -a, not mv: .prev stays in place, so a rollback that fails to start
# can be retried and the saved binary is never destroyed.
cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
if ! systemctl start durable-server; then
  echo "rollback: start failed — the service is DOWN with the previous binary installed; investigate: journalctl -u durable-server" >&2
  exit 1
fi
for _ in $(seq 240); do
  [[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)" == 200 ]] \
    && { deploy/smoke.sh --local-only; echo "rollback ok"; exit 0; }
  sleep 0.25
done
echo "rollback: /ready never came up — investigate: journalctl -u durable-server" >&2
exit 1
