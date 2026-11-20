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
systemctl stop durable-server
if ! deploy/format-guard.sh /usr/local/bin/durable-server.prev; then
  systemctl start durable-server
  echo "rollback refused — the running binary was restarted" >&2
  exit 1
fi
mv /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
systemctl start durable-server
for _ in $(seq 240); do
  [[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)" == 200 ]] \
    && { deploy/smoke.sh --local-only; echo "rollback ok"; exit 0; }
  sleep 0.25
done
echo "rollback: /ready never came up — investigate: journalctl -u durable-server" >&2
exit 1
