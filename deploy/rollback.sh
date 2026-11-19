#!/usr/bin/env bash
# rollback.sh — restore the binary upgrade.sh saved and restart, < 1 min.
# The journal is forward-compatible within v1 (D-054's reader accepts every
# shipped format), so a rollback replays the same journal the new binary wrote.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
[[ -f /usr/local/bin/durable-server.prev ]] || { echo "no .prev binary — nothing to roll back to" >&2; exit 1; }
mv /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
systemctl restart durable-server
for _ in $(seq 240); do
  [[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)" == 200 ]] \
    && { deploy/smoke.sh --local-only; echo "rollback ok"; exit 0; }
  sleep 0.25
done
echo "rollback: /ready never came up — investigate: journalctl -u durable-server" >&2
exit 1
