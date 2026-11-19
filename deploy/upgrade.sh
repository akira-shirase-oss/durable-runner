#!/usr/bin/env bash
# upgrade.sh — swap in a new release build and restart, < 2 min total with
# < 5 s client-visible unavailability while the unit is down (Caddy's
# lb_try_duration 15s covers the dial-refused window; the restart table in
# docs/DEPLOY.md). On a failed smoke the previous binary is restored.
#   sudo deploy/upgrade.sh path/to/durable-server[-new]
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
new=${1:?usage: upgrade.sh /path/to/new/durable-server}
[[ -x $new ]] || { echo "$new not executable" >&2; exit 1; }

# D-056: never swap in a binary that cannot read this journal — it would
# refuse startup and turn the upgrade into an outage.
deploy/format-guard.sh "$new"

cp -a /usr/local/bin/durable-server /usr/local/bin/durable-server.prev
install -m 0755 "$new" /usr/local/bin/durable-server
systemctl restart durable-server

# /ready polls until replay finishes (bounded: 90s covers 800k runs at the
# measured 1.7s on a droplet slower than this Mac — generous headroom).
for _ in $(seq 360); do
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)
  [[ "$code" == 200 ]] && break
  sleep 0.25
done
if [[ "${code:-000}" != 200 ]]; then
  echo "upgrade: /ready never came up — rolling back" >&2
  mv /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
  systemctl restart durable-server
  exit 1
fi
deploy/smoke.sh --local-only
echo "upgrade ok (previous binary kept at /usr/local/bin/durable-server.prev)"
