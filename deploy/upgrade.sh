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
# refuse startup and turn the upgrade into an outage. The journal must be
# quiescent when judged, so the unit stops first; a refused upgrade
# restarts the running binary unchanged.
if ! systemctl stop durable-server; then
  # If the unit is still active after a failed stop the journal is not
  # quiescent — refuse rather than race live commits.
  if systemctl is-active --quiet durable-server; then
    echo "upgrade: stop failed and the unit is still active — journal not quiescent" >&2
    exit 1
  fi
  echo "upgrade: stop reported an error but the unit is down — continuing" >&2
fi
# Files-only: the unit is down, so anything answering the port is a
# squatter — the diagnostics path would leak the admin key and could
# feed arithmetic-injection JSON into the guard's merges.
if ! FORMAT_GUARD_FILES_ONLY=1 deploy/format-guard.sh "$new"; then
  if ! systemctl start durable-server; then
    echo "upgrade refused AND the restart failed — the service is DOWN" >&2
    exit 1
  fi
  echo "upgrade refused — the previous binary was restarted" >&2
  exit 1
fi

cp -a /usr/local/bin/durable-server /usr/local/bin/durable-server.prev
install -m 0755 "$new" /usr/local/bin/durable-server
if ! systemctl start durable-server; then
  # The start failed while the NEW binary is installed: restore the
  # previous one and bring it up before reporting down.
  echo "upgrade: start failed — restoring the previous binary" >&2
  cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
  if ! systemctl start durable-server; then
    echo "upgrade: the restored binary failed to start too — the service is DOWN; investigate: journalctl -u durable-server" >&2
    exit 1
  fi
  echo "upgrade failed — the previous binary was restored and started" >&2
  exit 1
fi

# /ready polls until replay finishes (bounded: 90s covers 800k runs at the
# measured 1.7s on a droplet slower than this Mac — generous headroom).
for _ in $(seq 360); do
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)
  [[ "$code" == 200 ]] && break
  sleep 0.25
done
if [[ "${code:-000}" != 200 ]]; then
  echo "upgrade: /ready never came up — rolling back" >&2
  cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
  if ! systemctl restart durable-server; then
    echo "upgrade: the rollback failed to start — the service is DOWN; investigate: journalctl -u durable-server" >&2
    exit 1
  fi
  exit 1
fi
deploy/smoke.sh --local-only
echo "upgrade ok (previous binary kept at /usr/local/bin/durable-server.prev)"
