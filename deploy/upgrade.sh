#!/usr/bin/env bash
# upgrade.sh — swap in a new release build and restart, < 2 min total with
# < 5 s client-visible unavailability while the unit is down (Caddy's
# lb_try_duration 15s covers the dial-refused window; the restart table in
# docs/DEPLOY.md). On a failed start or /ready the previous binary is
# restored (never a lab build), waited for and smoked.
#   sudo deploy/upgrade.sh path/to/durable-server[-new]
set -euo pipefail
new=${1:?usage: upgrade.sh /path/to/new/durable-server}
[[ -x $new ]] || { echo "$new not executable" >&2; exit 1; }
guard="$(dirname "$0")/lab-build-guard.sh"
# Before anything else, root or not: never swap in a lab build (S3).
"$guard" "$new" || exit 1
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

# One root-owned copy is judged and installed (S3 re-review): the path can
# be rebuilt (a lab build into the same target/release) between a check of
# it and the install. Nothing below reads the original path again.
stage=$(mktemp /usr/local/bin/.durable-server.upgrade.XXXXXX)
trap 'rm -f "$stage"' EXIT
install -m 0755 "$new" "$stage"
"$guard" "$stage" || exit 1

wait_ready() { # /ready polls until replay finishes (bounded: 90s covers
  # 800k runs at the measured 1.7s on a droplet slower than this Mac).
  local code
  for _ in $(seq 360); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready 2>/dev/null || true)
    [[ "$code" == 200 ]] && return 0
    sleep 0.25
  done
  return 1
}

# Put the previous binary back: never a lab build (S3), then start it, wait
# for /ready and smoke it — the restore paths used to skip both.
restore_prev() { # start|restart
  if ! "$guard" /usr/local/bin/durable-server.prev; then
    echo "upgrade: the previous binary is a LAB build, not restored — the service is DOWN" >&2
    exit 1
  fi
  cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server
  if ! systemctl "$1" durable-server; then
    echo "upgrade: the restored binary failed to start too — the service is DOWN; investigate: journalctl -u durable-server" >&2
    exit 1
  fi
  if wait_ready; then
    deploy/smoke.sh --local-only || echo "upgrade: the restored binary is up but smoke failed" >&2
  else
    echo "upgrade: the restored binary never became ready — investigate: journalctl -u durable-server" >&2
  fi
}

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
if ! FORMAT_GUARD_FILES_ONLY=1 deploy/format-guard.sh "$stage"; then
  if ! systemctl start durable-server; then
    echo "upgrade refused AND the restart failed — the service is DOWN" >&2
    exit 1
  fi
  echo "upgrade refused — the previous binary was restarted" >&2
  exit 1
fi

cp -a /usr/local/bin/durable-server /usr/local/bin/durable-server.prev
install -m 0755 "$stage" /usr/local/bin/durable-server
if ! systemctl start durable-server; then
  # The start failed while the NEW binary is installed: restore the
  # previous one and bring it up before reporting down.
  echo "upgrade: start failed — restoring the previous binary" >&2
  restore_prev start
  echo "upgrade failed — the previous binary was restored and started" >&2
  exit 1
fi
if ! wait_ready; then
  echo "upgrade: /ready never came up — rolling back" >&2
  restore_prev restart
  exit 1
fi
deploy/smoke.sh --local-only
echo "upgrade ok (previous binary kept at /usr/local/bin/durable-server.prev)"
