#!/usr/bin/env bash
# durable-run — expand the env file into durable-server's argv. systemd
# can't repeat a flag per trusted-proxy CIDR, so this wrapper reads
# /etc/durable/trusted-proxies (one CIDR per line, written by
# deploy/cloudflare-ips.sh) and /etc/durable/handlers (handler=name=url
# per line) and execs the server with them.
set -euo pipefail

args=(--journal "${DURABLE_JOURNAL:-/var/lib/durable/journal}"
      --listen "${DURABLE_LISTEN:-127.0.0.1:58300}"
      --rate-limit-per-addr "${DURABLE_RATE_LIMIT_PER_ADDR:-100}"
      --max-conns-per-addr "${DURABLE_MAX_CONNS_PER_ADDR:-64}"
      --max-conns-per-proxy "${DURABLE_MAX_CONNS_PER_PROXY:-4096}")

if [[ -n "${DURABLE_BACKUP_ROOT:-}" ]]; then
  args+=(--backup-root "$DURABLE_BACKUP_ROOT")
fi
if [[ -n "${DURABLE_UI_DIR:-}" ]]; then
  args+=(--ui-dir "$DURABLE_UI_DIR")
fi
if [[ -f /etc/durable/trusted-proxies ]]; then
  while IFS= read -r cidr; do
    [[ -n "$cidr" ]] && args+=(--trusted-proxy "$cidr")
  done </etc/durable/trusted-proxies
fi
if [[ -f /etc/durable/handlers ]]; then
  while IFS= read -r h; do
    [[ -n "$h" && "$h" != \#* ]] && args+=(--handler "$h")
  done </etc/durable/handlers
fi

# Extra flags, e.g. DURABLE_EXTRA_ARGS="--max-live-runs 2000000".
# shellcheck disable=SC2086
exec /usr/local/bin/durable-server "${args[@]}" ${DURABLE_EXTRA_ARGS:-}
