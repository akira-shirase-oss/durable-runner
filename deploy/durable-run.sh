#!/usr/bin/env bash
# durable-run — expand the env file into durable-server's argv. systemd
# can't repeat a flag per CIDR, so this wrapper reads, from
# $DURABLE_ETC (default /etc/durable):
#   trusted-proxies  one CIDR per line (deploy/cloudflare-ips.sh)
#   deny-cidrs       one CIDR or address per line, `#` comments: ranges a
#                    handler call never dials besides the local blocks and
#                    this host's own interface addresses — list the
#                    droplet's reserved/floating IP here (it is not on any
#                    interface, so the server cannot find it itself)
#   handlers         handler=name=url per line
# and execs the server with them.
#
# The proxy hop: Caddy is the server's only client and connects over
# loopback, so with a loopback listener the hop (127.0.0.1/32, ::1/128) is
# always trusted for CF-Connecting-IP — Caddy overwrites that header with
# the client address it resolved from Cloudflare (deploy/Caddyfile), so a
# visitor cannot choose its own rate bucket. Without the hop every visitor
# shares 127.0.0.1's bucket (white-hat finding B).
set -euo pipefail

etc="${DURABLE_ETC:-/etc/durable}"
listen="${DURABLE_LISTEN:-127.0.0.1:58300}"
args=(--journal "${DURABLE_JOURNAL:-/var/lib/durable/journal}"
      --listen "$listen"
      --rate-limit-per-addr "${DURABLE_RATE_LIMIT_PER_ADDR:-100}"
      --max-conns-per-addr "${DURABLE_MAX_CONNS_PER_ADDR:-64}"
      --max-conns-per-proxy "${DURABLE_MAX_CONNS_PER_PROXY:-4096}")

if [[ -n "${DURABLE_BACKUP_ROOT:-}" ]]; then
  args+=(--backup-root "$DURABLE_BACKUP_ROOT")
fi
if [[ -n "${DURABLE_UI_DIR:-}" ]]; then
  args+=(--ui-dir "$DURABLE_UI_DIR")
fi
case "$listen" in
  127.*|localhost:*|\[::1\]:*)
    args+=(--trusted-proxy 127.0.0.1/32 --trusted-proxy ::1/128) ;;
esac
if [[ -f "$etc/trusted-proxies" ]]; then
  while IFS= read -r cidr; do
    [[ -n "$cidr" ]] && args+=(--trusted-proxy "$cidr")
  done <"$etc/trusted-proxies"
fi
if [[ -f "$etc/deny-cidrs" ]]; then
  while IFS= read -r cidr; do
    cidr="${cidr%%#*}"
    cidr="${cidr//[[:space:]]/}"
    [[ -n "$cidr" ]] && args+=(--deny-cidr "$cidr")
  done <"$etc/deny-cidrs"
fi
if [[ -f "$etc/handlers" ]]; then
  while IFS= read -r h; do
    [[ -n "$h" && "$h" != \#* ]] && args+=(--handler "$h")
  done <"$etc/handlers"
fi

# Extra flags, e.g. DURABLE_EXTRA_ARGS="--max-live-runs 2000000".
# shellcheck disable=SC2086
exec "${DURABLE_SERVER_BIN:-/usr/local/bin/durable-server}" "${args[@]}" ${DURABLE_EXTRA_ARGS:-}
