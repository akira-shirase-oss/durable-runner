#!/usr/bin/env bash
# Cloudflare's published address ranges, applied on the droplet:
#   - Caddy trusts them so it forwards the visitor's address
#     (/etc/caddy/cloudflare-trusted.caddy);
#   - durable-server trusts them for CF-Connecting-IP, so the per-address
#     rate limit counts the real client, not the edge
#     (/etc/durable/trusted-proxies, read by durable-run);
#   - ufw lets only them reach 443 (SSH stays as you set it).
# Run as root after install and whenever Cloudflare changes its list
# (rarely; a monthly timer is fine). Refuses to write anything from a
# list that doesn't look like address ranges. smoke.sh FAILS when
# /etc/durable/trusted-proxies is missing — D-052's per-address limit
# is silently per-edge without it.
#
#   sudo deploy/cloudflare-ips.sh [--print]
set -euo pipefail
v4=$(curl -fsS --max-time 10 https://www.cloudflare.com/ips-v4)
v6=$(curl -fsS --max-time 10 https://www.cloudflare.com/ips-v6)
ranges=$(printf '%s\n%s\n' "$v4" "$v6" | tr -d '\r' | sed '/^$/d')
count=$(wc -l <<<"$ranges" | tr -d ' ')
while IFS= read -r range; do
  [[ "$range" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$ || "$range" =~ ^[0-9a-f:]+/[0-9]{1,3}$ ]] \
    || { echo "error: '$range' isn't an address range; nothing written" >&2; exit 1; }
done <<<"$ranges"
(( count >= 10 )) || { echo "error: only $count ranges from Cloudflare; nothing written" >&2; exit 1; }

if [[ "${1:-}" == --print ]]; then
  echo "$ranges"
  exit 0
fi
[[ $EUID -eq 0 ]] || { echo "error: run as root (or pass --print)" >&2; exit 2; }

printf 'trusted_proxies static %s\n' "$(paste -sd' ' <<<"$ranges")" >/etc/caddy/cloudflare-trusted.caddy
echo "$ranges" >/etc/durable/trusted-proxies
chmod 0644 /etc/durable/trusted-proxies

# ufw: 443 from Cloudflare only. Existing rules for 443 are replaced; SSH is left alone.
while ufw status numbered | grep -qE '\b443(/tcp)?\b'; do
  n=$(ufw status numbered | grep -E '\b443(/tcp)?\b' | head -1 | sed -E 's/^\[ *([0-9]+)\].*/\1/')
  yes | ufw delete "$n" >/dev/null
done
while IFS= read -r range; do
  ufw allow proto tcp from "$range" to any port 443 comment 'cloudflare' >/dev/null
done <<<"$ranges"

echo "cloudflare-ips: $count ranges trusted by Caddy and durable-server, and allowed to 443"
echo "next: sudo systemctl reload caddy && sudo systemctl restart durable-server"
