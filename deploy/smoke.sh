#!/usr/bin/env bash
# smoke.sh — after install and every upgrade: is the droplet serving safely?
# Run on the droplet as root. --local-only skips the through-Cloudflare
# checks (used by upgrade.sh/rollback.sh). Exits non-zero on any failure.
# FAILS when /etc/durable/trusted-proxies is absent or empty: without
# Cloudflare's ranges the per-address limit (D-052) rates the edge, not
# the client, and CF-Connecting-IP is attacker-controlled.
# shellcheck disable=SC2015  # ok and bad only print, so `t && ok || bad` is if-then-else here.
set -uo pipefail
failures=0
ok()  { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

echo "the binary"
# A lab build turns the tenant SSRF guard off (S3 re-review 3). Judge the
# binary systemd is RUNNING (/proc/<MainPID>/exe), never a path from the
# environment. DURABLE_PROC_ROOT exists for deploy/test-lab-guard.sh only.
proc=${DURABLE_PROC_ROOT:-/proc}
pid=$(systemctl show -p MainPID --value durable-server 2>/dev/null || echo 0)
if [[ "$pid" =~ ^[0-9]+$ ]] && (( pid > 0 )); then
  "$(dirname "$0")/lab-build-guard.sh" "$proc/$pid/exe" 2>/dev/null \
    && ok "the running durable-server is a production build" \
    || bad "the running durable-server is a LAB build or unreadable — reinstall a plain cargo build --release"
else
  bad "no running durable-server to judge (MainPID $pid)"
fi

echo "services"
for unit in durable-server caddy; do
  systemctl is-active --quiet "$unit" && ok "$unit active" || bad "$unit not active"
done
systemctl is-active --quiet durable-backup.timer && ok "durable-backup.timer active" || bad "durable-backup.timer not active"

echo "the server, on loopback"
[[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:58300/health)" == 200 ]] \
  && ok "/health 200" || bad "/health not 200"
ready=$(curl -sS --max-time 10 http://127.0.0.1:58300/ready 2>/dev/null || true)
[[ "$ready" == *ready* || "$ready" == *true* ]] && ok "/ready ready" || bad "/ready: $ready"

echo "rate-limit keying (D-052)"
if [[ -s /etc/durable/trusted-proxies ]]; then
  n=$(grep -c '/' /etc/durable/trusted-proxies)
  (( n >= 10 )) && ok "trusted-proxies: $n Cloudflare ranges" || bad "only $n trusted ranges — rerun deploy/cloudflare-ips.sh"
else
  bad "/etc/durable/trusted-proxies missing — CF-Connecting-IP untrusted, run deploy/cloudflare-ips.sh"
fi
# The server sees only the Caddy hop: Caddy must overwrite the header
# with the client it resolved, and the server must trust that hop
# (durable-run adds it for a loopback listener) — else every visitor
# shares 127.0.0.1's bucket (white-hat finding B).
if grep -q 'header_up CF-Connecting-IP {client_ip}' /etc/caddy/Caddyfile 2>/dev/null; then
  ok "Caddy overwrites CF-Connecting-IP with its resolved client"
else
  bad "/etc/caddy/Caddyfile does not overwrite CF-Connecting-IP — install deploy/Caddyfile"
fi
# Zone-level Authenticated Origin Pulls with OUR CA (CREATE.md 3): the
# global Cloudflare origin-pull CA would admit any Cloudflare zone.
if grep -q 'trusted_ca_cert_file /etc/caddy/aop-zone-ca.pem' /etc/caddy/Caddyfile 2>/dev/null \
  && [[ -s /etc/caddy/aop-zone-ca.pem ]]; then
  ok "Caddy requires our zone-level origin-pull certificate"
else
  bad "Caddy does not require OUR zone AOP CA (/etc/caddy/aop-zone-ca.pem) — CREATE.md 3"
fi
argv=""
if [[ "$pid" =~ ^[0-9]+$ ]] && (( pid > 0 )); then
  argv=$(tr '\0' ' ' <"$proc/$pid/cmdline" 2>/dev/null || true)
fi
if [[ "$argv" == *"--trusted-proxy 127.0.0.1/32"* ]]; then
  ok "durable-server trusts the loopback proxy hop"
else
  bad "durable-server does not trust the loopback hop — install deploy/durable-run.sh"
fi

echo "backups"
if command -v durable-backup >/dev/null; then
  latest=$(durable-backup latest 2>/dev/null || echo none)
  hours=${latest##* }
  if [[ "$latest" != none && "$hours" =~ ^[0-9]+$ ]] && (( hours < 26 )); then
    ok "newest R2 backup ${hours}h old (${latest%% *})"
  else
    bad "no R2 backup in the last 26h ($latest)"
  fi
fi

echo "memory (record at exit gate)"
m=$(systemctl show durable-server -p MemoryCurrent --value 2>/dev/null || echo "")
[[ "$m" =~ ^[0-9]+$ ]] && ok "durable-server uses $((m/1024/1024)) MiB" || bad "no MemoryCurrent"

if [[ "${1:-}" != --local-only ]]; then
  echo "through Cloudflare"
  dom=$(sed -n 's/^DURABLE_PUBLIC_DOMAIN=//p' /etc/durable/durable.env 2>/dev/null)
  dom=${dom:-durable.404human.dev}
  code=$(curl -sS -o /dev/null -w '%{http_code} %{ssl_verify_result}' --max-time 10 "https://$dom/health" || echo "000 x")
  [[ "$code" == "200 0" ]] && ok "$dom/health 200 over valid TLS" || bad "$dom/health: $code"
  headers=$(curl -sSI --max-time 10 "https://$dom/inspector/" || true)
  grep -qi '^server: cloudflare' <<<"$headers" && ok "served via Cloudflare" || bad "no server: cloudflare header"
  grep -qi '^content-security-policy:' <<<"$headers" && ok "inspector has a CSP" || bad "no CSP on /inspector/"
  unauth=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 -X POST "https://$dom/jobs" -d '{}' || echo 000)
  [[ "$unauth" == 401 ]] && ok "POST /jobs without a key is 401" || bad "POST /jobs without a key: $unauth"
fi

(( failures == 0 )) && echo "smoke: all green" || { echo "smoke: $failures failure(s)"; exit 1; }
