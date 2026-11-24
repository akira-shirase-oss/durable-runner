#!/usr/bin/env bash
# smoke.sh — is the hosted Durable Runner on the shared droplet serving safely? Run as root.
#
#   smoke.sh --release   the checks direct/droplet-deploy.sh rolls back on: this release's binary
#                        and process, loopback health, the hosted settings it runs with
#   smoke.sh             those, plus the host (Caddy's merge, the origin certificate, Authenticated
#                        Origin Pulls) and the public URL through Cloudflare
#
# It copies the relevant checks of deploy/smoke.sh (the self-hosted droplet's), which assumes that
# droplet's layout: a root Caddyfile of its own, Cloudflare's ranges in trusted-proxies, a backup
# timer. Here the root is Colophon's, the server trusts the loopback hop alone, and the release
# lives in /opt/durable/current. It reads the process's environment for variable NAMES only and
# never prints a value. Exits non-zero on any failure.
# shellcheck disable=SC2015  # ok and bad only print, so `t && ok || bad` is if-then-else here.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=deploy/droplet/lib.sh
. "$here/lib.sh"
mode=${1:-full}
case "$mode" in --release|full) ;; *) echo "usage: smoke.sh [--release]" >&2; exit 2 ;; esac
failures=0
ok()  { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }
LOCAL=http://127.0.0.1:$DR_PUBLIC_PORT
bin=$DR_BIN/durable-server
need_accounts=$(env_get DURABLE_REQUIRE_ACCOUNTS "$DR_ENV"); need_accounts=${need_accounts:-1}
need_mail=$(env_get DURABLE_REQUIRE_MAIL "$DR_ENV"); need_mail=${need_mail:-1}
origin=$(env_get DURABLE_PUBLIC_ORIGIN "$DR_ENV")
host=$(origin_host "$origin" 2>/dev/null || true)

echo "the release"
# Judge the binary systemd is RUNNING (/proc/<MainPID>/exe), never a path from the environment,
# as deploy/smoke.sh does. DURABLE_PROC_ROOT is for the tests only.
proc_root=${DURABLE_PROC_ROOT:-$ROOT/proc}
systemctl is-active --quiet "$DR_UNIT" && ok "$DR_UNIT active" || bad "$DR_UNIT not active"
pid=$(unit_main_pid)
proc=$proc_root/$pid
if (( pid > 0 )) && [[ -e "$proc/exe" ]]; then
  "$here/../lab-build-guard.sh" "$proc/exe" >/dev/null 2>&1 \
    && ok "the running durable-server is a production build" \
    || bad "the running durable-server is a LAB build or unreadable (--allow-local-tenant-handlers turns the tenant SSRF guard off)"
  running=$(readlink -f "$proc/exe" 2>/dev/null || true)
  [[ -n "$running" && "$running" == "$(readlink -f "$bin")" ]] \
    && ok "it runs $bin as installed" \
    || bad "it runs ${running:-something unknown}, not the installed $bin (restart it)"
else
  bad "no running durable-server to judge (MainPID $pid)"
fi

echo "the server, on loopback"
[[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$LOCAL/health" 2>/dev/null)" == 200 ]] \
  && ok "/health 200" || bad "/health not 200"
ready=$(curl -sS --max-time 10 "$LOCAL/ready" 2>/dev/null || true)
[[ "$ready" == *'"ready":true'* ]] && ok "/ready ready" || bad "/ready: ${ready:-no answer}"

echo "what it runs with"
argv=""; envnames=""
if (( pid > 0 )) && [[ -r "$proc/cmdline" ]]; then
  argv=$(tr '\0' ' ' <"$proc/cmdline" 2>/dev/null || true)
  # Names only: everything after the first = is cut before anything else sees it.
  envnames=$(tr '\0' '\n' <"$proc/environ" 2>/dev/null | cut -d= -f1 || true)
fi
has_env() { grep -qx -- "$1" <<<"$envnames"; }
[[ "$argv" == *"--trusted-proxy 127.0.0.1/32"* ]] && ok "trusts the loopback proxy hop (Caddy)" \
  || bad "does not trust the loopback hop: every visitor would share one rate bucket"
if [[ -n "$host" ]]; then
  [[ "$argv" == *"--public-origin https://$host"* ]] && ok "--public-origin https://$host" \
    || bad "not started with --public-origin https://$host (mailed links need it)"
else
  bad "DURABLE_PUBLIC_ORIGIN in $DR_ENV isn't https://<host>"
fi
[[ "$argv" != *--allow-local-tenant-handlers* ]] && ok "no lab flag in argv" || bad "a lab flag is in argv"
if [[ "$need_accounts" == 1 ]]; then
  has_env DATABASE_URL && ok "the account store is configured (DATABASE_URL)" || bad "no DATABASE_URL in the process: the hosted service needs the account store"
  inv=$(systemctl show -p InvocationID --value "$DR_UNIT" 2>/dev/null || true)
  logs=""
  [[ "$inv" =~ ^[0-9a-f]{32}$ ]] && logs=$(journalctl -q -o cat "_SYSTEMD_INVOCATION_ID=$inv" 2>/dev/null || true)
  if [[ "$logs" == *"account store connected, migrations applied"* ]]; then
    ok "the account store answered at start (Neon, migrations applied)"
  else
    bad "no 'account store connected' since this start: journalctl -u $DR_UNIT (Neon unreachable, or the TLS check failed)"
  fi
fi
if [[ "$need_mail" == 1 ]]; then
  has_env RESEND_API_KEY && has_env DURABLE_MAIL_FROM && ok "mail goes through Resend" || bad "no RESEND_API_KEY / DURABLE_MAIL_FROM in the process"
fi
if has_env DURABLE_MAILER; then bad "DURABLE_MAILER is set in the process (log would print mail bodies)"; else ok "no DURABLE_MAILER override"; fi
m=$(systemctl show "$DR_UNIT" -p MemoryCurrent --value 2>/dev/null || echo "")
is_uint "$m" && ok "memory: $((m / 1024 / 1024)) MiB" || ok "memory: not reported"

if [[ "$mode" == full ]]; then
  echo "the host"
  systemctl is-active --quiet caddy && ok "caddy active" || bad "caddy not active"
  n=$(grep -cxF 'import /etc/caddy/sites/*.caddy' "$CADDY_DIR/Caddyfile" 2>/dev/null || true)
  [[ "$n" == 1 ]] && ok "the root Caddyfile imports sites/*.caddy once" \
    || bad "the root Caddyfile imports sites/*.caddy $n times (Colophon's installer rewrote it? run caddy-merge.sh)"
  site=$CADDY_DIR/sites/durable-runner.caddy
  dash=$(env_get DURABLE_DASHBOARD_PATH "$DR_ENV"); dash=${dash:-$DR_DASHBOARD_SHIPPED}
  want=$(render_site "$here/durable-runner.caddy" "${host:-x.invalid}" "$dash" 2>/dev/null || true)
  [[ -n "$want" && "$want" == "$(cat "$site" 2>/dev/null)" ]] && ok "$site is this release's (host $host, / -> $dash)" \
    || bad "$site differs from this release's: run caddy-merge.sh"
  pem=$CADDY_DIR/durable-origin.pem
  if [[ -s "$pem" ]] && openssl x509 -noout -checkhost "${host:-x.invalid}" -in "$pem" 2>/dev/null | grep -q 'does match'; then
    openssl x509 -noout -checkend $((30 * 86400)) -in "$pem" >/dev/null 2>&1 && ok "origin certificate covers $host" \
      || bad "the origin certificate expires within 30 days"
  else
    bad "$pem is missing or doesn't cover $host"
  fi
  trust=$CADDY_DIR/origin-pull-trust.caddy
  if [[ ! -s "$trust" ]]; then
    bad "$trust is missing: Authenticated Origin Pulls can't be required"
  elif grep -q cloudflare-origin-pull-ca.pem "$trust"; then
    bad "Authenticated Origin Pulls still trust Cloudflare's SHARED certificate (any Cloudflare zone gets in): Colophon's origin-pull-ca.sh --custom-only, once our zone certificate is enabled"
  else
    ok "Authenticated Origin Pulls trust our own zone certificate only"
  fi
  [[ ! -s "$DR_ETC/trusted-proxies" ]] && ok "the server trusts no ranges beyond the loopback hop" \
    || bad "$DR_ETC/trusted-proxies is not empty: on this host only Caddy (loopback) may name the client"

  echo "through Cloudflare"
  if [[ -n "$host" ]]; then
    code=$(curl -sS -o /dev/null -w '%{http_code} %{ssl_verify_result}' --max-time 10 "https://$host/health" 2>/dev/null || echo "000 x")
    [[ "$code" == "200 0" ]] && ok "https://$host/health 200 over valid TLS" || bad "https://$host/health: $code"
    headers=$(curl -sSI --max-time 10 "https://$host$dash" 2>/dev/null | tr -d '\r' || true)
    grep -qi '^server: cloudflare' <<<"$headers" && ok "served via Cloudflare" || bad "no server: cloudflare header"
    grep -qiE '^HTTP/[0-9.]+ 200' <<<"$headers" && ok "$dash 200" || bad "$dash not 200"
    grep -qi '^strict-transport-security: max-age=31536000$' <<<"$headers" && ok "HSTS, this host only" || bad "no HSTS (or includeSubDomains)"
    unauth=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 -X POST "https://$host/jobs" -H 'content-type: application/json' -d '{}' 2>/dev/null || echo 000)
    [[ "$unauth" == 401 ]] && ok "POST /jobs without a key: 401" || bad "POST /jobs without a key: $unauth"
  fi
fi

(( failures == 0 )) && echo "smoke: all green" || { echo "smoke: $failures failure(s)"; exit 1; }
