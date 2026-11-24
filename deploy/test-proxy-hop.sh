#!/usr/bin/env bash
# White-hat finding B (+ A's deploy half): the deploy keys visitors by the
# client Caddy resolved, and passes the operator's --deny-cidr ranges.
#   - durable-run.sh trusts the loopback hop (127.0.0.1/32, ::1/128) for a
#     loopback listener, never for a public one, and turns
#     $DURABLE_ETC/deny-cidrs into --deny-cidr flags;
#   - deploy/Caddyfile resolves the client from CF-Connecting-IP and
#     overwrites the header with it before proxying.
# Each check is also run against a known-bad copy and must fail there.
#
#   deploy/test-proxy-hop.sh
set -euo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-hop.XXXXXX")"
trap 'rm -rf "$work"' EXIT

# A stub server that records its argv, one argument per line.
cat >"$work/stub" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$STUB_OUT"
STUB
chmod +x "$work/stub"
mkdir -p "$work/etc"
printf '203.0.113.7/32   # the reserved IP\n\n# a comment line\n2001:db8:7::/48\n' >"$work/etc/deny-cidrs"

argv_of() { # script listen → the argv file
  STUB_OUT="$work/argv" DURABLE_ETC="$work/etc" DURABLE_SERVER_BIN="$work/stub" \
    DURABLE_LISTEN="$2" bash "$1"
  cat "$work/argv"
}
has_pair() { # file flag value
  local prev="" line
  while IFS= read -r line; do
    [[ "$prev" == "$1" && "$line" == "$2" ]] && return 0
    prev="$line"
  done
  return 1
}

check_run() { # script → 0 when every rule holds
  local s="$1" ok=0
  argv_of "$s" 127.0.0.1:58300 >"$work/loop"
  has_pair --trusted-proxy 127.0.0.1/32 <"$work/loop" || { echo "  no 127.0.0.1/32 hop"; ok=1; }
  has_pair --trusted-proxy ::1/128 <"$work/loop" || { echo "  no ::1/128 hop"; ok=1; }
  has_pair --deny-cidr 203.0.113.7/32 <"$work/loop" || { echo "  deny-cidrs line 1 not passed"; ok=1; }
  has_pair --deny-cidr 2001:db8:7::/48 <"$work/loop" || { echo "  deny-cidrs line 2 not passed"; ok=1; }
  local n
  n=$(grep -cx -- '--deny-cidr' "$work/loop" || true)
  [[ "$n" == 2 ]] || { echo "  $n --deny-cidr flags, want 2 (a comment or blank line leaked)"; ok=1; }
  argv_of "$s" 0.0.0.0:58300 >"$work/public"
  if has_pair --trusted-proxy 127.0.0.1/32 <"$work/public"; then
    echo "  a public listener trusts loopback"; ok=1
  fi
  return "$ok"
}

check_caddy() { # file → 0 when Caddy resolves and overwrites the header
  local f="$1" ok=0
  # Zone-level AOP with OUR CA, never Cloudflare's shared global one.
  grep -Eq '^[[:space:]]*trusted_ca_cert_file[[:space:]]+/etc/caddy/aop-zone-ca\.pem[[:space:]]*$' "$f" \
    || { echo "  client_auth does not trust our zone AOP CA"; ok=1; }
  if grep -q 'cloudflare-origin-pull-ca' "$f"; then
    echo "  the global Cloudflare origin-pull CA is trusted"; ok=1
  fi
  grep -Eq '^[[:space:]]*client_ip_headers[[:space:]]+CF-Connecting-IP[[:space:]]*$' "$f" \
    || { echo "  no client_ip_headers CF-Connecting-IP"; ok=1; }
  grep -Eq '^[[:space:]]*header_up[[:space:]]+CF-Connecting-IP[[:space:]]+\{client_ip\}[[:space:]]*$' "$f" \
    || { echo "  CF-Connecting-IP is not overwritten with {client_ip}"; ok=1; }
  return "$ok"
}

fails=0
echo "durable-run.sh"
check_run deploy/durable-run.sh || fails=1
echo "Caddyfile"
check_caddy deploy/Caddyfile || fails=1

# Known-bad proofs: each check must FAIL on a copy with its rule removed.
grep -v -- '--trusted-proxy 127.0.0.1/32 --trusted-proxy ::1/128' deploy/durable-run.sh >"$work/bad-run.sh"
if check_run "$work/bad-run.sh" >/dev/null; then
  echo "FAIL: the durable-run check passed without the hop"; fails=1
fi
sed 's/--deny-cidr "\$cidr"/--deny-cidr-ignored "$cidr"/' deploy/durable-run.sh >"$work/bad-deny.sh"
if check_run "$work/bad-deny.sh" >/dev/null; then
  echo "FAIL: the durable-run check passed without the deny-cidrs"; fails=1
fi
grep -v 'header_up CF-Connecting-IP' deploy/Caddyfile >"$work/bad-Caddyfile"
if check_caddy "$work/bad-Caddyfile" >/dev/null; then
  echo "FAIL: the Caddyfile check passed without the overwrite"; fails=1
fi
sed 's#/etc/caddy/aop-zone-ca.pem#/etc/caddy/cloudflare-origin-pull-ca.pem#' deploy/Caddyfile >"$work/bad-aop"
if check_caddy "$work/bad-aop" >/dev/null; then
  echo "FAIL: the Caddyfile check passed trusting the global origin-pull CA"; fails=1
fi

if [[ "$fails" -ne 0 ]]; then
  echo "test-proxy-hop: FAILED" >&2
  exit 1
fi
echo "test-proxy-hop: the hop is trusted, the header is Caddy's, AOP is zone-level, deny-cidrs pass through (and each check goes red without its rule)"
