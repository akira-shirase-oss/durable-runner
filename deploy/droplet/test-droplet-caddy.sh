#!/usr/bin/env bash
# durable-runner.caddy merged into Colophon's root (testdata/colophon-root.Caddyfile) by
# caddy-merge.sh, then run in the pinned Caddy image (the one Colophon's test-caddy.sh pins).
#
# Static, always (each check is also run on a known-bad copy and must fail there):
#   - a site block only: no global options;
#   - Authenticated Origin Pulls through Colophon's origin-pull-trust.caddy, our own certificate;
#   - CF-Connecting-IP overwritten with {client_ip}; lb_try_duration; an upstream keepalive shorter
#     than the server's 10 s header timeout; HSTS without includeSubDomains; nosniff.
# In Docker (skipped, and said so, when Docker isn't running):
#   - caddy-merge.sh's `caddy validate` is the real one, and it passes; a broken origin certificate
#     is refused for real and the files are restored;
#   - each host gets its own certificate by SNI; Colophon's site still serves beside ours;
#   - no client certificate: refused; ours: served;
#   - from a trusted peer the server is told Cf-Connecting-Ip; from an untrusted one, the peer,
#     whatever the request said; HSTS and a single nosniff; / redirects to the dashboard;
#   - a root that imports sites/*.caddy with no site files still validates (Colophon's side).
#
#   deploy/droplet/test-droplet-caddy.sh
# shellcheck disable=SC2015  # ok and bad only print (both return 0), so `t && ok || bad` is if-then-else here.
set -euo pipefail
kit=$(cd "$(dirname "$0")" && pwd)
IMAGE="caddy:2.10.2@sha256:c3d7ee5d2b11f9dc54f947f68a734c84e9c9666c92c88a7f30b9cba5da182adb"
HOST=durablerunnerapp.404human.dev
CHOST=colophonapp.404human.dev
work=$(mktemp -d "${TMPDIR:-/tmp}/dr-caddy-test.XXXXXX")
name="dr-test-caddy-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT
fails=0
ok() { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fails=$((fails + 1)); }

# check_site <file>: 0 when every static rule holds; prints what's missing.
check_site() {
  local f=$1 rc=0 first ka
  first=$(grep -vE '^[[:space:]]*(#|$)' "$f" | head -n 1)
  [[ "$first" =~ ^[a-z0-9.-]+:443\ \{$ ]] || { echo "    the first directive isn't a site address (global options?): $first"; rc=1; }
  grep -qE '^[[:space:]]*tls[[:space:]]+/etc/caddy/durable-origin\.pem[[:space:]]+/etc/caddy/durable-origin\.key[[:space:]]+\{' "$f" || { echo "    not our own origin certificate"; rc=1; }
  grep -qE '^[[:space:]]*mode[[:space:]]+require_and_verify[[:space:]]*$' "$f" || { echo "    client_auth isn't require_and_verify"; rc=1; }
  grep -qE '^[[:space:]]*import[[:space:]]+/etc/caddy/origin-pull-trust\.caddy[[:space:]]*$' "$f" || { echo "    no origin-pull-trust import"; rc=1; }
  grep -qE '^[[:space:]]*header_up[[:space:]]+CF-Connecting-IP[[:space:]]+\{client_ip\}[[:space:]]*$' "$f" || { echo "    CF-Connecting-IP isn't overwritten with {client_ip}"; rc=1; }
  grep -qE '^[[:space:]]*lb_try_duration[[:space:]]+[0-9]+s' "$f" || { echo "    no lb_try_duration"; rc=1; }
  ka=$(sed -nE 's/^[[:space:]]*keepalive[[:space:]]+([0-9]+)s[[:space:]]*$/\1/p' "$f" | head -n 1)
  if [[ "$ka" =~ ^[0-9]+$ ]] && (( ka < 10 )); then :; else echo "    the upstream keepalive (${ka:-none}) isn't under the server's 10 s header timeout"; rc=1; fi
  grep -qE '^[[:space:]]*header[[:space:]]+Strict-Transport-Security[[:space:]]+"max-age=[0-9]+"[[:space:]]*$' "$f" || { echo "    HSTS missing, or more than max-age (includeSubDomains?)"; rc=1; }
  grep -qE '^[[:space:]]*header[[:space:]]+>X-Content-Type-Options[[:space:]]+nosniff[[:space:]]*$' "$f" || { echo "    no deferred nosniff"; rc=1; }
  return "$rc"
}

echo "the site file, statically"
site=$kit/durable-runner.caddy
check_site "$site" && ok "durable-runner.caddy holds every rule" || bad "durable-runner.caddy breaks a rule (above)"
mutant() { # <description> <sed expression>: the check must fail on the mutated copy
  sed -E "$2" "$site" >"$work/mutant"
  if check_site "$work/mutant" >/dev/null; then bad "the check passed $1"; else ok "the check goes red $1"; fi
}
mutant "with a global options block" '1i\
{\
	auto_https off\
}'
mutant "without the CF-Connecting-IP overwrite" '/header_up CF-Connecting-IP/d'
mutant "with Colophon's certificate" 's#durable-origin#origin#g'
mutant "with a 30 s keepalive" 's/keepalive 5s/keepalive 30s/'
mutant "with includeSubDomains" 's/max-age=31536000"/max-age=31536000; includeSubDomains"/'
mutant "without the origin-pull import" '/origin-pull-trust/d'

if ! docker info >/dev/null 2>&1; then
  echo "  SKIP  Docker isn't running: the merged configuration was not run in Caddy"
  (( fails == 0 )) || { echo "test-droplet-caddy: $fails FAILED" >&2; exit 1; }
  echo "test-droplet-caddy: static checks green (Docker part skipped)"
  exit 0
fi
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null

# The scratch droplet: Colophon's root and files, our origin certificate, our env file.
R=$work/root; etc=$R/etc/caddy
mkdir -p "$etc" "$R/etc/durable" "$work/bin" "$work/state"
cp "$kit/testdata/colophon-root.Caddyfile" "$etc/Caddyfile"
cp "$kit/durable.env.template" "$R/etc/durable/durable.env"
trusted() { printf 'trusted_proxies static %s\n' "$1" >"$etc/cloudflare-trusted.caddy"; }
trusted '0.0.0.0/0 ::/0'
(
  cd "$work"
  openssl req -x509 -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:P-256 -days 2 -subj "/CN=test origin-pull CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" -addext "keyUsage=critical,keyCertSign,cRLSign" -keyout ca.key -out ca.pem 2>/dev/null
  openssl req -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:P-256 -subj "/CN=Cloudflare origin pull" -keyout client.key -out client.csr 2>/dev/null
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\n' >client.ext
  openssl x509 -req -days 2 -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -extfile client.ext -out client.pem 2>/dev/null
  for h in "$CHOST:origin" "$HOST:durable-origin"; do
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=${h%%:*}" -addext "subjectAltName=DNS:${h%%:*}" \
      -keyout "$etc/${h##*:}.key" -out "$etc/${h##*:}.pem" 2>/dev/null
  done
)
cp "$work/ca.pem" "$etc/origin-pull-custom-ca.pem"
printf 'trust_pool file /etc/caddy/origin-pull-custom-ca.pem\n' >"$etc/origin-pull-trust.caddy"
# Stand-ins for the two servers, test-only: what each was told. Ours sends its own nosniff, which
# Caddy's deferred header must replace, not double.
mkdir -p "$etc/sites"
cat >"$etc/sites/zz-test-standins.caddy" <<'EOF'
http://:58300 {
	header X-Content-Type-Options nosniff
	respond "dr cf=[{header.CF-Connecting-IP}]"
}
http://:58000 {
	respond "colophon xff=[{header.X-Forwarded-For}]"
}
EOF
chmod -R a+rX "$work"

# caddy-merge.sh with the real `caddy validate` (the image, /etc/caddy mounted from the scratch
# root) and a systemctl that only records.
cat >"$work/bin/caddy" <<EOF
#!/usr/bin/env bash
r='$R'
args=(); for a in "\$@"; do args+=("\${a#"\$r"}"); done
exec docker run --rm -v "$etc:/etc/caddy:ro" "$IMAGE" caddy "\${args[@]}"
EOF
printf '#!/usr/bin/env bash\necho "systemctl $*" >>"%s/state/calls"\n' "$work" >"$work/bin/systemctl"
chmod +x "$work/bin/caddy" "$work/bin/systemctl"

echo "caddy-merge.sh with Caddy's own validate"
good_key=$(cat "$etc/durable-origin.pem")
printf -- '-----BEGIN CERTIFICATE-----\nbroken\n-----END CERTIFICATE-----\n' >"$etc/durable-origin.pem"
cp "$etc/Caddyfile" "$work/root-before"
if PATH="$work/bin:$PATH" ROOT=$R "$kit/caddy-merge.sh" >"$work/merge-bad.out" 2>&1; then
  bad "the merge accepted a broken origin certificate"
else
  if ! grep -qi 'certificate\|PEM' "$work/merge-bad.out"; then
    bad "the merge failed, but not on the certificate: $(tail -4 "$work/merge-bad.out")"
  elif cmp -s "$etc/Caddyfile" "$work/root-before" && [[ ! -e "$etc/sites/durable-runner.caddy" ]]; then
    ok "a broken origin certificate is refused by caddy validate; the root and sites are as before"
  else
    bad "after a refused validate the files weren't restored"
  fi
fi
printf '%s\n' "$good_key" >"$etc/durable-origin.pem"
if PATH="$work/bin:$PATH" ROOT=$R "$kit/caddy-merge.sh" >"$work/merge.out" 2>&1; then
  ok "the merge validates in Caddy $(docker run --rm "$IMAGE" caddy version | cut -d' ' -f1) and reloads"
else
  bad "the merge failed: $(tail -5 "$work/merge.out")"
fi
chmod -R a+rX "$etc"

docker run -d --name "$name" -p 127.0.0.1::443 -v "$etc:/etc/caddy" "$IMAGE" \
  sh -c 'mkdir -p /var/lib/caddy && exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile' >/dev/null
port=""
for _ in $(seq 60); do
  port=$(docker port "$name" 443/tcp 2>/dev/null | sed -n 's/^127\.0\.0\.1:\([0-9]*\)$/\1/p' | head -1)
  [[ -n "$port" ]] && docker exec "$name" test -S /var/lib/caddy/admin.sock 2>/dev/null && break
  sleep 0.25
done
[[ -n "$port" ]] || { docker logs "$name" 2>&1 | tail -10; bad "Caddy didn't start"; exit 1; }
reload() { docker exec "$name" caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile --force >/dev/null 2>&1; }
req() { # <host> <path> [curl args...]: the body, with our client certificate
  local h=$1 p=$2; shift 2
  curl -sS --max-time 5 -k --connect-to "$h:443:127.0.0.1:$port" --cert "$work/client.pem" --key "$work/client.key" "$@" "https://$h$p" 2>&1 || true
}

echo "Caddy serving the merged configuration"
cn() { openssl s_client -connect "127.0.0.1:$port" -servername "$1" -cert "$work/client.pem" -key "$work/client.key" </dev/null 2>/dev/null \
  | openssl x509 -noout -subject 2>/dev/null | sed 's/.*CN *= *//'; }
[[ "$(cn "$HOST")" == "$HOST" && "$(cn "$CHOST")" == "$CHOST" ]] && ok "each host gets its own origin certificate by SNI" \
  || bad "SNI certificates: ours '$(cn "$HOST")', Colophon's '$(cn "$CHOST")'"
[[ "$(req "$CHOST" /api -H 'Cf-Connecting-Ip: 203.0.113.9')" == "colophon xff=[203.0.113.9]" ]] && ok "Colophon's site still serves beside ours" \
  || bad "Colophon's site: $(req "$CHOST" /api)"
code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 -k --connect-to "$HOST:443:127.0.0.1:$port" "https://$HOST/health" 2>/dev/null || true)
[[ "$code" != 200 ]] && ok "no client certificate: refused (Authenticated Origin Pulls)" || bad "served without a client certificate"
got=$(req "$HOST" /health -H 'Cf-Connecting-Ip: 203.0.113.7' -H 'CF-Connecting-IP: 198.51.100.66')
[[ "$got" == "dr cf=[203.0.113.7]" ]] && ok "from a trusted peer the server is told Cf-Connecting-Ip, one value" || bad "from a trusted peer the server was told '$got'"
headers=$(req "$HOST" /health -D - -o /dev/null | tr -d '\r')
grep -qix 'strict-transport-security: max-age=31536000' <<<"$headers" && ok "HSTS for this host only" || bad "HSTS: $(grep -i strict <<<"$headers")"
[[ "$(grep -ci '^x-content-type-options: nosniff$' <<<"$headers")" == 1 ]] && ok "exactly one nosniff" || bad "nosniff count: $(grep -ci '^x-content-type-options' <<<"$headers")"
loc=$(req "$HOST" / -o /dev/null -w '%{http_code} %{redirect_url}')
[[ "$loc" == "302 https://$HOST/inspector/" ]] && ok "/ redirects 302 to /inspector/" || bad "/ answered '$loc'"
trusted '203.0.113.0/24'
reload || bad "reload with other trusted ranges"
got=$(req "$HOST" /health -H 'Cf-Connecting-Ip: 198.51.100.4')
[[ "$got" =~ ^dr\ cf=\[[0-9a-f.:]+\]$ && "$got" != *198.51.100.4* ]] && ok "from an untrusted peer, its own address (${got#dr cf=}), not the header it sent" \
  || bad "from an untrusted peer the server was told '$got'"

echo "Colophon's side: the import with no site files"
E=$work/empty/etc/caddy; mkdir -p "$E/sites"
cp "$etc"/*.pem "$etc"/*.key "$etc/cloudflare-trusted.caddy" "$etc/origin-pull-trust.caddy" "$E/"
cp "$kit/testdata/colophon-root.Caddyfile" "$E/Caddyfile"
printf '\nimport /etc/caddy/sites/*.caddy\n' >>"$E/Caddyfile"
chmod -R a+rX "$work/empty"
docker run --rm -v "$E:/etc/caddy:ro" "$IMAGE" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1 \
  && ok "Colophon's root with the import line and an empty sites/ validates" || bad "an empty sites/ breaks Colophon's root"
rmdir "$E/sites"
docker run --rm -v "$E:/etc/caddy:ro" "$IMAGE" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1 \
  && ok "... and with no sites/ directory at all" || bad "a missing sites/ breaks Colophon's root"

if (( fails )); then echo "test-droplet-caddy: $fails FAILED" >&2; exit 1; fi
echo "test-droplet-caddy: all green"
