#!/usr/bin/env bash
# The secrets' path on the shared droplet, without a droplet:
#   durable-saas-run.sh (the unit's ExecStart) through the real deploy/durable-run.sh and its lab
#   guard into a stub server that records its argv and environment:
#     - each credential file becomes the variable the server reads, and no secret is in argv;
#     - --public-origin from the setting, --ops-listen only when set; invite-only "0" means off;
#     - it refuses (exit 78, the server never runs): a missing required credential, Neon's pooled
#       host, sslmode other than verify-full, channel_binding, DURABLE_MAILER=log, an in-memory
#       account store, a non-loopback ops listener, an odd origin, a flag repeated in
#       DURABLE_EXTRA_ARGS, a Resend key without a sender, no CREDENTIALS_DIRECTORY;
#   put-secret.sh: validated, root-only, the same value again a no-op, a different one only with
#   --replace (the old kept as .prev), nothing written on a refusal, the origin certificate checked
#   against our host and its key.
#
#   deploy/droplet/test-droplet-secrets.sh
# shellcheck disable=SC2015,SC2016  # ok/bad only print (both return 0); refused's setup is eval'd later, so single quotes.
set -euo pipefail
kit=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/dr-secrets-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
fails=0
ok() { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fails=$((fails + 1)); }
perm() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }

API=apikey-0123456789-0123456789-0123456789AB
READ=readkey-0123456789-0123456789-0123456789AB
DBURL='postgres://durable_app:s3cretPassw0rd-xyz@ep-cool-name-123456.us-east-2.aws.neon.tech/durable_runner?sslmode=verify-full'
RESEND=re_TestKey_0123456789abcdef

# --- durable-saas-run.sh ------------------------------------------------------------------
echo "durable-saas-run.sh"
cat >"$work/stub" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == --help ]]; then
  echo "Usage: durable-server [OPTIONS]"; echo "      --journal <JOURNAL>"
  [[ -n "${STUB_OLD:-}" ]] || { echo "      --public-origin <PUBLIC_ORIGIN>"; echo "      --ops-listen <OPS_LISTEN>"; }
  exit 0
fi
printf '%s\n' "$@" >"$STUB_DIR/argv"
env >"$STUB_DIR/env"
EOF
chmod +x "$work/stub"
creds=$work/creds; mkdir -p "$creds" "$work/etc"
setcreds() { rm -f "$creds"/*; printf '%s\n' "$API" >"$creds/api_key"; printf '%s' "$READ" >"$creds/read_key"; printf '%s\n' "$DBURL" >"$creds/database_url"; printf '%s' "$RESEND" >"$creds/resend_api_key"; }
# runw [VAR=value...]: the wrapper in a clean environment, as systemd would start it.
runw() {
  rm -f "$work/argv" "$work/env"
  env -i PATH="$PATH" HOME="$work" CREDENTIALS_DIRECTORY="$creds" DURABLE_ETC="$work/etc" \
    DURABLE_SERVER_BIN="$work/stub" STUB_DIR="$work" DURABLE_LISTEN=127.0.0.1:58300 \
    DURABLE_PUBLIC_ORIGIN=https://durablerunnerapp.404human.dev \
    DURABLE_MAIL_FROM="Durable Runner <durable-runner@404human.dev>" \
    DURABLE_REQUIRE_ACCOUNTS=1 DURABLE_REQUIRE_MAIL=1 "$@" "$kit/durable-saas-run.sh" >"$work/out" 2>&1
}
has_pair() { local prev="" line; while IFS= read -r line; do [[ "$prev" == "$1" && "$line" == "$2" ]] && return 0; prev=$line; done <"$work/argv"; return 1; }
envval() { sed -n "s/^$1=//p" "$work/env"; }

setcreds
if runw DURABLE_INVITE_ONLY=0; then ok "starts with every credential"; else bad "didn't start: $(cat "$work/out")"; fi
[[ "$(envval DURABLE_API_KEY)" == "$API" && "$(envval DURABLE_READ_KEY)" == "$READ" && "$(envval DATABASE_URL)" == "$DBURL" && "$(envval RESEND_API_KEY)" == "$RESEND" ]] \
  && ok "each credential is the variable the server reads (trailing newline dropped)" || bad "credentials in the environment: $(grep -E '^(DURABLE_API_KEY|DURABLE_READ_KEY|DATABASE_URL|RESEND_API_KEY)=' "$work/env" | cut -d= -f1 | tr '\n' ' ')"
leak=0; for v in "$API" "$READ" "s3cretPassw0rd" "$RESEND"; do grep -qF -- "$v" "$work/argv" && leak=1; grep -qF -- "$v" "$work/out" && leak=1; done
(( leak == 0 )) && ok "no secret in argv or in what it prints" || bad "a secret reached argv or the output"
has_pair --public-origin https://durablerunnerapp.404human.dev && ok "--public-origin from DURABLE_PUBLIC_ORIGIN" || bad "no --public-origin: $(tr '\n' ' ' <"$work/argv")"
grep -qx -- --ops-listen "$work/argv" && bad "--ops-listen passed although DURABLE_OPS_LISTEN is unset" || ok "no --ops-listen while the setting is off"
has_pair --trusted-proxy 127.0.0.1/32 && ok "durable-run still adds the loopback hop" || bad "no loopback hop"
grep -q '^DURABLE_INVITE_ONLY=' "$work/env" && bad "DURABLE_INVITE_ONLY=0 reached the server (it would turn invite-only ON)" || ok "DURABLE_INVITE_ONLY=0 is dropped (off)"
runw DURABLE_INVITE_ONLY=yes && [[ "$(envval DURABLE_INVITE_ONLY)" == 1 ]] && ok "DURABLE_INVITE_ONLY=yes reaches the server as 1" || bad "invite-only on didn't reach the server"
runw DURABLE_OPS_LISTEN=127.0.0.1:58301 && has_pair --ops-listen 127.0.0.1:58301 && ok "--ops-listen when DURABLE_OPS_LISTEN is set" || bad "no --ops-listen with the setting on"
runw STUB_OLD=1 DURABLE_OPS_LISTEN=127.0.0.1:58301 && ! grep -qx -- --public-origin "$work/argv" && ! grep -qx -- --ops-listen "$work/argv" \
  && grep -q 'WARNING DURABLE_OPS_LISTEN' "$work/out" && ok "a server without the S3 flags gets neither (clap would refuse them), with a warning for --ops-listen" \
  || bad "an older server: $(tr '\n' ' ' <"$work/argv"; cat "$work/out")"
rm -f "$creds/database_url" "$creds/resend_api_key"
runw DURABLE_REQUIRE_ACCOUNTS=0 DURABLE_REQUIRE_MAIL=0 && ! grep -q '^DATABASE_URL=' "$work/env" && ok "a key-only server starts without the account store when told to" || bad "REQUIRE_*=0 didn't start without them: $(cat "$work/out")"

refused() { # <description> <setup command> [VAR=value...]
  local what=$1 setup=$2 rc=0; shift 2
  setcreds; eval "$setup"
  runw "$@" || rc=$?
  if [[ "$rc" == 78 && ! -e "$work/argv" ]]; then ok "refuses $what"; else bad "didn't refuse $what (exit $rc, server ran: $([[ -e "$work/argv" ]] && echo yes || echo no)): $(cat "$work/out")"; fi
}
refused "a missing api_key" 'rm "$creds/api_key"'
refused "a missing database_url" 'rm "$creds/database_url"'
refused "a missing resend_api_key" 'rm "$creds/resend_api_key"'
refused "Neon's pooled host" 'printf %s "${DBURL/ep-cool-name-123456/ep-cool-name-123456-pooler}" >"$creds/database_url"'
refused "sslmode=require" 'printf %s "${DBURL/verify-full/require}" >"$creds/database_url"'
refused "channel_binding" 'printf %s "$DBURL&channel_binding=require" >"$creds/database_url"'
refused "a two-line credential" 'printf "a\nb\n" >"$creds/api_key"'
refused "a Resend key that isn't one" 'printf %s "sk_live_x" >"$creds/resend_api_key"'
refused "DURABLE_MAILER=log" ':' DURABLE_MAILER=log
refused "an in-memory account store" ':' DURABLE_ACCOUNTS_BACKEND=memory
refused "an unclear DURABLE_INVITE_ONLY" ':' DURABLE_INVITE_ONLY=maybe
refused "a public ops listener" ':' DURABLE_OPS_LISTEN=0.0.0.0:58301
refused "an origin with a path" ':' DURABLE_PUBLIC_ORIGIN=https://durablerunnerapp.404human.dev/x
refused "an http origin" ':' DURABLE_PUBLIC_ORIGIN=http://durablerunnerapp.404human.dev
refused "--public-origin repeated in DURABLE_EXTRA_ARGS" ':' "DURABLE_EXTRA_ARGS=--public-origin https://evil.example"
refused "a Resend key without DURABLE_MAIL_FROM" ':' DURABLE_MAIL_FROM=
setcreds; rc=0
env -i PATH="$PATH" DURABLE_SERVER_BIN="$work/stub" STUB_DIR="$work" "$kit/durable-saas-run.sh" >"$work/out" 2>&1 || rc=$?
[[ "$rc" == 78 ]] && ok "refuses to start outside systemd (no CREDENTIALS_DIRECTORY)" || bad "started without CREDENTIALS_DIRECTORY (exit $rc)"

# --- put-secret.sh ------------------------------------------------------------------------
echo "put-secret.sh"
R=$work/root
mkdir -p "$R/etc/durable/credentials" "$R/etc/caddy" "$work/bin"
chmod 0700 "$R/etc/durable/credentials"
cp "$kit/durable.env.template" "$R/etc/durable/durable.env"
printf '#!/usr/bin/env bash\necho "chown $*" >>"%s/calls"\n' "$work" >"$work/bin/chown"; chmod +x "$work/bin/chown"
put() { PATH="$work/bin:$PATH" ROOT=$R "$kit/put-secret.sh" "$@" >"$work/put.out" 2>&1; }
C=$R/etc/durable/credentials
noleft() { [[ -z "$(find "$C" "$R/etc/caddy" -maxdepth 1 -name '.dr-secret*')" ]]; }

printf '%s\n' "$DBURL" | put database_url && [[ "$(cat "$C/database_url")" == "$DBURL" && "$(perm "$C/database_url")" == 400 ]] \
  && ok "database_url saved, one line, 0400" || bad "database_url: $(cat "$work/put.out")"
grep -qF s3cretPassw0rd "$work/put.out" && bad "put-secret printed the secret" || ok "nothing secret printed"
printf '%s' "$DBURL" | put database_url && grep -q unchanged "$work/put.out" && ok "the same value again is a no-op" || bad "the same value: $(cat "$work/put.out")"
other=${DBURL/s3cretPassw0rd-xyz/an0therPassw0rd-abc}
if printf '%s' "$other" | put database_url; then bad "a different value was written without --replace"; else [[ "$(cat "$C/database_url")" == "$DBURL" ]] && ok "a different value needs --replace (file unchanged)" || bad "the file changed without --replace"; fi
printf '%s' "$other" | put --replace database_url && [[ "$(cat "$C/database_url")" == "$other" && "$(cat "$C/database_url.prev")" == "$DBURL" ]] \
  && ok "--replace writes it and keeps the old one as .prev" || bad "--replace: $(cat "$work/put.out")"
for bad_url in "${DBURL/ep-cool-name-123456/ep-cool-name-123456-pooler}" "${DBURL/verify-full/require}" "$DBURL&channel_binding=require" "mysql://u:p@h/d?sslmode=verify-full"; do
  if printf '%s' "$bad_url" | put --replace database_url; then bad "accepted a bad database_url"; fi
done
[[ "$(cat "$C/database_url")" == "$other" ]] && noleft && ok "pooled, require, channel_binding and non-postgres URLs are refused, nothing written or left" || bad "a refused URL changed something"
printf 'line1\nline2\n' | put resend_api_key && bad "accepted a two-line value" || ok "a two-line value is refused"
printf 'sk_live_nope' | put resend_api_key && bad "accepted a non-Resend key" || ok "a non-Resend key is refused"
printf '%s\n' "$RESEND" | put resend_api_key && ok "the Resend key is saved" || bad "Resend key: $(cat "$work/put.out")"
printf '%s' "$API" | put api_key && printf '%s' "$API" | put read_key && bad "accepted a read_key equal to the api_key" || ok "a read_key equal to the api_key is refused"
printf 'short' | put --replace api_key && bad "accepted a short api_key" || ok "a short api_key is refused"
rc=0; put nope </dev/null || rc=$?
[[ "$rc" == 2 ]] && ok "an unknown name is a usage error" || bad "an unknown name: exit $rc"

( cd "$work"
  for h in durablerunnerapp.404human.dev colophonapp.404human.dev; do
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj "/CN=$h" -addext "subjectAltName=DNS:$h" -keyout "$h.key" -out "$h.pem" 2>/dev/null
  done )
put origin_key <"$work/durablerunnerapp.404human.dev.key" && put origin_pem <"$work/durablerunnerapp.404human.dev.pem" \
  && [[ "$(perm "$R/etc/caddy/durable-origin.pem")" == 440 && "$(perm "$R/etc/caddy/durable-origin.key")" == 440 ]] \
  && grep -q 'chown root:caddy' "$work/calls" && ok "the origin certificate and key are saved root:caddy 0440" || bad "origin certificate: $(cat "$work/put.out")"
put --replace origin_pem <"$work/colophonapp.404human.dev.pem" && bad "accepted a certificate for another host" \
  || { grep -q "doesn't cover" "$work/put.out" && ok "a certificate for another host is refused" || bad "refused for another reason: $(cat "$work/put.out")"; }
put --replace origin_key <"$work/colophonapp.404human.dev.key" && bad "accepted a key that doesn't match the certificate" \
  || { grep -q "doesn't match" "$work/put.out" && ok "a key that doesn't match the installed certificate is refused" || bad "refused for another reason: $(cat "$work/put.out")"; }
cmp -s "$R/etc/caddy/durable-origin.pem" "$work/durablerunnerapp.404human.dev.pem" && noleft && ok "the refusals left the installed pair and no temp file" || bad "a refusal changed the pair"

if (( fails )); then echo "test-droplet-secrets: $fails FAILED" >&2; exit 1; fi
echo "test-droplet-secrets: all green"
