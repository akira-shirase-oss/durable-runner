#!/usr/bin/env bash
# install.sh and caddy-merge.sh against a scratch copy of the shared droplet, Colophon's files
# seeded as its installer leaves them. Fakes on PATH record every systemctl/caddy/useradd/chown
# call; nothing on this machine outside the scratch directory is touched.
#   - a first run sets everything up; a second run of both changes no file and makes no call
#     that changes anything;
#   - Colophon's files stay byte-identical; its root Caddyfile only gains one import line, once;
#   - a refused `caddy validate` (or reload) puts the root and the site file back as they were;
#   - the merge refuses a root that doesn't trust Cloudflare's ranges, or imports the sites twice;
#   - the installer refuses a unit file that isn't this kit's;
#   - a listener on the public or ops port owned by anything but durable-server's MainPID makes
#     the installer stop before it writes anything; one owned by MainPID passes.
# Known-bad proofs: an installer without its port check, and a merge without its restore, must
# each turn this test red.
#
#   deploy/droplet/test-droplet-install.sh
# shellcheck disable=SC2015  # ok and bad only print (both return 0), so `t && ok || bad` is if-then-else here.
set -euo pipefail
kit=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/dr-install-test.XXXXXX")
listener_pid=""
cleanup() { [[ -z "$listener_pid" ]] || kill "$listener_pid" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT
fails=0
ok() { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fails=$((fails + 1)); }
perm() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }
digest() { if command -v sha256sum >/dev/null; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }

# --- fakes --------------------------------------------------------------------------------
fake=$work/fake-bin
mkdir -p "$fake"
cat >"$fake/systemctl" <<'EOF'
#!/usr/bin/env bash
echo "systemctl $*" >>"$FAKE_STATE/calls"
case "$*" in
  "show -p MainPID --value durable-server") cat "$FAKE_STATE/mainpid" 2>/dev/null || echo 0 ;;
  "show -p "*) echo "" ;;
  "is-enabled --quiet durable-server") [[ -e "$FAKE_STATE/enabled" ]] ;;
  "enable durable-server") touch "$FAKE_STATE/enabled" ;;
  "reload caddy") exit "${FAKE_RELOAD_RC:-0}" ;;
  "is-active"*) exit 3 ;;
  *) exit 0 ;;
esac
EOF
cat >"$fake/caddy" <<'EOF'
#!/usr/bin/env bash
echo "caddy $*" >>"$FAKE_STATE/calls"
if [[ "${1:-}" == validate ]]; then
  if [[ "${FAKE_CADDY_VALIDATE:-0}" != 0 ]]; then echo "Error: adapting config using caddyfile: (fake) refused" >&2; exit 1; fi
  echo "Valid configuration"
fi
EOF
cat >"$fake/id" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == -u && -n "${2:-}" ]]; then [[ -e "$FAKE_STATE/users/$2" ]] && { echo 990; exit 0; }; exit 1; fi
exec /usr/bin/id "$@"
EOF
cat >"$fake/useradd" <<'EOF'
#!/usr/bin/env bash
echo "useradd $*" >>"$FAKE_STATE/calls"
mkdir -p "$FAKE_STATE/users"; touch "$FAKE_STATE/users/${*: -1}"
EOF
cat >"$fake/chown" <<'EOF'
#!/usr/bin/env bash
echo "chown $*" >>"$FAKE_STATE/calls"
EOF
chmod +x "$fake"/*
export PATH="$fake:$PATH"

# --- a droplet as Colophon's installer leaves it -------------------------------------------
# The root Caddyfile (testdata/colophon-root.Caddyfile) mirrors Colophon's deploy/droplet/Caddyfile
# as its install.sh renders it.
seed() { # <root>
  local r=$1
  if [[ -e "$r" ]]; then chmod -R u+w "$r"; rm -rf "$r"; fi
  mkdir -p "$r/etc/caddy" "$r/etc/colophon/credentials" "$r/etc/systemd/system" "$r/etc/redis" \
    "$r/etc/ssh/sshd_config.d" "$r/etc/ufw" "$r/var/lib/caddy" "$r/usr/local/bin"
  cp "$kit/testdata/colophon-root.Caddyfile" "$r/etc/caddy/Caddyfile"
  printf 'trusted_proxies static 173.245.48.0/20 103.21.244.0/22 2400:cb00::/32\n' >"$r/etc/caddy/cloudflare-trusted.caddy"
  printf 'trust_pool file /etc/caddy/origin-pull-custom-ca.pem\n' >"$r/etc/caddy/origin-pull-trust.caddy"
  printf -- '-----BEGIN CERTIFICATE-----\ncolophon\n-----END CERTIFICATE-----\n' >"$r/etc/caddy/origin.pem"
  printf -- '-----BEGIN PRIVATE KEY-----\ncolophon\n-----END PRIVATE KEY-----\n' >"$r/etc/caddy/origin.key"
  printf -- '-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n' >"$r/etc/caddy/origin-pull-custom-ca.pem"
  printf 'COLOPHON_PUBLIC_URL=https://colophonapp.404human.dev\nCOLOPHON_TRUSTED_PROXIES=127.0.0.1/32\n' >"$r/etc/colophon/colophon.env"
  printf 'postgres://colophon_app:x@ep-x.example.neon.tech/colophon?sslmode=verify-full' >"$r/etc/colophon/credentials/database_url"
  chmod 0700 "$r/etc/colophon/credentials"; chmod 0400 "$r/etc/colophon/credentials/database_url"
  printf '[Unit]\nDescription=Colophon CMS server\n[Service]\nExecStart=/usr/local/bin/colophon-server --role all\n' >"$r/etc/systemd/system/colophon.service"
  printf 'bind 127.0.0.1 -::1\ninclude /etc/redis/conf.d-colophon.conf\n' >"$r/etc/redis/redis.conf"
  printf 'PasswordAuthentication no\nAllowTcpForwarding no\n' >"$r/etc/ssh/sshd_config.d/10-colophon.conf"
  printf '### tuple ### allow tcp 443 0.0.0.0/0 any 173.245.48.0/20 in comment=cloudflare\n' >"$r/etc/ufw/user.rules"
  printf '#!/bin/sh\necho colophon-server\n' >"$r/usr/local/bin/colophon-server"; chmod 0755 "$r/usr/local/bin/colophon-server"
  # The maintainer's Cloudflare step: our own origin certificate, already put.
  printf -- '-----BEGIN CERTIFICATE-----\ndurable\n-----END CERTIFICATE-----\n' >"$r/etc/caddy/durable-origin.pem"
  printf -- '-----BEGIN PRIVATE KEY-----\ndurable\n-----END PRIVATE KEY-----\n' >"$r/etc/caddy/durable-origin.key"
}
# snap <root>: every path, its mode, and its content digest (or link target).
snap() {
  (cd "$1" && find . -mindepth 1 | LC_ALL=C sort | while IFS= read -r p; do
    if [[ -L "$p" ]]; then printf '%s link %s\n' "$p" "$(readlink "$p")"
    elif [[ -d "$p" ]]; then printf '%s dir %s\n' "$p" "$(perm "$p")"
    else printf '%s %s %s\n' "$p" "$(perm "$p")" "$(digest <"$p")"; fi
  done)
}
# colophon_snap <root>: Colophon's files only (everything the seed made except the root Caddyfile
# and our own origin certificate).
colophon_snap() { snap "$1" | grep -E '^\./(etc/(caddy/(cloudflare-trusted|origin-pull|origin\.)|colophon|systemd/system/colophon|redis|ssh|ufw)|usr/local/bin/colophon|var/lib/caddy)' || true; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'; }
P1=$(free_port); P2=$(free_port)
# run <root> <state> <script> [env...]: one script against the scratch root.
run() {
  local r=$1 st=$2 s=$3; shift 3
  mkdir -p "$st"
  env ROOT="$r" FAKE_STATE="$st" DURABLE_PUBLIC_PORT="$P1" DURABLE_OPS_PORT="$P2" "$@" "$s"
}
kitcopy() { # <dir>: a scratch copy of deploy/ (for the known-bad proofs)
  mkdir -p "$1"; cp -R "$kit/.." "$1/deploy"
}

# 1. First run, then a second: nothing changes the second time.
echo "first and second run"
A=$work/a; SA=$work/state-a
seed "$A"
seed_colophon=$(colophon_snap "$A")
seed_root=$(cat "$A/etc/caddy/Caddyfile")
if run "$A" "$SA" "$kit/install.sh" >"$work/a-install1.out" 2>&1 && run "$A" "$SA" "$kit/caddy-merge.sh" >"$work/a-merge1.out" 2>&1; then
  ok "first install + merge succeed"
else
  bad "first install + merge failed: $(tail -3 "$work"/a-*1.out)"
fi
[[ -s "$A/etc/durable/credentials/api_key" && "$(perm "$A/etc/durable/credentials/api_key")" == 400 && "$(perm "$A/etc/durable/credentials")" == 700 ]] \
  && ok "the ops keys are generated root-only (0400 in a 0700 directory)" || bad "the ops keys are missing or not 0400/0700"
for f in durable-run durable-lab-guard durable-saas-run; do
  [[ -x "$A/usr/local/bin/$f" ]] || bad "no /usr/local/bin/$f"
done
cmp -s "$A/usr/local/bin/durable-lab-guard" "$kit/../lab-build-guard.sh" && ok "durable-lab-guard installed beside durable-run" || bad "durable-lab-guard isn't lab-build-guard.sh"
grep -q 'systemctl daemon-reload' "$SA/calls" && grep -q 'systemctl enable durable-server' "$SA/calls" && grep -q 'systemctl reload caddy' "$SA/calls" \
  && ok "the first run reloads systemd, enables the unit, reloads Caddy" || bad "first-run calls: $(tr '\n' ';' <"$SA/calls")"
n=$(grep -cxF 'import /etc/caddy/sites/*.caddy' "$A/etc/caddy/Caddyfile" || true)
[[ "$n" == 1 ]] && ok "the root imports sites/*.caddy exactly once" || bad "the root has $n import lines"
root_after=$(cat "$A/etc/caddy/Caddyfile")
[[ "${root_after:0:${#seed_root}}" == "$seed_root" ]] && ok "the root's original text is untouched (only appended to)" || bad "the root's original text changed"
added=$(diff <(printf '%s\n' "$seed_root") "$A/etc/caddy/Caddyfile" | grep -c '^>' || true)
[[ "$added" -le 3 ]] && ok "the root gained $added lines (a blank, a comment, the import)" || bad "the root gained $added lines"
[[ "$(colophon_snap "$A")" == "$seed_colophon" ]] && ok "Colophon's files are byte-identical" || bad "Colophon's files changed: $(diff <(echo "$seed_colophon") <(colophon_snap "$A") | head -5)"
grep -qxF 'durablerunnerapp.404human.dev:443 {' "$A/etc/caddy/sites/durable-runner.caddy" && grep -qxF $'\tredir / /inspector/ 302' "$A/etc/caddy/sites/durable-runner.caddy" \
  && ok "the site file is rendered for durablerunnerapp.404human.dev, / -> /inspector/" || bad "the site file isn't rendered as expected"
[[ ! -e "$A/etc/durable/trusted-proxies" ]] && ok "no trusted-proxies file (loopback hop only)" || bad "trusted-proxies was written"

before=$(snap "$A"); : >"$SA/calls"
if run "$A" "$SA" "$kit/install.sh" >"$work/a-install2.out" 2>&1 && run "$A" "$SA" "$kit/caddy-merge.sh" >"$work/a-merge2.out" 2>&1; then
  ok "second install + merge succeed"
else
  bad "second install + merge failed: $(tail -3 "$work"/a-*2.out)"
fi
[[ "$(snap "$A")" == "$before" ]] && ok "the second run changed no file, mode or link" || bad "the second run changed: $(diff <(echo "$before") <(snap "$A") | head -6)"
changing=$(grep -vE '^systemctl (show|is-enabled|is-active) ' "$SA/calls" || true)
[[ -z "$changing" ]] && ok "the second run made no changing call (only show/is-enabled)" || bad "the second run called: $(tr '\n' ';' <<<"$changing")"

# 2. A refused validation on a fresh host: the root stays exactly as Colophon left it.
echo "a refused caddy validate"
B=$work/b; SB=$work/state-b
seed "$B"
run "$B" "$SB" "$kit/install.sh" >/dev/null 2>&1 || bad "install on host B failed"
seed_b=$(snap "$B/etc/caddy")
: >"$SB/calls"
if run "$B" "$SB" "$kit/caddy-merge.sh" FAKE_CADDY_VALIDATE=1 >"$work/b-merge.out" 2>&1; then
  bad "the merge succeeded although caddy validate refused"
else
  ok "the merge fails when caddy validate refuses"
fi
[[ "$(snap "$B/etc/caddy")" == "$seed_b" ]] && ok "the root is restored and no site file is left behind" || bad "after a refused validate /etc/caddy differs: $(diff <(echo "$seed_b") <(snap "$B/etc/caddy") | head -4)"
grep -q 'reload caddy' "$SB/calls" && bad "Caddy was reloaded after a refused validate" || ok "Caddy wasn't reloaded"
ls "$B/var/backups/durable-runner/caddy/"*/Caddyfile >/dev/null 2>&1 && ok "a timestamped backup was kept" || bad "no backup"

# 3. On the set-up host A: a changed site file that fails validation, then one whose reload fails.
echo "a refused change on a merged host"
sed -i.bak 's#^DURABLE_DASHBOARD_PATH=.*#DURABLE_DASHBOARD_PATH=/app/#' "$A/etc/durable/durable.env"; rm -f "$A/etc/durable/durable.env.bak"
caddy_before=$(snap "$A/etc/caddy")
run "$A" "$SA" "$kit/caddy-merge.sh" FAKE_CADDY_VALIDATE=1 >/dev/null 2>&1 && bad "a refused change was accepted" || ok "a refused change fails"
[[ "$(snap "$A/etc/caddy")" == "$caddy_before" ]] && ok "the previous site file is back" || bad "the site file wasn't restored"
run "$A" "$SA" "$kit/caddy-merge.sh" FAKE_RELOAD_RC=1 >/dev/null 2>&1 && bad "a failed reload was accepted" || ok "a failed reload fails"
[[ "$(snap "$A/etc/caddy")" == "$caddy_before" ]] && ok "after a failed reload the files match the running config again" || bad "files not restored after a failed reload"
: >"$SA/calls"
run "$A" "$SA" "$kit/caddy-merge.sh" >/dev/null 2>&1 || bad "the /app/ merge failed"
grep -qxF $'\tredir / /app/ 302' "$A/etc/caddy/sites/durable-runner.caddy" && [[ "$(grep -c 'reload caddy' "$SA/calls")" == 1 ]] \
  && ok "the accepted change lands with one reload" || bad "the /app/ change didn't land once"
[[ "$(grep -cxF 'import /etc/caddy/sites/*.caddy' "$A/etc/caddy/Caddyfile")" == 1 ]] && ok "still one import line" || bad "import lines multiplied"

# 4. Roots the merge must not touch.
echo "roots the merge refuses"
C=$work/c; SC=$work/state-c; seed "$C"; run "$C" "$SC" "$kit/install.sh" >/dev/null 2>&1 || bad "install on host C failed"
grep -v 'cloudflare-trusted' "$C/etc/caddy/Caddyfile" >"$work/root" && cp "$work/root" "$C/etc/caddy/Caddyfile"
c_before=$(snap "$C/etc/caddy")
run "$C" "$SC" "$kit/caddy-merge.sh" >"$work/c.out" 2>&1 && bad "merged into a root that doesn't trust Cloudflare's ranges" \
  || { grep -q cloudflare-trusted "$work/c.out" && ok "refuses a root that doesn't import cloudflare-trusted.caddy" || bad "refused for another reason: $(cat "$work/c.out")"; }
[[ "$(snap "$C/etc/caddy")" == "$c_before" ]] || bad "a refused merge changed /etc/caddy"
seed "$C"; rm -rf "$SC"; run "$C" "$SC" "$kit/install.sh" >/dev/null 2>&1 || bad "install on host C failed"
printf 'import /etc/caddy/sites/*.caddy\nimport /etc/caddy/sites/*.caddy\n' >>"$C/etc/caddy/Caddyfile"
run "$C" "$SC" "$kit/caddy-merge.sh" >"$work/c.out" 2>&1 && bad "merged into a root that imports the sites twice" \
  || { grep -q '2 lines' "$work/c.out" && ok "refuses a root with two import lines" || bad "refused for another reason: $(cat "$work/c.out")"; }

# 5. A unit file that isn't ours.
echo "a unit file that isn't this kit's"
D=$work/d; SD=$work/state-d; seed "$D"
printf '[Unit]\nDescription=someone else\n' >"$D/etc/systemd/system/durable-server.service"
run "$D" "$SD" "$kit/install.sh" >"$work/d.out" 2>&1 && bad "install overwrote a foreign unit file" \
  || { grep -q "isn't this kit's" "$work/d.out" && ok "refuses a foreign durable-server.service" || bad "refused for another reason: $(cat "$work/d.out")"; }
[[ ! -e "$D/etc/durable" ]] && ok "and wrote nothing" || bad "it wrote /etc/durable before refusing"

# 6. The ports.
echo "the ports"
python3 -c 'import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(); print("up", flush=True); time.sleep(120)' "$P1" >"$work/listener.out" 2>&1 &
listener_pid=$!
for _ in $(seq 50); do grep -q up "$work/listener.out" 2>/dev/null && break; sleep 0.1; done
E=$work/e; SE=$work/state-e; seed "$E"
run "$E" "$SE" "$kit/install.sh" >"$work/e.out" 2>&1 && bad "install ran with $P1 held by another process" \
  || { grep -q "pid $listener_pid" "$work/e.out" && ok "a foreign listener on the public port stops the install, named by pid" || bad "refused, but not naming pid $listener_pid: $(cat "$work/e.out")"; }
[[ ! -e "$E/etc/durable" && ! -e "$E/usr/local/bin/durable-run" ]] && ok "nothing was written before the refusal" || bad "files were written before the port refusal"
kill -0 "$listener_pid" 2>/dev/null && ok "the foreign listener was left running" || bad "the foreign listener is gone"
mkdir -p "$SE"; echo "$listener_pid" >"$SE/mainpid"
run "$E" "$SE" "$kit/install.sh" >"$work/e2.out" 2>&1 && ok "a listener that is durable-server's MainPID passes" || bad "durable-server's own listener was refused: $(tail -2 "$work/e2.out")"
F=$work/f; SF=$work/state-f; seed "$F"
env ROOT="$F" FAKE_STATE="$SF" DURABLE_PUBLIC_PORT="$P2" DURABLE_OPS_PORT="$P1" "$kit/install.sh" >"$work/f.out" 2>&1 && bad "install ran with the ops port held" \
  || { grep -q 'ops listener' "$work/f.out" && ok "a foreign listener on the ops port stops the install" || bad "ops port refused for another reason: $(cat "$work/f.out")"; }

# 7. Known-bad proofs: each protection removed must turn a check above red.
echo "known-bad proofs"
K=$work/k; kitcopy "$K"
grep -v '^port_check ' "$K/deploy/droplet/install.sh" >"$work/i" && cat "$work/i" >"$K/deploy/droplet/install.sh"
G=$work/g; SG=$work/state-g; seed "$G"
if run "$G" "$SG" "$K/deploy/droplet/install.sh" >/dev/null 2>&1; then ok "without port_check the install runs over the foreign listener (the port check is what stops it)"; else bad "the install still failed without port_check: the port test proves nothing"; fi
kill "$listener_pid" 2>/dev/null || true; wait "$listener_pid" 2>/dev/null || true; listener_pid=""
K2=$work/k2; kitcopy "$K2"
sed 's/^  restore || exit 1$/  :/' "$K2/deploy/droplet/caddy-merge.sh" >"$work/m" && cat "$work/m" >"$K2/deploy/droplet/caddy-merge.sh"
grep -q '^  restore || exit 1$' "$K2/deploy/droplet/caddy-merge.sh" && bad "the restore mutant didn't apply"
H=$work/h; SH=$work/state-h; seed "$H"; run "$H" "$SH" "$kit/install.sh" >/dev/null 2>&1 || bad "install on host H failed"
h_before=$(snap "$H/etc/caddy")
run "$H" "$SH" "$K2/deploy/droplet/caddy-merge.sh" FAKE_CADDY_VALIDATE=1 >/dev/null 2>&1 || true
[[ "$(snap "$H/etc/caddy")" != "$h_before" ]] && ok "without restore a refused validate leaves the root changed (the restore check can go red)" || bad "the restore check passes without restore"

if (( fails )); then echo "test-droplet-install: $fails FAILED" >&2; exit 1; fi
echo "test-droplet-install: all green"
