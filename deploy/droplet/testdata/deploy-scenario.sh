#!/usr/bin/env bash
# Runs INSIDE the throwaway container test-droplet-deploy.sh starts (Ubuntu 24.04, as root), never
# on a real host: it writes /usr/local/bin, /etc/durable and /opt. systemd is a fake that runs
# the unit's ExecStart (durable-saas-run, with the credentials copied as LoadCredential would) in
# the background; durable-server is a stub (a shell script that execs a tiny HTTP server, so its
# pid stays the unit's MainPID) whose /proc view lives under DURABLE_PROC_ROOT.
#
# The real install.sh, put-secret.sh, droplet-deploy.sh, upgrade.sh, rollback.sh, format-guard.sh,
# lab-build-guard.sh and droplet smoke run against it:
#   0. a first release that crashes: the unit is stopped, nothing serves;
#   1. the first release; 2. an upgrade (.prev kept, dashboards switched);
#   3. --rollback; 4. another upgrade;
#   5. a build that dies on start: upgrade.sh's restore_prev brings the previous one back;
#   6. a build that is up but fails its smoke: rollback.sh brings the previous one back;
#   7. a lab build: refused before anything stops;
#   8. a stale build record: refused;
#   9. droplet-build.sh: a commit on main builds and is recorded; one off main, a lab build and a
#      branch name are refused.
# shellcheck disable=SC2015  # ok and bad only print (both return 0), so `t && ok || bad` is if-then-else here.
set -euo pipefail
fails=0
ok() { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fails=$((fails + 1)); }
S=/run/fake-systemd
export DURABLE_PROC_ROOT=/run/fakeproc
mkdir -p "$S" "$DURABLE_PROC_ROOT" /run/systemd/system /opt/stub

# --- fake systemd and journald --------------------------------------------------------------
cat >/usr/local/sbin/systemctl <<'EOF'
#!/usr/bin/env bash
S=/run/fake-systemd
echo "systemctl $*" >>"$S/calls"
unit_pid() { local p; p=$(cat "$S/pid" 2>/dev/null || echo 0); if [[ "$p" != 0 ]] && kill -0 "$p" 2>/dev/null; then echo "$p"; else echo 0; fi; }
start() {
  [[ "$(unit_pid)" == 0 ]] || return 0
  local c=/run/credentials/durable-server k inv
  rm -rf "$c"; mkdir -p "$c"
  for k in api_key read_key database_url resend_api_key; do
    cp "/etc/durable/credentials/$k" "$c/" 2>/dev/null || { echo "durable-server.service: Failed to set up credentials ($k)" >&2; return 1; }
  done
  inv=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'); echo "$inv" >"$S/inv"
  # Like PID 1's children, the service inherits no descriptor from whoever ran systemctl (the
  # deploy's lock on fd 9 included).
  # (No redirection on the loop: bash would park stderr on a descriptor the loop then closes.)
  ( for fd in $(seq 3 30); do eval "exec $fd>&-"; done
    set -a; . /etc/durable/durable.env; set +a; export CREDENTIALS_DIRECTORY=$c; exec /usr/local/bin/durable-saas-run ) >"$S/log-$inv" 2>&1 &
  echo $! >"$S/pid"
  # Real systemd reports a Type=simple start before the process can fail; the fake waits a moment
  # and reports a process that died at once, which is what upgrade.sh's restore path handles.
  sleep 0.4
  [[ "$(unit_pid)" != 0 ]]
}
stop() {
  local p i; p=$(unit_pid); [[ "$p" == 0 ]] && return 0
  kill "$p"; for i in $(seq 50); do kill -0 "$p" 2>/dev/null || return 0; sleep 0.1; done; kill -9 "$p"
}
case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  is-active) [[ "$(unit_pid)" != 0 ]] ;;
  show) case "$*" in *MainPID*) unit_pid ;; *InvocationID*) cat "$S/inv" 2>/dev/null || echo ;; *) echo ;; esac ;;
  is-enabled) [[ -e "$S/enabled" ]] ;;
  enable) touch "$S/enabled" ;;
  *) exit 0 ;;
esac
EOF
cat >/usr/local/sbin/journalctl <<'EOF'
#!/usr/bin/env bash
for a; do case "$a" in _SYSTEMD_INVOCATION_ID=*) cat "/run/fake-systemd/log-${a#*=}" 2>/dev/null ;; esac; done
EOF
chmod +x /usr/local/sbin/systemctl /usr/local/sbin/journalctl
cat >/opt/stub/serve.py <<'EOF'
import http.server
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = {"/health": b"ok", "/ready": b'{"ready":true,"code":null}'}.get(self.path)
        self.send_response(200 if body else 404); self.end_headers(); self.wfile.write(body or b"")
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", 58300), H).serve_forever()
EOF
# mkstub <file> <version> <ok|crash|nodb|lab>: a durable-server stand-in.
mkstub() {
  cat >"$1" <<EOF
#!/usr/bin/env bash
case "\${1:-}" in
  --help) echo "Usage: durable-server [OPTIONS]"; echo "      --journal <JOURNAL>"; echo "      --public-origin <PUBLIC_ORIGIN>"; [[ $3 == lab ]] && echo "      --allow-local-tenant-handlers"; exit 0 ;;
  --version) echo "durable-server $2"; exit 0 ;;
  --journal-format) echo "3 2 1"; exit 0 ;;
esac
[[ $3 == crash ]] && { echo "durable-server: $2 crashed" >&2; exit 1; }
p=\$DURABLE_PROC_ROOT/\$\$; mkdir -p "\$p"
printf '%s\0' "\$0" "\$@" >"\$p/cmdline"; env -0 >"\$p/environ"; ln -sfn /usr/local/bin/durable-server "\$p/exe"
echo "durable-server $2 starting" >&2
# What inspector_csp would hash: the dashboard linked at this start.
grep -o '<title>[^<]*' "\${DURABLE_UI_DIR:-/nonexistent}/index.html" 2>/dev/null | cut -c8- >"\$p/ui-at-start" || true
[[ -n "\${DATABASE_URL:-}" && $3 != nodb ]] && echo "durable-server: account store connected, migrations applied" >&2
exec python3 /opt/stub/serve.py
EOF
  chmod 0755 "$1"
}

# --- the host, installed ---------------------------------------------------------------------
echo "install.sh on a fresh host (real useradd, ss, GNU tools)"
mkdir -p /etc/caddy
/kit/deploy/droplet/install.sh >/tmp/install.out 2>&1 && ok "install.sh" || { cat /tmp/install.out; bad "install.sh failed"; }
id -u durable >/dev/null && ok "user durable made" || bad "no user durable"
[[ "$(stat -c '%a %U' /etc/durable/credentials)" == "700 root" && "$(stat -c '%a %U' /var/lib/durable/journal)" == "750 durable" ]] \
  && ok "credentials 0700 root, journal 0750 durable" || bad "modes/owners: $(stat -c '%n %a %U' /etc/durable/credentials /var/lib/durable/journal)"
/kit/deploy/droplet/install.sh >/tmp/install2.out 2>&1 && ! grep -qE 'made|installed|generated|enabled now' /tmp/install2.out \
  && ok "a second install.sh changes nothing" || bad "second install: $(cat /tmp/install2.out)"
printf '%s' 'postgres://durable_app:pw0123456789@ep-x-123.us-east-2.aws.neon.tech/durable_runner?sslmode=verify-full' | /kit/deploy/droplet/put-secret.sh database_url >/dev/null
printf '%s' 're_test_0123456789' | /kit/deploy/droplet/put-secret.sh resend_api_key >/dev/null
[[ "$(stat -c '%a' /etc/durable/credentials/database_url)" == 400 ]] && ok "secrets put, 0400" || bad "secrets"

# --- builds ----------------------------------------------------------------------------------
B=/opt/durable-build; src=$B/src; rel=$B/target/release
mkdir -p "$rel" "$src"
git -C "$src" init -q; git -C "$src" config user.email t@example.invalid; git -C "$src" config user.name test
cp -R /kit/deploy "$src/deploy"; git -C "$src" add -A; git -C "$src" commit -qm base
# build <version> <mode>: a commit, its stub binaries, the build record, a dashboard tarball.
build() {
  git -C "$src" commit -q --allow-empty -m "$1"
  commit=$(git -C "$src" rev-parse HEAD)
  mkstub "$rel/durable-server" "$1" "$2"
  printf '#!/bin/sh\necho durable-cli %s\n' "$1" >"$rel/durable-cli"; chmod 0755 "$rel/durable-cli"
  printf '%s %s %s\n' "$commit" "$(sha256sum "$rel/durable-server" | cut -d' ' -f1)" "$(sha256sum "$rel/durable-cli" | cut -d' ' -f1)" >"$B/built"
  rm -rf /tmp/dash; mkdir -p /tmp/dash; echo "<!doctype html><title>$1</title>" >/tmp/dash/index.html
  tgz=/root/dashboard-$1.tgz; tar -czf "$tgz" -C /tmp/dash .
}
deploy() { /kit/deploy/droplet/direct/droplet-deploy.sh "$commit" "$tgz" >"/tmp/deploy-$1.out" 2>&1; }
live() { /usr/local/bin/durable-server --version | cut -d' ' -f2; }
prev() { /usr/local/bin/durable-server.prev --version 2>/dev/null | cut -d' ' -f2; }
dash() { grep -o '<title>[^<]*' /opt/durable/dashboard/index.html 2>/dev/null | cut -c8-; }
ready() { [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:58300/ready)" == 200 ]]; }
pid() { systemctl show -p MainPID --value durable-server; }
# csp_ok: the running server started with the dashboard that is linked now (its CSP matches).
csp_ok() { [[ "$(cat "$DURABLE_PROC_ROOT/$(pid)/ui-at-start" 2>/dev/null)" == "$(dash)" ]]; }

echo "0. a first release that crashes"
build v0 crash
deploy v0 && bad "a crashing first release was reported live" || ok "fails"
[[ ! -e /usr/local/bin/durable-server && -e /usr/local/bin/durable-server.failed ]] && ! systemctl is-active --quiet durable-server \
  && ok "fails closed: unit stopped, the binary moved aside (the next deploy is a first install again)" || bad "after a failed first install: $(ls /usr/local/bin)"

echo "1. the first release"
build v1 ok
deploy v1 && ok "succeeds" || { tail -5 /tmp/deploy-v1.out; bad "failed"; }
[[ "$(live)" == v1 && "$(dash)" == v1 ]] && ready && csp_ok && ok "v1 serves its own dashboard (started with it linked), /ready" || bad "live $(live), dashboard $(dash)"
[[ -x /usr/local/bin/durable-lab-guard && -x /usr/local/bin/durable-run && -x /usr/local/bin/durable-saas-run && "$(/usr/local/bin/durable-cli)" == "durable-cli v1" ]] \
  && ok "start-up scripts, lab guard and CLI in /usr/local/bin" || bad "scripts in /usr/local/bin: $(ls /usr/local/bin)"
[[ ! -e /root/dashboard-v1.tgz && ! -e /usr/local/bin/durable-server.failed ]] && ok "the upload and the failed binary are cleaned up" || bad "leftovers"

echo "2. an upgrade"
build v2 ok
deploy v2 && ok "succeeds" || { tail -8 /tmp/deploy-v2.out; bad "failed"; }
[[ "$(live)" == v2 && "$(prev)" == v1 && "$(dash)" == v2 ]] && ready && csp_ok && ok "v2 live, v1 kept as .prev, dashboard v2" || bad "live $(live) prev $(prev) dash $(dash)"
grep -q 'upgrade ok' /tmp/deploy-v2.out && ok "through upgrade.sh (staged copy, format guard, smoke)" || bad "upgrade.sh didn't report ok"

echo "3. --rollback"
/kit/deploy/droplet/direct/droplet-deploy.sh --rollback >/tmp/rollback.out 2>&1 && ok "succeeds" || { tail -5 /tmp/rollback.out; bad "failed"; }
[[ "$(live)" == v1 && "$(dash)" == v1 ]] && ready && csp_ok && ok "v1 and its dashboard back, the CSP matching it" || bad "after --rollback: live $(live) dash $(dash)"

echo "4. another upgrade"
build v3 ok
deploy v3 && [[ "$(live)" == v3 && "$(dash)" == v3 ]] && ok "v3 live" || { tail -5 /tmp/deploy-v3.out; bad "v3: live $(live)"; }
n=$(find /opt/durable/dashboards -mindepth 1 -maxdepth 1 -type d | wc -l)
(( n <= 2 )) && ok "dashboards kept: current and previous ($n)" || bad "$n dashboards kept"

echo "5. a build that dies on start"
before=$(pid)
build v4 crash
deploy v4 && bad "a crashing build was reported live" || ok "fails"
[[ "$(live)" == v3 && "$(dash)" == v3 ]] && ready && csp_ok && ok "restore_prev put v3 back, and its dashboard, the CSP matching it" || bad "after a crash: live $(live) dash $(dash)"
[[ "$(pid)" != "$before" ]] && ok "(v3 restarted by upgrade.sh)" || bad "v3 wasn't restarted"
[[ ! -d "$(ls -d /opt/durable/dashboards/*v4* 2>/dev/null || echo /nonexistent)" ]] && ok "the failed dashboard is gone" || bad "v4's dashboard is left"

echo "6. a build that is up but fails its smoke"
build v5 nodb
deploy v5 && bad "a build without its account store was reported live" || ok "fails"
grep -q 'rollback.sh' /tmp/deploy-v5.out && ok "rollback.sh was run" || bad "no rollback.sh: $(tail -5 /tmp/deploy-v5.out)"
[[ "$(live)" == v3 && "$(dash)" == v3 ]] && ready && csp_ok && ok "v3 back and serving, the CSP matching its dashboard" || bad "after a failed smoke: live $(live) dash $(dash)"

echo "7. a lab build"
before=$(pid); : >"$S/calls"
build v6 lab
deploy v6 && bad "a lab build was deployed" || { grep -q 'LAB' /tmp/deploy-v6.out && ok "refused as a LAB build" || bad "refused for another reason: $(tail -3 /tmp/deploy-v6.out)"; }
[[ "$(pid)" == "$before" && "$(live)" == v3 ]] && ! grep -qE 'systemctl (stop|start|restart)' "$S/calls" && ok "nothing was stopped or swapped" || bad "the lab refusal touched the service: $(cat "$S/calls")"

echo "8. a stale build record"
build v7 ok
echo "0000000000000000000000000000000000000000 x y" >"$B/built"
deploy v7 && bad "deployed without a matching build record" || { grep -q 'last build was of' /tmp/deploy-v7.out && ok "refused: the record names another commit" || bad "refused for another reason: $(tail -3 /tmp/deploy-v7.out)"; }
[[ "$(live)" == v3 ]] && ok "v3 still live" || bad "live $(live)"

echo "9. droplet-build.sh (a local repository as GitHub; a fake cargo and rustup)"
printf '[toolchain]\nchannel = "1.99.1"\n' >"$src/rust-toolchain.toml"
git -C "$src" add -A; git -C "$src" commit -qm toolchain
git init -q --bare /srv/dr.git
git -C "$src" push -q /srv/dr.git HEAD:refs/heads/main
main_commit=$(git -C "$src" rev-parse HEAD)
git -C "$src" commit -q --allow-empty -m "not on main"; git -C "$src" push -q /srv/dr.git HEAD:refs/heads/side
side_commit=$(git -C "$src" rev-parse HEAD)
mkdir -p /root/.cargo/bin /opt/stub/builds
mkstub /opt/stub/builds/ok vB ok; mkstub /opt/stub/builds/lab vL lab
cat >/root/.cargo/bin/cargo <<'EOF'
#!/usr/bin/env bash
echo "cargo $*" >>/run/fake-systemd/cargo-calls
mkdir -p "$CARGO_TARGET_DIR/release"
cp "/opt/stub/builds/${FAKE_BUILD_MODE:-ok}" "$CARGO_TARGET_DIR/release/durable-server"
printf '#!/bin/sh\necho durable-cli\n' >"$CARGO_TARGET_DIR/release/durable-cli"; chmod 0755 "$CARGO_TARGET_DIR/release/durable-cli"
echo "    Finished \`release\` profile [optimized] target(s)"
EOF
cat >/root/.cargo/bin/rustup <<'EOF'
#!/usr/bin/env bash
echo "rustup $*" >>/run/fake-systemd/cargo-calls
[[ "$1 $2" == "toolchain list" ]] && echo "1.99.1-x86_64-unknown-linux-gnu (default)"
exit 0
EOF
chmod +x /root/.cargo/bin/cargo /root/.cargo/bin/rustup
b() { env DURABLE_BUILD_DIR=/opt/b2 DURABLE_REPO_URL=file:///srv/dr.git "$@" >/tmp/build.out 2>&1; }
b /kit/deploy/droplet/direct/droplet-build.sh "$main_commit" && ok "builds a commit on main" || { cat /tmp/build.out; bad "the build of main failed"; }
read -r rc rs _ </opt/b2/built 2>/dev/null || true
[[ "$rc" == "$main_commit" && "$rs" == "$(sha256sum /opt/b2/target/release/durable-server | cut -d' ' -f1)" && "$(git -C /opt/b2/src rev-parse HEAD)" == "$main_commit" ]] \
  && ok "records the commit and the binaries' sha256; the checkout is at the commit" || bad "build record: $(cat /opt/b2/built 2>/dev/null)"
grep -q -- 'build --release --locked -p durable-server -p durable-cli' /run/fake-systemd/cargo-calls && ! grep -q -- '--features' /run/fake-systemd/cargo-calls \
  && ok "cargo build --release --locked, both packages, no features" || bad "cargo was called as: $(cat /run/fake-systemd/cargo-calls)"
b /kit/deploy/droplet/direct/droplet-build.sh "$main_commit" && ok "a second build of the same commit (incremental)" || bad "the second build failed"
b /kit/deploy/droplet/direct/droplet-build.sh "$side_commit" && bad "built a commit that isn't on main" \
  || { grep -q "isn't on GitHub's main" /tmp/build.out && [[ ! -e /opt/b2/built ]] && ok "refuses a commit that isn't on main, and drops the old record" || bad "side-branch refusal: $(cat /tmp/build.out)"; }
b env FAKE_BUILD_MODE=lab /kit/deploy/droplet/direct/droplet-build.sh "$main_commit" && bad "recorded a lab build" \
  || { grep -q 'LAB' /tmp/build.out && [[ ! -e /opt/b2/built ]] && ok "refuses to record a lab build" || bad "lab refusal: $(cat /tmp/build.out)"; }
b /kit/deploy/droplet/direct/droplet-build.sh main && bad "took a branch name for a commit" || ok "takes only a full commit id"

systemctl stop durable-server
(( fails == 0 )) || { echo "deploy-scenario: $fails FAILED"; exit 1; }
echo "deploy-scenario: all green"
