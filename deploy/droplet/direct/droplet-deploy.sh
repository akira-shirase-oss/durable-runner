#!/usr/bin/env bash
# droplet-deploy.sh — on the droplet, as root (direct/deploy.sh uploads it from the commit and
# starts it detached):
#
#   droplet-deploy.sh <commit> <dashboard.tgz>   install the build droplet-build.sh made at <commit>
#   droplet-deploy.sh --rollback                 back to the previous binary and dashboard
#   droplet-deploy.sh --status
#
# The binary goes through deploy/upgrade.sh at that commit, unchanged: a root-owned staged copy,
# the lab guard, the unit stopped, the D-056 format guard on the quiet journal, the swap with
# /usr/local/bin/durable-server.prev kept, start, /ready, smoke, and restore_prev (guard, start,
# /ready, smoke) when the start or /ready fails. On this host deploy/smoke.sh hands over to
# deploy/droplet/smoke.sh. When upgrade.sh fails with the NEW binary in place (it started and was
# ready, but its smoke failed), this runs deploy/rollback.sh. The very first install (no binary
# yet) takes the same steps by hand, and stops the unit if they fail.
#
# Around it: the binary installed must be the one droplet-build.sh recorded for <commit> (sha256),
# copied out of the build tree under the build lock; the dashboard is unpacked into its own
# release directory and switched through the /opt/durable/dashboard link (switched back on any
# failure); durable-run, durable-lab-guard, durable-saas-run and the unit come from the same
# commit. It keeps the current and previous binary and dashboard, deletes the upload, and wipes
# the build cache past DURABLE_BUILD_CACHE_CAP_GB (8).
set -euo pipefail
BUILD=${DURABLE_BUILD_DIR:-/opt/durable-build}
src=$BUILD/src
[[ -f "$src/deploy/droplet/lib.sh" ]] || { echo "droplet-deploy: error: no checkout at $src (droplet-build.sh makes it)" >&2; exit 1; }
# shellcheck source=deploy/droplet/lib.sh
. "$src/deploy/droplet/lib.sh"
CAP_GB=${DURABLE_BUILD_CACHE_CAP_GB:-8}
is_uint "$CAP_GB" || die "DURABLE_BUILD_CACHE_CAP_GB must be a whole number"
LOCAL=http://127.0.0.1:$DR_PUBLIC_PORT
start=$SECONDS
log() { printf '%s  %s\n' "$(date -u +%H:%M:%S)" "$*"; }
[[ $EUID -eq 0 ]] || die "run as root"

dash_now() { [[ -L "$DR_DASH_LINK" ]] && basename "$(readlink "$DR_DASH_LINK")" || true; }
dash_prev() { cat "$DR_OPT/dashboard.prev" 2>/dev/null || true; }
# switch_dash <name>: the link, replaced atomically (GNU mv -T).
switch_dash() {
  [[ -n "$1" && -d "$DR_DASHES/$1" ]] || return 0
  ln -sfn "$DR_DASHES/$1" "$DR_DASH_LINK.next"
  mv -Tf "$DR_DASH_LINK.next" "$DR_DASH_LINK"
}
wait_ready() {
  local deadline=$((SECONDS + 90)) code
  while (( SECONDS < deadline )); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "$LOCAL/ready" 2>/dev/null || true)
    [[ "$code" == 200 ]] && return 0
    sleep 0.25
  done
  return 1
}

if [[ "${1:-}" == --status ]]; then
  echo "binary     $(sha256_of "$DR_BIN/durable-server" 2>/dev/null || echo none)  $("$DR_BIN/durable-server" --version 2>/dev/null || true)"
  echo "previous   $(sha256_of "$DR_BIN/durable-server.prev" 2>/dev/null || echo none)"
  echo "deployed   $(cat "$DR_OPT/deployed" 2>/dev/null || echo unknown)"
  echo "dashboard  $(dash_now) (previous: $(dash_prev))"
  echo "unit       $(systemctl is-active "$DR_UNIT" 2>/dev/null || true)"
  exit 0
fi

exec 9>"$BUILD/.lock"
flock -w 120 9 || die "a build or deploy has held $BUILD/.lock for 2 minutes"

if [[ "${1:-}" == --rollback ]]; then
  prev=$(dash_prev) now=$(dash_now)
  log "rolling back: deploy/rollback.sh (previous binary), dashboard ${now:-none} -> ${prev:-unchanged}"
  (cd "$src" && deploy/rollback.sh) || die "deploy/rollback.sh failed (above); the dashboard was left as it was"
  switch_dash "$prev"
  echo "rolled back $(date -u +%FT%TZ) from: $(cat "$DR_OPT/deployed" 2>/dev/null || echo unknown)" >"$DR_OPT/deployed"
  log "rolled back ($((SECONDS - start)) s)"
  exit 0
fi

commit=${1:-} tgz=${2:-}
[[ "$commit" =~ ^[0-9a-f]{40}$ && -n "$tgz" ]] || die "usage: droplet-deploy.sh <commit> <dashboard.tgz> | --rollback | --status"
[[ -s "$tgz" ]] || die "no dashboard tarball at $tgz"
[[ "$(git -C "$src" rev-parse HEAD)" == "$commit" ]] || die "$src isn't at $commit: run droplet-build.sh $commit first"
read -r built_commit sum_server sum_cli <"$BUILD/built" 2>/dev/null || die "no build record ($BUILD/built): run droplet-build.sh $commit"
[[ "$built_commit" == "$commit" ]] || die "the last build was of $built_commit, not $commit"

# The binaries, copied out of the build tree and checked against the build's record.
incoming=$DR_OPT/incoming
rm -rf "$incoming"; mkdir -m 0700 "$incoming"
trap 'rm -rf "$incoming"' EXIT
cp "$BUILD/target/release/durable-server" "$BUILD/target/release/durable-cli" "$incoming/"
[[ "$(sha256_of "$incoming/durable-server")" == "$sum_server" && "$(sha256_of "$incoming/durable-cli")" == "$sum_cli" ]] \
  || die "the binaries in $BUILD/target/release aren't the ones droplet-build.sh recorded for $commit"
"$src/deploy/lab-build-guard.sh" "$incoming/durable-server" || die "a LAB build: refusing"
log "build of ${commit:0:12} verified ($("$incoming/durable-server" --version 2>/dev/null | head -1))"

# What the service needs before anything stops.
need_accounts=$(env_get DURABLE_REQUIRE_ACCOUNTS "$DR_ENV"); need_mail=$(env_get DURABLE_REQUIRE_MAIL "$DR_ENV")
for k in api_key $([[ "${need_accounts:-1}" == 1 ]] && echo database_url) $([[ "${need_mail:-1}" == 1 ]] && echo resend_api_key); do
  [[ -s "$DR_CREDS/$k" ]] || die "the $k credential is missing: put-secret.sh $k (nothing was changed)"
done
[[ -s "$DR_CREDS/read_key" ]] || die "the read_key credential is missing: run install.sh (nothing was changed)"
[[ "$(head -n 1 "$DR_UNIT_FILE" 2>/dev/null)" == "$DR_MARK" ]] || die "$DR_UNIT_FILE isn't this kit's: run install.sh first"
port_check "$DR_PUBLIC_PORT" "the public listener"
if [[ -n "$(env_get DURABLE_OPS_LISTEN "$DR_ENV")" ]]; then port_check "$DR_OPS_PORT" "the ops listener"; fi

# The dashboard, into its own release directory.
name=src-${commit:0:12}-$(date -u +%Y%m%dT%H%M%SZ)
ensure_dir "$DR_DASHES" 0755
rm -rf "$DR_DASHES/$name.partial"; mkdir "$DR_DASHES/$name.partial"
tar -xzf "$tgz" --no-same-owner -C "$DR_DASHES/$name.partial"
[[ -f "$DR_DASHES/$name.partial/index.html" ]] || die "the dashboard tarball has no index.html"
chmod -R u=rwX,go=rX "$DR_DASHES/$name.partial"
mv "$DR_DASHES/$name.partial" "$DR_DASHES/$name"
old_dash=$(dash_now)

# Start-up scripts and the unit, from the same commit.
if install_scripts "$src/deploy"; then log "start-up scripts updated in $DR_BIN"; fi
if put_file "$src/deploy/droplet/durable-server.service" "$DR_UNIT_FILE" 0644; then systemctl daemon-reload; log "unit updated"; fi

fail() { # <message>: the dashboard goes back, the new one is deleted, and we stop.
  if [[ -n "$old_dash" ]]; then switch_dash "$old_dash"; fi
  rm -rf "${DR_DASHES:?}/$name"
  die "$1"
}
log "switching dashboard ${old_dash:-none} -> $name"
switch_dash "$name"

if [[ ! -e "$DR_BIN/durable-server" ]]; then
  # The first install: upgrade.sh needs a binary to keep as .prev, so these are its steps by hand.
  log "first install of durable-server"
  stage=$(mktemp "$DR_BIN/.durable-server.install.XXXXXX")
  install -m 0755 "$incoming/durable-server" "$stage"
  "$src/deploy/lab-build-guard.sh" "$stage" || { rm -f "$stage"; fail "a LAB build"; }
  if systemctl is-active --quiet "$DR_UNIT"; then rm -f "$stage"; fail "$DR_UNIT is active with no binary installed: investigate"; fi
  (cd "$src" && FORMAT_GUARD_FILES_ONLY=1 deploy/format-guard.sh "$stage") || { rm -f "$stage"; fail "the format guard refused the build (above)"; }
  mv -f "$stage" "$DR_BIN/durable-server"
  if ! systemctl start "$DR_UNIT" || ! wait_ready || ! "$src/deploy/droplet/smoke.sh" --release; then
    # Fail closed: a first release that fails its checks doesn't serve. The binary is moved aside so
    # the next deploy is a first install again (upgrade.sh would keep it as .prev).
    systemctl stop "$DR_UNIT" || true
    mv -f "$DR_BIN/durable-server" "$DR_BIN/durable-server.failed"
    fail "the first release failed its checks (above); the unit is stopped: journalctl -u $DR_UNIT"
  fi
else
  log "deploy/upgrade.sh (staged copy, format guard, swap, /ready, smoke; restore_prev on a failed start)"
  if ! (cd "$src" && deploy/upgrade.sh "$incoming/durable-server"); then
    if [[ "$(sha256_of "$DR_BIN/durable-server")" == "$sum_server" ]]; then
      log "the new build is in place but failed its checks: deploy/rollback.sh"
      (cd "$src" && deploy/rollback.sh) || fail "upgrade failed its checks and deploy/rollback.sh failed too (above): journalctl -u $DR_UNIT"
      fail "${commit:0:12} failed its checks; rolled back to the previous binary and dashboard"
    fi
    fail "upgrade.sh refused or restored the previous binary (above); the previous dashboard is back"
  fi
fi
put_file "$incoming/durable-cli" "$DR_BIN/durable-cli" 0755 || true
if [[ -n "$old_dash" && "$old_dash" != "$name" ]]; then echo "$old_dash" >"$DR_OPT/dashboard.prev"; fi
echo "$commit $name $(date -u +%FT%TZ)" >"$DR_OPT/deployed"
log "${commit:0:12} is live ($((SECONDS - start)) s)"

# Cleanup: the current and previous dashboards stay; the upload goes; the build cache is capped.
keep_prev=$(dash_prev)
for d in "$DR_DASHES"/*; do
  [[ -d "$d" ]] || continue
  n=$(basename "$d")
  [[ "$n" == "$name" || "$n" == "$keep_prev" ]] && continue
  rm -rf "$d" && log "removed old dashboard $n"
done
rm -f "$tgz" "$DR_BIN/durable-server.failed"
size=$(du -sk "$BUILD/target" 2>/dev/null | cut -f1 || echo 0)
if is_uint "$size" && (( size > CAP_GB * 1024 * 1024 )); then
  rm -rf "$BUILD/target" "$BUILD/built" && log "build cache was $((size / 1048576)) GB (cap $CAP_GB): wiped, the next build is cold"
fi
log "disk: $(df -h / | awk 'NR==2 {print $4" free of "$2}'), build cache $(du -sh "$BUILD/target" 2>/dev/null | cut -f1 || echo 0)"
