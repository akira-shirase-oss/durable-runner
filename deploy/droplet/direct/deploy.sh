#!/usr/bin/env bash
# deploy.sh — a commit of main live on the shared droplet in under 10 minutes, no CI in the way
# (Colophon's direct deploy, D-242, for Durable Runner). Run from a checkout on the maintainer's
# machine:
#
#   DURABLE_DEPLOY_HOST=<ssh alias> DURABLE_PUBLIC_URL=https://durablerunnerapp.404human.dev \
#     deploy/droplet/direct/deploy.sh origin/main        # or any commit on origin/main
#   ... deploy/droplet/direct/deploy.sh --rollback        # the previous binary and dashboard
#
# 1. fetch; <rev> must be on origin/main and carry this kit;
# 2. droplet-build.sh and droplet-deploy.sh go up from THAT commit (not this working tree), and the
#    droplet builds durable-server + durable-cli at it (incremental, nice, detached through
#    systemd-run so a dropped SSH can't stop it), while
# 3. the dashboard is built here at the same commit (git archive, npm ci, build) and uploaded;
# 4. droplet-deploy.sh installs through deploy/upgrade.sh and rolls back on any failed check;
# 5. /health, /ready and the dashboard are checked through the public URL; the time is printed.
# Settings: DURABLE_DASHBOARD_PATH (/app/), DURABLE_DASHBOARD_APP (apps/inspector),
# DURABLE_BUILD_TIMEOUT_SECS (1800: the first build on a droplet is cold), DURABLE_DEPLOY_TIMEOUT_SECS (600).
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
die() { echo "deploy: error: $*" >&2; exit 1; }
HOST=${DURABLE_DEPLOY_HOST:-}
URL=${DURABLE_PUBLIC_URL:-}
DASH=${DURABLE_DASHBOARD_PATH:-/app/}
APP=${DURABLE_DASHBOARD_APP:-apps/inspector}
BUILD_TIMEOUT=${DURABLE_BUILD_TIMEOUT_SECS:-1800}
DEPLOY_TIMEOUT=${DURABLE_DEPLOY_TIMEOUT_SECS:-600}
[[ "$HOST" =~ ^[A-Za-z0-9][A-Za-z0-9._@-]*$ ]] || die "set DURABLE_DEPLOY_HOST to the droplet's ssh alias"
URL=${URL%/}
[[ "$URL" =~ ^https://([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || die "set DURABLE_PUBLIC_URL to https://<public host>"
[[ "$DASH" =~ ^/([a-z0-9_-]+/)+$ ]] || die "DURABLE_DASHBOARD_PATH must look like /app/"
[[ "$APP" =~ ^apps/[a-z0-9_-]+$ ]] || die "DURABLE_DASHBOARD_APP must be apps/<name>"
for v in BUILD_TIMEOUT DEPLOY_TIMEOUT; do [[ "${!v}" =~ ^[0-9]+$ ]] || die "$v must be a whole number of seconds"; done
REMOTE=/root/durable-deploy
t0=$SECONDS
step() { printf '%4ss  %s\n' "$((SECONDS - t0))" "$*"; }
remote() { ssh -o ConnectTimeout=15 -o BatchMode=yes "$HOST" "$@"; }

# wait_status <name> <timeout>: the exit status the detached job wrote, polled every few seconds.
# A failed poll (a dropped connection) is retried until the deadline; the job runs on regardless.
wait_status() {
  local deadline=$((SECONDS + $2)) st
  while (( SECONDS < deadline )); do
    st=$(remote "cat $REMOTE/$1.status 2>/dev/null" 2>/dev/null || true)
    if [[ -n "$st" ]]; then
      [[ "$st" =~ ^[0-9]+$ ]] || die "$1 wrote a status that isn't a number"
      printf '%s' "$st"; return 0
    fi
    sleep 5
  done
  return 1
}
# detached <name> <command>: runs on the droplet as a transient systemd unit, output in
# $REMOTE/<name>.out, exit status in $REMOTE/<name>.status. <command> holds only validated words.
detached() {
  remote "rm -f $REMOTE/$1.status && systemd-run --quiet --collect --unit=durable-$1-$(date +%s) --setenv=HOME=/root /bin/sh -c '$2 >$REMOTE/$1.out 2>&1; echo \$? >$REMOTE/$1.status'"
}
public_checks() {
  local p code body
  for p in /health /ready "$DASH"; do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "$URL$p" 2>/dev/null || echo 000)
    step "$URL$p $code"
    [[ "$code" == 200 ]] || die "$URL$p answered $code"
  done
  body=$(curl -sS --max-time 10 "$URL/ready" 2>/dev/null || true)
  [[ "$body" == *'"ready":true'* ]] || die "$URL/ready says: ${body:-nothing}"
}

if [[ "${1:-}" == --rollback ]]; then
  step "rolling back on $HOST"
  remote "$REMOTE/droplet-deploy.sh --rollback"
  public_checks
  step "rolled back in $((SECONDS - t0)) s"
  exit 0
fi

rev=${1:-}
[[ -n "$rev" ]] || die "usage: deploy.sh <rev on origin/main> | --rollback"
git -C "$REPO" fetch -q origin main
commit=$(git -C "$REPO" rev-parse --verify --quiet "$rev^{commit}") || die "'$rev' isn't a commit here"
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || die "can't resolve '$rev'"
git -C "$REPO" merge-base --is-ancestor "$commit" origin/main || die "${commit:0:12} isn't on origin/main: only reviewed, merged commits deploy"
for f in deploy/droplet/direct/droplet-build.sh deploy/droplet/direct/droplet-deploy.sh deploy/droplet/lib.sh "$APP/package-lock.json"; do
  git -C "$REPO" cat-file -e "$commit:$f" 2>/dev/null || die "${commit:0:12} has no $f (it predates the shared-droplet kit?)"
done
step "deploying ${commit:0:12} to $HOST"

work=$(mktemp -d "${TMPDIR:-/tmp}/durable-deploy.XXXXXX")
trap 'rm -rf "$work"' EXIT
for f in droplet-build.sh droplet-deploy.sh; do git -C "$REPO" show "$commit:deploy/droplet/direct/$f" >"$work/$f"; done
chmod 0700 "$work"/droplet-*.sh
remote "install -d -m 0700 $REMOTE"
scp -q -o ConnectTimeout=15 -o BatchMode=yes "$work/droplet-build.sh" "$work/droplet-deploy.sh" "$HOST:$REMOTE/"
remote "chmod 0700 $REMOTE/droplet-build.sh $REMOTE/droplet-deploy.sh"

# The server builds on the droplet while the dashboard builds here.
detached build "$REMOTE/droplet-build.sh $commit"
step "droplet build started"
mkdir "$work/src"
git -C "$REPO" archive "$commit" "$APP" | tar -x -C "$work/src"
(cd "$work/src/$APP" && npm ci --no-audit --no-fund --loglevel=error >/dev/null && npm run build >/dev/null) \
  || die "the dashboard build failed (rerun it in $APP to see why); the droplet build carries on, nothing is installed"
[[ -f "$work/src/$APP/build/index.html" ]] || die "the dashboard build has no index.html"
COPYFILE_DISABLE=1 tar --no-xattrs -czf "$work/dashboard.tgz" -C "$work/src/$APP/build" . 2>/dev/null \
  || COPYFILE_DISABLE=1 tar -czf "$work/dashboard.tgz" -C "$work/src/$APP/build" .
scp -q -o ConnectTimeout=15 -o BatchMode=yes "$work/dashboard.tgz" "$HOST:$REMOTE/dashboard-$commit.tgz"
step "dashboard built and uploaded ($(du -h "$work/dashboard.tgz" | cut -f1))"

st=$(wait_status build "$BUILD_TIMEOUT") || die "the droplet build didn't finish within $BUILD_TIMEOUT s (it may still be running: ssh $HOST tail $REMOTE/build.out)"
if [[ "$st" != 0 ]]; then remote "tail -n 25 $REMOTE/build.out" >&2 || true; die "the droplet build failed (above)"; fi
step "server built on the droplet ($(remote "grep -o 'built in [0-9]* s' $REMOTE/build.out | tail -1" || true))"

detached deploy "$REMOTE/droplet-deploy.sh $commit $REMOTE/dashboard-$commit.tgz"
st=$(wait_status deploy "$DEPLOY_TIMEOUT") || die "the install didn't finish within $DEPLOY_TIMEOUT s: ssh $HOST tail $REMOTE/deploy.out"
remote "tail -n 12 $REMOTE/deploy.out" || true
[[ "$st" == 0 ]] || die "the install failed (above); it rolled back if it had switched"

public_checks
elapsed=$((SECONDS - t0))
step "done: ${commit:0:12} live in $elapsed s"
(( elapsed <= 600 )) || echo "deploy: note: over the 10-minute budget (a cold build or toolchain install?)" >&2
