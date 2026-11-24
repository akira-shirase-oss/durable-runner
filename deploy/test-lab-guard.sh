#!/usr/bin/env bash
# S3 re-review 3: install.sh, upgrade.sh and smoke.sh refuse a lab build
# of durable-server (its --help lists --allow-local-tenant-handlers).
# Stub binaries stand in for the builds; each script is also run with
# its guard line removed and must then let the lab build through.
# Never installs anything: the guard runs before the root check, and
# the known-bad copies only run as a normal user.
#   deploy/test-lab-guard.sh
# shellcheck disable=SC2015  # pass and flunk only print, so `t && pass || flunk` is if-then-else here.
set -euo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-lab.XXXXXX")"
trap 'rm -rf "$work"' EXIT
fail=0
pass() { printf '  ok    %s\n' "$1"; }
flunk() { printf '  FAIL  %s\n' "$1"; fail=1; }

stub() { # name help-text exit-code
  printf '#!/usr/bin/env bash\nprintf %%s %q\nexit %s\n' "$2" "$3" >"$work/$1"
  chmod +x "$work/$1"
}
stub prod $'Usage: durable-server [OPTIONS]\n      --journal <JOURNAL>\n      --allow-local-handlers\n' 0
stub lab $'Usage: durable-server [OPTIONS]\n      --journal <JOURNAL>\n      --allow-local-tenant-handlers\n' 0
stub broken 'segfault' 1
stub other $'Usage: something-else [OPTIONS]\n' 0

echo "lab-build-guard.sh"
deploy/lab-build-guard.sh "$work/prod" 2>/dev/null && pass "a production build passes" || flunk "a production build was refused"
for b in lab broken other; do
  deploy/lab-build-guard.sh "$work/$b" 2>/dev/null && flunk "the $b binary passed" || pass "the $b binary is refused"
done

# A copy of the repo's deploy/ with target/release/durable-server = $1.
tree() { # binary [drop-guard]
  rm -rf "$work/tree"
  mkdir -p "$work/tree/target/release"
  cp -R deploy "$work/tree/deploy"
  cp "$work/$1" "$work/tree/target/release/durable-server"
  if [[ "${2:-}" == drop ]]; then
    for f in install.sh upgrade.sh smoke.sh; do
      grep -v 'lab-build-guard.sh' "$work/tree/deploy/$f" >"$work/tree/deploy/$f.new"
      mv "$work/tree/deploy/$f.new" "$work/tree/deploy/$f"
      chmod +x "$work/tree/deploy/$f"
    done
  fi
}
refused_lab() { grep -q 'LAB build' <<<"$1"; }

echo "install.sh / upgrade.sh"
tree lab
out=$(bash "$work/tree/deploy/install.sh" 2>&1 || true)
refused_lab "$out" && pass "install.sh refuses a lab build" || flunk "install.sh did not refuse a lab build: $out"
out=$(bash "$work/tree/deploy/upgrade.sh" "$work/tree/target/release/durable-server" 2>&1 || true)
refused_lab "$out" && pass "upgrade.sh refuses a lab build" || flunk "upgrade.sh did not refuse a lab build: $out"

echo "smoke.sh"
smoke_line() { # binary → the binary check's line
  DURABLE_SERVER_BIN="$work/$1" bash "$work/tree/deploy/smoke.sh" --local-only 2>/dev/null \
    | grep -E 'production build|LAB build' || true
}
[[ "$(smoke_line lab)" == *FAIL*"LAB build"* ]] && pass "smoke.sh fails a lab build" || flunk "smoke.sh accepted a lab build"
[[ "$(smoke_line prod)" == *ok*"production build"* ]] && pass "smoke.sh accepts a production build" || flunk "smoke.sh refused a production build"

echo "known-bad: the scripts with their guard line removed"
if [[ $EUID -eq 0 ]]; then
  echo "  skip  known-bad runs need a normal user (as root the copies would install)"
else
  tree lab drop
  out=$(bash "$work/tree/deploy/install.sh" 2>&1 || true)
  refused_lab "$out" && flunk "install.sh without its guard still refused — the check proves nothing" \
    || pass "install.sh without its guard lets the lab build through (to the root check)"
  out=$(bash "$work/tree/deploy/upgrade.sh" "$work/tree/target/release/durable-server" 2>&1 || true)
  refused_lab "$out" && flunk "upgrade.sh without its guard still refused" \
    || pass "upgrade.sh without its guard lets the lab build through"
  [[ -z "$(smoke_line lab)" ]] && pass "smoke.sh without its guard says nothing about the binary" \
    || flunk "smoke.sh without its guard still checked the binary"
fi
(( fail == 0 )) && echo "test-lab-guard: all green" || { echo "test-lab-guard: FAILED"; exit 1; }
