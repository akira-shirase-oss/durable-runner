#!/usr/bin/env bash
# S3 re-reviews: no lab build of durable-server (its --help lists
# --allow-local-tenant-handlers) is installed, started, restored or
# accepted by smoke:
#   - install.sh and upgrade.sh refuse one before anything else;
#   - durable-run.sh (every start) refuses whatever the env file names;
#   - smoke.sh judges the binary systemd runs (/proc/<MainPID>/exe), never
#     DURABLE_SERVER_BIN from the operator's shell;
#   - upgrade.sh judges, format-guards and installs ONE staged copy, and its
#     restore paths and rollback.sh guard the saved binary first.
# Stub binaries stand in for the builds, a stub systemctl and a fake /proc
# for the droplet; each rule is also checked on a copy with it removed,
# which must then let the lab build through. Never installs anything: the
# guard runs before the root check, and the known-bad runs need a normal user.
#   deploy/test-lab-guard.sh
# shellcheck disable=SC2015,SC2016  # pass/flunk only print (so `t && pass || flunk` is if-then-else); the single-quoted $ strings are literal source text.
set -euo pipefail
cd "$(dirname "$0")/.."
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-lab.XXXXXX")"
trap 'rm -rf "${work:?}"' EXIT
fail=0
pass() { printf '  ok    %s\n' "$1"; }
flunk() { printf '  FAIL  %s\n' "$1"; fail=1; }
edit() { # sed-script file — in place, portably
  sed "$1" "$2" >"$2.new" && mv "$2.new" "$2"
}

stub() { # name help-text help-exit — runs record themselves in $RAN
  printf '#!/usr/bin/env bash\nif [[ "${1:-}" == --help ]]; then printf %%s %q; exit %s; fi\n[[ -n "${RAN:-}" ]] && : >"$RAN"\nexit 0\n' \
    "$2" "$3" >"$work/$1"
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

# A copy of deploy/ with target/release/durable-server = $1; `drop` removes
# each script's guard (the known-bad copies).
tree() { # binary [drop]
  rm -rf "${work:?}/tree"
  mkdir -p "$work/tree/target/release"
  cp -R deploy "$work/tree/deploy"
  cp "$work/$1" "$work/tree/target/release/durable-server"
  if [[ "${2:-}" == drop ]]; then
    local d="$work/tree/deploy"
    edit '/^deploy\/lab-build-guard.sh target\/release\/durable-server/d' "$d/install.sh"
    edit '/^"\$guard" "\$new" || exit 1$/d' "$d/upgrade.sh"
    edit '/^"\$guard" "\$bin" || exit 1$/d' "$d/durable-run.sh"
    edit 's#"$(dirname "$0")/lab-build-guard.sh" "$proc/$pid/exe"#true#' "$d/smoke.sh"
  fi
}
refused_lab() { grep -q 'LAB build' <<<"$1"; }

# A stub systemctl (MainPID from $FAKE_PID) and a fake /proc whose
# <pid>/exe is the running binary.
mkdir -p "$work/bin" "$work/proc/4242"
cat >"$work/bin/systemctl" <<'SYS'
#!/usr/bin/env bash
case "$*" in
  *"MainPID --value durable-server"*) echo "${FAKE_PID:-0}" ;;
  *MemoryCurrent*) echo "" ;;
  *) exit 3 ;;
esac
SYS
chmod +x "$work/bin/systemctl"
running() { ln -sf "$work/$1" "$work/proc/4242/exe"; }
smoke_line() { # [VAR=value...] → smoke's line about the running binary
  env PATH="$work/bin:$PATH" DURABLE_PROC_ROOT="$work/proc" FAKE_PID="${FAKE_PID:-4242}" "$@" \
    bash "$work/tree/deploy/smoke.sh" --local-only 2>/dev/null \
    | grep -E 'production build|LAB build|no running durable-server' || true
}
run_start() { # binary → durable-run's output; $work/ran exists if it exec'd the binary
  rm -f "${work:?}/ran"
  mkdir -p "$work/etc"
  RAN="$work/ran" DURABLE_ETC="$work/etc" DURABLE_SERVER_BIN="$work/$1" \
    bash "$work/tree/deploy/durable-run.sh" 2>&1 || true
}

tree lab
echo "install.sh / upgrade.sh"
out=$(bash "$work/tree/deploy/install.sh" 2>&1 || true)
refused_lab "$out" && pass "install.sh refuses a lab build" || flunk "install.sh did not refuse a lab build: $out"
out=$(bash "$work/tree/deploy/upgrade.sh" "$work/tree/target/release/durable-server" 2>&1 || true)
refused_lab "$out" && pass "upgrade.sh refuses a lab build" || flunk "upgrade.sh did not refuse a lab build: $out"

echo "durable-run.sh (every start)"
out=$(run_start lab)
[[ ! -e "$work/ran" ]] && refused_lab "$out" && pass "a lab build named by the env file is never started" \
  || flunk "durable-run started a lab build: $out"
out=$(run_start prod)
[[ -e "$work/ran" ]] && pass "a production build starts" || flunk "durable-run refused a production build: $out"

echo "smoke.sh (the binary systemd runs)"
running lab
[[ "$(smoke_line)" == *FAIL*"LAB build"* ]] && pass "smoke fails a running lab build" || flunk "smoke accepted a running lab build"
[[ "$(smoke_line DURABLE_SERVER_BIN="$work/prod")" == *FAIL*"LAB build"* ]] \
  && pass "smoke ignores DURABLE_SERVER_BIN from the shell" || flunk "smoke judged DURABLE_SERVER_BIN, not the running binary"
running prod
[[ "$(smoke_line)" == *ok*"production build"* ]] && pass "smoke accepts a running production build" || flunk "smoke refused a running production build"
[[ "$(FAKE_PID=0 smoke_line)" == *FAIL*"no running durable-server"* ]] \
  && pass "smoke fails when nothing runs to judge" || flunk "smoke passed with no running server"

# upgrade.sh past the root check, and rollback.sh, need root and a droplet:
# their rules are checked in the source.
check_upgrade() { # file → 0 when one staged copy is judged and installed, and restores are guarded
  local f="$1" ok=0 code after body g c
  code=$(grep -v '^[[:space:]]*#' "$f")
  grep -qF 'install -m 0755 "$new" "$stage"' <<<"$code" || { echo "  no staged copy"; ok=1; }
  after=$(awk 'f; index($0, "install -m 0755 \"$new\" \"$stage\"") {f=1}' <<<"$code")
  if grep -qF '$new' <<<"$after"; then echo "  the original path is read after staging"; ok=1; fi
  grep -qF '"$guard" "$stage"' <<<"$after" || { echo "  the staged copy is not guarded"; ok=1; }
  grep -qF 'format-guard.sh "$stage"' <<<"$after" || { echo "  the staged copy is not format-guarded"; ok=1; }
  grep -qF 'install -m 0755 "$stage" /usr/local/bin/durable-server' <<<"$after" \
    || { echo "  the staged copy is not what is installed"; ok=1; }
  body=$(awk '/^restore_prev\(\) \{/{f=1} f{print} f&&/^}/{exit}' <<<"$code")
  g=$(grep -n -m1 -F '"$guard" /usr/local/bin/durable-server.prev' <<<"$body" | cut -d: -f1 || true)
  c=$(grep -n -m1 -F 'cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server' <<<"$body" | cut -d: -f1 || true)
  [[ -n "$g" && -n "$c" && "$g" -lt "$c" ]] \
    || { echo "  restore_prev does not guard the saved binary before restoring it"; ok=1; }
  [[ "$(grep -cF 'cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server' <<<"$code")" == 1 ]] \
    || { echo "  a restore outside restore_prev"; ok=1; }
  return "$ok"
}
check_rollback() { # file → 0 when the saved binary is guarded before anything stops or is copied
  local f="$1" g s c
  g=$(grep -n -m1 -F 'lab-build-guard.sh" /usr/local/bin/durable-server.prev' "$f" | cut -d: -f1 || true)
  s=$(grep -n -m1 -F 'systemctl stop' "$f" | cut -d: -f1 || true)
  c=$(grep -n -m1 -F 'cp -a /usr/local/bin/durable-server.prev' "$f" | cut -d: -f1 || true)
  [[ -n "$g" && -n "$s" && -n "$c" && "$g" -lt "$s" && "$g" -lt "$c" ]] \
    || { echo "  rollback does not guard the saved binary first"; return 1; }
}
echo "upgrade.sh staging + restores, rollback.sh (source rules)"
check_upgrade deploy/upgrade.sh && pass "upgrade judges and installs one staged copy; restores are guarded" || flunk "upgrade.sh rules"
check_rollback deploy/rollback.sh && pass "rollback guards the saved binary before it stops anything" || flunk "rollback.sh rule"

echo "known-bad: each rule removed"
sed 's#install -m 0755 "$stage" /usr/local/bin/durable-server#install -m 0755 "$new" /usr/local/bin/durable-server#' \
  deploy/upgrade.sh >"$work/bad-up-install"
check_upgrade "$work/bad-up-install" >/dev/null && flunk "the upgrade check passed installing the unstaged path" \
  || pass "installing the original path (the TOCTOU) fails the upgrade check"
sed 's#"$guard" /usr/local/bin/durable-server.prev#true#' deploy/upgrade.sh >"$work/bad-up-restore"
check_upgrade "$work/bad-up-restore" >/dev/null && flunk "the upgrade check passed an unguarded restore" \
  || pass "an unguarded restore fails the upgrade check"
{ cat deploy/upgrade.sh; echo 'cp -a /usr/local/bin/durable-server.prev /usr/local/bin/durable-server'; } >"$work/bad-up-inline"
check_upgrade "$work/bad-up-inline" >/dev/null && flunk "the upgrade check passed a restore outside restore_prev" \
  || pass "a restore outside restore_prev fails the upgrade check"
sed 's#"$(dirname "$0")/lab-build-guard.sh" /usr/local/bin/durable-server.prev#true#' deploy/rollback.sh >"$work/bad-rollback"
check_rollback "$work/bad-rollback" >/dev/null && flunk "the rollback check passed without the guard" \
  || pass "rollback without its guard fails the rollback check"
if [[ $EUID -eq 0 ]]; then
  echo "  skip  the known-bad script runs need a normal user (as root the copies would install)"
else
  tree lab drop
  out=$(bash "$work/tree/deploy/install.sh" 2>&1 || true)
  refused_lab "$out" && flunk "install.sh without its guard still refused" \
    || pass "install.sh without its guard lets the lab build through (to the root check)"
  out=$(bash "$work/tree/deploy/upgrade.sh" "$work/tree/target/release/durable-server" 2>&1 || true)
  refused_lab "$out" && flunk "upgrade.sh without its guard still refused" \
    || pass "upgrade.sh without its guard lets the lab build through"
  run_start lab >/dev/null
  [[ -e "$work/ran" ]] && pass "durable-run without its guard starts the lab build" \
    || flunk "durable-run without its guard still refused"
  running lab
  [[ "$(smoke_line)" == *ok*"production build"* ]] && pass "smoke without its guard accepts the lab build" \
    || flunk "smoke without its guard still failed the lab build"
fi
(( fail == 0 )) && echo "test-lab-guard: all green" || { echo "test-lab-guard: FAILED"; exit 1; }
