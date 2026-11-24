#!/usr/bin/env bash
# Every protection the attack suite covers is proven by mutating it away.
#
# Each protection line carries a marker, `// ATTACK(<name>)`. For every
# marker this script copies the tree to a scratch directory, applies the
# marker's regex mutation on the marked line, rebuilds durable-server
# there, and runs the one attack test that proves it — which must then
# FAIL as a test (a build error is not a proof, and neither is a pass).
# A marker without a mutation below, or a mutation whose marker is gone,
# fails the script too: a new protection cannot land without its proof.
#
#   tests/attacks/prove-attacks.sh [--out FILE]   (FILE: a JSON record)
#
# The attacks suite itself compiles once (into target/prove); each run
# points DURABLE_SERVER_BIN at the scratch-mutated binary. The scratch
# tree is deleted at the end.
set -euo pipefail
case "${1:-}" in -h|--help) sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;; esac
cd "$(dirname "$0")/../.."
here="$PWD"
out=""
case "${1:-}" in
  "") ;;
  --out) out="${2:?--out needs a file}" ;;
  *) echo "prove-attacks.sh: unknown argument $1 (see --help)" >&2; exit 2 ;;
esac

work="$(mktemp -d "${TMPDIR:-/tmp}/durable-prove.XXXXXX")"
trap 'rm -rf "$work"' EXIT
tar --exclude ./target --exclude ./.git --exclude ./node_modules -cf - . | tar -xf - -C "$work"
export CARGO_TARGET_DIR="$here/target/prove"

# name|file|test target|test|pattern~~>replacement (a Python regex on the
# marked line; the pattern may itself contain | and =>, so it goes last
# and the in-field delimiter is ~~>)
mutations=(
  "auth|crates/durable-server/src/main.rs|auth_bypass|a_garbage_bearer_is_refused_and_nothing_is_recorded|app\.keyring\.verify\(key\)\.await~~>Verified::Granted(Auth { ws: 0, permission: Permission::Admin })"
  "tenant-scope|crates/durable-server/src/main.rs|cross_tenant|a_second_workspace_cannot_read_anothers_run|Some\(run\) if run\.ws == auth\.ws => Ok\(run\)~~>Some(run) => Ok(run)"
  "ops-scope|crates/durable-server/src/main.rs|cross_tenant|a_tenant_admin_cannot_reach_ops_routes|auth\.ws != 0 \|\| auth\.permission != Permission::Admin~~>false"
  "rate-limit|crates/durable-server/src/main.rs|rate_limit|the_bucket_refuses_after_its_tokens|limits\.rate_per_addr > 0\.0~~>false"
  "cf-header|crates/durable-server/src/addr_limits.rs|header_spoof|spoofed_client_header_does_not_evade_the_bucket|trusted\.trusts\(peer\)~~>peer == peer"
  "ssrf-register|crates/durable-server/src/main.rs|ssrf|local_handler_urls_are_refused_at_registration|, allow_local\)~~>, true)"
  "ssrf-resolve|crates/durable-server/src/ssrf.rs|ssrf|a_domain_that_resolves_local_never_gets_a_packet|addrs\.into_iter\(\)\.any\(is_local\)~~>false"
  "body-limit|crates/durable-server/src/main.rs|bodies|an_oversized_body_is_refused_413|DefaultBodyLimit::max\(MAX_REQUEST_BYTES\)~~>DefaultBodyLimit::disable()"
  "body-deadline|crates/durable-server/src/main.rs|bodies|a_dribbled_body_is_cut_at_its_deadline|limits\.body_deadline~~>Duration::from_secs(3600)"
  "head-timeout|crates/durable-server/src/main.rs|bodies|a_partial_head_is_cut|limits\.header_timeout~~>Duration::from_secs(3600)"
  "submit-dedup|crates/durable-engine/src/lib.rs|replay|a_replayed_submit_returns_the_same_run|self\.by_op\.get\(&\(ws, key\.clone\(\)\)\)~~>Option::<&RunId>::None"
  "deadline-drop|crates/durable-engine/src/shell.rs|unit:durable-engine|the_sweep_requeues_every_popped_deadline_on_commit_error|core\.requeue_deadline\(rest\)~~>let _ = &rest"
  "liveness-flood|crates/durable-server/src/main.rs|unit:durable-server|health_survives_a_pinned_connection_pool|ready_pool\\.max\\(1\\)~~>ready_pool.min(0)"
  "slot-camp|crates/durable-server/src/main.rs|unit:durable-server|a_fresh_head_gets_a_one_second_grace_then_dies_under_pressure|served_n == 0~~>false"
  "overload-lane|crates/durable-server/src/main.rs|unit:durable-server|the_overload_lane_answers_conns_from_four_buckets|lane_answer\([^)]*\)~~>{ let _ = (lane_guard, lane_permit); drop(stream); }"
  "obs-metrics-scope|crates/durable-server/src/main.rs|unit:durable-server|metrics_answer_only_the_callers_workspace|auth\.ws == 0~~>true"
  "obs-run-scope|crates/durable-server/src/main.rs|unit:durable-server|run_events_answer_only_the_callers_workspace|Some\(run\) if run\.ws == auth\.ws => run,~~>Some(run) => run,"
  "obs-feed-scope|crates/durable-server/src/main.rs|unit:durable-server|the_feed_answers_only_the_callers_workspace|app\.obs\.feed\(auth\.ws, limit\)~~>app.obs.feed(0, limit)"
  "obs-record-scope|crates/durable-engine/src/observe.rs|unit:durable-engine|a_workspace_view_holds_only_its_own_records|self\.ws\(o\.ws, o\.at\)~~>self.ws(0, o.at)"
  "parked-untrack|crates/durable-engine/src/shell.rs|unit:durable-engine|a_cancel_through_the_actor_drops_the_runs_scheduler_entries|exec!\(\)\.untrack\(w, run\);~~>let _ = w;"
  "obs-call-scope|crates/durable-engine/src/observe.rs|unit:durable-server|metrics_answer_only_the_callers_workspace|obs\.ws\(ws, wall_ms\(\)\)~~>obs.ws(0, wall_ms())"
)

# Every marker in the source has exactly one mutation, and every mutation
# a marker — the bijection is itself checked before any build.
found="$(grep -rhoE 'ATTACK\([a-z-]+\)' crates tests | sed 's/ATTACK(\(.*\))/\1/' | sort -u)"
defined="$(printf '%s\n' "${mutations[@]}" | cut -d'|' -f1 | sort -u)"
if [ "$found" != "$defined" ]; then
  echo "error: markers and mutations differ:" >&2
  echo "$found" > "$work/found.txt"; echo "$defined" > "$work/defined.txt"
  diff "$work/found.txt" "$work/defined.txt" >&2 || true
  exit 1
fi

mutate() { # file marker pattern replacement
  python3 -I - "$work/$1" "$2" "$3" "$4" <<'PY'
import re, sys
path, marker, pattern, replacement = sys.argv[1:]
lines = open(path).read().split("\n")
tag = f"ATTACK({marker})"
changed = 0
for i, line in enumerate(lines):
    if tag not in line:
        continue
    target = i if line.split("//")[0].strip() else i + 1
    new = re.sub(pattern, replacement, lines[target], count=1)
    if new != lines[target]:
        lines[target] = new
        changed += 1
if not changed:
    sys.exit(f"the mutation for {marker} changed nothing")
open(path, "w").write("\n".join(lines))
PY
}

# The suite's own tests compile once — only the attacked binary changes.
( cd "$here" && cargo build -p durable-attacks --tests --locked ) >&2

failures=0
results=()
for row in "${mutations[@]}"; do
  IFS='|' read -r name file target test subst <<<"$row"
  pattern="${subst%%~~>*}"
  replacement="${subst#*~~>}"
  cp "$here/$file" "$work/$file"
  mutate "$file" "$name" "$pattern" "$replacement"
  started=$(date +%s)
  set +e
  build_out="$(cd "$work" && cargo build -p durable-server --locked 2>&1)"
  build_status=$?
  if [ "$build_status" -ne 0 ]; then
    output="the mutated source no longer builds:\n$(tail -15 <<<"$build_out")"
    status=99
  else
    case "$target" in
      unit:*)
        # A unit test lives inside the mutated crate: the mutated code
        # IS the test binary, so it compiles and runs in the scratch
        # tree — a red result there is the proof.
        pkg="${target#unit:}"
        unit_flag="--bin $pkg"
        [ "$pkg" = "durable-engine" ] && unit_flag="--lib"
        output="$(cd "$work" && cargo test -p "$pkg" $unit_flag --locked -- "$test" 2>&1)"
        status=$?
        ;;
      *)
        # An attacks-suite test drives the mutated binary, which is the
        # scratch build DURABLE_SERVER_BIN points at.
        output="$(cd "$here" && DURABLE_SERVER_BIN="$CARGO_TARGET_DIR/debug/durable-server" \
            cargo test -p durable-attacks --locked --test "$target" -- --exact "$test" 2>&1)"
        status=$?
        ;;
    esac
  fi
  set -e
  seconds=$(( $(date +%s) - started ))
  cp "$here/$file" "$work/$file"
  if [ "$status" -ne 0 ] && grep -q 'test result: FAILED' <<<"$output"; then
    verdict=proven
  elif [ "$status" -ne 0 ]; then
    verdict="broken (did not fail as a test)"
    failures=$((failures + 1))
  else
    verdict="NOT PROVEN — the attack passed without its protection"
    failures=$((failures + 1))
  fi
  printf '%-16s %-55s %ss %s\n' "$name" "$test" "$seconds" "$verdict" >&2
  results+=("{\"name\":\"$name\",\"test\":\"$test\",\"seconds\":$seconds,\"verdict\":\"$verdict\"}")
done

if [ -n "$out" ]; then
  printf '{\n  "mutations": [\n    %s\n  ]\n}\n' "$(IFS=,; echo "${results[*]}")" > "$out"
fi
if [ "$failures" -gt 0 ]; then
  echo "prove-attacks: $failures unproven protection(s)" >&2
  exit 1
fi
echo "prove-attacks: every protection makes its attack go red" >&2
