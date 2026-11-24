#!/usr/bin/env bash
# Every protection the attack suite covers is proven by mutating it away.
#
# Each protection line carries a marker, `// ATTACK(<name>)`. For every
# marker this script copies the tree to a scratch directory, applies the
# marker's regex mutation on the marked line, rebuilds durable-server
# there, and runs the one test that proves it — which must then FAIL, and
# fail AT ITS DECISIVE ASSERTION: the line right after a
# `// DECISIVE(<name>)` marker in the test (re-review #10). A build error
# is not a proof; neither is a pass; neither is a failure anywhere else
# (a setup panic, a helper's unwrap) — that is INCONCLUSIVE and fails the
# script. A marker without a mutation, a mutation without its marker, or
# a row without exactly one DECISIVE marker fails the script before any
# build: a new protection cannot land without its proof.
#
#   tests/attacks/prove-attacks.sh [--out FILE]   (FILE: a JSON record)
#   tests/attacks/prove-attacks.sh --discover     (print where each
#                                                  mutant's test fails)
#   PROVE_ONLY=row,row tests/attacks/prove-attacks.sh   (chosen rows)
#
# Unit rows (`unit:<crate>`) name the test by its full path and run it
# with --exact. The attacks suite itself compiles once (into
# target/prove); each run points DURABLE_SERVER_BIN at the scratch-mutated
# binary. The scratch tree is deleted at the end.
set -euo pipefail
case "${1:-}" in -h|--help) sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;; esac
cd "$(dirname "$0")/../.."
here="$PWD"
out=""
discover=0
case "${1:-}" in
  "") ;;
  --out) out="${2:?--out needs a file}" ;;
  --discover) discover=1 ;;
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
  "auth|crates/durable-server/src/main.rs|auth_bypass|a_garbage_bearer_is_refused_and_nothing_is_recorded|verify_key\(app, parts, key\)\.await~~>Verified::Granted(Principal::OpsKey)"
  "tenant-scope|crates/durable-server/src/main.rs|cross_tenant|a_second_workspace_cannot_read_anothers_run|Some\(run\) if run\.ws == ws => Ok\(run\)~~>Some(run) => Ok(run)"
  "ops-scope|crates/durable-server/src/main.rs|cross_tenant|a_tenant_admin_cannot_reach_ops_routes|!matches!\(auth, Principal::OpsKey\)~~>false"
  "rate-limit|crates/durable-server/src/main.rs|rate_limit|the_bucket_refuses_after_its_tokens|limits\.rate_per_addr > 0\.0~~>false"
  "cf-header|crates/durable-server/src/addr_limits.rs|header_spoof|spoofed_client_header_does_not_evade_the_bucket|trusted\.trusts\(peer\)~~>peer == peer"
  "ssrf-register|crates/durable-server/src/main.rs|ssrf|local_handler_urls_are_refused_at_registration|ssrf::check_url\(&parsed, policy\)~~>Ok::<(), &str>(())"
  "ssrf-resolve|crates/durable-server/src/ssrf.rs|ssrf|a_domain_that_resolves_local_never_gets_a_packet|addrs\.into_iter\(\)\.any\(\|ip\| policy\.refusal\(ip\)\.is_some\(\)\)~~>false"
  "body-limit|crates/durable-server/src/main.rs|bodies|an_oversized_body_is_refused_413|DefaultBodyLimit::max\(MAX_REQUEST_BYTES\)~~>DefaultBodyLimit::disable()"
  "body-deadline|crates/durable-server/src/main.rs|bodies|a_dribbled_body_is_cut_at_its_deadline|limits\.body_deadline~~>Duration::from_secs(3600)"
  "head-timeout|crates/durable-server/src/main.rs|bodies|a_partial_head_is_cut|limits\.header_timeout~~>Duration::from_secs(3600)"
  "submit-dedup|crates/durable-engine/src/lib.rs|replay|a_replayed_submit_returns_the_same_run|self\.by_op\.get\(&\(ws, key\.clone\(\)\)\)~~>Option::<&RunId>::None"
  "deadline-drop|crates/durable-engine/src/shell.rs|unit:durable-engine|shell::tests::the_sweep_requeues_every_popped_deadline_on_commit_error|core\.requeue_deadline\(rest\)~~>let _ = &rest"
  "liveness-flood|crates/durable-server/src/main.rs|unit:durable-server|tests::health_survives_a_pinned_connection_pool|ready_pool\\.max\\(1\\)~~>ready_pool.min(0)"
  "slot-camp|crates/durable-server/src/main.rs|unit:durable-server|tests::a_fresh_head_gets_a_one_second_grace_then_dies_under_pressure|served_n == 0~~>false"
  "overload-lane|crates/durable-server/src/main.rs|unit:durable-server|tests::the_overload_lane_answers_conns_from_four_buckets|lane_answer\([^)]*\)~~>{ let _ = (lane_guard, lane_permit); drop(stream); }"
  "obs-metrics-scope|crates/durable-server/src/main.rs|unit:durable-server|tests::metrics_answer_only_the_callers_workspace|ws == 0~~>true"
  "obs-run-scope|crates/durable-server/src/main.rs|unit:durable-server|tests::run_events_answer_only_the_callers_workspace|Some\(run\) if run\.ws == auth\.ws\(\) => run,~~>Some(run) => run,"
  "obs-feed-scope|crates/durable-server/src/main.rs|unit:durable-server|tests::the_feed_answers_only_the_callers_workspace|app\.obs\.feed\(auth\.ws\(\), limit,~~>app.obs.feed(0, limit,"
  "obs-record-scope|crates/durable-engine/src/observe.rs|unit:durable-engine|observe::tests::a_workspace_view_holds_only_its_own_records|self\.ws\(o\.ws, o\.at\)~~>self.ws(0, o.at)"
  "parked-untrack|crates/durable-engine/src/shell.rs|unit:durable-engine|shell::tests::a_cancel_through_the_actor_drops_the_runs_scheduler_entries|exec!\(\)\.untrack\(w, run\);~~>let _ = w;"
  "ssrf-own-address|crates/durable-server/src/ssrf.rs|unit:durable-server|ssrf::tests::the_hosts_own_addresses_are_refused_by_both_layers|self\.host\.iter\(\)\.any\(\|&own\| same_address\(ip, own\)\)~~>false"
  "ssrf-deny-cidr|crates/durable-server/src/ssrf.rs|unit:durable-server|tests::a_denied_or_own_address_gets_no_packet|self\.deny\.iter\(\)\.any\(\|&block\| in_block\(ip, block\)\)~~>false"
  "peer-canonical|crates/durable-server/src/main.rs|unit:durable-server|tests::visitors_behind_the_loopback_proxy_hop_get_their_own_buckets|peer\.ip\(\)\.to_canonical\(\)~~>peer.ip()"
  "obs-call-scope|crates/durable-engine/src/observe.rs|unit:durable-server|tests::metrics_answer_only_the_callers_workspace|Some\(_\) => self\.ws\(ws, now\)~~>Some(_) => self.ws(0, now)"
  "tenant-strict-ssrf|crates/durable-server/src/main.rs|ssrf|the_operators_local_opt_out_never_reaches_a_tenant|lab_local_tenant_handlers\(&args\)~~>args.allow_local_handlers"
  "tenant-register-policy|crates/durable-server/src/main.rs|ssrf|the_operators_local_opt_out_never_reaches_a_tenant|&app\.handler_factory\.store_policy~~>&app.handler_factory.flag_policy"
  "signed-store-client|crates/durable-server/src/main.rs|unit:durable-server|tests::a_signed_handler_uses_the_store_pair_not_the_flag_pair|&self\.client,~~>&self.flag_client,"
  "signed-store-policy|crates/durable-server/src/main.rs|unit:durable-server|tests::a_signed_handler_uses_the_store_pair_not_the_flag_pair|&self\.store_policy,~~>&self.flag_policy,"
  "call-time-literal|crates/durable-server/src/main.rs|unit:durable-server|tests::a_literal_local_url_is_refused_at_call_time|ssrf::check_url\(u, &self\.policy\)\.is_ok\(\)~~>true"
  "reconcile-recheck|crates/durable-server/src/main.rs|unit:durable-server|tests::reconcile_never_arms_a_planted_local_row|let ok = r\.url == BUILTIN_ECHO~~>let ok = true || r.url == BUILTIN_ECHO"
  "startup-recheck|crates/durable-server/src/main.rs|unit:durable-server|tests::the_startup_snapshot_never_arms_a_planted_local_row|let ok = reqwest::Url::parse\(&entry\.url\)~~>let ok = true || reqwest::Url::parse(&entry.url)"
  "numeric-id-ws0|crates/durable-server/src/main.rs|cross_tenant|a_tenant_cannot_address_its_own_run_by_its_internal_number|if ws == 0 && ~~>if true && "
  "keys-scope|crates/durable-engine/src/shell.rs|unit:durable-engine|shell::tests::keys_never_answer_another_workspaces_run|\.filter\(\|r\| r\.ws == ws\)~~>"
  "list-scope|crates/durable-server/src/main.rs|unit:durable-server|tests::workspaces_are_isolated_end_to_end|\.list_in\(auth\.ws\(\),~~>.list_in(0,"
  "diag-scope|crates/durable-server/src/main.rs|unit:durable-server|tests::a_tenant_admin_key_never_reaches_the_ops_routes|flat_access\(&app, &parts, Permission::Write, Role::Viewer, false\)\.await~~>Ok::<u64, ApiError>(0)"
  "v6-agg-first|crates/durable-server/src/addr_limits.rs|unit:durable-server|addr_limits::tests::a_slash48_spray_leaves_room_for_new_clients|self\.wait\(LimitKey::V6Aggregate\(p\), now\)~~>Duration::ZERO"
  "v6-per-agg-cap|crates/durable-server/src/addr_limits.rs|unit:durable-server|addr_limits::tests::a_slash48_holds_at_most_its_cap_of_slash64_entries|if at_cap\(self\) \{~~>if false {"
  "v6-agg-idle-reclaim|crates/durable-server/src/addr_limits.rs|unit:durable-server|addr_limits::tests::a_slash48_cap_frees_once_its_slash64s_go_idle|self\.reclaim_agg\(p, now\);~~>let _ = (p, now);"
  "limiter-idle-clock|crates/durable-server/src/addr_limits.rs|unit:durable-server|addr_limits::tests::refusals_do_not_refresh_idleness_and_the_scan_is_rate_limited|b\.active > i~~>b.updated > i"
  "obs-scan-resistant|crates/durable-engine/src/observe.rs|unit:durable-engine|observe::tests::tracked_workspaces_are_an_lru_with_workspace_0_pinned|\.min_by_key~~>.max_by_key"
  "registry-batch|crates/durable-server/src/main.rs|unit:durable-server|tests::the_registry_writer_saves_once_per_batch|batch\.len\(\) < REGISTRY_BATCH~~>batch.len() < 1"
  "registry-noop-skip|crates/durable-server/src/main.rs|unit:durable-server|tests::a_noop_put_does_no_store_epoch_or_disk_work|if app\.registry~~>if false && app.registry"
  "registry-set-after-save|crates/durable-server/src/main.rs|unit:durable-server|tests::a_put_whose_save_failed_is_retried_not_skipped|if saved\.is_ok\(\) \{~~>if true {"
  "registry-view-invalidate|crates/durable-server/src/main.rs|unit:durable-server|tests::a_failed_put_never_leaves_the_old_url_claiming_armed|app\.registry\.remove\(auth\.ws\(\), name\);~~>let _ = name;"
  "registry-delete-forgets|crates/durable-server/src/main.rs|unit:durable-server|tests::a_delete_whose_save_failed_lets_the_same_put_rearm|app\.registry\.remove\(auth\.ws\(\), &name\);~~>let _ = &name;"
  "rotate-set-after-save|crates/durable-server/src/main.rs|unit:durable-server|tests::a_rotate_whose_save_failed_lets_a_put_save_the_new_secret|if saved\.is_ok\(\) \{~~>if true {"
  "reconcile-forgets|crates/durable-server/src/main.rs|unit:durable-server|tests::a_reconcile_pass_that_did_not_land_forgets_its_views|for ws in &touched \{~~>for ws in &touched[..0] {"
  "registry-rate|crates/durable-server/src/main.rs|unit:durable-server|tests::registry_mutations_are_rate_limited_per_workspace|app\.registry\.admit\(ws, Instant::now\(\)\)~~>Ok::<(), Duration>(())"
  "deny-cidr-mapped|crates/durable-server/src/ssrf.rs|unit:durable-server|ssrf::tests::an_ipv4_mapped_deny_cidr_names_its_ipv4_range|v6\.to_ipv4_mapped\(\)~~>None::<Ipv4Addr>"
  "session-forge|crates/durable-server/src/main.rs|unit:durable-server|tests::s30_session_forge_dies_before_the_store|!bool::from\(subtle::ConstantTimeEq::ct_eq~~>bool::from(subtle::ConstantTimeEq::ct_eq"
  "csrf|crates/durable-server/src/main.rs|unit:durable-server|tests::s30_signup_login_session_round_trip|csrf_token\(seed\)~~>csrf_token(&[0; 32])"
  "csrf-open-post|crates/durable-server/src/main.rs|unit:durable-server|tests::s30_csrf_open_post|site != \"same-origin\" && site != \"none\"~~>false"
  "cookie-attrs|crates/durable-server/src/main.rs|unit:durable-server|tests::s30_cookie_attributes|session_set_cookie\(&token\)~~>String::from(\"x\")"
  "token-replay|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s31_verify_token_replay_410|!t\.consumed~~>true"
  "token-kind-confusion|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s31_reset_token_cannot_verify|t\.kind == \"verify\"~~>true"
  "prereg-takeover|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s31_verify_revokes_prereg_session|inner\.sessions\.values_mut\(\)~~>inner.sessions.values_mut().take(0)"
  "addr-spec-injection|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s31_addr_spec_refused|!crate::mail::valid_addr_spec\(&mail\.send_to\)~~>false"
  "mail-budget|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s31_outbox_hourly_cap|in_window >= crate::mail::MAX_HOURLY_PER_ADDR~~>in_window >= i64::MAX"
  "enumeration|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_forgot_resend_envelopes_match|let _ = issued;~~>if !issued { return Ok(no_store(StatusCode::OK.into_response())); }"
  "invite-required|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_invite_only_gate|!ok~~>false"
  "verify-sets-cookie|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_verify_round_trip|TokenCookie::Never~~>TokenCookie::UnlessForeignSession"
  "lockout-shape|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_lockout_cools_off|app\.lockouts\.fail\(&email, prefix, now\)~~>false"
  "mail-global|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_mail_global_budget_stops_drain|!app\.mail_budget\.open\(unix_ms\(\)\)~~>false"
  "session-fixation|crates/durable-server/src/main.rs|unit:durable-server|tests::s32_login_never_adopts_a_planted_cookie|mint_session_token\(&app\.session_key\)~~>session_cookie(&parts).unwrap_or_default()"
  "cooloff|crates/durable-server/src/main.rs|unit:durable-server|tests::s31_lockout_cools_off|app\.lockouts\.check\(&email, prefix, now\)~~>None::<u64>"
  "idle-timeout|crates/durable-server/src/main.rs|unit:durable-server|tests::s32_idle_slides_then_times_out|grant\.expires_at_ms <= now \|\| idle <= now~~>false"
  "session-idor|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s32_revoke_one_session|s\.account == account~~>true"
  "reset-token-replay|crates/durable-accounts/src/memory.rs|unit:durable-server|tests::s32_reset_kills_cached_sessions_and_replay_dies|!t\.consumed~~>true"
)

# Every marker in the source has exactly one mutation, and every mutation
# a marker — the bijection is itself checked before any build.
found="$(grep -rhoE 'ATTACK\([a-z0-9-]+\)' crates tests | sed 's/ATTACK(\(.*\))/\1/' | sort -u)"
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

# Every row names its decisive assertion: exactly one `// DECISIVE(<name>)`
# marker, on the line right above the assertion whose failure is the proof.
decisive_for() { # name → "file:line" of the asserted line
  local hits n f l
  hits="$(grep -rn --include='*.rs' "DECISIVE($1)" crates tests || true)"
  n="$(grep -c . <<<"$hits" || true)"
  [ "$n" = 1 ] || return 1
  f="${hits%%:*}"
  l="${hits#*:}"
  l="${l%%:*}"
  [[ "$l" =~ ^[0-9]+$ ]] || return 1
  echo "$f:$((l + 1))"
}
if [ "$discover" -eq 0 ]; then
  missing=""
  for row in "${mutations[@]}"; do
    name="${row%%|*}"
    decisive_for "$name" >/dev/null || missing="$missing $name"
  done
  if [ -n "$missing" ]; then
    echo "error: rows without exactly one DECISIVE marker:$missing" >&2
    exit 1
  fi
fi

# Pre-flight, before ANY build: apply every row's substitution to the real
# source and require it to change the marked line. A stale pattern or a
# marker one line off its guard is a NO-OP that otherwise costs a whole
# scratch rebuild per row to discover (CI run 45481961653). Same targeting
# rule as mutate(): an inline marker mutates its own line, a standalone
# `// ATTACK(<name>)` comment mutates the line below it.
cat > "$work/selfcheck.py" <<'PY'
import re, sys
bad = []
for row in sys.stdin:
    row = row.rstrip("\n")
    if not row:
        continue
    name, file, _target, _test, subst = row.split("|", 4)
    pattern, replacement = subst.split("~~>", 1)
    lines = open(file).read().split("\n")
    tag = f"ATTACK({name})"
    hits = [i for i, l in enumerate(lines) if tag in l]
    changed = any(
        re.sub(pattern, replacement, lines[t], count=1) != lines[t]
        for i in hits
        for t in [i if lines[i].split("//")[0].strip() else i + 1]
    )
    if not hits:
        bad.append(f"{name}: no ATTACK marker found")
    elif not changed:
        bad.append(f"{name}: mutation changes nothing on its target line(s)")
if bad:
    for b in bad:
        print(f"prove-attacks self-check: {b}", file=sys.stderr)
    sys.exit(1)
PY
printf '%s\n' "${mutations[@]}" | python3 -I "$work/selfcheck.py" || {
  echo "prove-attacks: a mutation row is a no-op — fix the pattern or the marker" >&2
  exit 1
}

# The suite's own tests compile once — only the attacked binary changes.
( cd "$here" && cargo build -p durable-attacks --tests --locked ) >&2

failures=0
results=()
ran=0
for row in "${mutations[@]}"; do
  IFS='|' read -r name file target test subst <<<"$row"
  # PROVE_ONLY=name,name re-proves chosen rows (a CI run proves all).
  if [ -n "${PROVE_ONLY:-}" ] && [[ ",$PROVE_ONLY," != *",$name,"* ]]; then
    continue
  fi
  ran=$((ran + 1))
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
        # tree — named by its full path, run alone (--exact).
        pkg="${target#unit:}"
        unit_flag="--bin $pkg"
        [ "$pkg" = "durable-engine" ] && unit_flag="--lib"
        output="$(cd "$work" && cargo test -p "$pkg" $unit_flag --locked -- --exact "$test" 2>&1)"
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
  # Where the TARGET test's own thread panicked (not a helper task's).
  esc_test="$(printf '%s' "$test" | sed 's/[][\.*^$/]/\\&/g')"
  at="$(grep -oE "thread '${esc_test}'( \([0-9]+\))? panicked at [^ ]+:[0-9]+:[0-9]+" <<<"$output" \
        | head -1 | sed -E 's/.* panicked at //; s/:[0-9]+$//' || true)"
  failed=0
  if [ "$status" -ne 0 ] && grep -qE "^test ${esc_test} \.\.\. FAILED" <<<"$output"; then
    failed=1
  fi
  if [ "$failed" -eq 1 ] && [ "$discover" -eq 1 ]; then
    verdict="failed at ${at:-?}"
  elif [ "$failed" -eq 1 ]; then
    want="$(decisive_for "$name")"
    if [ "$at" = "$want" ]; then
      verdict=proven
    else
      verdict="INCONCLUSIVE — failed at ${at:-?}, not its decisive assertion $want"
      failures=$((failures + 1))
    fi
  elif [ "$status" -ne 0 ]; then
    verdict="broken (the target test did not fail as a test)"
    failures=$((failures + 1))
  else
    verdict="NOT PROVEN — the attack passed without its protection"
    failures=$((failures + 1))
  fi
  printf '%-22s %-60s %ss %s\n' "$name" "$test" "$seconds" "$verdict" >&2
  results+=("{\"name\":\"$name\",\"test\":\"$test\",\"seconds\":$seconds,\"verdict\":\"$verdict\"}")
done

if [ "$ran" -eq 0 ]; then
  echo "prove-attacks: no row ran (PROVE_ONLY=${PROVE_ONLY:-} names none)" >&2
  exit 1
fi
if [ -n "$out" ]; then
  printf '{\n  "mutations": [\n    %s\n  ]\n}\n' "$(IFS=,; echo "${results[*]}")" > "$out"
fi
if [ "$failures" -gt 0 ]; then
  echo "prove-attacks: $failures unproven protection(s)" >&2
  exit 1
fi
if [ "$discover" -eq 1 ]; then
  echo "prove-attacks: discovery only — nothing is proven until every row has its DECISIVE marker" >&2
  exit 0
fi
echo "prove-attacks: every protection makes its attack go red at its decisive assertion" >&2
