#!/usr/bin/env bash
# Durable-runner load battery.
#
#   perf/battery.sh                      # the smoke battery: every scenario, small sizes (~4 min)
#   perf/battery.sh --full               # the whole gate battery (~3 h)
#   perf/battery.sh --full GROUP         # one leg of the gate battery (what load.yml's matrix runs)
#
# GROUPs for --full (each is its own CI job, own artifact, own timeout):
#   sustained            the million-op sustained run
#   workspaces-memory    4 workspaces x 275k, memory backend
#   workspaces-postgres  4 workspaces x 275k, postgres backend
#   rest                 replay, submit ramps, stall, admission, compact
#   all (default)        every group, as one battery — local runs
#
# Every scenario writes a result JSON to perf/results/ (machine spec + commit
# recorded). A failing scenario exits non-zero here too; its result is kept —
# never rerun to green. Ports: PORT (server, default 53340), HPORT (handler,
# 53341); PERF_HOST_LABEL names the machine in results (never a hostname).
set -u
cd "$(dirname "$0")/.."
# The heavy slot is machine-wide: a battery is one heavy run, and a second
# heavy run anywhere on the box refuses rather than races it. Re-exec under
# heavy.sh so every leg (each a load.mjs child) inherits DR_HEAVY_HELD and
# skips its own acquire.
if [ -z "${DR_HEAVY_HELD:-}" ]; then
  exec perf/heavy.sh "$0" "$@"
fi
MODE=${1:-smoke}
GROUP=${2:-all}
PORT=${PORT:-53340}
HPORT=${HPORT:-53341}
FAILED=()
# The generator is O(1) by design (measured peak ~170 MB at 1.1 M ops) —
# cap the heap so a regression to per-op retention dies fast instead of
# burning a runner for two hours first. Override with NODE_OPTIONS.
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=512}"

run() {
  echo "== load.mjs $*"
  node perf/load.mjs "$@" --port="$PORT" --hport="$HPORT" || FAILED+=("$1")
}

full_sustained() {
  run sustained --rate=100 --total=1000000
}

full_workspaces_memory() {
  # S2: workspaces share one server at million scale (4 x 275k submits
  # plus churn, paging and signed handler calls) — memory backend.
  run workspaces --workspaces=4 --per=275000 --backend=memory
}

full_workspaces_postgres() {
  # The same million-scale run against the account store — load.yml's
  # postgres service provides PG_DATABASE_URL; without it the scenario
  # reports a visible skip (never silently measures the memory path).
  DATABASE_URL="${PG_DATABASE_URL:-}" run workspaces --workspaces=4 --per=275000 --backend=postgres
}

full_rest() {
  run replay --events=100000
  run submit --rate=10 --seconds=60
  run submit --rate=25 --seconds=60
  run submit --rate=50 --seconds=60
  run submit --rate=100 --seconds=60
  run stall --runs=15 --stall_ms=20000
  run admission --n=3000
  run compact --seconds=60
}

case "$MODE" in
  --full)
    # The million-scale gate: what the ship evidence and CI run.
    case "$GROUP" in
      sustained) full_sustained ;;
      workspaces-memory) full_workspaces_memory ;;
      workspaces-postgres) full_workspaces_postgres ;;
      rest) full_rest ;;
      all)
        full_rest
        full_sustained
        full_workspaces_memory
        full_workspaces_postgres
        ;;
      *)
        echo "unknown --full group: $GROUP (want: sustained | workspaces-memory | workspaces-postgres | rest | all)" >&2
        exit 2
        ;;
    esac
    ;;
  --smoke | smoke | *)
    # Small sizes — laptop evidence only, marked smoke:true in each result.
    run replay --smoke
    run submit --smoke
    run stall --smoke
    run admission --smoke
    run compact --smoke
    run sustained --smoke
    # S2: memory backend is always available, so the smoke always
    # exercises the workspace path instead of skipping.
    run workspaces --smoke --backend=memory
    ;;
esac

if ((${#FAILED[@]})); then
  echo "FAILED scenarios (results kept in perf/results/): ${FAILED[*]}"
  exit 1
fi
echo "battery complete"
