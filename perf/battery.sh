#!/usr/bin/env bash
# Durable-runner load battery.
#
#   perf/battery.sh            # the smoke battery: every scenario, small sizes (~4 min)
#   perf/battery.sh --full     # the full battery .github/workflows/load.yml runs (~3 h)
#
# Every scenario writes a result JSON to perf/results/ (machine spec + commit
# recorded). A failing scenario exits non-zero here too; its result is kept —
# never rerun to green. Ports: PORT (server, default 53340), HPORT (handler,
# 53341); PERF_HOST_LABEL names the machine in results (never a hostname).
set -u
cd "$(dirname "$0")/.."
MODE=${1:-smoke}
PORT=${PORT:-53340}
HPORT=${HPORT:-53341}
FAILED=()

run() {
  echo "== load.mjs $*"
  node perf/load.mjs "$@" --port="$PORT" --hport="$HPORT" || FAILED+=("$1")
}

case "$MODE" in
  --full)
    # The million-scale gate: what the ship evidence and CI run.
    run replay --events=100000
    run submit --rate=10 --seconds=60
    run submit --rate=25 --seconds=60
    run submit --rate=50 --seconds=60
    run submit --rate=100 --seconds=60
    run stall --runs=15 --stall_ms=20000
    run admission --n=3000
    run compact --seconds=60
    run sustained --rate=100 --total=1000000
    # S2: workspaces share one server — needs DATABASE_URL, else a visible skip.
    run workspaces --workspaces=4 --per=25000
    ;;
  --smoke | smoke | *)
    # Small sizes — laptop evidence only, marked smoke:true in each result.
    run replay --smoke
    run submit --smoke
    run stall --smoke
    run admission --smoke
    run compact --smoke
    run sustained --smoke
    # S2: skips visibly without DATABASE_URL (no account graph to load).
    run workspaces --smoke
    ;;
esac

if ((${#FAILED[@]})); then
  echo "FAILED scenarios (results kept in perf/results/): ${FAILED[*]}"
  exit 1
fi
echo "battery complete"
