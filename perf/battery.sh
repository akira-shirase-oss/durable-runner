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
# The generator is O(1) by design (measured peak ~170 MB at 1.1 M ops) —
# cap the heap so a regression to per-op retention dies fast instead of
# burning a runner for two hours first. Override with NODE_OPTIONS.
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=512}"

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
    # S2: workspaces share one server at million scale (4 × 275k submits
    # plus churn, paging and signed handler calls) — once per account
    # backend. memory always runs; postgres runs when CI provides
    # PG_DATABASE_URL (load.yml's service), else a visible skip.
    run workspaces --workspaces=4 --per=275000 --backend=memory
    DATABASE_URL="${PG_DATABASE_URL:-}" run workspaces --workspaces=4 --per=275000 --backend=postgres
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
