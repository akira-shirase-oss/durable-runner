#!/usr/bin/env bash
# Durable-runner load battery — runs the latency-measured scenarios after
# the colophon P6 soak (k6 PID given as $1) exits. One suite at a time.
set -u
cd "$(dirname "$0")/.."
K6_PID=${1:?usage: battery.sh <k6-pid-to-wait-for>}
LOG=perf/results/battery-runner.log

echo "battery armed: waiting for k6 pid $K6_PID" >> "$LOG"
while kill -0 "$K6_PID" 2>/dev/null; do sleep 60; done
echo "$(date -u +%FT%TZ) soak finished — machine free; battery starting" >> "$LOG"

# Let the soak's own result-writing flush before we touch the machine.
sleep 30

node perf/load.mjs replay --events=100000      >> "$LOG" 2>&1
node perf/load.mjs submit --rate=10 --seconds=60  >> "$LOG" 2>&1
node perf/load.mjs submit --rate=50 --seconds=60  >> "$LOG" 2>&1
node perf/load.mjs submit --rate=100 --seconds=60 >> "$LOG" 2>&1
node perf/load.mjs sustained --rate=100 --total=1000000 >> "$LOG" 2>&1

echo "$(date -u +%FT%TZ) battery complete" >> "$LOG"
