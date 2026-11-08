# Durable Runner — load matrix

The workspace rule applies: every traffic-serving feature owes a scenario;
full runs are million-scale where a traffic unit exists (submits and
journaled transitions are the units here); results land in `perf/results/`
with machine spec and producing commit; misses are kept, never rerun to
green. Laptop numbers are development observations, not production capacity.

Harness: `node perf/load.mjs <scenario> [--key=value]` — spawns the release
server + a node handler fixture, drives real HTTP, writes a result JSON.

| Scenario | What it proves | Command | Result |
|---|---|---|---|
| submit | Sustained submit rate with per-record fsync; latency + done drain | `node perf/load.mjs submit --rate=20 --seconds=10` (validation) | **pass** — 200/200 accepted, all done, p50 87ms p99 198ms (`2026-11-08T06-45-01`) |
| admission | Over-capacity refuses honestly; never queues silently | `node perf/load.mjs admission --n=3000` | **pass** — 1,038 × 200 + 1,962 × 503 under a 3,000-submit flood (`2026-11-08T06-44-43`) |
| replay | Restart replay at ~100k committed transitions; runs recovered | `node perf/load.mjs replay --events=100000` | pending — queued behind the colophon soak |
| stall | Deadline sweep → lost reply → reconcile probe → done, never repeated | `node perf/load.mjs stall --runs=20 --stall_ms=30000` | pending |
| compact-under-load | Compaction overlapping live submits; no mixed generations | `node perf/load.mjs compact --seconds=30` | pending |
| sustained | The million-op run: 1,000,000 submits at a fixed rate | `node perf/load.mjs sustained --rate=100 --total=1000000` | pending — starts when the machine is free |

Machine constraint honored: no durable load runs while another project's
soak occupies the machine — concurrent suites contaminate evidence.

## Deferred until the evidence exists

- The 10→100 submits/s curve (submit scenario run at each rate step).
- p99 latency at saturation vs idle.
- Journal size growth over 1M records; compaction timing under load.
