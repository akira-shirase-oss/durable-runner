# Durable Runner — load matrix

The workspace rule applies: every traffic-serving feature owes a scenario;
full runs are million-scale where a traffic unit exists (submits and
journaled transitions are the units here); results land in `perf/results/`
with machine spec and producing commit; misses are kept, never rerun to
green. Laptop numbers are development observations, not production capacity.

Harness: `node perf/load.mjs <scenario> [--key=value]` — spawns the release
server + a node handler fixture, drives real HTTP, writes a result JSON, and
exits non-zero on a FAIL. `--smoke` picks the small local sizes.

Batteries: `perf/battery.sh` runs every scenario at smoke sizes (~4 min);
`perf/battery.sh --full` is the gate battery — `.github/workflows/load.yml`
runs it on hosted CI (workflow_dispatch) and commits the results back.

## Declared thresholds (checked inside every result)

| Bound | Applies to | Rationale |
|---|---|---|
| every submit accepted | submit, compact, sustained | in-capacity rate: refusal is a bug, not backpressure |
| every accepted run `done` | sustained; ≥99% in submit | drain must finish |
| p99 < 2000 ms | submit, compact, sustained | pre-group-commit failed at p99 24–30 s; current ~40 ms — 50× headroom |
| every filled run recovered + replay < 30 s | replay | recovery is correctness; the time bound catches O(n²) replay |
| every stalled run reconciles `done` | stall | the vanished-reply contract |
| only 200/409/503, and the bound is hit | admission | refuse honestly, never queue silently |
| all compactions 200 | compact | no overlapping/mixed generations |

## Scenarios

| Scenario | What it proves | Smoke | Full | Result |
|---|---|---|---|---|
| submit | Sustained submit rate with per-record fsync (group commit); latency + drain | 20/s × 10 s | ladder 10/25/50/100/s × 60 s | **pass** — 200/200, p50 87 ms p99 198 ms (`2026-11-08T06-45-01`, pre-group-commit); ladder re-run on CI |
| replay | Restart replay over committed records; runs recovered | 10k events | 100k events | **pass** at 212 ms (`2026-11-08T19-03-45`, pre-group-commit); refreshed by the battery |
| stall | Deadline sweep → lost reply → reconcile probe → done, never repeated | 5 runs × 6 s stall | 15 runs × 20 s stall | **pass** — 15/15 (`2026-11-08T12-45-51`); honest miss kept (`T12-44-56`) |
| admission | Over-capacity refuses honestly; never queues silently | 1,500 parallel | 3,000 parallel | **pass** — 1,038×200 + 1,962×503 (`2026-11-08T06-44-43`) |
| compact | Compaction overlapping live submits; no mixed generations | 15 s | 60 s | **pass** — 1,435 submits / 6 compactions (`2026-11-08T12-46-28`) |
| sustained | The million-op run: fixed submit rate for a duration | 50/s × 5,000 | 100/s × 1,000,000 | **pass** — 1,000,000/1,000,000, p50 12.5 ms p99 41.3 ms, journal 496 MiB at `cdbdfb8` (`2026-11-17T08-42-51`); re-runs on CI |
| compaction-cost | Compaction at 100k retained 1 KiB runs: snapshot, ack stall, restart, RSS | — | `PROBE_RUNS=100000 … cargo test --release … compaction_cost_probe` | **measured** — 103 MiB snapshot, 0.10–0.17 s, 0.46 GiB RSS (`2026-11-16T17-25-00`) |
| restart-memory | Peak RSS of a restart at 100k/800k runs, snapshot vs journal | — | `restart_probe_fill` + `restart_probe_open` under `/usr/bin/time -l` | **measured** — 135/153 MiB @100k, 1,057/1,200 MiB @800k (`2026-11-16T18-40`, `T20-10`) |

Machine constraint honored: no durable load runs while another project's
soak occupies the machine — concurrent suites contaminate evidence.

## Deferred until the evidence exists

- p99 latency at saturation vs idle — the submit ladder in `--full` gives
  the curve; declare the saturation bound only after CI measures it.
