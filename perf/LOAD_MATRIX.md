# Durable Runner — load matrix

The workspace rule applies: every traffic-serving feature owes a scenario;
full runs are million-scale where a traffic unit exists (submits and
journaled transitions are the units here); results land in `perf/results/`
with machine spec and producing commit; misses are kept, never rerun to
green. Laptop numbers are development observations, not production capacity.

Harness: `node perf/load.mjs <scenario> [--key=value]` — spawns the release
server + a node handler fixture, drives real HTTP, writes a result JSON, and
exits non-zero on a FAIL. `--smoke` picks the small local sizes.

Batteries: `perf/battery.sh` runs every scenario at smoke sizes (~2 min);
`perf/battery.sh --full` is the gate battery — `.github/workflows/load.yml`
runs it on hosted CI (workflow_dispatch, `ubuntu-24.04`) and uploads
`perf/results/` as an artifact (a FAIL uploads too); the coordinator
commits them through a reviewed branch.

The server under test is spawned with `--rate-limit-per-addr 100000
--max-conns-per-addr 0`: the battery measures the engine's bounds, not
the per-address limiter's (D-052 has its own tests; the rate-limit code
path still runs on every request).

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
| submit | Sustained submit rate with per-record fsync (group commit); latency + drain | 20/s × 10 s | ladder 10/25/50/100/s × 60 s | **pass** — 200/200, p99 30 ms at `f267f35` (`2026-11-18T23-56-33`); ladder re-run on CI |
| replay | Restart replay over committed records; runs recovered | 10k events | 100k events | **pass** — 1,112 runs, replay 55 ms at `f267f35` (`2026-11-18T23-56-15`) |
| stall | Deadline sweep → lost reply → reconcile probe → done, never repeated | 5 runs × 6 s stall | 15 runs × 20 s stall | **pass** — 5/5 done at `f267f35` (`2026-11-18T23-56-54`); older honest miss kept (`2026-11-08T12-44-56`) |
| admission | Over-capacity refuses honestly; never queues silently | 1,500 parallel | 3,000 parallel | **pass** — 639×200 + 861 tcp refusals at `f267f35` (`2026-11-18T23-56-58`; macOS accept-queue is the bound — CI may split differently) |
| compact | Compaction overlapping live submits; no mixed generations | 15 s | 60 s | **pass** at `f267f35` (`2026-11-18T23-57-17`), p99 11 ms |
| sustained | The million-op run: fixed submit rate for a duration | 50/s × 5,000 | 100/s × 1,000,000 | **pass** — 1,000,000/1,000,000, p50 12.5 ms p99 41.3 ms, journal 496 MiB at `cdbdfb8` (`2026-11-17T08-42-51`); smoke 5,000/5,000 p99 43 ms at `f267f35` (`2026-11-18T23-59-28`); re-runs on CI |
| compaction-cost | Compaction at 100k retained 1 KiB runs: snapshot, ack stall, restart, RSS | — | `PROBE_RUNS=100000 … cargo test --release … compaction_cost_probe` | **measured** — 103 MiB snapshot, 0.10–0.17 s, 0.46 GiB RSS (`2026-11-16T17-25-00`) |
| restart-memory | Peak RSS of a restart at 100k/800k runs, snapshot vs journal | — | `restart_probe_fill` + `restart_probe_open` under `/usr/bin/time -l` | **measured** — 135/153 MiB @100k, 1,057/1,200 MiB @800k (`2026-11-16T18-40`, `T20-10`) |
| restart-ready | Spawn → `/ready` over a journal of N queued runs — the upgrade's unavailability | — | `node perf/restart_ready.mjs` at 100k/300k/800k | **measured** at `731b322` (`2026-11-19T00-05/06`): 1,055 ms @100k (369 MiB), 3,135 ms @300k (1.1 GiB), **8,683 ms @800k (2.9 GiB) — misses the 5 s target**. ~11 ms/1k queued runs; the deploy plan quotes this per size, never hidden |

Machine constraint honored: no durable load runs while another project's
soak occupies the machine — concurrent suites contaminate evidence.

## Deferred until the evidence exists

- p99 latency at saturation vs idle — the submit ladder in `--full` gives
  the curve; declare the saturation bound only after CI measures it.
