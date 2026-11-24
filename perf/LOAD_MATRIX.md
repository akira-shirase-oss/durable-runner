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
runs it on hosted CI (workflow_dispatch, `ubuntu-24.04`) as four matrix
legs — `sustained`, `workspaces-memory`, `workspaces-postgres`, `auth`, `rest`
(`perf/battery.sh --full GROUP` runs one leg; `--full` alone runs all of
them locally) — each with its own timeout and its own `perf/results/`
artifact (a FAIL uploads too); the coordinator commits them through a
reviewed branch.

The server under test is spawned with `--rate-limit-per-addr 100000
--max-conns-per-addr 0`: the battery measures the engine's bounds, not
the per-address limiter's (D-052 has its own tests; the rate-limit code
path still runs on every request).

## Declared thresholds (checked inside every result)

| Bound | Applies to | Rationale |
|---|---|---|
| every submit accepted | submit, compact | in-capacity rate: refusal is a bug, not backpressure |
| every offered op admitted within the submit budget | sustained, workspaces | a refusal is pacing: the op retries inside the budget; landing every op is the measurement; achieved accept rate is reported |
| every accepted run `done` | sustained; ≥99% in submit, workspaces | drain must finish |
| p99 < 2000 ms per-attempt response | submit, compact, sustained, workspaces | pre-group-commit failed at p99 24–30 s; current ~40 ms — 50× headroom; bucketed-histogram upper edge; kernel-level refusals contribute no sample (they never reached the server) |
| every cached session verify 200 + forged `drs_` 401 | auth | S3-0: the dashboard's hottest route must stay on the tag check + generation-checked cache — never the store per op; a forged token still dies pre-I/O under load |
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
| sustained | The million-op run: fixed-rate OFFER; refused ops retry inside the submit budget — achieved accept rate + backpressure are the measurement | 50/s × 5,000 | 100/s × 1,000,000 | **pass** — 1,000,000/1,000,000, p50 12.5 ms p99 41.3 ms, journal 496 MiB at `cdbdfb8` (`2026-11-17T08-42-51`, this Mac); smoke 5,000/5,000 p99 43 ms at `f267f35` (`2026-11-18T23-59-28`). **CI misses kept:** `c94edfd` (run 44440111562, `2026-11-19T04-47-37`): 835,397/1,000,000 accepted, 161,535×503 `at_capacity` + 3,068 TCP connect timeouts, p99 10 s — ~85 completions/s on a 4-core CI runner vs 1M/1M at 100/s on a 12-core laptop; live backlog reached the 100k admission bound at ~6,700 s and refused honestly. `3d2c774` (run 44760152519): same machine-bound class, plus the generator itself died — see workspaces. Classified RUNNER + harness: the generator now retries refused ops inside a submit budget so a slower box is measured, not auto-failed |
| auth | S3-0: session verify at battery rate on the cached path — signup+login once, then N `GET /auth/session`; all 200, forged `drs_` dies 401 | 2,000 ops | 1,000,000 ops | prepared at `S3-0` — no runs yet (machine busy) |
| compaction-cost | Compaction at 100k retained 1 KiB runs: snapshot, ack stall, restart, RSS | — | `PROBE_RUNS=100000 … cargo test --release … compaction_cost_probe` | **measured** — 103 MiB snapshot, 0.10–0.17 s, 0.46 GiB RSS (`2026-11-16T17-25-00`) |
| restart-memory | Peak RSS of a restart at 100k/800k runs, snapshot vs journal | — | `restart_probe_fill` + `restart_probe_open` under `/usr/bin/time -l` | **measured** — 135/153 MiB @100k, 1,057/1,200 MiB @800k (`2026-11-16T18-40`, `T20-10`) |
| restart-ready | Spawn → `/ready` over a journal of N queued runs — the upgrade's unavailability | — | `node perf/restart_ready.mjs` at 100k/300k/800k | **v2 records (D-054 base64), at `f2b9822`+ (`2026-11-19T00-22/23`): 400 ms @100k (161 MiB), 599 ms @300k (467 MiB), 1,716 ms @800k (1.2 GiB) — all under 5 s; decode 857 ms at 800k**. Previous v1 records at `731b322`: 1,055/3,135/**8,683 ms** (369/1,113/2,942 MiB) — the miss that motivated D-054, kept as evidence |

Machine constraint honored: no durable load runs while another project's
soak occupies the machine — concurrent suites contaminate evidence.

## Deferred until the evidence exists

- p99 latency at saturation vs idle — the submit ladder in `--full` gives
  the curve; declare the saturation bound only after CI measures it.
