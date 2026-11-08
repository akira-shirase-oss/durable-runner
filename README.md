# Durable Workflow Runner

> **Status — D4 in progress.** D0–D3 proven (model, journal, execution,
> maintenance incl. a live kill-9 → restore drill). CLI, Axum server,
> `GET /jobs` list, and the SvelteKit inspector at `/inspector` are live;
> `examples/pilot/run.sh` walks the whole second-developer path. The load
> plan (`perf/LOAD_MATRIX.md`) is the remaining gate. See `docs/ROADMAP.md`.

A background-task runner: submit versioned jobs, get durable acceptance,
recover acknowledged state after process death. A framed append-only journal
with one writer is the sole authority — no database queue, no Redis.

- `docs/END_GOAL.md` — what done means, the four required failure cases, the ship gate
- `docs/ARCHITECTURE.md` — sources of truth, submit/step/recovery/compaction paths
- `docs/ROADMAP.md` — D0 model → D1 journal → D2 execute → D3 maintain → D4 ship
- `docs/DECISIONS.md` — file journal, one writer, allowlisted HTTP, at-least-once,
  admission-checked handlers, trial-reduce-before-append
- `docs/OPERATING.md` — run / inspect / cancel / backup / restore, the honest states
- `examples/pilot/` — one-command workflow walkthrough (the ship-gate rehearsal)
- `perf/` — load harness + matrix + results (machine + commit on every run)

**What counts as done:** acknowledged jobs survive the documented crash cases
and a second developer can run, inspect, back up and restore a workflow.

```sh
cargo build --release && examples/pilot/run.sh   # the whole story, one command
```
