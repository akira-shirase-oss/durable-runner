# Durable Workflow Runner

> **Status — v1.0.0.** D0–D4 complete: model, journal, execution and
> maintenance proven (incl. a live kill-9 → restore drill). The CLI,
> server, and an inspector at `/inspector` cover every API operation
> (submit/list/cancel/detail/spec/diagnostics/compact/backup);
> `examples/pilot/run.sh` walks the second-developer path; per-address
> rate limits and connection caps are on; restarting with 800k queued
> runs takes 1.7 s (v2 records, D-054); a single-droplet deploy package
> with Caddy + Cloudflare, R2+age backups and a restore drill lives in
> `deploy/` and `docs/DEPLOY.md` (D-055). Load: 1M/1M submits at 100/s
> on the dev machine; the CI 4-core sustained miss is kept in
> `perf/results/` (machine-bound, honest `at_capacity` refusals).
> `scripts/check.sh` is the gate CI runs. See `CHANGELOG.md`,
> `docs/ROADMAP.md` and `docs/OPERATING.md`.

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
scripts/check.sh                                 # the gate CI runs on every push
```

## License

MIT; see [LICENSE](LICENSE).
