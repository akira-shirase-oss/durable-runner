# Durable Workflow Runner

> **Status — D4 complete, ship gate rehearsed.** D0–D3 proven (model,
> journal, execution, maintenance incl. a live kill-9 → restore drill).
> D4: CLI, server, and an inspector at `/inspector` covering every API
> operation (submit/list/cancel/detail/spec/diagnostics/compact/backup);
> `examples/pilot/run.sh` walks the second-developer path; per-address
> rate limits and connection caps are on; the six-scenario smoke battery
> is green; restarting with 800k queued runs takes 1.7 s after the v2
> record format (D-054); a single-droplet deploy package with Caddy +
> Cloudflare, R2+age backups and a restore drill lives in `deploy/` and
> `docs/DEPLOY.md` (D-055). `scripts/check.sh` is the gate CI runs. See
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
