# Durable Workflow Runner

> **Status — D0 scaffold.** Contract read, docs written, workspace laid out.
> The state model is the first build step. See `docs/ROADMAP.md`.

A background-task runner: submit versioned jobs, get durable acceptance,
recover acknowledged state after process death. A framed append-only journal
with one writer is the sole authority — no database queue, no Redis.

- `docs/END_GOAL.md` — what done means, the four required failure cases, the ship gate
- `docs/ARCHITECTURE.md` — sources of truth, submit/step/recovery/compaction paths
- `docs/ROADMAP.md` — D0 model → D1 journal → D2 execute → D3 maintain → D4 ship
- `docs/DECISIONS.md` — file journal, one writer, allowlisted HTTP, at-least-once

**What counts as done:** acknowledged jobs survive the documented crash cases
and a second developer can run, inspect, back up and restore a workflow.
