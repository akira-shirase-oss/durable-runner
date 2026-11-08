# Architecture

## Sources of truth

1. **The journal is the only authority** — a framed append-only record of every
   decision, written by exactly one writer under an exclusive lock.
2. **Checkpoints/snapshots are accelerators** — compacted state that speeds
   replay; they never override journal records.
3. **The in-memory index is rebuilt** from snapshot + journal at startup and is
   never authoritative.

## Components

| Crate | Role |
|---|---|
| `durable-domain` | Pure state machine: job/step identities, transitions, invalid-state list, limits. No I/O — enforced. |
| `durable-journal` | Framed append-only store: format version, length, sequence, checksum per record; generation manifest; torn-tail detection; exclusive writer lock. |
| `durable-engine` | Execution: step claims, per-attempt deadlines, bounded retries with capped jitter, cancellation, effect reconciliation. |
| `durable-server` | Axum API + the coordinator loop (pure reducer + fake clock, bounded channels). |
| `durable-cli` | Submit/inspect/cancel/backup/restore from a shell. |
| `apps/inspect` | Small SvelteKit inspection UI (read-only views of runs, steps, journal stats). |

## Paths

**Submit:** `POST /api/jobs {operation_key, version, input}` → admission check
(reserve + caps) → journal append → fsync → `202 {run_id}` → dispatch.

**Step:** coordinator claims due step → per-attempt deadline → allowlisted HTTP
handler → checkpoint record → schedule next step or retry (persisted
next-run + attempt count) → finish → history becomes terminal.

**Recovery:** read manifest → load snapshot → replay journal records in order →
discard only a provably-torn tail append → rebuild index → serve.

**Compaction:** write the new generation to a new directory → fsync → swap the
manifest atomically → open old or new complete generation, never mixed → expire
terminal history after 7 days while preserving live state and dedup deps.

## Failure cases

See `END_GOAL.md` — the four required cases are contract, not options:
crash around acknowledgment, vanished handler reply, crash during compaction,
full disk / second writer.

## Layout

```
crates/        durable-domain, durable-journal, durable-engine,
               durable-server, durable-cli
apps/inspect/  SvelteKit inspection UI
docs/          END_GOAL, ARCHITECTURE, ROADMAP, DECISIONS, modules/
fixtures/      torn-tail + crash-model fixtures, handler test server
perf/          submit/replay/stall/compaction scenarios + results
```
