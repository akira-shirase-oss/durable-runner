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

**Submit:** `POST /jobs {op_key, spec: {version, steps, input_b64}}` (API key)
→ admission check (allowlist, reserve, caps) → journal append → fsync →
`200 {run_id}` → dispatch.

**Step:** coordinator takes a concurrency permit (none free → the run stays
queued) → journals `StepDispatched` with a per-attempt deadline → allowlisted
HTTP handler → checkpoint record (which is the next step's dispatch; it
inherits the permit) → schedule next step or retry (persisted next-run +
attempt count) → finish → history becomes terminal. Dispatch is capped per
tick; a call past its deadline is cancelled locally and reconciled.

**Recovery:** read manifest → load snapshot → replay journal records in order →
discard only a provably-torn tail append → rebuild index → serve.

**Compaction:** write the new generation to a new directory → fsync → swap the
manifest atomically → open old or new complete generation, never mixed → expire
terminal history after 7 days while preserving live state and dedup deps.

## HTTP surface

| Route | Auth | Purpose |
|---|---|---|
| `GET /health` | open | liveness |
| `GET /ready` | open | readiness: 503 `no_space` / `engine_stopped` |
| `GET /jobs`, `GET /jobs/{id}` | open | list / inspect runs |
| `POST /jobs` | API key | submit (dedup by `op_key`) |
| `POST /jobs/{id}/cancel` | API key | cancel (stops scheduling, never undoes) |
| `POST /admin/compact` | API key | compact into a new generation |
| `POST /admin/backup {name}` | API key | backup to `<backup-root>/<name>` only |
| `GET /admin/diagnostics` | API key | bounded counts |
| `/inspector/` | open (cancel needs the key) | SvelteKit UI |

The key is `DURABLE_API_KEY`, compared as SHA-256 digests in constant time.
Errors share one envelope: `{"error": <safe message>, "code": <stable code>}`.

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
