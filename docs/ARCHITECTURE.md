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
| `durable-journal` | Framed append-only store: frame v2 = version, length, sequence, batch length, payload checksum and a header checksum per record (D-032; v1 still read); generation manifest; torn-tail detection that never cuts more than an incomplete final frame; exclusive writer lock. |
| `durable-engine` | Execution: step claims, per-attempt deadlines, bounded retries with capped jitter, cancellation, effect reconciliation. |
| `durable-server` | Axum API + the coordinator loop (pure reducer + fake clock, bounded channels). |
| `durable-cli` | Submit/inspect/cancel/backup/restore from a shell. |
| `apps/inspect` | Small SvelteKit inspection UI (read-only views of runs, steps, journal stats). |

## Paths

**Submit:** `POST /jobs {op_key, spec: {version, steps, input_b64}}` (API key)
→ admission check (allowlist, reserve, caps) → record applied in memory and
queued → the writer thread's group commit (one write + one fsync for
everything queued) → `200 {run_id}` → dispatch. Acks, reads and handler
calls wait for the fsync that covers their record (D-014, D-015).

**Writer:** a dedicated OS thread owns `journal.log`. The actor never
writes or fsyncs; it hands the writer bounded batches, one in flight at a
time, and learns the durable commit index back. Compaction and backup run
on the same thread, in commit order. A failed write or fsync stops the
writer, the actor and the process; startup replay recovers.

**Step:** coordinator takes a concurrency permit (none free → the run stays
queued) → journals `StepDispatched` with a per-attempt deadline → allowlisted
HTTP handler → checkpoint record (which is the next step's dispatch; it
inherits the permit) → schedule next step or retry (persisted next-run +
attempt count) → finish → history becomes terminal. Dispatch is capped per
tick; a call past its deadline is cancelled locally and reconciled.

**Recovery:** read manifest → load snapshot → replay journal records in order →
discard only a provably-torn tail (the file ends inside its last frame, whose
header checks out, and nothing after it is a frame) → rebuild index → serve.
Any other damage — a header checksum, a payload checksum, a frame that does
not fit its batch — stops startup with the log untouched.

**Compaction:** the actor takes a pointer view of the retained runs (copy-on-
write) → the writer streams it as the binary index into the new generation's
checksummed `snapshot.bin` → fsync → swap the
manifest atomically → open old or new complete generation, never mixed → expire
terminal history after 7 days while preserving live state and dedup deps →
delete every superseded `gen-*` directory (retention). Runs on `POST
/admin/compact`, and on a schedule only if `--compact-every-secs` is set.

## HTTP surface

| Route | Auth | Purpose |
|---|---|---|
| `GET /health` | open | liveness |
| `GET /ready` | open | readiness: 503 `no_space` / `engine_stopped` |
| `GET /jobs?after=&limit=` | read key or API key (open only with `--open-reads` on loopback) | one page of runs, ids ascending (limit 100 default, 1000 max); `next_after` continues |
| `GET /jobs?order=desc&before=&limit=` | read key or API key (same) | newest first; `next_before` continues (the inspector's view) |
| `GET /jobs/{id}` | read key or API key (same) | inspect a run |
| `POST /jobs` | API key | submit (dedup by `op_key`) |
| `GET /jobs/{id}/spec` | API key only | the steps and input a run was submitted with, as a submit body's `spec` (D-051) |
| `POST /jobs/{id}/cancel` | API key | cancel (stops scheduling, never undoes) |
| `POST /admin/compact` | API key | compact into a new generation |
| `POST /admin/backup {name}` | API key | backup to `<backup-root>/<name>` only |
| `GET /admin/diagnostics` | API key | bounded counts |
| `/inspector/` | static files open; the UI sends its key on every call | SvelteKit UI |

The key is `DURABLE_API_KEY`; the optional `DURABLE_READ_KEY` may only read
(403 on every other route, D-037). Both are compared as SHA-256 digests in
constant time.
Request bodies are capped at 256 KiB; operation keys at 256 bytes and step
and handler names at 128 (non-empty, no control characters in keys); a
handler reply is read only up to the 256 KiB output limit.
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
perf/          submit/replay/stall/compaction scenarios, drill.py (kill -9
               drills with compactions, backups and cancels) + results
scripts/       check.sh: the gate, the same one .github/workflows/ci.yml runs
```
