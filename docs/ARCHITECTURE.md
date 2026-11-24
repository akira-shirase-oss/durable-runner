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
| `durable-engine` | Execution: step claims, per-attempt deadlines, bounded retries with capped jitter, cancellation, effect reconciliation. `observe`: the bounded, derived observability layer (D-058). |
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
| `POST /jobs` | API key | submit (dedup by `(workspace, op_key)`) |
| `GET /jobs/{id}/spec` | API key only | the steps and input a run was submitted with, as a submit body's `spec` (D-051) |
| `POST /jobs/{id}/cancel` | API key | cancel (stops scheduling, never undoes) |
| `POST /admin/compact` | API key | compact into a new generation |
| `POST /admin/backup {name}` | API key | backup to `<backup-root>/<name>` only |
| `GET /admin/diagnostics` | API key | bounded counts (incl. `workspaces_active`, a count only) |
| `GET /whoami` | any key | the caller's workspace `public_id` + permission |
| `GET /admin/workspaces` | ops key (workspace-0 admin) | bounded workspace list (S2) |
| `POST /admin/workspaces {name}` | ops key | create a workspace; returns its `public_id` (UUIDv7) |
| `GET /admin/workspaces/{public_id}` | ops key | one workspace by public id |
| `GET /admin/workspaces/{public_id}/keys` | ops key | that workspace's keys (digest metadata only) |
| `POST /admin/workspaces/{public_id}/keys {name, permission}` | ops key | mint a key; the `key` plaintext is returned only here |
| `DELETE /admin/workspaces/{public_id}/keys/{key_id}` | ops key | revoke; cached grants are invalidated at once |
| `GET /handlers` | workspace key | that workspace's handler registry (name → url) |
| `PUT /handlers/{name} {url}` | workspace admin key | register or update a handler, ≤256 per workspace |
| `DELETE /handlers/{name}` | workspace admin key | remove a registration |
| `GET /metrics?window=15m\|1h\|6h\|24h` | any key | metrics ring for the window, totals, state counts, per-handler stats (D-058); a tenant key gets its own workspace's, a workspace-0 key the global view with workspace 0's handlers (D-060) |
| `GET /jobs/{id}/events` | any key | the caller's run's timeline: commit-order events, `complete`/`reason` (D-058); another workspace's run is 404, like `GET /jobs/{id}` (D-060) |
| `GET /events?limit=` | any key | the caller's workspace's activity feed, newest first (1-200, default 50; D-058, D-060) |
| `/inspector/` | static files open; the UI sends its key on every call | SvelteKit UI |

The key is `DURABLE_API_KEY`; the optional `DURABLE_READ_KEY` may only read
(403 on every other route, D-037). Both are compared as SHA-256 digests in
constant time.

**Workspaces (S2).** With `DATABASE_URL` set, keys minted through
`POST /admin/workspaces/{public_id}/keys` authenticate into their
workspace: every `/jobs` route sees only that workspace's runs, and a
run id from another workspace answers `404` — identical to a missing
run, so a foreign id leaks nothing ("no oracle"). The ops routes are
reachable only with a workspace-0 admin key (`DURABLE_API_KEY` counts);
without `DATABASE_URL` they answer `404` with the reason in the error
message. Workspace management routes take the workspace's `public_id`
(a UUIDv7): the internal sequential id never appears in a route,
response body or header, and a regression test scans for it.

**Handler registry (S2-4).** A step's `handler` resolves only inside the
submitting workspace: registrations live in the account store, delivered
to the engine's `(workspace, name)` registry through the bounded command
channel, so dispatch never waits on the store. A spec naming a handler
the workspace has not registered fails admission as
`unregistered_handler` — before anything is journaled — and a foreign
workspace's registrations are invisible. Startup loads the
last-known-good snapshot (`<journal>/handlers.json`) before serving, and
a 30 s reconciler refreshes the registry from store truth; a store
outage leaves the in-memory set serving and raises `registry_stale` in
`/admin/diagnostics`. Compat-mode `--handler` flags are workspace 0's
registrations; a store row shadows a flag on a name conflict. The bound
is 256 handlers per workspace, checked before the store write.

**SSRF guard (S2-5).** No handler call may reach a non-global address.
Registration refuses literal local targets (`local_target_forbidden`);
domains are judged at call time inside the handler client's DNS
resolver — every address a name returns must pass the non-global check
before reqwest is told it exists, so a DNS answer that flips local
between registration and dispatch still sends zero packets (no
validate-then-re-resolve gap; one local address refuses the whole
resolution). A refused call is a definitive failed attempt, never
"unknown". `--allow-local-handlers` opts both layers out for
development and fixtures. The guard adds one OS lookup plus a CIDR
scan per new connection; keep-alive calls pay nothing (measured in
`ssrf.rs` tests).

**Signed handler calls (S2-6).** Every call a store-registered handler
receives carries `x-durable-timestamp` and `x-durable-signature`: hex
HMAC-SHA256 under the workspace's 32-byte secret over the newline-joined
`ts\nmethod\nauthority\npath\nrun\nstep\nattempt\nidem_key\nSHA256(body)`
(`authority` is the handler's configured authority, `run` the opaque public id;
probe: empty digest). The
secret is drawn at workspace creation, shown once there and once at
`POST /handler-secret` rotation, stored retrievably in the account
store (a handler needs it to verify), never in `GET` responses or logs.
Rotation re-arms the engine's whole registry for that workspace in the
same request — old-secret signatures die immediately and in-flight
calls fail honestly at the handler. The secret also rides the
last-known-good `handlers.json` snapshot (0600, beside the journal), so
a restart during a store outage still signs. Compat mode and `--handler`
flags stay unsigned. The handler-side contract is `docs/HANDLERS.md`;
`verify_sig` in the server's test module is the reference check.
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
docs/          END_GOAL, ARCHITECTURE, ROADMAP, DECISIONS, OPERATING,
               SAAS_PLAN, FAILURE_CASES
fixtures/      torn-tail + crash-model fixtures, handler test server
perf/          submit/replay/stall/compaction scenarios, drill.py (kill -9
               drills with compactions, backups and cancels) + results
scripts/       check.sh: the gate, the same one .github/workflows/ci.yml runs
```
