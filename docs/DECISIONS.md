# Decisions

Deliberate choices, newest last. D-NNN numbering continues the workspace
convention; these seed the contract's prescribed directions.

- **D-001 — File journal, not a database queue.** A framed append-only journal
  with sequence numbers, checksums and generation manifests is the authority.
  The guide explicitly preserves this over substituting a database queue.
- **D-002 — One writer, exclusive lock.** A single coordinator serializes all
  decisions; a second local writer fails startup rather than risk corruption.
- **D-003 — Allowlisted HTTP handlers first.** Execution goes through named,
  declared HTTP handlers. The jsrt adapter is an optional later milestone.
- **D-004 — At-least-once + one business idempotency key.** Never claim
  external exactly-once; reconcile uncertain effects before repeating.
- **D-005 — Seven-day expiry applies to terminal history.** Live inputs, step
  outputs and deduplication dependencies are preserved regardless of age.
- **D-006 — In-memory index is rebuilt, never authoritative.** Corrupt
  committed history stops startup; only a provably-torn tail may be discarded.
- **D-007 — Journaled envelopes carry the commit clock.** The first live
  restore drill (2026-11-07) caught replay reproducing `finished_at=0`:
  every replayed terminal run read as finished at the epoch and expired
  instantly under compaction. `Envelope { run, at, event }` stamps the
  commit time so replay reproduces real deadlines and expiry. Found by the
  drill, not by tests — the tests now cover it.
- **D-008 — Admission is where the allowlist bites.** A step naming a handler
  outside the registry is a spec error refused at submit
  (`UnregisteredHandler`, HTTP 409), never journaled. A journaled run whose
  handler leaves the registry later stays queued until registry repair or
  cancellation — there is nothing to journal because nothing happened.
  Found via the inspector's first smoke test (2026-11-08): a StepFailed
  committed for a never-dispatched run reduced to StaleReply *after* the
  record was appended — a committed event that could not apply — and
  `apply_committed`'s remove-then-reduce then dropped the run from the
  index. `commit` now trial-reduces before appending so refusals stay out
  of the journal, and `apply_committed` uses get/get_mut so a refused event
  can never lose the run.
- **D-009 — A failed write rolls back; a failed fsync poisons the writer.**
  Before this, a failed `write_all` or `sync_data` returned early but left
  whatever bytes reached the file, and the next append reused the same
  sequence number behind them: the next startup refused the journal as
  `Corrupt`. Now the writer tracks the durable length (bytes covered by a
  successful fsync). A failed write truncates back to it and fsyncs the
  truncation; the record does not exist and its sequence number is free.
  A failed fsync is **never retried** — after one, the kernel may have
  dropped the dirty pages, and a second fsync can succeed for data that is
  gone — so the writer poisons: every later append, compaction and backup
  refuses, the engine actor stops, and `durable-server` exits non-zero so
  a supervisor restarts it and startup replay decides what is durable. A
  failed rollback also poisons. Proven with fault injection (the journal's
  `fault-injection` feature, enabled only by dev-dependencies).
- **D-010 — A dispatch is journaled only while holding a permit.** Before
  this, the tick journaled `StepDispatched` for every due run and then
  tried to take one of the `max_in_flight` permits; without one it
  silently skipped the call ("the next tick retries" was false). The run
  sat `Running` until its deadline, reconciled "not found", and burned an
  attempt. Now the tick takes the permit first and stops dispatching when
  none is free (the run stays queued: nothing happened, nothing is
  recorded). A finished attempt or probe hands its permit to the chained
  step its checkpoint journaled, so no other run can take that capacity in
  between. Reconcile probes are handler calls and share the same permits
  (a probe without one waits in a queue, oldest first). The deadline sweep
  cancels the local call it declares lost, so a stalled handler cannot hold
  a permit forever (cancelling never undoes a remote effect; the probe
  decides). Dispatches and expiries are capped per tick
  (`max_dispatch_per_tick`, default 64), longest-waiting first.
- **D-011 — An API key on every mutating and admin route; backups only
  under a configured root.** No route was authenticated, and
  `/admin/backup` wrote to whatever path the client sent. Now the server
  refuses to start without `DURABLE_API_KEY` (at least 32 characters, read
  from the environment, never argv) and keeps only its SHA-256 digest;
  every mutating and admin handler takes an `Authorized` extractor that
  compares digests in constant time (`subtle`), before the body is parsed.
  Reads stay open on purpose for the loopback pilot and the read-only
  inspector; the SaaS plan must gate them per tenant before exposure.
  Backups take a plain name and land only in `<--backup-root>/<name>`
  (no separators, no leading `.`, at most 64 characters); without a root
  backups are disabled, and an existing name is refused rather than merged
  into (`create_dir` is the atomic check). The pilot generates a random
  key per run and now restores with `durable-cli restore` into a separate
  directory before starting the second server.
- **D-012 — A full disk is a 503 and a not-ready server, never a 500 or
  a silent drop.** `NoSpace` used to surface as a 500, `/health` was
  always ok, compaction and backup ignored the reserve, and the runtime
  discarded commit errors with `unwrap_or_default()` (a handler outcome
  that could not be journaled simply vanished). Now: the reserve is
  configurable (`--reserve-bytes`) and every journal write — append,
  compaction snapshot, backup copy — refuses with `NoSpace` unless what it
  writes fits with the reserve still free, checked before anything is
  written. Compaction is all-or-nothing (expiry is applied to memory only
  after the new generation is durable). The server maps `NoSpace` (and a
  write that hit ENOSPC) to `503 {"code":"no_space"}`; `GET /ready` is
  false while space is low or the engine has stopped. The tick does no
  journal work below the reserve, so no handler is called whose outcome
  could not be recorded; errors with no caller are counted by kind in
  diagnostics and logged on transitions. One threshold for every write
  keeps the rule explainable; an emergency floor below the reserve for
  in-flight work is a possible refinement, not built. A refused submit no
  longer consumes a run id.
