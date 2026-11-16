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
- **D-013 — Every read and write a client can size is bounded.** `GET
  /jobs` returned the whole index in one response; operation keys and
  step/handler names had no length limit (an oversized key rides along in
  every record, idempotency key and probe URL forever); a handler reply
  was buffered in full before its size was checked. Now the index is
  ordered by id (a `BTreeMap`, whose snapshot encoding is unchanged) and
  `GET /jobs` pages it (`limit` default 100, clamped to 1000; `after`
  cursor; `next_after`). `Limits` gains `max_key_bytes` (256) and
  `max_name_bytes` (128), checked at admission before journaling — not in
  the reducer, so replay of older records is unaffected. Request bodies are
  capped at 256 KiB. A reply is refused from its `Content-Length` when it
  declares too much and otherwise read chunk by chunk only up to
  `max_output_bytes`; an oversized reply fails the attempt (retried under
  the same idempotency key, then failed) rather than being truncated.
  The load harness now counts runs from the bounded diagnostics.
- **D-014 — Crash boundaries, written down before journal batching is
  optimised** (contract §03).

  *Crash model.* Process death (kill -9, panic, OOM kill) at any
  instruction, and power loss on a local filesystem that honours fsync.
  `File::sync_data` is `fdatasync` on Linux and `fcntl(F_FULLFSYNC)` on
  macOS (Rust std, checked in the 1.97 source), so macOS numbers do not
  carry to Linux. Assumed: a *successful* fsync makes every byte written to
  that file before it durable. Not assumed: that unsynced bytes reach disk
  in order or whole, that a file's length and its data persist together,
  or that a failed fsync can be retried. Known gap (not built): directory
  entries are fsynced for manifest and snapshot renames, but not for the
  generation directory and `journal.log` created on first open.

  *The boundaries of one record.*
  1. **Before the write.** Nothing is on disk. A crash loses only the
     request; the client saw no acknowledgement and retries with the same
     `op_key`.
  2. **Written, not fsynced.** Any prefix of the bytes may survive. Replay
     keeps complete frames (valid checksum, next sequence number) and
     discards a demonstrably torn tail. A surviving complete frame was
     never acknowledged: that is at-least-once, and the retry with the same
     `op_key` returns that run. A full-length tail frame with a bad
     checksum is refused as corruption (only an incomplete tail may be
     discarded), a deliberate choice the length-before-data case can hit.
  3. **fsync returned success.** The record is committed. Only now may it
     be acknowledged, answered in a read, or acted on (handler call,
     probe).
  4. **A write or fsync failed.** A failed write is cut back to the last
     durable offset (D-009). A failed fsync is never retried: the writer
     poisons and the process exits, and startup replay decides what is
     durable.

  *The rules any batching (group commit) must keep*, tested one by one:
  (a) nothing a record decides (ack, read answer, handler call, probe)
  leaves the process before the fsync that covers it returns. Memory may
  run ahead of disk, but nothing escapes it. (b) One writer assigns
  contiguous sequence numbers in commit order, and a batch is consecutive
  frames written then fsynced once. (c) If a batch's write or fsync fails,
  every waiter in it, and every later one, fails. The fsync is not
  retried; the log is truncated to the durable offset when possible, and
  the process exits so replay rebuilds memory from disk, because memory
  already reflects records that are not durable. (d) Batches are bounded
  in records and bytes, and the writer waits a bounded time for more. A
  crash mid-batch loses only records nobody was told about. (e)
  Compaction and backup run in commit order behind earlier appends, so a
  snapshot never covers a record the log does not have.
- **D-015 — Group commit on a dedicated writer thread.** The load gate's
  root cause: every record was written and fsynced on its own, on the
  async actor's thread, so throughput was one fsync per record and the
  actor stalled for each. Now one OS thread owns the journal file. The
  actor applies each record in memory when it decides it (so later
  decisions see it) and queues it; the writer writes everything queued as
  consecutive frames with one fsync, and reports the highest commit index
  that fsync covers. Exactly one batch is in flight; the next accumulates
  behind it. Every D-014 rule holds and has its own test: acks, read
  answers, handler calls and probes are held until the covering fsync,
  so memory may run ahead of disk but nothing escapes it; sequence
  numbers stay contiguous; a failed write or fsync fails every waiter in
  the batch and after it (`CommitFailed`, HTTP 503
  `journal_unavailable`), is never retried, and stops the writer, the
  actor and the server process so replay rebuilds memory. Batches are
  bounded (`max_batch_records` 1024, `max_batch_bytes` 4 MiB, `max_wait`
  0 by default), and so is the actor's outbox (`max_outbox_records`
  16384; beyond it commands wait in the bounded command channel and are
  refused `busy`). Compaction and backup run on the writer in commit
  order; compaction expiry reaches memory only when the writer confirms.
  This changes D-009 in one respect: a rolled-back write used to let the
  writer continue. With group commit, memory already reflects the
  batch's records, so a failed write now also exits and replays. The
  free-space admission check stays per record, as in D-012. Not done
  here: the two O(N) index scans per tick (time-ordered due and deadline
  queues), and the load battery has **not** been re-run. No throughput
  claim is made until it is.
- **D-016 — Retention deletes superseded generations, on a schedule.**
  Compaction wrote a new generation but never removed the old ones, and
  ran only when someone asked, so the "7-day expiry" never reclaimed a
  byte. A workspace could not honestly be deleted while every old
  generation stayed on disk. Now, once the new generation and its
  directory entries are durable and the manifest names it, every older
  `gen-*` directory is deleted. A crash part-way leaves leftovers the next
  compaction removes; a failed delete never undoes the compaction and
  shows as `stale_generations`. The server compacts hourly by default
  (`--compact-every-secs`, 0 = off); a scheduled compaction that fails
  (e.g. `no_space`) is counted in diagnostics. The step-by-step history of
  expired runs is gone with their generation. Only the current state of
  retained runs is kept (in the snapshot), which is all `inspect` ever
  showed.
- **D-017 — A crashed compaction never blocks the next one.** Crash points
  inside `compact` (fault injection) proved the contract case "crash
  during compaction": killed after the new generation's log exists, after
  its snapshot, or after the manifest swap, a restart opens the old
  complete generation (steps 1–2) or the new one (step 3), never a mix.
  It also found a real bug. A compaction killed before its manifest swap
  left `gen-(N+1)/journal.log` behind, and every later compaction failed
  with `File exists`, because the new log is opened `create_new`. A
  `gen-(N+1)` that the manifest (still naming N) does not reference was
  never authority, so it is now removed before the new generation is
  built.
- **D-018 — Scheduled compaction is off by default.** D-016 made
  compaction hourly by default without measuring it. An independent
  review then found two things. Compaction cloned and JSON-encoded the
  whole index on the actor: at 100,000 runs with 1 KiB inputs that was a
  1.2 GB snapshot, and acks stalled for 6-7 s. And an I/O error after its
  manifest swap could lose acknowledged jobs. Hourly scheduling made both
  reachable with no operator action. `--compact-every-secs` now defaults
  to 0 (off) and `ShellConfig::compact_every` to `None`. Operators who
  schedule it opt in. The default can be revisited once compaction is
  cheap (D-021) and its error path is fatal (D-019).
- **D-019 — A compaction error from the manifest swap onward is fatal.**
  D-009/D-015 made failed appends stop the process, but compaction's
  error path was left out. Independent review reproduced the loss on the
  release binary with a real EACCES: the manifest was renamed to name
  N+1, the following directory fsync failed, and `compact` returned an
  error with the writer still pointing at N. The writer and actor treated
  that as an ordinary failed compaction, kept appending to N and
  acknowledged 30 jobs; the restart opened N+1 and all 30 were gone,
  while `/ready` stayed 200. Now everything the writer needs after the
  swap (the new log handle, created with `create_new`) is opened before
  it, so nothing can fail past the point of no return except the rename
  itself and the directory fsync. Any error from the rename onward
  poisons the journal (`ManifestSwapFailed`). The writer stops exactly
  as for a failed append: every waiter fails, the server exits non-zero,
  and restart opens whatever the manifest durably names. A compaction
  failing before the swap is still an ordinary error: nothing moved, and
  the engine carries on. Proven on a running engine at each step by fault
  injection, with a real EACCES at engine level, and at process level
  against the real binary. D-017's crash-only test could not show this,
  because dropping the journal turned the error into a crash.
- **D-020 — The leftover cleanup checks the durable manifest first.**
  D-017 removed `gen-(N+1)` before a compaction on the strength of the
  in-memory generation N. When memory and the manifest disagreed (as
  D-019's bug left them), that directory was the live generation: review
  showed a later crashed compaction losing 10 of 10 runs. `compact` now
  reads `manifest.json` from disk and removes `gen-(N+1)` only if the
  manifest names N. Otherwise it deletes nothing, poisons the writer
  (`ManifestMismatch`), and lets the restart open what the manifest
  names. D-019 makes the disagreement unreachable inside one process;
  this check stops the cleanup from assuming it.
- **D-021 — Compaction: a binary, checksummed snapshot built from a
  pointer view, not by the actor.** Review measured compaction at 100,000
  runs with 1 KiB inputs. The snapshot was 1.2 GB because inputs were
  JSON number arrays inside a payload JSON-encoded again as a number
  array. The whole index was cloned and encoded on the actor, so a submit
  waited 6-7 s, and peak RSS was about 1.4 GB. Now:
  - The index holds `Arc<Run>`, and a run is copied only when it changes
    while a view shares it (`Arc::make_mut`). Planning a compaction is a
    pointer per retained run.
  - The writer thread streams that view through an explicit binary
    encoding (`snapshot.rs`, `DRIX` v1, a bounds-checked decoder that
    refuses anything malformed) into `snapshot.bin`, framed with a magic,
    a length and a CRC-32. Its memory is a buffer, not the snapshot's
    size.
  - The snapshot is the only authority for compacted state, so a damaged
    one now stops startup. This closes the "snapshot has no checksum"
    gap. A legacy `snapshot.json` from older builds still loads, and the
    next compaction rewrites it.
  - The exact encoded size is checked against a byte budget
    (`--max-snapshot-bytes`, default 2 GiB; over it is 409
    `snapshot_over_budget`, nothing written) and the disk reserve before
    anything is written.

  Measured with the same probe, A/B on a busy laptop (load 25-79; see
  `perf/results/2026-11-16T17-25-00Z-compaction-cost.json`):

  | | before | after |
  |---|---|---|
  | snapshot | 1,104 MiB | 103 MiB |
  | compaction | 2.4-2.5 s | 0.10-0.17 s |
  | submit issued during it | ~2.0 s | 82-159 ms |
  | restart | 4.1 s | 0.06-0.08 s |
  | peak RSS | 1.8 GiB | 0.46 GiB |

  Not done: the writer still writes the snapshot inline, so appends queue
  behind it for as long as the write and fsync take, which still grows
  with the index. The next step is to write the snapshot on a helper
  thread and copy the records appended meanwhile into the new
  generation's log before the swap. Until that is built, scheduled
  compaction stays off by default (D-018).
