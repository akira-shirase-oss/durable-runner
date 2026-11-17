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
  or that a failed fsync can be retried. Directory entries are fsynced for
  manifest and snapshot renames and, since D-025, for the generation
  directory and `journal.log` created on first open.

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

  *Power loss inside a batch.* A batch is one write of many frames and one
  fsync. If power fails before that fsync, the device may have persisted
  any subset of the batch's blocks, not just a prefix. For example frames
  k and k+2 may land while k+1 does not. Replay then finds a
  complete-looking frame with a bad checksum or the wrong sequence number
  before the tail and refuses `Corrupt`, so startup stops and needs a
  manual repair (truncate the log at the first bad frame). No
  acknowledged record is affected, because nothing in an unsynced batch
  was acknowledged. Group commit widens the window from one frame to one
  batch. Since D-032 every frame records its batch, and such a `Corrupt`
  says the damage lies inside the log's last batch; replay still refuses
  rather than guess. Process death (kill -9, OOM) cannot cause
  it: the kernel keeps every written byte.

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
  bounded (`max_batch_records` 1024, `max_batch_bytes` 4 MiB; `max_wait`
  was removed by D-026), and so is the actor's outbox (`max_outbox_records`
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
- **D-022 — Batches and the outbox are bounded in bytes, not only
  records.** D-014(d) and D-015 claimed byte-bounded batches, but the
  actor merged up to `max_batch_records` records into one message with no
  byte check, and the writer took the first message whole. Review put
  40 records of 120 KB in one write under a 16 KiB bound; at the defaults
  one batch could reach about 235 MB, and the 16,384-record outbox about
  3.8 GB. Now `take_batch` and the actor's merge stop at
  `max_batch_bytes` as well as `max_batch_records` (a single record
  larger than the bound still moves, alone). The writer's gather keeps to
  both bounds. The actor stops taking commands once its backlog reaches
  `max_outbox_bytes` (64 MiB) or `max_outbox_records`. The bounded
  command channel then refuses submits as `busy` rather than buffering.
  Diagnostics report `largest_batch_bytes`.
- **D-023 — Reads are projected and isolated from writes.** `GET /jobs`
  is open (D-011) and went through the writers' command queue. Each page
  also cloned whole runs, inputs of up to 64 KiB included, only to emit
  five fields. Review measured 16 paging clients raising submit p99 from
  24 to 541 ms. Now a page is built from `RunSummary` (id, key, state,
  step count, finished_at) inside the actor, and reads travel on their
  own bounded channel (`read_capacity`, 256). A read burst can fill it
  and be refused busy, but it can no longer take a slot a submit or
  cancel needs. Rate limiting open reads remains a requirement before any
  non-loopback exposure.
- **D-024 — Admission is bounded by live runs and index bytes.** Nothing
  bounded how many runs, or how many bytes of them, the index held.
  Inputs stay in memory at least seven days. Group commit made the index
  fill about 15 times faster, and first-step dispatch is capped by
  construction (64 per 50 ms tick, 1,280/s), so accepted submits could
  outrun dispatch indefinitely. The contract says "cap memory and disk;
  reject overload explicitly". `Limits` gains `max_live_runs` (100,000
  non-terminal runs) and `max_index_bytes` (1 GiB, an approximation:
  payloads plus a fixed per-run and per-step overhead). The core keeps
  both counts current, so the check is O(1), and refuses past them with
  `AtCapacity` (503 `at_capacity`) before journaling. Finishing a run
  frees its live-run slot (and, since D-030, its input bytes); its dedup
  record's bytes return only when a compaction expires it after seven
  days. Duplicate submits still return their
  original run. The server exposes `--max-live-runs`,
  `--max-index-bytes`, `--max-in-flight` and `--max-dispatch-per-tick`.
  Diagnostics report `active_runs` and `index_bytes`.
- **D-025 — First open makes its directory entries durable.** D-014
  named the gap: the manifest's rename was fsynced, but `gen-000001/`
  and its `journal.log`, created on first open, never had their directory
  entries fsynced. Under the power-loss model, jobs acknowledged before
  the first compaction could vanish with the entry. `Journal::open` now
  fsyncs the directory holding each entry it creates before returning:
  the root's parent (if it created the root), the root (manifest),
  `gen-000001/` (`journal.log`) and the root again (`gen-000001/`).
- **D-026 — No writer linger.** `GroupCommit::max_wait` let the writer
  wait for more records before writing, but the actor sends nothing while
  a batch is in flight (that is what makes the next batch accumulate), so
  the writer waited with nothing to collect. Review: `max_wait` 200 ms
  with 51 records ready gave 2 batches in 400 ms. It only ever added
  latency, and a test (`group_commit_wait_is_bounded`) enshrined that
  penalty. The knob and that test are removed. The writer gathers what
  is already queued, within the record and byte bounds, and writes it.
- **D-027 — A backup's name appears only when it is complete.** A
  `kill -9` during a backup left a directory with no manifest under the
  backup's name, forever: the name was taken (`409 backup_exists`), it
  used space, nothing reported it, and `restore` correctly refused it.
  Review left three such directories in 15 kills. A backup is now copied
  into a hidden `.<name>.partial` (valid backup names cannot start with
  `.`), manifest last, then fsynced and renamed into place, and the
  parent directory is fsynced. The next attempt at that name removes a
  stale partial first. The server sweeps every `.*.partial` from the
  backup root at startup.
- **D-028 — Restart streams the snapshot and the journal.** Every poison
  exit (D-009, D-015, D-019) recovers by restarting, so restart must fit
  the host. `load_snapshot` read the whole `snapshot.bin`, then copied
  its payload, then decoded the index while the copy was alive. Review
  measured 2.7 GB at 800k runs. Journal replay collected every record
  into a `Vec` before applying any of them, and with compaction off by
  default (D-018) that is the common path: 4.3 GB at 800k. Now:
  - The snapshot is decoded straight from a buffered reader with a
    running CRC. `SnapshotReader::finish` verifies the checksum before
    the decoded index is used, and a length that disagrees with the
    header is refused before reading.
  - The decoder bounds every allocation by the bytes left.
  - `Journal::open` verifies the log frame by frame with one reused
    buffer.
  - The engine replays through `records()`, one record at a time.

  Measured restart peak RSS in a separate process (load 9-28,
  `perf/results/2026-11-16T18-40-00Z-restart-memory.json`):

  | 1 KiB runs | from snapshot | from journal |
  |---|---|---|
  | 100k | 342 -> 135 MiB | 547 -> 153 MiB |
  | 800k | 2,716 -> 1,057 MiB | 4,334 -> 1,200 MiB |

  Restart memory is now the index itself, about 1.3 KB per queued 1 KiB
  run. The default index bound is sized against that in D-030.
- **D-029 — A compacted generation without its snapshot stops startup.**
  A generation above 1 whose `snapshot.bin` (and legacy `snapshot.json`)
  was missing used to open as empty state, silently: every compacted run
  vanished, run ids restarted at 1, and every op_key would re-execute. No
  crash causes this, because compaction writes and fsyncs the snapshot
  before the manifest names the generation; it takes a deletion, a
  partial copy, or filesystem damage. D-021 already treats a damaged
  snapshot as fatal, and now a missing one is too
  (`snapshot is damaged: generation N has no snapshot`): restore from a
  backup.
- **D-030 — A finished run keeps only a dedup record.** Index-byte
  capacity came back only when runs expired, which takes seven days and a
  compaction, and compaction is opt-in. Finished runs kept their full
  inputs, so with defaults about 860k 1 KiB jobs filled the bound and the
  runner refused work forever. Review reproduced it: every run done,
  `active_runs 0`, still `503 at_capacity` after a compaction. The
  contract keeps live inputs and dedup dependencies, not finished inputs.
  So when a run becomes terminal its input is dropped and its bytes leave
  the capacity count at once. It keeps key, state, step count and finish
  time, which is everything dedup and inspection use, charged
  `160 + key + Σ(48 + names)` (about 230 B for a one-step job). Replay
  does the same, and a snapshot from an older build has its finished
  inputs dropped at load. Measured at 800k finished runs: `index_bytes`
  184 MB, against 1.0 GB live, and a restart from their snapshot took
  272 MiB. The `503` message says what frees room, and OPERATING gives
  the arithmetic.
- **D-031 — The default index bound is sized for a 2 GB host.** With
  restart streamed (D-028) and finished runs reduced to dedup records
  (D-030), memory follows `index_bytes`. Measured with the restart probes
  (`perf/results/2026-11-16T20-10-00Z-restart-memory-sizing.json`):
  - Live 1 KiB runs: restart and steady state at 1.1–1.26 × the index.
  - Finished dedup records: 1.55 × (272 MiB for 184 MB at 800k).
  - Journal replay: its peak is bounded by the highest index the runtime
    held, because replay passes through the same states.

  This in-process probe saw no compaction spike, but the review saw RSS
  rise by about one snapshot during compaction on the HTTP server, cause
  not identified. The sizing rule allows for it: peak ≈
  `2.6 × max_index_bytes + 100 MiB`, which must leave 0.5 GiB of a 2 GiB
  host. So `max_index_bytes` defaults to 512 MiB, down from 1 GiB, and a
  test pins the rule. OPERATING states the formula and the throughput
  consequence: with the seven-day dedup window, a 2 GB host sustains
  about 3.9 one-step 1 KiB jobs/s with scheduled compaction. That is a
  real limit of keeping every finished key for a week on that host, not
  a measurement artefact.
- **D-032 — Frame v2: a checksummed header and a recorded batch; a
  damaged length stops startup.** The frame checksum covered only the
  payload. Replay treated a length that pointed past the end of the log
  as a torn tail, so one damaged 4-byte length in any committed frame (a
  bad sector, a stray write) dropped that record and every acknowledged
  one after it, and startup then truncated them away. Review reproduced
  it on the release binary: 25 of 50 runs served, the log cut from 10,181
  to 5,081 bytes. That broke non-negotiable 2: only a demonstrably
  incomplete tail may be discarded. Now:
  - Every new frame is v2: `version 2 | len u32 | seq u64 | batch_len u32
    | payload_crc u32 | header_crc u32 | payload` (26-byte header, 8 more
    than v1). The header checksum covers version, length, sequence,
    batch length and payload checksum. A header that fails it is never
    trusted and never cut: `Corrupt`, with the log left byte for byte.
  - `batch_len` is the byte length of the group commit (one write, one
    fsync) the frame was written in. The frames of a batch must tile it
    exactly, a batch must be complete before the next one starts, and a
    single `append` is a batch of one. So a damaged length is caught a
    second way, and a `Corrupt` inside the log's last batch says so (the
    one case a power loss before that batch's fsync explains, D-014).
    `GroupCommit` is clamped so a batch always fits the u32.
  - The only tail cut is a file that ends inside its last frame: fewer
    bytes than a header, or a header that checks out whose payload runs
    past the end, with nothing after the header that checks out as a
    frame. That cut is less than one frame, so never more than the last
    batch. Startup reports it (`durable-server: discarded a torn tail: N
    bytes at offset X`, and `discarded_tail_bytes` in diagnostics).
  - A full-length frame with a bad payload checksum is still refused, as
    D-014 chose.

  *Migration.* A v2 build reads v1 frames. An existing log keeps its v1
  frames untouched and new appends go after them as v2; a log may hold v1
  then v2, never v2 then v1 (that is `Corrupt`). The next compaction
  starts a generation that holds only v2. A v1 frame has no header
  checksum, so for v1 the length is trusted only as far as the forward
  rule allows: a v1 length that runs past the end with a later valid
  frame behind it is `Corrupt` (the reviewer's repro on a v1 log now
  stops startup), and a damaged length in a v1 log's *last* frame is
  still indistinguishable from a torn one. There is no downgrade: an
  older binary refuses a v2 frame (`version 2`) and stops, so roll back
  by restoring a backup taken before the upgrade. Frame headers start
  with a 0x00 byte and the engine's JSON payloads never contain one, so
  the forward check finds no false frames inside a payload.
- **D-033 — Stored runs are held to stored bounds when decoded.** The
  streaming snapshot decoder (D-028) bounded each count and length only by
  the bytes left. Each `StepSpec` is 48 bytes in memory and 8 or more on
  disk, so one corrupted step count could reserve about 6 times the rest
  of the file; keys and names could be as long as the file. The checksum
  is verified after decoding, so random damage reaches that code. Now
  `durable_domain::STORED` names the widest a stored run may be: 64
  steps and 64 KiB of input (admission has never allowed more, since
  D0), and 64 KiB keys and names. Keys and names get a wider bound than
  admission's 256 and 128 bytes because those bounds date only from
  D-013: a run accepted before them, or under a looser configured bound,
  must still load (`existing_over_long_key_still_deduplicates`). The
  decoder refuses any field past these before allocating for it, and a
  legacy `snapshot.json` (which has no checksum) has every run checked
  against the same bounds. `Limits::validate` refuses, at spawn,
  admission limits looser than the stored bounds, so the engine can never
  accept a run that its own snapshot would refuse.
