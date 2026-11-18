# The four required failure cases — where each is proven

`docs/END_GOAL.md` names four failure cases the ship gate requires. This file
maps each one to the tests that prove it and the live evidence that backs the
tests, so the gate can be audited without re-deriving the search. The crash
model they run under is written down in D-014 (process death at any
instruction; power loss on a filesystem that honours fsync).

Test references are names, not line numbers — grep the name to find the test.
Unit and engine tests run under `cargo test --workspace`; the process tests
run the shipped binary (`cargo test -p durable-server --test process_exit`);
the drills run the release build (`perf/drill.py`, `examples/pilot/run.sh`).

## 1. Crash before/after acknowledgment

> Only committed acks survive; retrying a submission key returns the original run.

The rule: a record is acknowledged only after the fsync that covers it
returns (D-014, D-015); memory may run ahead of disk but nothing escapes it.

| Proof | Where |
|---|---|
| Accepted runs survive a close + reopen | `accepted_survives_reopen`, `full_lifecycle_replays_identically` (`durable-engine/src/lib.rs`) |
| A resubmitted op_key returns its first run | `duplicate_key_returns_original_run` (`durable-engine/src/lib.rs`); `existing_over_long_key_still_deduplicates` |
| Nothing acks, answers a read, calls a handler or probes before its covering fsync | `nothing_escapes_before_the_covering_fsync` (`durable-engine/src/shell.rs`) |
| One fsync commits a whole batch, contiguous sequence | `queued_commits_share_one_fsync_with_contiguous_sequence` (`shell.rs`) |
| A failed batch write/fsync fails every waiter and is never retried | `failed_group_write_fails_the_batch_and_stops`, `failed_group_fsync_fails_every_waiter_and_is_never_retried` (`shell.rs`); journal-level `partial_write_is_rolled_back_and_history_stays_replayable`, `failed_fsync_poisons_the_writer_and_replay_stays_clean` |
| Fsync failure → process exit → replay recovers | `fsync_failure_stops_the_engine_and_restart_replays` (`shell.rs`) |
| A provably torn tail (never acknowledged) is discarded; everything before it replays | `torn_tail_is_discarded_history_is_kept`, `a_genuine_torn_tail_is_still_recovered_at_every_cut`, `a_batch_torn_mid_write_is_dropped_whole`, `every_torn_batch_reopens_after_more_appends` (`durable-journal/src/lib.rs`, D-043) |
| Corrupt committed history stops startup, log untouched | `corrupt_committed_history_stops_startup`, `corrupted_frame_length_stops_startup_and_leaves_the_log_unchanged` (journal); `damaged_frame_length_stops_startup_instead_of_dropping_runs` (engine) |
| SIGTERM drain: every acknowledged submit survives, exit 0 | `sigterm_drains_in_flight_work_and_exits_zero`, `sigterm_with_a_request_still_open_exits_non_zero` (`crates/durable-server/tests/process_exit.rs`); engine-level `shutdown_drains_what_was_decided_and_refuses_the_rest` |
| **Live evidence** | `perf/drill.py` kill -9 drill (D-042): 36 kills under load, 25,201 acknowledged submits + 9,119 cancels, **0 lost, 0 duplicated**, resubmits returned the same run — `perf/results/2026-11-17T02-28-00Z-kill9-drill.json`; `examples/pilot/run.sh` kills -9 mid-run and restores |

## 2. Handler succeeded but the reply vanished

> Repeat safely only under the handler contract or reconcile; never claim external exactly-once.

The rule: a call past its deadline is cancelled locally and reconciled — a
probe asks the handler whether the idempotency key landed before any retry
(D-004, D-050). Cancelling never undoes a remote effect.

| Proof | Where |
|---|---|
| A lost reply reconciles before any retry (domain) | `lost_reply_reconciles_before_retry` (`durable-domain/src/lib.rs`) |
| Engine: lost reply → probe (handler saw the key) → done, never repeated | `lost_reply_reconciles_and_advances` (`shell.rs`) |
| Crash mid-attempt / mid-probe → restart reconciles or re-probes | `restart_orphaned_running_run_reconciles`, `restart_orphaned_reconciling_run_reprobes`, `crash_between_dispatch_and_reply_resumes_running_then_recovers` (`shell.rs`, `lib.rs`) |
| No probe before the ReplyLost record is durable | `no_probe_before_the_reply_lost_record_is_durable` (`shell.rs`) |
| A stalled handler cannot hold a permit past its deadline | `stalled_handler_cannot_hold_capacity_past_its_deadline` (`shell.rs`) |
| The probe names run, step and the attempt it reconciles | `handler_calls_name_their_run_step_and_attempt` (`durable-server/src/main.rs`, D-050) |
| Honest failure after bounded retries (probe says "not landed") | `retries_bounded_then_failed` (domain); the kept `stall` miss `perf/results/2026-11-08T12-44-56-713Z-stall.json` |
| **Live evidence** | `node perf/load.mjs stall` — `perf/results/2026-11-08T12-45-51-376Z-stall.json`: 15/15 stalled past a 2 s deadline → reconciled → `done` |

## 3. Crash during compaction

> Open the old OR the new complete generation — never a mixed manifest.

The rule: the new generation is built and fsynced before the manifest swap;
anything after the swap that fails is fatal (D-019); a leftover generation
the manifest does not name is removed only after re-reading the manifest
from disk (D-017, D-020).

| Proof | Where |
|---|---|
| A crash at each compaction step reopens one complete generation | `crash_at_any_compaction_step_reopens_one_complete_generation` (`durable-engine/src/lib.rs`, fault injection) |
| A failed swap followed by a crash loses nothing | `failed_swap_then_crashed_compaction_loses_nothing` (`lib.rs`); `compaction_error_at_any_step_loses_no_acknowledged_job` (`shell.rs`) |
| A real EACCES after the swap stops the engine | `real_io_error_after_manifest_swap_stops_the_engine` (`shell.rs`) |
| The process exits non-zero; restart replays everything acknowledged | `failed_manifest_swap_exits_the_process_and_loses_nothing` (`tests/process_exit.rs`) |
| Leftover cleanup trusts the durable manifest, not memory | `leftover_cleanup_never_deletes_the_generation_the_manifest_names` (`durable-journal/src/lib.rs`, D-020) |
| A missing or damaged snapshot of a compacted generation stops startup | `missing_snapshot_of_a_compacted_generation_stops_startup` (engine); `damaged_snapshot_is_refused_not_trusted` (journal) |
| The compaction view cannot be changed by commits in flight | `compaction_view_is_unchanged_by_later_commits` (`lib.rs`) |
| **Live evidence** | `perf/drill.py --targeted` kills 0–30 ms after compactions (D-042 result above: 69 compactions, 0 mixed generations); the first live drill is recorded in D-007/ROADMAP D3 |

## 4. Full disk / second writer

> Fail admission/readiness safely; a second local writer fails startup rather than corrupt the journal.

The rule: every journal write is refused unless it fits with the reserve
still free, checked before anything is written (D-012); the exclusive writer
lock is held for the journal's whole life (D-002, D-034).

| Proof | Where |
|---|---|
| Appends, compaction and backup respect the reserve | `appends_compaction_and_backup_respect_the_reserve` (`durable-journal/src/lib.rs`) |
| Full disk → submit/compact/backup are 503 `no_space`, `/ready` false, both recover | `full_disk_is_503_no_space_and_readiness_false` (`durable-server/src/main.rs`, injected free-space probe) |
| A handler outcome that lands while the disk is full is counted, then reconciled | `outcome_during_full_disk_is_counted_then_reconciled` (`shell.rs`) |
| A second writer on the journal is refused | `second_writer_refused` (journal); `the_restore_lock_keeps_servers_out_and_restores_out_of_servers` (journal, D-034); `a_second_server_on_the_same_journal_refuses_startup` (`tests/process_exit.rs`) |
| **Live evidence** | `test/TEST-REPORT.md` §3: a second server on the same directory fails startup (`LOCK held`) while the first keeps serving |

## What these proofs do not claim

Honest limits, each documented in `docs/OPERATING.md` ("Known limits"):

- A power loss inside the journal's *last* batch with the file's length on
  disk stops startup (indistinguishable from damage) — restore a backup; the
  one lossy repair is written down in OPERATING.
- A damaged length in the *last* frame of a log holding only v1 frames
  (written before D-032) still reads as a torn tail and costs that one
  record. The window closes at the first v2 append.
- Process death inside a backup leaves a hidden `.<name>.partial`, swept at
  the next attempt or start (D-027) — `interrupted_backup_leaves_no_half_backup_and_the_name_free`,
  `partial_backups_are_swept_and_finished_ones_kept`.
