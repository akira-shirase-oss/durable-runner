# Test report: Durable Workflow Runner (`durable-runner`)

**Commit tested:** `a7b0a84` (D4 close-out)
**Date:** 2026-11-18 · **Machine:** Apple M2 Pro, 12 cores, 32 GB, macOS
**Toolchain:** Rust 1.99.1 via rustup proxies · node for `perf/load.mjs` · python3 + curl + lsof for the pilot
**Guide:** `test/durable-runner-TESTING.md`
**Verdict: the durability contract holds under every required failure case and
every shipped gate is green.** The earlier red verdict — the sustained-load
latency gate failing at the per-record-fsync ceiling — was root-caused and
fixed by group commit (D-015); the 1,000,000-submit run now passes
(`perf/results/2026-11-17T08-42-51-578Z-sustained.json`). The misses are kept
below, not rerun away.

---

## 1. Test suite

`cargo test --workspace` (debug) and the same under `--locked` on CI
(`scripts/check.sh` = `.github/workflows/ci.yml`):

| Crate / file | Tests | Covers |
|---|---|---|
| `durable-domain` | 9 | state machine: named refusals, terminal states, bounded retries, reconcile-before-retry, admission limits |
| `durable-journal` | 35 | framing v1+v2, torn tails, batch integrity, corruption refusals, writer lock, reserve, compaction generations, backup/restore verification, owner-only files, golden-file formats |
| `durable-engine` (lib + shell + snapshot) | 58 | group commit, dispatch permits, reconcile probes, crash/reopen cases, compaction crash steps, capacity bounds, drain, deadline sweep |
| `durable-server` (unit) | 40 | keys and scopes, paging, error envelope, connection limits, CSP, open-reads Host check, full-disk 503s |
| `durable-server` (`tests/process_exit.rs`) | 7 | real binary: failed manifest swap, SIGTERM drain ×2, second writer, key/open-reads startup refusals, proxy env ignored |
| `durable-cli` | 3 | step parsing, input `@file`, backup-name validation |

Plus fault-injection drills inside tests (`durable-journal` dev-feature only,
kept out of the shipped binaries — an architecture check in `check.sh`
fails otherwise).

## 2. The four required failure cases

`docs/END_GOAL.md` requires four. `docs/FAILURE_CASES.md` maps each to its
tests and live evidence — the short version:

| Case | Proven by |
|---|---|
| Crash before/after ack → only committed acks survive; resubmit returns the original run | `accepted_survives_reopen`, `duplicate_key_returns_original_run`, `nothing_escapes_before_the_covering_fsync`, torn-tail + corruption refusals; kill-9 drill: 36 kills, 25,201 acks, **0 lost / 0 duplicated** (`2026-11-17T02-28-00Z-kill9-drill.json`); SIGTERM drain process tests |
| Handler reply vanished → reconcile before any repeat | `lost_reply_reconciles_before_retry` (domain), `lost_reply_reconciles_and_advances`, `restart_orphaned_*`, `no_probe_before_the_reply_lost_record_is_durable`; `stall` scenario pass (`2026-11-08T12-45-51`) |
| Crash during compaction → old OR new complete generation | `crash_at_any_compaction_step_reopens_one_complete_generation`, `compaction_error_at_any_step_loses_no_acknowledged_job`, `real_io_error_after_manifest_swap_stops_the_engine`, process-level `failed_manifest_swap_exits_the_process_and_loses_nothing`; drill kills targeted at compactions |
| Full disk / second writer | `appends_compaction_and_backup_respect_the_reserve`, `outcome_during_full_disk_is_counted_then_reconciled`, HTTP-level `full_disk_is_503_no_space_and_readiness_false`, `second_writer_refused`, `a_second_server_on_the_same_journal_refuses_startup` (binary) |

## 3. Ship-gate rehearsal

`examples/pilot/run.sh` performs `docs/OPERATING.md` end to end — submit,
inspect, `kill -9` → restart replay, compact, backup, `durable-cli restore`
on a second server with identical state, SIGTERM drain — and exits non-zero
on the first failed check. It runs on every push via `scripts/check.sh`.

## 4. Load evidence

`perf/LOAD_MATRIX.md` is the index; every result carries machine + commit.
The full million-scale battery re-runs on CI (`.github/workflows/load.yml`);
small local smokes are the laptop evidence.

| Scenario | Latest evidence | Verdict |
|---|---|---|
| sustained 1M @100/s | `2026-11-17T08-42-51-578Z` at `cdbdfb8`: 1,000,000/1,000,000 accepted + drained, p50 12.5 ms, p99 41.3 ms, journal 496 MiB | **pass** |
| kill-9 drill | `2026-11-17T02-28-00Z`: 0 lost across 36 kills + compactions + backups + cancel bursts | **pass** |
| connection drills | `2026-11-17T03-20` idle-head sockets, `2026-11-17T03-45` non-reading clients | **pass** |
| replay @100k events | earlier pass at 212 ms (`2026-11-08T19-03-45`); re-measured in the smoke battery | pending refresh |
| submit / admission / stall / compact | `2026-11-08` runs on the pre-group-commit code | pass, re-measured in the smoke battery |

Laptop numbers are development observations (load averages are recorded in
each result); they are not production capacity.

## 5. History — kept, not rerun to green

- **2026-11-08, `c36f5c3`, contended with the colophon soak:** sustained 1M
  accepted 619,480 / refused 380,520 (bounded 503s, nothing lost), p99 24.4 s.
- **2026-11-10, `e193b20`, solo re-run:** worse, not better — 481,914 accepted,
  p50 7.5 ms / p99 29.9 s. Proved the ceiling was the journal's
  per-record-fsync path, not contention. → fixed by group commit (D-015,
  writer thread, one fsync per batch); the solo run at `cdbdfb8` above is the
  green counterpart.
- **Bug found by an earlier pass (`464e908`):** restart-orphaned runs sat
  `running` forever — the deadline sweep only reconciled attempts this
  process had fired. Fixed with the failing-first regression tests
  `restart_orphaned_running_run_reconciles` /
  `restart_orphaned_reconciling_run_reprobes`.
- The first `stall` run (`2026-11-08T12-44-56`) honestly `failed` ×15: the
  fixture recorded idem keys only at reply time, so probes correctly said
  "not landed" and bounded retries exhausted — evidence of the
  honest-refusal path, kept.
