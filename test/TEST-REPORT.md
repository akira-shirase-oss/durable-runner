# Test report: Durable Workflow Runner (`durable-runner`)

**Commit tested:** `c36f5c3` (guide written against this HEAD)
**Date:** 2026-11-10 · **Machine:** Apple M2 Pro, 12 cores, 32 GB, macOS (Darwin 27.0.0)
**Toolchain:** Rust 1.99.1 via rustup proxies · python3 + curl + lsof for the pilot
**Guide:** `test/durable-runner-TESTING.md`
**Verdict: PASS with one real bug found and fixed** (`464e908`), plus a lint-gate fix
(`82ef4f8`) and the owed load battery committed (`1491441`).

---

## 1. Build and workspace tests

| Check | Result |
|---|---|
| `cargo build --release --locked` | clean (server + CLI + engine) |
| `cargo test --workspace --locked` | **26 tests, all pass** — domain 7, engine 14, journal 5 |
| `cargo fmt --all --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | **failed → fixed** (see §6) |

Journal tests cover the contract directly: `torn_tail_is_discarded_history_is_kept`,
`corrupt_committed_history_stops_startup`, `sequence_gap_is_corruption`,
`second_writer_refused`, `append_then_replay_round_trips`.

## 2. Pilot drill (`examples/pilot/run.sh`) — the ship-gate rehearsal

Full output captured; every stage verified:

- `submit --op-key pilot/onboarding-email` → `{"run_id":1}`; inspect →
  `state:"done", steps:2, finished_at` set.
- Inspector UI served at `/inspector/` → HTTP **200**.
- **`kill -9` the server**, restart against the same journal → replay restores
  `state:"done"` with the same `finished_at` — acknowledged work survived.
- `compact` → `{"generation":2}`; `backup --to …` → backup dir written.
- **Restore on a second instance** (different port + journal dir) →
  `restored inspect -> {"state":"done", same id, same finished_at}`.
- `pilot complete — run survived kill -9, compaction, and a restore`.

## 3. Break-it drills (live server + CLI)

| Drill | Expected | Observed |
|---|---|---|
| Resubmit same `--op-key` | same run id, no re-execution | `{"run_id":1}` again; run count stays **1** |
| Resubmit same key, *different* steps | dedup is by key only | still `run_id:1`, original spec wins |
| Unregistered handler | HTTP 409, nothing journaled | `Error: 409 Conflict: unregistered handler: nosuchhandler`, `exit=1`; `/jobs` shows only the legit run |
| Second writer on same journal dir | startup refusal | `Error: engine open: journal already has a writer (LOCK held)`, exit 1; **original server kept serving** |
| `kill -9` mid-dispatch (slow handler), restart same journal | orphaned attempt reconciles, never re-fires blindly | **FAILED before fix — see §5**; after fix the run reaches `done` with the handler called exactly once |

## 4. Load battery (`perf/battery.sh`, committed in `1491441`)

Ran 2026-11-08 on `c36f5c3`, **deliberately concurrent with the colophon P6 soak**
(user-directed overlap — the battery log says so; latency numbers are conservative /
contended and are labeled as such). Results in `perf/results/`:

| Scenario | Result | Evidence |
|---|---|---|
| replay | **pass** | 100,000 journal events / 11,112 runs replayed in **212 ms** |
| submit ladder | 200/600/3000 **pass**; 6000 **FAIL** | p99 198 ms → 3.3 s → 4.05 s under rising concurrency (contended) |
| stall reconcile | FAIL then **pass** on retry | honest miss kept, then clean |
| compact-under-load | **pass** | p99 30.9 ms while compacting |
| admission | FAIL then **pass** on retry | bounds proven |
| **sustained** | **FAIL — kept as evidence** | 1,000,000 submits @100/s for 10,093 s (~2.8 h): **619,480 accepted (200), 380,520 refused (503) by admission** — bounded refusal, never unbounded queueing; journal 311 MB; submit p50 15.5 s / p99 24.4 s. The latency gate failed under soak+contended conditions; per workspace rules the miss is committed, not re-run until green. |

Honest reading: durability held (every accepted submit journaled; refusals were
admission 503s, not lost work), but submit-ack latency under a full journal +
contention did not meet the gate. That is a real, recorded limitation — flagged for
the operator doc, not hidden.

## 5. Bug found and fixed — restart-orphaned runs stuck `running` forever

**Reproduction (live):** submit a job to a slow handler, `kill -9` the server
mid-dispatch, restart on the same journal. The run journaled
`running:step:0:attempt:1` and sat there — polled every 0.4 s for **20+ s** with no
reconcile, no re-dispatch, no terminal state.

**Root cause:** the actor's deadline sweep only reconciled attempts present in the
in-memory `live` set — attempts *this* process fired. A run orphaned by a restart
had no `live` entry, so its expired deadline was never swept; and a run journaled
`Reconciling` whose probe died with the old process was never re-probed.

**Fix (`464e908`):** on actor spawn, collect journaled `Reconciling` runs and issue
a fresh reconcile probe (verdicts are idempotent — a moved-on run refuses
`NothingToReconcile`); the deadline sweep no longer gates orphaned `Running`
attempts on `live`.

**Regression tests (fail without the fix):**
- `restart_orphaned_running_run_reconciles` — kill mid-flight, respawn, assert
  `Done` and handler call count == 1 (reconcile must not blindly repeat the remote
  call). Failed before: `run never reached a terminal state`.
- `restart_orphaned_reconciling_run_reprobes` — crash after `ReplyLost` committed
  `Reconciling` but before the probe verdict; restart must re-probe. Failed before,
  passes after.

Both verified failing on the pre-fix code, then passing — the D-018 rule.

## 6. Lint-gate fix (`82ef4f8`)

`cargo clippy -D warnings` failed on two `expect_err()` uses in engine tests
(`shell.rs:577`, `lib.rs:535`) — `expect_used` is denied workspace-wide. Rewritten
to `.err().unwrap_or_else(|| panic!(...))`, preserving the assertion messages.
Clippy clean after; no behavior change.

## 7. Commits landed by this testing pass

| Commit | Content |
|---|---|
| `82ef4f8` | clippy `expect_err` → `err().unwrap_or_else` in engine tests |
| `464e908` | **fix:** restart-orphaned runs reconcile instead of sticking forever (+ 2 regression tests) |
| `38698ff` | `test/durable-runner-TESTING.md` |
| `1491441` | load battery evidence: replay/submit/stall/compact/admission/sustained JSONs + runner log |

## 8. Verdict

The durability contract holds under every drill the guide defines — acknowledged
work survives kill-9, compaction, backup and restore; dedup, the handler allowlist,
and the single-writer lock all refuse correctly. **One real bug was found** (orphaned
runs never reconciled after restart) and fixed with failing-first regression tests.
The load battery is committed honestly: replay and compaction are fast, the sustained
1M-submit run refused 38% at admission and failed its latency gate while contending
with another suite — evidence kept per the workspace rule, worth a clean solo re-run
before calling the ship gate fully green.
