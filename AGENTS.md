# Durable Workflow Runner — agent instructions

A background-task runner in Rust. Acknowledged work survives restarts; the
journal — a framed append-only file store with one writer — is the only
authority. Contract: `../../docs/Project_Guides/04_Durable_Workflow_Runner.docx` (text
extract in `docs/`).

## Read first

1. `docs/END_GOAL.md` — what done means, scope, ship gate, the four required
   failure cases.
2. `docs/ARCHITECTURE.md` — sources of truth, paths, layout.
3. `docs/DECISIONS.md` — prescribed directions (file journal, one writer,
   allowlisted HTTP, at-least-once, terminal-only expiry).
4. `docs/ROADMAP.md` — D0–D4 step order.

## Non-negotiables

1. **Commit before action.** Journal durable commit before ack or dispatch.
   The in-memory index is rebuilt at startup, never authoritative.
2. **Honest recovery.** Only a demonstrably incomplete tail may be discarded;
   corrupt committed history stops startup. A second local writer fails
   startup, never shares the journal.
3. **At-least-once, honestly.** One business idempotency key across attempts;
   reconcile uncertain effects before repeating; never claim external
   exactly-once; cancellation cannot undo a remote action.
4. **Bounded everything.** Queues, channels, payloads, retries, logs, reserves.
   Admission is rejected before reserve exhaustion.
5. **Pure domain.** `durable-domain` carries no I/O — enforced like the other
   projects' domain crates.
6. **Evidence over claims.** `perf/` results carry machine + commit; load runs
   are million-scale where a unit exists; misses are kept, never rerun until
   green. **Throughput without synchronization is not a valid durability
   comparison.** Laptop numbers are development observations.

## Rust rules

- `export PATH="$(brew --prefix rustup)/bin:$PATH"` before every cargo command —
  Homebrew's cargo ignores the pin.
- Typed errors (`thiserror`) in libraries; `anyhow` only in binaries.
- No `unwrap`/`expect` on external input. Deadlines on every I/O await.
- No secrets or job payloads in logs.

## Testing rule (D-018, shared)

Every feature ships regression tests + a testbed/fixture path + a load
scenario, in the same change. Every fixed bug gets a test that fails without it.

## Verification

`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` — before every commit, plus the crash-case drills when
the journal/execution code changed.
