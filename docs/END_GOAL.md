# Durable Workflow Runner — end goal

**Accepted work survives restarts without an uncontrolled history bill.**

A background-task runner, motivated by the storage and retry problems seen in
existing applications. A job is accepted only after its record is durable on
disk; acknowledged work survives process death; finished history expires on a
schedule. One Linux host, persistent local filesystem, no RDS, no Redis.

## In scope (the first useful release)

- Submit a versioned job with an **operation key** → durable acceptance.
- Named steps executed by **allowlisted HTTP handlers** — checkpoints after each.
- Delayed starts, linear workflows, cancellation, status and history reads.
- Bounded retries: finite count, capped jitter, per-attempt deadlines,
  persisted next-run time. Attempts are **at-least-once** under one business
  idempotency key.
- Compaction and **7-day expiry of terminal history** — live inputs, step
  outputs and deduplication dependencies are preserved.
- Exclusive writer lock, consistent backups, bounded diagnostic logs.
- Rust CLI + a small SvelteKit inspection UI.

## Out of scope (later extensions, each needs an explicit contract)

- Branches/fan-out, recurring schedules, multi-host failover.
- The custom runtime (jsrt) execution adapter — optional later milestone.
- External exactly-once delivery — **never claimable**; at-least-once is the
  honest model, uncertainty is reconciled, not hidden.

## Non-negotiable invariants

1. **Commit before action.** The journal writer reports durable commit before
   acknowledgment or dispatch. In-memory index is reconstructed, never
   authoritative.
2. **Honest recovery.** Snapshot + ordered journal replay. Only a demonstrably
   incomplete tail append may be discarded; corrupt committed history stops
   startup. The last valid manifest is kept until the new generation is durable.
3. **Bounded everything.** Queues, channels, payloads, retries, logs and disk
   reserves all have limits; admission is rejected before reserve exhaustion.
4. **No transaction spans an external call.** Intent is committed to the
   journal, executed, evidenced, reconciled.

## Required failure cases

| Trigger | Required result |
|---|---|
| Crash before/after acknowledgment | Only committed acks survive; retrying a submission key returns the original run |
| Handler succeeded but reply vanished | Repeat safely only under the handler contract or reconcile; never claim external exactly-once |
| Crash during compaction | Open the old OR the new complete generation — never a mixed manifest |
| Full disk / second writer | Fail admission/readiness safely; a second local writer fails startup rather than corrupt the journal |

## Ship gate

Ship when acknowledged jobs survive the documented crash cases **and a separate
developer can run, inspect, back up and restore a noncritical workflow.**

## Evidence plan

1 KiB job inputs, three small steps, seeded schedule. Durable submit latency at
10 then 100 jobs/s with the fsync policy reported; replay time at 100,000
retained transitions; stalled handlers, full queues, compaction overlap. Load
runs are aggressive per the workspace standard; results committed with machine +
commit. **Throughput without synchronization is not a valid durability
comparison.** Laptop numbers are development observations, never production
capacity.
