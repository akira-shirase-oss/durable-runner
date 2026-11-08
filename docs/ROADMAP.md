# Roadmap

Step order follows the contract: model, journal, execute, maintain — each with
its proof before the next starts.

| Step | Content | Gate | Status (2026-11-07) |
|---|---|---|---|
| **D0 — scaffold + model** | Workspace, docs, the state machine: transitions, invalid states, identities, limits; deterministic tests under a fake clock | Every invalid transition named and refused; reducer is pure and replayable | **Done** — 7 domain tests: named refusals, bounded retries, deterministic backoff |
| **D1 — journal** | Framing (version/length/seq/checksum), append + fsync, exclusive writer lock, snapshot, manifest, replay; torn-tail fixtures | The four contract failure cases proven: ack durability, duplicate-key return, second-writer refusal, torn tail | **Done** — 5 journal tests + engine-level dedup/durability tests |
| **D2 — execute** | Allowlisted HTTP handlers, step checkpoints, delayed work, bounded retries (jitter + deadlines), cancellation, effect reconciliation | Vanished-reply reconciliation proven; at-least-once with one idempotency key; cancel stops scheduling, never undoes a remote action | **Done** — actor runtime: bounded in-flight dispatch, deadline sweep → reconcile probe, `{op_key}/{step}` idempotency keys, delayed starts |
| **D3 — maintain** | Compaction into a new generation, 7-day terminal expiry preserving live state + dedup deps, consistent backup/restore, bounded diagnostics, admission reserve | Crash-during-compaction opens one complete generation; restore drill on a real directory | **Done** — live drill: kill -9 → replay recovery, compact → generation 2, backup → restore → second server recovers identical state. Found + fixed: commit-time envelopes (replay reproduced `finished_at=0`, expiring everything instantly). Bounded diagnostics deferred to load work |
| **D4 — surface + ship** | CLI polish, inspection UI, one noncritical pilot workflow, the load plan (10→100 submits/s w/ fsync policy, replay @100k transitions, stall/full/compaction cases) | The ship gate: documented crash survival + a second developer runs/inspects/backs up/restores | **In progress** — CLI + Axum server live; `GET /jobs` list + SvelteKit inspector at `/inspector` (24 tests incl. journal-safety regressions); pilot workflow, load plan, and ship gate remain |

## Later extensions (explicit contracts required)

Branches/fan-out, recurring schedules, multi-host failover, the jsrt execution
adapter. Each gets its own execution and ownership contract before work starts.
