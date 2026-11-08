# Roadmap

Step order follows the contract: model, journal, execute, maintain — each with
its proof before the next starts.

| Step | Content | Gate |
|---|---|---|
| **D0 — scaffold + model** | Workspace, docs, the state machine: transitions, invalid states, identities, limits; deterministic tests under a fake clock | Every invalid transition named and refused; reducer is pure and replayable |
| **D1 — journal** | Framing (version/length/seq/checksum), append + fsync, exclusive writer lock, snapshot, manifest, replay; torn-tail fixtures | The four contract failure cases proven: ack durability, duplicate-key return, second-writer refusal, torn tail |
| **D2 — execute** | Allowlisted HTTP handlers, step checkpoints, delayed work, bounded retries (jitter + deadlines), cancellation, effect reconciliation | Vanished-reply reconciliation proven; at-least-once with one idempotency key; cancel stops scheduling, never undoes a remote action |
| **D3 — maintain** | Compaction into a new generation, 7-day terminal expiry preserving live state + dedup deps, consistent backup/restore, bounded diagnostics, admission reserve | Crash-during-compaction opens one complete generation; restore drill on a real directory |
| **D4 — surface + ship** | CLI polish, inspection UI, one noncritical pilot workflow, the load plan (10→100 submits/s w/ fsync policy, replay @100k transitions, stall/full/compaction cases) | The ship gate: documented crash survival + a second developer runs/inspects/backs up/restores |

## Later extensions (explicit contracts required)

Branches/fan-out, recurring schedules, multi-host failover, the jsrt execution
adapter. Each gets its own execution and ownership contract before work starts.
