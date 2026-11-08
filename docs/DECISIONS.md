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
