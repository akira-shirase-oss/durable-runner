# Changelog

All notable changes to Durable Runner are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2026-11-20

Rollback-safety release: the independent review of the workspace work
found two ways v1.0.1 was an unsafe rollback floor, both fixed here.

### Fixed

- **Runs of any workspace ≠ 0 are parked, never executed.** This binary
  replays v3 records faithfully but its handler registry is name-keyed
  and workspace-blind — dispatching a tenant run would POST the tenant's
  input and idempotency key to workspace 0's same-named handler. Parked
  runs (queued, running past deadline, or crash-orphaned in Reconciling)
  are now excluded from dispatch, the deadline sweep, and startup's
  orphan re-probe; they surface as `parked_runs` in diagnostics.
- **format-guard checks the snapshot (DRIX) version.** v1.0.1 wrote
  DRIX 2 on every compaction, but D-056's guard compared record versions
  only — so a rollback to v1.0.0 (which reads DRIX 1) passed the guard
  and then refused startup. The guard now refuses any binary whose
  snapshot reader is older than the journal's highest DRIX version.
- **format-guard refuses non-parking binaries once ws ≠ 0 records
  exist.** A v3 journal carrying tenant runs refuses any binary that
  cannot park them — regardless of record-version compatibility.

### Changed

- `--journal-format` now prints three numbers —
  `<max record version> <max DRIX version> <parks ws ≠ 0>` —
  `3 2 1` for this release. Older binaries print one number; the guard
  maps that to the release table (record-reader 3 ⇒ DRIX 2, older ⇒
  DRIX 1, no parking).
- Diagnostics grew `journal_snapshot_version_max`, `journal_max_ws`,
  and `parked_runs` so the guard sees the journal's shape while the
  server is up; the maxima now track **live commits and live
  compactions**, not journal open alone.
- **`journal.shape` sidecar.** The engine persists the journal's shape
  (highest record version, highest DRIX version, highest workspace id)
  to `<journal>/journal.shape`, written BEFORE the record or manifest
  swap it describes and only ever upward — so after `prune_superseded`
  deletes the generations that held ws ≠ 0 records, the down-server
  guard can still see tenant runs living inside `snapshot.bin`. The
  sidecar stays OUT of backups this build writes: open rebuilds it
  from the replayed maxima, and a backup listing it would refuse
  under the v1.0.1 restore tool (its file list admits generation
  paths only).
- **Restore accepts the agreed backup set (D-057).** The file list a
  backup may carry is one contract across versions: generation files,
  plus the journal-dir flats `journal.shape` and `public_id.key` that
  newer releases write. v1.0.2 still writes generation files only,
  but it now restores all of them — an S2 backup whose journal this
  binary still reads (record ≤ 3, DRIX ≤ 2) is a legitimate rollback
  target. v1.0.2 issues no public ids; a restored `public_id.key` is
  carried forward so a later S2 boot does not orphan the ids it
  issued — and its presence means that backup no longer restores
  under v1.0.1, whose file list admits generation paths only: the
  floor for a key-holding backup becomes this release. A backup
  taken on a key-holding directory carries the key forward. Whether
  the restored journal can then run under this binary is the
  unchanged D-056 gate (record/snapshot/ws versions), never the file
  list.
- **format-guard always scans the files** and takes the max of files
  and `/admin/diagnostics` — a running server's numbers were open-time
  facts that live commits could leave behind. A DRIX ≥ 2 snapshot with
  no sidecar is ambiguous: every ws-aware writer stamps a `ws` field on
  every record since v1.0.1, so a sidecar-less journal can never prove
  a pre-workspace writer made it — the guard refuses it for every
  non-parking binary.

### Rollback note (read before downgrading)

- **v1.0.0** is not a safe floor once any compaction has run under
  v1.0.1 or later: DRIX 2 snapshots fail v1.0.0 startup (safe, but the
  rollback is the outage).
- **v1.0.1** is not a safe floor once any ws ≠ 0 data may exist —
  including parked runs living only inside a snapshot. Its
  workspace-blind registry would dispatch tenant work on workspace 0's
  handlers. `journal.shape` is the proof; an ambiguous journal
  (DRIX ≥ 2, no sidecar, ws-capable records) is refused too.
- A v1.0.1-era journal is **indistinguishable** from a ws-aware one —
  v1.0.1 already stamps `"ws":0` on every record, so no sidecar-less
  DRIX ≥ 2 journal can prove it never saw tenant data: the guard
  refuses v1.0.1 on any of them. Compaction under v1.0.1 breaks
  **v1.0.0 only**, not v1.0.1.
- Parked runs hold live-run and index capacity like any open run;
  after a rollback, enough parked tenants can admit-refuse workspace 0
  submits (`at_capacity`). The count is logged at startup and reported
  as `parked_runs`.

## [1.0.1] - 2026-11-20

S2-0: the v3 journal-record reader, shipped one release before any writer
emits v3 (D-056). No behavior change for existing users — every record is
still written at v2.

### Added

- **Record format v3 reader**: every envelope may carry `ws` (the owning
  workspace's internal id); v1/v2 records without it read as workspace 0,
  the default. A committed event whose workspace disagrees with its run's
  stops startup as corrupt history (`WorkspaceMismatch`).
- **Snapshot format v2** (`DRIX` version 2): each run carries its
  workspace; v1 snapshots still decode with `ws` 0. Independent golden
  fixtures pin both versions
  (`durable-engine/tests/fixtures/index{,-v2}.drix`).
- The in-memory index is workspace-aware: runs carry `ws`, idempotency
  dedup keys on `(workspace, op_key)`, and the legacy JSON snapshot's
  keys map into workspace 0.

### Load evidence

- `cargo test --workspace`: all existing suites green plus five new v3
  tests (replay with workspaces, v2→ws0, the v1.0.0 reader's refusal,
  torn v3 tail, workspace-mismatch startup stop) and dual-version golden
  snapshots.

## [1.0.0] - 2026-11-19

The first stable release. A background-task runner whose framed
append-only file journal is the sole authority: a submit is acknowledged
only after a durable commit, acknowledged runs survive process death,
`kill -9` and reboot, and recovery replays the journal honestly — only a
demonstrably incomplete tail is discarded, corrupt committed history
stops startup, and a second writer is refused. Execution is
at-least-once over allowlisted HTTP step handlers under one business
idempotency key, with deadline-sweep reconciliation of uncertain
effects. D0–D3 (the pure state machine, the journal, execution,
maintenance) were proven before the surface work below landed; this
section records everything since.

### Added

- **Inspector** at `/inspector` (D-053): a SvelteKit UI covering every
  API and CLI operation — submit, list, detail, spec read-back, cancel,
  diagnostics, compact, backup — with light/dark themes via semantic
  tokens, sessionStorage-only key handling, two-step admin confirms, and
  a two-row phone layout that keeps ids, keys and the cancel action
  unclipped at 390 px (asserted in `apps/inspector/capture.mjs`).
- **Per-address limits** (D-052): per-address request-rate limits
  (429 + `Retry-After`) and connection caps, keyed by
  `CF-Connecting-IP` only behind `--trusted-proxy` ranges; IPv6 keys
  use /64 with a /48 aggregate.
- **Versioned journal records** (D-054): new records carry `v: 2` and
  serialize byte fields as base64; readers accept v1 (untagged, byte
  arrays) forever and refuse unknown future versions rather than guess.
- **Rollback format guard** (D-056): `durable-server --journal-format`
  reports the highest record version the binary reads,
  `/admin/diagnostics` reports the journal's highest present version,
  and `deploy/format-guard.sh` refuses an upgrade or rollback to a
  binary that cannot read the journal — failing closed on unreadable
  state. New formats ship a reader one release before any writer emits
  them.
- **Deployment package** (D-055): `deploy/` — a hardened systemd unit,
  a Caddyfile with `lb_try_duration 15s` / `lb_try_interval 250ms`,
  Cloudflare ranges plus Authenticated Origin Pulls, `cloudflare-ips.sh`
  feeding both trust boundaries, daily R2 + age backups under a 7-day
  object lock, and install/upgrade/rollback/smoke scripts;
  `docs/DEPLOY.md` carries the restart table, the capacity-planning
  probe, the upgrade rehearsal and the restore drill.
- **Failure-case map** (`docs/FAILURE_CASES.md`): the four required
  crash cases — acknowledged-job durability, duplicate-key return,
  second-writer refusal, torn-tail recovery — each mapped to its proof.
- **Load battery**: six scenarios (submit, replay, stall, admission,
  compact, sustained) with declared thresholds, smoke sizes locally and
  million-scale full runs on CI (`.github/workflows/load.yml`,
  `perf/battery.sh`).
- Handler calls name the run, step and attempt (D-050); the API key
  reads back a run's submitted spec (D-051).
- `--version` on `durable-server` and `durable`.

### Changed

- Journal records in the v2 format are ~2.4× smaller; restart-to-ready
  over 800k queued runs went from 8.7 s to 1.7 s, and replay decode is
  ~8× cheaper (D-054).
- `perf/load.mjs` spawns the server under test with per-address limits
  lifted (`--rate-limit-per-addr 100000 --max-conns-per-addr 0`) in
  every battery mode, so load results measure the engine's bounds, not
  the D-052 limiter's.

### Fixed

- A crashed compaction no longer refuses every later compaction; a
  crash at any compaction step reopens one complete generation.
- A client that never reads cannot hold a connection (write-stall
  disconnect); a request body cut off at its deadline is 408, not 400.
- Restore fsyncs the target directory before publishing the manifest
  and re-verifies each file after opening; a backup list with a
  duplicate or missing checksum is refused by name.
- Submit input must be canonical base64, refused with one stable
  message.

### Load evidence

- Sustained million-submit run: **1,000,000/1,000,000** accepted and
  done at 100/s, p99 41 ms (12-core laptop, `cdbdfb8`,
  `perf/results/2026-11-17T08-42-51-578Z-sustained.json`).
- The CI 4-core sustained miss is kept, not rerun:
  835,397/1,000,000 accepted, then honest `503 at_capacity` refusals at
  the 100,000-live-run bound once the machine's ~85 completions/s
  ceiling was crossed; zero 429s (`c94edfd`, run 44440111562,
  `perf/results/2026-11-19T04-47-37-078Z-sustained.json`). Classified
  RUNNER capacity, not product or harness; the droplet's ceiling is
  measured at deploy per `docs/DEPLOY.md`.
