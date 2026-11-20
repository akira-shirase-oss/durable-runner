# Changelog

All notable changes to Durable Runner are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
