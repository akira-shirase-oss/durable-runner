# Changelog

All notable changes to Durable Runner are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

S2-1 + S2-2: the account graph and the workspace-scoped API. With
`DATABASE_URL` set, API keys minted per workspace authenticate into that
workspace and nothing else; without it the server behaves exactly as
v1.0.1 (every key lands in workspace 0). The journal remains the sole
authority for jobs — the account store is consulted at ingress only,
never inside the commit path, and already-accepted runs keep executing
through a store outage.

S2-3: the writer now emits **v3** journal envelopes (`v:3` + `ws` on
every record, including compat-mode workspace 0). v1.0.1 — shipped one
release early per D-056 — is the rollback floor: v1.0.0's reader refuses
a v3 journal cleanly, v1.0.1 replays mixed v2/v3 history and discards
only demonstrably incomplete torn tails.

S2-4: the **per-workspace handler registry**. A step's `handler` now
resolves only inside the submitting workspace — a spec naming an
unregistered or foreign-workspace handler fails admission as
`unregistered_handler` before anything is journaled.

S2-5: the **SSRF guard**. No handler call may reach a non-global
address — enforced where enforcement cannot be bypassed.

S2-6: **signed handler calls**. Every call a store-registered handler
receives carries `x-durable-timestamp` + `x-durable-signature`
(HMAC-SHA256 under the workspace's secret over
`ts.run.step.attempt.idem_key.SHA256(body)`); `POST /handler-secret`
rotates and re-arms the engine in the same request; the handler-side
contract + reference verifier ship in `docs/HANDLERS.md`.

### Added

- **`durable-accounts`**: the `AccountStore` trait over Postgres (sqlx,
  `sslmode=verify-full`-capable) and an in-memory implementation for
  tests. Schema `0001_init.sql`: workspaces (internal `u64` + external
  `public_id` UUIDv7), accounts, members, api_keys (SHA-256 digests,
  never plaintext), handler_registrations; the default workspace 0 is
  seeded by migration.
- **Keyring authentication**: bearer digests verify against the store
  with a bounded write-through cache (60 s positive / 5 s negative TTL,
  10k entries). Store-backed revocation invalidates the cache instantly;
  an unreachable store answers `503 accounts_unavailable` for uncached
  keys while cached grants keep working. `DURABLE_API_KEY` /
  `DURABLE_READ_KEY` are unchanged and always land in workspace 0.
- **Workspace-scoped API**: every `/jobs` route resolves the workspace
  from the authenticated key — list, inspect, spec and cancel see only
  that workspace's runs. A run id from another workspace answers `404`,
  identical to a missing run (no existence oracle). Idempotency dedups
  per `(workspace, op_key)` — the same key under two workspaces is two
  runs.
- **Workspace management routes** (`/admin/workspaces…`, workspace-0
  admin only): create/list/get workspaces by `public_id` (UUIDv7); mint,
  list and revoke API keys — minted `dr_…` plaintext is returned exactly
  once, at creation. `GET /whoami` reports the caller's `public_id` and
  permission.
- **`Diagnostics.workspaces_active`**: distinct workspaces holding live
  runs, as a count only — diagnostics never carries a workspace id.
- **Record format v3 writer** (S2-3): every committed record now carries
  `ws`. Journals written by this build refuse the v1.0.0 binary (reader
  to v2) and remain readable by v1.0.1 — the rollback floor D-056 kept.
  Mixed v2/v3 journals replay each run into its own workspace; a torn
  v3 tail is discarded exactly as a torn v2 tail was.
- **Neon CI workflow** (`.github/workflows/neon.yml`): the Postgres
  store's contract tests run against real Neon behind a secret; absent,
  the job skips with a visible notice rather than silently passing.
- **`GET/PUT/DELETE /handlers`** (S2-4): workspace-scoped registration
  routes backed by the account store (`handler_registrations`); each
  workspace is bounded at 256 handlers, checked before the store write.
- **Engine `(workspace, name)` registry**: registrations reach the
  engine over the same bounded command channel as submits — dispatch
  never performs a store read. Compat `--handler` flags are workspace
  0's registrations, shadowed by same-name store rows.
- **Last-known-good snapshot** (`<journal>/handlers.json`, temp-file +
  rename): loaded before serving at startup, refreshed by a 30 s
  reconciler. A store outage keeps the in-memory set dispatching and
  raises `registry_stale` in `/admin/diagnostics`; mutation and
  reconciler writes to the file are serialized.
- **SSRF guard** (`ssrf.rs`, S2-5): a `PolicyResolver` inside the
  handler client's DNS path refuses every resolution containing a
  non-global address — the check *is* the resolution, so a DNS answer
  that flips local after registration sends zero packets (tested: a
  loopback listener counts connections on a refused call). Literal
  local targets — including parser-normalized decimal/hex/mapped-v6
  forms — refuse at registration as `422 local_target_forbidden`; a
  refused call is a failed attempt, never "unknown".
  `--allow-local-handlers` is the explicit dev/fixture opt-out.
- **Signed handler calls** (S2-6): store-registered handlers carry
  `x-durable-timestamp` + `x-durable-signature` — hex HMAC-SHA256 under
  the workspace's 32-byte secret over
  `ts.run.step.attempt.idem_key.SHA256(body)`; probes sign the
  empty-digest form. The secret rides `handlers.json` so a restart
  during a store outage still signs. `--handler` flags and compat mode
  stay unsigned.
- **`POST /handler-secret`** (S2-6): draws a fresh workspace secret,
  returns it exactly once, retires the old one immediately — the
  engine's registrations are re-armed in the same request and
  in-flight old-secret calls fail honestly at the handler. Workspace
  creation (`POST /admin/workspaces`) also returns `handler_secret`
  once; no `GET`, list or diagnostics endpoint ever carries it.
- **`docs/HANDLERS.md`**: the handler contract — call, probe, the
  canonical string, verification order (freshness ±300 s → recompute →
  `(ts, idem)` replay bound) and rotation semantics. `verify_sig` in
  the server's test module is the reference check; `spawn_verifier` is
  the live fixture.

### Security

- Internal sequential workspace ids never appear in routes, response
  bodies, tokens or headers — workspaces cannot be enumerated or
  counted. A regression test scans every affected response for the u64.
- Handler-call signatures bind the run, step, attempt, idempotency key
  and exact body to the workspace's secret — a tampered body, stale
  timestamp, wrong-secret signature or replayed `(ts, idem)` fails the
  handler's own check (each rejection class regression-tested).

### Load evidence

- `cargo test --workspace`: 198 tests green including the isolation
  matrix (cross-workspace inspect/spec/cancel/list → 404), the key
  lifecycle over HTTP (mint → submit → revoke → immediate 401), the
  no-u64 leak scan, ops-scope refusal, outage semantics and
  restart-preserved workspace tags.
- `load.mjs workspaces`: every workspace registers `step` through the
  route, then 120 submits across 2 workspaces run concurrent with 1,213
  register→delete churn ops — all admitted, all drained
  (`perf/results/`).
- `ssrf.rs` overhead test: 200 real resolve+validate passes plus a
  10k-iteration `is_local` scan — the guard costs one OS lookup per new
  connection and nanoseconds per check (keep-alive calls pay nothing).

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
