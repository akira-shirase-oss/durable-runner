# S2 — accounts, workspaces, handler registry (plan)

Phase S2 of `docs/SAAS_PLAN.md`: API keys scoped to workspaces, a per-workspace
HTTP handler registry behind an SSRF guard, signed handler calls. S1 (droplet)
is blocked on the maintainer; S3 (dashboard, signup/login) consumes this API
and is **not** built here.

**Status (2026-11-20): approved by the coordinator** with three decisions
folded in below — store-trait testing, opaque external workspace ids, and
S2-0 tagging its own v1.0.1 release. The hold lifted 2026-11-19
(maintainer: complete every in-development project). Landed on
`devin/dr-s2`: **S2-0** (`e528686` + `e6951b5`, tagged v1.0.1 by the
coordinator), **S2-1** (account store, keyring + cache, scoped submit,
visible-skip Neon job), **S2-2** (scoped `/jobs`, workspace management
routes by `public_id`, key lifecycle, no-u64-leak test). Backend only: no
design or UI work for Durable Runner until the maintainer designs it.

Exit gate (unchanged): **workspace A can never see, cancel or run handlers for
workspace B; SSRF tests refuse private targets.** Plus the standing rule: every
slice ships its failing-first test, its failure case, and its load scenario in
the same change.

Invariants this plan must not break:

- The journal stays the only authority for jobs. The account store is
  consulted at API ingress and handler dispatch only — never inside the
  commit path.
- Accepted runs keep executing if the account store is unreachable.
- The engine's dispatch path never performs a network read (no Neon, no DNS
  wait on the actor loop — resolution happens inside the handler's own
  bounded HTTP call, as today).

---

## D-057 — Account records in Neon Postgres; the journal remains sole job authority

*(proposed; lands in DECISIONS.md with the implementing change)*

SAAS_PLAN already directs Neon. This entry records the weigh-and-pick, because
the alternative — a local SQLite account store, the choice Service Monitor
made for its data (its D-003) — is genuinely competitive and deserves an honest
answer.

**What the store holds** (none of it job authority): accounts, workspaces
(internal `u64` PK plus an opaque `public_id` — see *Workspace identity*),
members, API keys (hashed, per the existing `sha256` + constant-time compare
in `ApiKey`), handler registrations, plan limits, the workspace signing
secret.

**SQLite would buy:** zero external dependency, zero network latency even on
cache miss, single-file backup inside the existing R2+age tar, and no new
failure mode at all — when the box is down the store is down with it, which is
honest.

**SQLite costs:** the account graph dies on the same disk as the journal. A
droplet loss (not a crash — a region/host loss) then means restoring the
journal backup onto a workspace graph that must be rebuilt from nothing;
with Neon the graph survives independently and the restored journal
reattaches by workspace id. The account store is also the piece that will
outlive any single droplet (members, keys, plan state — and eventually
billing-adjacent records), and the family standardizes account records on
Neon; SQLite would buy back latency only until the product needs a second
process or host.

**Picked: Neon Postgres** (sqlx, `sslmode=verify-full`, bounded pool ~5
connections, a deadline on every query). One external dependency with a
defined degraded mode; no new process on the 2 GB droplet.

### Failure modes, stated honestly

| Situation | Effect |
|---|---|
| Neon unreachable | Sign-in/sign-up/workspace admin: down. Submit with an **uncached** key: `503 accounts_unavailable` — fail closed, never silently. Submit with a **cached verified** key: proceeds within TTL. **Accepted runs: unaffected** — dispatch consults only the in-memory registry and the journal. |
| Droplet/disk loss | Journal restores from its own backup (D-046) and reattaches to workspace ids; the Neon graph survives independently. |
| Both lost | Worst case: journaled runs still complete anywhere the journal is restored, but their workspace rows must be re-registered — documented, not hidden. |

### Submit-path latency — the verified-key cache

A Neon round-trip on every submit is unacceptable (10–50 ms×admission is the
whole budget at 100/s). Design:

- Cache `sha256(bearer) → {workspace, permission, verified_at}` in the server
  process. Verified-hit lookup is a memory read — the hot path never waits on
  Neon.
- TTL **60 s**, with **write-through invalidation**: the single API process
  makes every key change itself, so create/revoke edit the cache immediately —
  revocation is effective at once; the TTL only bounds changes made outside
  the API (a direct DB edit, a second future writer).
- Negative cache (unknown/revoked bearer → refusal) with a short **5 s** TTL
  so a Neon outage is not amplified by a miss flood; a bound on total entries
  (~10k, LRU) keeps the cache itself inside "bounded everything".
- Neon-down behavior is tested, not assumed: cached keys keep submitting,
  uncached keys get a clean 503, and the moment Neon returns the cache
  refreshes — all three asserted in the slice's failure case.

### Backups and restore

- Neon point-in-time recovery is the primary; an optional `pg_dump` into the
  existing daily R2+age tar is cheap (account data is KB–MB) and is listed as
  a deploy task, not a promise.
- Restore order: redeploy → point `DATABASE_URL` at the restored Neon → the
  journal is untouched and reattaches by workspace id. The journal never
  waits on Neon at startup except to warm the handler registry (S2-4 covers
  the outage-at-restart case).

### Ops cost

One `DATABASE_URL` secret, sqlx migrations run at startup against a bounded
pool, no new droplet process. The single-tenant/self-hosted path stays fully
functional with no `DATABASE_URL` — see *compat mode* below.

---

## Workspace identity in the journal (record v3)

Every journal record gains a workspace id. Concrete format:

- `Envelope {v, ws, run, at, event}` — `ws: u64`, the workspaces-table primary
  key (`bigint generated always as identity`). `#[serde(default)]` maps v1/v2
  records to **`ws: 0`, the built-in `default` workspace** — existing journals
  need no rewrite; their entire history reads as workspace 0.
- `v = 3` = v2 + `ws`. `durable-domain`'s `Run` gains `ws`; the dedup map
  `by_op` keys `(ws, OperationKey)` so the same idempotency key in two
  workspaces is two runs, never a collision.
- Index snapshot `DRIX VERSION 1 → 2`; the v2 reader defaults missing `ws` to
  0 so snapshots written by v1.0.0 still open.
- Diagnostics gains bounded per-workspace live-run counts (cap distinct
  workspaces shown; overflow aggregates into `other`).

**Internal vs external ids (decided):** `ws: u64` is internal-only — it
lives in journal records, the in-memory index, and the store's joins.
Everything external uses an **opaque `public_id` (UUIDv7)** stored alongside
the PK: route paths (`/v1/workspaces/{public_id}`), API responses, headers
and tokens carry only the opaque id, so workspaces cannot be enumerated and
their count cannot be inferred. The API boundary translates `public_id →
u64` through the same cached lookup path as key verification; the engine
never sees the opaque form. (Run ids stay sequential — they are scoped
inside one workspace, and a guessed cross-workspace id returns 404, the same
no-oracle rule the isolation matrix asserts.)

**D-056's reader-one-release-early rule applies literally:**

- **S2-0** ships `READER_VERSION = 3` with `ENVELOPE_VERSION` still 2: accepts
  v3 records, defaults `ws` to 0, writes v2. It tags its own **v1.0.1**
  release (decided) — the binary a future rollback can land on.
- The writer bump (`ENVELOPE_VERSION = 3`) lands with the workspaces slice.
  From then on every record carries `ws` — in SaaS mode the authenticated
  workspace, in compat mode 0.
- Once a journal holds v3 records, rollback targets must be ≥ the S2-0
  reader; the format-guard already refuses anything older, and that refusal
  is exercised in the slice's failure case.

**Migration of a v1.x journal:** none on disk. The account-store migration
creates workspace `0` ("default") and maps the env keys onto it:
`DURABLE_API_KEY` acts as its admin key, `DURABLE_READ_KEY` as its read key,
so every existing deployment keeps working byte-for-byte. The first human
account can claim/rename it when S3 exists.

**Compat mode:** no `DATABASE_URL` → the server runs exactly as v1.0.0 did:
env keys scoped to workspace 0, account/workspace routes refuse with a clear
error, records still write v3/ws=0. This is the self-hosted path and keeps
the whole unit battery runnable without Neon.

**Testing (decided):** an `AccountStore` trait with a Postgres/sqlx
implementation — the same implementation runs against Neon in production and
against a local Postgres in dev and CI, so tests exercise the real dialect;
engine-level tests that need no dialect use a small in-memory impl. A
real-Neon CI job runs behind a secret and, when the secret is absent, skips
with a visible notice — never silently passing.

---

## Handler registry, SSRF guard, signed calls

### Per-workspace registry

`handler_registrations(workspace, name, url, created_at, updated_at)` in
Neon — the registrations' authority. At runtime the engine's `Registry`
becomes keyed `(ws, name) → handler`; mutations reach the engine through the
same bounded command channel as `Submit`/`Cancel` (single-writer ordering —
a registration can never be dispatched before it is committed in Neon *and*
delivered to the engine).

**Account-store-down restart:** on startup the registry warms from Neon; if
Neon is unreachable, the engine falls back to a **last-known-good registry
snapshot file** (written on every registration change, bounded size, marked
`registry: stale` in diagnostics while in use). Queued runs then keep
dispatching against the last committed registry rather than parking — the
"accepted jobs keep running" invariant holds across a restart-during-outage.
Absent both store and snapshot, runs whose handler is unresolvable stay
queued honestly (the existing "handler left the registry" path).

### SSRF guard — at registration *and* every call

Port Service Monitor's D-017 gate (its `probe.rs`: `is_local` +
`PolicyResolver` inside reqwest + `.no_proxy()`), adapted to a
tenant-controlled registry:

- **Registration** (fail-fast courtesy check, not the enforcement): scheme
  `http`/`https` only; a literal IP is refused if `is_local`; a hostname is
  resolved and *every* answer validated — one non-global answer refuses the
  registration. A refused URL is never stored.
- **Every call** (the enforcement): the handler client uses a
  `PolicyResolver` as reqwest's `dns_resolver` — resolution returns only
  validated addresses, so the check *is* the dial input and there is no
  validate-then-resolve window (no TOCTOU). Re-resolution happens per new
  connection; pooled connections can only ever point at validated addresses.
- Already in place today and kept: `redirect::Policy::none()` — a 3xx is the
  handler's answer, a failed attempt, never followed (simpler than SM's hop
  revalidation); `.no_proxy()` — env proxy vars cannot reroute tenant calls;
  connect timeout 5 s, bounded reply body.
- Block list: every non-global IANA range — v4 `0/8, 10/8, 100.64/10, 127/8,
  169.254/16, 172.16/12, 192.0.0/24, 192.0.2/24, 192.168/16, 198.18/15,
  198.51.100/24, 203.0.113/24, 224/4+`; v6 `::/128, ::1, ::ffff:` mapped-v4
  (validated via the inner v4), `64:ff9b::/96, 100::/64, 2001:db8::/32,
  fc00::/7, fe80::/10, ff00::/8` and the rest of SM's non-global set.
  Obfuscated literal forms (decimal/octal/hex IPv4, compressed v6) normalize
  through the `url` crate before the check, as SM's does.
- **Lab flag** for fixtures/dev (SM's `SMON_PROFILE=lab` precedent):
  `--allow-local-handlers`, logged loudly at startup, never set in deploy.
- Honest residuals, documented: a public-IP host running something internal
  is out of policy scope; resolution failure at call time classifies as the
  existing connect-failure (`Failed`, retried) — unknown is never called
  success.

### Signed handler calls

Per-workspace HMAC-SHA256 signing secret (`workspaces.handler_secret`, 32
random bytes, shown once at creation/rotation — needed to sign, so stored
retrievably in Neon, never logged, never returned by `GET`).

Headers added alongside the existing D-050 set:

- `x-durable-timestamp` — unix seconds.
- `x-durable-signature` — hex `HMAC_SHA256(secret, ts.run.step.attempt.idem_key.SHA256(body))` —
  binds the call identity, the idempotency key, and the exact body. Probe
  requests sign `ts.run.step.attempt.idem_key.` (empty body digest).

Handler-side contract (documented in `docs/HANDLERS.md`, S2-6): recompute
with its copy of the secret; reject `|now − ts| > 300 s`; dedupe by
`(ts, idem_key)` — replay-bounded by the timestamp window and the key the
handler already honors. A verification example fixture ships with the slice.

---

## Slices, in order

Each carries: outcome / invariant / failing-first test / failure case / load
scenario. None ships without all three, per the testing rule.

| # | Slice | Outcome & invariant | Fails-first test | Failure case | Load |
|---|---|---|---|---|---|
| S2-0 | Reader for record v3 | `READER_VERSION=3`, `Run.ws`, snapshot v2 reader, `(ws,key)` dedup map. Still writes v2. Tags **v1.0.1** (decided) — the binary a future rollback can land on. | A v3 journal fixture refuses on the v1.0.0 binary, replays on the new one | Startup on a v3 journal + rollback attempt to v1.0.0 → format-guard refuses | 800k-record replay timing vs the D-054 baseline (1.72 s) |
| S2-1 | Account store + keyring | `AccountStore` trait + Neon schema (accounts, workspaces with `public_id`, members, api_keys, handler_registrations), migrations, key verification + write-through cache, ops env key → ws 0. *Store never in the commit path.* | Unknown key → 401; revoked → 401 immediately | Neon unreachable → cached submits pass, uncached → `503 accounts_unavailable`, accepted runs continue; recovery refreshes | 1M submits through the cached path; miss-flood under outage (negative cache bound) |
| S2-2 | Workspace-scoped API | Every data route resolves `ws` from the key; routes take `public_id`, translated to `u64` at the boundary; runs/specs/cancels/lists scoped; dedup per `(ws,key)`. *No route answers across workspaces.* | B's key reads A's run → 404 (existence not leaked); B cancels A's run → 404; same idem key in both ws → two runs; **no response or header carries the internal u64** | Kill −9 mid-multi-tenant run → restart replay rebuilds ws tags; isolation identical after | Two-workspace concurrent battery (splits share the live bound honestly) |
| S2-3 | Writer emits v3 | `ENVELOPE_VERSION=3`; every record carries `ws` (0 in compat mode). | v1.0.x binary refuses the journal; S2-0 reader opens it | Crash between v3 writes; torn-tail + replay with mixed v2/v3 journal | Full `battery.sh` under workspace mode |
| S2-4 | Per-workspace registry | `handler_registrations` + CRUD routes + engine `(ws,name)` registry + snapshot file. *Dispatch never waits on Neon.* | B's spec naming an A-only handler → `unregistered_handler`; register/delete visible only in owning ws | Restart with Neon down → dispatch continues from snapshot, `registry: stale` in diagnostics | Registration churn concurrent with dispatch lookups |
| S2-5 | SSRF guard | `net_guard` (ported `is_local` + `PolicyResolver`), registration check, lab flag. *No handler call may reach a non-global address.* | Full matrix below — incl. rebind: resolve public at registration, private at call → refused before dial | Fixture: private listener asserts zero packets on a refused call | Resolve+validate overhead per call at battery rate (expect µs–ms/conn; measure) |
| S2-6 | Signed calls | Secret gen/rotate, signature headers on call + probe, `docs/HANDLERS.md` + verifier fixture. *A replayed or tampered call is rejected by the handler's own check.* | Fixture verifier rejects: tampered body, stale ts, wrong secret, replayed `(ts, idem)` | Rotate secret mid-run → old signatures rejected, new accepted; in-flight attempts fail honestly and retry | HMAC per call (~µs) — asserted inside the battery, not assumed |
| S2-7 | The gate | Isolation + SSRF matrices green; crash drills re-run multi-workspace | The matrix itself | All four required failure cases under two workspaces | `battery.sh --full` (or documented runner-bound equivalent) two-tenant |

Then S3: signup/login/session routes and the dashboard consuming the API
surface below.

---

## Isolation matrix (the S2 gate, half 1)

Asserted via API *and* re-asserted after kill −9 + replay:

- A's run id read by B → **404** (not 403 — existence is not leaked);
  cancel → 404; `/v1/runs`, `/v1/runs/{id}/spec` show nothing of A.
- Same idempotency key submitted in A and B → two independent runs.
- A's key cannot register/list/delete handlers in B; B's spec naming an
  A-only handler → `unregistered_handler`.
- Revoked key → 401 on the next request (write-through invalidation), under
  read *and* write routes; read key still cannot write.
- Env/ops keys scope to workspace 0 only — never to a tenant workspace.
- No API response or header carries the internal `ws` u64 — opaque
  `public_id` only, asserted by a response-scanning test; sequential ids
  cannot be enumerated or counted.
- Neon outage: a cached A key still scopes to A — **fail-closed, never
  fail-open to another tenant.**
- Handler-secret rotation: old signatures rejected, new accepted.
- Per-workspace diagnostics show only the caller's workspace.

## SSRF matrix (the gate, half 2)

Registration *and* call-time, each asserted independently:

- Refused at registration: literal loopback, `10/8`, `172.16/12`,
  `192.168/16`, `169.254.169.254` (metadata), CGNAT `100.64/10`, doc ranges,
  unspecified, multicast; `::1`, `fc00::/7`, `fe80::/10`, v4-mapped
  `::ffff:127.0.0.1`; obfuscated literals (`2130706433`, `0x7f000001`,
  `0177.0.0.1`); non-http(s) schemes (`file:`, `gopher:`, `ftp:`).
- Hostname resolving to *mixed* answers (one private among publics) →
  refused at registration and at call.
- **Rebind**: resolves public at registration, private at call → refused
  before dial; a private listener fixture asserts it never saw a packet.
- Proxy env vars set → call still goes direct (`no_proxy`), env cannot
  reroute.
- Redirects: never followed — 3xx stays a failed attempt (documented).
- DNS failure at call → honest `Failed` (retried), never silently dialed.
- Lab flag permits the local fixture handler; refused without it.

---

## What S3 consumes (API contract owned by S2)

- `POST /v1/signup`, `POST /v1/login`, `POST /v1/logout` — session cookie;
  S3 scope, but the schema reserves `sessions` now so no migration churn.
- `GET/POST /v1/workspaces`, `GET/PATCH /v1/workspaces/{public_id}`,
  member management `GET/POST/DELETE .../members` (roles minimal: `admin`,
  `member` — read keys carry the read role).
- `GET/POST/DELETE /v1/workspaces/{public_id}/keys` — create returns the
  secret once; list shows prefix/created/revoked only.
- `GET/POST/PATCH/DELETE /v1/workspaces/{public_id}/handlers` +
  `POST .../test` (a probe-call through the same SSRF/signature path).
- The existing job surface under workspace scope: `POST /v1/jobs`,
  `GET /v1/jobs`, `GET /v1/jobs/{id}`, `.../spec`, `POST .../cancel` —
  the dashboard reuses the inspector's contract; the inspector itself
  gains a workspace view from the scoped reads.
- `GET /v1/workspaces/{public_id}/diagnostics` — bounded per-ws counts.
- Error envelope stays `{code, message}`; new codes:
  `accounts_unavailable`, `ssrf_refused`, `unregistered_handler` (existing),
  `forbidden` (authenticated but wrong scope).

## Explicitly not built in S2

Signup/login UI and sessions (schema reserves for them), per-workspace
plan-limit enforcement at admission (schema field `plan_limits` reserved;
the global 100k bound still guards the box), billing, e-mail, anything the
dashboard renders.

## Decisions taken by the coordinator (2026-11-19)

1. **Testing without live Neon:** `AccountStore` trait; the Postgres impl
   runs against a local Postgres for tests (real dialect, same code path)
   plus an in-memory impl where no dialect is needed; a real-Neon CI job
   runs behind a secret and skips with a visible notice when absent —
   never silently passing.
2. **External workspace ids are opaque:** internal `u64` PK in the journal,
   index and store joins; `public_id` (UUIDv7) on every external surface —
   URLs, API responses, headers, tokens — so workspaces cannot be
   enumerated or counted. A test asserts no response or header carries the
   u64.
3. **S2-0 tags its own v1.0.1 release** — D-056's reader-one-release-early
   rule honored as written; folding it into the S2 release would have
   narrowed the rollback window the rule exists to protect.
