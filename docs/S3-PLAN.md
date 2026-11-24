# S3 — accounts, onboarding and the SaaS dashboard (plan)

Phase S3 of `docs/SAAS_PLAN.md`: open sign-up with email verification,
member/team workspaces, self-service API keys and handler management,
saved workflows with Run now, plan-aware free limits, and the operator
console rebuilt into a dashboard a stranger can sign up to, wire their
endpoint into, send jobs to and trust.

**Status: plan, not built.** Written against the S2 tip (workspaces,
workspace-scoped API keys, the per-workspace handler registry with SSRF
guard and signed calls) and `main` (D-058 observability + the monitoring
dashboard). A separate coordinator branch merges S2 onto main and makes
`GET /metrics`, `GET /events` and `GET /jobs/{id}/events`
workspace-scoped; that part is **not** planned here — S3 consumes it.

Invariants this plan must not break (from S2-PLAN, restated because they
shape every auth decision):

- The journal stays the only authority for jobs. The account store is
  consulted at API ingress and handler dispatch only — never inside the
  commit path.
- Accepted runs keep executing if the account store is unreachable.
- The engine's dispatch path never performs a network read.

---

## Decided upstream (recorded, not reopened)

1. **Open sign-up** with email verification. An operator flag switches
   to invite-only for abuse emergencies (`DURABLE_INVITE_ONLY=1` at boot;
   sign-up then requires a valid invite token — decided as an env flag,
   not a store toggle, because the emergency is precisely "I cannot
   reach the console fast enough").
2. **Free-only v1, plan-aware.** A `plans` table with one `free` row;
   limits and the usage page read from it. No payment provider. The Free
   numbers below are **proposals to be confirmed by measurement in S4**,
   not commitments.
3. **Public status and outside checks come from Service Monitor** (its
   API + signed webhooks), not from the runner. No status page in this
   product. The runner's own `GET /health`/`GET /ready` stay exactly as
   they are so a customer — or Service Monitor — can monitor them.
4. Account records in Neon Postgres (D-059). The journal is the only
   authority for jobs; the store is consulted at ingress and dispatch,
   never in the commit path; accepted runs survive a store outage.
5. **Same-origin deployment:** durable-server serves the dashboard
   (`--ui-dir`) and the API on `durablerunnerapp.404human.dev` —
   Cloudflare in front, on the shared droplet. One build at `/app`;
   `/inspector` answers a 308 to `/app` (path tail and query preserved)
   so old bookmarks keep working.
6. **Email through Resend**, sending domain `404human.dev` (verified);
   the API key comes from the environment (`RESEND_API_KEY`), is never
   logged, never argv.
7. **Design stays:** the existing vermilion design system of the
   inspector (tokens, components, white page in light, separated
   sidebar). This plan defines information architecture and states, not
   a new look.

---

## What S3 builds on

S2 + main merged give us, already built:

- `durable-accounts`: `AccountStore` trait over Postgres (`Pg`,
  `sslmode=verify-full` enforced for non-loopback hosts, pool 5,
  `STORE_DEADLINE` 5 s) and `Memory`. Tables `0001_init.sql`:
  workspaces (`u64` PK + `public_id` UUIDv7 + `handler_secret` +
  `plan_limits`), accounts (`password_hash` reserved, unused),
  members (`admin|member`), api_keys (sha256 digests, `admin|read`),
  handler_registrations, sessions (reserved, unused).
- `durable-server`: `Authorized`/`ReadAuthorized` extractors → `Auth
  {ws, permission}`; a `Keyring` with the verified-grant cache
  (`GRANT_TTL` 60 s, `DENY_TTL` 5 s, `KEY_CACHE_CAP` 10k, write-through
  invalidation); the `dr_`-prefix shape pre-check; `pubid` opaque run
  ids; workspace-scoped `/jobs`, `/diagnostics`, `/whoami`,
  `/handlers*`, `/handler-secret`; ops-only `/admin/*`; the SSRF guard
  (`ssrf.rs`, `PolicyResolver` inside reqwest, literal-local refusal at
  registration and at startup registry load); signed calls
  (`sign_call`, `with_call`); `HandlerFactory.make_signed`; per-address
  conn/rate/head/body limits (`addr_limits.rs`); the strict CSP for
  served UI.
- `durable-engine` on main: `observe.rs` (D-058) — bounded in-memory
  metrics rings (360×10 s + 1440×60 s), per-handler stats, run timelines
  (20k LRU, head+tail), the 1000-event feed; `GET /metrics`, `GET
  /events`, `GET /jobs/{id}/events` (being made workspace-scoped by the
  coordinator merge).
- `apps/inspector` on main: SvelteKit static build, hash router,
  `paths.base=/inspector`, mounted by `--ui-dir`; sidebar shell,
  `session.svelte.ts` (two sessionStorage keys), monitoring pages
  (overview/runs/handlers/submit/admin/settings), strict CSP hashed by
  `inspector_csp`.
- Attack-suite conventions: `tests/attacks/` (`run.sh`,
  `prove-attacks.sh` — every `// ATTACK(name)` marker has a scratch-copy
  mutant that must turn its test red); perf battery conventions
  (`perf/battery.sh`, `perf/load.mjs` with bounded keep-alive agents);
  the check gate `scripts/check.sh`.

---

## Decision records to add

Numbered from D-063 — ruling 1 numbered them from D-059; at the S2 merge
main had taken D-057–D-061 (+3 → D-062–D-072), and at the c95cdd6 merge
main's S3 round took D-062 (+1 → D-063–D-073, 2026-11-24).

- **D-063 — Open sign-up with email verification; invite-only kill
  switch.** Accounts are created `unverified` and sign in immediately,
  but every workspace-level mutation answers `403 email_unverified`
  until `POST /auth/verify` consumes a token mailed at sign-up.
  `DURABLE_INVITE_ONLY=1` restricts sign-up to invite tokens (abuse
  emergency, ops-set at boot).
- **D-064 — Server-side sessions, not signed cookies.** A `sessions`
  row per session: 256-bit opaque token, SHA-256 stored, idle expiry
  14 d, absolute expiry 30 d, rotation on sign-in and on every
  privilege-affecting change, revoke-one and revoke-all. Rationale:
  revocation on sign-out, password change, role change and compromise is
  the requirement; a signed cookie is either unrevocable inside its
  lifetime or needs a server denylist — i.e., a session table in
  disguise. Sessions verify through the same bounded cache pattern as
  API keys (60 s positive TTL, 5 s negative, 10k cap, write-through
  invalidation), so a Neon outage keeps signed-in dashboards working for
  cached sessions and fails closed (`accounts_unavailable`) for new
  ones — the same posture as submits.
- **D-065 — Cookie session + synchroniser CSRF.** Cookie
  `__Host-dr_session`: `Secure; HttpOnly; SameSite=Lax; Path=/`, no
  `Domain` (so no sibling subdomain can plant or read it; the `__Host-`
  prefix makes the browser enforce that). `Lax`, not `Strict`: it
  already blocks cross-site POSTs, and email links land on top-level
  GETs which keep working. On top of that, every cookie-authenticated
  mutation must carry `x-durable-csrf` equal to the session's CSRF
  token — `hex(HMAC-SHA256(sessions.csrf_seed, "csrf"))`, computed
  server-side per session, returned by `GET /auth/session` and compared
  constant-time on mutations. Deterministic per session, so two tabs
  hold the same token and there is no rotation race; the seed rotates
  when the session does. A planted or guessed value fails the session
  check; and when `Origin`/`Sec-Fetch-Site` headers are present they
  must say this origin/`same-origin`. Bearer-key requests are exempt
  (header auth is not ambient authority).
- **D-066 — Member principals on the flat API via
  `x-durable-workspace`.** `Authorized`/`ReadAuthorized` grow a second
  principal kind: session cookie + `x-durable-workspace: <public_id>`
  resolves to `{account, ws, role}` by a membership lookup (cached like
  key grants). This keeps one flat API surface — `/jobs`,
  `/jobs/{id}/events`, `/metrics`, `/handlers` — that the dashboard uses
  with its selected workspace, instead of duplicating ~15 routes under
  `/workspaces/{id}/`. The selection lives in the SPA's
  `sessionStorage`, so two tabs can hold different workspaces; a bearer
  request ignores the header (its ws comes from the key). Role < member
  cannot mutate; viewer reads. Sessions also carry the CSRF check on
  mutations (D-065).
- **D-067 — Plan-aware free tier.** `plans(id, name, limits jsonb)`
  seeded with one `free` row; `workspaces.plan_id` defaults to it.
  Limits are enforced at ingress (server admission and the account
  store), never inside the engine's commit path; the engine's uniform
  per-workspace bounds stay as the hard backstop. The usage page reads
  the same row, so "what the plan allows" and "what the meter says"
  cannot drift.
- **D-068 — Transactional email through an outbox table with bounded
  retries.** Every mail is enqueued in `email_outbox` **in the same
  transaction** as the fact that needs it (account row, invite row), so
  "account exists but mail was lost" cannot happen. A drain worker
  sends due rows (batch ≤ 25, per-send deadline 10 s, exponential
  backoff with jitter, 8 attempts then `failed` kept for inspection).
  Resend is the provider; the outbox row's `idempotency_key` rides
  Resend's `Idempotency-Key` header if supported at implementation time,
  else the row itself is the dedupe. Sent rows are purged after 24 h —
  rendered bodies contain links with tokens; there is no reason to keep
  them. Per-address cap 3 mails/hour so the outbox cannot amplify
  sign-up spam into a mail flood.
- **D-069 — Workflows are saved, versioned job definitions; Run now goes
  through the normal submit path.** A workflow = name + immutable
  `spec` versions (`steps`, default `input_b64`). `POST
  /workspaces/{pid}/workflows/{wid}/run` generates the operation key
  (`wfr_<uuidv7>`), submits through `engine.submit_scoped` — same
  admission, same dedup, same journal — and records the link in
  `workflow_runs`. It replaces the Submit page; it does not add a second
  way into the engine.
- **D-070 — The handler test call goes through the signed, SSRF-guarded
  dispatch path and is never journaled.** `POST /handlers/{name}/test`
  (the flat route, reachable by write keys and member+ sessions via
  D-066) builds the handler via `handler_factory.make_signed` (same
  `PolicyResolver`, same signing, same timeout and bounded reply) and
  makes one call with `idem_key = "test/<uuidv7>"` and no `Call`
  context (run/step/attempt sign as `-`). A refused target is refused
  before dial, exactly like a real call. Rate-limited per workspace and
  per address. It is not a run: nothing is journaled, no at-least-once
  semantics, documented as such.
- **D-071 — `last_used` and session `last_seen` are flushed, not written
  per request.** Verified uses mark an in-memory dirty map (`key_id →
  at`); a task flushes every 60 s in one `UPDATE … WHERE id = ANY`;
  shutdown tries one final flush with a short deadline. A crash loses
  at most 60 s of timestamps — the honest alternative (one write per
  request) prices a read in Neon writes. Documented loss window.
- **D-072 — Workspace deletion is soft, and job data leaves on the
  journal's own schedule.** `workspaces.deleted_at`: admission refuses
  `410 workspace_closed` at once, keys are revoked write-through,
  members lose it from their lists, queued runs are cancelled by the
  engine (cancel stops scheduling; it never undoes a remote effect).
  What it does **not** do: surgically erase journal records — finished
  history leaves on the normal 7-day expiry + compaction horizon, and
  the account-graph rows purge after a 7-day grace for undo. Stated
  plainly on the delete dialog (data-controls note): deleting a
  workspace stops new work and access immediately; already-finished job
  records expire like all finished history.
- **D-073 — API keys stay data-plane only; sessions own the control
  plane.** A key (read or write) can submit, cancel, read, spec, manage
  handlers and test-call — never manage members, invites, workspaces,
  other keys, sessions, or plan data. `POST /handler-secret` tightens
  from S2's "any admin key" to session admin/owner only: a leaked write
  key must not be able to mint the signing secret it would need to forge
  calls.

---

## Data model

Migrations continue `crates/durable-accounts/migrations/` (0001 exists;
new files only — a merged migration is never edited, per the
migration rule; CI checksum manifest gains the new files' hashes).

**Neon constraints applied to every statement** (deploy-readiness
rules): no functions or `ALTER … SET` on custom settings, no role
passwords as SCRAM verifiers, `ALTER OWNER` only with the granted
pattern, no `CREATE ROLE` inside migrations, `CREATE EXTENSION` only for
`pgcrypto` (already present). All DDL below is plain tables/indexes —
the neon-sweep script must run it clean on real Neon before the slice
lands.

### 0002_auth_teams.sql

```sql
-- Members gain owner/viewer; exactly one owner per workspace (partial
-- unique index). Existing 'admin'/'member' rows keep their meaning.
ALTER TABLE members DROP CONSTRAINT members_role_check;
ALTER TABLE members ADD CONSTRAINT members_role_check
    CHECK (role IN ('owner','admin','member','viewer'));
ALTER TABLE members ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE UNIQUE INDEX members_one_owner ON members (workspace_id)
    WHERE role = 'owner';

ALTER TABLE accounts
    ADD COLUMN public_id UUID,
    ADD COLUMN email_verified_at TIMESTAMPTZ;
UPDATE accounts SET public_id = gen_random_uuid() WHERE public_id IS NULL;
ALTER TABLE accounts ALTER COLUMN public_id SET NOT NULL;
CREATE UNIQUE INDEX accounts_public_id ON accounts (public_id);
-- emails are stored lower-cased at the boundary; enforce shape cheaply:
ALTER TABLE accounts ADD CONSTRAINT accounts_email_lc
    CHECK (email = lower(email) AND position('@' IN email) > 1);

-- Sessions become real: opaque token hashed at rest, idle+absolute
-- expiry, revocation, rotation linkage, CSRF token, bookkeeping.
ALTER TABLE sessions
    ADD COLUMN public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    -- No default (review #11): a NULL-able or defaulted seed would mint
    -- the publicly computable CSRF token HMAC(zero, "csrf") for any row
    -- that forgot to bind it.
    ADD COLUMN csrf_seed BYTEA NOT NULL CHECK (octet_length(csrf_seed) = 32),
    ADD COLUMN last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN idle_expires_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN revoked_at TIMESTAMPTZ,
    ADD COLUMN replaced_by BIGINT REFERENCES sessions (id),
    ADD COLUMN user_agent TEXT,
    ADD COLUMN ip INET;
CREATE UNIQUE INDEX sessions_public_id ON sessions (public_id);
CREATE INDEX sessions_account_live ON sessions (account_id)
    WHERE revoked_at IS NULL;

CREATE TABLE plans (
    id TEXT PRIMARY KEY,                  -- 'free'
    name TEXT NOT NULL,
    limits JSONB NOT NULL,                -- keys listed in "Plans"
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO plans (id, name, limits) VALUES ('free', 'Free', '{ … }');

ALTER TABLE workspaces
    ADD COLUMN created_by BIGINT REFERENCES accounts (id),
    ADD COLUMN plan_id TEXT NOT NULL DEFAULT 'free' REFERENCES plans (id),
    ADD COLUMN deleted_at TIMESTAMPTZ;
CREATE INDEX workspaces_live ON workspaces (id) WHERE deleted_at IS NULL;

-- Single-use tokens for email verification and password reset.
CREATE TABLE email_tokens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts (id),
    kind TEXT NOT NULL CHECK (kind IN ('verify','reset')),
    token_hash BYTEA NOT NULL UNIQUE,     -- sha256 of the mailed value
    sent_to TEXT NOT NULL,                -- the address the token went to
    -- The sign-up's password hash, pending (review #2): verify installs
    -- the password of the sign-up whose mail was actually opened, so a
    -- later re-sign-up always wins and a pre-registration attacker's
    -- credential never survives verification.
    password_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    superseded_at TIMESTAMPTZ             -- a newer token of same kind+account
);
CREATE INDEX email_tokens_pending ON email_tokens (account_id, kind)
    WHERE used_at IS NULL AND superseded_at IS NULL;

-- Workspace invites: one live invite per (workspace, email); the token
-- binds the invite to the address it was sent to (no hijack by another
-- account).
CREATE TABLE invites (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    public_id UUID NOT NULL UNIQUE,
    workspace_id BIGINT NOT NULL REFERENCES workspaces (id),
    email TEXT NOT NULL,                  -- lower-cased
    role TEXT NOT NULL CHECK (role IN ('admin','member','viewer')),
    invited_by BIGINT NOT NULL REFERENCES accounts (id),
    token_hash BYTEA NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX invites_pending ON invites (workspace_id, email)
    WHERE accepted_at IS NULL AND revoked_at IS NULL;

-- Review #4c/#17: ws 0 is the ops plane, never a tenant. Membership and
-- invites can only exist for tenant workspaces (id <> 0), so no session
-- principal can ever resolve to the ops workspace through a member row.
ALTER TABLE members
    ADD CONSTRAINT members_ws_nonzero CHECK (workspace_id <> 0);
ALTER TABLE invites
    ADD CONSTRAINT invites_ws_nonzero CHECK (workspace_id <> 0);

-- Review S3: api_keys.id (sequential) crossed GET …/keys; add the same
-- public_id indirection every other entity has (exposed in S3-5).
ALTER TABLE api_keys
    ADD COLUMN public_id UUID,
    ADD COLUMN created_by BIGINT REFERENCES accounts (id);
UPDATE api_keys SET public_id = gen_random_uuid() WHERE public_id IS NULL;
ALTER TABLE api_keys ALTER COLUMN public_id SET NOT NULL;
CREATE UNIQUE INDEX api_keys_public_id ON api_keys (public_id);

-- Transactional mail. Enqueued in the same transaction as the fact that
-- needs it; the drain worker owns status transitions.
CREATE TABLE email_outbox (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    kind TEXT NOT NULL,                   -- verify|reset|invite|notice
    send_to TEXT NOT NULL,
    subject TEXT NOT NULL,
    text_body TEXT NOT NULL,
    html_body TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE, -- stable per row for resend-after-crash
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','sending','sent','failed')),
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);
CREATE INDEX outbox_due ON email_outbox (next_attempt_at)
    WHERE status IN ('pending','sending');
```

### 0003_product.sql

```sql
ALTER TABLE api_keys
    ADD COLUMN created_by BIGINT REFERENCES accounts (id),
    ADD COLUMN last_used_at TIMESTAMPTZ;
CREATE INDEX api_keys_ws_live ON api_keys (workspace_id)
    WHERE revoked_at IS NULL;

ALTER TABLE handler_registrations
    ADD COLUMN created_by BIGINT REFERENCES accounts (id),
    ADD COLUMN last_test_at TIMESTAMPTZ,
    ADD COLUMN last_test_status TEXT,     -- ok|failed|refused|lost
    ADD COLUMN last_test_latency_ms INT;

CREATE TABLE workflows (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    public_id UUID NOT NULL UNIQUE,
    workspace_id BIGINT NOT NULL REFERENCES workspaces (id),
    name TEXT NOT NULL,
    created_by BIGINT REFERENCES accounts (id),
    current_version INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX workflows_ws_name ON workflows (workspace_id, name)
    WHERE archived_at IS NULL;

-- Immutable: a version row is inserted, never updated; a workflow edit
-- appends the next version.
CREATE TABLE workflow_versions (
    workflow_id BIGINT NOT NULL REFERENCES workflows (id),
    version INT NOT NULL,
    spec JSONB NOT NULL,   -- {"version":1,"steps":[{"name","handler"}],"input_b64"}
    created_by BIGINT REFERENCES accounts (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, version)
);

-- Which runs a workflow produced. run_pubid is the opaque id the client
-- got back — the internal RunId never crosses the store boundary
-- (pubid permutation is stable per journal; recorded at submit).
CREATE TABLE workflow_runs (
    workspace_id BIGINT NOT NULL REFERENCES workspaces (id),
    workflow_id BIGINT NOT NULL REFERENCES workflows (id),
    run_pubid TEXT NOT NULL,
    submitted_by BIGINT REFERENCES accounts (id),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, run_pubid)
);
CREATE INDEX workflow_runs_ws ON workflow_runs (workspace_id, submitted_at DESC);
```

Bounds enforced at insert (store-side where a race exists, like the
S2-6 handler bound; route-side otherwise): plan-capped counts for keys,
members, handlers, workflows; `email_outbox` per-address pending cap
(live pending rows per address ≤ 3; beyond it the mail is silently
coalesced — the *response* is still identical, so no oracle).

### Engine change the data model needs (small)

`WorkspaceDiagnostics` gains `finished_runs: u64` — a per-workspace
counter incremented when a run reaches a terminal state, replay-derived
like every index fact (no journal change: replay walks the same
transitions) — and `in_flight: u64`, the workspace's live dispatch-permit
count (the same counter the per-ws permit pool decrements; the usage
meter reads it). The onboarding checklist's "first run finished" and the
usage meter need facts, not flags; scanning run pages would be an
unbounded read on a hot path.

---

## Authentication

### Password hashing — argon2id

- `accounts.password_hash` = PHC string from `argon2` (RustCrypto,
  pure-Rust), variant **argon2id**, parameters **m = 19 MiB, t = 2,
  p = 1** — the OWASP Password Storage Cheat Sheet floor.
  Why not RFC 9106's m = 64 MiB/t = 3/p = 4: hashing runs on the shared
  2 GB droplet; verify+hash go through `spawn_blocking` behind a
  semaphore of **4**, so worst-case resident is ~80 MiB vs ~256 MiB —
  the larger parameters buy margin the threat model doesn't need at
  friends-beta scale, and the parameter set is a config constant that
  can be raised once measured (recorded in the decision entry).
- **Never on an async worker, and the queue is bounded** (review #9):
  the semaphore permit is acquired on the async side *before*
  `spawn_blocking` (blocking-pool threads never park on auth work);
  at most ~32 waiters, each waiting ≤ 2 s — beyond either bound the
  request is refused `503 busy` + `Retry-After`. One argon2 job per
  client address at a time (a second concurrent request from the same
  address → `503 busy`); rate-limit, body and shape checks all run
  before the permit. (Reserve-1-permit for known-device cookies rides
  with the S3-2 lockout mechanics.)
- **Signup hashes on every branch** (review S3): the argon2 hash runs
  before `store.signup` is called, so Created / VerifyResent / Taken
  all cost the same work — the branch is not a timing oracle. Forgot/
  resend do their store work *after* the identical 202 is composed
  (same reason, S3-1).
- Constant-work for unknown accounts: a login attempt for an email with
  no account verifies against a **dummy PHC string** (a fixed argon2id
  hash of a fixed input), so "email exists" is not a timing oracle.
  Asserted by instrumentation (a counter on the dummy path), not wall
  time — machine-speed tests are banned.

### Sign-up, verification, sign-in

- `POST /auth/signup` validates (email shape, password policy), then in
  one transaction: insert account (unverified) + verify `email_tokens`
  row + `email_outbox` row. Response is always
  `202 {"status":"verification_sent"}` — the same body whether the
  address is new, registered, or registered-but-unverified (in which
  case a *new* verify token supersedes the old and a fresh mail goes;
  for a verified existing account a "someone tried to sign up — if this
  was you, sign in or reset" notice mail is enqueued instead, capped by
  the outbox per-address limit). `DURABLE_INVITE_ONLY=1` → `403
  invite_required` unless the body carries a valid invite token.
- Password policy: 12–128 chars, UTF-8, a small common-password
  denylist; no composition rules (NIST-aligned).
- `POST /auth/verify {token}` consumes the token in a transaction that
  sets `email_verified_at`. Wrong/expired/consumed/superseded → `410
  invalid_or_expired_token` (one message; which kind of dead is not
  said). The dashboard may then sign in.
- `POST /auth/login {email,password}`: generic `401
  invalid_credentials` for unknown email *and* wrong password; sets the
  session cookie on success and returns the session bootstrap.
  Unverified accounts sign in (they see the verify-wall; workspace
  mutations refuse `email_unverified`).

### Sessions (D-064)

- Token: `drs_` + hex(16 B random ∥ 16 B tag) where
  `tag = HMAC-SHA256(session_key, random)[..16]` — **self-authenticating**
  (review #8): a forged value fails the tag before any store I/O, so a
  random-token flood can never reach the Neon pool. The store keeps
  SHA-256 of the whole token as today. `session_key` is derived
  server-side from the journal's `public_id.key` via
  `HMAC(key, "session-token-v1")` — one secret file, domain-separated.
  New-format API keys get the same tagged shape (`drk_`, S3-5); legacy
  `dr_` keys keep verifying through the store until rotated.
- `sessions`: `idle_expires_at` = now+14 d, slid forward on use;
  `expires_at` = created+30 d (absolute, never slides); `revoked_at` for
  logout/revoke; `replaced_by` chains rotations.
- Rotation: sign-in always mints a *new* token (the cookie is set
  fresh — fixation is trivially impossible since the client never
  chooses it); password change, email verify, and role/membership change
  revoke the session's siblings and mint new (a stale session after a
  privilege change is the classic session-fixation-adjacent hole).
- Revoke-all: `POST /me/sessions/revoke-all` revokes every session
  except the current one; password reset/change **and email verify**
  revoke **all** (including current — the reset response sets a fresh
  session after `POST /auth/reset`; verify never sets a cookie at all,
  so a mailed link can never sign the opener's browser in as the
  account — review #17). Re-sign-up of an unverified address also
  revokes its sessions (the pre-registration takeover's second leg).
- `last_seen_at` flushes per D-071 (60 s flush; idle-expiry checks use
  the flushed value — a session may over-live by ≤ 60 s + flush lag,
  documented).
- **Cache-fill generations (review #10).** Every cache filled after an
  `await` records `(invalidation-key, fill-start)`: a bump logged for
  the same key inside the await window means the fetched value
  post-dates nothing — the fill is dropped and the answer denied
  rather than served stale. Bumps: `revoke_session`/`revoke-all`
  (session token / account), `revoke_key`/`rotate_handler_secret`/
  `delete_workspace` (key hash / workspace), `remove_member`/`demote`
  (membership). The bump log is a bounded in-memory vec (~60 s of
  entries, matching the grant TTL) — overflow clears the cache, which
  is a safe global invalidation. Hits check their key's latest bump
  too: an entry older than its invalidation counts as a miss.
- **Verify/reset revoke semantics:** `POST /auth/verify` consumes the
  token in one transaction that installs the token's pending password
  hash, sets `email_verified_at` and revokes every session of the
  account — and sets **no cookie**. `POST /auth/reset` revokes all and
  sets a fresh session **only when the request carries no session
  cookie or the cookie is for the same account** (never signs a
  different signed-in browser over).
- Per-account session cap: 20 live sessions; the 21st sign-in evicts the
  oldest-idle (LRU), never refuses — a cap that refuses is a self-DoS.
- Max absolute lifetime bounds "stolen laptop" exposure; idle bounds
  shared-machine exposure.

### CSRF (D-065)

- `sessions.csrf_seed` = 32 random bytes drawn at session creation; the
  CSRF token is `hex(HMAC-SHA256(seed, "csrf"))`, computed on demand —
  nothing client-usable is stored, and every tab/bootstrap computes the
  same token for a session.
- Every cookie-authenticated non-GET must send `x-durable-csrf:
  <token>`; the server recomputes and compares constant-time.
  Missing/wrong → `403 csrf_failed`.
- Additionally, if `Origin` is present its authority must equal the
  request `Host`; if `Sec-Fetch-Site` is present it must be
  `same-origin`/`none` (`same-site` is refused — sibling subdomains are
  not the origin).
- **The same Origin/Sec-Fetch-Site check runs on every unauthenticated
  `/auth` POST** (signup/login/forgot/reset/verify) — review #17: with
  no session the synchroniser token cannot exist, so the origin check
  is the CSRF layer there. JSON-only bodies (`J<T>` → 415 on
  form-encoded) and no CORS layer are the other two invariants.
- Bearer-key calls skip CSRF — but **when any `Authorization` header is
  present the request resolves as bearer only** (review S3): no cookie
  fallback, so a `drs_` bearer and a `dr_` cookie both just 401. The
  CSRF exemption follows the resolved principal, not the header.

### Enumeration resistance

- Identical response bodies/statuses for sign-up and forgot regardless
  of account existence; the difference is only which outbox row was
  enqueued — invisible to the caller.
- Login: same `401` for unknown and wrong password + dummy-hash verify
  on the unknown path (constant-work, asserted by counter).
- Verify/reset/invite tokens: one `invalid_or_expired` refusal for
  unknown, consumed, expired, superseded — no "this kind of dead"
  oracle.
- Run/workspace/handler ids keep the S2 no-oracle rules (404, never
  403, for foreign ids).

### Rate limits (proposed; tuned in S4)

| Limit | Value | Scope |
|---|---|---|
| `POST /auth/signup` | 5/hour, 20/day | per address (IP) |
| `POST /auth/login` | 10/min | per address |
| login failures | 10 / 10 min → 15-min cool-off | per (email, client /24-or-/64), in-memory bounded counters — keyed by normalized email **whether or not the account exists** so 429-vs-401 is not an oracle (review #13); a known-device cookie bypasses the cool-off; a successful reset clears it; the owner notice mail rides the per-(address,kind) budget, at most once a day |
| `POST /auth/forgot`, `/auth/resend-verification` | 5/hour | per address |
| `POST /auth/verify`, `/auth/reset`, `/invites/accept` | 20/min | per address |
| outbound mail | 3/hour per (address, kind) — newest replaces pending | per email address (outbox); plus a **global** daily budget with a circuit breaker (review #14) |
| invites | 20/day | per workspace |
| handler test calls | plan value (Free: 30/hour) | per workspace + 10/min/address |

Per-account login throttling **slows, never locks** — a hard lock is a
"lock the victim out" DoS; the cool-off is bounded and the reset path
always works. The existing per-address conn/rate/head/body limits stay
in front of all of it.

---

## The authorization matrix

Principal kinds (review #4 — one enum, no collapsing):
`Principal { OpsKey, OpsRead, StoreKey { ws, Write|Read },
Member { account, ws, role } }`. The env keys (`DURABLE_API_KEY`,
`DURABLE_READ_KEY`) are the ops plane: they grant `OpsKey`/`OpsRead`
**only on the loopback `--ops-listen` listener** on a SaaS deployment
(ruling 2'); with no `--ops-listen` flag (self-host/dev) they keep
working on the main listener, exactly like v1.0.x/S2. Store keys carry
`Write|Read` — the Rust enum is `Permission::{Write, Read}`; `admin`
stays the stored string, `write` is the API/UI word (ruling 3).
`ops_admin` matches `OpsKey` only — a store key on ws 0 can never reach
`/admin/*`. A session resolves to `Member{…}` only through a member
row; `CHECK (workspace_id <> 0)` means no session is ever ws-0.
`—` = route unreachable for that principal; `404*` = foreign-workspace
id answers 404, never 403 (no oracle).

Membership resolution (review #1): `/workspaces/{pid}/*` routes resolve
the member role from the **path** `pid` only (`PathMember`); if
`x-durable-workspace` is also present and differs → `400
workspace_mismatch`. Flat routes (`/jobs`, `/handlers`,
`/diagnostics`, `/whoami`) take the workspace from
`x-durable-workspace` for session principals — absent or unresolvable
→ `400 workspace_required` (a session never defaults to ws 0). Bearer
keys need no header: the workspace is in the key.

| Route | Ops key | Write key | Read key | Owner | Admin | Member | Viewer |
|---|---|---|---|---|---|---|---|
| `GET /health`, `/ready` | open | open | open | open | open | open | open |
| `GET /jobs` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `GET /jobs/{id}` | ✓ | ✓ (404*) | ✓ | ✓ | ✓ | ✓ | ✓ |
| `GET /jobs/{id}/events` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `GET /events`, `GET /metrics` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `GET /jobs/{id}/spec` | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `POST /jobs` | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `POST /jobs/{id}/cancel` | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `GET /diagnostics` (own ws) | ✓ | ✓ | — | ✓ | ✓ | ✓ | ✓ |
| `GET /whoami` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `GET /handlers` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `PUT /handlers/{name}` | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `DELETE /handlers/{name}` | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `POST /handlers/{name}/test` (new) | ✓ | ✓ | — | ✓ | ✓ | ✓ | — |
| `POST /handler-secret` | ✓ | **—** (D-073, tightened) | — | ✓ | ✓ | — | — |
| `POST /admin/compact`, `/admin/backup`, `GET /admin/diagnostics`, `/admin/workspaces*` | ✓ | — | — | — | — | — | — |
| `POST /auth/*` (login etc.) | — | — | — | open | open | open | open |
| `GET /auth/session`, `POST /auth/logout`, `GET /me*` | — | — | — | ✓ | ✓ | ✓ | ✓ |
| `POST /workspaces` | — | — | — | any verified session, ≤ `workspaces_per_account` (creator becomes its owner) |
| `GET /workspaces/{pid}` | — | — | — | ✓ | ✓ | ✓ | ✓ |
| `PATCH /workspaces/{pid}` (rename) | — | — | — | ✓ | ✓ | — | — |
| `DELETE /workspaces/{pid}` | — | — | — | ✓ | — | — | — |
| `GET/POST …/members`, `GET …/invites` | — | — | — | ✓ | ✓ | ✓ GET only | ✓ GET only |
| `POST/DELETE …/invites*` | — | — | — | ✓ | ✓ | — | — |
| `PATCH/DELETE …/members/{account_pid}` | — | — | — | ✓ | ✓ (not owner) | — | — |
| `GET/POST/DELETE/POST-rotate …/keys*` | — | — | — | ✓ | ✓ | ✓ GET only | ✓ GET only |
| `POST …/workflows`, `POST …/workflows/{wid}/run`, `PATCH/DELETE …/workflows/{wid}` | — | — | — | ✓ | ✓ | ✓ | — |
| `GET …/workflows*`, `GET …/workflows/{wid}/runs` | — | — | — | ✓ | ✓ | ✓ | ✓ |
| `GET …/usage`, `GET …/onboarding` | — | — | — | ✓ | ✓ | ✓ | ✓ |
| `POST /invites/accept`, `/invites/decline` | — | — | — | any **verified** session (accept requires session email == invite email — review #2) |

What an API key can never do: anything under `/auth`, `/me`,
`/workspaces*` — sessions only. What a session can never do: `/admin/*`
— ops key only (the ops console stays operator-side). A session whose
`x-durable-workspace` names a workspace the account does not belong to:
`403 forbidden` ("not a member") on the flat routes — the refusal is on
membership, which leaks nothing about whether the workspace exists; and
`404` on the `/workspaces/{pid}`-scoped routes, matching S2's
public_id no-oracle (a foreign `public_id` reads exactly like an unknown
one).

---

## API surface (new)

All under the same envelope `{"error","code"}` and body caps. `{pid}` =
workspace `public_id` (UUIDv7). Session-authed routes also accept
`x-durable-workspace` where noted. New codes:
`unauthenticated`, `invalid_credentials`, `email_unverified`,
`invalid_or_expired_token`, `csrf_failed`, `plan_limit`,
`workspace_closed`, `invite_required`, `invite_pending`,
`invite_email_mismatch`, `name_taken`, `owner_required`, `last_owner`.

### Auth (open; address-rate-limited; bodies ≤ 4 KiB)

| Method | Path | Request → Response | Errors |
|---|---|---|---|
| POST | `/auth/signup` | `{email,password,invite_token?}` → `202 {status:"verification_sent"}` (always) | `bad_input`, `invite_required` 403, `rate_limited` 429 |
| POST | `/auth/verify` | `{token}` → `200 {verified:true}` | `invalid_or_expired_token` 410 |
| POST | `/auth/login` | `{email,password}` → `200 {account:{email,verified}, csrf}` + Set-Cookie | `invalid_credentials` 401, `rate_limited` 429 |
| POST | `/auth/logout` | — → `204` + expired cookie | — |
| POST | `/auth/forgot` | `{email}` → `202 {status:"reset_sent_if_known"}` (always) | `rate_limited` 429 |
| POST | `/auth/reset` | `{token,password}` → `200` + fresh session cookie | `invalid_or_expired_token` 410, `bad_input` |
| POST | `/auth/resend-verification` | `{email}` → `202 {status:"verification_sent"}` (always) | `rate_limited` 429 |
| GET | `/auth/session` | — → `200 {account:{public_id,email,verified}, csrf, workspaces:[{public_id,name,role,plan}]}` | `unauthenticated` 401 |

### Me (session)

| Method | Path | Purpose |
|---|---|---|
| `GET /me` | profile + workspaces list (same as `/auth/session` minus csrf) |
| `POST /me/password` | `{current,next}` → 200; verifies current, rehashes, **revokes all sessions incl. current**, sets fresh cookie |
| `GET /me/sessions` | `[{public_id,created_at,last_seen_at,ip,user_agent,current}]` (bounded 50) |
| `POST /me/sessions/revoke-all` | revokes every session except current → `{revoked:n}` |
| `DELETE /me/sessions/{sid}` | revoke one → 204 |

### Workspaces + team (session, `x-durable-workspace` where `{pid}`)

| Method | Path | Role | Notes |
|---|---|---|---|
| `POST /workspaces` | `{name}` | verified acct | returns `{public_id,name,handler_secret}` — secret shown once here; ≤ `workspaces_per_account` |
| `GET /workspaces/{pid}` | — | viewer+ | `{public_id,name,plan,role,created_at}` |
| `PATCH /workspaces/{pid}` | `{name}` | admin+ | |
| `DELETE /workspaces/{pid}` | `{confirm:"delete <name>"}` | owner | soft delete (D-072); live runs cancelled; keys revoked write-through |
| `GET /workspaces/{pid}/members` | — | viewer+ | `[{account:{public_id,email},role,since}]` |
| `POST /workspaces/{pid}/members` | `{email,role}` | admin+ | creates an **invite** (never a silent add) → `{invite:{public_id,email,role,expires_at}, invite_url}` — `invite_url` carries the token once, shown to the inviter for out-of-band sharing; `409 invite_pending` on a live dup; re-inviting = revoke + create |
| `PATCH /workspaces/{pid}/members/{account_pid}` | `{role}` | admin+ | owner row immutable (`403 owner_required`/`last_owner`) |
| `DELETE /workspaces/{pid}/members/{account_pid}` | — | admin+ | `last_owner` 409 if removing the owner |
| `GET /workspaces/{pid}/invites` | — | viewer+ | pending invites `[{public_id,email,role,expires_at}]` |
| `DELETE /workspaces/{pid}/invites/{iid}` | — | admin+ | revoke → 204 |
| `POST /invites/accept` | `{token}` | any session | `403 invite_email_mismatch` if session email ≠ invite email → member row + `{workspace}` |
| `POST /invites/decline` | `{token}` | any session | marks revoked; 204 |

### Keys (session admin+; viewer GET only)

| Method | Path | Notes |
|---|---|---|
| `GET /workspaces/{pid}/keys` | `[{id, name, prefix, scope, created_at, last_used_at, revoked}]` — metadata only, never secrets |
| `POST /workspaces/{pid}/keys` | `{name, scope:"read"\|"write"}` → `{key:"dr_…"}` **once**; ≤ `api_keys` plan cap |
| `DELETE /workspaces/{pid}/keys/{key_id}` | revoke → write-through cache invalidation, 204 |
| `POST /workspaces/{pid}/keys/{key_id}/rotate` | one transaction: revoke old + create new (same name+scope) → `{key}` once |

### Handlers (session + key; viewer GET only for session)

Existing `/handlers`, `/handlers/{name}`, `/handler-secret` stay
unchanged for keys; session principals reach them via D-066 with the
role column above — no `/workspaces/{pid}/handlers` aliases, one path.
Changes to the flat route's behaviour:

| Method | Path | Notes |
|---|---|---|
| `POST /handlers/{name}/test` (new) | `{input_b64?}` (≤ 8 KiB) → `{status:"ok\|failed\|refused\|lost", http_status, latency_ms, reply_excerpt_b64 ≤1KiB}` — the D-070 path, write key or member+ |
| `GET /handlers` (extended) | each entry gains `health:{last_test_at,last_test_status,last_test_latency_ms, p50_ms, p95_ms}` — test columns from the store row, p-values from the ws-scoped observer rings |

### Usage, onboarding, workflows

| Method | Path | Notes |
|---|---|---|
| `GET /workspaces/{pid}/usage` | `{plan, limits:{…}, used:{jobs_this_minute, live_runs, in_flight, handlers, api_keys, members, workflows, test_calls_hour}, windows:{minute_ends_at,hour_ends_at}}` |
| `GET /workspaces/{pid}/onboarding` | `{steps:[{id:"handler",done,detail},{id:"test_call"…},{id:"api_key"…},{id:"first_run"…}]}` — **derived from facts**: `list_handlers` non-empty; `last_test_status == "ok"`; live key exists; `diag_ws.finished_runs > 0` |
| `GET /workspaces/{pid}/workflows` | list `[{public_id,name,current_version,updated_at,last_run:{pubid,at,state}?}]` |
| `POST /workspaces/{pid}/workflows` | `{name, spec:{steps,input_b64}}` → creates v1; name unique per ws (`409 name_taken`); spec validated exactly like a submit (handlers must resolve — `unregistered_handler` 409 — before the row exists) |
| `GET /workspaces/{pid}/workflows/{wid}` | detail + all versions `[{version,spec,created_at}]` |
| `PATCH /workspaces/{pid}/workflows/{wid}` | `{name?, spec?}` — a spec change appends version n+1 (immutable); `name` change only bumps `updated_at` |
| `DELETE /workspaces/{pid}/workflows/{wid}` | archive (`archived_at`); list/get omit it |
| `POST /workspaces/{pid}/workflows/{wid}/run` | `{input_b64?}` (override default; a client `Idempotency-Key` header derives the op key `wfr_<wid>_<uuid>` from it so SPA retries don't double effects — review S3) → `engine.submit_scoped(ws, op_key, spec)` → `{run_id, version}`; `workflow_runs` row |
| `GET /workspaces/{pid}/workflows/{wid}/runs` | page of `workflow_runs` joined through `scoped_run` (never raw `engine.inspect` — review #3) (≤50/page) → `[{run_id,submitted_at,state,finished_at}]` |
| `builtin:echo` | **No public `/echo` route** (ruling 5' replacing ruling 5): the onboarding sample is an in-process builtin handler — name `builtin:echo` registers like a normal handler, no network hop, echo of the call input bounded to the step input cap, per-workspace rate-limited like every test call, clearly labelled "sample handler" in the UI. It can never be a reflector, needs no signature headers, and shares no bucket across tenants (review #16 is moot). The exit journey uses it, then also registers a real fixture handler URL |

Errors: `plan_limit` 429 carries `{limit,plan,used,cap}` + `Retry-After`
for rate kinds; `unregistered_handler` 409; `workspace_closed` 410;
`name_taken` 409.

---

## Plans and limits

`plans.limits` keys — the Free row's values below are **accepted as the
v1 defaults** (ruling 4), labelled proposals in UI copy; S4 measurement
confirms or changes them **through a new `plans` row value, never a
code change**:

| Limit | Free (proposed) | Enforced at | Refusal |
|---|---|---|---|
| `jobs_per_min` | 60 (token bucket, burst 10) | server ingress, per ws | 429 `plan_limit` + Retry-After |
| `live_runs` | 500 accepted-unfinished | server ingress pre-submit (count from `diag_ws`; engine's uniform per-ws bound stays the hard backstop) | 429 `plan_limit` |
| `in_flight` | 8 concurrent dispatches | engine per-ws bound (S2 flag `max_in_flight_per_ws` defaults already apply; plan row records the number) | queue, not refused — dispatch slots are fairness, not admission |
| `handlers` | 16 | store insert | 429 `plan_limit` |
| `max_input_bytes` | 64 KiB | admission (same as engine's cap; a paid tier could raise it) | 429 `plan_limit` |
| `retention_days` | 7 | informational — matches the engine's terminal expiry | — |
| `members` | 5 (owner included) | invite insert | 429 `plan_limit` |
| `api_keys` | 10 | key insert | 429 `plan_limit` |
| `workflows` | 20 | workflow insert | 429 `plan_limit` |
| `test_calls_per_hour` | 30 | test route bucket | 429 `plan_limit` + Retry-After |
| `workspaces_per_account` | 3 | workspace create | 429 `plan_limit` |

The 429 body: `{"error":"…","code":"plan_limit","limit":"api_keys","plan":"free","used":10,"cap":10}`. Dashboard copy per limit, e.g.
keys: *"The Free plan allows 10 API keys. Revoke one you no longer need,
or wait for Pro — every limit is already in the plan table."* Rate kinds
add *"retry in N s"* from `Retry-After`.

The submit-rate bucket lives beside `addr_limits` — a per-workspace
token bucket keyed on `auth.ws`, in memory, bounded (map cap = live
workspaces, LRU beyond), so a tenant's burst hits its own bucket, never
the shared per-address limiter's fairness. **Run now calls the same
`admit(ws, spec)` as `POST /jobs`** (review #7): one admission path —
the per-ws submit bucket and the `live_runs`/`retained_bytes`
pre-checks — so a workflow run can never bypass plan limits.

**Plan capacity bounds (review #7):** the Free row gains
`retained_bytes` (~16 MiB proposal) enforced as the per-workspace index
share at admission — so `workspaces_per_account` × share stays far
below the global index bound and two free accounts cannot exhaust it.
Soft-deleted workspaces count toward `workspaces_per_account` until the
purge removes the row (no delete+recreate cycle to dodge the cap).

**Neon growth bounds (review #15):** every table above has a bounded
lifetime: workflow versions ≤ 50 per workflow (insert refuses past it)
with spec ≤ 96 KiB and a per-workspace workflow-write rate; a purge
job (nightly, in-process behind `store`-mode) deletes runs-index rows,
revoked+expired sessions, used/expired `email_tokens`, closed invites,
failed `email_outbox` rows and dropped mail bodies past token expiry —
the store can never grow without bound under churn.

---

## Onboarding

- **Checklist** (`GET …/onboarding`, facts not flags): *register a
  handler* → *send a test call that answers ok* → *create an API key* →
  *finish your first run*. The page checks each item off from the
  endpoint's `done`/`detail`; it can never claim "done" the data can't
  prove.
- **Quick start** on the empty states and the onboarding card:
  copy-paste curl for register-handler and submit (with the workspace's
  real `dr_` key placeholder `<key>` — never an actual secret in the
  page) and short SDK snippets (TypeScript `fetch`, Python `requests`,
  and a signature-verification snippet matching `docs/HANDLERS.md`).
- **Sample workflow:** a "try it" card on the Workflows empty state
  creates a workflow `demo` whose one step calls `builtin:echo` — the
  in-process sample handler (ruling 5'; no URL, no network hop, echoes
  its input bounded to the step cap) — then Run now lands the user on
  the finished timeline. Zero setup; the journey is the product
  teaching itself. The card also offers "point it at a real endpoint"
  with the register-handler curl.
- Signed-in-but-unverified users see a verify-wall card (resend button →
  `/auth/resend-verification`); read pages work so they can look around
  — mutations refuse `email_unverified` with a banner.

---

## The dashboard

**Stack unchanged:** the same SvelteKit static build, hash router,
`paths.base` — the app is served by durable-server via `--ui-dir`
at the product mount (proposed `/app`; the `/inspector` mount stays for
self-host/ops installs — one build, two mounts is just two `nest_service`
calls). The strict CSP is recomputed the same way
(`inspector_csp` → `ui_csp`, hashing the built `index.html`).

**Mode detection:** the SPA boots on `GET /auth/session`. `401` →
signed-out pages. `404` (no account store — compat mode) → the legacy
key-paste mode exactly as today (self-hosters keep their console).

### Information architecture

Sidebar (top→bottom): **workspace switcher** (select; shows every
membership + plan badge) → **Overview · Runs · Workflows · Handlers ·
API keys · Usage · Team · Settings** → footer: docs link, **account
menu** (email, theme switch, sign out). Hash routes:
`#/overview` `#/runs` `#/runs/{id}` `#/workflows` `#/workflows/{id}`
`#/handlers` `#/keys` `#/usage` `#/team` `#/settings`.
Signed-out (no sidebar): `#/sign-in` `#/sign-up` `#/verify`
`#/verify-sent` `#/forgot` `#/reset` `#/accept-invite` — tokens travel
in the fragment (`#/verify?token=…`), so mail-client prefetchers GET
`/app/` and never see the token; the page POSTs it on load (verify) or
on submit (reset, invite-accept's "Accept" button).

### Per page: purpose · data · actions · states · viewer

- **Sign in / Sign up / Forgot / Reset / Verify / Accept-invite** —
  single forms, one error banner each, the enumeration-safe copy
  ("if that address is registered, mail is on its way"). Loading =
  pending spinner on the button; forbidden n/a. Unverified sign-in lands
  on the verify-wall.
- **Overview** — workspace health at a glance: KPI tiles (live runs,
  finished today, failed, in-flight), the time-window charts from the
  monitoring dashboard (`GET /metrics` ws-scoped, `/diagnostics`), the
  onboarding checklist until it's done, then the activity feed
  (`GET /events`). Empty: the checklist + quick start instead of empty
  charts. Error: banner + retry with backoff (existing poll). Viewer:
  same page, no CTAs.
- **Runs** — today's run list (ws-scoped `GET /jobs`, filters, cursor),
  row → `#/runs/{id}`: timeline (`/jobs/{id}/events`), spec view
  (`/jobs/{id}/spec` hidden for viewers), cancel for member+ (confirm).
  Empty: "no runs yet — Run a workflow or submit via API" + curl. 404
  run id → not-found card. Viewer: read.
- **Workflows** — list, create/edit (steps editor: name + registered
  handler pick from the registry, default input), version history,
  **Run now** → lands on the run's timeline; archive. Empty: sample
  workflow card + quick start. Errors: `plan_limit`, `unregistered_handler`
  inline. Viewer: list/detail, no Run now, no edit.
- **Handlers** — registry table (name, URL, health: last test status +
  latency, p50/p95 from metrics), register/edit/remove, **send test
  call** (result card with status + reply excerpt), signing-secret panel:
  *reveal-once* (shown at workspace creation / rotation only; otherwise
  "shown once at creation — rotate to see a new one"), rotate with
  confirm + copy-the-new-secret-now flow, verify-signature snippet.
  Viewer: table + health, no mutations.
- **API keys** — list (name, prefix, scope, created, last used,
  revoked), create (name + scope → shown once modal, "copy now, it is
  never shown again"), revoke, rotate. `last_used` shows "—" until the
  first flush, honestly. Viewer: list only.
- **Usage** — plan card + every meter (`GET …/usage`) with used/cap bars
  and the exact limit copy; retention row. Viewer: same.
- **Team** — members table (email, role, since), invite by email + role,
  pending invites (revoke), role change, remove — gated by matrix
  (`last_owner`/`owner_required` toasts). Viewer: sees the table, no
  mutations.
- **Settings** — workspace rename (admin+), delete workspace (owner,
  confirm-phrase + the D-072 data note), and the personal section:
  password change, sessions list + revoke-all, theme. Compat mode shows
  the legacy key-paste card here instead.

Every page's **loading** state is the existing skeleton/poll pattern;
**error** states carry the envelope's `code` with a retry; **forbidden**
(`403`/`email_unverified`) renders the page read-only with the reason
banner, not a dead end.

### What happens to the old pages

The monitoring pages (`overview`, `runs`, `handlers` charts) become the
workspace pages above — the merge already makes their data
workspace-scoped. `submit` is **replaced by Workflows/Run now**
(decided). `admin` (compact/backup/diagnostics) stays ops-key-only on
the `/inspector` mount — it is the operator console, not a tenant page.
`settings` splits into workspace settings + personal settings as above.

---

## Transactional email

Through the outbox (D-068) + `Mailer` trait: `Resend` (HTTPS, deadline
10 s, `RESEND_API_KEY` env — never argv/logs), `Log` (dev only: prints
the mail body including the link to stderr, gated by
`DURABLE_MAILER=log`, never compiled into the deploy path's default),
`Memory` (tests assert on captured mails).

Three v1 kinds (+ `notice` for security events: new-sign-in on a
previously-unknown sign-in? — kept minimal: notice used only for the
login-cool-off threshold and signup-collision mail). Each mail: plain
text + a simple HTML rendition of the same text (one layout, the product
wordmark as text, no images, **no tracking pixels/links**). Subjects:
"Verify your Durable Runner email", "Reset your Durable Runner
password", "You're invited to <workspace> on Durable Runner". Links
built from `--public-origin` (the configured service origin — never the
request's `Host`, which is attacker-controlled on the open auth routes).
Token format for all three kinds: `drt_<43 base64url chars>` = 32 random
bytes, URL-safe, carried in the link's fragment (`#/verify?token=…`),
stored as SHA-256 at rest, single-use (`used_at`/`accepted_at` set in
the consuming transaction). Lifetimes: verify 24 h, reset 1 h, invite
14 d; issuing a newer token of the same kind + subject supersedes the
pending old one.

---

## Threats considered

| Threat | Handling |
|---|---|
| Credential stuffing / password guessing | argon2id cost + semaphore; per-IP and per-account budgets; generic 401; dummy-hash constant work; notice mail at threshold |
| Session theft (XSS, stolen cookie) | HttpOnly + Secure + `__Host-`; strict CSP (existing, recomputed); SameSite=Lax; rotation on privilege change; revoke-all; idle+absolute expiry; sessions list |
| CSRF on cookie mutations | synchroniser bound to session + `Origin`/`Sec-Fetch-Site` checks + SameSite=Lax (layered; bearer path exempt by design) |
| Session fixation | server mints every token; sign-in/reset/privilege change mints fresh and revokes siblings |
| Enumeration (account exists, email registered) | identical 202/401 bodies; enqueue-vs-not hidden in the outbox; dummy-hash timing; no "which dead" detail on tokens |
| Token replay / cross-kind confusion | single-use `used_at`; kind column bound in the consume query; hashed at rest; TTLs; supersede-on-reissue |
| Invite hijack | accept requires session email == invite email; decline path exists; invite tokens 14 d, single-use |
| IDOR across workspaces | membership check on `x-durable-workspace`; foreign ids 404 (no oracle); matrix enforced at one choke point (`SessionAuth`) |
| Role escalation | one matrix; least-privilege defaults; owner row immutable; admin≠key; write key can't mint keys or secrets |
| SSRF via test call / registration | same `PolicyResolver` + literal checks as dispatch — the test call is built by `handler_factory.make_signed`; zero-packet assertion reused |
| Secret leakage | keys/secrets/tokens shown once, hashed at rest; `handler_secret` only at create/rotate; no secrets in logs (existing `skip_all`/`Debug`-redact patterns); tokens in fragments/POST bodies, never query strings on our side |
| Outbox amplification | per-address cap + signup rate limits + coalescing duplicates |
| Host-header reset-link poisoning | links built from configured `--public-origin` |
| Neon outage | grant/session caches + fail-closed unknowns; accepted runs unaffected (invariant) |
| Deleted-workspace resurrection | `deleted_at` filters every lookup; keys revoked write-through; admission refuses; queued runs cancelled |
| Write amplification (`last_used`, `last_seen`) | dirty-map flush 60 s (D-071) — measured loss window, not silent writes |
| Response-side u64 leak | `public_id`/pubid everywhere; S2's no-u64-leak scan extended to every new route's response |
| DoS via oversized names/inputs | existing byte caps; new text fields bounded (names ≤128, emails ≤254, bodies by `MAX_REQUEST_BYTES`) |
| Timing on token verify | sha256 of high-entropy token → unique-index lookup; digest compare constant-time; timing carries no usable signal |
| Login CSRF (victim logged in as attacker) | Origin/Sec-Fetch-Site + JSON-only bodies on unauthenticated `/auth` POSTs (review #17) — a cross-site form can no longer reach login at all |
| Pre-registration takeover (attacker signs up a stranger's email before they do) | verify token carries the pending password hash — the verify link's sign-up wins; verify/re-sign-up revoke all sessions; verify sets no cookie; invite accept needs a verified email (review #2) |
| Token forgery floods the store | self-authenticating `drs_`/`drk_` formats — tag failure dies before I/O (review #8) |
| Argon2 queue exhaustion | bounded semaphore + bounded waiters + ≤2 s wait → 503 `busy` + Retry-After; one job per address (review #9) |
| Stale cache resurrects revoked credential | generation-checked fill (review #10): invalidate-during-await drops the fill and denies |
| Header/path workspace confusion | path-only membership on `/workspaces/{pid}/*`; mismatch → 400, not silent merge (review #1) |
| Sequential ids crossing the API | `sessions.public_id`, `api_keys.public_id`, `invites.public_id` — u64 never crosses (review S3) |
| Bearer+cookie ambiguity | `Authorization` present → bearer-only resolution (review S3) |
| Open `/echo` reflector | none exists — `builtin:echo` is in-process, per-ws bounded (ruling 5') |
| Env key replay over the public edge | ops keys honoured on `--ops-listen` loopback only (ruling 2') |
| Mail as a spam/cost vector | fixed subjects, escaped bodies, strict addr-spec, per-(address,kind) + global budgets, outbox caps (review #14) |
| Neon growth without bound | purge job + bounded lifetimes on every table (review #15) |

**Explicitly out of scope for v1** (each leaves room, none blocks):
2FA/TOTP (session model already supports "privilege change → rotate";
the `accounts` row can gain a `totp_secret` later), SSO, payment/plan
purchase, recurring schedules, a public status page (Service Monitor's
job), audit log UI (the data exists in members/invites rows; exposing it
is a later slice), account deletion (workspace deletion is in; the
account row stays for members-integrity — a later lifecycle slice),
email change, per-workspace custom domains, multi-region anything.

---

## Slices, in build order

Each slice: outcome + the invariant it protects / **fails-first test** /
**attack proofs** (each listed attack test must fail with its guard
mutated in `prove-attacks.sh` — new `// ATTACK(name)` markers) /
**load or failure drill**. None ships without all of them (D-018).

| # | Slice | Outcome + invariant | Fails-first tests | Attack proofs | Load / drill |
|---|---|---|---|---|---|
| S3-0 | Session plumbing + sign-up + login | Migration `0002`; `AccountStore` methods for accounts/sessions; `Principal` enum (review #4) + `SessionAuth` + generation-checked session cache (#10); self-authenticating `drs_` tokens (#8); argon2id on `spawn_blocking` behind the **bounded** semaphore (#9); Origin/Sec-Fetch-Site on unauthenticated `/auth` POSTs (#17); signup hashes on every branch; `POST /auth/signup` (+token+outbox one tx), `POST /auth/login`, `POST /auth/logout`, `GET /auth/session`. *A full sign-up→login→session round-trip works end to end against the Memory store.* | sign-up → 202 + account row unverified; login → cookie + `{csrf}`; a request with the cookie resolves `{account}`; forged/expired/revoked token 401s **before** store I/O (tag check); cookie attributes asserted (Secure/HttpOnly/SameSite=Lax/Path=/ /no Domain); `busy` 503 + Retry-After when the argon2 queue saturates; `Cache-Control: no-store` + `Vary: Cookie` asserted | `session-forge` (random `drs_` value → 401, denied on tag before the store — mutation removes the tag check), `csrf` (mutation without/with wrong `x-durable-csrf` → 403), `csrf-open-post` (Origin/Sec-Fetch-Site refusal on unauthenticated `/auth` POSTs), `cookie-attrs` | session verify at battery rate on the cached path; Neon-down drill: cached sessions proceed, uncached → `accounts_unavailable` |
| S3-1 | Verify + reset + outbox worker + enumeration | `POST /auth/verify`, `/auth/resend-verification`, `/auth/forgot`, `/auth/reset`; `email_tokens`; outbox drain (bounded batch, backoff, `failed`); `Mailer` impls (`Memory`, `Log`, `Resend` wired at deploy); `DURABLE_INVITE_ONLY` | verify → verified; reused token → 410; reset-kind token refused at `/auth/verify`; forgot/reset round-trip; invite-only flag blocks sign-up without a token | `token-replay`, `token-kind-confusion`, `enumeration` (unknown vs known email → identical 202 + identical body; dummy-verify counter shows equal work), `invite-required` | outbox drain: 10k pending rows bounded-batch + backoff; mailer-down drill: retries then `failed`, never lost; Resend send deadline |
| S3-2 | Session lifecycle + /me | rotation on sign-in/privilege change, idle+absolute expiry, revoke-one + revoke-all, sessions list, `/me/password`; per-account login cool-off | reset revokes all sessions; the 21st session LRU-evicts the oldest-idle; idle-expired token → 401; cool-off shape after 10 failures | `session-fixation` (a pre-set cookie value never becomes a session; login mints fresh), `reset-token-replay`, `cooloff` (429 shape, not silence), `idle-timeout` (clock-injected expiry check) | login throughput vs the argon2 semaphore (memory bounded under concurrency — measured) |
| S3-3 | Workspaces + members + roles + invites | `POST /workspaces` (+ owner member row + `handler_secret` shown once), PATCH rename, DELETE soft (D-072), members via invites only, role changes, remove; the matrix enforced in `SessionAuth` for every member route | member of A on B's routes → 404; viewer mutation → 403; invite accept with mismatched session email → 403; deleted ws: keys dead immediately, admission 410, live runs cancelled | `idor-workspace`, `role-escalation` (viewer PATCH member; member DELETE ws; admin demotes owner), `invite-hijack`, `last-owner` (remove/demote owner → 409), `deleted-ws-key` (key of a deleted ws → 401 same request) | two-workspace membership churn vs cached lookups; delete drill: a live run is cancelled honestly, finished history untouched |
| S3-4 | Plans + usage + enforcement | `plans` + `plan_id`; per-ws submit bucket; store-side count limits; `GET …/usage`; the limit envelope | 11th key → 429 `plan_limit{"limit":"api_keys"}`; 61st submit in a minute → 429 + `Retry-After`; usage numbers == the plan row | `plan-bypass` (a second key/member of the same ws shares ONE bucket — the bucket is ws-keyed, not caller-keyed), `limit-message-shape` | submit battery through the plan bucket: 60/min admitted, excess refused honestly, other ws unaffected; `uptime` reported |
| S3-5 | Keys self-service | `GET/POST/DELETE/rotate …/keys`; `last_used` flush (D-071); `created_by` | create → `dr_` shown once; list shows prefix only; rotate → old key 401 on the next call; viewer POST → 403 | `key-self-escalation` (a write key cannot reach any `/workspaces/*` or `/auth/*` route), `cross-ws-key-create` (session on A POSTs a key under B's pid → 404), `last-used-amplification` (N verifies → ≤ 1 UPDATE per flush) | 1M submits through the cached key path with the flush task live — zero per-request Neon writes (counted) |
| S3-6 | Handlers mgmt + test call | session roles on `/handlers*`; `POST /handlers/{name}/test` (D-070); health columns on `GET /handlers`; `handler-secret` tightened to session admin+ (D-073) | test call to the fixture → `{status:"ok",latency_ms}`; private target → `refused`, zero packets; rotate → old signatures die | `ssrf-testcall` (private listener fixture asserts no packet on a refused test call), `secret-in-get` (response scan: no GET carries `handler_secret`), `testcall-abuse` (31st/hr → 429), `handler-secret-key` (write key → 403 — the S2 tightening) | test calls at the plan bound under a full submit battery — dispatch accounting asserted unaffected |
| S3-7 | Workflows + Run now | `workflows` + `workflow_versions` + `workflow_runs`; Run now → `submit_scoped` (D-069); migration `0003` | create/edit → version n+1, old versions immutable; run → `{run_id}` lands a journaled run visible in `/jobs`; spec with a foreign handler → `unregistered_handler` before the row exists | `idor-workflow` (B reads/runs A's workflow → 404), `runnow-opkey` (every Run now is a fresh uuidv7 key — a retried client click is a new run, honest), `viewer-runnow` → 403 | 1k Run-now submits inside the battery — admission cost == direct submit (measured, not assumed) |
| S3-8 | Dashboard | the IA above: signed-out pages, shell + switcher, all workspace pages, onboarding card, verify-wall, compat key-paste mode | page-contract specs red first per page; `GET /auth/session` bootstrap drives signed-in vs signed-out vs compat mode | `session-xss-surface` (CSP hash manifest still pins `index.html`; no `unsafe-inline` scripts), `bearer-csrf-exempt` (API clients unaffected by the CSRF layer) | `vite build` + `scripts/check.sh`; UI before/after screenshots 1440+390, light+dark, per the UI rule |
| S3-9 | The gate | SAAS_PLAN S3 exit gate end-to-end | the journey spec itself | whole matrix green on the merged build; `prove-attacks.sh` covers every new marker | `neon-sweep.sh` on 0002+0003 clean against real Neon; the Playwright journey |

**S3-9's Playwright journey** (the exit gate, from SAAS_PLAN — spec and
capture scripts land in S3-8; the coordinator runs them outside the
sandbox per the browser rule):

1. Sign up with a fresh address → verify (mail captured from the Log
   mailer / test outbox).
2. Register the `builtin:echo` sample handler → send test call → `ok`;
   then register a real fixture handler URL (the suite's own listener)
   → test call → `ok` (ruling 5': the journey exercises a real
   dispatch, not only the builtin).
3. Create an API key (scope write) → shown once.
4. Create workflow `demo` (step → the fixture handler) → **Run now** →
   land on the run's timeline → it finishes (steps, attempt, timestamps
   all rendered).

Plus negative journeys: wrong password, expired verify token, invite
accept by the wrong account, CSRF-blocked mutation (the journey asserts
the refusal, not just the happy path).

---

## Deploy notes

- Hostname: `durablerunnerapp.404human.dev` → Cloudflare →
  Caddy → durable-server on its loopback port (same-origin UI+API; one
  site file per product on the shared droplet). One dashboard build at
  `/app` (signed-in SaaS mode + compat key-paste mode); `/inspector`
  308-redirects to `/app` preserving the path tail and query.
- New env: `RESEND_API_KEY`, `DURABLE_MAIL_FROM` =
  `Durable Runner <durable-runner@404human.dev>` (ruling 7),
  `--public-origin https://durablerunnerapp.404human.dev` — **required
  at boot whenever an account store is configured** (review #18; absent
  it, the server refuses to start rather than mint Host-derived links),
  `DURABLE_INVITE_ONLY` (default off, env-at-boot only per ruling 9),
  `DURABLE_MAILER` (default resend when `RESEND_API_KEY` set; `log`
  allowed only with `--allow-local-handlers` or a dev flag — never
  silently in deploy).
- **`--ops-listen 127.0.0.1:PORT`** (ruling 2'): on a SaaS deployment
  the env ops keys are honoured only on this loopback listener, reached
  by SSH tunnel; proxied requests bearing an env key get 401
  (`unauthenticated`) on the public listener. With no account store
  (self-host), env keys keep working on the public listener exactly as
  v1.0.x. `/admin/*` lives only behind `OpsKey` either way.
- Mail is hardened (review #14): subjects are fixed server-side strings
  (only the workspace *name* is interpolated, length-capped), HTML
  bodies escape user data (`workspace`, names), `send_to` is a strict
  single addr-spec (no commas/display-name tricks — the parser, not a
  regex, validates), invites per workspace/day and per (address,kind)
  pending caps bound the outbox, and a global daily send budget trips a
  circuit breaker (alert via the log) before a mail loop can bill us.
- Response hygiene (review #18/S3): session-authed responses carry
  `Cache-Control: no-store`; `Vary: Cookie` joins `Vary:
  x-durable-workspace` on every authed route so a shared cache can
  never serve one tenant's body to another. `GET /auth/session` returns
  **404** (not 401) when no account store is configured — the
  dashboard's compat key-paste mode keys off exactly that status.
- Migrations 0002/0003 through `sqlx::migrate!` at startup as today;
  `scripts/deploy-readiness/neon-sweep.sh` must pass both on real Neon
  before the slices merge (no superuser constructs anywhere above).

---

## Rulings (2026-11-24)

The coordinator's answers to this plan's open questions, binding for the
build:

1. **D-numbers** — number from D-059 as planned; the coordinator
   renumbers at merge if main moves first. *(Renumbered twice 2026-11-24:
   +3 at the S2 merge for main's D-057–D-061, then +1 at the c95cdd6
   merge for main's D-062 — this plan's records are D-063–D-073.)*
2. **Hostname and mount** — `durablerunnerapp.404human.dev`. ONE
   dashboard build (signed-in SaaS mode + the compat key-paste mode for
   self-hosters) mounted at `/app`; `/inspector` answers 308 → `/app`
   (same path tail and query) so existing bookmarks keep working. No
   second UI.
3. **Key scopes** — boundary translation (`admin` stored, `write` on
   the API/UI). No data migration.
4. **Free-plan numbers** — the proposed table stands as the v1
   defaults, labelled proposals; S4 measurement confirms or changes
   them through a new `plans` row value, never a code change.
5. **`/echo` on the runner origin** — accepted, with: JSON in/out only,
   `Content-Type: application/json`, `X-Content-Type-Options: nosniff`,
   `Content-Security-Policy: default-src 'none'`, body ≤ 8 KiB,
   address-rate-limited as planned, and the signed-call header shape is
   required (`x-durable-timestamp` + `x-durable-signature` present;
   unsigned calls get `401 unsigned`, so it is not an open reflector —
   it does not need the workspace secret). Dispatch reaches it through
   the public origin like any customer handler: no SSRF exception, no
   loopback shortcut.
6. **Session-authed submits** — members may `POST /jobs` directly,
   behind the CSRF layer. Confirmed.
7. **Mail sender** — `Durable Runner <durable-runner@404human.dev>`.
8. **Resend idempotency** — check `Idempotency-Key` support at
   implementation (S3-1); the outbox row claim (`pending→sending`) is
   the dedupe either way.
9. **Invite-only** — env-at-boot only for v1.

### Amended rulings (2026-11-24, after the white-hat review)

- **Ruling 5′ (replaces 5): no public `/echo`.** The onboarding sample
  is the in-process builtin handler `builtin:echo` (no network hop,
  input echo bounded to the step cap, per-workspace rate-limited like
  every test call, labelled "sample" in the UI). The exit journey uses
  it *and* registers a real fixture handler URL.
- **Ruling 2′ (extends 2):** env ops keys are honoured only on the
  loopback `--ops-listen` listener on a SaaS deployment (SSH tunnel);
  a proxied request bearing an env key → `401`. The compat key-paste
  mode is offered only when `GET /auth/session` answers 404.

---

## White-hat review 2026-11-24

Independent review of this plan: 5 × S1, 13 × S2, plus S3 items. Every
finding is accepted unless ruled otherwise; each maps to the slice that
fixes it and the attack proof that must go red with its guard mutated.
Findings **5** (SSRF to the host's own public addresses) and **6**
(client address trusted behind Caddy) are fixed on the coordinator's
S2-onto-main branch — **the rate-limit and test-call sections here
depend on them**: every address-keyed bucket in this plan assumes
`client_addr` is the *untrusted* socket address unless the peer is a
configured trusted proxy, and every SSRF claim assumes the resolver
also refuses the host's own public IPs. Nothing in S3 re-litigates
those two; S3 slices inherit the coordinator's fix.

| # | Finding (sev) | Fixed in | Mechanism | Attack proof (must go red mutated) |
|---|---|---|---|---|
| 1 | Workspace confusion: header vs path (S1) | S3-3 | path-only `PathMember` on `/workspaces/{pid}/*`; header mismatch → `400 workspace_mismatch` | `ws-header-path-mismatch` |
| 2 | Pre-registration account takeover (S1) | S3-0/S3-1/S3-3 | pending `password_hash` on the verify token; verify & re-sign-up revoke all sessions; verify sets no cookie; invite accept requires verified email; unverified purge | `prereg-takeover`, `verify-sets-cookie` |
| 3 | Engine calls resolved outside the workspace scope (S1) | S3-3/S3-7 | every engine read goes through `scoped_run(ws, pubid)` — never raw `inspect`/`diag` | `cross-ws-inspect` (S3-3), `idor-workflow` (S3-7) |
| 4 | Session/key principal collapse + ws-0 store key reaches ops (S1) | S3-0/S3-3 | `Principal{OpsKey,OpsRead,StoreKey{ws,Write|Read},Member}`; `ops_admin` = OpsKey only; `CHECK(workspace_id<>0)` | `ops-storekey`, `member-ws0` |
| 5 | SSRF to the host's own public addresses (S1) | **coordinator's S2-merge branch** | resolver refuses own public IPs | inherited (`ssrf-*` proofs) |
| 6 | Client address trusted behind Caddy (S1) | **coordinator's S2-merge branch** | trusted-proxy list; else socket addr | inherited (`xff-spoof`) |
| 7 | Plan capacity bounds absent (S2) | S3-4 | `retained_bytes` share; one `admit()` for `/jobs` + Run now; soft-deleted ws counted | `plan-capacity`, `runnow-bypass` |
| 8 | Token forgery reaches the store (S2) | S3-0 (`drs_`), S3-5 (`drk_`) | self-authenticating tag checked before I/O | `session-forge`, `key-forge` |
| 9 | Argon2 queue unbounded (S2) | S3-0 | permit before `spawn_blocking`; ≤32 waiters ≤2 s → 503 `busy` + Retry-After; 1/address | `argon2-flood` |
| 10 | Cache fill races invalidation (S2) | S3-0 (session/keyring), S3-3 (membership) | generation bump on every invalidate; fill dropped if bumped mid-await; hit checks staleness | `stale-session-cache`, `stale-key-cache` |
| 11 | `csrf_seed` default ⇒ computable token (S2) | S3-0 | `NOT NULL CHECK(octet_length=32)`, no default; minted at insert | `csrf-seed-zero` |
| 12 | Session ids sequential in API (S2) | S3-0 | `sessions.public_id` exposed, u64 never crosses | covered by no-u64-leak scan |
| 13 | Lockout leaks account existence (S2) | S3-2 | counters keyed by normalized email whether or not the account exists; per-(email, client-prefix) cool-off; known-device bypass | `lockout-oracle` |
| 14 | Mail: template injection + spend amplification (S2) | S3-1 | fixed subjects, escaped bodies, strict single addr-spec, invite caps, global daily budget + breaker | `mail-budget`, `addr-spec-injection` |
| 15 | Neon tables grow without bound (S2) | S3-4/S3-7 | purge job + bounded lifetimes/versions | `purge` drill (bounded-row assertion) |
| 16 | `/echo` = open reflector (S2) | removed (ruling 5′) | `builtin:echo`, in-process, per-ws bucket | moot — no route exists |
| 17 | CSRF gaps: unauthenticated posts + `same-site` + verify-sets-cookie (S2) | S3-0 | Origin/Sec-Fetch-Site on all unauth `/auth` POSTs; `same-site` refused; JSON-only bodies; verify never sets a cookie | `csrf-open-post`, `csrf-samesite`, `verify-sets-cookie` |
| 18 | Deployment leaks ops surface (S2) | S3-3 (ops-listen flag), S3-9 (deploy) | `--ops-listen` loopback; `--public-origin` required with an account store; env key on public edge → 401 | `ops-key-public-edge` |

S3 items folded in above: `api_keys.public_id` (S3-5 list/get by
public_id), bearer-precedence when `Authorization` is present (S3-0),
signup hashing on every branch + forgot/resend work after the 202
(S3-0/S3-1), Run-now idempotency key derived from a client-supplied
`Idempotency-Key` (S3-7), `Cache-Control: no-store`/`Vary: Cookie`
(S3-0), `GET /auth/session` → 404 without a store (S3-0), and the
`Vary` on every authed route (S3-8's header assertion).
