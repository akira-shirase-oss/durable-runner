# Next milestone slice — S3: dashboard + onboarding

Written 2026-11-21 on `devin/dr-s2` (post-S2, pending the re-review and the
ae7f5a9 battery). Sources: `docs/SAAS_PLAN.md` phases, `docs/S2-PLAN.md`
§"What S3 consumes", `docs/END_GOAL.md`, `docs/ROADMAP.md` (D0–D4 done).
Planning only — nothing here is built.

## Why this slice

ROADMAP ends at D4 and END_GOAL's scope is proven; the SaaS plan's phase
table makes S3 ("dashboard + onboarding") the next gate, and S2 deliberately
reserved everything it needs: `sessions`/`members`/`accounts.password_hash`
in migration 0001, `plan_limits` JSONB, the `/v1/` API contract listing.
S4 (plan-limit enforcement + noisy-tenant gate) and S5 (droplet 1M) follow.

## Scope

A person signs up (invite-code gated), signs in, gets a workspace, registers
a handler with a test call, submits a job from the dashboard, and watches it
finish — the SAAS_PLAN S3 exit journey. Sub-slices, each with failing-first
test + failure case + load piece per the testing rule:

| # | Slice | Outcome & invariant |
|---|---|---|
| S3-0 | Sessions + auth | `POST /v1/signup|login|logout`. Opaque session token, `token_hash` at rest (never the raw token), bounded lifetime, `Set-Cookie` HttpOnly+Secure+SameSite. Verify path is cached like the S2-1 keyring — Neon is never in the request path past cache fill. |
| S3-1 | Members + tenant routes | `/v1/workspaces` self-service create/list; `GET/PATCH`/`members` CRUD with `admin`/`member` roles; keys + handlers move from ops-scoped `/admin/...` to caller-scoped `/v1/workspaces/{public_id}/...`. `forbidden` when authed but wrong scope; another tenant still sees 404. |
| S3-2 | Handler test call | `POST /v1/workspaces/{public_id}/handlers/{name}/test` runs the same SSRF-resolve + signature path as a dispatch, reported pass/fail honestly. |
| S3-3 | Dashboard shell | `apps/dashboard` on its own design system (NOT Colophon's — own palette/type, maintainer chooses from options): auth forms, onboarding steps, settings (members, keys, handlers + test call). |
| S3-4 | Job views | List with filters, detail timeline (steps/attempts/retries), submit/cancel — consuming the scoped API, no endpoint the UI needs that the API lacks. |
| S3-5 | The gate | Playwright journey signup→handler→submit→finished timeline, plus the isolation suite re-run under session auth. |

## Failure cases and attack classes → mutation proof

Each lands in `prove-limits`-style mutation proofs; a test that can't go red
is itself the bug (twice proven this week).

| Attack/failure | Test asserts | Mutant that must go red |
|---|---|---|
| Stolen/expired session token | 401 past expiry; token rejected after logout | expiry check removed; logout not deleting the row |
| Raw token at rest (DB leak) | `sessions` row never contains the raw token | store the raw token |
| Session fixation | signup/login mints a fresh token; a planted token is not honored | accept caller-supplied token |
| CSRF on cookie routes | cross-origin POST without the bearer/origin check → refused | SameSite/origin check removed |
| Credential stuffing / user enumeration | per-address login bound; identical response shape and timing class for unknown user vs bad password | limiter bypassed; early-return on unknown account |
| Password at weak cost / in logs | argon2id (decision needed) params; no password in any log line | downgrade to fast hash / log the body |
| Role bypass | `member` → admin routes = `forbidden`; read key on write = 403/401 | permission check removed |
| Cross-tenant via members | invite/add/role-change only in own workspace; member of A sees 404 on B | ws scope check removed from member routes |
| `/test` as SSRF oracle | test call still refuses private targets; result leaks nothing past pass/fail + status | skip the guard for test calls |
| `/test` as signature oracle | test call carries a real signature; wrong-secret fixture verifier still rejects | sign with a fixed/test secret |
| Dashboard token handling | session lives in the cookie only; no token in localStorage/URLs; strict CSP like the inspector's (D-053) | weaker CSP / token in JS-readable store |
| Neon outage mid-session | cached session still verifies; uncached → `503 accounts_unavailable`; never fail-open to another user | fail-open on store error |
| Cookie flags | HttpOnly+Secure+SameSite asserted on `Set-Cookie` | drop a flag |

## 1M load scenario

New `sessioned` scenario in `perf/load.mjs`, added to the `rest` leg:
1M authenticated requests through the session-verify path against the memory
store — measures the verify cache hit rate, bounds the miss flood under an
injected Neon outage (the S2-1 negative-cache pattern), and asserts p99 <
2000 ms like the other legs. Login-rate cost (hashing ~ms/attempt) is
measured separately at small scale; the bound is per-address, asserted, not
assumed.

## Risks

- **Session cookies add CSRF** — the API is bearer-only today; the cookie
  surface needs its own origin/SameSite proof, not reuse of the key path.
- **Hashing cost vs login throughput** — a memory-hard hash at unbounded
  rate is a CPU DoS; the per-address bound must land with the route.
- **Session verify in the request path** — must reuse the verified-cache
  pattern or Neon becomes a hard dependency of every dashboard request
  (violates the S2 "store never in the commit path" spirit).
- **Roles vs key permissions** — member roles and key read/write perms are
  parallel concepts; conflating them is a privilege-escalation hole. One
  decision entry, one enforcement point.
- **Design system is a maintainer decision** — options + screenshots first,
  per the workspace rule; building before that choice is wasted work.
- **Migration discipline** — if S3 needs columns beyond reserved ones, new
  migration files only (the checksum rule); never edit 0001.

## END_GOAL claims the branch hasn't proven yet

1. **Ship gate, second half** — "a second developer can run, inspect, back
   up and restore a noncritical workflow": the deploy package and
   rehearsals exist (`docs/DEPLOY.md`, D-055) and the pilot + restore
   drill are proven locally, but the droplet column is empty — no
   install/upgrade/restore rehearsal on the actual host, no idle-memory
   number (SAAS_PLAN S1 gate).
2. **The million-scale battery, green** — dispatched on ae7f5a9 (run
   44881585860, four legs) but not yet landed; the prior `sustained` leg
   kept a RUNNER-classified miss at the GitHub runner's ~85 completions/s
   ceiling. A green 1M sustained on any host is still unproven; results
   aren't committed.
3. **"Finished history expires on a schedule"** — the expiry mechanism is
   proven, but scheduled compaction is deliberately off by default
   (D-018: on only once cheap + fatal-error path, per D-021/D-019). The
   contract is met opt-in, not by default — worth saying plainly.
4. **"Bounded diagnostic logs"** — diagnostics *counters* are bounded
   (state histograms, O(kinds)); process logging is `eprintln!` to stderr
   with no rate bound or rotation story of its own. Probably fine under
   journald, but no bound is proven or documented.
5. **Restart-to-ready on the droplet** — DEPLOY.md's table has the laptop
   column (800k → 1.716 s); the droplet column the deploy gate measures
   is unfilled.
6. **Capacity on the droplet** — capacity planning says the ceiling is
   "measured at deploy, not assumed"; unmeasured so far.
7. **Neon under real failure** — pg impl tests run on a real Neon CI job
   behind a secret (`neon.yml`), but the multi-day outage/partition
   behavior (cache TTL edges, recovery refresh) is tested against the
   memory store's fault injection, not observed against Neon.
8. **UI covering every operation** — the inspector covers the v1 API;
   the member/key/handler-test surface the SaaS needs is S3 scope.
9. **Docs site + docs-native changelog** — the workspace-wide
   requirement (Astro Starlight site, `/changelog` inside it, examples
   run in CI) is unbuilt; `CHANGELOG.md` is a file, not the site.

## What this plan does not cover

S4 plan-limit enforcement (`plan_limits` is reserved but unread), S5
droplet load, S6 beta, recurring schedules — each its own contract later.
The END_GOAL "out of scope" list stands: no fan-out, no multi-host, no
jsrt adapter.
