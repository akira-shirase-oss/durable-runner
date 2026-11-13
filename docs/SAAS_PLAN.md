# Durable Runner as a SaaS — plan

Status: **plan, not built.** Written 2026-11-13. Nothing here is deployed yet; numbers measured on the droplet do
not exist yet and are marked *unmeasured*.

## The goal

Durable Runner runs as its own hosted product: a person signs up, gets a workspace and an API key, registers the
HTTP endpoints that do their work, submits jobs, and watches every step, attempt and retry in a dashboard. An
accepted job is a durable commit. The first users are the maintainer's friends; payments are bypassed.

Decisions already made (2026-11-13, by the maintainer):

- **Independent product** with its own repository, accounts, login, dashboard and droplet. It meets Colophon and
  Service Monitor only through public APIs and tokens.
- **One 2 GB DigitalOcean droplet.**
- **Neon Postgres** for account records; the journal stays the authority for jobs (see "Data").
- **Landing page content is edited in Colophon** and served as a static site.
- **Payments are bypassed:** a free plan with limits.

## Where Durable Runner is today

- No authentication: the server listens on localhost (`--listen 127.0.0.1:58300`, `docs/OPERATING.md`).
- Handlers are a static allowlist given as flags at startup (`--handler step=http://…`, D-003, D-008).
- API: `/jobs`, `/jobs/{id}`, `/jobs/{id}/cancel`, `/health`, `/admin/backup`, `/admin/compact`, `/admin/diagnostics`.
- Data: one framed append-only journal with one writer is the only authority (D-001/D-006); the in-memory index is
  rebuilt, never authoritative. No database queue, no Redis.
- UI: a one-page SvelteKit inspector (`apps/inspector`).
- Out of scope today (`docs/END_GOAL.md`): recurring schedules, fan-out, multi-host failover.
- Readiness: D0–D3 proven; **the latency-gated load battery is red.** The 2026-11-11 solo re-run disproved the
  contention theory; the cause is still open.

## The gaps between "my runner" and "SaaS"

| Gap | What has to change |
|---|---|
| Latency gate | Find and fix the cause of the red gate before anyone else depends on the product. This comes first. |
| Network exposure | Public listener behind Caddy/TLS; nothing reachable without a credential. |
| Accounts | Signup (invite-code gated for the beta), email verification, sign-in, password reset. |
| Workspaces | Every job, step and handler belongs to a workspace; every read, cancel and admin action is scoped by it. Members and roles. |
| API keys | Per-workspace keys for submitting and reading jobs (Colophon uses one). |
| Handler registry per workspace | Workspaces register their own handler URLs at runtime instead of startup flags, keeping admission-time checks (D-008). Outbound calls get an SSRF guard (public addresses only, DNS answers validated, redirects revalidated) and an HMAC signature so handlers can verify the call came from the workspace's runner. |
| Limits | Free plan: jobs per minute, jobs in flight, handlers, payload size (1 KiB inputs are the measured case), retention. Per-workspace fairness so one tenant cannot starve the single writer. |
| Recurring schedules | Needed for nightly jobs (a Colophon ledger export, a site rebuild). Not in the current contract; it needs a new `D-NNN` decision and its own failure cases (missed ticks across restarts, no duplicate tick). Until then, an external cron submits jobs. |
| Dashboard | Rebuilt on a copy of Colophon's `@colophon/ui` (Tailwind v4 + shadcn-svelte), which this repo then owns: shell, auth forms, onboarding, settings (members, API keys, handlers), job list with filters, job detail timeline (steps, attempts, retries), submit/cancel/retry. Own accent and logo. |
| Onboarding | Sign up → create workspace → register a handler (with a "test call" button) → submit a first job from the UI or with a copied `curl` → watch it finish. |
| Account lifecycle | Delete workspace (its journal records are compacted away), export job history. |

## Data

- **The journal on the droplet** (on a DigitalOcean volume) remains the only authority for jobs, now with a
  workspace identifier in every record. Moving jobs into Postgres would replace the product's core guarantee.
- **Neon** holds accounts, workspaces, members, API keys, handler registrations and plan limits.
- If Neon is unreachable, accepted jobs keep running; new submissions fail closed when a key cannot be verified
  (or are served from a short-lived verified-key cache, if that is decided and tested).
- Backups: `/admin/backup` and the restore drill already exist; schedule them and copy off the droplet.

## Droplet layout (one 2 GB droplet)

```
Caddy (TLS)  →  durable-server (API + dashboard + engine, one journal writer)
                 ├── journal on a DigitalOcean volume
                 └── Neon Postgres (accounts, keys, handler registry)
```

Memory and throughput are *unmeasured* on the droplet; plan limits come from those measurements.

## Integrations (public API only)

- **Colophon:** a Colophon user adds a Durable Runner API key; Colophon's publish webhook submits post-publish jobs
  (static export, cache purge). Colophon's scheduled publishing stays inside Colophon (its D-060).
- **Service Monitor:** monitors `/health` from outside, like any customer's monitor.

## Phases and exit gates

| Phase | Outcome | Exit gate (evidence) |
|---|---|---|
| S0 Latency gate | The red load gate understood and fixed or re-scoped by a recorded decision. | The latency battery passes, committed under `perf/results/`. |
| S1 Deploy as-is | Behind Caddy with a single API key, used by the maintainer's own Colophon. | Kill-9 + restore drill on the droplet; idle memory recorded. |
| S2 Accounts + workspaces | Signup, workspaces, API keys, per-workspace handler registry with SSRF guard and signed calls. | Isolation tests: workspace A can never see, cancel or run handlers for workspace B; SSRF tests refuse private targets. |
| S3 Dashboard + onboarding | The rebuilt dashboard on the copied UI and the onboarding journey. | Playwright journey: signup → register handler → submit → finished job timeline. |
| S4 Limits + fairness | Plan limits and per-workspace fairness at the writer. | Noisy-tenant run: one workspace at its limit does not move another's submit latency past the threshold. |
| S5 Load on the droplet | ≥1M submits across workspaces, generated from another machine, plus the crash cases. | Results committed with droplet spec and commit hash. |
| S6 Friends beta | Invite codes go out. | Runbook exercised: restore, workspace deletion, handler outage. |

Recurring schedules are a separate milestone after S6 unless the maintainer moves them earlier.

## Open questions

1. What causes the red latency gate (S0)?
2. Recurring schedules: build them, or rely on external cron for the beta?
3. Handler registry: are handlers limited to HTTPS URLs only?
4. Free-plan limits: jobs per minute, in-flight jobs, retention?
