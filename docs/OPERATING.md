# Operating a durable-runner server

The second-developer path: from a fresh checkout to running, inspecting,
backing up, and restoring a workflow. `examples/pilot/run.sh` performs
this entire document as one script — it is the ship-gate rehearsal.

## Build

```sh
export PATH="$(brew --prefix rustup)/bin:$PATH"   # the pinned toolchain
cargo build --release -p durable-server -p durable-cli
cd apps/inspector && npm install && npm run build  # the UI (optional)
```

## Run

```sh
export DURABLE_API_KEY=$(openssl rand -hex 32)   # >= 32 chars; keep it in your secret store
target/release/durable-server \
  --journal data/journal \
  --listen 127.0.0.1:58300 \
  --ui-dir apps/inspector/build \
  --backup-root data/backups \
  --handler step=http://127.0.0.1:58320/step
```

- **Reserve.** `--reserve-bytes N` (default 64 MiB) must stay free on the
  journal filesystem; see "What honest looks like" for the 503s.
- **API key.** The server refuses to start without `DURABLE_API_KEY` (read
  from the environment, never argv). Every mutating and admin route —
  `POST /jobs`, `POST /jobs/{id}/cancel`, `POST /admin/compact`,
  `POST /admin/backup`, `GET /admin/diagnostics` — needs
  `Authorization: Bearer $DURABLE_API_KEY`, else `401 {"code":"unauthorized"}`.
  Reads (`/health`, `GET /jobs`, `GET /jobs/{id}`, the inspector) are open.
  The CLI sends the same variable; the inspector asks for it before cancel.
  The pilot generates a fresh random key per run and exports it.
- **Backup root.** `--backup-root DIR` is the only place backups are
  written; without it `/admin/backup` answers `403 backups_disabled`.

- `--handler name=URL` is the allowlist — repeatable; a submit naming a
  handler outside it is refused 409 with `unregistered handler`.
- The journal directory is created on first run: `LOCK`, `manifest.json`,
  `gen-00000N/{journal.log,snapshot.json}`. A second server on the same
  directory **fails startup** — the exclusive writer lock is held.
- The inspector is at `http://127.0.0.1:58300/inspector/`.

## Submit, inspect, cancel (the CLI)

```sh
CLI="target/release/durable-cli --server http://127.0.0.1:58300"
$CLI submit --op-key pilot/onboarding-email \
      --step prepare=step --step deliver=step \
      --input '{"user":"alice"}'
$CLI inspect --run 1
$CLI cancel --run 1
$CLI compact
$CLI backup --name nightly-1          # -> data/backups/nightly-1
```

A backup name is a plain name (`[A-Za-z0-9_.-]`, up to 64, not starting
with `.`): the server writes only `<backup-root>/<name>` and refuses an
existing name rather than merge into it.

`--op-key` is the dedup contract: resubmitting the same key returns the
same run id, nothing re-executes. Cancel stops scheduling — it never
undoes a remote effect already attempted.

## Restore

```sh
# server on the backup must use its own journal dir — restore into a dir:
target/release/durable-cli restore --from data/backups/nightly-1 --to data/restored
target/release/durable-server --journal data/restored --listen 127.0.0.1:58301 \
  --handler step=http://127.0.0.1:58320/step
```

The restored server replays snapshot + journal and rebuilds identical
state — same run ids, same `finished_at`. Verified live by
`examples/pilot/run.sh`.

## What "honest" looks like in the output

- `queued` / `retry_waiting:*` — scheduled, not yet attempted.
- `running:step:N:attempt:M` — an attempt is in flight with a deadline.
- `reconciling:step:N` — a reply vanished; the probe is deciding whether
  the remote effect landed before any retry.
- `done` / `failed:step:N` / `cancelled` — terminal; expires from the
  index after seven days (dedup deps preserved until then).
- HTTP 409 — a named refusal (duplicate semantics aside: stale reply,
  terminal transition, unregistered handler, an existing backup name).
- HTTP 401 — no or wrong API key on a mutating/admin route.
- HTTP 503 — the server is bounded and said so instead of queueing
  silently. The `code` says which bound: `busy` (the command queue is
  full — retry with the same `op_key`), `no_space` (free disk is below
  `--reserve-bytes`, default 64 MiB — nothing was written), or
  `journal_unavailable` (a journal write failed and was rolled back, or
  the writer is poisoned and the process is about to exit).
- `GET /ready` — readiness, separate from `/health` (liveness): `503
  {"ready":false,"code":"no_space"}` while free space is below the
  reserve, `engine_stopped` once the engine has stopped. Point the load
  balancer or supervisor's readiness check here.
- Full disk: submits get `503 no_space`; compaction and backup refuse
  unless what they would write fits above the reserve; dispatch pauses
  (a call whose outcome cannot be journaled is not started). An outcome
  that arrives while the disk is full is counted in
  `GET /admin/diagnostics` (`errors.no_space`) and the run reconciles
  after its deadline once space returns.
- Process exit with `engine stopped: journal writer poisoned` — a journal
  fsync failed. It is never retried; run the server under a supervisor
  that restarts on failure (systemd `Restart=on-failure`) and startup
  replay recovers exactly the durable history.
