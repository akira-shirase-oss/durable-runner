# Operating a durable-runner server

The second-developer path: from a fresh checkout to running, inspecting,
backing up, and restoring a workflow. `examples/pilot/run.sh` performs
this entire document as one script — it is the ship-gate rehearsal. It
checks every step, exits non-zero on the first that fails, and takes its
ports from `DURABLE_PILOT_PORT` (default 58330; the restored server uses
that + 1, the pilot handler that - 10). `scripts/check.sh` is the whole
gate, the same one CI runs (D-041).

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
- **Capacity.** `--max-live-runs` (100,000 non-terminal runs) and
  `--max-index-bytes` (512 MiB of retained runs) bound the index; past them
  a submit is `503 at_capacity` (nothing journaled; retry with the same
  `op_key`). `--max-in-flight` (32 handler calls) and
  `--max-dispatch-per-tick` (64 per 50 ms tick) bound execution; raise
  them if dispatch is the bottleneck. `GET /admin/diagnostics` shows
  `active_runs` and `index_bytes`.

  *Memory sizing (D-031).* Plan for a peak RSS of about
  `2.6 × max_index_bytes + 100 MiB`. Measured, restart and steady state
  run at 1.1–1.6 × the index; a compaction may add up to one snapshot's
  size, as the review saw on the HTTP server. The 512 MiB default keeps
  that within 1.5 GiB of a 2 GB host. On a bigger host raise
  `--max-index-bytes` in proportion, and leave the OS at least 0.5 GiB.

  *Capacity arithmetic.* A run is charged `160 + key + Σ(48 + step name +
  handler name) + input` bytes while it is live. When it finishes its
  input is dropped at once and it becomes a dedup record (key, state,
  step count, finish time) charged `160 + key + Σ(48 + names)`. For a
  one-step job with a 20-byte key and a 1 KiB input that is about
  1,250 B live and 230 B finished. At the 512 MiB default that is about
  430,000 such runs live at once, or about 2.3 million finished records.
  A record stays until a compaction runs at least seven days after its run
  finished (compaction is opt-in, `--compact-every-secs`). So at a
  sustained R jobs/s, finished records need about `R × 604,800 s × 230 B`
  of `--max-index-bytes` on top of the live runs. At the default and with
  scheduled compaction, the seven-day dedup window sustains about 3.9
  such jobs/s on a 2 GB host; more needs a bigger host. Without scheduled
  compaction the records never leave, and `503 at_capacity` comes once
  about `max_index_bytes ÷ 230 B` jobs have run.
- **Retention.** Compaction runs only when asked (`durable compact`) unless
  you pass `--compact-every-secs N` (default 0 = off; see D-018 for why it
  is opt-in). Each compaction removes the dedup records of runs that
  finished more than seven days ago (an `op_key` reused after that
  re-executes, and their capacity returns) and deletes every superseded
  `gen-*` directory, so the journal directory holds one generation. `GET /admin/diagnostics` reports
  `stale_generations` (0 unless a delete failed; the next compaction
  retries it). A compaction whose snapshot would exceed
  `--max-snapshot-bytes` (default 2 GiB) is refused `409
  snapshot_over_budget` before anything is written.
- **Compaction stalls acks for as long as it writes.** The snapshot is
  written by the journal writer itself, so appends queue behind it: every
  submit, cancel and state change waits until the snapshot is written
  and fsynced. The stall grows with the snapshot (about the live inputs
  plus about 230 B per finished run). Measured on the development laptop
  (not capacity): about 100 MiB of snapshot (100k live 1 KiB runs) took
  0.10–0.35 s, and the longest submit during it waited 82–364 ms; about
  820 MiB (800k runs) took 1.03 s, with a 934 ms submit. Schedule
  compaction off-peak, or keep it manual, until the snapshot is written
  on a helper thread (D-021, not built). A damaged
  `snapshot.bin` stops startup (`snapshot is damaged`): restore from a
  backup.
- **API key.** The server refuses to start without `DURABLE_API_KEY` (read
  from the environment, never argv, at least 32 characters). Every
  mutating and admin route — `POST /jobs`, `POST /jobs/{id}/cancel`,
  `POST /admin/compact`, `POST /admin/backup`, `GET /admin/diagnostics` —
  needs `Authorization: Bearer $DURABLE_API_KEY`, else `401
  {"code":"unauthorized"}`.
- **Reads need a key too** (D-037): `GET /jobs` and `GET /jobs/{id}` take
  the API key or the optional read-only `DURABLE_READ_KEY` (at least 32
  characters, different from the API key; it gets `403 forbidden` on
  every other route). Give the inspector or a dashboard the read key, not
  the admin key. `--open-reads` serves reads without a key, and is
  refused at startup unless `--listen` is a loopback IP: never pass it
  behind a reverse proxy, which makes loopback reachable. `/health`,
  `/ready` and the inspector's static files are open. The CLI sends
  `DURABLE_API_KEY` (reads fall back to `DURABLE_READ_KEY`); the
  inspector asks for a key and sends it on every call. The pilot
  generates a fresh random key per run and exports it.
- **Backup root.** `--backup-root DIR` is the only place backups are
  written; without it `/admin/backup` answers `403 backups_disabled`.

- **Stopping.** Send SIGTERM (or Ctrl-C). The server stops accepting,
  lets requests in flight finish, makes everything it already decided
  durable, releases the journal and exits 0; it exits non-zero if that
  takes longer than `--drain-timeout-secs` (default 30, applied to the
  requests and then to the engine), and a restart replays. Handler calls
  in flight are abandoned and reconcile after the restart. Give the
  supervisor at least twice the drain timeout (systemd
  `TimeoutStopSec=70`).
- `--handler name=URL` is the allowlist — repeatable, `http://` or
  `https://` only; a submit naming a handler outside it is refused 409
  with `unregistered handler`. Calls go only to that URL: redirects are
  not followed (a 3xx fails the attempt) and the proxy environment
  variables are ignored (D-039).
- The journal directory is created on first run: `LOCK`, `manifest.json`,
  `gen-00000N/{journal.log,snapshot.bin}`, owner-only (directories 0700,
  files 0600; backups and restores too, D-038). Run the server as the
  user that owns them. A second server on the same
  directory **fails startup** — the exclusive writer lock is held.
- The inspector is at `http://127.0.0.1:58300/inspector/`: newest runs
  first, 50 per page, with newer/older controls.

## Submit, inspect, cancel (the CLI)

```sh
CLI="target/release/durable-cli --server http://127.0.0.1:58300"
$CLI submit --op-key pilot/onboarding-email \
      --step prepare=step --step deliver=step \
      --input '{"user":"alice"}'
$CLI inspect --run 1
$CLI list --limit 50                  # one page; continue with --after <next_after>
$CLI list --newest --limit 50         # newest first; continue with --before <next_before>
$CLI cancel --run 1
$CLI compact
$CLI backup --name nightly-1          # -> data/backups/nightly-1
```

Bounds a submit must respect (refused `409 {"code":"over_limits"}`
otherwise, before anything is journaled): `op_key` 1-256 bytes without
control characters, step and handler names 1-128 bytes, input 64 KiB, at
most 64 steps; the whole request body 256 KiB (`413`). A handler reply
larger than 256 KiB fails the attempt (it is read only up to the limit).
The key bound applies to new keys: resubmitting a key that already has a
run returns that run even if the key is longer than today's bound (it was
accepted under an earlier one).

A backup name is a plain name (`[A-Za-z0-9_.-]`, up to 64, not starting
with `.`): the server writes only `<backup-root>/<name>` and refuses an
existing name rather than merge into it. A backup appears under its name
only once complete; an interrupted one is a hidden `.<name>.partial` that
the next attempt or the next server start removes.

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

`restore` takes the target's journal lock for the whole copy, so it
fails while a server runs on that directory, and no server can start on
it half restored. It refuses a target that already holds a journal
(move a damaged one aside first; it never merges two histories), and it
copies only the backup's own regular files, refusing symlinks. An
interrupted restore leaves generations without a manifest; a server
refuses to start there, so remove the directory and restore again.

## What "honest" looks like in the output

- `queued` / `retry_waiting:*` — scheduled, not yet attempted.
- `running:step:N:attempt:M` — an attempt is in flight with a deadline.
- `reconciling:step:N` — a reply vanished; the probe is deciding whether
  the remote effect landed before any retry.
- `done` / `failed:step:N` / `cancelled` — terminal. The run's input is
  dropped at once and a small dedup record stays (resubmitting its
  `op_key` returns this run). The record leaves the index when a
  compaction runs at least seven days later; nothing expires without a
  compaction.
- HTTP 409 — a named refusal (duplicate semantics aside: stale reply,
  terminal transition, unregistered handler, an existing backup name,
  `compaction_running` when a compaction is requested while one runs).
- HTTP 401 — no or wrong API key on a mutating/admin route.
- HTTP 503 — the server is bounded and said so instead of queueing
  silently. The `code` says which bound: `busy` (the command queue is
  full — retry with the same `op_key`), `at_capacity` (the live-run or
  index-byte bound), `no_space` (free disk is below
  `--reserve-bytes`, default 64 MiB, or a real ENOSPC — nothing was
  written), `unavailable` (free space could not be read), or
  `journal_unavailable` (a journal write failed and was rolled back, or
  the writer is poisoned and the process is about to exit), or `timeout`
  (the engine did not answer within `--reply-timeout-ms`, default 10 s,
  e.g. a stuck disk; a submit may still commit, so retry with the same
  `op_key`).
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
  write or fsync failed, or a compaction failed at its manifest swap. It is never retried: every request waiting on
  that group commit (and after it) got `503 journal_unavailable`, nothing
  of it was acknowledged. Run the server under a supervisor that restarts
  on failure (systemd `Restart=on-failure`) and startup replay recovers
  exactly the durable history.
- Startup refused with `corrupt committed history at seq N: … (frame at
  offset X)` — a committed frame is damaged (a header or payload
  checksum, or a frame that does not fit the batch it was written in).
  Nothing was cut: the log is exactly as found. Restore from a backup.
  If the message ends `the damage lies inside the log's last batch` and
  the host lost power, the damaged frames were in a batch whose fsync
  had not returned, so none of them was acknowledged: copy the journal
  aside, then truncate `journal.log` to offset X and start again.
- `durable-server: discarded a torn tail: N bytes at offset X` at startup
  — the log ended inside its last frame (a crash mid-write). That frame
  was never acknowledged; every complete frame before it was kept.
