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
  --handler step=http://127.0.0.1:58320/step \
  --allow-local-handlers   # loopback handler — dev only
```

- **Reserve.** `--reserve-bytes N` (default 64 MiB) must stay free on the
  journal filesystem; see "What honest looks like" for the 503s.
- **Capacity.** `--max-live-runs` (100,000 non-terminal runs) and
  `--max-index-bytes` (512 MiB of retained runs) bound the index; past them
  a submit is `503 at_capacity` (nothing journaled; retry with the same
  `op_key`). `--max-in-flight` (256 handler calls) and
  `--max-dispatch-per-tick` (512 per 50 ms tick) bound execution — sized
  after the S2-7 drain measurement (32/64 held ~640–1,300 runs/s against a
  fast handler; 256/512 reached ~5k/s; the journal itself sustains ~33k/s).
  `GET /admin/diagnostics` shows `active_runs` and `index_bytes`.

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
  refused at startup unless `--listen` is a loopback IP, and even then
  answers a request without a key only if its `Host` is `localhost` or a
  loopback IP (DNS rebinding, D-047): never pass it behind a reverse
  proxy, which makes loopback reachable. `/health`,
  `/ready` and the inspector's static files are open. The CLI sends
  `DURABLE_API_KEY` (reads fall back to `DURABLE_READ_KEY`); the
  inspector asks for a key and sends it on every call. The pilot
  generates a fresh random key per run and exports it.
- **Account store (S2).** `DATABASE_URL` pointing at Postgres (Neon in
  production, `sslmode=verify-full` required there) enables the account
  graph: store-issued API keys verify through a 60 s positive / 5 s
  negative cache (bounded at 10k entries), and a submit under such a key
  lands in the key's workspace — dedup keys on `(workspace, op_key)`.
  Unset, the server runs exactly as before: the env keys are the only
  keys and everything is workspace 0. The pool is lazy and migrations
  run best-effort at startup, so a Neon outage never stops startup:
  cached keys keep authenticating inside the TTL, uncached bearers fail
  closed as `503 accounts_unavailable`, and already-accepted runs keep
  executing — the store is never on the commit or dispatch path.
  `DURABLE_ACCOUNTS_BACKEND=memory` (dev/test only — announced loudly at
  startup) serves the same graph from an in-memory store: workspaces and
  keys die with the process. It conflicts with `DATABASE_URL` — set one
  or the other, never both.
- **Backup root.** `--backup-root DIR` is the only place backups are
  written; without it `/admin/backup` answers `403 backups_disabled`.

- **Connections.** `--max-connections` (512) are served at once (keep
  it below `ulimit -n` less 64). A request head must arrive within
  `--header-timeout-ms` (10 s, idle keep-alive connections too), a body
  within `--body-deadline-ms` (30 s; past it, `408 body_timeout`), and
  each response write must make
  progress within `--write-stall-ms` (10 s). A connection gives at most
  `--max-requests-per-connection` (100) answers and lives at most
  `--max-connection-secs` (600) (D-045, D-049). What that proves, and no
  more: a slow or non-reading client holds a slot for at most one of
  those limits, then the slot frees. It does not stop many clients
  holding every slot for that long, and nothing here is per client
  address. For any non-local exposure, put a reverse proxy in front that
  terminates TLS, rate-limits per client, caps connections per client
  address, and buffers or times out slow responses.
- **Stopping.** Send SIGTERM (or Ctrl-C). The server stops accepting,
  lets requests in flight finish, makes everything it already decided
  durable, releases the journal and exits 0. It exits non-zero if the
  requests in flight are not done within `--drain-timeout-secs`
  (default 30; they are cut, after which the engine still drains), or if
  the engine's drain takes longer than that again (a restart replays). Handler calls
  in flight are abandoned and reconcile after the restart. Give the
  supervisor at least twice the drain timeout (systemd
  `TimeoutStopSec=70`).
- A handler call is `POST URL` with `{"input": [bytes], "idem_key":
  "<op_key>/<step index>"}` and the headers `x-idempotency-key` (the same
  key), `x-durable-run-id`, `x-durable-step` (the step's index) and
  `x-durable-attempt` (1 for the first try). A reconcile probe is `GET
  URL?idem_key=…` with the same run, step and attempt headers (the
  attempt it reconciles); 200 means "it landed" (D-050). Calls from
  store-registered handlers are additionally signed — `x-durable-timestamp`
  + `x-durable-signature`; verification and rotation are the contract in
  `docs/HANDLERS.md` (S2-6).
- `--handler name=URL` is the allowlist — repeatable, `http://` or
  `https://` only; a submit naming a handler outside it is refused 409
  with `unregistered handler`. Calls go only to that URL: redirects are
  not followed (a 3xx fails the attempt) and the proxy environment
  variables are ignored (D-039).
- **SSRF guard (S2-5):** handler URLs targeting loopback, private,
  link-local or other non-global addresses are refused — literal forms
  at registration, and any name that *resolves* to one at call time,
  inside the client's DNS resolver (a DNS answer that flips local after
  registration still sends zero packets; `--allow-local-handlers` opts
  out for development and fixtures — every loopback example here needs
  it).
- The journal directory is created on first run: `LOCK`, `manifest.json`,
  `gen-00000N/{journal.log,snapshot.bin}`, owner-only (directories 0700,
  files 0600; backups and restores too, D-038). Run the server as the
  user that owns them. A second server on the same
  directory **fails startup** — the exclusive writer lock is held.
- The inspector is at `http://127.0.0.1:58300/inspector/` and covers
  every API operation (list both orders, run detail, spec reveal,
  submit, cancel, diagnostics, compact, backup, health/ready). It
  holds the read key for reads and the API key for everything else,
  each in its own sessionStorage slot; *Forget keys* clears both.
  Dark and light themes share semantic tokens; the toggle is in the
  header.

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

## Crash drills

`perf/drill.py` kills the release server with SIGKILL, at random and
right after compactions, backups and cancel bursts, while clients submit
and cancel, then restarts it and checks that every acknowledged submit
and cancel survived and that backups restore complete (D-042):

```sh
cargo build --release --locked -p durable-server -p durable-cli
python3 -I perf/drill.py --rounds 20 --targeted 16 --clients 16 \
  --out perf/results/<timestamp>-kill9-drill.json
```

It exits non-zero on any loss. `--port` (default 53400, and that + 1 for
the restored server) moves it off ports in use.

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

`restore` first checks the backup against its own `backup.json` (every
file's size and checksum, and the generation its manifest names), then
checks that generation replays; a backup that fails any check is refused
and nothing is left in the target (D-044). It takes the target's journal
lock for the whole copy, so it
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
  Nothing was cut: the log is exactly as found. **Restore from a
  backup.** Do not truncate the log at the reported frame offset: that
  can leave part of a batch behind, and frames after it may have been
  acknowledged. If the message ends `the damage lies inside the log's
  last batch, which starts at offset B`, the host lost power and you
  have no backup, the one lossy repair is: copy the journal aside, then
  truncate `journal.log` to B (the start of that batch, never X). That
  drops the whole last batch; its records were acknowledged only if its
  fsync returned before the power cut, which the log cannot tell.
- `durable-server: discarded a torn tail: N bytes at offset X` at startup
  — the log ended inside its last group commit (a crash mid-write). The
  whole of that batch, from its first frame at X, was dropped: its fsync
  never returned, so none of it was acknowledged (D-043). Every earlier
  batch was kept.

## Known limits

What this server does not do, so the deployment must (or must not rely on
it):

- **Exposure.** It caps connections and times heads and bodies (D-045),
  and limits each client address's request rate and each peer's
  connection count (D-052: IPv6 is keyed by /64 with a shared /48
  aggregate, so a spray of distinct /48s can still push a new real
  client into the overflow bucket). It still has no TLS: put Cloudflare
  (or a reverse proxy that does TLS) in front, and pass its ranges as
  `--trusted-proxy` so the per-address limits count real clients — the
  trusted edges then get `--max-conns-per-proxy` (4096) instead of the
  untrusted `--max-conns-per-addr` (64). Never combine a proxy with
  `--open-reads`.
- **Handler calls are unsigned.** A handler cannot tell a call came from
  this runner except by network placement (D-039); keep handlers on a
  private network.
- **One op_key, one run.** Resubmitting an op_key returns its first run
  even if the spec differs; there is no 409 for a changed spec.
- **Keys.** One API key and one optional read-only key, read at startup:
  rotating either needs a restart, and there is no cancel-only scope.
- **Reads are limited per address, not per key.** One key holder across
  many addresses can still fill the read queue (bounded: 503 `busy`),
  never the write queue.
- **Power loss inside the last batch** with the file's length on disk
  stops startup (the batch cannot be told apart from damage); restore a
  backup (see "What honest looks like" above for the one lossy repair).
- **v1 logs.** A damaged length in the *last* frame of a log that holds
  only v1 frames (written before D-032) still reads as a torn tail and
  costs that one record. The window closes at the first v2 append.
- **Compaction stalls acks** for as long as it writes (D-021), and is
  opt-in for that reason.
- **One host.** No replication or failover; durability is this disk's
  fsync. Backups are the recovery path.
