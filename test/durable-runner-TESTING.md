# Testing guide: Durable Workflow Runner (`durable-runner`)

https://github.com/akira-shirase-oss/durable-runner

Durable Runner is a background-job runner in Rust: you submit a versioned job, get a durable acknowledgement, and the process is supposed to recover that state after it dies. A framed append-only file journal with a single writer is the sole authority — no database queue, no Redis. Status in the README is D4 in progress (D0–D3 proven, including a live `kill -9` → restore drill); the load plan is the remaining ship gate.

Written against commit `c36f5c3` (2026-11-08). `Cargo.toml` says MIT; there is no `LICENSE` file.

## Prerequisites

- [rustup](https://rustup.rs) — pins **Rust 1.99.1** in `rust-toolchain.toml` (edition 2021).
- **python3** and **curl** for `examples/pilot/run.sh`.
- **`lsof`** — the pilot script uses `lsof -ti :<port>` to `kill -9` the server. If `lsof` is missing, that step fails.
- Optional, for the inspector UI: Node.js (the inspector app does not declare an `engines` field; it is SvelteKit 3 / Vite 8). `npm install` and `npm run build` in `apps/inspector`.

On macOS:

```sh
export PATH="$(brew --prefix rustup)/bin:$PATH"
```

## Setup and run

The path the repo itself recommends:

```sh
git clone https://github.com/akira-shirase-oss/durable-runner.git
cd durable-runner
cargo build --release --locked && examples/pilot/run.sh
```

`examples/pilot/run.sh` builds `durable-server` and `durable-cli` if needed, starts a tiny Python handler, submits a two-step job, waits until it is `done`, `kill -9`s the server, restarts against the same journal, then compacts, backs up, and restores on a second port. It is the ship-gate rehearsal for `docs/OPERATING.md`.

To run it yourself (inspector optional):

```sh
cargo build --release --locked -p durable-server -p durable-cli
cd apps/inspector && npm install && npm run build && cd ../..

python3 examples/pilot/handler.py &
target/release/durable-server \
  --journal data/journal \
  --listen 127.0.0.1:58300 \
  --ui-dir apps/inspector/build \
  --handler step=http://127.0.0.1:58320/step

CLI="target/release/durable-cli --server http://127.0.0.1:58300"
$CLI submit --op-key pilot/onboarding-email \
      --step prepare=step --step deliver=step \
      --input '{"user":"alice"}'
$CLI inspect --run 1
```

The inspector, if you built it, is at `http://127.0.0.1:58300/inspector/`. `GET /jobs` lists runs.

`--handler name=URL` is the allowlist (repeatable). A submit naming a handler that is not registered is refused **409** (`unregistered handler`). The journal directory is created on first run (`LOCK`, `manifest.json`, `gen-00000N/{journal.log,snapshot.json}`). A second server on the same directory **fails startup** — the exclusive writer lock is held.

Tests (no extra services):

```sh
cargo test --workspace --locked
```

**What was verified on a Linux box (2026-11-10, America/Toronto):** `cargo test --workspace --locked` passed (domain, journal, engine — including crash-between-dispatch, backup/restore, torn-tail, and second-writer tests). `cargo build --release --locked -p durable-server -p durable-cli` succeeded. The full `examples/pilot/run.sh` was not executed here because `lsof` was not installed. The inspector npm build was not run.

## Happy-path checklist

- [ ] `cargo test --workspace --locked` is green.
- [ ] `examples/pilot/run.sh` finishes with the job still `done` after `kill -9`, compaction, and restore (same run id).
- [ ] `$CLI inspect --run <id>` shows an honest state (`queued` / `running:...` / `reconciling:...` / `done` / `failed:...` / `cancelled` — see `docs/OPERATING.md`).
- [ ] Resubmitting the same `--op-key` returns the same run id and does not re-execute.
- [ ] Optional: the inspector page loads and shows the run.

## Try to break it

**Crash recovery from the journal**

- The pilot already does the headline case: `kill -9` mid-life, restart with the same `--journal`, inspect the same run. Acknowledged state should still be there.
- Start a long / hanging handler, kill the server between dispatch and reply, restart. The engine tests describe the expected behaviour: it resumes `running`, then reconciles before retrying (`crash_between_dispatch_and_reply_resumes_running_then_recovers`, `lost_reply_reconciles_and_advances`). You should see `reconciling:step:N` rather than a blind second fire-and-forget.
- `kill -9` during a write, then restart. A torn tail should be discarded and earlier history kept (`torn_tail_is_discarded_history_is_kept`). Corrupt *committed* history, or a sequence gap, should **refuse to start**, not skip forward (`corrupt_committed_history_stops_startup`, `sequence_gap_is_corruption`).

**Single writer, dedup, allowlist**

- Launch a second `durable-server` on the same journal directory; it should fail to start.
- Resubmit the same `--op-key`; you get the original run, nothing re-runs.
- Submit a step whose handler name is not in `--handler`; expect HTTP 409, and the journal should not have taken the job.
- `$CLI cancel --run <id>` should stop scheduling. It must **not** undo a remote effect already attempted.
- `$CLI backup --to /tmp/durable-backup` then restore into a new directory (or use the pilot's restore). The restored server should show the same run ids and `finished_at`.

```sh
target/release/durable-cli restore --from /tmp/durable-backup --to data/restored
target/release/durable-server --journal data/restored --listen 127.0.0.1:58301 \
  --handler step=http://127.0.0.1:58320/step
```

(`examples/pilot/run.sh` currently restarts the restored server on the backup directory itself; the operating doc's `restore --from ... --to ...` path is the more general one.)

If the server is full, a 503 is the honest refusal (admission reserve), not a silent queue.

## How to report a bug

Send Vinay (`akira-shirase-oss` / @404humandev) a note, or open a GitHub issue, including:

- Commit SHA (`git rev-parse HEAD`).
- OS, `rustc --version`.
- The exact `durable-server` / `durable-cli` command lines (journal path, listen address, handler allowlist).
- Run id, `--op-key`, inspect JSON before and after the failure, and the server log.
- For crash bugs: when you killed the process (before ack, mid-step, after `done`), and whether restart used the same journal directory.
- If you can, a copy of the journal dir (`LOCK`, `manifest.json`, `gen-*/journal.log`, `snapshot.json`) from a failing restore — it's the evidence.

## Setup notes we hit

- No `LICENSE` file, despite `license = "MIT"` in `Cargo.toml`.
- No CI.
- `examples/pilot/run.sh` depends on `lsof` (not declared in the README's "Requires: python3, curl").
- The inspector UI is optional; the pilot still curls `/inspector/` and will print a non-200 if you skipped the npm build.
