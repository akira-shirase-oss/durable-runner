#!/usr/bin/env python3
"""kill -9 drill: acknowledged runs and cancels must survive every crash.

Each round starts the release server on one persistent journal, runs
concurrent clients that submit (half run at once through a local handler,
half wait far in the future) and cancel about 40% of their acknowledged
waiting runs, while a maintenance thread compacts and backs up. The server
is then killed with SIGKILL, either at a random moment or 0-30 ms after a
compaction, a backup or a burst of cancels, and restarted. After every
restart the drill checks, across all rounds so far:

- every acknowledged submit is present with its op_key (lost / id mismatch);
- resubmitting a sample of acknowledged op_keys returns the same run id
  (duplicates);
- every acknowledged cancel still reads `cancelled`;
- the round's last completed backup restores (durable-cli restore), opens on
  a second server, agrees with the live journal on every run it holds, and
  holds every run acknowledged before that backup was requested;
- no `.partial` backup is visible under its name.

Standard library only. Usage:
  perf/drill.py --rounds 20 --targeted 15 --clients 16 --out perf/results/X.json
"""
import argparse
import fcntl
import http.client
import json
import os
import random
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAR = 9_999_999_999_999

# The heavy slot, same contract as perf/heavy.sh (S2 re-review F4): an
# flock(2) on ONE FIXED PATH — the git common dir, the one path every
# worktree of this clone resolves identically — kernel-held for as long
# as this process (or an inheriting child) keeps the fd: no stale sweep,
# no pid to liveness-check, refused loudly after a few tries, never a
# silent unlocked run.
def _heavy_lock_path():
    if os.environ.get("DR_HEAVY_LOCK"):
        return os.environ["DR_HEAVY_LOCK"]
    out = subprocess.run(["git", "rev-parse", "--git-common-dir"],
                         cwd=ROOT, capture_output=True, check=True)
    return os.path.join(os.path.realpath(
        os.path.join(ROOT, out.stdout.decode().strip())),
        "durable-runner-heavy.lock")


HEAVY_LOCK = _heavy_lock_path()


def heavy_lock():
    # Under perf/heavy.sh we already hold the slot: fd 9 IS the lock
    # file's open description (the fd, not DR_HEAVY_HELD, is the proof).
    # Acquiring a second description here would contend with our own
    # parent — flock excludes other descriptions, not its own.
    try:
        held = os.fstat(9)
        lock = os.stat(HEAVY_LOCK)
        if (held.st_dev, held.st_ino) == (lock.st_dev, lock.st_ino):
            # The fd is the proof, and the proof is the LOCK: a relock
            # on our inherited description never contends; an fd merely
            # open on a contended lock would fail here (N5).
            fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return None
    except OSError:
        pass
    fd = os.open(HEAVY_LOCK, os.O_RDWR | os.O_CREAT, 0o666)
    for _ in range(4):
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                with open(HEAVY_LOCK + ".holder", "w") as h:
                    h.write(f"{os.getpid()} drill.py\n")
            except OSError:
                pass
            return fd  # the caller keeps it alive for the whole run
        except OSError:
            time.sleep(1)
    holder = ""
    try:
        with open(HEAVY_LOCK + ".holder") as h:
            holder = h.read().strip()
    except OSError:
        pass
    print(f"drill.py: the heavy slot is held ({holder or 'unknown'}) — "
          "one load run per machine; refusing to race", file=sys.stderr)
    sys.exit(3)


class Handler(BaseHTTPRequestHandler):
    seen = set()
    lock = threading.Lock()

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        with Handler.lock:
            Handler.seen.add(req.get("idem_key", ""))
        self._reply(200, b'{"ok":true}')

    def do_GET(self):
        key = self.path.split("idem_key=", 1)[-1]
        with Handler.lock:
            found = any(k.replace("/", "%2F") == key or k == key for k in Handler.seen)
        self._reply(200 if found else 404)

    def log_message(self, *a):
        pass


class QuietServer(ThreadingHTTPServer):
    """A killed runner drops its calls mid-reply; that is expected here."""

    def handle_error(self, request, client_address):
        pass


class Api:
    def __init__(self, port, key):
        self.port, self.key = port, key
        self.conn = None

    def call(self, method, path, body=None, timeout=10):
        for _ in range(2):
            try:
                if self.conn is None:
                    self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
                hdrs = {"authorization": f"Bearer {self.key}"}
                data = None
                if body is not None:
                    data = json.dumps(body).encode()
                    hdrs["content-type"] = "application/json"
                self.conn.request(method, path, body=data, headers=hdrs)
                r = self.conn.getresponse()
                raw = r.read()
                try:
                    return r.status, json.loads(raw or b"null")
                except ValueError:
                    return r.status, None
            except (OSError, http.client.HTTPException):
                if self.conn is not None:
                    self.conn.close()
                self.conn = None
        return 0, None


def target_directory():
    """Where cargo puts this workspace's builds (honours CARGO_TARGET_DIR)."""
    out = subprocess.run(["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
                         cwd=ROOT, capture_output=True, check=True)
    return json.loads(out.stdout)["target_directory"]


def submit_body(op_key, delayed):
    b = {
        "op_key": op_key,
        "spec": {"version": 1, "steps": [{"name": "a", "handler": "step"}], "input_b64": "aGVsbG8="},
    }
    if delayed:
        b["not_before"] = FAR
    return b


def start(binary, journal, port, handler_port, backups, key, log):
    env = dict(os.environ, DURABLE_API_KEY=key)
    env.pop("DURABLE_READ_KEY", None)
    p = subprocess.Popen(
        [binary, "--journal", journal, "--listen", f"127.0.0.1:{port}",
         "--handler", f"step=http://127.0.0.1:{handler_port}/step",
         # The fixture handler is loopback — the S2-5 opt-out, same as
         # the load harness's spawned server.
         "--allow-local-handlers",
         # The drill measures crash durability, not admission: resubmit
         # bursts must not hit the per-address 429 limiter.
         "--rate-limit-per-addr", "0",
         *(["--backup-root", backups] if backups else [])],
        env=env, stdout=subprocess.DEVNULL, stderr=log)
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"server exited at startup with {p.returncode}")
        try:
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            c.request("GET", "/health")
            if c.getresponse().status == 200:
                return p
        except OSError:
            pass
        time.sleep(0.05)
    p.kill()
    raise RuntimeError("server never became healthy")


def all_runs(api):
    runs, after = {}, None
    while True:
        path = "/jobs?limit=1000" + (f"&after={after}" if after else "")
        st, v = api.call("GET", path, timeout=30)
        if st != 200:
            raise RuntimeError(f"list answered {st}")
        for r in v["runs"]:
            runs[r["id"]] = (r["key"], r["state"])
        after = v.get("next_after")
        if not after:
            return runs


def main():
    _heavy_fd = heavy_lock()  # held until exit: one load run per machine
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=20, help="random-moment kills")
    ap.add_argument("--targeted", type=int, default=15, help="kills 0-30 ms after compact/backup/cancel burst")
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--port", type=int, default=53400)
    release = os.path.join(target_directory(), "release")
    ap.add_argument("--binary", default=os.path.join(release, "durable-server"))
    ap.add_argument("--cli", default=os.path.join(release, "durable-cli"))
    ap.add_argument("--out")
    args = ap.parse_args()
    rnd = random.Random(int(os.environ.get("DRILL_SEED", "20261116")))

    work = tempfile.mkdtemp(prefix="dr-drill-")
    journal, backups = os.path.join(work, "journal"), os.path.join(work, "backups")
    key = secrets.token_hex(32)
    log = open(os.path.join(work, "server.log"), "ab")
    hs = QuietServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=hs.serve_forever, daemon=True).start()
    hport = hs.server_address[1]

    acked = {}        # op_key -> run id
    cancelled = set()  # run ids whose cancel was acknowledged
    lock = threading.Lock()
    totals = {"rounds": 0, "acked_submits": 0, "acked_cancels": 0, "lost": 0, "dup": 0,
              "id_mismatch": 0, "cancels_lost": 0, "compactions": 0, "backups": 0,
              "backups_restored_complete": 0, "backups_checked": 0, "visible_partial": 0,
              "kills": {"random": 0, "after_compact": 0, "after_backup": 0, "after_cancels": 0}}
    failures = []
    plan = ["random"] * args.rounds + [
        ["after_compact", "after_backup", "after_cancels"][i % 3] for i in range(args.targeted)]
    rnd.shuffle(plan)

    for rno, kind in enumerate(plan, 1):
        server = start(args.binary, journal, args.port, hport, backups, key, log)
        stop = threading.Event()
        last_backup = {}

        def client(t):
            api, i = Api(args.port, key), 0
            mine = []
            while not stop.is_set():
                op = f"r{rno}-c{t}-{i}"
                i += 1
                delayed = rnd.random() < 0.5
                st, v = api.call("POST", "/jobs", submit_body(op, delayed))
                if st == 200:
                    with lock:
                        acked[op] = v["run_id"]
                    if delayed:
                        mine.append(v["run_id"])
                if mine and rnd.random() < 0.4:
                    rid = mine.pop(rnd.randrange(len(mine)))
                    st, _ = api.call("POST", f"/jobs/{rid}/cancel")
                    if st == 200:
                        with lock:
                            cancelled.add(rid)
                if st == 0:
                    time.sleep(0.01)

        def maintenance():
            api, k = Api(args.port, key), 0
            while not stop.is_set():
                time.sleep(rnd.uniform(0.05, 0.25))
                if rnd.random() < 0.5:
                    st, _ = api.call("POST", "/admin/compact", timeout=60)
                    if st == 200:
                        with lock:
                            totals["compactions"] += 1
                else:
                    name = f"b{rno}-{k}"
                    k += 1
                    with lock:
                        before = set(acked.values())
                    st, _ = api.call("POST", "/admin/backup", {"name": name}, timeout=60)
                    if st == 200:
                        with lock:
                            totals["backups"] += 1
                        last_backup.clear()
                        last_backup.update(name=name, before=before)

        threads = [threading.Thread(target=client, args=(t,)) for t in range(args.clients)]
        threads.append(threading.Thread(target=maintenance))
        for th in threads:
            th.start()
        time.sleep(rnd.uniform(0.4, 1.2))
        if kind != "random":
            api = Api(args.port, key)
            if kind == "after_compact":
                threading.Thread(target=api.call, args=("POST", "/admin/compact"), kwargs={"timeout": 60}).start()
            elif kind == "after_backup":
                threading.Thread(target=api.call, args=("POST", "/admin/backup", {"name": f"k{rno}"}),
                                 kwargs={"timeout": 60}).start()
            else:
                with lock:
                    waiting = [rid for rid in acked.values() if rid not in cancelled][-50:]
                def burst(ids=waiting):
                    a = Api(args.port, key)
                    for rid in ids:
                        st, _ = a.call("POST", f"/jobs/{rid}/cancel")
                        if st == 200:
                            with lock:
                                cancelled.add(rid)
                threading.Thread(target=burst).start()
            time.sleep(rnd.uniform(0, 0.030))
        server.send_signal(signal.SIGKILL)
        server.wait()
        totals["kills"][kind] += 1
        stop.set()
        for th in threads:
            th.join()
        totals["rounds"] += 1

        # Restart and verify everything acknowledged so far.
        server = start(args.binary, journal, args.port, hport, backups, key, log)
        api = Api(args.port, key)
        runs = all_runs(api)
        with lock:
            snapshot_acked, snapshot_cancelled = dict(acked), set(cancelled)
        lost = [op for op, rid in snapshot_acked.items() if rid not in runs]
        mismatch = [op for op, rid in snapshot_acked.items() if rid in runs and runs[rid][0] != op]
        cancels_lost = [rid for rid in snapshot_cancelled if runs.get(rid, ("", ""))[1] != "cancelled"]
        dup = 0
        for op in rnd.sample(sorted(snapshot_acked), min(200, len(snapshot_acked))):
            st, v = api.call("POST", "/jobs", submit_body(op, True))
            if st != 200 or v.get("run_id") != snapshot_acked[op]:
                dup += 1
        totals["lost"] += len(lost)
        totals["id_mismatch"] += len(mismatch)
        totals["cancels_lost"] += len(cancels_lost)
        totals["dup"] += dup
        if lost or mismatch or cancels_lost or dup:
            failures.append({"round": rno, "kind": kind, "lost": lost[:10], "mismatch": mismatch[:10],
                             "cancels_lost": cancels_lost[:10], "dup": dup})
        visible = [n for n in os.listdir(backups) if n.endswith(".partial") and not n.startswith(".")]
        totals["visible_partial"] += len(visible)

        # The round's last completed backup restores complete.
        if last_backup:
            totals["backups_checked"] += 1
            dest = os.path.join(work, f"restored-{rno}")
            r = subprocess.run([args.cli, "restore", "--from", os.path.join(backups, last_backup["name"]),
                                "--to", dest], capture_output=True)
            ok = r.returncode == 0
            if ok:
                rs = start(args.binary, dest, args.port + 1, hport, None, key, log)
                try:
                    restored = all_runs(Api(args.port + 1, key))
                finally:
                    rs.send_signal(signal.SIGTERM)
                    rs.wait(timeout=60)
                agree = all(runs.get(rid, (None,))[0] == k for rid, (k, _) in restored.items())
                complete = last_backup["before"] <= set(restored)
                ok = agree and complete and rs.returncode == 0
            if ok:
                totals["backups_restored_complete"] += 1
            else:
                failures.append({"round": rno, "backup": last_backup["name"], "restore_rc": r.returncode,
                                 "stderr": r.stderr.decode()[-300:]})
            shutil.rmtree(dest, ignore_errors=True)
        server.send_signal(signal.SIGKILL)
        server.wait()
        print(f"round {rno:2} {kind:13} acked {len(snapshot_acked):6} cancels {len(snapshot_cancelled):5} "
              f"lost {len(lost)} dup {dup} mismatch {len(mismatch)} cancels_lost {len(cancels_lost)}",
              flush=True)

    totals["acked_submits"] = len(acked)
    totals["acked_cancels"] = len(cancelled)
    # Final stop: SIGTERM must drain and exit 0.
    server = start(args.binary, journal, args.port, hport, backups, key, log)
    server.send_signal(signal.SIGTERM)
    totals["final_sigterm_exit"] = server.wait(timeout=90)
    with open(os.path.join(journal, "manifest.json")) as f:
        totals["final_generation"] = json.load(f)["generation"]
    hs.shutdown()
    log.close()
    result = {"totals": totals, "failures": failures,
              "seed": int(os.environ.get("DRILL_SEED", "20261116")), "clients": args.clients}
    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
    shutil.rmtree(work, ignore_errors=True)
    bad = (totals["lost"] or totals["dup"] or totals["id_mismatch"] or totals["cancels_lost"]
           or totals["visible_partial"] or totals["final_sigterm_exit"] != 0
           or totals["backups_restored_complete"] != totals["backups_checked"])
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
