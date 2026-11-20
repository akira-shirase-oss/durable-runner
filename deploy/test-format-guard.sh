#!/usr/bin/env bash
# test-format-guard.sh — the D-056 refusal logic against stub binaries
# that report a lower reader version than the journal holds. No server,
# no real journal: stub binaries echo a version, scratch dirs stand in
# for /var/lib/durable/journal.
set -uo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
fails=0

mkstub() { printf '#!/bin/sh\necho %s\n' "$2" >"$work/$1"; chmod +x "$work/$1"; }
mkstub reads-v1 1
mkstub reads-v2 2
mkstub reads-v3 3
mkstub reads-v3-2-1 '"3 2 1"'   # v1.0.2+: records, DRIX, parks ws≠0
printf '#!/bin/sh\nexit 1\n' >"$work/broken"; chmod +x "$work/broken"

# A gen's snapshot.bin: 28-byte DRSNAP frame (content opaque to the
# guard) then the DRIX payload — magic at 28-31, u16 BE version at 32-33.
mksnap() { install -d "$1/gen-000001"
  printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxDRIX' >"$1/gen-000001/snapshot.bin"
  printf "$(printf '\\x%02x\\x%02x' $(( $2 >> 8 )) $(( $2 & 255 )))" \
    >>"$1/gen-000001/snapshot.bin"; }

mkjournal() { install -d "$work/j$1/gen-000001"; printf '{"v":%s,"run":1}\n' "$2" >"$work/j$1/gen-000001/journal.log"; }
mkjournal v1 1; mkjournal v2 2; mkjournal v3 3

run() { # <expected 0|1> <journal> <stub> [desc]
  local want=$1 jrnl=$2 stub=$3
  FORMAT_GUARD_LISTEN=127.0.0.1:1 FORMAT_GUARD_KEY_FILE=/nonexistent \
    FORMAT_GUARD_JOURNAL_ROOT="$work/$jrnl" deploy/format-guard.sh "$work/$stub" >/dev/null 2>"$work/err"
  if [[ $? -ne $want ]]; then
    echo "FAIL: $jrnl + $stub expected exit $want"; cat "$work/err"; fails=1
  fi
}

run 0 jv2 reads-v2          # equal versions: fine
run 1 jv2 reads-v1          # journal ahead of the reader: refused
run 1 jv3 reads-v2          # a future bump: refused
run 0 jv1 reads-v1          # v1 journal, v1 reader: fine
run 0 jv1 reads-v2          # newer reader on an old journal: fine

# The refusal must name both versions (same shape as Colophon's
# unknown-migration check).
FORMAT_GUARD_LISTEN=127.0.0.1:1 FORMAT_GUARD_KEY_FILE=/nonexistent \
  FORMAT_GUARD_JOURNAL_ROOT="$work/jv3" deploy/format-guard.sh "$work/reads-v2" 2>"$work/err" || true
grep -q 'journal holds record version 3' "$work/err" && grep -q 'reads only up to 2' "$work/err" \
  || { echo "FAIL: refusal did not name both versions"; cat "$work/err"; fails=1; }

# A journal file with no "v" field at all (a real v1 journal) is 1.
install -d "$work/jnov/gen-000001"; printf '{"run":1,"event":"x"}\n' >"$work/jnov/gen-000001/journal.log"
run 0 jnov reads-v1

# No journal at all (fresh install): nothing written, anything reads it.
run 0 nope reads-v2

# A binary that cannot report its version: fail closed.
run 1 jv1 broken

# Fail-closed journal states — an unreadable or damaged journal is a
# refusal, never a silent v1.
install -d "$work/jperm/gen-000001"; printf '{"v":1}\n' >"$work/jperm/gen-000001/journal.log"
chmod 000 "$work/jperm"; run 1 jperm reads-v2; chmod 755 "$work/jperm"

install -d "$work/jnolog/gen-000001"; run 1 jnolog reads-v2          # gen dir, no journal.log

install -d "$work/jnoperm/gen-000001"; printf '{"v":9}\n' >"$work/jnoperm/gen-000001/journal.log"
chmod 000 "$work/jnoperm/gen-000001/journal.log"; run 1 jnoperm reads-v2
chmod 644 "$work/jnoperm/gen-000001/journal.log"

install -d "$work/jmani"; printf '{"generation":1}\n' >"$work/jmani/manifest.json"
run 1 jmani reads-v2                                                  # manifest but no gen dir

install -d "$work/jlogdir/gen-000001/journal.log"; run 1 jlogdir reads-v2  # journal.log is a dir

# --- v1.0.2: the snapshot (DRIX) version gates rollback too -----------
# The e6951b5 hole: v1.0.1 writes DRIX 2 on every compaction while
# format-guard compared record versions only — a rollback to v1.0.0
# passed the guard and then refused startup.
mkjournal snap2 2; mksnap "$work/jsnap2" 2
run 1 jsnap2 reads-v2          # v1.0.0-class reader on a DRIX-2 journal
# A sidecar-less DRIX-2 journal can never prove a pre-workspace writer
# (v1.0.1+ stamps "ws" on every record) — refused even for reads-v3.
run 1 jsnap2 reads-v3          # ambiguous: cannot park, refused
run 0 jsnap2 reads-v3-2-1      # the parking three-number answer

# A snapshot from the future refuses even a three-number binary.
mkjournal snap9 2; mksnap "$work/jsnap9" 9
run 1 jsnap9 reads-v3-2-1

# The refusal names the snapshot versions.
FORMAT_GUARD_LISTEN=127.0.0.1:1 FORMAT_GUARD_KEY_FILE=/nonexistent \
  FORMAT_GUARD_JOURNAL_ROOT="$work/jsnap2" deploy/format-guard.sh "$work/reads-v2" 2>"$work/err" || true
grep -q 'snapshot format 2' "$work/err" && grep -q 'only up to 1' "$work/err" \
  || { echo "FAIL: snapshot refusal did not name both versions"; cat "$work/err"; fails=1; }

# --- v1.0.2: ws ≠ 0 records gate rollback ------------------------------
# Once tenant-scoped records exist, only a binary that parks ws ≠ 0 runs
# may open this journal — a name-keyed registry would run tenant work on
# workspace 0's handlers.
mkjournal ws 3; printf '{"v":3,"ws":7,"run":1}\n' >"$work/jws/gen-000001/journal.log"
run 1 jws reads-v3           # v1.0.1 cannot park: refused
run 1 jws reads-v2           # nor v1.0.0
run 0 jws reads-v3-2-1       # v1.0.2 parks: admitted

# ws:0 is the writer's own default, not tenant data.
mkjournal ws0 3; printf '{"v":3,"ws":0,"run":1}\n' >"$work/jws0/gen-000001/journal.log"
run 0 jws0 reads-v3

# The ws refusal says what the older binary would do wrong.
FORMAT_GUARD_LISTEN=127.0.0.1:1 FORMAT_GUARD_KEY_FILE=/nonexistent \
  FORMAT_GUARD_JOURNAL_ROOT="$work/jws" deploy/format-guard.sh "$work/reads-v3" 2>"$work/err" || true
grep -q 'workspace-scoped' "$work/err" && grep -q 'park' "$work/err" \
  || { echo "FAIL: ws refusal did not explain the hazard"; cat "$work/err"; fails=1; }

# A garbled or truncated snapshot is damage, not a v1 journal.
mkjournal badmagic 2; head -c 40 /dev/zero >"$work/jbadmagic/gen-000001/snapshot.bin"
run 1 jbadmagic reads-v3-2-1
mkjournal short 2; printf 'xxDRIX' >"$work/jshort/gen-000001/snapshot.bin"
run 1 jshort reads-v3-2-1

# A 33-byte snapshot: magic fits, the u16 version does not.
mkjournal b33 2
python3 -c "import sys; sys.stdout.buffer.write(b'x'*28 + b'DRIX' + b'\x00')" \
  >"$work/jb33/gen-000001/snapshot.bin"
run 1 jb33 reads-v3-2-1

# --- v1.0.2 (fix review): journal.shape — the server-down sidecar -----
# After compaction, ws ≠ 0 runs can live ONLY inside snapshot.bin; the
# sidecar is what the guard reads when the records are pruned.
mkjournal scws 2; mksnap "$work/jscws" 2
printf 'records=2\nsnapshots=2\nws=9\n' >"$work/jscws/journal.shape"
run 1 jscws reads-v3           # tenant runs hidden in the snapshot: refused
run 0 jscws reads-v3-2-1       # a parking binary: admitted

mkjournal sc0 2; mksnap "$work/jsc0" 2
printf 'records=2\nsnapshots=2\nws=0\n' >"$work/jsc0/journal.shape"
run 0 jsc0 reads-v3            # precise: a clean sidecar admits

# A malformed or partial sidecar is damage, not a zero.
mkjournal scbad 2; printf 'records=zzz\n' >"$work/jscbad/journal.shape"
run 1 jscbad reads-v3-2-1
mkjournal scmiss 2; printf 'records=2\nsnapshots=2\n' >"$work/jscmiss/journal.shape"
run 1 jscmiss reads-v3-2-1

# Ambiguity without a sidecar refuses: DRIX ≥ 2 + any ws-bearing record
# (a ws-capable writer ran) could mean parked tenant runs pruned into
# the snapshot before sidecars existed.
mkjournal ambws 2; mksnap "$work/jambws" 2
printf '{"v":2,"ws":0,"run":1}\n' >"$work/jambws/gen-000001/journal.log"
run 1 jambws reads-v3
run 0 jambws reads-v3-2-1

# Same for a v3 record with no ws field, and for a crash right after
# compaction that left the newest journal.log empty.
mkjournal amb3 3; mksnap "$work/jamb3" 2
printf '{"v":3,"run":1}\n' >"$work/jamb3/gen-000001/journal.log"
run 1 jamb3 reads-v3
mkjournal ambmt 2; mksnap "$work/jambmt" 2
: >"$work/jambmt/gen-000001/journal.log"
run 1 jambmt reads-v3

# Hardest case: v2 records WITHOUT ws fields + a DRIX ≥ 2 snapshot and
# no sidecar — the only combination the old guard admitted. But every
# ws-aware writer (v1.0.1+) stamps "ws" on each record, so ws-free
# records can't prove the compactor was pre-workspace: refuse.
mkjournal ambold 2; mksnap "$work/jambold" 2
printf '{"v":2,"run":1}\n' >"$work/jambold/gen-000001/journal.log"
run 1 jambold reads-v3
run 0 jambold reads-v3-2-1

# Multi-generation: the scan walks every gen dir.
mkjournal multi 3; install -d "$work/jmulti/gen-000002"
printf '{"v":3,"ws":4,"run":1}\n' >"$work/jmulti/gen-000002/journal.log"
python3 -c "import sys; sys.stdout.buffer.write(b'x'*28 + b'DRIX' + b'\x00\x02')" \
  >"$work/jmulti/gen-000002/snapshot.bin"
printf 'records=3\nsnapshots=2\nws=4\n' >"$work/jmulti/journal.shape"
run 1 jmulti reads-v3
run 0 jmulti reads-v3-2-1

# --- v1.0.2 (fix review): the server-UP path --------------------------
# A running server under-reports (open-time maxima, pre-live-commit):
# the files must outrank it. Stub diagnostics over loopback.
mkjournal upws 3; printf '{"v":3,"ws":7,"run":1}\n' >"$work/jupws/gen-000001/journal.log"
printf 'records=3\nsnapshots=1\nws=7\n' >"$work/jupws/journal.shape"
printf 'x' >"$work/keyfile"
# A canned /admin/diagnostics claiming a clean journal (2/1/0) must NOT
# admit a non-parking binary: the files win.
python3 - "$work" <<'PY' &
import http.server, json, sys, threading
work = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps({"journal_record_version_max": 2,
                        "journal_snapshot_version_max": 1,
                        "journal_max_ws": 0}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass
s = http.server.HTTPServer(("127.0.0.1", 58401), H)
threading.Thread(target=s.serve_forever, daemon=True).start()
import os, subprocess, time
time.sleep(0.4)
env = dict(os.environ, FORMAT_GUARD_LISTEN="127.0.0.1:58401",
           FORMAT_GUARD_KEY_FILE=f"{work}/keyfile",
           FORMAT_GUARD_JOURNAL_ROOT=f"{work}/jupws")
for stub, want in [("reads-v3", 1), ("reads-v3-2-1", 0)]:
    r = subprocess.run(["deploy/format-guard.sh", f"{work}/{stub}"],
                       env=env, capture_output=True)
    print(f"server-up {stub}: exit {r.returncode} (want {want})",
          file=sys.stderr)
    if r.returncode != want:
        print(r.stderr.decode(), file=sys.stderr); sys.exit(1)
PY
pypid=$!
# A diagnostics answer that carries a non-numeric ws field refuses —
# the workspace scope can never be guessed at (line: `jws =~ ^[0-9]+$`).
python3 - "$work" <<'PY' &
import http.server, json, sys, threading
work = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        b = json.dumps({"journal_record_version_max": 2,
                        "journal_snapshot_version_max": 1,
                        "journal_max_ws": "nine"}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass
s = http.server.HTTPServer(("127.0.0.1", 58402), H)
threading.Thread(target=s.serve_forever, daemon=True).start()
import os, subprocess, time
time.sleep(0.4)
env = dict(os.environ, FORMAT_GUARD_LISTEN="127.0.0.1:58402",
           FORMAT_GUARD_KEY_FILE=f"{work}/keyfile",
           FORMAT_GUARD_JOURNAL_ROOT=f"{work}/jsc0")
r = subprocess.run(["deploy/format-guard.sh", f"{work}/reads-v3-2-1"],
                   env=env, capture_output=True)
print(f"server-up nonnumeric-ws: exit {r.returncode} (want 1)", file=sys.stderr)
if r.returncode != 1:
    print(r.stderr.decode(), file=sys.stderr); sys.exit(1)
PY
pypid2=$!
wait $pypid || { echo "FAIL: server-up merge wrong"; fails=1; }
wait $pypid2 || { echo "FAIL: nonnumeric-ws refusal wrong"; fails=1; }

(( fails == 0 )) && echo "format-guard tests: all pass" || exit 1
