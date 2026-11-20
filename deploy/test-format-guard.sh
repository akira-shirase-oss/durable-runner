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
run 0 jsnap2 reads-v3          # v1.0.1 floor: reader 3 implies DRIX 2
run 0 jsnap2 reads-v3-2-1      # the explicit three-number answer

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

(( fails == 0 )) && echo "format-guard tests: all pass" || exit 1
