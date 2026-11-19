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
printf '#!/bin/sh\nexit 1\n' >"$work/broken"; chmod +x "$work/broken"

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

(( fails == 0 )) && echo "format-guard tests: all pass" || exit 1
