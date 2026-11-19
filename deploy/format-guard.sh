#!/usr/bin/env bash
# format-guard — D-056's upgrade/rollback guard. Refuses (exit 1) to run
# a binary whose reader is older than the journal's highest record
# version: such a binary refuses startup, which turns a bad upgrade into
# an outage — or makes an automatic rollback the outage.
#
#   deploy/format-guard.sh /path/to/durable-server
#
# The journal's highest version comes from the running server's
# /admin/diagnostics when reachable; otherwise it is read straight off
# the journal files ("v" fields — a v1-only journal legitimately has
# none, so no match means 1). The fallback fails CLOSED: an unreadable
# journal, a gen-* dir without a readable journal.log, or a grep error
# is a refusal, never a silent "1".
# Env overrides exist only for the test (deploy/test-format-guard.sh).
set -euo pipefail
bin=${1:?usage: format-guard.sh /path/to/durable-server}
LISTEN=${FORMAT_GUARD_LISTEN:-127.0.0.1:58300}
KEY_FILE=${FORMAT_GUARD_KEY_FILE:-/etc/durable/api-key}
JOURNAL_ROOT=${FORMAT_GUARD_JOURNAL_ROOT:-/var/lib/durable/journal}

refuse() { echo "format-guard: REFUSED — $*" >&2; exit 1; }

jmax=""
if [[ -r $KEY_FILE ]]; then
  jmax=$(curl -fsS --max-time 5 \
    -H "authorization: Bearer $(tr -d '\n' <"$KEY_FILE")" \
    "http://$LISTEN/admin/diagnostics" 2>/dev/null \
    | jq -r '.journal_record_version_max // empty' 2>/dev/null || true)
fi
if [[ -z $jmax ]]; then
  # Server-down fallback: read the highest "v" off the journal files.
  # Fail CLOSED here — a scan that cannot complete is not a v1 journal.
  if [[ ! -d $JOURNAL_ROOT ]]; then
    jmax=1                                   # no journal at all: nothing written
  elif [[ ! -r $JOURNAL_ROOT || ! -x $JOURNAL_ROOT ]]; then
    refuse "journal dir $JOURNAL_ROOT is not readable"
  else
    shopt -s nullglob
    gens=( "$JOURNAL_ROOT"/gen-* )
    logs=()
    for g in "${gens[@]}"; do
      f=$g/journal.log
      [[ -f $f && -r $f ]] || refuse "$g has no readable journal.log"
      logs+=("$f")
    done
    # Generations gone while a manifest names them is damage, not v1.
    if (( ${#gens[@]} == 0 )) && [[ -f $JOURNAL_ROOT/manifest.json ]]; then
      refuse "$JOURNAL_ROOT has a manifest but no gen-*/journal.log"
    fi
    jmax=1
    for f in ${logs[@]+"${logs[@]}"}; do
      # grep 0 = matches, 1 = no match (v1 records only — fine), >=2 = error.
      if out=$(LC_ALL=C grep -ao '"v":[0-9]*' "$f" 2>/dev/null); then rc=0; else rc=$?; fi
      (( rc < 2 )) || refuse "cannot scan $f (grep exit $rc)"
      [[ -n $out ]] || continue
      m=$(printf '%s\n' "$out" | sed 's/.*://' | sort -n | tail -1)
      (( m > jmax )) && jmax=$m
    done
  fi
fi
[[ $jmax =~ ^[0-9]+$ ]] || refuse "cannot determine the journal's record version"
bmax=$("$bin" --journal-format 2>/dev/null) || {
  echo "format-guard: $bin --journal-format failed — refusing" >&2
  exit 1
}
if (( bmax < jmax )); then
  echo "format-guard: REFUSED — the journal holds record version $jmax" \
    "but $bin reads only up to $bmax" >&2
  exit 1
fi
echo "format-guard: journal v$jmax, $bin reads to v$bmax — ok"
