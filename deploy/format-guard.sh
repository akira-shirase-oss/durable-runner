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
# none, so no match means 1). If neither works the guard refuses.
# Env overrides exist only for the test (deploy/test-format-guard.sh).
set -euo pipefail
bin=${1:?usage: format-guard.sh /path/to/durable-server}
LISTEN=${FORMAT_GUARD_LISTEN:-127.0.0.1:58300}
KEY_FILE=${FORMAT_GUARD_KEY_FILE:-/etc/durable/api-key}
JOURNAL_ROOT=${FORMAT_GUARD_JOURNAL_ROOT:-/var/lib/durable/journal}

jmax=""
if [[ -r $KEY_FILE ]]; then
  jmax=$(curl -fsS --max-time 5 \
    -H "authorization: Bearer $(tr -d '\n' <"$KEY_FILE")" \
    "http://$LISTEN/admin/diagnostics" 2>/dev/null \
    | jq -r '.journal_record_version_max // empty' 2>/dev/null || true)
fi
if [[ -z $jmax ]]; then
  if compgen -G "$JOURNAL_ROOT/gen-*/journal.log" >/dev/null; then
    jmax=$(LC_ALL=C grep -ao '"v":[0-9]*' "$JOURNAL_ROOT"/gen-*/journal.log 2>/dev/null \
      | sed 's/.*://' | sort -n | tail -1 || true)
  fi
  jmax=${jmax:-1}
fi
[[ $jmax =~ ^[0-9]+$ ]] || {
  echo "format-guard: cannot determine the journal's record version — refusing" >&2
  exit 1
}
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
