#!/usr/bin/env bash
# The migration freeze guard: crates/durable-accounts/migrations/CHECKSUMS
# pins every migration file by sha256. A migration that was edited,
# deleted, or added without a manifest entry fails the gate — a database
# that ran the earlier version must never meet a different file under the
# same number (AGENTS.md: a migration is never edited once any database
# may have applied it).
#
# Adding a migration: write the next-numbered file and append its
# `shasum -a 256` line to CHECKSUMS. While a migration is still unmerged
# its line may be regenerated alongside the edit; once merged the line is
# frozen — a checksum update inside an "unrelated" diff is exactly the
# edit this guard exists to make loud.
set -euo pipefail
cd "$(dirname "$0")/.."
MIGRATIONS=${MIGRATIONS:-crates/durable-accounts/migrations}

# audit DIR: every manifest line must name a present file with a matching
# hash; every *.sql must be listed. Names the exact failure, exit 1.
audit() {
  local dir=$1 fails=0
  local manifest=$dir/CHECKSUMS
  if [[ ! -f $manifest ]]; then
    echo "migrations: $dir has no CHECKSUMS manifest" >&2
    return 1
  fi
  local line sum f got
  while IFS= read -r line || [[ -n $line ]]; do
    [[ -z $line ]] && continue
    sum=${line%%  *}
    f=${line#*  }
    if [[ ! -f $dir/$f ]]; then
      echo "migrations: $f is listed but missing" >&2; fails=1; continue
    fi
    got=$(shasum -a 256 "$dir/$f" | cut -d' ' -f1)
    if [[ $got != "$sum" ]]; then
      echo "migrations: $f was edited — its content no longer matches CHECKSUMS" >&2
      fails=1
    fi
  done <"$manifest"
  local listed unlisted
  for f in "$dir"/*.sql; do
    [[ -e $f ]] || continue
    listed=$(basename "$f")
    unlisted=$(grep -c "  $listed\$" "$manifest" || true)
    if [[ $unlisted != 1 ]]; then
      echo "migrations: $listed is not listed in CHECKSUMS" >&2
      fails=1
    fi
  done
  return "$fails"
}

# Known-bad proof: three tampered copies MUST fail — edited content, a
# missing listed file, an unlisted new file. A guard whose checks can't
# go red is the same as no guard (S2 re-review N7).
work=$(mktemp -d "${TMPDIR:-/tmp}/dr-migrations.XXXXXX")
trap 'rm -rf "$work"' EXIT
cp -R "$MIGRATIONS" "$work/edited"
printf -- '-- tampered\n' >>"$work/edited/0001_init.sql"
audit "$work/edited" >/dev/null 2>&1 \
  && { echo "check-migrations: an EDITED migration passed — the guard is broken" >&2; exit 1; }
cp -R "$MIGRATIONS" "$work/missing"
first=$(head -1 "$work/missing/CHECKSUMS" | sed 's/.*  //')
rm "$work/missing/$first"
audit "$work/missing" >/dev/null 2>&1 \
  && { echo "check-migrations: a MISSING migration passed — the guard is broken" >&2; exit 1; }
cp -R "$MIGRATIONS" "$work/unlisted"
printf 'select 1;\n' >"$work/unlisted/9999_sneaked.sql"
audit "$work/unlisted" >/dev/null 2>&1 \
  && { echo "check-migrations: an UNLISTED migration passed — the guard is broken" >&2; exit 1; }

audit "$MIGRATIONS" || exit 1
echo "migrations: CHECKSUMS holds (edited/missing/unlisted all refused)"
