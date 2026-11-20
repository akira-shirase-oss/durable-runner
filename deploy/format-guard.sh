#!/usr/bin/env bash
# format-guard — D-056's upgrade/rollback guard. Refuses (exit 1) to run
# a binary that cannot safely read this journal:
#   * record version  — an older reader refuses startup, which turns a
#     bad upgrade (or an automatic rollback) into the outage;
#   * snapshot format — a compacted journal can out-date an older reader
#     even when no new record version was ever written (v1.0.0 reads
#     DRIX 1 only; v1.0.1 wrote DRIX 2 — the e6951b5 gap);
#   * workspace scope — a v3 journal may carry ws ≠ 0 runs; any binary
#     that cannot park them would execute tenant work on workspace 0's
#     name-keyed handlers (S2 review).
#
#   deploy/format-guard.sh /path/to/durable-server
#
# The journal's shape comes from the running server's /admin/diagnostics
# when reachable, joined with what is read straight off the journal
# files ("v"/"ws" fields — a v1-only journal legitimately has neither —
# and gen-*/snapshot.bin DRIX headers). The file scan fails CLOSED: an
# unreadable journal, a gen-* dir without a readable journal.log, or a
# malformed snapshot is a refusal, never a silent "1".
#
# `$bin --journal-format` prints three numbers since v1.0.2 —
# "<records> <snapshots> <parks-ws≠0>"; older binaries print only the
# record version, and their snapshot capability is the release table
# (record-reader 3 = DRIX 2; older = DRIX 1). None of them parks.
# Env overrides exist only for the test (deploy/test-format-guard.sh).
set -euo pipefail
bin=${1:?usage: format-guard.sh /path/to/durable-server}
LISTEN=${FORMAT_GUARD_LISTEN:-127.0.0.1:58300}
KEY_FILE=${FORMAT_GUARD_KEY_FILE:-/etc/durable/api-key}
JOURNAL_ROOT=${FORMAT_GUARD_JOURNAL_ROOT:-/var/lib/durable/journal}

refuse() { echo "format-guard: REFUSED — $*" >&2; exit 1; }

jmax=""
jsnap=""
jws=""
if [[ -r $KEY_FILE ]]; then
  diag=$(curl -fsS --max-time 5 \
    -H "authorization: Bearer $(tr -d '\n' <"$KEY_FILE")" \
    "http://$LISTEN/admin/diagnostics" 2>/dev/null || true)
  jmax=$(printf '%s' "$diag" | jq -r '.journal_record_version_max // empty' 2>/dev/null || true)
  jsnap=$(printf '%s' "$diag" | jq -r '.journal_snapshot_version_max // empty' 2>/dev/null || true)
  jws=$(printf '%s' "$diag" | jq -r '.journal_max_ws // empty' 2>/dev/null || true)
fi
# File scan — ALWAYS, and the max of it and diagnostics wins: a running
# server's numbers reflect journal open, not what live commits and a
# live compaction have written since (S2 review). Fail CLOSED: an
# unreadable journal, a gen-* dir without a readable journal.log, or a
# malformed snapshot/sidecar is a refusal, never a silent "1".
smax=1; ssnap=1; sws=0
if [[ ! -d $JOURNAL_ROOT ]]; then
  :                                        # no journal at all
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
  for f in ${logs[@]+"${logs[@]}"}; do
    # grep 0 = matches, 1 = no match (v1 records only — fine), >=2 = error.
    if out=$(LC_ALL=C grep -ao '"v":[0-9]*' "$f" 2>/dev/null); then rc=0; else rc=$?; fi
    (( rc < 2 )) || refuse "cannot scan $f (grep exit $rc)"
    if [[ -n $out ]]; then
      m=$(printf '%s\n' "$out" | sed 's/.*://' | sort -n | tail -1)
      (( m > smax )) && smax=$m
    fi
    # Workspace-scoped history: a record with ws ≠ 0 is what a
    # pre-v1.0.2 binary cannot park — it would run tenant work on
    # workspace-0's name-keyed handlers (S2 review). "ws":0 is the
    # writer's own default, not tenant data.
    if LC_ALL=C grep -aq '"ws":[1-9]' "$f" 2>/dev/null; then sws=1; fi
  done
  # Snapshot formats: gen-*/snapshot.bin is a framed record whose
  # payload is "DRIX" + u16 version — so the magic sits at bytes 28-31
  # and the version at 32-33. Anything shorter cannot carry both.
  for g in "${gens[@]}"; do
    sbin=$g/snapshot.bin
    [[ -e $sbin ]] || continue
    [[ -f $sbin && -r $sbin ]] || refuse "cannot read $sbin"
    sz=$(wc -c <"$sbin" | tr -d ' ')
    (( sz >= 34 )) || refuse "$sbin is too short to carry a DRIX header"
    magic=$(dd bs=1 skip=28 count=4 <"$sbin" 2>/dev/null || true)
    [[ $magic == DRIX ]] || refuse "$sbin is not a DRIX snapshot"
    vb=$(od -An -j32 -N2 -tu1 <"$sbin" 2>/dev/null) || refuse "cannot scan $sbin"
    read -r b0 b1 _ <<<"$vb"
    [[ -n ${b0:-} && -n ${b1:-} ]] || refuse "$sbin is too short to carry a DRIX header"
    sv=$(( b0 * 256 + b1 ))
    (( sv > ssnap )) && ssnap=$sv
  done
  # journal.shape — the sidecar a sidecar-era writer maintains: highest
  # record version, snapshot version, and workspace id, including what
  # now lives ONLY inside snapshot.bin (prune_superseded removed the
  # records that carried it). It outranks the greppable scans; malformed
  # or unreadable refuses.
  sidecar=0
  shape=$JOURNAL_ROOT/journal.shape
  if [[ -e $shape ]]; then
    [[ -f $shape && -r $shape ]] || refuse "cannot read $shape"
    for k in records snapshots ws; do
      v=$(LC_ALL=C grep -ao "^$k=[0-9][0-9]*\$" "$shape" 2>/dev/null | head -1 | cut -d= -f2 || true)
      [[ $v =~ ^[0-9]+$ ]] || refuse "$shape is malformed (missing/bad $k)"
      case $k in
        records)   (( v > smax ))  && smax=$v ;;
        snapshots) (( v > ssnap )) && ssnap=$v ;;
        ws)        (( v > 0 ))     && sws=1 ;;
      esac
    done
    sidecar=1
  fi
  # Ambiguity is a refusal, not an assumption: a snapshot CAN hide ws ≠ 0
  # runs once compaction deleted their records, and every ws-aware writer
  # has stamped a "ws" field on every record since v1.0.1 — so a
  # sidecar-less DRIX ≥ 2 journal can never prove a pre-workspace writer
  # made it. No sidecar and DRIX ≥ 2 refuses every non-parking binary.
  if (( sidecar == 0 && ssnap >= 2 )); then
    sws=1
  fi
fi
# Whatever the files hold outranks whatever a still-running older
# server reports — the journal is the authority, not the process.
# (Defaults first: max() of two zeros must still land on a number.)
jmax=${jmax:-0}; jsnap=${jsnap:-0}; jws=${jws:-0}
[[ $smax =~ ^[0-9]+$ ]] && (( smax > jmax )) && jmax=$smax
[[ $ssnap =~ ^[0-9]+$ ]] && (( ssnap > jsnap )) && jsnap=$ssnap
[[ $sws =~ ^[0-9]+$ ]] && (( sws > jws )) && jws=$sws
[[ $jmax =~ ^[0-9]+$ ]] || refuse "cannot determine the journal's record version"
[[ $jsnap =~ ^[0-9]+$ ]] || refuse "cannot determine the journal's snapshot version"
[[ $jws =~ ^[0-9]+$ ]] || refuse "cannot determine the journal's workspace scope"
bmax=""
bout=$("$bin" --journal-format 2>/dev/null) || {
  echo "format-guard: $bin --journal-format failed — refusing" >&2
  exit 1
}
# "3 2 1" since v1.0.2: records, snapshots, parks-ws≠0. A lone integer
# predates the extra fields: its snapshot capability is the release
# table (record-reader 3 = DRIX 2; older = DRIX 1) and it parks nothing.
read -r bmax bsnap bpark <<<"$bout"
[[ $bmax =~ ^[0-9]+$ ]] || refuse "cannot parse $bin --journal-format: '$bout'"
if [[ -z $bsnap ]]; then bsnap=$(( bmax >= 3 ? 2 : 1 )); fi
bpark=${bpark:-0}
if (( bmax < jmax )); then
  echo "format-guard: REFUSED — the journal holds record version $jmax" \
    "but $bin reads only up to $bmax" >&2
  exit 1
fi
if (( bsnap < jsnap )); then
  echo "format-guard: REFUSED — the journal holds snapshot format $jsnap" \
    "but $bin reads only up to $bsnap" >&2
  exit 1
fi
if (( jws > 0 && bpark < 1 )); then
  echo "format-guard: REFUSED — the journal holds workspace-scoped records" \
    "and $bin cannot park them: it would run tenant work on workspace-0 handlers" >&2
  exit 1
fi
echo "format-guard: journal v$jmax/snap v$jsnap/ws $jws, $bin reads to v$bmax/snap $bsnap — ok"
