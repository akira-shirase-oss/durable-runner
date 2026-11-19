#!/usr/bin/env bash
# durable-backup — named journal backup via the API, tar+age, upload to R2.
# Daily via durable-backup.timer; the R2 bucket has a 7-day object lock
# (CREATE.md), so what lands can't be deleted or changed for a week.
#
#   durable-backup            # backup now, upload, prune >14 days old
#   durable-backup latest     # newest upload and its age in hours (for smoke)
set -euo pipefail
ENV=/etc/durable/durable.env
[[ -r $ENV ]] || { echo "durable-backup: $ENV unreadable" >&2; exit 1; }
# shellcheck disable=SC1090
. "$ENV"
R2_REMOTE="${R2_REMOTE:-r2:durable-backups}"
RECIP=/etc/durable/backup-age-recipient
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

if [[ "${1:-}" == latest ]]; then
  newest=$(rclone lsf "$R2_REMOTE/daily/" --max-depth 1 2>/dev/null | sort | tail -1 || true)
  [[ -n "$newest" ]] || { echo none; exit 0; }
  epoch=$(rclone lsl "$R2_REMOTE/daily/$newest" 2>/dev/null | awk '{print $2" "$3}' | xargs -I{} date -d {} +%s 2>/dev/null || echo 0)
  echo "$newest $(( ($(date +%s) - epoch) / 3600 ))"
  exit 0
fi

name="backup-$(date -u +%Y%m%dT%H%MZ)"
key=$(tr -d '\n' </etc/durable/api-key)
# The server copies the journal under --backup-root atomically.
# DURABLE_LISTEN is 127.0.0.1:PORT on the droplet; curl it directly.
code=$(curl -sS -o "$STAGE/backup.json" -w '%{http_code}' --max-time 600 \
  -X POST "http://${DURABLE_LISTEN}/admin/backup" \
  -H "authorization: Bearer $key" -H 'content-type: application/json' \
  -d "{\"name\":\"$name\"}")
[[ "$code" == 200 ]] || { echo "durable-backup: /admin/backup $code: $(cat "$STAGE/backup.json")" >&2; exit 1; }

tar -C "$DURABLE_BACKUP_ROOT" -cf "$STAGE/$name.tar" "$name"
age -R "$RECIP" -o "$STAGE/$name.tar.age" "$STAGE/$name.tar"
rclone copyto "$STAGE/$name.tar.age" "$R2_REMOTE/daily/$name.tar.age"
echo "durable-backup: $name.tar.age uploaded ($(du -h "$STAGE/$name.tar.age" | cut -f1))"

# Local staging and uploads older than 14 days go (the 7-day bucket lock
# protects the newest week regardless).
rm -rf "${DURABLE_BACKUP_ROOT:?}/$name"
rclone lsf "$R2_REMOTE/daily/" --max-depth 1 | while read -r f; do
  d=${f#backup-}; d=${d%%T*}
  [[ "$(date -d "$d" +%s 2>/dev/null || echo 0)" -lt "$(date -d '14 days ago' +%s)" ]] \
    && rclone delete "$R2_REMOTE/daily/$f" 2>/dev/null || true
done
