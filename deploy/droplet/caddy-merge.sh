#!/usr/bin/env bash
# caddy-merge.sh — Durable Runner's site into the shared droplet's Caddy, nothing else changed.
#
#   sudo deploy/droplet/caddy-merge.sh
#
# The root /etc/caddy/Caddyfile is Colophon's (its installer writes it whole, global options
# included). This script:
#   1. checks the root already trusts Cloudflare the way this site relies on (the servers block
#      imports cloudflare-trusted.caddy and resolves the client from Cf-Connecting-Ip), that the
#      origin-pull trust and our own origin certificate are in place, and that nothing else
#      already serves our host;
#   2. renders durable-runner.caddy (host and dashboard redirect from /etc/durable/durable.env)
#      and installs it as /etc/caddy/sites/durable-runner.caddy;
#   3. appends exactly one `import /etc/caddy/sites/*.caddy` to the root, only if it's absent;
#   4. keeps a timestamped backup of both files (/var/backups/durable-runner/caddy/<time>/),
#      runs `caddy validate`, and only then `systemctl reload caddy`. A refused validation or
#      reload puts both files back as they were (a failed reload leaves Caddy on its old config,
#      so the disk matches what runs again).
# A second run with nothing to change touches nothing and calls nothing.
#
# Colophon's installer rewrites the root and would drop the import line (docs/DEPLOY.md, "The
# Colophon side"); smoke.sh says so when it happens, and running this again puts it back.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=deploy/droplet/lib.sh
. "$here/lib.sh"
[[ $EUID -eq 0 || -n "$ROOT" ]] || die "run as root"

root_file=$CADDY_DIR/Caddyfile
sites=$CADDY_DIR/sites
site=$sites/durable-runner.caddy
IMPORT_LINE='import /etc/caddy/sites/*.caddy'
backups=$ROOT/var/backups/durable-runner/caddy
KEEP_BACKUPS=10

# 1. What must already be true.
[[ -s "$root_file" ]] || die "no $root_file: the root Caddyfile (Colophon's installer writes it) must exist first; this script never writes global options"
grep -qE '^[[:space:]]*import[[:space:]]+/etc/caddy/cloudflare-trusted\.caddy[[:space:]]*$' "$root_file" \
  || die "$root_file doesn't import /etc/caddy/cloudflare-trusted.caddy in its servers block: every visitor would be keyed as Cloudflare's edge; nothing changed"
grep -qiE '^[[:space:]]*client_ip_headers[[:space:]]+Cf-Connecting-Ip[[:space:]]*$' "$root_file" \
  || die "$root_file doesn't resolve the client from Cf-Connecting-Ip (client_ip_headers); nothing changed"
[[ -s "$CADDY_DIR/cloudflare-trusted.caddy" ]] || die "$CADDY_DIR/cloudflare-trusted.caddy is missing or empty (Colophon's cloudflare-ips.sh writes it); nothing changed"
[[ -s "$CADDY_DIR/origin-pull-trust.caddy" ]] || die "$CADDY_DIR/origin-pull-trust.caddy is missing (Colophon's origin-pull-ca.sh writes it): no Authenticated Origin Pulls without it; nothing changed"
for f in durable-origin.pem durable-origin.key; do
  [[ -s "$CADDY_DIR/$f" ]] || die "$CADDY_DIR/$f is missing: put Cloudflare's origin certificate for our host first (put-secret.sh origin_pem / origin_key); nothing changed"
done
[[ -s "$DR_ENV" ]] || die "$DR_ENV is missing: run install.sh first (the host and dashboard path come from it)"
origin=$(env_get DURABLE_PUBLIC_ORIGIN "$DR_ENV")
host=$(origin_host "$origin") || die "fix DURABLE_PUBLIC_ORIGIN in $DR_ENV; nothing changed"
dash=$(env_get DURABLE_DASHBOARD_PATH "$DR_ENV")
dash=${dash:-$DR_DASHBOARD_SHIPPED}
# Our host served from the root (or any file other than ours) would be a duplicate site.
if grep -qE "^[[:space:]]*(https://)?${host//./\\.}(:443)?([[:space:],{]|$)" "$root_file"; then
  die "$root_file already has a site for $host: remove it there (this site belongs in $site); nothing changed"
fi
imports=$(grep -cxF -- "$IMPORT_LINE" "$root_file" || true)
is_uint "$imports" || die "can't count the import lines in $root_file"
(( imports <= 1 )) || die "$root_file has $imports lines '$IMPORT_LINE': each site would load twice; leave one, then run this again"

rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT
render_site "$here/durable-runner.caddy" "$host" "$dash" >"$rendered" || die "can't render the site file"

site_same=0
[[ -f "$site" ]] && cmp -s "$rendered" "$site" && site_same=1
if (( site_same == 1 && imports == 1 )); then
  say "Caddy: already merged ($site current, the root imports sites/*.caddy once); nothing to do"
  exit 0
fi

# 2. The backup.
ts=$(date -u +%Y%m%dT%H%M%SZ)
bdir=$backups/$ts
[[ ! -e "$bdir" ]] || bdir=$bdir-$$
mkdir -p "$backups"
chmod 0700 "$ROOT/var/backups/durable-runner" "$backups" 2>/dev/null || true
mkdir -m 0700 "$bdir"
cp -p "$root_file" "$bdir/Caddyfile"
if [[ -f "$site" ]]; then cp -p "$site" "$bdir/durable-runner.caddy"; else : >"$bdir/no-site-file"; fi
say "backup: $bdir"
# Only the newest backups are kept (the names sort by time).
all=$(find "$backups" -mindepth 1 -maxdepth 1 -type d | sort)
n=$(grep -c . <<<"$all" || true)
if is_uint "$n" && (( n > KEEP_BACKUPS )); then
  while IFS= read -r d; do [[ -n "$d" ]] && rm -rf "$d"; done < <(head -n "$((n - KEEP_BACKUPS))" <<<"$all")
fi

restore() {
  local tmp
  tmp=$(mktemp "$CADDY_DIR/.dr-restore.XXXXXX")
  cp -p "$bdir/Caddyfile" "$tmp" && mv -f "$tmp" "$root_file"
  if [[ -f "$bdir/durable-runner.caddy" ]]; then
    tmp=$(mktemp "$sites/.dr-restore.XXXXXX")
    cp -p "$bdir/durable-runner.caddy" "$tmp" && mv -f "$tmp" "$site"
  else
    rm -f "$site"
  fi
  cmp -s "$bdir/Caddyfile" "$root_file" || { echo "error: the root Caddyfile could NOT be restored from $bdir: copy it back by hand" >&2; return 1; }
  say "restored $root_file and $site from $bdir"
}

# 3. The change: our site file, then the import line.
ensure_dir "$sites" 0755
if put_file "$rendered" "$site" 0644; then say "site file: $site ($host, / -> $dash)"; fi
if (( imports == 0 )); then
  tmp=$(mktemp "$CADDY_DIR/.dr-root.XXXXXX")
  cp -p "$root_file" "$tmp"
  [[ -z "$(tail -c 1 "$tmp")" ]] || printf '\n' >>"$tmp"
  printf '\n# One site file per product on this shared droplet (Durable Runner deploy/droplet/caddy-merge.sh).\n%s\n' "$IMPORT_LINE" >>"$tmp"
  mv -f "$tmp" "$root_file"
  say "root: appended '$IMPORT_LINE'"
fi

# 4. Validate, then reload; either refusing puts everything back.
if ! out=$(caddy validate --config "$root_file" --adapter caddyfile 2>&1); then
  printf '%s\n' "$out" | tail -n 8 >&2
  restore || exit 1
  die "caddy validate refused the merged configuration (above); both files are back as they were and Caddy wasn't reloaded"
fi
say "caddy validate: ok"
if ! systemctl reload caddy; then
  restore || exit 1
  die "systemctl reload caddy failed; Caddy keeps its previous configuration, and both files are back to match it"
fi
say "caddy reloaded: $host is served from $site"
