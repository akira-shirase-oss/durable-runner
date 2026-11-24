#!/usr/bin/env bash
# neon-setup.sh — Durable Runner's database and role on Neon. Run on the maintainer's machine.
#
#   deploy/droplet/neon-setup.sh --owner-url-file <file> --out <new-or-own-dir> [--rotate]
#
# <file> holds the Neon project's owner connection string, as the dashboard gives it (pooled or
# direct; it is made the direct host over verify-full, neon-url.sh). As that owner (Neon has no
# superuser, AGENTS.md "Deploy readiness" 1):
#   1. CREATE DATABASE durable_runner, if it isn't there;
#   2. CREATE ROLE durable_app LOGIN PASSWORD '<plaintext>', if it isn't there: Neon refuses a SCRAM
#      verifier and hashes the plaintext itself (sent over verify-full). createrole_self_grant =
#      'set, inherit' first, so the owner can SET ROLE to it and manage what it will own;
#   3. in durable_runner: CONNECT and TEMPORARY for durable_app, and none for PUBLIC (another
#      product's role on this Neon project can't connect); pgcrypto, created by the owner, so
#      migration 0001's CREATE EXTENSION IF NOT EXISTS is a no-op for durable_app; USAGE and CREATE
#      on schema public (PostgreSQL 15+ gives PUBLIC no CREATE there), because the server runs its
#      own migrations at startup as durable_app and owns its tables. No superuser construct, no
#      function, no ALTER ... SET, no ALTER OWNER, no role created inside a migration;
#   4. logs in as durable_app over verify-full and checks what it can do.
#
# The app's password is generated here into <out>/app_password and the server's URL written to
# <out>/database_url, both 0400 in a 0700 directory. Put the URL on the droplet:
#   ssh "$DURABLE_DEPLOY_HOST" /opt/durable-build/src/deploy/droplet/put-secret.sh database_url <"<out>/database_url"
# Nothing secret is printed or put on a command line: psql takes each password from its own
# environment (PGPASSWORD) or reads it from the file itself.
#
# A second run changes nothing. If durable_app exists but <out>/app_password doesn't (a lost
# file), it stops; --rotate makes a new password (the old file kept as app_password.prev), sets it
# on the role, and rewrites database_url (then put-secret.sh --replace database_url and restart).
# If a statement holding the password ever fails, Neon's server log may hold its text: rotate.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=deploy/droplet/neon-url.sh
. "$here/neon-url.sh"
DB=durable_runner
ROLE=durable_app
die() { echo "neon-setup: error: $*" >&2; exit 1; }
say() { printf 'neon-setup: %s\n' "$*"; }

owner_file="" out="" rotate=0
while (($#)); do
  case "$1" in
    --owner-url-file) owner_file=${2:-}; shift 2 ;;
    --out) out=${2:-}; shift 2 ;;
    --rotate) rotate=1; shift ;;
    *) echo "usage: neon-setup.sh --owner-url-file <file> --out <dir> [--rotate]" >&2; exit 2 ;;
  esac
done
[[ -n "$owner_file" && -n "$out" ]] || { echo "usage: neon-setup.sh --owner-url-file <file> --out <dir> [--rotate]" >&2; exit 2; }
[[ -r "$owner_file" ]] || die "can't read $owner_file"
command -v psql >/dev/null || die "needs psql 16 or newer (macOS: brew install libpq; Ubuntu: postgresql-client)"
rootcert=${NEON_SSLROOTCERT:-system}
if [[ "$rootcert" == system ]]; then
  # sslrootcert=system (the OS's public roots) is libpq 16+; older ones look for ~/.postgresql/root.crt.
  major=$(psql --version | grep -oE '[0-9]+' | head -1)
  [[ "$major" =~ ^[0-9]+$ ]] && (( major >= 16 )) || die "psql $major can't verify against the system's roots (needs 16+); or set NEON_SSLROOTCERT to a CA bundle file"
fi

url=$(tr -d '\r\n' <"$owner_file")
url=${url#"${url%%[![:space:]]*}"}; url=${url%"${url##*[![:space:]]}"}
fixed=$(neon_url_fix "$url")
for note in "${NEON_URL_NOTES[@]+"${NEON_URL_NOTES[@]}"}"; do say "owner URL: $note"; done
neon_url_split "$fixed" || die "the owner URL isn't postgres://<user>:<password>@<host>/<database>"
owner_user=$NEON_USER owner_pass=$NEON_PASSWORD host=$NEON_HOST port=$NEON_PORT owner_db=$NEON_DB
[[ "$host" == *.neon.tech || -n "${NEON_SETUP_ANY_HOST:-}" ]] || die "$host isn't a Neon host (*.neon.tech)"

mkdir -p "$out"
chmod 0700 "$out"
pw_file=$out/app_password
url_file=$out/database_url

# psql_as <user> <password> <database> [psql args...]: the password in psql's environment only.
psql_as() {
  local user=$1 pass=$2 db=$3; shift 3
  PGHOST=$host PGPORT=$port PGUSER=$user PGPASSWORD=$pass PGDATABASE=$db \
    PGSSLMODE=verify-full PGSSLROOTCERT=$rootcert PGAPPNAME=durable-neon-setup PGCONNECT_TIMEOUT=15 \
    NEON_SETUP_PW_FILE=$pw_file psql -X -q -v ON_ERROR_STOP=1 "$@"
}

have=$(psql_as "$owner_user" "$owner_pass" "$owner_db" -At -c "SELECT count(*) FROM pg_roles WHERE rolname = '$ROLE'") \
  || die "can't connect as the owner over verify-full (above); nothing changed"
[[ "$have" == 0 || "$have" == 1 ]] || die "unexpected answer counting roles: '$have'"

newpw() { (umask 077; head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n' >"$pw_file.tmp" && chmod 0400 "$pw_file.tmp" && mv -f "$pw_file.tmp" "$pw_file"); }
if (( rotate == 1 )); then
  (( have == 1 )) || die "--rotate: $ROLE doesn't exist yet; run without --rotate"
  if [[ -e "$pw_file" ]]; then chmod 0600 "$pw_file"; mv -f "$pw_file" "$pw_file.prev"; chmod 0400 "$pw_file.prev"; fi
  newpw; action=rotate
elif [[ -s "$pw_file" ]]; then
  if (( have == 1 )); then action=keep; else action=create; fi
else
  (( have == 0 )) || die "$ROLE exists on Neon but $pw_file doesn't: its password is unknown here. Rerun with --rotate to set a new one (the droplet's database_url then changes too)"
  newpw; action=create
fi
[[ "$(head -c 64 "$pw_file")" =~ ^[A-Za-z0-9_-]{32,}$ ]] || die "$pw_file doesn't hold a generated password"

psql_as "$owner_user" "$owner_pass" "$owner_db" -v action="$action" -v db="$DB" -v role="$ROLE" -f - <<'SQL' \
  || die "the setup statements failed (above). If it was the CREATE/ALTER ROLE, rotate: rerun with --rotate"
SELECT NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db') AS need_db \gset
\if :need_db
CREATE DATABASE :"db";
\echo neon-setup: database created
\else
\echo neon-setup: database present
\endif
SELECT :'action' = 'create' AS do_create, :'action' = 'rotate' AS do_rotate \gset
\set app_pw `cat "$NEON_SETUP_PW_FILE"`
\if :do_create
SET createrole_self_grant = 'set, inherit';
CREATE ROLE :"role" WITH LOGIN PASSWORD :'app_pw';
\echo neon-setup: role created
\elif :do_rotate
ALTER ROLE :"role" WITH LOGIN PASSWORD :'app_pw';
\echo neon-setup: role password rotated
\else
\echo neon-setup: role present, password kept
\endif
\unset app_pw
\c :db
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db" TO :"role";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
GRANT USAGE, CREATE ON SCHEMA public TO :"role";
\echo neon-setup: grants in place (CONNECT, TEMPORARY; USAGE and CREATE on public; pgcrypto)
SQL

app_pass=$(cat "$pw_file")
check=$(psql_as "$ROLE" "$app_pass" "$DB" -At -c "SELECT current_user, has_database_privilege('$DB', 'CONNECT'), has_schema_privilege('public', 'CREATE'), has_database_privilege('public', '$DB', 'CONNECT'), (SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto')") \
  || die "$ROLE can't log in to $DB over verify-full (above)"
[[ "$check" == "$ROLE|t|t|f|1" ]] || die "$ROLE's rights aren't what the migrations need: '$check' (want $ROLE|t|t|f|1: connect, create in public, PUBLIC kept out, pgcrypto present)"
say "checked: $ROLE logs in over verify-full, can create in public, PUBLIC can't connect, pgcrypto present"

hostport=$host; [[ "$port" == 5432 ]] || hostport=$host:$port
want="postgres://$ROLE:$app_pass@$hostport/$DB?sslmode=verify-full"
unset app_pass
if [[ -s "$url_file" ]] && [[ "$(cat "$url_file")" == "$want" ]]; then
  say "$url_file: unchanged"
else
  if [[ -e "$url_file" ]]; then chmod 0600 "$url_file"; mv -f "$url_file" "$url_file.prev"; chmod 0400 "$url_file.prev"; fi
  (umask 077; printf '%s' "$want" >"$url_file.tmp" && chmod 0400 "$url_file.tmp" && mv -f "$url_file.tmp" "$url_file")
  say "$url_file: written (0400)"
fi
unset want
say "done: database $DB, role $ROLE ($action). Next: ssh \"\$DURABLE_DEPLOY_HOST\" <checkout>/deploy/droplet/put-secret.sh$([[ $action == rotate ]] && echo ' --replace') database_url <$url_file"
