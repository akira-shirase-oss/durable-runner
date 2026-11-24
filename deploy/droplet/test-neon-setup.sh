#!/usr/bin/env bash
# neon-setup.sh against a fake psql that records its argv, its libpq environment and the SQL it
# is fed (no database is reached). It checks what the script controls:
#   - the owner URL becomes Neon's direct host over verify-full with the system's roots;
#   - no password (the owner's or the app's) is in any argv or in what the script prints;
#   - the SQL is the Neon-safe set: createrole_self_grant before CREATE ROLE, a plaintext password
#     through a psql variable (never a SCRAM literal), PUBLIC kept out, pgcrypto by the owner,
#     USAGE+CREATE on public, and nothing a non-superuser can't run;
#   - the app password and the server's URL land 0400 in a 0700 directory, and a second run keeps
#     them; a lost password file with the role present stops; --rotate keeps .prev copies;
#   - an owner URL that isn't Neon's or isn't postgres, an old psql, or a role whose rights don't
#     check out, stop it.
# What only real Neon can prove (that each statement is accepted there) is the coordinator's
# rehearsal: docs/DEPLOY.md, "Neon".
#
#   deploy/droplet/test-neon-setup.sh
# shellcheck disable=SC2015  # ok and bad only print (both return 0), so `t && ok || bad` is if-then-else here.
set -euo pipefail
kit=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/dr-neon-test.XXXXXX")
trap 'rm -rf "$work"' EXIT
fails=0
ok() { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fails=$((fails + 1)); }
perm() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }

mkdir -p "$work/bin" "$work/log"
cat >"$work/bin/psql" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == --version ]]; then echo "psql (PostgreSQL) ${FAKE_PSQL_MAJOR:-17}.2"; exit 0; fi
n=$(ls "$FAKE_LOG" | wc -l | tr -d ' '); d=$FAKE_LOG/call-$n; mkdir -p "$d"
printf '%s\n' "$@" >"$d/argv"
for v in PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE PGSSLMODE PGSSLROOTCERT NEON_SETUP_PW_FILE; do printf '%s=%s\n' "$v" "${!v:-}" >>"$d/env"; done
case " $* " in
  *" -f - "*)
    cat >"$d/sql"
    # As psql's backtick would: the password file the script pointed at.
    cat "$NEON_SETUP_PW_FILE" >"$d/pw-seen"
    exit "${FAKE_PSQL_SETUP_RC:-0}" ;;
  *"FROM pg_roles"*) cat "$FAKE_STATE/have_role" 2>/dev/null || echo 0 ;;
  *"current_user"*) echo "${FAKE_PSQL_CHECK:-durable_app|t|t|f|1}" ;;
esac
EOF
chmod +x "$work/bin/psql"
OWNER_PASS='Own3rPa55-wordX'
printf 'postgresql://neondb_owner:%s@ep-cool-name-123456-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require\n' "$OWNER_PASS" >"$work/owner"
out=$work/out
# neon [args...]: one run; calls recorded under a fresh log directory.
neon() {
  rm -rf "$work/log" "$work/state"; mkdir -p "$work/log" "$work/state"
  PATH="$work/bin:$PATH" FAKE_LOG="$work/log" FAKE_STATE="$work/state" "$@" >"$work/run.out" 2>&1
}
argv_all() { cat "$work"/log/*/argv 2>/dev/null; }
call_env() { sed -n "s/^$2=//p" "$work/log/call-$1/env"; }

echo "a first run"
neon "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$out" && ok "succeeds" || bad "failed: $(cat "$work/run.out")"
pw=$(cat "$out/app_password" 2>/dev/null || true)
[[ "$pw" =~ ^[A-Za-z0-9_-]{43}$ ]] && ok "app_password: 43 random URL-safe characters" || bad "app_password: '${pw:0:4}...' (${#pw})"
[[ "$(perm "$out")" == 700 && "$(perm "$out/app_password")" == 400 && "$(perm "$out/database_url")" == 400 ]] && ok "0400 files in a 0700 directory" || bad "modes: $(perm "$out") $(perm "$out/app_password") $(perm "$out/database_url")"
want="postgres://durable_app:$pw@ep-cool-name-123456.us-east-2.aws.neon.tech/durable_runner?sslmode=verify-full"
[[ "$(cat "$out/database_url")" == "$want" ]] && ok "database_url: durable_app on the direct host, durable_runner, verify-full" || bad "database_url isn't the direct verify-full URL"
[[ "$(call_env 0 PGHOST)" == ep-cool-name-123456.us-east-2.aws.neon.tech && "$(call_env 0 PGSSLMODE)" == verify-full && "$(call_env 0 PGSSLROOTCERT)" == system ]] \
  && ok "psql connects to the direct host, verify-full against the system's roots" || bad "psql env: $(cat "$work/log/call-0/env" | grep -v PASSWORD | tr '\n' ' ')"
[[ "$(call_env 0 PGPASSWORD)" == "$OWNER_PASS" && "$(call_env 2 PGUSER)" == durable_app && "$(call_env 2 PGPASSWORD)" == "$pw" ]] \
  && ok "each password travels in psql's environment (owner for setup, app for the check)" || bad "passwords didn't reach psql's environment"
if argv_all | grep -qF -e "$OWNER_PASS" -e "$pw"; then bad "a password is in psql's argv"; else ok "no password in any argv"; fi
if grep -qF -e "$OWNER_PASS" -e "$pw" "$work/run.out"; then bad "a password was printed"; else ok "no password printed"; fi
[[ "$(cat "$work/log/call-1/pw-seen")" == "$pw" ]] && ok "psql reads the app password from the file itself" || bad "the SQL's password file isn't app_password"
grep -q 'channel_binding dropped' "$work/run.out" && grep -q 'pooler host became the direct one' "$work/run.out" && ok "says what it changed in the owner URL" || bad "no URL notes"
argv1=$(cat "$work/log/call-1/argv")
grep -qx 'action=create' <<<"$argv1" && ok "action=create for a new role" || bad "action: $(grep action <<<"$argv1")"
sql=$work/log/call-1/sql
order_ok=$(awk '/SET createrole_self_grant = .set, inherit.;/{s=NR} /CREATE ROLE :"role" WITH LOGIN PASSWORD :.app_pw.;/{c=NR} END{print (s && c && s < c) ? "yes" : "no"}' "$sql")
[[ "$order_ok" == yes ]] && ok "createrole_self_grant 'set, inherit' is set before CREATE ROLE" || bad "createrole_self_grant isn't set before CREATE ROLE"
for stmt in 'CREATE DATABASE :"db";' "REVOKE ALL ON DATABASE :\"db\" FROM PUBLIC;" 'GRANT CONNECT, TEMPORARY ON DATABASE :"db" TO :"role";' \
  'CREATE EXTENSION IF NOT EXISTS pgcrypto;' 'GRANT USAGE, CREATE ON SCHEMA public TO :"role";' '\c :db'; do
  grep -qxF -- "$stmt" "$sql" || bad "the SQL lacks: $stmt"
done
ok "the SQL creates the database, keeps PUBLIC out, grants CONNECT/TEMPORARY and USAGE+CREATE on public, makes pgcrypto"
if grep -qiE 'SUPERUSER|ALTER SYSTEM|CREATE (OR REPLACE )?FUNCTION|OWNER TO|SCRAM-SHA-256|ALTER (DATABASE|ROLE) [^ ]+ SET |GRANT SET ON PARAMETER' "$sql"; then
  bad "the SQL uses something Neon's owner can't: $(grep -iE 'SUPERUSER|ALTER SYSTEM|FUNCTION|OWNER TO|SCRAM|SET ON PARAMETER' "$sql" | head -2)"
else
  ok "no superuser construct, function, ALTER ... SET, OWNER TO or SCRAM literal"
fi
grep -qF "PASSWORD '" "$sql" && bad "a literal password is in the SQL text" || ok "the password reaches SQL only as :'app_pw'"

echo "a second run"
sum1=$(cat "$out/app_password" "$out/database_url")
echo 1 >"$work/state/have_role"
cp "$work/state/have_role" "$work/have"
neon_keep() { rm -rf "$work/log"; mkdir -p "$work/log"; PATH="$work/bin:$PATH" FAKE_LOG="$work/log" FAKE_STATE="$work/statek" "$@" >"$work/run.out" 2>&1; }
mkdir -p "$work/statek"; echo 1 >"$work/statek/have_role"
neon_keep "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$out" && ok "succeeds" || bad "failed: $(cat "$work/run.out")"
[[ "$(cat "$out/app_password" "$out/database_url")" == "$sum1" ]] && ok "keeps the password and the URL" || bad "the files changed"
grep -qx 'action=keep' "$work/log/call-1/argv" && grep -q 'unchanged' "$work/run.out" && ok "action=keep; database_url unchanged" || bad "second-run action: $(grep action "$work/log/call-1/argv")"

echo "a lost password file"
mv "$out/app_password" "$work/saved_pw"
neon_keep "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$out" && bad "carried on without the role's password" \
  || { grep -q -- '--rotate' "$work/run.out" && [[ ! -e "$out/app_password" ]] && ok "stops, asks for --rotate, leaves no new password behind" || bad "lost-file refusal: $(cat "$work/run.out")"; }
[[ ! -e "$work/log/call-1" ]] && ok "and ran no setup statement" || bad "it ran the setup anyway"
mv "$work/saved_pw" "$out/app_password"

echo "--rotate"
neon_keep "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$out" --rotate && ok "succeeds" || bad "failed: $(cat "$work/run.out")"
new=$(cat "$out/app_password")
[[ "$new" != "$pw" && "$(cat "$out/app_password.prev")" == "$pw" && "$(cat "$out/database_url.prev")" == "$want" ]] && ok "a new password; the old password and URL kept as .prev" || bad "rotation files"
grep -qF "durable_app:$new@" "$out/database_url" && grep -qx 'action=rotate' "$work/log/call-1/argv" && ok "action=rotate; database_url carries the new password" || bad "rotation didn't reach the URL/SQL"

echo "refusals"
printf 'mysql://u:p@ep-x.aws.neon.tech/db\n' >"$work/owner-bad"
neon "$kit/neon-setup.sh" --owner-url-file "$work/owner-bad" --out "$work/o2" && bad "accepted a non-postgres URL" || ok "a non-postgres owner URL"
printf 'postgres://u:p@db.example.com/db\n' >"$work/owner-bad"
neon "$kit/neon-setup.sh" --owner-url-file "$work/owner-bad" --out "$work/o2" && bad "accepted a non-Neon host" || ok "a host that isn't Neon's"
neon env FAKE_PSQL_MAJOR=15 "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$work/o3" && bad "ran with psql 15" \
  || { grep -q '16' "$work/run.out" && ok "psql older than 16 (no sslrootcert=system)" || bad "psql 15 refused for another reason"; }
neon env FAKE_PSQL_CHECK='durable_app|t|f|f|1' "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$work/o4" && bad "accepted a role that can't create in public" \
  || { grep -q "rights aren't" "$work/run.out" && ok "a role whose rights don't check out" || bad "rights refusal: $(cat "$work/run.out")"; }
[[ ! -e "$work/o4/database_url" ]] && ok "and wrote no database_url for it" || bad "wrote database_url despite the failed check"
neon env FAKE_PSQL_SETUP_RC=3 "$kit/neon-setup.sh" --owner-url-file "$work/owner" --out "$work/o5" && bad "carried on after the SQL failed" || ok "a failed setup statement"

if (( fails )); then echo "test-neon-setup: $fails FAILED" >&2; exit 1; fi
echo "test-neon-setup: all green"
