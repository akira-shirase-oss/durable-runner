#!/usr/bin/env bash
# durable-saas-run — the hosted service's ExecStart (deploy/droplet/durable-server.service).
#
# The server reads its secrets from its environment, never argv (argv is visible to every local
# user). systemd hands them over as files in $CREDENTIALS_DIRECTORY (LoadCredential=, from the
# root-only /etc/durable/credentials); this wrapper exports each under the name the server reads,
# checks the settings that would make a hosted deploy unsafe, adds the hosted flags, and execs
# durable-run beside it (/usr/local/bin; deploy/durable-run.sh in a checkout), which builds the
# rest of argv (listen, journal, limits, the loopback hop, deny-cidrs, handlers), runs the lab
# guard on the binary and execs durable-server. It prints no secret.
#
#   credential       variable           required when
#   api_key          DURABLE_API_KEY    always (the ops plane's write key, >= 32 characters)
#   read_key         DURABLE_READ_KEY   optional
#   database_url     DATABASE_URL       DURABLE_REQUIRE_ACCOUNTS=1 (Neon, verify-full, direct host)
#   resend_api_key   RESEND_API_KEY     DURABLE_REQUIRE_MAIL=1 (with DURABLE_MAIL_FROM)
#
# Hosted flags, from settings, each only when the binary's --help lists it: --public-origin
# "$DURABLE_PUBLIC_ORIGIN" (required with an account store by a server that has it), and
# --ops-listen "$DURABLE_OPS_LISTEN" when that is set (durable.env.template). Refused: DURABLE_MAILER=log (it prints mail bodies, tokens
# included, to the journal) and DURABLE_ACCOUNTS_BACKEND (an in-memory account store).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
refuse() { echo "durable-saas-run: refusing to start: $*" >&2; exit 78; }  # 78: EX_CONFIG

creds=${CREDENTIALS_DIRECTORY:-}
[[ -n "$creds" && -d "$creds" ]] || refuse "no CREDENTIALS_DIRECTORY: start this through systemd (LoadCredential=), not by hand"

need_accounts=${DURABLE_REQUIRE_ACCOUNTS:-1}
need_mail=${DURABLE_REQUIRE_MAIL:-1}
[[ "$need_accounts" == 0 || "$need_accounts" == 1 ]] || refuse "DURABLE_REQUIRE_ACCOUNTS must be 0 or 1"
[[ "$need_mail" == 0 || "$need_mail" == 1 ]] || refuse "DURABLE_REQUIRE_MAIL must be 0 or 1"

# load <credential> <variable> <required 0|1>: one line, exported; its value is never printed.
load() {
  local f=$creds/$1 v
  if [[ ! -s "$f" ]]; then
    [[ "$3" == 0 ]] || refuse "the $1 credential is missing or empty (put-secret.sh $1)"
    return 0
  fi
  v=$(<"$f")
  [[ -n "$v" && "$v" != *$'\n'* ]] || refuse "the $1 credential must be one non-empty line"
  export "$2=$v"
}
load api_key DURABLE_API_KEY 1
load read_key DURABLE_READ_KEY 0
load database_url DATABASE_URL "$need_accounts"
load resend_api_key RESEND_API_KEY "$need_mail"

if [[ -n "${DATABASE_URL:-}" ]]; then
  [[ "$DATABASE_URL" =~ ^postgres(ql)?://[^@/]+@([^/:?]+)(:[0-9]+)?/[^?]+\?(.*)$ ]] \
    || refuse "database_url isn't postgres://<user>:<password>@<host>/<database>?sslmode=verify-full"
  db_host=${BASH_REMATCH[2]} db_query=${BASH_REMATCH[4]}
  [[ "&$db_query&" == *"&sslmode=verify-full&"* ]] \
    || refuse "database_url must say sslmode=verify-full: require encrypts without checking who answers"
  [[ "$db_host" != *-pooler.* ]] \
    || refuse "database_url names Neon's pooled host ($db_host); use the direct one (drop -pooler)"
  [[ "&$db_query&" != *"&channel_binding="* ]] \
    || refuse "database_url carries channel_binding, which the driver doesn't do; drop it (neon-setup.sh writes the URL without it)"
fi
if [[ -n "${RESEND_API_KEY:-}" ]]; then
  [[ "$RESEND_API_KEY" =~ ^re_[A-Za-z0-9_]+$ ]] || refuse "resend_api_key doesn't look like a Resend key (re_...)"
  [[ -n "${DURABLE_MAIL_FROM:-}" ]] || refuse "RESEND_API_KEY is set but DURABLE_MAIL_FROM isn't (durable.env)"
fi
[[ "${DURABLE_MAILER:-}" != log ]] || refuse "DURABLE_MAILER=log prints every mail body, sign-in links included, to the journal: never on a hosted deploy"
[[ -z "${DURABLE_ACCOUNTS_BACKEND:-}" ]] || refuse "DURABLE_ACCOUNTS_BACKEND=${DURABLE_ACCOUNTS_BACKEND} keeps accounts in memory only: never on a hosted deploy"

# Invite-only: the server tests presence alone, so an "off" value would turn it on.
if [[ -n "${DURABLE_INVITE_ONLY+set}" ]]; then
  case "$(printf '%s' "$DURABLE_INVITE_ONLY" | tr '[:upper:]' '[:lower:]')" in
    ""|0|false|off|no) unset DURABLE_INVITE_ONLY; echo "durable-saas-run: invite-only sign-up is off" >&2 ;;
    1|true|on|yes) export DURABLE_INVITE_ONLY=1; echo "durable-saas-run: invite-only sign-up is ON" >&2 ;;
    *) refuse "DURABLE_INVITE_ONLY='$DURABLE_INVITE_ONLY': use 1 (on), or remove the line (off)" ;;
  esac
fi

# The hosted flags go only to a server that has them: clap refuses an unknown flag, and main's
# server can predate S3's (--public-origin is on the S3 branch; --ops-listen isn't written yet).
# The binary's own --help says, as the lab guard reads it.
bin=${DURABLE_SERVER_BIN:-/usr/local/bin/durable-server}
help=$("$bin" --help 2>&1 || true)
has_flag() { grep -qE -- "(^|[[:space:],])$1([[:space:]=,<]|$)" <<<"$help"; }
extra=""
origin=${DURABLE_PUBLIC_ORIGIN:-}
if [[ -n "$origin" ]]; then
  origin=${origin%/}
  [[ "$origin" =~ ^https://([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] \
    || refuse "DURABLE_PUBLIC_ORIGIN='$origin' must be https://<host>, nothing else"
  if has_flag --public-origin; then
    extra="--public-origin $origin"
  else
    echo "durable-saas-run: this server has no --public-origin yet (it predates S3); not passed" >&2
  fi
elif [[ -n "${DATABASE_URL:-}" ]] && has_flag --public-origin; then
  refuse "an account store needs DURABLE_PUBLIC_ORIGIN: mailed links are built from it"
fi
if [[ -n "${DURABLE_OPS_LISTEN:-}" ]]; then
  [[ "$DURABLE_OPS_LISTEN" =~ ^(127\.0\.0\.1|\[::1\]):[0-9]{1,5}$ ]] \
    || refuse "DURABLE_OPS_LISTEN='$DURABLE_OPS_LISTEN' must be a loopback address:port (the ops keys answer there)"
  [[ "$DURABLE_OPS_LISTEN" != "${DURABLE_LISTEN:-127.0.0.1:58300}" ]] || refuse "DURABLE_OPS_LISTEN can't be the public listener's address"
  if has_flag --ops-listen; then
    extra="$extra --ops-listen $DURABLE_OPS_LISTEN"
  else
    echo "durable-saas-run: WARNING DURABLE_OPS_LISTEN is set but this server has no --ops-listen: not passed, so the ops keys still answer on the public listener" >&2
  fi
fi
case " ${DURABLE_EXTRA_ARGS:-} " in
  *" --public-origin"*|*" --ops-listen"*) refuse "DURABLE_EXTRA_ARGS repeats a flag this wrapper sets (--public-origin / --ops-listen): use DURABLE_PUBLIC_ORIGIN / DURABLE_OPS_LISTEN" ;;
esac
# durable-run word-splits DURABLE_EXTRA_ARGS; every value added here was checked above to hold no
# space or glob character.
DURABLE_EXTRA_ARGS="$extra ${DURABLE_EXTRA_ARGS:-}"
export DURABLE_EXTRA_ARGS
run=$here/durable-run
[[ -x "$run" ]] || run=$here/../durable-run.sh
[[ -x "$run" ]] || refuse "no durable-run beside $0"
exec "$run"
