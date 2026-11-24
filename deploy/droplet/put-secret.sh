#!/usr/bin/env bash
# put-secret.sh [--replace] <name> — one secret, read from stdin, into its root-only file.
#
# Never an argument or an environment variable: from the maintainer's machine,
#   ssh "$DURABLE_DEPLOY_HOST" /opt/durable-build/src/deploy/droplet/put-secret.sh database_url <neon-out/database_url
# (or paste it on the droplet: nothing is echoed; end with Enter and Ctrl-D).
#
#   name            file                                     owner, mode   checked
#   database_url    /etc/durable/credentials/database_url    root  0400    postgres URL, verify-full, Neon's direct host
#   resend_api_key  /etc/durable/credentials/resend_api_key  root  0400    re_...
#   api_key         /etc/durable/credentials/api_key         root  0400    >= 32 of [A-Za-z0-9_-] (install.sh makes it)
#   read_key        /etc/durable/credentials/read_key        root  0400    the same, and not the api_key
#   origin_pem      /etc/caddy/durable-origin.pem            root:caddy 0440  a certificate for our host, not expired, matching the key
#   origin_key      /etc/caddy/durable-origin.key            root:caddy 0440  a private key, matching the certificate
#
# An existing file is changed only with --replace, which keeps the old one as <file>.prev; the
# same content again is a no-op. A value that fails its check writes nothing. Nothing secret is
# printed. Colophon's /root/put-secret writes Colophon's own files; this is Durable Runner's.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=deploy/droplet/lib.sh
. "$here/lib.sh"
[[ $EUID -eq 0 || -n "$ROOT" ]] || die "run as root"

replace=0
if [[ "${1:-}" == --replace ]]; then replace=1; shift; fi
name=${1:-}
case "$name" in
  database_url|resend_api_key|api_key|read_key) dir=$DR_CREDS mode=0400 group="" line=1 ;;
  origin_pem) dir=$CADDY_DIR file=durable-origin.pem mode=0440 group=caddy line=0 ;;
  origin_key) dir=$CADDY_DIR file=durable-origin.key mode=0440 group=caddy line=0 ;;
  *) echo "usage: put-secret.sh [--replace] database_url|resend_api_key|api_key|read_key|origin_pem|origin_key  (the value on stdin)" >&2; exit 2 ;;
esac
file=${file:-$name}
target=$dir/$file
[[ -d "$dir" ]] || die "$dir doesn't exist (install.sh makes $DR_CREDS; Caddy's package makes $CADDY_DIR)"
if [[ "$dir" == "$DR_CREDS" && "$(mode_of "$dir")" != 700 ]]; then die "$dir isn't 0700; run install.sh"; fi

umask 077
tmp=$(mktemp "$dir/.dr-secret.XXXXXX")
trap 'rm -f "$tmp" "$tmp.pub1" "$tmp.pub2"' EXIT
if [[ -t 0 ]]; then
  printf 'Paste %s, then press Enter and Ctrl-D (nothing is shown):\n' "$name" >&2
  stty -echo 2>/dev/null || true
  cat >"$tmp"; stty echo 2>/dev/null || true
else
  cat >"$tmp"
fi
if (( line == 1 )); then
  # One line: whitespace at the ends (a pasted newline, a CR) is dropped; anything else isn't.
  v=$(tr -d '\r' <"$tmp")
  v=${v#"${v%%[![:space:]]*}"}; v=${v%"${v##*[![:space:]]}"}
  [[ -n "$v" ]] || die "$name is empty; nothing written"
  [[ "$v" != *$'\n'* ]] || die "$name must be one line; nothing written"
  printf '%s' "$v" >"$tmp"
fi

pub_of_cert() { openssl x509 -noout -pubkey -in "$1" 2>/dev/null | openssl sha256 2>/dev/null | sed 's/^.*= *//'; }
pub_of_key() { openssl pkey -pubout -in "$1" 2>/dev/null | openssl sha256 2>/dev/null | sed 's/^.*= *//'; }
case "$name" in
  database_url)
    [[ "$v" =~ ^postgres(ql)?://[^:@/]+:[^@/]+@([^/:?]+)(:[0-9]+)?/[^?]+\?(.*)$ ]] \
      || die "database_url must be postgres://<user>:<password>@<host>/<database>?sslmode=verify-full; nothing written"
    h=${BASH_REMATCH[2]} q=${BASH_REMATCH[4]}
    [[ "&$q&" == *"&sslmode=verify-full&"* ]] || die "database_url must say sslmode=verify-full; nothing written"
    [[ "$h" != *-pooler.* ]] || die "database_url names Neon's pooled host; use the direct one (neon-setup.sh writes it); nothing written"
    [[ "&$q&" != *"&channel_binding="* ]] || die "database_url carries channel_binding; drop it; nothing written" ;;
  resend_api_key)
    [[ "$v" =~ ^re_[A-Za-z0-9_]+$ ]] || die "that doesn't look like a Resend API key (re_...); nothing written" ;;
  api_key|read_key)
    [[ "$v" =~ ^[A-Za-z0-9_-]{32,}$ ]] || die "$name must be at least 32 of [A-Za-z0-9_-]; nothing written"
    other=$DR_CREDS/api_key; [[ "$name" == api_key ]] && other=$DR_CREDS/read_key
    if [[ -s "$other" ]] && cmp -s "$tmp" "$other"; then die "the api_key and the read_key must differ; nothing written"; fi ;;
  origin_pem)
    openssl x509 -noout -in "$tmp" 2>/dev/null || die "origin_pem isn't a PEM certificate; nothing written"
    openssl x509 -noout -checkend 0 -in "$tmp" >/dev/null 2>&1 || die "origin_pem has expired; nothing written"
    host=$(origin_host "$(env_get DURABLE_PUBLIC_ORIGIN "$DR_ENV")") || die "can't read our host from $DR_ENV (DURABLE_PUBLIC_ORIGIN); run install.sh first"
    openssl x509 -noout -checkhost "$host" -in "$tmp" 2>/dev/null | grep -q 'does match' \
      || die "origin_pem doesn't cover $host (Cloudflare: SSL/TLS > Origin Server > Create Certificate, for $host); nothing written"
    if [[ -s "$CADDY_DIR/durable-origin.key" && "$(pub_of_cert "$tmp")" != "$(pub_of_key "$CADDY_DIR/durable-origin.key")" ]]; then
      die "origin_pem doesn't match the installed durable-origin.key: put both (origin_key with --replace first); nothing written"
    fi ;;
  origin_key)
    openssl pkey -noout -in "$tmp" 2>/dev/null || die "origin_key isn't a PEM private key; nothing written"
    if [[ -s "$CADDY_DIR/durable-origin.pem" && "$(pub_of_cert "$CADDY_DIR/durable-origin.pem")" != "$(pub_of_key "$tmp")" ]]; then
      die "origin_key doesn't match the installed durable-origin.pem: replace both, the key first with --replace, then the certificate; nothing written"
    fi ;;
esac

if [[ -e "$target" ]]; then
  if cmp -s "$tmp" "$target"; then say "$name: unchanged"; exit 0; fi
  (( replace == 1 )) || die "$target exists and differs; pass --replace to change it (the old one is kept as $file.prev)"
  cp -p "$target" "$target.prev"
fi
chmod "$mode" "$tmp"
if [[ -n "$group" ]]; then chown "root:$group" "$tmp"; fi
mv -f "$tmp" "$target"
say "$name saved ($(wc -c <"$target" | tr -d ' ') bytes, $mode)"
case "$name" in
  origin_pem|origin_key) say "next: caddy-merge.sh (first time) or systemctl reload caddy" ;;
  *) say "next: systemctl restart $DR_UNIT (or the next deploy) picks it up" ;;
esac
