#!/usr/bin/env bash
# install.sh — Durable Runner's own pieces on the droplet it shares with Colophon (docs/DEPLOY.md).
#
#   sudo deploy/droplet/install.sh          # from a checkout on the droplet (/opt/durable-build/src)
#
# It makes, if missing: the system user durable; /etc/durable (settings), /etc/durable/credentials
# (0700, root), /var/lib/durable (the journal), /opt/durable/dashboards; the env file from
# durable.env.template; the two ops keys (generated here, never shown); durable-run,
# durable-lab-guard and durable-saas-run in /usr/local/bin; the unit durable-server.service,
# enabled. It starts nothing: direct/deploy.sh installs the first build and starts it, once the
# Neon URL and the Resend key are in place (put-secret.sh).
#
# Coexisting with Colophon: it never touches ufw, sshd, redis, Caddy (caddy-merge.sh is the one
# Caddy step) or any file outside the paths above; an existing unit file or env file that isn't
# ours stops it. Before anything, it checks loopback 58300 (the public listener) and 58301 (the
# ops listener) are free or held by durable-server itself, and stops otherwise, naming what holds
# them. A second run changes nothing and calls nothing that changes anything.
#
# Tests: ROOT=<scratch dir> runs it against a copy of the filesystem (test-droplet-install.sh).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=deploy/droplet/lib.sh
. "$here/lib.sh"
[[ $EUID -eq 0 || -n "$ROOT" ]] || die "run as root"
if [[ -z "$ROOT" ]]; then
  [[ -d /run/systemd/system ]] || die "no systemd here"
fi

printf '== Durable Runner on a shared droplet: install\n'

# 1. The ports, before anything is written.
port_check "$DR_PUBLIC_PORT" "the public listener"
port_check "$DR_OPS_PORT" "the ops listener (--ops-listen)"
say "ports $DR_PUBLIC_PORT and $DR_OPS_PORT: free or durable-server's own"

# 2. Ours, or refused: files of these names that this kit didn't write.
for f in "$DR_UNIT_FILE" "$DR_ENV"; do
  if [[ -e "$f" && "$(head -n 1 "$f")" != "$DR_MARK" ]]; then
    die "$f exists and isn't this kit's (its first line isn't '$DR_MARK'): a self-hosted install, or another product? Nothing was changed"
  fi
done

# 3. The user, and the directories.
if id -u "$DR_USER" >/dev/null 2>&1; then
  say "user $DR_USER: present"
else
  useradd --system --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin "$DR_USER"
  say "user $DR_USER: made"
fi
ensure_dir "$DR_ETC" 0755
ensure_dir "$DR_CREDS" 0700
ensure_dir "$DR_STATE" 0750 "$DR_USER:$DR_USER"
ensure_dir "$DR_STATE/journal" 0750 "$DR_USER:$DR_USER"
ensure_dir "$DR_STATE/backups" 0750 "$DR_USER:$DR_USER"
ensure_dir "$DR_OPT" 0755
ensure_dir "$DR_DASHES" 0755
ensure_dir "$DR_BUILD" 0755

# 4. Settings: written once, never overwritten (edit it; this run keeps your changes).
if [[ -f "$DR_ENV" ]]; then
  say "env file: kept $DR_ENV"
else
  origin=${DURABLE_PUBLIC_ORIGIN:-https://$DR_SITE_HOST_SHIPPED}
  host=$(origin_host "$origin") || die "DURABLE_PUBLIC_ORIGIN must be https://<host>"
  tmp=$(mktemp "$DR_ETC/.dr-env.XXXXXX")
  sed "s#^DURABLE_PUBLIC_ORIGIN=.*#DURABLE_PUBLIC_ORIGIN=https://$host#" "$here/durable.env.template" >"$tmp"
  chmod 0644 "$tmp" && mv -f "$tmp" "$DR_ENV"
  say "env file: $DR_ENV (public origin https://$host)"
fi
origin=$(env_get DURABLE_PUBLIC_ORIGIN "$DR_ENV")
host=$(origin_host "$origin") || die "fix DURABLE_PUBLIC_ORIGIN in $DR_ENV"
labels=$(tr -cd . <<<"$host" | wc -c | tr -d ' ')
if is_uint "$labels" && (( labels > 2 )); then
  say "warning: $host is more than one level under its zone: Cloudflare's free certificate covers the apex and *.zone only"
fi
for f in handlers deny-cidrs; do
  if [[ ! -e "$DR_ETC/$f" ]]; then
    case $f in
      handlers) printf '# name=url per line: handlers named on the command line (workspace 0). Tenants register theirs through the API.\n' >"$DR_ETC/$f" ;;
      deny-cidrs) printf '# One CIDR or address per line that a handler call must never dial and that is not on an interface: the droplet'"'"'s reserved IP, if it has one.\n' >"$DR_ETC/$f" ;;
    esac
    chmod 0644 "$DR_ETC/$f"
    say "made $DR_ETC/$f (comments only)"
  fi
done
if [[ -s "$DR_ETC/trusted-proxies" ]]; then
  say "warning: $DR_ETC/trusted-proxies is not empty: on this host the server trusts the loopback hop alone (Caddy); remove it"
fi

# 5. The ops keys: generated here, root-only, never printed.
newkey() { head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n'; }
for k in api_key read_key; do
  if [[ -s "$DR_CREDS/$k" ]]; then
    say "credential $k: kept"
  else
    (umask 077; newkey >"$DR_CREDS/$k.tmp" && chmod 0400 "$DR_CREDS/$k.tmp" && mv -f "$DR_CREDS/$k.tmp" "$DR_CREDS/$k")
    say "credential $k: generated (root only)"
  fi
done
missing=()
for k in database_url resend_api_key; do [[ -s "$DR_CREDS/$k" ]] || missing+=("$k"); done

# 6. The start-up scripts, beside one another in /usr/local/bin as the self-hosted install puts
# them: the unit starts durable-saas-run, which execs durable-run, which runs durable-lab-guard on
# the binary before every exec and refuses to start without it. Each deploy refreshes them from
# the commit it installs (direct/droplet-deploy.sh). The binaries come with the first deploy.
ensure_dir "$DR_BIN" 0755
if install_scripts "$here/.."; then say "start-up scripts: installed in $DR_BIN"; else say "start-up scripts: current"; fi

# 7. The unit: replaced only when this kit's copy differs, then one daemon-reload.
if put_file "$here/durable-server.service" "$DR_UNIT_FILE" 0644; then
  systemctl daemon-reload
  say "unit: $DR_UNIT_FILE installed (daemon-reload)"
else
  say "unit: current"
fi
if systemctl is-enabled --quiet "$DR_UNIT" 2>/dev/null; then
  say "unit: enabled"
else
  systemctl enable "$DR_UNIT" >/dev/null 2>&1 || die "systemctl enable $DR_UNIT failed"
  say "unit: enabled now (starts on boot; the first deploy starts it)"
fi

printf '\n== install: done. Untouched: ufw, sshd, redis, Caddy, Colophon.\n'
if ((${#missing[@]})); then
  printf '   still needed before the first deploy (docs/DEPLOY.md):\n'
  for k in "${missing[@]}"; do printf '     put-secret.sh %s   <its file on stdin>\n' "$k"; done
fi
[[ -s "$CADDY_DIR/durable-origin.pem" ]] || printf '     put-secret.sh origin_pem / origin_key, then caddy-merge.sh\n'
