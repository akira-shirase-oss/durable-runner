#!/usr/bin/env bash
# install.sh — install durable-server from a checkout of this repo on a
# 2 GB Toronto droplet (docs/DEPLOY.md). Run as root. Target: ~15 min
# including CREATE.md's droplet + R2 + Cloudflare prep. Idempotent:
# re-running replaces binaries and units, never the journal or env.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f target/release/durable-server ]] || { echo "build first: cargo build --release" >&2; exit 1; }
# Before anything else, root or not: never install a lab build (S3).
deploy/lab-build-guard.sh target/release/durable-server || exit 1
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

id -u durable >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin durable
install -d -o durable -g durable -m 0750 /var/lib/durable /var/lib/durable/backups /var/lib/durable/journal
install -d -m 0755 /etc/durable /usr/local/share/durable

install -m 0755 target/release/durable-server /usr/local/bin/
install -m 0755 target/release/durable-cli /usr/local/bin/ 2>/dev/null || true
install -m 0755 deploy/durable-run.sh /usr/local/bin/durable-run
[[ -d apps/inspector/build ]] && rm -rf /usr/local/share/durable/inspector \
  && cp -r apps/inspector/build /usr/local/share/durable/inspector || true

if [[ ! -f /etc/durable/durable.env ]]; then
  api=$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
  read_key=$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
  sed -e "s/__API_KEY__/$api/" -e "s/__READ_KEY__/$read_key/" \
    deploy/durable.env.template >/etc/durable/durable.env
  chmod 0600 /etc/durable/durable.env
  umask 077; echo "$api" >/etc/durable/api-key   # for durable-backup
fi
install -m 0644 deploy/durable-server.service /etc/systemd/system/
install -m 0755 deploy/durable-backup.sh /usr/local/bin/durable-backup
install -m 0644 deploy/durable-backup.service deploy/durable-backup.timer /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now durable-server
systemctl enable --now durable-backup.timer

cat <<'EOF'
installed. Next (docs/DEPLOY.md):
  1. /etc/durable/handlers        — one name=url per handler step
  2. deploy/cloudflare-ips.sh     — CF ranges to Caddy, trusted-proxies, ufw
  3. /etc/caddy/origin.{pem,key} + aop-zone-ca.pem (OUR zone AOP CA), Caddyfile
  4. /etc/durable/backup-age-recipient (+ offline private half)
  5. deploy/smoke.sh
EOF
