# Creating what the droplet needs (once, ~20 min)

Everything here is clicked/created before `docs/DEPLOY.md` step 1. Secrets
are typed only on the droplet, into root-only files.

1. **Droplet.** DigitalOcean → Create → Droplet: Ubuntu 24.04, Toronto
   (TOR1), Regular 2 GB / 2 vCPU ($14/mo — the v1 sizing, `perf/LOAD_MATRIX.md`).
   Add a cloud firewall: inbound 22 from your IP only; 443 from anywhere
   (host ufw narrows it to Cloudflare's ranges); nothing else.

2. **DNS + TLS mode.** Cloudflare zone: `durable.404human.dev` → A/AAAA to
   the droplet, proxied (orange). SSL/TLS → **Full (strict)**. SSL/TLS →
   Origin Server → Create Certificate → save `origin.pem`/`origin.key`
   for DEPLOY.md step 4 (15-year, hostnames `durable.404human.dev`).

3. **Authenticated Origin Pulls.** SSL/TLS → Origin Server →
   Authenticated Origin Pulls → **On** for the zone (or hostname). Download
   the origin-pull CA certificate it links
   (`cloudflare-origin-pull-ca.pem`) — Caddy requires it, so a direct hit
   on the droplet fails the TLS handshake even before the range check.

4. **R2 bucket.** R2 → Create bucket `durable-backups`: **enable object
   lock, 7-day retention** (the point: a stolen API key or a fat-finger
   can't delete last week's backups). Create an API token scoped to this
   bucket; keep the access-key-id/secret for rclone on the droplet.

5. **age key** — generated on the droplet in DEPLOY.md step 5; the
   private half goes to an offline store.

6. **Handlers.** Whatever runs the steps — for the reference setup a
   second systemd unit serving one `POST /step` on loopback. Its URL goes
   in `/etc/durable/handlers`.
