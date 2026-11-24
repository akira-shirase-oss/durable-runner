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

3. **Authenticated Origin Pulls, zone-level, with OUR certificate.**
   Cloudflare's global origin-pull certificate is the same for every
   Cloudflare customer: an origin that trusts it lets anyone point their
   own Cloudflare zone at this droplet. Use a zone-level certificate from
   a CA we control instead (on your laptop, not the droplet):

   ```sh
   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
     -subj '/CN=durable origin-pull CA' -keyout aop-ca.key -out aop-zone-ca.pem
   openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
     -subj '/CN=cloudflare origin pull durable.404human.dev' -keyout aop-client.key -out aop-client.csr
   openssl x509 -req -in aop-client.csr -CA aop-zone-ca.pem -CAkey aop-ca.key \
     -CAcreateserial -days 825 -out aop-client.pem
   ```

   Upload `aop-client.pem` + `aop-client.key` to the zone (SSL/TLS →
   Origin Server → Authenticated Origin Pulls → upload a certificate; or
   `POST /zones/{zone_id}/origin_tls_client_auth` with `certificate` and
   `private_key`), then turn zone-level AOP **On** (`PATCH
   /zones/{zone_id}/settings/tls_client_auth` `{"value":"on"}`). The client
   key lives only at Cloudflare; the CA key stays offline (it re-signs the
   client certificate before it expires); `aop-zone-ca.pem` — public —
   goes to the droplet in DEPLOY.md step 2. Caddy requires a client
   certificate it signed, so a direct hit, or another Cloudflare zone,
   fails the TLS handshake.

4. **R2 bucket.** R2 → Create bucket `durable-backups`: **enable object
   lock, 7-day retention** (the point: a stolen API key or a fat-finger
   can't delete last week's backups). Create an API token scoped to this
   bucket; keep the access-key-id/secret for rclone on the droplet.

5. **age key** — generated on the droplet in DEPLOY.md step 5; the
   private half goes to an offline store.

6. **Handlers.** Whatever runs the steps — for the reference setup a
   second systemd unit serving one `POST /step` on loopback. Its URL goes
   in `/etc/durable/handlers`.
