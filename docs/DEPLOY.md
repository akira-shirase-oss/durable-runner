# Deploying the runner to one droplet

Durable Runner on a 2 GB / 2-vCPU DigitalOcean droplet in Toronto, behind
Cloudflare at `https://durable.404human.dev`. No database, no containers:
the journal on the droplet's disk is the only state. **Before you start:**
everything in [deploy/CREATE.md](../deploy/CREATE.md) exists. Secrets are
typed only on the droplet into root-only files.

**Exit gate:** `deploy/smoke.sh` all green, the restore drill done once
(step 8), idle memory recorded. Targets: install ≈ 15 min, upgrade < 2 min
with < 5 s client-visible unavailability, rollback < 1 min.

`$CHECKOUT` is a clone of this repo on the droplet. Build release binaries
elsewhere (a 2 GB host can't hold the Rust build comfortably) and `scp`
`target/release/durable-server` + `durable-cli` + `apps/inspector/build/`
into the checkout before step 1.

## Restart-to-ready (what an upgrade costs clients)

Measured on this Mac, release build, journal of N *queued* runs, spawn →
`/ready` = 200 (`perf/restart_ready.mjs`; v2 record format, D-054):

| queued runs | journal | restart-to-ready |
|---|---|---|
| 100k | 161 MiB | **400 ms** |
| 300k | 467 MiB | **599 ms** |
| 800k | 1.2 GiB | **1,716 ms** |

These are development observations on a fast laptop; the droplet numbers
come from the rehearsal (step 7) and go in this table's droplet column.
While the unit restarts the dial fails; Caddy's `lb_try_duration 15s`
holds each request and retries — a client sees added latency, never an
error, and nothing reaches the old process so **even a submit is safe**.

## 1. Host + package

```sh
ssh root@<droplet-ip>
apt-get update && apt-get -y upgrade
apt-get install -y ufw jq curl ca-certificates age rclone
timedatectl set-timezone UTC
ufw default deny incoming && ufw default allow outgoing
ufw allow from <your-ip> to any port 22 proto tcp && ufw --force enable

cd $CHECKOUT
deploy/install.sh           # user, dirs, keys, units; enables + starts
```

`install.sh` generated `DURABLE_API_KEY`/`DURABLE_READ_KEY` into
`/etc/durable/durable.env` (0600). Write `/etc/durable/handlers` — one
`name=url` per step handler (the `step=…` allowlist) — then
`systemctl restart durable-server`.

## 2. Caddy + Cloudflare

```sh
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' >/etc/apt/sources.list.d/caddy-stable.list
apt-get update && apt-get install -y caddy

umask 077; cat >/etc/caddy/origin.pem        # paste, Ctrl-D
cat >/etc/caddy/origin.key                    # paste, Ctrl-D
# Cloudflare's Authenticated-Origin-Pulls CA (CREATE.md 3):
cat >/etc/caddy/cloudflare-origin-pull-ca.pem # paste, Ctrl-D
chown root:caddy /etc/caddy/origin.* && chmod 0640 /etc/caddy/origin.* /etc/caddy/cloudflare-origin-pull-ca.pem

install -m 0644 $CHECKOUT/deploy/Caddyfile /etc/caddy/Caddyfile
$CHECKOUT/deploy/cloudflare-ips.sh          # CF ranges → Caddy + trusted-proxies + ufw
caddy validate --config /etc/caddy/Caddyfile && systemctl reload caddy
systemctl restart durable-server            # pick up trusted-proxies
```

`cloudflare-ips.sh` writes the CF ranges into both trust boundaries:
Caddy's `trusted_proxies` (so it appends the real client to
`X-Forwarded-For`) and `/etc/durable/trusted-proxies` (so `--trusted-proxy`
lets `CF-Connecting-IP` key the D-052 rate limit by the real client).
**smoke.sh fails if that file is missing** — without it the rate key is
the edge's address. Only Cloudflare can reach 443 at two layers: ufw
ranges *and* Authenticated Origin Pulls' client-cert check.

## 3. Verify it's sealed, from your laptop

```sh
curl -sS --max-time 5 --resolve durable.404human.dev:443:<droplet-ip> \
  https://durable.404human.dev/health \
  && echo "FAIL: droplet answers directly" || echo "ok: only Cloudflare reaches 443"
```

## 4. Inspector

`install.sh` copies `apps/inspector/build/` to
`/usr/local/share/durable/inspector` and the env points `--ui-dir` at it.
Open `https://durable.404human.dev/inspector/`, paste the read key for
listing and the API key for submit/admin — both live in sessionStorage
only, Forget keys clears them.

## 5. Backups: R2 + age + 7-day lock

```sh
age-keygen -o /etc/durable/backup-age-identity 2>/dev/null && chmod 0400 /etc/durable/backup-age-identity
age-keygen -y /etc/durable/backup-age-identity >/etc/durable/backup-age-recipient
cat /etc/durable/backup-age-identity    # copy AGE-SECRET-KEY-… offline, clear screen

rclone config                            # remote 'r2', S3-compat, the bucket-scoped token
systemctl start durable-backup           # first run now
journalctl -u durable-backup -n 10 --no-pager
durable-backup latest                    # name + age in hours
```

Daily at 04:17 UTC the timer calls `POST /admin/backup` (the server's own
atomic copy under `--backup-root`), tars it, encrypts to the recipient,
uploads to `r2:durable-backups/daily/`, prunes local and remote older
than 14 days. The bucket's 7-day object lock means a leaked rclone token
still can't touch last week's backups.

## 6. Smoke

```sh
$CHECKOUT/deploy/smoke.sh     # services, loopback health, CF-only, backups, memory
```

## 7. Upgrade rehearsal (gate; run once, record in install-feedback)

The rehearsal proves the Caddy hold-and-retry behavior end to end. It
needs a journal with queued runs, so run it right after install before
real traffic:

```sh
# On the droplet: queue ~100k runs against a slow handler (or replay a
# scratch journal from perf/restart_ready.mjs), start a client loop that
# records failures and latency, then upgrade:
while true; do
  t0=$(date +%s%N)
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 30 https://durable.404human.dev/ready)
  printf '%s %s %s\n' "$(date +%s)" "$code" "$(( ($(date +%s%N)-t0)/1000000 ))" >>/root/rehearsal.log
  sleep 0.1
done &
LOOP=$!
deploy/upgrade.sh /tmp/durable-server-new      # or: systemctl restart durable-server
kill $LOOP
awk '$2!=200{f++} $3>m{m=$3} END{print "failed:",f+0, "worst-ms:",m}' /root/rehearsal.log
```

Pass = **0 non-200s** and worst added latency under the journal-size row
of the restart table + margin (the dial-refused window + replay).
Re-run per journal size (100k/300k/800k queued runs); record the droplet
numbers in the table above — this is the promised Linux/2-vCPU
measurement.

## 8. Restore drill (gate; ~10 min)

```sh
# Newest backup → scratch dir, decrypted:
rclone copy r2:durable-backups/daily/<newest>.tar.age /tmp/drill
age --decrypt -i /etc/durable/backup-age-identity -o /tmp/drill/backup.tar /tmp/drill/<newest>.tar.age
mkdir /tmp/drill/journal && tar -C /tmp/drill/journal -xf /tmp/drill/backup.tar --strip-components 1

# Second server on a drill port, SAME env (keys) but its own journal:
DURABLE_API_KEY=$KEY DURABLE_READ_KEY=$RKEY \
  /usr/local/bin/durable-server --journal /tmp/drill/journal --listen 127.0.0.1:58399 \
  --handler step=http://127.0.0.1:59010/step &
curl -s http://127.0.0.1:58399/ready
curl -s -H "authorization: Bearer $RKEY" 'http://127.0.0.1:58399/jobs?order=desc' | jq 'length'
# Counts/state match the main server's at backup time → restore proven.
kill %1 && rm -rf /tmp/drill
```

The drill also proves D-054's promise: a journal written by any shipped
version replays under the current binary.

## When something is wrong

| Symptom | Look at |
|---|---|
| `durable-server` won't start | `journalctl -u durable-server -n 100` — a corrupt journal or a second writer names itself |
| `/ready` stuck | replay still running on a big journal (see table); `DURABLE_PROFILE_OPEN=1` logs the phases |
| Cloudflare 52x | `systemctl status caddy`, origin.pem/origin-pull CA, `ufw status` |
| Every visitor shares one rate limit | `/etc/durable/trusted-proxies` missing — rerun `cloudflare-ips.sh` |
| `durable-backup` fails | `journalctl -u durable-backup`: `backups_disabled` → `--backup-root` unset; rclone remote `r2` misnamed |
| Second writer refused | expected — the lock worked; find the stray process |
