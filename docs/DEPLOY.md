# Deploying Durable Runner

Two layouts, two kits:

- **The hosted service** at `https://durablerunnerapp.404human.dev` runs on a
  DigitalOcean droplet it **shares with Colophon**. Accounts live in Neon and
  mail goes through Resend. The kit is [`deploy/droplet/`](../deploy/droplet/),
  and the next part of this guide covers it.
- **Self-hosting on your own droplet**: one product per host, the journal as the
  only state, no database. The kit is `deploy/install.sh`, `upgrade.sh`,
  `rollback.sh`, `smoke.sh` and `deploy/Caddyfile`. See
  [Self-hosting on your own droplet](#self-hosting-on-your-own-droplet) below.

The two share the server's own safety scripts: `deploy/upgrade.sh` (a staged
root-owned copy, the lab guard, the D-056 format guard on a quiet journal,
`restore_prev`), `deploy/rollback.sh`, `deploy/lab-build-guard.sh` (installed
as `durable-lab-guard`, run by `durable-run` before every start) and
`deploy/format-guard.sh`.

## The hosted service on the shared droplet

### What lives where

Colophon got to the droplet first, so every path below is either Durable
Runner's own or shared with Colophon on purpose. Nothing else on the host is
touched. Durable Runner's scripts never change ufw, sshd, redis, Colophon's
files, or the global options in the root Caddyfile.

| What | Where | Owner |
|---|---|---|
| Caddy root config (global options, Colophon's site) | `/etc/caddy/Caddyfile` | Colophon's installer; we append one `import /etc/caddy/sites/*.caddy` line |
| Our Caddy site (site block only) | `/etc/caddy/sites/durable-runner.caddy` | `caddy-merge.sh`, rendered from `deploy/droplet/durable-runner.caddy` |
| Cloudflare ranges, origin-pull trust | `/etc/caddy/cloudflare-trusted.caddy`, `/etc/caddy/origin-pull-trust.caddy` | Colophon (`cloudflare-ips.sh`, `origin-pull-ca.sh`); we only read them |
| Our origin certificate | `/etc/caddy/durable-origin.pem` + `.key` (root:caddy 0440) | `put-secret.sh origin_pem / origin_key` |
| Settings (no secrets) | `/etc/durable/durable.env` | `install.sh`, once; you edit it after that |
| Secrets | `/etc/durable/credentials/*` (0400 root, dir 0700) | `install.sh` (ops keys), `put-secret.sh` (the rest) |
| Journal, backups | `/var/lib/durable/{journal,backups}` (user `durable`) | the server |
| Server, CLI, start-up scripts | `/usr/local/bin/durable-server` (+ `.prev`), `durable-cli`, `durable-run`, `durable-lab-guard`, `durable-saas-run` | `deploy/upgrade.sh` (via the direct deploy), `install.sh` |
| Dashboard | `/opt/durable/dashboards/<release>`, served through the `/opt/durable/dashboard` link | `direct/droplet-deploy.sh` (current and previous kept) |
| Build checkout and cache | `/opt/durable-build/{src,target}` | `direct/droplet-build.sh` |
| Unit | `durable-server.service` | `install.sh`, refreshed by each deploy |
| Loopback ports | 58300 public listener (Caddy proxies here), 58301 ops listener | the server |

Colophon uses 58000 and its own `/etc/colophon`, `/opt/colophon*`, unit and
Redis. `install.sh` and every deploy check that 58300 and 58301 are free, or
held by durable-server's own MainPID. Anything else holding them stops the
script, which names the pid and leaves that process running.

### Where the secrets live

Every secret is a root-only file. systemd hands each one to the service with
`LoadCredential=`, and `durable-saas-run` exports it under the name the server
reads. None of them is ever in argv, `durable.env`, the unit file, a log, or
shell history.

| Credential file | Becomes | Made by |
|---|---|---|
| `api_key` | `DURABLE_API_KEY` (the ops write key) | `install.sh` generates it on the droplet |
| `read_key` | `DURABLE_READ_KEY` | `install.sh` generates it on the droplet |
| `database_url` | `DATABASE_URL`: Neon, `durable_app`, the direct host, `sslmode=verify-full` | `neon-setup.sh` on your machine, then `put-secret.sh database_url` |
| `resend_api_key` | `RESEND_API_KEY` (sending as `DURABLE_MAIL_FROM`) | the maintainer's Resend key, then `put-secret.sh resend_api_key` |

`put-secret.sh` reads the value from stdin and validates it. A pooled Neon
host, anything other than `verify-full`, `channel_binding`, or a non-`re_` key
is refused with nothing written. A different value needs `--replace`, which
keeps the old one as `.prev`. The same value again is a no-op. Colophon's
`/root/put-secret` writes Colophon's files; this is Durable Runner's
equivalent. The wrapper refuses to start the hosted service without the
account store or the mail key (`DURABLE_REQUIRE_ACCOUNTS=1`,
`DURABLE_REQUIRE_MAIL=1`). It also refuses `DURABLE_MAILER=log`, which would
print sign-in links to the journal, and an in-memory account store.

### First time, in order

`$H` below is the droplet's SSH alias (`DURABLE_DEPLOY_HOST`). The commands
run from a checkout of this repository on the maintainer's machine unless
they say "on the droplet".

**0. Check the host is as this kit expects** (read-only):

```sh
ssh $H 'head -n 30 /etc/caddy/Caddyfile; ls -la /etc/caddy /etc/caddy/sites 2>&1; cat /etc/caddy/origin-pull-trust.caddy'
ssh $H 'ss -Hltnp "sport = :58300 or sport = :58301"; id durable 2>&1; ls /etc/durable /etc/systemd/system/durable-server.service 2>&1'
ssh $H 'ls /root/.cargo/bin/rustup; free -m; df -h /; swapon --show; systemctl is-active caddy colophon'
```

The root must import `/etc/caddy/cloudflare-trusted.caddy` and set
`client_ip_headers Cf-Connecting-Ip` in its `servers` block. Both ports must be
empty, and there must be no `/etc/durable` and no `durable-server.service`.
`caddy-merge.sh` and `install.sh` check these themselves and refuse otherwise.

**1. Cloudflare (the maintainer, in the dashboard).** The zone is `404human.dev`.

- DNS: add an **A** record `durablerunnerapp` pointing at the same droplet
  address `colophonapp` uses, **proxied** (orange cloud). Add an **AAAA**
  record too if `colophonapp` has one. The name is one level under the zone,
  so the free certificate covers it.
- SSL/TLS → Origin Server → **Create Certificate**: hostname
  `durablerunnerapp.404human.dev` only, 15 years, PEM. Save the certificate
  and the private key as two files on your machine, for example
  `~/durable-origin.pem` and `~/durable-origin.key`. Never paste them in chat.
- Leave SSL/TLS mode at **Full (strict)** and **Authenticated Origin Pulls**
  as they are. Both are zone-level and already on for Colophon. It is the same
  zone, so Cloudflare presents the same client certificate to our host, and
  Caddy checks it against Colophon's `origin-pull-trust.caddy`.

**2. Neon (the maintainer's machine).** Save the Neon project's owner
connection string from the dashboard into a file that only you can read. Then:

```sh
deploy/droplet/neon-setup.sh --owner-url-file ~/neon-owner-url --out ~/durable-neon
```

The script creates the `durable_runner` database and the `durable_app` role,
with a plaintext password over `verify-full`, because Neon has no superuser.
It sets `createrole_self_grant = 'set, inherit'` first. It gives the role
CONNECT and TEMPORARY on the database, with PUBLIC kept out, plus USAGE and
CREATE on `public`, and creates `pgcrypto` as the owner, which is what the
server's startup migrations need. It then logs in as `durable_app` to check
all of that. The password and the server's URL are written to
`~/durable-neon/{app_password,database_url}` (0400). Nothing secret is
printed or passed in argv. Re-running is a no-op; `--rotate` sets a new
password. It needs psql 16 or newer (`brew install libpq`). Then sweep the
migrations once against a scratch Neon database as the owner before the first
deploy, to catch any statement Neon refuses before deploy day.

**3. The checkout and the installer (on the droplet, as root):**

```sh
ssh $H 'git clone -q --filter=blob:none https://github.com/akira-shirase-oss/durable-runner.git /opt/durable-build/src'
ssh $H /opt/durable-build/src/deploy/droplet/install.sh
```

The installer makes the `durable` user, the directories, `durable.env`, the
two ops keys, `durable-run` + `durable-lab-guard` + `durable-saas-run` in
`/usr/local/bin`, and the unit (enabled, not started). Run it twice: the
second run reports everything as kept or current. The public origin defaults
to `https://durablerunnerapp.404human.dev`. For another host, put
`DURABLE_PUBLIC_ORIGIN=https://<host>` in front of the first run.

**4. The secrets (from your machine; stdin only):**

```sh
P=/opt/durable-build/src/deploy/droplet/put-secret.sh
ssh $H "$P database_url"   <~/durable-neon/database_url
ssh $H "$P resend_api_key" <~/resend-api-key
ssh $H "$P origin_key"     <~/durable-origin.key
ssh $H "$P origin_pem"     <~/durable-origin.pem
```

The certificate is checked: it must cover the host in `DURABLE_PUBLIC_ORIGIN`,
must not be expired, and must match the key.

**5. Caddy (on the droplet):**

```sh
ssh $H /opt/durable-build/src/deploy/droplet/caddy-merge.sh
```

This installs our site file and appends the one import line to the root (only
if it isn't there). It keeps a backup under `/var/backups/durable-runner/caddy/`,
runs `caddy validate`, and then `systemctl reload caddy`. If either is refused,
the files are put back as they were. Until the first deploy, Cloudflare
answers 502 for our host, because nothing listens on 58300 yet.

**6. The first deploy (from your machine):** the routine command below. The
first build on the droplet is cold: it installs Rust from
`rust-toolchain.toml` and compiles every dependency, so allow up to 30
minutes. Later builds are incremental.

**7. Check everything (on the droplet):**

```sh
ssh $H /opt/durable-build/src/deploy/droplet/smoke.sh
```

The full smoke checks the release, the hosted settings, Caddy's merge, the
origin certificate, Authenticated Origin Pulls, and the public URL through
Cloudflare. It FAILS while origin pulls still trust Cloudflare's shared
certificate; that trust is set by Colophon's `origin-pull-ca.sh` for the whole
zone, and should end as our own zone certificate only, never the shared one.

**8. Optional hardening.** If the droplet has a reserved IP, add it to
`/etc/durable/deny-cidrs`; addresses on the droplet's interfaces are refused
already. The egress firewall in the self-host section below is keyed to the
`durable` user, so it doesn't affect Colophon.

### The routine deploy (under 10 minutes)

```sh
DURABLE_DEPLOY_HOST=$H DURABLE_PUBLIC_URL=https://durablerunnerapp.404human.dev \
  deploy/droplet/direct/deploy.sh origin/main
```

`<rev>` must be on `origin/main` and must contain this kit. `deploy.sh`:

1. Uploads `droplet-build.sh` and `droplet-deploy.sh` **from that commit**,
   not from your working tree.
2. Builds `durable-server` and `durable-cli` on the droplet at that commit,
   in `/opt/durable-build`. The build is incremental, runs at nice 15 with
   idle I/O and 2 jobs, and is detached through `systemd-run`, so a dropped
   SSH can't stop it. It waits for any other cargo on the host (Colophon's
   deploy builds there too) and records the binaries' sha256.
3. Builds the dashboard here at the same commit (`git archive`, `npm ci`,
   `npm run build` in `apps/inspector`) and uploads it.
4. Runs `droplet-deploy.sh`:
   - copies out and verifies the recorded binaries and refuses a lab build;
   - checks the credentials and the ports before anything stops;
   - unpacks the dashboard into its own release directory and switches the
     link;
   - refreshes the start-up scripts and the unit from the commit;
   - installs the binary through `deploy/upgrade.sh`. If the start or
     `/ready` fails, `upgrade.sh`'s `restore_prev` puts the previous binary
     back. If the new binary starts but fails its smoke, `deploy/rollback.sh`
     runs. On any failure the previous dashboard comes back too.
   - keeps the current and previous binary and dashboard, and wipes the build
     cache past 8 GB.
5. Checks `/health`, `/ready` and the dashboard through the public URL, and
   prints the elapsed time.

Logs are on the droplet in `/root/durable-deploy/{build,deploy}.out`, plus
`/opt/durable-build/cargo.log` and `journalctl -u durable-server`.
`ssh $H /root/durable-deploy/droplet-deploy.sh --status` prints what is live.

### Rollback

```sh
DURABLE_DEPLOY_HOST=$H DURABLE_PUBLIC_URL=https://durablerunnerapp.404human.dev \
  deploy/droplet/direct/deploy.sh --rollback
```

This runs `deploy/rollback.sh` on the droplet. It refuses a lab `.prev`, stops
the unit, refuses a `.prev` that can't read the journal (D-056), restores it,
then waits for `/ready` and smokes it. It also switches the dashboard back to
the previous one, then checks the public URL. A rollback swaps binaries only;
the journal and Neon are untouched.

### Settings to flip later

All of these live in `/etc/durable/durable.env`. Restart after a change
(`systemctl restart durable-server`), or let the next deploy pick it up.

- `DURABLE_OPS_LISTEN=127.0.0.1:58301`: set this once the server has
  `--ops-listen` (S3 ruling 2'). clap refuses unknown flags, so the wrapper
  passes it only when this line is set and the binary's `--help` lists it. If
  the setting is on but the binary lacks the flag, the wrapper logs a warning
  instead. The ops keys are then answered there only, through
  `ssh -L 58301:127.0.0.1:58301 $H`. `--public-origin` follows the same rule:
  it is passed only to a server that has it (the S3 branch has it, main at
  8b46ec3 doesn't).
- `DURABLE_DASHBOARD_PATH=/app/`: set this once the server mounts the
  dashboard at `/app` (with `/inspector` answering 308). Then rerun
  `caddy-merge.sh` (for the root redirect) and pass
  `DURABLE_DASHBOARD_PATH=/app/` to `deploy.sh` (for its public check).
- `DURABLE_INVITE_ONLY`: off by default; the line stays commented. The server
  only checks whether the variable exists, so `0` would mean on. The wrapper
  reads `0`/`false`/`off`/`no`/empty as off and drops the variable, and reads
  `1`/`true`/`on`/`yes` as on.

### The Colophon side

Colophon's `install.sh` writes `/etc/caddy/Caddyfile` whole, so re-running it
drops our import line. The validate and reload that follow then take our site
offline. `smoke.sh` says so, and rerunning `caddy-merge.sh` puts the line
back. The lasting fix is in Colophon: its Caddyfile template ends with
`import /etc/caddy/sites/*.caddy`. A root with that line and no `sites/`
directory, or an empty one, still validates; `test-droplet-caddy.sh` checks
both.

### What not to run on the shared host

- `deploy/cloudflare-ips.sh`: it rewrites ufw's 443 rules and
  `cloudflare-trusted.caddy`, which are Colophon's. It would also put
  Cloudflare's ranges in `trusted-proxies`, but here the server trusts the
  loopback hop alone.
- `deploy/install.sh`, `deploy/Caddyfile` and `deploy/smoke.sh` by themselves:
  that is the self-host layout. On this host, `deploy/smoke.sh` hands over to
  `deploy/droplet/smoke.sh`, because `durable.env` carries the kit's marker.
- `deploy/upgrade.sh` or `rollback.sh` with a binary built anywhere else. The
  direct deploy runs them from `/opt/durable-build/src` at the deployed commit.

### Not yet on the shared host

Journal backups to R2 are not wired up here. `durable-backup` assumes the
self-host paths and the ops key on the public listener, so they wait for
`--ops-listen`. Until then the journal lives only on the droplet's disk, plus
DigitalOcean's droplet backups if those are on.

### The kit's tests

Each test runs in seconds, on a Mac or Linux, and touches nothing outside its
scratch directory or container.

| Test | Proves |
|---|---|
| `deploy/droplet/test-droplet-install.sh` | `install.sh` + `caddy-merge.sh` against a seeded Colophon host, twice: the second run changes nothing; Colophon's files are byte-identical; there is one import line; a refused validate or reload restores; a foreign unit or a foreign listener on 58300/58301 stops the install before it writes anything. Known-bad proofs for the port check and the restore. |
| `deploy/droplet/test-droplet-caddy.sh` | the site file's rules (each one proven red on a mutant), then the merged root run in the pinned Caddy image: a real validate, SNI per host, Colophon's site still served, AOP required, the client address from a trusted vs an untrusted peer, and an empty `sites/` that still validates. Needs Docker. |
| `deploy/droplet/test-droplet-secrets.sh` | `durable-saas-run` through the real `durable-run` and its lab guard: credentials to env and never to argv, the hosted flags, every refusal. Plus `put-secret.sh`'s checks. |
| `deploy/droplet/test-neon-setup.sh` | `neon-setup.sh` against a recording psql: the direct host over verify-full, no password in argv or output, the Neon-safe SQL, idempotency, `--rotate`. Real Neon is proven only by the first run. |
| `deploy/droplet/test-droplet-deploy.sh` | `droplet-deploy.sh` as root in a throwaway Ubuntu 24.04 container, through the real `upgrade.sh`, `rollback.sh`, format guard, lab guard and smoke: first-install fail-closed, upgrade, `--rollback`, `restore_prev` after a crash, `rollback.sh` after a failed smoke, lab and stale-build refusals. Needs Docker. |

### When something is wrong

| Symptom | Look at |
|---|---|
| `install.sh`: port 58300/58301 held | the pid and command it names; never kill it blindly, it may be another product's |
| `caddy-merge.sh` refused | its message. A root without Cloudflare's trust, two import lines, our host already in the root, or a missing origin certificate change nothing. A failed validate or reload restores both files. |
| Cloudflare 52x for our host only | `ls /etc/caddy/sites` and `grep -c 'sites/\*.caddy' /etc/caddy/Caddyfile` (did Colophon's installer rewrite the root?), `systemctl status caddy`, `durable-origin.pem` covers the host |
| `durable-server` won't start | `journalctl -u durable-server -n 50`. `durable-saas-run: refusing to start` names the setting or credential. `Failed to set up credentials` means a file in `/etc/durable/credentials` is missing. |
| smoke: no "account store connected" | Neon unreachable or the TLS check failed: `journalctl -u durable-server | grep -i account` |
| a deploy failed | `/root/durable-deploy/deploy.out`. It rolled back if it had switched; `droplet-deploy.sh --status` |
| the build is slow or killed | `/opt/durable-build/cargo.log`, `free -m`. Two Rust builds don't fit in 2 GB, so it waits for Colophon's. |

## Self-hosting on your own droplet

One product on its own 2 GB / 2-vCPU droplet behind Cloudflare at
`https://<your-host>`. No database and no containers: the journal on the
droplet's disk is the only state. This is the layout the self-hosting docs
describe. It is **not** the hosted service's (that one is above).
**Before you start:** everything in [deploy/CREATE.md](../deploy/CREATE.md)
exists. Secrets are typed only on the droplet, into root-only files.

**Exit gate:** `deploy/smoke.sh` all green, the restore drill done once
(step 8), idle memory recorded. Targets: install ≈ 15 min, upgrade < 2 min
with < 5 s client-visible unavailability, rollback < 1 min.

`$CHECKOUT` is a clone of this repo on the droplet. Build the release binaries
elsewhere (a 2 GB host can't hold the Rust build comfortably while serving),
then `scp` `target/release/durable-server`, `durable-cli` and
`apps/inspector/build/` into the checkout before step 1.

### Restart-to-ready (what an upgrade costs clients)

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

### Capacity planning (measured, not assumed)

A submit is acknowledged after the journal commit; the run then executes
at machine speed. Accepted-but-unfinished runs are bounded at 100,000
live — when the offered rate outruns the completion rate long enough for
the backlog to reach that bound, admission refuses `503
{"code":"at_capacity"}` rather than queueing silently (retrying after
drain is safe; the submit's idempotency key dedupes). Plan the offered
rate from the *completion* ceiling, not the accept ceiling.

Measured ceilings (development observations, per-record fsync via group
commit):

| machine | result |
|---|---|
| 12-core Apple M2 Pro (this Mac) | **100 submits/s sustained** — 1,000,000/1,000,000 accepted and done, p99 41 ms (`cdbdfb8`) |
| 4-core `ubuntu-24.04` GitHub runner | **~85 completions/s** — a 100/s offer accepted 835,397 then refused 161,535 honestly at the live bound; p99 hit the 10 s reply deadline (`c94edfd`, run 44440111562, kept in `perf/results/`) |

The droplet's own ceiling is **measured at deploy, not assumed** — a
2-vCPU host lands below the laptop. After install, run `deploy/smoke.sh`,
then a short sustained probe. The harness needs `nodejs` on the droplet
(`apt-get install -y nodejs`); it spawns its own server + handler on the
ports given, against a scratch journal — it does not touch the running
unit's state, though it does share the machine, so run it before real
traffic:

```sh
cd $CHECKOUT   # the checkout's target/release/durable-server is what gets measured
node perf/load.mjs sustained --rate=50 --total=30000 --port=58399 --hport=58398
# watch done-rate + p99 in the result; repeat a rung above/below to bracket the ceiling
```

Record the droplet's sustained completions/s here before pointing real
traffic at it; if the probe's refused share is nonzero, the offered rate
is above the machine, not above the product's bounds. Ceilings move with
disk latency (fsync) more than with cores.

### 1. Host + package

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

`install.sh` (and `upgrade.sh`) first refuse a lab build — a
`cargo build --features lab` binary, which `examples/pilot/run-workspaces.sh`
leaves in the same `target/release` — so rebuild with a plain
`cargo build --release` if it says so (D-062). `install.sh` also puts the
guard beside `durable-run` (`/usr/local/bin/durable-lab-guard`), which
runs it before every start; a droplet installed before D-062 gets it by
re-running `install.sh` (idempotent).

`install.sh` generated `DURABLE_API_KEY`/`DURABLE_READ_KEY` into
`/etc/durable/durable.env` (0600). Write `/etc/durable/handlers` — one
`name=url` per step handler (the `step=…` allowlist) — then
`systemctl restart durable-server`.

### 2. Caddy + Cloudflare

```sh
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' >/etc/apt/sources.list.d/caddy-stable.list
apt-get update && apt-get install -y caddy

umask 077; cat >/etc/caddy/origin.pem        # paste, Ctrl-D
cat >/etc/caddy/origin.key                    # paste, Ctrl-D
# OUR zone-level Authenticated-Origin-Pulls CA certificate (CREATE.md 3) —
# public, but only its holder's client certificate gets in:
cat >/etc/caddy/aop-zone-ca.pem # paste, Ctrl-D
chown root:caddy /etc/caddy/origin.* /etc/caddy/aop-zone-ca.pem
chmod 0640 /etc/caddy/origin.* /etc/caddy/aop-zone-ca.pem

install -m 0644 $CHECKOUT/deploy/Caddyfile /etc/caddy/Caddyfile
$CHECKOUT/deploy/cloudflare-ips.sh          # CF ranges → Caddy + trusted-proxies + ufw
caddy validate --config /etc/caddy/Caddyfile && systemctl reload caddy
systemctl restart durable-server            # pick up trusted-proxies
```

`cloudflare-ips.sh` writes the CF ranges into Caddy's `trusted_proxies`
and `/etc/durable/trusted-proxies`. Caddy is the one that judges them:
from a Cloudflare peer it takes the client from `CF-Connecting-IP`
(`client_ip_headers`), from anyone else the TCP peer, and it **overwrites**
`CF-Connecting-IP` with that address before proxying (`header_up
CF-Connecting-IP {client_ip}`). durable-server only ever sees Caddy over
loopback, so `durable-run` trusts that hop (`--trusted-proxy 127.0.0.1/32
--trusted-proxy ::1/128`, added for a loopback `DURABLE_LISTEN`) and keys
the D-052 rate limit by the header Caddy wrote (D-061). Without the hop
every visitor shares 127.0.0.1's bucket; without the overwrite a visitor
could pick its own. **smoke.sh fails if the CF ranges file is missing, if
the installed Caddyfile does not overwrite the header, or if the running
server does not trust the hop.** Only Cloudflare can reach 443 at two
layers: ufw ranges *and* Authenticated Origin Pulls' client-cert check.

#### Handler egress: deny-cidrs and the firewall

The SSRF guard already refuses the local blocks and every address on the
droplet's interfaces (D-061). List what reaches the droplet *without*
being on an interface — the reserved IP, if one is assigned — one per
line in `/etc/durable/deny-cidrs` (`ADDR[/PREFIX]`, `#` comments), then
`systemctl restart durable-server`:

```sh
echo '<reserved-ip>/32   # reserved IP' >>/etc/durable/deny-cidrs
```

Recommended as the second layer: an egress firewall for the server's
user, so even a guard bug cannot reach the host itself or the private
network. With nftables (`durable` is the unit's user):

```sh
nft add table inet durable_egress
nft add chain inet durable_egress out '{ type filter hook output priority 0; policy accept; }'
# replies on connections Caddy opened to the server, and on the server's own calls
nft add rule inet durable_egress out meta skuid durable ct state established,related accept
# the local DNS stub (systemd-resolved): the one local destination the server needs
nft add rule inet durable_egress out meta skuid durable ip daddr 127.0.0.53 meta l4proto '{ tcp, udp }' th dport 53 accept
# never this host itself: every local address, the public ones included
nft add rule inet durable_egress out meta skuid durable fib daddr type local drop
# never the metadata service or the private ranges (the VPC)
nft add rule inet durable_egress out meta skuid durable ip daddr '{ 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 }' drop
nft add rule inet durable_egress out meta skuid durable ip6 daddr '{ fc00::/7, fe80::/10 }' drop
# handler calls on 80/443; Neon on 5432 when DATABASE_URL is set
nft add rule inet durable_egress out meta skuid durable tcp dport '{ 80, 443, 5432 }' accept
nft add rule inet durable_egress out meta skuid durable drop
```

(`fib daddr type local` matches every address of this host, the public
ones included. Order matters: the drops come before the port accept, or
port 80 would reach the metadata service. A handler on another port needs
its own accept, and a `--handler` flag on this host needs
`--allow-local-handlers` *and* its own rule. That flag opens the
operator's own `--handler` flags (workspace 0) only: a handler registered
through the API — any tenant, and workspace 0's store rows too — is always
checked against the full SSRF policy (local blocks, this host's addresses,
`/etc/durable/deny-cidrs`), whatever the flag says. A same-host handler is
therefore a `--handler` line in `/etc/durable/handlers`, never an API
registration. The backup unit uploads to
R2 over 443. This is deploy guidance, not applied by `install.sh`, and it
was not exercised on a droplet when written: after applying it, check
`nft list ruleset`, a handler call, and that
`curl --max-time 5 http://<droplet-public-ip>:22` run as `durable` fails.)

### 3. Verify it's sealed, from your laptop

```sh
curl -sS --max-time 5 --resolve <your-host>:443:<droplet-ip> \
  https://<your-host>/health \
  && echo "FAIL: droplet answers directly" || echo "ok: only Cloudflare reaches 443"
```

### 4. Inspector

`install.sh` copies `apps/inspector/build/` to
`/usr/local/share/durable/inspector` and the env points `--ui-dir` at it.
Open `https://<your-host>/inspector/`, paste the read key for
listing and the API key for submit/admin — both live in sessionStorage
only, Forget keys clears them.

### 5. Backups: R2 + age + 7-day lock

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

### 6. Smoke

```sh
$CHECKOUT/deploy/smoke.sh     # services, loopback health, CF-only, backups, memory
```

### 7. Upgrade rehearsal (gate; run once, record in install-feedback)

The rehearsal proves the Caddy hold-and-retry behavior end to end. It
needs a journal with queued runs, so run it right after install before
real traffic:

```sh
# On the droplet: queue ~100k runs against a slow handler (or replay a
# scratch journal from perf/restart_ready.mjs), start a client loop that
# records failures and latency, then upgrade:
while true; do
  t0=$(date +%s%N)
  code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 30 https://<your-host>/ready)
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

### 8. Restore drill (gate; ~10 min)

```sh
# Newest backup → scratch dir, decrypted:
rclone copy r2:durable-backups/daily/<newest>.tar.age /tmp/drill
age --decrypt -i /etc/durable/backup-age-identity -o /tmp/drill/backup.tar /tmp/drill/<newest>.tar.age
mkdir /tmp/drill/journal && tar -C /tmp/drill/journal -xf /tmp/drill/backup.tar --strip-components 1

# Second server on a drill port, SAME env (keys) but its own journal:
DURABLE_API_KEY=$KEY DURABLE_READ_KEY=$RKEY \
  /usr/local/bin/durable-server --journal /tmp/drill/journal --listen 127.0.0.1:58399 \
  --handler step=http://127.0.0.1:59010/step \
  --allow-local-handlers &
curl -s http://127.0.0.1:58399/ready
curl -s -H "authorization: Bearer $RKEY" 'http://127.0.0.1:58399/jobs?order=desc' | jq 'length'
# Counts/state match the main server's at backup time → restore proven.
kill %1 && rm -rf /tmp/drill
```

The drill also proves D-054's promise: a journal written by any shipped
version replays under the current binary.

### When something is wrong

| Symptom | Look at |
|---|---|
| `durable-server` won't start | `journalctl -u durable-server -n 100` — a corrupt journal or a second writer names itself |
| `/ready` stuck | replay still running on a big journal (see table); `DURABLE_PROFILE_OPEN=1` logs the phases |
| Cloudflare 52x | `systemctl status caddy`, origin.pem, `aop-zone-ca.pem` matches the client certificate uploaded to the zone (and zone-level AOP is on), `ufw status` |
| Every visitor shares one rate limit | the server does not trust the loopback hop (an old `durable-run`, or a non-loopback `DURABLE_LISTEN`), or Caddy does not overwrite `CF-Connecting-IP` — reinstall `deploy/durable-run.sh` and `deploy/Caddyfile`; `smoke.sh` names which |
| `durable-backup` fails | `journalctl -u durable-backup`: `backups_disabled` → `--backup-root` unset; rclone remote `r2` misnamed |
| Second writer refused | expected — the lock worked; find the stray process |
| `format-guard: REFUSED` | the binary reads an older record version than the journal holds (D-056) — install a newer binary or replay under the old one from a backup |
