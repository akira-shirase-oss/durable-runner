//! durable-server: the Axum coordinator. One engine, one writer, HTTP on
//! top — submit/inspect/cancel/compact/backup. Handlers are allowlisted
//! HTTP endpoints configured at startup; the registry is the trust edge.

mod addr_limits;
mod mailout;
mod pubid;
mod ssrf;

use addr_limits::{
    client_addr, ConnGuard, ConnLimit, IpNet, RateLimit, TrustedProxies, DEFAULT_V6_PREFIX,
    MAX_LIMITED_ADDRS,
};
use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, FromRequestParts, Path as AxPath, Query, State},
    http::{header, request::Parts, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use base64::Engine as _;
use clap::Parser;
use durable_accounts::{
    AccountError, AccountStore, ApiKeyInfo, Mail, Mailer, Permission, Pg, Role,
};
use durable_domain::{JobSpec, Millis, OperationKey, RunId, RunState, StepSpec};
use durable_engine::observe::{self, Observer, Window};
use durable_engine::shell::{
    spawn, Call, Engine, Order, Outcome, Registry, ShellConfig, StepHandler, TestReply,
    MAX_HANDLERS_PER_WS,
};
use durable_engine::EngineError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "durable-server",
    version,
    about = "Durable workflow runner coordinator"
)]
struct Args {
    /// Journal directory (created on first run).
    #[arg(long, default_value = "data/journal")]
    journal: PathBuf,
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:58300")]
    listen: String,
    /// Dedicated ops listener (review finding 18): when set, the env
    /// keys (DURABLE_API_KEY / DURABLE_READ_KEY) are accepted only on
    /// this socket — point the platform's private network here and
    /// publish only `--listen`. A request on `--listen` bearing an env
    /// key answers 401 like any bad bearer. Must be a loopback address:
    /// anything else refuses at boot. Unset (self-host): env keys work
    /// on `--listen` as always.
    #[arg(long)]
    ops_listen: Option<String>,
    /// Dedicated liveness listener: /health and /ready on their own
    /// socket and backlog, so a connection flood on the main port cannot
    /// starve probes (S2 wedge: keep-alive hoarding RST'd /health at the
    /// accept queue). Point readiness/liveness checks here.
    #[arg(long)]
    health_listen: Option<String>,
    /// HTTP handler registrations: name=URL (repeatable).
    #[arg(long = "handler", value_parser = parse_handler)]
    handlers: Vec<(String, String)>,
    /// Permit the operator's own `--handler` flag URLs (workspace 0) to
    /// target loopback/private/link-local addresses — a handler on this
    /// host, development and fixtures. It never applies to handlers
    /// registered through the API (any workspace, the store): those are
    /// always checked against the full policy, whatever this flag says
    /// (re-review S1-1). Without it, literal local targets are refused at
    /// startup and any name that resolves to one is refused before a
    /// packet leaves (S2-5).
    #[arg(long)]
    allow_local_handlers: bool,
    /// LAB BUILDS ONLY (`--features lab`; shipped binaries have no such
    /// flag): let API-registered (tenant) handlers target local addresses,
    /// so the workspace load scenario and pilot can use loopback fixtures.
    #[cfg(feature = "lab")]
    #[arg(long)]
    allow_local_tenant_handlers: bool,
    /// Address range (repeatable, `ADDR/PREFIX` or one address) a handler
    /// call never dials, on top of the local blocks and this host's own
    /// interface addresses (which are refused automatically). List the
    /// droplet's reserved/floating IP and any other address that reaches
    /// this host without being on one of its interfaces. Refused even
    /// with --allow-local-handlers.
    #[arg(long = "deny-cidr", value_parser = ssrf::parse_cidr)]
    deny_cidrs: Vec<(std::net::IpAddr, u8)>,
    /// Attempt timeout per step call, ms (bounded by domain limits).
    #[arg(long, default_value_t = 20_000)]
    step_timeout_ms: u64,
    /// Static dashboard UI directory, served at /app (optional).
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    /// Directory that holds backups. `POST /admin/backup {"name": N}`
    /// writes only to `<backup-root>/N`; without this flag backups are
    /// disabled. Created at startup if missing.
    #[arg(long)]
    backup_root: Option<PathBuf>,
    /// Bytes that must stay free on the journal filesystem. Below it,
    /// submits are refused 503 `no_space`, `/ready` is false, and
    /// compaction and backup refuse unless what they write fits above it.
    #[arg(long, default_value_t = durable_journal::DEFAULT_RESERVE_BYTES)]
    reserve_bytes: u64,
    /// Compact (expire week-old terminal runs, delete superseded
    /// generations) every N seconds; 0 (the default) = only on request.
    #[arg(long, default_value_t = 0)]
    compact_every_secs: u64,
    /// Refuse a compaction whose snapshot would exceed this many bytes
    /// (409 `snapshot_over_budget`) instead of writing it.
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    max_snapshot_bytes: u64,
    /// Non-terminal runs admitted at once; beyond it submits are 503
    /// `at_capacity`.
    #[arg(long, default_value_t = 100_000)]
    max_live_runs: usize,
    /// One workspace's share of the live-run bound — a tenant at its
    /// share is refused while everyone else keeps submitting (S2 #6).
    #[arg(long, default_value_t = 25_000)]
    max_live_runs_per_ws: usize,
    /// Approximate bytes of all retained runs; beyond it submits are 503
    /// `at_capacity`. Default 512 MiB, sized for a 2 GB host (D-031).
    #[arg(long, default_value_t = durable_domain::Limits::default().max_index_bytes)]
    max_index_bytes: u64,
    /// One workspace's share of the index bytes — a single tenant at
    /// this bound is refused while the rest keep submitting. The
    /// effective share is also capped at a quarter of `max_index_bytes`.
    #[arg(long, default_value_t = durable_domain::Limits::default().max_index_bytes_per_ws)]
    max_index_bytes_per_ws: u64,
    /// Handler calls (attempts and probes) in flight at once. A submit
    /// burst saturates the permits and defers the rest to the tick — 32
    /// held the workspaces drain to ~640 runs/s measured (S2-7).
    #[arg(long, default_value_t = 256)]
    max_in_flight: usize,
    /// The share of `--max-in-flight` one workspace may hold at once —
    /// a tenant's slow handler parks at most this many calls (S2
    /// review #6). The default comes from `ShellConfig` so it cannot
    /// drift from the value the engine documents (S2 re-review N3);
    /// compat (workspace-0 only) deployments get the whole pool like
    /// v1.0.x — see `ws_in_flight_cap`.
    #[arg(long, default_value_t = ShellConfig::default().max_in_flight_per_ws)]
    max_in_flight_per_ws: usize,
    /// Dispatches per scheduler tick (ticks every 50 ms, so the deferred
    /// dispatch rate is at most 20 × this per second — 64 capped a backlog
    /// at ~1,280/s, the measured ~1.3k/s workspaces bound; S2-7).
    #[arg(long, default_value_t = 512)]
    max_dispatch_per_tick: usize,
    /// How long a submit, cancel or read waits for the engine before it is
    /// answered 503 `timeout` (a stuck disk must not hang every request).
    #[arg(long, default_value_t = 10_000)]
    reply_timeout_ms: u64,
    /// Serve reads (`GET /jobs`, `GET /jobs/{id}`) without a key. Refused
    /// at startup unless `--listen` is a loopback address: anything that
    /// can reach a non-loopback listener could read every op_key (D-037).
    #[arg(long)]
    open_reads: bool,
    /// The public origin every mailed link and redirect is built from
    /// (e.g. https://durablerunnerapp.example.com) — never the request
    /// Host. Required at boot when an account store is configured
    /// (review #18); absent it then, the server refuses to start.
    #[arg(long)]
    public_origin: Option<String>,
    /// Connections served at once; more wait in the kernel's accept queue.
    /// Keep it below the process's descriptor limit (`ulimit -n`) less 64.
    #[arg(long, default_value_t = 512)]
    max_connections: usize,
    /// A request head (or the next one on an idle keep-alive connection)
    /// must arrive within this many ms, or the connection is closed.
    #[arg(long, default_value_t = 10_000)]
    header_timeout_ms: u64,
    /// A request body must arrive in full within this many ms.
    #[arg(long, default_value_t = 30_000)]
    body_deadline_ms: u64,
    /// A response write that makes no progress for this many ms closes the
    /// connection (a client that does not read its responses).
    #[arg(long, default_value_t = 10_000)]
    write_stall_ms: u64,
    /// A connection is closed after this many seconds, once the request in
    /// flight is answered (keep-alive clients reconnect).
    #[arg(long, default_value_t = 600)]
    max_connection_secs: u64,
    /// A connection's this-many-th answer says `Connection: close`, so a
    /// client pipelining requests gets at most this many per connection.
    #[arg(long, default_value_t = 100)]
    max_requests_per_connection: usize,
    /// Requests per second one client address may make before it is
    /// refused 429 `rate_limited` with a Retry-After (a token bucket with
    /// a one-second burst; the address table is bounded). 0 disables.
    #[arg(long, default_value_t = 100.0)]
    rate_limit_per_addr: f64,
    /// Prefix bits one IPv6 client address is keyed by for the rate
    /// limit: a /64 holds 2^64 addresses, so keying the full address
    /// would let one client mint unlimited buckets. Inside a /48 the
    /// per-address buckets also share a /48 aggregate at 10x this rate.
    #[arg(long, default_value_t = DEFAULT_V6_PREFIX, value_parser = parse_v6_prefix)]
    rate_limit_v6_prefix: u8,
    /// Connections one untrusted peer address may hold at once; past the
    /// cap the socket is closed at accept. 0 disables.
    #[arg(long, default_value_t = 64)]
    max_conns_per_addr: usize,
    /// Connections one peer inside the trusted-proxy ranges may hold: a
    /// proxy's edge legitimately carries many real clients at once, so
    /// it needs its own, higher cap (the global `--max-connections`
    /// still bounds everything). 0 = unlimited.
    #[arg(long, default_value_t = 4096)]
    max_conns_per_proxy: usize,
    /// Proxy CIDR (repeatable) whose `CF-Connecting-IP` header names the
    /// client address for the per-address limits — Cloudflare's ranges
    /// when Cloudflare fronts this server. The header from any other
    /// peer is ignored: anyone can set it.
    #[arg(long = "trusted-proxy", value_parser = IpNet::parse)]
    trusted_proxies: Vec<IpNet>,
    /// On SIGTERM or SIGINT: how long to let requests in flight finish,
    /// and then how long the engine may take to make everything it
    /// decided durable, before exiting non-zero instead of 0.
    #[arg(long, default_value_t = 30)]
    drain_timeout_secs: u64,
    /// Print this binary's journal-format capabilities and exit:
    /// `<max record version> <max snapshot (DRIX) version> <parks ws≠0>`
    /// — three numbers for v1.0.2+, a single number before it.
    /// deploy/upgrade.sh and deploy/rollback.sh compare them against the
    /// journal's highest versions (D-056).
    #[arg(long)]
    journal_format: bool,
}

/// The environment variable holding the API key. Read from the
/// environment, never from argv (argv is visible to every local user).
const API_KEY_ENV: &str = "DURABLE_API_KEY";
/// An optional second key that may only read (`GET /jobs`, `GET
/// /jobs/{id}`): what the inspector or a dashboard should hold.
const READ_KEY_ENV: &str = "DURABLE_READ_KEY";
const MIN_API_KEY_LEN: usize = 32;
/// Largest request body accepted on any route.
const MAX_REQUEST_BYTES: usize = 256 * 1024;

/// `name=URL`, where the URL is http or https with a host: the allowlist
/// can only name HTTP endpoints.
fn parse_handler(s: &str) -> Result<(String, String), String> {
    let (name, url) = s.split_once('=').ok_or("expected name=URL")?;
    if name.is_empty() {
        return Err("a handler needs a name before '='".into());
    }
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("handler URL {url:?}: {e}"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none_or(str::is_empty) {
        return Err(format!(
            "handler URL {url:?} must be http:// or https:// with a host"
        ));
    }
    Ok((name.to_string(), url.to_string()))
}

/// `--rate-limit-v6-prefix`: 0–128 bits.
fn parse_v6_prefix(s: &str) -> std::result::Result<u8, String> {
    let v: u8 = s.parse().map_err(|_| format!("bad prefix {s:?}"))?;
    if v > 128 {
        return Err(format!("an IPv6 prefix is at most /128, got /{v}"));
    }
    Ok(v)
}

/// The client every handler call goes through (D-039): it never follows a
/// redirect (a 3xx is the handler's answer, a failed attempt, so a job's
/// input and idempotency key go only to the registered URL), it ignores
/// the proxy environment variables (nothing outside the allowlist can
/// route calls), and it gives up connecting after 5 s.
fn handler_client(policy: &ssrf::Policy) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        // The SSRF guard (S2-5): every name resolves through the OS and
        // each returned address must pass `is_local` before reqwest is
        // told it exists — the check is the resolution, so a DNS answer
        // that flips local between registration and dispatch sends zero
        // packets. No TOCTOU by construction.
        .dns_resolver(ssrf::resolver(policy))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("build the handler HTTP client")
}

/// An allowlisted HTTP handler: POSTs `{input, idem_key}` to its URL and
/// classifies the reply honestly — 2xx output, other status failure,
/// transport error or timeout → Lost (unknown, never "failed").
struct HttpHandler {
    client: reqwest::Client,
    url: String,
    timeout: Duration,
    /// Largest reply body read; a bigger one is refused mid-stream.
    max_output: usize,
    /// The workspace's signing secret (S2-6); `None` is compat mode and
    /// `--handler` flag registrations — their calls go unsigned.
    secret: Option<Arc<[u8; 32]>>,
    /// The parsed URL for the call-time SSRF re-check (S2 review #3):
    /// literal-IP hosts never reach the resolver — hyper dials them
    /// directly — so a literal target planted by a store row, a file
    /// edit, or an `--allow-local-handlers` process must be refused
    /// here, at the last point before the wire. `None` = unparseable,
    /// refused like any local target.
    policy_url: Option<reqwest::Url>,
    /// The SSRF policy the check shares with registration and the
    /// resolver (local blocks, this host's own addresses, --deny-cidr).
    policy: ssrf::Policy,
    /// `x-durable-run-id` carries the opaque public id, never the
    /// global sequence — a raw one would tell the tenant how much work
    /// every other workspace submitted between two of its calls.
    pubid: pubid::PubId,
}

impl HttpHandler {
    /// The dial-time check every call and probe must pass: a literal
    /// local (or unparseable) target is a refused attempt — definitive,
    /// nothing sent — mirroring `ssrf::refused`'s classification.
    fn policy_ok(&self) -> bool {
        match &self.policy_url {
            // ATTACK(call-time-literal)
            Some(u) => ssrf::check_url(u, &self.policy).is_ok(),
            None => false,
        }
    }
}

/// Read a reply body, stopping as soon as it would exceed `max` bytes —
/// never buffering an unbounded body first. `Ok(None)` = over the limit.
async fn read_bounded(
    mut res: reqwest::Response,
    max: usize,
) -> Result<Option<Vec<u8>>, reqwest::Error> {
    if res.content_length().is_some_and(|n| n > max as u64) {
        return Ok(None); // declared too big: not a byte is read
    }
    let mut body = Vec::new();
    while let Some(chunk) = res.chunk().await? {
        if body.len() + chunk.len() > max {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// The headers that tell a handler which call it is answering (D-050): the
/// run `GET /jobs/{id}` describes, the step's index, and the attempt (for a
/// probe, the attempt being reconciled). Numbers only: nothing the handler
/// was not already told by the idempotency key, and no secret.
const RUN_ID_HEADER: &str = "x-durable-run-id";
const STEP_HEADER: &str = "x-durable-step";
const ATTEMPT_HEADER: &str = "x-durable-attempt";
/// S2-6's signed-call headers: the timestamp bounds replay, the
/// signature binds run, step, attempt, idempotency key and body to the
/// workspace's secret. Documented in `docs/HANDLERS.md`.
const TS_HEADER: &str = "x-durable-timestamp";
const SIG_HEADER: &str = "x-durable-signature";

/// `HMAC_SHA256(secret, "{ts}\n{method}\n{authority}\n{path}\n{run}\n{step}\n{attempt}\n{idem}\n{digest}")`
/// — newline-separated so no field can be re-split into its neighbours
/// (`idem_key` may carry `.`; it cannot carry `\n` — the runner rejects
/// control bytes in keys), and bound to the request's method, target
/// authority and path so a signature minted for one call replays
/// nowhere else — not even a same-path handler at a different host in
/// the same workspace (S3). Run/step/attempt are `-` on a compat
/// (no-`Call`) path; `digest` is hex sha256 of the body for a call and
/// empty for a probe.
#[allow(clippy::too_many_arguments)]
fn sign_call(
    secret: &[u8; 32],
    ts: u64,
    method: &str,
    authority: &str,
    path: &str,
    r: &str,
    s: &str,
    a: &str,
    idem_key: &str,
    body_digest: &str,
) -> String {
    let canonical =
        format!("{ts}\n{method}\n{authority}\n{path}\n{r}\n{s}\n{a}\n{idem_key}\n{body_digest}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
    let tag = ring::hmac::sign(&key, canonical.as_bytes());
    hex(tag.as_ref())
}

#[allow(clippy::too_many_arguments)]
fn with_call(
    req: reqwest::RequestBuilder,
    call: Option<Call>,
    secret: Option<&[u8; 32]>,
    idem_key: &str,
    body_digest: &str,
    method: &str,
    authority: &str,
    path: &str,
    pubid: &pubid::PubId,
) -> reqwest::RequestBuilder {
    let (r, s, a) = call.map_or_else(
        || ("-".to_string(), "-".to_string(), "-".to_string()),
        |c| {
            (
                pubid.encode(c.run),
                c.step.0.to_string(),
                c.attempt.to_string(),
            )
        },
    );
    let req = match call {
        Some(_) => req
            .header(RUN_ID_HEADER, &r)
            .header(STEP_HEADER, &s)
            .header(ATTEMPT_HEADER, &a),
        None => req,
    };
    match secret {
        Some(secret) => {
            let ts = unix_secs();
            req.header(TS_HEADER, ts).header(
                SIG_HEADER,
                sign_call(
                    secret,
                    ts,
                    method,
                    authority,
                    path,
                    &r,
                    &s,
                    &a,
                    idem_key,
                    body_digest,
                ),
            )
        }
        None => req,
    }
}

impl HttpHandler {
    async fn call(&self, call: Option<Call>, input: &[u8], idem_key: &str) -> Outcome {
        // Literal IPs skip the DNS resolver entirely — re-check the
        // target at the last point before the wire.
        if !self.policy_ok() {
            return Outcome::Failed;
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "input": input,
            "idem_key": idem_key,
        }))
        .unwrap_or_default();
        let digest = hex(&sha256(&body));
        let req = self
            .client
            .post(&self.url)
            .header("x-idempotency-key", idem_key)
            .header("content-type", "application/json")
            .body(body)
            .timeout(self.timeout);
        let (authority, path) = self.authority_and_path();
        let res = with_call(
            req,
            call,
            self.secret.as_deref(),
            idem_key,
            &digest,
            "POST",
            &authority,
            &path,
            &self.pubid,
        )
        .send()
        .await;
        match res {
            Ok(r) if r.status().is_success() => match read_bounded(r, self.max_output).await {
                Ok(Some(b)) => Outcome::Checkpoint(b),
                // The reply breaks the output bound: the attempt failed (it
                // is retried under the same idempotency key, then fails).
                Ok(None) => Outcome::Failed,
                Err(_) => Outcome::Lost,
            },
            Ok(r) if r.status().is_server_error() => Outcome::Failed,
            Ok(r) if r.status().is_client_error() => Outcome::Failed,
            Ok(_) => Outcome::Failed,
            Err(e) if e.is_timeout() => Outcome::Lost,
            // The SSRF guard's refusal is definitive — the target is
            // forbidden, so the attempt failed; nothing was sent.
            Err(e) if ssrf::refused(&e) => Outcome::Failed,
            Err(e) if e.is_connect() => Outcome::Failed,
            Err(_) => Outcome::Lost, // genuinely unknown — reconcile
        }
    }

    /// The D-070 probe: ONE call, exactly the dispatched path's request
    /// shape — same body, same signing, same policy recheck, same
    /// timeout, same bounded read — with no `Call` context and a reply
    /// detail instead of an `Outcome`. Never journaled.
    async fn probe_call_once(&self, input: &[u8], idem_key: &str) -> TestReply {
        let start = Instant::now();
        let refused = || TestReply {
            status: "refused",
            http_status: None,
            latency_ms: 0,
            excerpt: Vec::new(),
        };
        if !self.policy_ok() {
            return refused();
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "input": input,
            "idem_key": idem_key,
        }))
        .unwrap_or_default();
        let digest = hex(&sha256(&body));
        let req = self
            .client
            .post(&self.url)
            .header("x-idempotency-key", idem_key)
            .header("content-type", "application/json")
            .body(body)
            .timeout(self.timeout);
        let (authority, path) = self.authority_and_path();
        let res = with_call(
            req,
            None,
            self.secret.as_deref(),
            idem_key,
            &digest,
            "POST",
            &authority,
            &path,
            &self.pubid,
        )
        .send()
        .await;
        let latency = start.elapsed().as_millis() as u64;
        match res {
            Ok(r) => {
                let http = r.status();
                match read_bounded(r, 1024).await {
                    Ok(excerpt) => TestReply {
                        status: if http.is_success() { "ok" } else { "failed" },
                        http_status: Some(http.as_u16()),
                        latency_ms: latency,
                        excerpt: excerpt.unwrap_or_default(),
                    },
                    Err(_) => TestReply {
                        status: "failed",
                        http_status: Some(http.as_u16()),
                        latency_ms: latency,
                        excerpt: Vec::new(),
                    },
                }
            }
            Err(e) if ssrf::refused(&e) => refused(),
            Err(e) if e.is_timeout() => TestReply {
                status: "lost",
                http_status: None,
                latency_ms: latency,
                excerpt: Vec::new(),
            },
            Err(e) if e.is_connect() => TestReply {
                status: "failed",
                http_status: None,
                latency_ms: latency,
                excerpt: Vec::new(),
            },
            Err(_) => TestReply {
                status: "lost",
                http_status: None,
                latency_ms: latency,
                excerpt: Vec::new(),
            },
        }
    }

    /// Probe asks the handler whether the call under `idem_key` landed.
    /// `GET {url}?idem_key=…` → 200 means found; anything else is unknown
    /// → reported as not-found honestly (default trait behaviour).
    async fn ask(&self, call: Option<Call>, idem_key: &str) -> bool {
        if !self.policy_ok() {
            return false;
        }
        let req = self
            .client
            .get(&self.url)
            .query(&[("idem_key", idem_key)])
            .timeout(self.timeout);
        // A probe signs an empty body digest — the same canonical
        // shape, GET and the URL's authority+path bound in.
        let (authority, path) = self.authority_and_path();
        with_call(
            req,
            call,
            self.secret.as_deref(),
            idem_key,
            "",
            "GET",
            &authority,
            &path,
            &self.pubid,
        )
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
    }

    /// The signed authority is the handler's configured authority —
    /// brackets on an IPv6 literal, a port only when the URL names a
    /// non-default one — bound at registration, never the received
    /// `Host` (a replayer controls that: a sig minted for `a/step`
    /// must never verify at `b/step`, S2 re-review #4).
    fn authority_and_path(&self) -> (String, String) {
        self.policy_url
            .as_ref()
            .map(|u| {
                let host = match u.host() {
                    Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
                    Some(url::Host::Ipv4(ip)) => ip.to_string(),
                    Some(url::Host::Domain(d)) => d.to_string(),
                    None => String::new(),
                };
                let authority = match u.port() {
                    Some(p) => format!("{host}:{p}"),
                    None => host,
                };
                (authority, u.path().to_string())
            })
            .unwrap_or_else(|| ("".into(), "/".into()))
    }
}

#[async_trait::async_trait]
impl StepHandler for HttpHandler {
    async fn execute(&self, _step: &StepSpec, input: &[u8], idem_key: &str) -> Outcome {
        self.call(None, input, idem_key).await
    }

    async fn probe(&self, _step: &StepSpec, idem_key: &str) -> bool {
        self.ask(None, idem_key).await
    }

    async fn execute_call(
        &self,
        call: Call,
        _step: &StepSpec,
        input: &[u8],
        idem_key: &str,
    ) -> Outcome {
        self.call(Some(call), input, idem_key).await
    }

    async fn probe_call(&self, call: Call, _step: &StepSpec, idem_key: &str) -> bool {
        self.ask(Some(call), idem_key).await
    }

    /// The D-070 probe goes through the real signed request path — a
    /// stored-url handler is exercised exactly as a dispatch would,
    /// minus journaling.
    async fn test_call(&self, _step: &StepSpec, input: &[u8], idem_key: &str) -> TestReply {
        self.probe_call_once(input, idem_key).await
    }
}

// ---- S2-4: the per-workspace handler registry ---------------------------

/// Builds `HttpHandler`s for registered URLs through the one bounded
/// client — the same limits every `--handler` flag gets.
struct HandlerFactory {
    client: reqwest::Client,
    timeout: Duration,
    max_output: usize,
    /// The SSRF policy for API-registered handlers (every workspace, the
    /// store): the registration check, each built handler's call-time
    /// check, and `client`'s resolver. Strict in every shipped build —
    /// `--allow-local-handlers` never reaches it (re-review S1-1).
    store_policy: ssrf::Policy,
    /// The operator's `--handler` flags (workspace 0): `--allow-local-handlers`
    /// applies here only, with its own client and resolver.
    flag_policy: ssrf::Policy,
    flag_client: reqwest::Client,
    /// The run-id encoder every built handler signs its headers with.
    pubid: pubid::PubId,
}

impl HandlerFactory {
    /// The factory with one client per policy (each client's resolver
    /// enforces its own policy at dial time).
    fn new(
        store_policy: ssrf::Policy,
        flag_policy: ssrf::Policy,
        timeout: Duration,
        max_output: usize,
        pubid: pubid::PubId,
    ) -> Result<Self> {
        Ok(Self {
            client: handler_client(&store_policy)?,
            flag_client: handler_client(&flag_policy)?,
            timeout,
            max_output,
            store_policy,
            flag_policy,
            pubid,
        })
    }

    /// A `--handler` flag registration (workspace 0) — unsigned, under the
    /// flag policy.
    fn make(&self, url: &str) -> Arc<HttpHandler> {
        self.build(url, None, &self.flag_client, &self.flag_policy)
    }

    /// An API-registered handler that signs with the workspace's secret,
    /// under the store policy.
    fn make_signed(&self, url: &str, secret: Option<Arc<[u8; 32]>>) -> Arc<HttpHandler> {
        self.build(
            url,
            secret,
            // The store's client: its resolver judges every name at dial.
            // ATTACK(signed-store-client)
            &self.client,
            // The store's policy: the call-time check of a literal address.
            // ATTACK(signed-store-policy)
            &self.store_policy,
        )
    }

    fn build(
        &self,
        url: &str,
        secret: Option<Arc<[u8; 32]>>,
        client: &reqwest::Client,
        policy: &ssrf::Policy,
    ) -> Arc<HttpHandler> {
        Arc::new(HttpHandler {
            client: client.clone(),
            url: url.to_string(),
            timeout: self.timeout,
            max_output: self.max_output,
            secret,
            policy_url: reqwest::Url::parse(url).ok(),
            policy: policy.clone(),
            pubid: self.pubid.clone(),
        })
    }
}

/// The onboarding sample handler (ruling 5'): the reserved name
/// `builtin:echo` registers like any handler but executes in-process —
/// no network hop, nothing to sign, and it can never be a reflector.
/// Its "url" is the marker string itself.
const BUILTIN_ECHO: &str = "builtin:echo";

struct BuiltinEcho;
#[async_trait::async_trait]
impl StepHandler for BuiltinEcho {
    async fn execute(&self, _step: &StepSpec, input: &[u8], _idem_key: &str) -> Outcome {
        Outcome::Checkpoint(input.to_vec())
    }
    /// The D-070 probe runs in-process through the same outcome mapping.
    async fn test_call(&self, step: &StepSpec, input: &[u8], idem_key: &str) -> TestReply {
        let start = Instant::now();
        let outcome = self.execute(step, input, idem_key).await;
        TestReply::from_outcome(outcome, start.elapsed().as_millis() as u64)
    }
}

/// What one stored registration arms: `builtin:echo` is in-process,
/// every other URL builds through the signed, SSRF-guarded path.
fn arm_url(
    factory: &HandlerFactory,
    url: &str,
    secret: Option<Arc<[u8; 32]>>,
) -> Arc<dyn StepHandler> {
    if url == BUILTIN_ECHO {
        Arc::new(BuiltinEcho)
    } else {
        factory.make_signed(url, secret)
    }
}

/// The admission rule for a registered URL: http(s) with a host, and a
/// literal local target refused unless `--allow-local-handlers` (S2-5's
/// registration layer — domains are judged at dial by the resolver).
fn valid_handler_url(url: &str, policy: &ssrf::Policy) -> Result<String, ApiError> {
    // Bounded like everything else (S2 review #6): an unbounded URL is
    // an unbounded registry row, snapshot line, and dial string.
    if url.len() > 2048 {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "bad_handler_url",
            "handler URL exceeds 2048 bytes".to_string(),
        ));
    }
    let parsed = reqwest::Url::parse(url).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_handler_url",
            format!("handler URL {url:?}: {e}"),
        )
    })?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none_or(str::is_empty) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_handler_url",
            format!("handler URL {url:?} must be http:// or https:// with a host"),
        ));
    }
    // ATTACK(ssrf-register)
    ssrf::check_url(&parsed, policy).map_err(|why| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "local_target_forbidden",
            format!("handler URL {url:?} refused: {why}"),
        )
    })?;
    Ok(url.to_string())
}

/// One persisted registration: the internal ws id stays on disk (never an
/// external surface), matching the journal's own records. `secret` is the
/// workspace's signing secret as hex — the file is mode 0600 beside the
/// journal; a restart during a store outage must still sign (S2-6).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RegistryEntry {
    ws: u64,
    name: String,
    url: String,
    #[serde(default)]
    secret: Option<String>,
}

/// The last-known-good registry (`<journal>/handlers.json`): read at
/// startup so a restart during an account-store outage dispatches from
/// the snapshot instead of losing every registered handler. Rebuildable
/// from the store — it is a cache, never an authority.
fn load_registry_file(path: &Path) -> Result<Vec<RegistryEntry>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| {
            anyhow::anyhow!(
                "{path:?} is corrupt ({e}); refusing to treat last-known-good rows as empty"
            )
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(anyhow::anyhow!("cannot read {path:?}: {e}")),
    }
}

/// Whole-file replace, tmp + fsync + rename: a crash mid-write leaves
/// either the old snapshot or the new one, never a torn file — and the
/// fsync makes "the new one" durable, not merely renamed (S3: the file
/// carried secrets but the write was never forced to disk).
fn save_registry_file(path: &Path, entries: &[RegistryEntry]) -> Result<()> {
    use std::io::Write as _;
    let tmp = path.with_extension("json.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&serde_json::to_vec(entries)?)?;
    f.sync_all()?;
    // The file carries signing secrets — owner-only like the journal.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    // The rename itself must reach disk or a crash can resurrect the old
    // registry under the same path.
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// How often the store reconciler refreshes the engine's registry and
/// the snapshot file. Dispatch reads the in-memory registry — this loop
/// is never in a call's path.
const REGISTRY_SYNC: Duration = Duration::from_secs(30);
/// Workspaces the reconciler walks per pass (bounded per D-021).
const REGISTRY_SYNC_WS_LIMIT: u32 = 1024;

/// One serialized write to `handlers.json` (S3): every mutation and every
/// reconcile snapshot goes through the writer thread — the load, merge,
/// tmp-write and two fsyncs run off the async workers, and the epoch
/// check happens under the same serialization the file gets.
enum RegistryWrite {
    /// Replace the `(ws, name)` row with `entry`.
    Upsert {
        entry: RegistryEntry,
        ack: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Drop the `(ws, name)` row.
    Remove {
        ws: u64,
        name: String,
        ack: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Replace one workspace's whole set (a secret rotation).
    ReplaceWs {
        ws: u64,
        entries: Vec<RegistryEntry>,
        ack: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// A reconcile pass's whole-file snapshot, taken at `epoch_seen`.
    /// `preserve_ws` holds workspaces the pass saw but could not arm —
    /// their last-known-good rows carry forward so a refused set never
    /// strands a workspace's handlers at the next restart (S2 review N2).
    Snapshot {
        entries: Vec<RegistryEntry>,
        preserve_ws: Vec<u64>,
        epoch_seen: u64,
        /// The pass is settled when the writer has decided — applied or
        /// discarded as stale, either way the sender hears about it.
        ack: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

/// The most queued writes one save covers.
const REGISTRY_BATCH: usize = 512;

/// The single writer of `handlers.json`. Mutations apply in send order;
/// a reconcile snapshot lands only while the epoch it read still holds —
/// the check is inside the serialization, so an overtaken pass can never
/// overwrite a mutation that already won.
///
/// Re-review S2-4: the file is read once and kept in memory; each wake
/// drains everything queued (up to `REGISTRY_BATCH`), applies it in order,
/// and saves ONCE if anything changed — one tmp-write and two fsyncs per
/// batch, none for a batch that changed nothing. A failed save answers
/// every changing write in the batch with the error and keeps the last
/// saved state, so memory never runs ahead of the disk.
fn registry_writer(
    path: PathBuf,
    epoch: Arc<std::sync::atomic::AtomicU64>,
    rx: std::sync::mpsc::Receiver<RegistryWrite>,
    saves: Arc<std::sync::atomic::AtomicU64>,
) {
    let mut saved: Option<Vec<RegistryEntry>> = None;
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        // ATTACK(registry-batch)
        while batch.len() < REGISTRY_BATCH {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        if saved.is_none() {
            match load_registry_file(&path) {
                Ok(v) => saved = Some(v),
                Err(e) => {
                    // Unreadable or corrupt last-known-good: never merge
                    // onto an empty view — the write would erase every
                    // workspace's rows. Report it; the caller's 503 is the
                    // honest answer (re-review #8).
                    eprintln!("durable-server: {e}");
                    let msg = e.to_string();
                    for op in batch {
                        let _ = op.into_ack().send(Err(anyhow::anyhow!("{msg}")));
                    }
                    continue;
                }
            }
        }
        let Some(current) = saved.as_ref() else {
            continue;
        };
        let mut all = current.clone();
        // (ack, whether this write changed the file)
        let mut acks = Vec::with_capacity(batch.len());
        for op in batch {
            let before = all.len();
            let changed = match op {
                RegistryWrite::Upsert { entry, ack } => {
                    let same = all.iter().any(|e| {
                        e.ws == entry.ws
                            && e.name == entry.name
                            && e.url == entry.url
                            && e.secret == entry.secret
                    });
                    if !same {
                        all.retain(|e| !(e.ws == entry.ws && e.name == entry.name));
                        all.push(entry);
                    }
                    acks.push((ack, !same));
                    continue;
                }
                RegistryWrite::Remove { ws, name, ack } => {
                    all.retain(|e| !(e.ws == ws && e.name == name));
                    acks.push((ack, all.len() != before));
                    continue;
                }
                RegistryWrite::ReplaceWs { ws, entries, ack } => {
                    all.retain(|e| e.ws != ws);
                    all.extend(entries);
                    acks.push((ack, true));
                    continue;
                }
                RegistryWrite::Snapshot {
                    entries,
                    preserve_ws,
                    epoch_seen,
                    ack,
                } => {
                    if epoch.load(std::sync::atomic::Ordering::Acquire) == epoch_seen {
                        all.retain(|e| preserve_ws.contains(&e.ws));
                        all.extend(entries);
                        (ack, true)
                    } else {
                        eprintln!(
                            "durable-server: registry reconcile discarded a pass overtaken by a mutation"
                        );
                        (ack, false)
                    }
                }
            };
            acks.push(changed);
        }
        let any_change = acks.iter().any(|(_, c)| *c) && all != *current;
        let result = if any_change {
            let r = save_registry_file(&path, &all)
                .map_err(|e| anyhow::anyhow!("cannot write {path:?}: {e}"));
            match &r {
                Ok(()) => {
                    saves.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    saved = Some(all);
                }
                Err(e) => eprintln!("durable-server: {e}"),
            }
            r.map_err(|e| e.to_string())
        } else {
            Ok(())
        };
        for (ack, changed) in acks {
            let r = match (&result, changed) {
                (Err(msg), true) => Err(anyhow::anyhow!("{msg}")),
                _ => Ok(()),
            };
            let _ = ack.send(r);
        }
    }
}

impl RegistryWrite {
    fn into_ack(self) -> tokio::sync::oneshot::Sender<Result<()>> {
        match self {
            RegistryWrite::Upsert { ack, .. }
            | RegistryWrite::Remove { ack, .. }
            | RegistryWrite::ReplaceWs { ack, .. }
            | RegistryWrite::Snapshot { ack, .. } => ack,
        }
    }
}

/// Per-workspace serialization of registry mutations against reconcile
/// passes (S2 review finding 4). A mutation holds its workspace's stripe
/// across the store write + engine update + file send, and a reconcile
/// holds the same stripe across `list_handlers` + `set_workspace_handlers`
/// — a DELETE can never land between a pass's read of a row and its
/// re-arm, which is what let a deleted handler resurrect in the engine
/// and in `handlers.json`. The epoch stays as the writer's third layer;
/// the lock closes the check-then-act window it could not see. Striped
/// rather than keyed: ws N and ws N+64 share a stripe, which at most
/// serializes two unrelated tenants, never one workspace's own flow.
pub struct RegistryLocks([tokio::sync::Mutex<()>; 64]);

impl RegistryLocks {
    fn stripe(&self, ws: u64) -> &tokio::sync::Mutex<()> {
        &self.0[(ws % self.0.len() as u64) as usize]
    }
}

impl Default for RegistryLocks {
    fn default() -> Self {
        Self(std::array::from_fn(|_| tokio::sync::Mutex::new(())))
    }
}

/// Spawn the registry writer thread; the sender goes in `App`. `saves`
/// counts the file saves it makes.
fn spawn_registry_writer(
    path: Arc<Path>,
    epoch: Arc<std::sync::atomic::AtomicU64>,
    saves: Arc<std::sync::atomic::AtomicU64>,
) -> std::sync::mpsc::SyncSender<RegistryWrite> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<RegistryWrite>(512);
    let path = path.to_path_buf();
    std::thread::Builder::new()
        .name("registry-writer".into())
        .spawn(move || registry_writer(path, epoch, rx, saves))
        .unwrap_or_else(|e| panic!("registry writer thread: {e}"));
    tx
}

/// API registry mutations (PUT/DELETE a handler, rotate the secret) a
/// workspace may make per second, and its burst (re-review S2-4: one
/// tenant at 100/s cost 200 fsyncs/s on the journal's disk and three
/// store queries each).
const REGISTRY_MUTATIONS_PER_SEC: f64 = 2.0;
const REGISTRY_MUTATION_BURST: f64 = 20.0;
/// Workspaces whose mutation buckets are kept at once (full, idle ones
/// are dropped first; past it every newcomer waits a second).
const REGISTRY_RATE_TABLE: usize = 4096;

/// The registry's per-process state beside the writer thread.
#[derive(Debug, Default)]
struct RegistryState {
    /// What this process has armed from the API (store rows, not
    /// `--handler` flags): ws → name → url. A PUT of exactly what is
    /// armed is a no-op (re-review S2-4). Kept by every mutation, the
    /// reconcile pass and the startup snapshot.
    view: std::sync::Mutex<HashMap<u64, HashMap<String, String>>>,
    /// Per-workspace mutation buckets: (tokens, refilled at).
    rate: std::sync::Mutex<HashMap<u64, (f64, Instant)>>,
    /// File saves the writer has made (tests and diagnostics).
    saves: Arc<std::sync::atomic::AtomicU64>,
}

impl RegistryState {
    fn armed(&self, ws: u64, name: &str) -> Option<String> {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ws)
            .and_then(|m| m.get(name).cloned())
    }

    fn set(&self, ws: u64, name: &str, url: &str) {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(ws)
            .or_default()
            .insert(name.to_string(), url.to_string());
    }

    fn remove(&self, ws: u64, name: &str) {
        let mut v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(m) = v.get_mut(&ws) {
            m.remove(name);
            if m.is_empty() {
                v.remove(&ws);
            }
        }
    }

    fn replace_ws(&self, ws: u64, rows: impl IntoIterator<Item = (String, String)>) {
        let m: HashMap<String, String> = rows.into_iter().collect();
        let mut v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        if m.is_empty() {
            v.remove(&ws);
        } else {
            v.insert(ws, m);
        }
    }

    /// Ok: `ws` may mutate its registry now. Err: refuse, retry after.
    fn admit(&self, ws: u64, now: Instant) -> Result<(), Duration> {
        let mut m = self.rate.lock().unwrap_or_else(|e| e.into_inner());
        if !m.contains_key(&ws) && m.len() >= REGISTRY_RATE_TABLE {
            m.retain(|_, (tokens, at)| {
                let refill =
                    now.saturating_duration_since(*at).as_secs_f64() * REGISTRY_MUTATIONS_PER_SEC;
                *tokens + refill < REGISTRY_MUTATION_BURST
            });
            if m.len() >= REGISTRY_RATE_TABLE {
                return Err(Duration::from_secs(1));
            }
        }
        let (tokens, at) = m.entry(ws).or_insert((REGISTRY_MUTATION_BURST, now));
        let refill = now.saturating_duration_since(*at).as_secs_f64() * REGISTRY_MUTATIONS_PER_SEC;
        *tokens = (*tokens + refill).min(REGISTRY_MUTATION_BURST);
        *at = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64(
                (1.0 - *tokens) / REGISTRY_MUTATIONS_PER_SEC,
            ))
        }
    }
}

/// The per-workspace registry-mutation limit, as the API's refusal.
fn registry_rate(app: &App, ws: u64) -> Result<(), ApiError> {
    // ATTACK(registry-rate)
    app.registry.admit(ws, Instant::now()).map_err(|wait| {
        ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "registry_rate_limited",
            format!(
                "this workspace changes its handler registry too often; retry in {} s",
                wait.as_secs().max(1)
            ),
        )
    })
}

/// One reconcile pass: store truth → engine registry + snapshot file.
/// `flags` are the ws-0 `--handler` registrations — they union with the
/// store's ws-0 rows (a store row shadows a flag on a name conflict).
#[expect(
    clippy::too_many_arguments,
    reason = "the pass's collaborators are App's registry fields; tests drive it with each one"
)]
async fn registry_sync(
    store: &Arc<dyn AccountStore>,
    engine: &Engine,
    factory: &HandlerFactory,
    flags: &[(String, Arc<dyn StepHandler>)],
    tx: &std::sync::mpsc::SyncSender<RegistryWrite>,
    epoch: &std::sync::atomic::AtomicU64,
    locks: &RegistryLocks,
    view: &RegistryState,
) -> Result<(), AccountError> {
    // A mutation anywhere in the pass's read window makes its applies
    // stale — bail out; the mutation applied itself and the next pass
    // converges (S3: the race was up to a whole 30 s pass).
    let e0 = epoch.load(std::sync::atomic::Ordering::Acquire);
    let stale = || epoch.load(std::sync::atomic::Ordering::Acquire) != e0;
    // The whole graph, paged: more than REGISTRY_SYNC_WS_LIMIT
    // workspaces must still reconcile — a capped first page silently
    // dropped every later tenant's handlers (S3).
    let mut workspaces = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_workspaces(REGISTRY_SYNC_WS_LIMIT, after).await?;
        match page.last() {
            Some(last) if page.len() == REGISTRY_SYNC_WS_LIMIT as usize => {
                after = Some(last.id);
            }
            _ => {
                workspaces.extend(page);
                break;
            }
        }
        workspaces.extend(page);
    }
    let mut all = Vec::new();
    let mut preserve_ws = Vec::new();
    let mut touched: Vec<u64> = Vec::new();
    let mut refused = 0u64;
    for ws in &workspaces {
        // Finding 4: the read and the apply are one critical section
        // per workspace — a mutation of this workspace cannot land
        // between them, so a pass never re-arms a row that was deleted
        // underneath it. One stripe per iteration; unrelated workspaces
        // still run in parallel.
        let _ws_guard = locks.stripe(ws.id).lock().await;
        // Per-workspace error isolation (S2 review #6): one bad row or
        // engine refusal must not abort the pass for every later
        // workspace — the engine keeps that workspace's last-armed set,
        // and the writer carries its file rows forward (`preserve_ws`)
        // so a restart still has them (S2 review N2).
        let regs = match store.list_handlers(ws.id).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("durable-server: registry reconcile skipped a workspace: {e}");
                preserve_ws.push(ws.id);
                continue;
            }
        };
        let mut set: Vec<(String, Arc<dyn StepHandler>)> = if ws.id == 0 {
            flags.to_vec()
        } else {
            Vec::new()
        };
        let secret = Some(Arc::new(ws.handler_secret));
        let mut accepted = Vec::new();
        for r in &regs {
            // Re-check the stored URL at load: a row planted by an
            // --allow-local-handlers process or a direct store edit
            // never reaches the registry (call-time policy still
            // refuses it; dropping it here refuses it *honestly*).
            // ATTACK(reconcile-recheck)
            let ok = r.url == BUILTIN_ECHO
                || reqwest::Url::parse(&r.url)
                    .map(|u| ssrf::check_url(&u, &factory.store_policy).is_ok())
                    .unwrap_or(false);
            if !ok {
                refused += 1;
                continue;
            }
            set.push((r.name.clone(), arm_url(factory, &r.url, secret.clone())));
            accepted.push(RegistryEntry {
                ws: ws.id,
                name: r.name.clone(),
                url: r.url.clone(),
                secret: Some(hex(&ws.handler_secret)),
            });
        }
        // No pass-wide stale() bail here (S3 re-review N7): the per-ws
        // stripe above already serializes this workspace's read+apply
        // against its own mutations, so one tenant's mutation must not
        // abort reconciliation for every later workspace.
        if let Err(e) = engine.set_workspace_handlers(ws.id, set).await {
            eprintln!("durable-server: registry reconcile skipped a workspace: {e}");
            // The engine keeps the last-armed set; the file keeps its
            // rows for the same reason.
            preserve_ws.push(ws.id);
            continue;
        }
        // The snapshot file and the armed view mirror what the engine
        // actually armed — a refused set (over the per-ws bound, dead
        // channel) never lands.
        view.replace_ws(
            ws.id,
            accepted.iter().map(|e| (e.name.clone(), e.url.clone())),
        );
        touched.push(ws.id);
        all.extend(accepted);
    }
    if refused > 0 {
        eprintln!(
            "durable-server: registry reconcile dropped {refused} stored \
             handler row(s) whose URL fails the SSRF policy"
        );
    }
    // S3 re-review 2: the views above go up before the file write; a pass
    // whose snapshot does not land forgets them again. Forgetting is
    // always safe — it costs the next PUT its no-op skip, nothing else.
    let forget = |why: &str| {
        // ATTACK(reconcile-forgets)
        for ws in &touched {
            view.replace_ws(*ws, std::iter::empty());
        }
        if !touched.is_empty() {
            eprintln!(
                "durable-server: registry reconcile {why}; {} workspace view(s) forgotten",
                touched.len()
            );
        }
    };
    if stale() {
        eprintln!("durable-server: registry reconcile discarded a pass overtaken by a mutation");
        forget("was overtaken");
        return Ok(());
    }
    // The writer checks the epoch again under its own serialization —
    // an overtaken pass can never land over a mutation that already won.
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    if tx
        .try_send(RegistryWrite::Snapshot {
            entries: all,
            preserve_ws,
            epoch_seen: e0,
            ack: ack_tx,
        })
        .is_err()
    {
        eprintln!("durable-server: registry writer unavailable; the file keeps last-known-good");
        forget("could not reach the writer");
        return Ok(());
    }
    // The pass is settled when the writer decides — applied or dropped
    // as stale, either way a bounded wait hears about it.
    let settled = tokio::time::timeout(std::time::Duration::from_secs(10), ack_rx).await;
    // Acked but overtaken: the writer dropped the pass as stale.
    if !matches!(settled, Ok(Ok(Ok(())))) || stale() {
        forget("did not land");
    }
    match settled {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => {
            eprintln!("durable-server: registry writer could not persist a reconcile pass: {e}");
            Err(AccountError::Unavailable(
                "registry writer could not persist the pass".into(),
            ))
        }
        _ => {
            eprintln!("durable-server: registry writer did not settle a reconcile pass");
            Err(AccountError::Unavailable(
                "registry writer did not settle the pass".into(),
            ))
        }
    }
}

/// The reconciler loop: on a store error the engine keeps serving the
/// last-known-good set and `registry_stale` reports it.
fn spawn_registry_reconcile(
    store: Arc<dyn AccountStore>,
    app: &App,
    stale: Arc<std::sync::atomic::AtomicBool>,
) {
    let engine = app.engine.clone();
    let factory = Arc::clone(&app.handler_factory);
    let flags = Arc::clone(&app.flag_handlers);
    let tx = app.registry_tx.clone();
    let epoch = Arc::clone(&app.registry_epoch);
    let locks = Arc::clone(&app.registry_locks);
    let view = Arc::clone(&app.registry);
    tokio::spawn(async move {
        loop {
            match registry_sync(
                &store, &engine, &factory, &flags, &tx, &epoch, &locks, &view,
            )
            .await
            {
                Ok(()) => stale.store(false, std::sync::atomic::Ordering::Relaxed),
                Err(e) => {
                    stale.store(true, std::sync::atomic::Ordering::Relaxed);
                    eprintln!(
                        "durable-server: registry reconcile failed ({e}); \
                         serving last-known-good handlers"
                    );
                }
            }
            tokio::time::sleep(REGISTRY_SYNC).await;
        }
    });
}

/// The API key, kept only as its SHA-256 digest. Comparing fixed-length
/// digests in constant time leaks neither the key's bytes nor its length.
#[derive(Clone)]
struct ApiKey(Arc<[u8; 32]>);

impl ApiKey {
    fn new(secret: &str) -> Self {
        Self(Arc::new(sha256(secret.as_bytes())))
    }
    fn verify(&self, presented: &str) -> bool {
        use subtle::ConstantTimeEq as _;
        sha256(presented.as_bytes()).ct_eq(&*self.0).into()
    }
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let d = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

/// Who an authenticated caller is (review #4 — one enum, never a
/// collapsed boolean): the ops plane (`DURABLE_API_KEY`/`DURABLE_READ_KEY`
/// env keys), a per-workspace store key, or a signed-in member resolving
/// through a member row. `ws` is internal only — it never serializes
/// into a response.
#[derive(Clone, Copy, Debug)]
enum Principal {
    /// `DURABLE_API_KEY` — the only principal `ops_admin` accepts.
    OpsKey,
    /// `DURABLE_READ_KEY`.
    OpsRead,
    /// A per-workspace API key verified through the store.
    StoreKey { ws: u64, permission: Permission },
    /// A signed-in session, resolved to a workspace through
    /// `x-durable-workspace` + the members table.
    #[expect(dead_code, reason = "constructed by member resolution in S3-3")]
    Member { account: u64, ws: u64, role: Role },
}

/// Back-compat alias at the auth boundary: handlers hold a Principal.
type Auth = Principal;

impl Principal {
    /// The workspace this principal acts on; ops keys answer ws 0.
    fn ws(self) -> u64 {
        match self {
            Principal::OpsKey | Principal::OpsRead => 0,
            Principal::StoreKey { ws, .. } | Principal::Member { ws, .. } => ws,
        }
    }

    /// May this principal mutate? OpsKey and write-scope store keys;
    /// members per their role (viewer is read-only).
    fn may_write(self) -> bool {
        match self {
            Principal::OpsKey => true,
            Principal::OpsRead => false,
            Principal::StoreKey { permission, .. } => permission == Permission::Write,
            Principal::Member { role, .. } => role.may_write(),
        }
    }
}

/// The outcome of checking one bearer: a verified grant, a verified
/// negative, or a store that could not answer. An unreachable store is
/// never collapsed into "denied" — the caller must fail closed.
enum Verified {
    Granted(Auth),
    Denied,
    Unavailable,
}

/// How long a verified grant (or denial) may serve without the store.
/// Store-verified keys keep working through a Neon outage within this
/// window; revocation inside this process invalidates write-through, so
/// the TTL only bounds edits made outside the server.
const GRANT_TTL: Duration = Duration::from_secs(60);
/// Verified negatives are cached much shorter so a key created during a
/// brief denial window becomes usable quickly.
const DENY_TTL: Duration = Duration::from_secs(5);
/// Bound on cached verifications.
const KEY_CACHE_CAP: usize = 10_000;

#[derive(Clone, Copy)]
struct CacheEntry {
    at: Instant,
    outcome: Cached,
}

#[derive(Clone, Copy)]
enum Cached {
    Granted(Auth),
    Denied,
}

/// The only shapes a store key can have (`new_key_secret`): `drk_` plus
/// 48 lowercase hex — 52 chars (legacy `dr_` keys, 51 chars, keep
/// verifying until their owners rotate them). Non-conforming bearers
/// deny cheaply.
fn store_key_shape(bearer: &str) -> bool {
    let hex = if bearer.len() == 52 && bearer.starts_with("drk_") {
        &bearer[4..]
    } else if bearer.len() == 51 && bearer.starts_with("dr_") {
        &bearer[3..]
    } else {
        return false;
    };
    hex.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Resolves bearers to `Auth`. Env keys always grant workspace 0 — the
/// ops path that predates S2. When an `AccountStore` is configured,
/// other bearers are checked against it through a bounded cache: cached
/// grants survive a store outage for `GRANT_TTL`, anything the cache
/// cannot answer fails closed as `accounts_unavailable`.
struct Keyring {
    api_key: ApiKey,
    read_key: Option<ApiKey>,
    store: Option<Arc<dyn AccountStore>>,
    cache: Mutex<HashMap<[u8; 32], CacheEntry>>,
    /// key_hash → last-seen unix ms, coalesced between flushes (D-071):
    /// N verifies in a tick land as ONE `last_used_at` UPDATE, so the
    /// hot verify path writes to the store zero times.
    touches: Mutex<HashMap<[u8; 32], i64>>,
}

impl Keyring {
    fn new(
        api_key: ApiKey,
        read_key: Option<ApiKey>,
        store: Option<Arc<dyn AccountStore>>,
    ) -> Self {
        Self {
            api_key,
            read_key,
            store,
            cache: Mutex::new(HashMap::new()),
            touches: Mutex::new(HashMap::new()),
        }
    }

    /// Mark a store key used — cheap, coalesced, flushed by the tick.
    fn touch_key(&self, hash: [u8; 32]) {
        let mut touches = self.touches.lock().unwrap_or_else(|e| e.into_inner());
        if touches.len() >= TOUCH_CAP {
            touches.clear();
        }
        touches.insert(hash, unix_ms());
    }

    /// Drain the pending key touches for the flush task.
    fn drain_key_touches(&self) -> Vec<durable_accounts::KeyTouch> {
        let mut touches = self.touches.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *touches)
            .into_iter()
            .map(|(key_hash, seen_ms)| durable_accounts::KeyTouch { key_hash, seen_ms })
            .collect()
    }

    async fn verify(&self, bearer: &str) -> Verified {
        if self.api_key.verify(bearer) {
            return Verified::Granted(Principal::OpsKey);
        }
        if self.read_key.as_ref().is_some_and(|k| k.verify(bearer)) {
            return Verified::Granted(Principal::OpsRead);
        }
        // Store keys have exactly one shape — `dr_` + 48 hex. Anything
        // else can never verify, so it is denied here without spending
        // a store query (S2 review #6): an arbitrary-bearer flood used
        // to price itself in Neon round-trips.
        if !store_key_shape(bearer) {
            return Verified::Denied;
        }
        let hash = sha256(bearer.as_bytes());
        if let Some(outcome) = self.cached(hash) {
            if matches!(outcome, Verified::Granted(Principal::StoreKey { .. })) {
                self.touch_key(hash);
            }
            return outcome;
        }
        let Some(store) = &self.store else {
            return Verified::Denied;
        };
        let verified = match store.verify_key(&hash).await {
            Ok(Some(grant)) => Verified::Granted(Principal::StoreKey {
                ws: grant.workspace,
                permission: grant.permission,
            }),
            Ok(None) => Verified::Denied,
            Err(_) => Verified::Unavailable,
        };
        self.remember(hash, &verified);
        if matches!(verified, Verified::Granted(Principal::StoreKey { .. })) {
            self.touch_key(hash);
        }
        verified
    }

    fn cached(&self, hash: [u8; 32]) -> Option<Verified> {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let entry = cache.get(&hash)?;
        let ttl = match entry.outcome {
            Cached::Granted(_) => GRANT_TTL,
            Cached::Denied => DENY_TTL,
        };
        if entry.at.elapsed() >= ttl {
            return None;
        }
        Some(match entry.outcome {
            Cached::Granted(auth) => Verified::Granted(auth),
            Cached::Denied => Verified::Denied,
        })
    }

    fn remember(&self, hash: [u8; 32], verified: &Verified) {
        let outcome = match verified {
            Verified::Granted(auth) => Cached::Granted(*auth),
            Verified::Denied => Cached::Denied,
            Verified::Unavailable => return, // outages are never cached
        };
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= KEY_CACHE_CAP {
            let now = Instant::now();
            cache.retain(|_, e| {
                now.duration_since(e.at)
                    < match e.outcome {
                        Cached::Granted(_) => GRANT_TTL,
                        Cached::Denied => DENY_TTL,
                    }
            });
            if cache.len() >= KEY_CACHE_CAP {
                // Denials go before grants (S2 review #6): a negative
                // costs one cheap re-check while a dropped grant costs
                // a store round-trip under outage.
                cache.retain(|_, e| matches!(e.outcome, Cached::Granted(_)));
            }
            if cache.len() >= KEY_CACHE_CAP {
                // Still full of grants: evict the oldest-decaying
                // tenth — the hot keys re-verify immediately; a flood
                // of one-shot keys cannot displace all real ones.
                let cutoff = KEY_CACHE_CAP - KEY_CACHE_CAP / 10;
                let mut by_age: Vec<([u8; 32], Instant)> =
                    cache.iter().map(|(k, e)| (*k, e.at)).collect();
                by_age.sort_unstable_by_key(|(_, at)| *at);
                for (k, _) in by_age.into_iter().take(cache.len() - cutoff) {
                    cache.remove(&k);
                }
            }
        }
        cache.insert(
            hash,
            CacheEntry {
                at: Instant::now(),
                outcome,
            },
        );
    }

    /// Write-through invalidation: key revocation performed through this
    /// process takes effect immediately instead of waiting out the TTL.
    fn invalidate(&self, bearer_hash: [u8; 32]) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&bearer_hash);
    }

    /// Write-through on workspace delete (D-072): every cached grant of
    /// the workspace dies now — its keys are revoked in the same commit.
    fn invalidate_ws(&self, ws: u64) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, e| !matches!(e.outcome, Cached::Granted(p) if p.ws() == ws));
    }
}

#[derive(Clone)]
struct App {
    engine: Engine,
    keyring: Arc<Keyring>,
    /// Reads need no key (`--open-reads`, loopback listeners only).
    open_reads: bool,
    /// Canonical backup directory; `None` disables `/admin/backup`.
    backup_root: Option<Arc<Path>>,
    /// Builds registered handlers through the bounded client (S2-4).
    handler_factory: Arc<HandlerFactory>,
    /// ws-0 `--handler` registrations — unioned into ws 0 on every
    /// reconcile so a store row shadows but never silently deletes them.
    flag_handlers: Arc<Vec<(String, Arc<dyn StepHandler>)>>,
    /// The engine is serving the snapshot because the store could not
    /// answer the last reconcile (`registry_stale` in diagnostics).
    registry_stale: Arc<std::sync::atomic::AtomicBool>,
    /// `<journal>/handlers.json`: the last-known-good registrations —
    /// written only by the registry writer thread, fed by this channel
    /// (the load/merge/fsync work stays off the async workers — S3).
    registry_file: Arc<Path>,
    registry_tx: std::sync::mpsc::SyncSender<RegistryWrite>,
    /// Bumped by every route mutation (PUT/DELETE/rotate) BEFORE the
    /// store write, so a reconcile pass that overlaps the mutation
    /// always sees the epoch move. The writer re-checks it under its
    /// own serialization before a snapshot may land (S3).
    registry_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Per-workspace stripes serializing route mutations against the
    /// reconciler's read+apply of the same workspace (finding 4).
    registry_locks: Arc<RegistryLocks>,
    /// The armed view, the per-workspace mutation limit, the save count.
    registry: Arc<RegistryState>,
    /// Opaque public run ids (S2 #7): every run id crosses the API
    /// through this keyed permutation, so no response reveals the
    /// global sequence or another workspace's volume.
    pubid: pubid::PubId,
    /// Domain-separated HMAC key for session-token tags (review #8).
    session_key: std::sync::Arc<ring::hmac::Key>,
    /// The generation-checked session cache (review #10).
    sessions: std::sync::Arc<SessionCache>,
    /// The generation-checked member-role cache (D-066).
    members: std::sync::Arc<MemberCache>,
    /// D-070 probe budgets: 30/hour per workspace (the Free plan's
    /// bound; S3-4 makes it plan-read) + 10/minute per client address.
    test_calls_ws: std::sync::Arc<Mutex<HashMap<u64, std::collections::VecDeque<Instant>>>>,
    /// `--ops-listen` is set: env-key grants hold only on that socket.
    ops_gate: bool,
    test_calls_addr: std::sync::Arc<Mutex<SlidingWindow>>,
    /// The bounded argon2 queue (review #9).
    argon: std::sync::Arc<ArgonQueue>,
    /// Per-address limits for the open auth surface.
    auth_limits: std::sync::Arc<AuthLimits>,
    /// Login-failure cool-offs per (email, client prefix) — in-memory,
    /// bounded, keyed whether or not the account exists (review #13).
    lockouts: std::sync::Arc<LoginLockouts>,
    /// Transactional mail; MemoryMailer in tests, Log/Resend at deploy.
    mailer: std::sync::Arc<dyn Mailer>,
    /// Global outbound guard: daily budget + circuit breaker (#14).
    mail_budget: std::sync::Arc<MailBudget>,
    /// `DURABLE_INVITE_ONLY` — env-at-boot abuse switch (ruling 9).
    invite_only: bool,
    /// `--public-origin` — the base every mailed link is built from
    /// (never the request Host). Required at boot with an account
    /// store (review #18); a test harness may pass None.
    public_origin: Option<String>,
    /// Session-clock override for tests (the idle-timeout proof is
    /// clock-injected): 0 = wall clock. Only the session-expiry and
    /// session-mint paths consult it; lockouts keep wall time.
    now_override_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,
    /// The engine's observer (D-058, scoped per workspace by D-060).
    obs: Arc<Observer>,
}

impl App {
    /// The session clock — the override when a test set one, else wall.
    fn now_ms(&self) -> i64 {
        let v = self
            .now_override_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if v == 0 {
            unix_ms()
        } else {
            v
        }
    }
}

/// Arm the last-known-good snapshot rows at startup (before the first
/// reconcile). Every row is re-checked against the store policy — the file
/// may have been written by another build or edited offline, so a local
/// target never enters the registry — and a tenant row with no usable
/// secret is refused rather than armed unsigned. Returns how many armed.
async fn arm_snapshot(
    entries: Vec<RegistryEntry>,
    factory: &HandlerFactory,
    engine: &Engine,
) -> Vec<(u64, String, String)> {
    let mut armed = Vec::new();
    for entry in entries {
        // ATTACK(startup-recheck)
        let ok = reqwest::Url::parse(&entry.url)
            .map(|u| ssrf::check_url(&u, &factory.store_policy).is_ok())
            .unwrap_or(false);
        if !ok {
            eprintln!(
                "durable-server: snapshot handler {:?} ({:?}) refused by the \
                 SSRF policy at startup",
                entry.name, entry.url
            );
            continue;
        }
        let secret = entry.secret.as_deref().and_then(unhex).map(Arc::new);
        // A tenant handler with no usable secret must not arm
        // *unsigned* — that is the S3 hole (a corrupt entry would
        // silently downgrade to trust-anyone). ws 0 stays honest:
        // its flag handlers are unsigned by design.
        if entry.ws != 0 && secret.is_none() {
            eprintln!(
                "durable-server: snapshot handler {:?} (ws {}) has no usable \
                 secret; refusing it rather than arming unsigned",
                entry.name, entry.ws
            );
            continue;
        }
        let h = arm_url(factory, &entry.url, secret);
        match engine.put_handler(entry.ws, &entry.name, h).await {
            Ok(()) => armed.push((entry.ws, entry.name, entry.url)),
            Err(e) => eprintln!(
                "durable-server: snapshot handler {:?} refused at startup: {e}",
                entry.name
            ),
        }
    }
    armed
}

/// `--allow-local-tenant-handlers`, which exists only in a lab build.
fn lab_local_tenant_handlers(args: &Args) -> bool {
    #[cfg(feature = "lab")]
    {
        args.allow_local_tenant_handlers
    }
    #[cfg(not(feature = "lab"))]
    {
        let _ = args;
        false
    }
}

/// Tests' SSRF policy: open for fixtures, else the IANA blocks only (a
/// test that needs this host's addresses builds `Policy::boot` itself).
#[cfg(test)]
fn test_policy(allow_local: bool) -> ssrf::Policy {
    if allow_local {
        ssrf::Policy::open()
    } else {
        ssrf::Policy::strict()
    }
}

/// Test constructor: the registry plumbing (`handlers.json`, factory,
/// flags) points inside the test's journal dir like the real wiring.
#[cfg(test)]
fn test_app(
    engine: Engine,
    keyring: Arc<Keyring>,
    open_reads: bool,
    backup_root: Option<Arc<Path>>,
    journal_dir: &Path,
    allow_local: bool,
    ops_gate: bool,
) -> App {
    let pubid = pubid::PubId::load_or_generate(journal_dir).unwrap_or_else(|e| panic!("{e}"));
    let registry_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let registry = Arc::new(RegistryState::default());
    let registry_tx = spawn_registry_writer(
        Arc::from(journal_dir.join("handlers.json").as_path()),
        Arc::clone(&registry_epoch),
        Arc::clone(&registry.saves),
    );
    let obs = engine.observer().clone();
    App {
        engine,
        keyring,
        ops_gate,
        open_reads,
        backup_root,
        handler_factory: Arc::new(HandlerFactory {
            client: handler_client(&test_policy(allow_local)).unwrap_or_else(|e| panic!("{e}")),
            flag_client: handler_client(&test_policy(allow_local))
                .unwrap_or_else(|e| panic!("{e}")),
            timeout: Duration::from_secs(2),
            max_output: 1024 * 1024,
            store_policy: test_policy(allow_local),
            flag_policy: test_policy(allow_local),
            pubid: pubid.clone(),
        }),
        flag_handlers: Arc::new(Vec::new()),
        registry_stale: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        registry_file: Arc::from(journal_dir.join("handlers.json").as_path()),
        registry_tx,
        registry_epoch,
        registry_locks: Arc::new(RegistryLocks::default()),
        registry,
        session_key: std::sync::Arc::new(pubid.derive_key(b"session-token-v1")),
        sessions: std::sync::Arc::new(SessionCache::new()),
        members: std::sync::Arc::new(MemberCache::new()),
        test_calls_ws: std::sync::Arc::new(Mutex::new(HashMap::new())),
        test_calls_addr: std::sync::Arc::new(Mutex::new(SlidingWindow::new(
            10,
            Duration::from_secs(60),
        ))),
        argon: std::sync::Arc::new(ArgonQueue::new()),
        auth_limits: std::sync::Arc::new(AuthLimits::new()),
        lockouts: std::sync::Arc::new(LoginLockouts::new()),
        mailer: std::sync::Arc::new(durable_accounts::MemoryMailer::new()),
        mail_budget: std::sync::Arc::new(MailBudget::new(MAIL_DAILY_BUDGET)),
        invite_only: false,
        public_origin: None,
        now_override_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        pubid,
        obs,
    }
}

/// The bearer token a request carries: `<scheme> <token>`, the scheme
/// case-insensitive (RFC 9110 §11.1), surrounding whitespace ignored.
fn bearer(parts: &Parts) -> Option<&str> {
    parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().split_once(char::is_whitespace))
        .and_then(|(scheme, token)| scheme.eq_ignore_ascii_case("bearer").then(|| token.trim()))
}

fn unauthorized() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "missing or invalid API key (Authorization: Bearer <key>)",
    )
}

fn accounts_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "accounts_unavailable",
        "the account store cannot answer right now; retry shortly — \
         cached keys keep working, unknown bearers fail closed",
    )
}

/// Proof that the request carried an admin-capable key: which workspace
/// it belongs to travels in `auth.ws()`. Every mutating and admin handler
/// takes this as its first extractor, so it runs before the body is
/// parsed. A read-permission key is recognised and refused 403: it is
/// valid, but not here.
struct Authorized(Auth);

// ---- S3-0: accounts, sessions, the auth surface ---------------------

const SESSION_COOKIE: &str = "__Host-drs";
const SESSION_PREFIX: &str = "drs_";
const SESSION_IDLE_MS: i64 = 14 * 24 * 3600 * 1000;
const SESSION_ABS_MS: i64 = 30 * 24 * 3600 * 1000;
const SESSION_CACHE_CAP: usize = 10_000;
const SESSION_GRANT_TTL: Duration = Duration::from_secs(60);
const BUMP_TTL: Duration = Duration::from_secs(120);
const BUMP_CAP: usize = 4096;
const ARGON_PERMITS: usize = 4;
const ARGON_WAITERS: usize = 32;
const ARGON_WAIT: Duration = Duration::from_secs(2);

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Mint a session token: `drs_` + hex(16 B random ∥ 16 B tag), the tag
/// `HMAC(session_key, random)[..16]` — self-authenticating (review #8):
/// a forged value dies on the tag before any store I/O. The store keeps
/// sha256 of the whole string, as with API keys.
fn mint_session_token(key: &ring::hmac::Key) -> String {
    let mut raw = [0u8; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut raw)
        .unwrap_or_else(|_| panic!("rng"));
    let tag = ring::hmac::sign(key, &raw);
    let mut full = [0u8; 32];
    full[..16].copy_from_slice(&raw);
    full[16..].copy_from_slice(&tag.as_ref()[..16]);
    format!("{SESSION_PREFIX}{}", hex(&full))
}

/// Shape + tag check on a presented token; `Some(sha256)` only for a
/// value this process could have minted — the store lookup key.
fn session_token_hash(key: &ring::hmac::Key, token: &str) -> Option<[u8; 32]> {
    let body = token.strip_prefix(SESSION_PREFIX)?;
    let raw = unhex(body)?;
    let (rand, tag) = raw.split_at(16);
    // ring's `verify` demands the full-length tag; ours is a 16-byte
    // truncation, so compare the computed prefix constant-time.
    let computed = ring::hmac::sign(key, rand);
    // ATTACK(session-forge)
    if !bool::from(subtle::ConstantTimeEq::ct_eq(&computed.as_ref()[..16], tag)) {
        return None;
    }
    Some(sha256(token.as_bytes()))
}

/// The synchroniser CSRF value: `drc_` + hex of the seed's HMAC —
/// never stored, recomputed per request.
fn csrf_token(seed: &[u8; 32]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, seed);
    format!("drc_{}", hex(ring::hmac::sign(&key, b"csrf").as_ref()))
}

fn csrf_ok(seed: &[u8; 32], presented: &str) -> bool {
    // ATTACK(csrf)
    let expect = csrf_token(seed);
    presented.len() == expect.len()
        && bool::from(subtle::ConstantTimeEq::ct_eq(
            presented.as_bytes(),
            expect.as_bytes(),
        ))
}

fn session_cookie(parts: &Parts) -> Option<String> {
    let raw = parts.headers.get(header::COOKIE)?.to_str().ok()?;
    for pair in raw.split(';') {
        let pair = pair.trim();
        if let Some(v) = pair.strip_prefix(SESSION_COOKIE) {
            return v.strip_prefix('=').map(|t| t.to_string());
        }
    }
    None
}

fn session_set_cookie(token: &str) -> String {
    format!("{SESSION_COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Lax")
}

fn session_clear_cookie() -> &'static str {
    "__Host-drs=; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=0"
}

// ---- S3-1: email tokens, known-device, lockout, outbox drain --------

const EMAIL_TOKEN_PREFIX: &str = "drt_";
const VERIFY_TOKEN_TTL_MS: i64 = 24 * 3600 * 1000;
const RESET_TOKEN_TTL_MS: i64 = 3600 * 1000;
const KNOWN_DEVICE_COOKIE: &str = "__Host-drd";
const KNOWN_DEVICE_MAX_AGE_S: u64 = 400 * 24 * 3600;
/// The login cool-off (review #13): failures per (normalized email,
/// client /24-or-/64) — keyed whether or not the account exists, so a
/// 429 never answers "is this address registered".
const LOCKOUT_FAILS: u32 = 10;
const LOCKOUT_WINDOW_MS: i64 = 10 * 60 * 1000;
const LOCKOUT_COOLOFF_MS: i64 = 15 * 60 * 1000;
const LOCKOUT_CAP: usize = 10_000;
/// Outbox drain (D-068): bounded batch, per-send deadline, claim
/// deadline that makes a worker crash reclaimable.
const OUTBOX_BATCH: u32 = 25;
const OUTBOX_SEND_DEADLINE: Duration = Duration::from_secs(10);
const OUTBOX_CLAIM_MS: i64 = 30_000;
const OUTBOX_TICK: Duration = Duration::from_secs(5);
/// Consecutive provider failures that open the circuit breaker.
const MAIL_BREAKER_FAILS: u32 = 8;
const MAIL_BREAKER_MS: i64 = 5 * 60 * 1000;
/// Default global send budget per UTC day (`DURABLE_MAIL_DAILY_BUDGET`).
const MAIL_DAILY_BUDGET: u32 = 500;

/// Mint an email token (`verify`/`reset` share the shape — the kind
/// column is the guard against confusion, proven by its attack).
fn mint_email_token() -> String {
    let mut b = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b)
        .unwrap_or_else(|_| panic!("rng"));
    format!(
        "{EMAIL_TOKEN_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    )
}

/// Shape-check + lookup hash for a presented `drt_` value. Malformed
/// tokens answer `None` → the route's single 410 (no dead-kind oracle).
fn email_token_hash(token: &str) -> Option<[u8; 32]> {
    let body = token.strip_prefix(EMAIL_TOKEN_PREFIX)?;
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(body)
        .ok()?;
    (raw.len() == 32).then(|| sha256(token.as_bytes()))
}

/// Shape-check + lookup hash for a presented `dri_` invite token — the
/// sign-up gate parses the INVITE kind, not the `drt_` mail kind
/// (first-deploy bug: a live invite never passed the gate).
fn invite_token_hash(token: &str) -> Option<[u8; 32]> {
    let body = token.strip_prefix(INVITE_PREFIX)?;
    let raw = unhex(body)?;
    (raw.len() == 32).then(|| sha256(token.as_bytes()))
}

/// The lockout's client key: the /24 (v4) or /64 (v6) holding the
/// socket address — a single host can't dodge its own counter.
fn client_prefix(addr: std::net::IpAddr) -> std::net::IpAddr {
    match addr {
        std::net::IpAddr::V4(a) => {
            let mut o = a.octets();
            o[3] = 0;
            std::net::IpAddr::V4(o.into())
        }
        std::net::IpAddr::V6(a) => {
            let mut o = a.octets();
            o[8..].fill(0);
            std::net::IpAddr::V6(o.into())
        }
    }
}

/// A signed device-recognition cookie: `drd_` + hex(16 B public_id ∥
/// 16 B tag), the tag `HMAC(session_key, "drd/" ∥ public_id) — same
/// self-authenticating shape as the session token; a forgery dies on
/// the tag. It only ever buys a lockout bypass — it is never an
/// authentication factor.
fn mint_known_device(key: &ring::hmac::Key, public_id: &uuid::Uuid) -> String {
    let id = public_id.as_bytes();
    let mut msg = Vec::with_capacity(4 + 16);
    msg.extend_from_slice(b"drd/");
    msg.extend_from_slice(id);
    let tag = ring::hmac::sign(key, &msg);
    let mut body = [0u8; 32];
    body[..16].copy_from_slice(id);
    body[16..].copy_from_slice(&tag.as_ref()[..16]);
    format!("drd_{}", hex(&body))
}

fn known_device_ok(key: &ring::hmac::Key, parts: &Parts, public_id: &uuid::Uuid) -> bool {
    let Some(raw) = parts
        .headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| {
            raw.split(';').find_map(|p| {
                p.trim()
                    .strip_prefix(KNOWN_DEVICE_COOKIE)
                    .and_then(|v| v.strip_prefix('='))
                    .map(|s| s.to_string())
            })
        })
    else {
        return false;
    };
    let Some(body) = raw.strip_prefix("drd_").and_then(unhex) else {
        return false;
    };
    let (id, tag) = body.split_at(16);
    let mut msg = Vec::with_capacity(4 + 16);
    msg.extend_from_slice(b"drd/");
    msg.extend_from_slice(id);
    let computed = ring::hmac::sign(key, &msg);
    id == public_id.as_bytes()
        && bool::from(subtle::ConstantTimeEq::ct_eq(&computed.as_ref()[..16], tag))
}

fn known_device_set_cookie(value: &str) -> String {
    format!(
        "{KNOWN_DEVICE_COOKIE}={value}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age={KNOWN_DEVICE_MAX_AGE_S}"
    )
}

/// Bounded login-failure counters (review #13): 10 failures inside 10
/// minutes → a 15-minute cool-off for that (email, client-prefix)
/// pair. Counters live per normalized email whether or not an account
/// exists — a 429 must not reveal registration.
struct LoginLockouts {
    map: Mutex<HashMap<(String, std::net::IpAddr), LockEntry>>,
}

struct LockEntry {
    fails: u32,
    window_start_ms: i64,
    cooloff_until_ms: i64,
}

impl LoginLockouts {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// `Some(retry_after_s)` while the pair is in cool-off.
    fn check(&self, email: &str, prefix: std::net::IpAddr, now_ms: i64) -> Option<u64> {
        let map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let until = map.get(&(email.to_string(), prefix))?.cooloff_until_ms;
        (until > now_ms).then(|| ((until - now_ms) / 1000).max(1) as u64)
    }

    /// Record one failure; `true` on the trip into cool-off (the
    /// transition — the notice mail's one-per-day edge rides it).
    fn fail(&self, email: &str, prefix: std::net::IpAddr, now_ms: i64) -> bool {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= LOCKOUT_CAP {
            map.retain(|_, e| e.cooloff_until_ms > now_ms || e.fails > 0);
            if map.len() >= LOCKOUT_CAP {
                if let Some(oldest) = map
                    .iter()
                    .min_by_key(|(_, e)| e.window_start_ms)
                    .map(|(k, _)| k.clone())
                {
                    map.remove(&oldest);
                }
            }
        }
        let e = map.entry((email.to_string(), prefix)).or_insert(LockEntry {
            fails: 0,
            window_start_ms: now_ms,
            cooloff_until_ms: 0,
        });
        if e.window_start_ms + LOCKOUT_WINDOW_MS <= now_ms {
            e.fails = 0;
            e.window_start_ms = now_ms;
        }
        if e.cooloff_until_ms > now_ms {
            return false;
        }
        e.fails += 1;
        if e.fails >= LOCKOUT_FAILS {
            e.cooloff_until_ms = now_ms + LOCKOUT_COOLOFF_MS;
            return true;
        }
        false
    }

    fn clear(&self, email: &str, prefix: std::net::IpAddr) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(email.to_string(), prefix));
    }

    /// A successful reset clears every counter for the address — the
    /// mailbox owner proved themselves.
    fn clear_email(&self, email: &str) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(e, _), _| e != email);
    }
}

/// The global outbound guard (review #14): a per-day send budget plus
/// a circuit breaker on consecutive provider failures. In-process —
/// the budget is a tripwire, not an accounting record.
struct MailBudget {
    inner: Mutex<BudgetInner>,
}

struct BudgetInner {
    day: i64,
    sent: u32,
    cap: u32,
    fails: u32,
    open_until_ms: i64,
}

impl MailBudget {
    fn new(cap: u32) -> Self {
        Self {
            inner: Mutex::new(BudgetInner {
                day: 0,
                sent: 0,
                cap,
                fails: 0,
                open_until_ms: 0,
            }),
        }
    }

    /// Sends may proceed: budget left today and the breaker is closed.
    fn open(&self, now_ms: i64) -> bool {
        let mut b = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let day = now_ms / 86_400_000;
        if b.day != day {
            b.day = day;
            b.sent = 0;
        }
        b.sent < b.cap && b.open_until_ms <= now_ms
    }

    fn sent(&self, now_ms: i64) {
        let mut b = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let day = now_ms / 86_400_000;
        if b.day != day {
            b.day = day;
            b.sent = 0;
        }
        b.sent += 1;
        b.fails = 0;
    }

    fn failed(&self, now_ms: i64) {
        let mut b = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        b.fails += 1;
        if b.fails >= MAIL_BREAKER_FAILS {
            b.open_until_ms = now_ms + MAIL_BREAKER_MS;
            b.fails = 0;
        }
    }
}

/// Drain one bounded batch (D-068): sweep dead rows, claim due rows,
/// send each under its deadline, mark the outcome. `failed` and
/// `dropped` are terminal states the sweep owns — a mail is never
/// lost, only dead-lettered or intentionally dropped.
async fn drain_outbox_once(app: &App) -> usize {
    let Some(store) = app.keyring.store.clone() else {
        return 0;
    };
    let now = unix_ms();
    let _ = store.outbox_sweep(now).await;
    let _ = store.outbox_purge(now).await;
    let claimed = match store.outbox_claim(now, OUTBOX_BATCH, OUTBOX_CLAIM_MS).await {
        Ok(c) => c,
        Err(_) => return 0,
    };
    let mut sent = 0;
    for row in claimed {
        // ATTACK(mail-global)
        if !app.mail_budget.open(unix_ms()) {
            break; // claimed rows stay 'sending' — reclaimable later
        }
        let res = tokio::time::timeout(OUTBOX_SEND_DEADLINE, app.mailer.send(&row.mail)).await;
        let outcome = match res {
            Ok(Ok(())) => {
                app.mail_budget.sent(unix_ms());
                sent += 1;
                durable_accounts::MailOutcome::Sent
            }
            Ok(Err(durable_accounts::MailError::Refused(e))) => {
                app.mail_budget.failed(unix_ms());
                durable_accounts::MailOutcome::Failed { error: e }
            }
            res => {
                app.mail_budget.failed(unix_ms());
                let error = match res {
                    Ok(Err(durable_accounts::MailError::Send(e))) => e,
                    Ok(Err(durable_accounts::MailError::Refused(_))) => unreachable!(),
                    Ok(Ok(())) => unreachable!(),
                    Err(_) => "send deadline".to_string(),
                };
                // Backoff: 30 s doubling to a 1 h cap, plus jitter.
                let shift = row.attempts.clamp(0, 6) as u32;
                let base = 30_000i64.saturating_mul(1i64 << shift).min(3_600_000);
                let mut j = [0u8; 2];
                ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut j)
                    .unwrap_or_else(|_| panic!("rng"));
                durable_accounts::MailOutcome::Retry {
                    error,
                    next_attempt_at_ms: unix_ms()
                        + base
                        + i64::from(u16::from_be_bytes(j) % 15_000),
                }
            }
        };
        let _ = store.outbox_mark(row.id, outcome).await;
    }
    sent
}

/// The background drain worker — store mode only; tests drive
/// `drain_outbox_once` directly.
fn spawn_mail_drain(app: App) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(OUTBOX_TICK);
        loop {
            tick.tick().await;
            drain_outbox_once(&app).await;
        }
    });
}

/// Idle-expiry sliding (D-071): requests record touches in memory; one
/// batched monotonic store update per tick keeps `idle_expires_at`
/// moving without a write per request. A missed flush only slides
/// less — never serves a dead session.
const SESSION_TOUCH_TICK: std::time::Duration = std::time::Duration::from_secs(30);

async fn flush_session_touches(app: &App) {
    let Some(store) = app.keyring.store.clone() else {
        return;
    };
    let touches = app.sessions.drain_touches();
    if !touches.is_empty() {
        let _ = store.touch_sessions(&touches).await;
    }
    let key_touches = app.keyring.drain_key_touches();
    if !key_touches.is_empty() {
        let _ = store.touch_keys(&key_touches).await;
    }
}

fn spawn_session_flush(app: App) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SESSION_TOUCH_TICK);
        loop {
            tick.tick().await;
            flush_session_touches(&app).await;
        }
    });
}

/// Generation-checked session cache (review #10): a fill that spans an
/// invalidation is dropped; a hit older than its key's bump is a miss.
/// Bumps come in two keys — a session's token hash (logout, revoke-one)
/// and its account (revoke-all, verify, reset, password change) — so an
/// account-wide revocation dies in cache too, not just in the store.
/// The bump maps are bounded; overflow clears the grants — a safe
/// global invalidation. `touches` is the pending idle slide (D-071).
struct SessionCache {
    grants: Mutex<HashMap<[u8; 32], SessionEntry>>,
    bumps: Mutex<HashMap<[u8; 32], Instant>>,
    account_bumps: Mutex<HashMap<u64, Instant>>,
    /// token_hash → last-seen unix ms, flushed to the store on the
    /// D-071 interval; bounded — overflow clears (a lost touch slides
    /// less, never serves a dead session).
    touches: Mutex<HashMap<[u8; 32], i64>>,
}

struct SessionEntry {
    at: Instant,
    grant: std::sync::Arc<durable_accounts::SessionGrant>,
}

const TOUCH_CAP: usize = 50_000;

impl SessionCache {
    fn new() -> Self {
        Self {
            grants: Mutex::new(HashMap::new()),
            bumps: Mutex::new(HashMap::new()),
            account_bumps: Mutex::new(HashMap::new()),
            touches: Mutex::new(HashMap::new()),
        }
    }

    /// The slide floor a pending touch gives this token's idle
    /// deadline — the store row lags by up to the flush interval, so
    /// expiry checks take max(stored, pending) rather than under-live.
    fn touch_idle(&self, hash: &[u8; 32]) -> i64 {
        self.touches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(hash)
            .map(|seen| seen + SESSION_IDLE_MS)
            .unwrap_or(0)
    }

    /// Record use of a resolved session for the next flush.
    fn touch(&self, hash: [u8; 32], now_ms: i64) {
        let mut touches = self.touches.lock().unwrap_or_else(|e| e.into_inner());
        if touches.len() >= TOUCH_CAP {
            touches.clear();
        }
        touches.insert(hash, now_ms);
    }

    /// Drain pending touches for the flush task.
    fn drain_touches(&self) -> Vec<durable_accounts::SessionTouch> {
        let mut touches = self.touches.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *touches)
            .into_iter()
            .map(|(token_hash, seen_ms)| durable_accounts::SessionTouch {
                token_hash,
                seen_ms,
                idle_expires_at_ms: seen_ms + SESSION_IDLE_MS,
            })
            .collect()
    }

    fn account_bumped(&self, account: u64, since: Instant) -> bool {
        self.account_bumps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&account)
            .is_some_and(|b| *b > since)
    }

    fn get(
        &self,
        hash: &[u8; 32],
        now_ms: i64,
    ) -> Option<std::sync::Arc<durable_accounts::SessionGrant>> {
        let entry = {
            let grants = self.grants.lock().unwrap_or_else(|e| e.into_inner());
            grants.get(hash).map(|e| SessionEntry {
                at: e.at,
                grant: std::sync::Arc::clone(&e.grant),
            })
        }?;
        if entry.at.elapsed() >= SESSION_GRANT_TTL {
            return None;
        }
        if self
            .bumps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(hash)
            .is_some_and(|b| *b > entry.at)
            || self.account_bumped(entry.grant.account.id, entry.at)
        {
            return None;
        }
        let idle = entry.grant.idle_expires_at_ms.max(self.touch_idle(hash));
        if entry.grant.expires_at_ms <= now_ms || idle <= now_ms {
            return None;
        }
        Some(entry.grant)
    }

    /// `fill_start` is the instant BEFORE the store await; a bump inside
    /// the window means the fetched grant post-dates nothing — the fill
    /// is refused and the caller denies.
    fn put(
        &self,
        hash: [u8; 32],
        grant: std::sync::Arc<durable_accounts::SessionGrant>,
        fill_start: Instant,
    ) -> bool {
        if self
            .bumps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&hash)
            .is_some_and(|b| *b > fill_start)
            || self.account_bumped(grant.account.id, fill_start)
        {
            return false;
        }
        let mut grants = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        if grants.len() >= SESSION_CACHE_CAP {
            grants.retain(|_, e| e.at.elapsed() < SESSION_GRANT_TTL);
            if grants.len() >= SESSION_CACHE_CAP {
                grants.clear();
            }
        }
        grants.insert(
            hash,
            SessionEntry {
                at: Instant::now(),
                grant,
            },
        );
        true
    }

    /// Write-through invalidation: revoke/logout lands at once.
    fn bump(&self, hash: [u8; 32]) {
        let mut bumps = self.bumps.lock().unwrap_or_else(|e| e.into_inner());
        if bumps.len() >= BUMP_CAP {
            bumps.retain(|_, at| at.elapsed() < BUMP_TTL);
            if bumps.len() >= BUMP_CAP {
                self.grants
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                bumps.clear();
            }
        }
        bumps.insert(hash, Instant::now());
    }

    /// Account-wide invalidation (review #10's account key): verify,
    /// reset, password change, revoke-all and unverified re-sign-up all
    /// kill sessions in the store — this makes the cache agree before
    /// the next request rather than at TTL.
    fn bump_account(&self, account: u64) {
        let mut bumps = self.account_bumps.lock().unwrap_or_else(|e| e.into_inner());
        if bumps.len() >= BUMP_CAP {
            bumps.retain(|_, at| at.elapsed() < BUMP_TTL);
            if bumps.len() >= BUMP_CAP {
                self.grants
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                bumps.clear();
            }
        }
        bumps.insert(account, Instant::now());
    }
}

// ---- S3-3: the member-role cache (D-066) ----------------------------

const MEMBER_GRANT_TTL: Duration = Duration::from_secs(60);
const MEMBER_CACHE_CAP: usize = 10_000;

struct MemberEntry {
    at: Instant,
    role: Role,
}

/// Roles resolve exactly like key grants: cached a bounded time,
/// generation-checked across the store await, invalidated write-through.
/// The invalidation key is the workspace — any member change in it
/// (role set, remove, accept, workspace delete) clears every cached
/// role in that workspace at once; membership churn is rare, the blast
/// radius small.
struct MemberCache {
    grants: Mutex<HashMap<(u64, u64), MemberEntry>>,
    ws_bumps: Mutex<HashMap<u64, Instant>>,
}

impl MemberCache {
    fn new() -> Self {
        Self {
            grants: Mutex::new(HashMap::new()),
            ws_bumps: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, account: u64, ws: u64) -> Option<Role> {
        let grants = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        let entry = grants.get(&(account, ws))?;
        (entry.at.elapsed() < MEMBER_GRANT_TTL).then_some(entry.role)
    }

    /// Fill after the store await — refused if a bump landed during it
    /// (review #10): deny rather than serve a role already changed.
    fn put(&self, account: u64, ws: u64, role: Role, fill_start: Instant) -> bool {
        if self
            .ws_bumps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ws)
            .is_some_and(|b| *b > fill_start)
        {
            return false;
        }
        let mut grants = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        if grants.len() >= MEMBER_CACHE_CAP {
            grants.retain(|_, e| e.at.elapsed() < MEMBER_GRANT_TTL);
            if grants.len() >= MEMBER_CACHE_CAP {
                grants.clear();
            }
        }
        grants.insert(
            (account, ws),
            MemberEntry {
                at: Instant::now(),
                role,
            },
        );
        true
    }

    /// Write-through: every cached role in `ws` dies now, and a fill
    /// that started before this moment dies on `put`.
    fn bump_ws(&self, ws: u64) {
        self.grants
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(_, w), _| *w != ws);
        let mut bumps = self.ws_bumps.lock().unwrap_or_else(|e| e.into_inner());
        if bumps.len() >= BUMP_CAP {
            bumps.retain(|_, at| at.elapsed() < BUMP_TTL);
            if bumps.len() >= BUMP_CAP {
                bumps.clear();
            }
        }
        bumps.insert(ws, Instant::now());
    }
}

/// The bounded argon2 queue (review #9): the job permit is acquired on
/// the async side before `spawn_blocking` — blocking-pool threads never
/// park on auth work. Waiters are bounded and timed out; one job per
/// client address at a time.
struct ArgonQueue {
    /// The public pool — `ARGON_PERMITS - 1`, so one permit is always
    /// left for the reserve (review #13/S3-2).
    jobs: tokio::sync::Semaphore,
    /// The single reserved permit: only requests carrying a valid
    /// known-device cookie may take it, so a credential-stuffing flood
    /// that saturates the public pool can never starve the owner out.
    reserve: tokio::sync::Semaphore,
    tickets: tokio::sync::Semaphore,
    addrs: Mutex<HashSet<std::net::IpAddr>>,
    /// Test-visible count of hashes run — the dummy path must cost the
    /// same work as a real verify.
    ran: std::sync::atomic::AtomicU64,
}

impl ArgonQueue {
    fn new() -> Self {
        Self {
            jobs: tokio::sync::Semaphore::new(ARGON_PERMITS - 1),
            reserve: tokio::sync::Semaphore::new(1),
            tickets: tokio::sync::Semaphore::new(ARGON_WAITERS),
            addrs: Mutex::new(HashSet::new()),
            ran: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn busy() -> ApiError {
        let mut e = ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "authentication is saturated; retry in a moment",
        );
        e.retry_after = Some(1);
        e
    }

    /// `reserved_ok` marks a request the reserve may serve (a valid
    /// known-device cookie): it competes for the public pool first and
    /// falls back to the one reserved permit — total concurrency still
    /// caps at `ARGON_PERMITS`.
    async fn run<T: Send + 'static>(
        &self,
        addr: std::net::IpAddr,
        reserved_ok: bool,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, ApiError> {
        {
            let mut addrs = self.addrs.lock().unwrap_or_else(|e| e.into_inner());
            if !addrs.insert(addr) {
                return Err(Self::busy());
            }
        }
        struct AddrGuard<'a>(&'a ArgonQueue, std::net::IpAddr);
        impl Drop for AddrGuard<'_> {
            fn drop(&mut self) {
                self.0
                    .addrs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&self.1);
            }
        }
        let _addr = AddrGuard(self, addr);
        let _ticket = self.tickets.try_acquire().map_err(|_| Self::busy())?;
        let _permit = match tokio::time::timeout(ARGON_WAIT, self.jobs.acquire()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) if !reserved_ok => return Err(Self::busy()),
            _ => tokio::time::timeout(ARGON_WAIT, self.reserve.acquire())
                .await
                .map_err(|_| Self::busy())?
                .map_err(|_| Self::busy())?,
        };
        self.ran.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::task::spawn_blocking(work)
            .await
            .map_err(|_| Self::busy())
    }
}

/// A bounded sliding-window counter — `cap` hits per `span` per key.
/// `RateLimit` couples burst to the per-second rate, which makes
/// "5/hour" mean "~1 request": low-frequency auth limits want real
/// windows instead.
struct SlidingWindow {
    cap: usize,
    span: Duration,
    hits: HashMap<std::net::IpAddr, std::collections::VecDeque<Instant>>,
}

impl SlidingWindow {
    fn new(cap: usize, span: Duration) -> Self {
        Self {
            cap,
            span,
            hits: HashMap::new(),
        }
    }

    /// Ok: counted. Err: refused — how long until the oldest hit ages
    /// out of the window (a Retry-After the client can honour).
    fn check(&mut self, addr: std::net::IpAddr, now: Instant) -> Result<(), Duration> {
        if self.hits.len() >= 50_000 {
            let idle = now - self.span;
            self.hits.retain(|_, h| h.back().is_some_and(|t| *t > idle));
            if self.hits.len() >= 50_000 {
                return Err(Duration::from_secs(60));
            }
        }
        let h = self.hits.entry(addr).or_default();
        while h
            .front()
            .is_some_and(|t| now.duration_since(*t) >= self.span)
        {
            h.pop_front();
        }
        if h.len() >= self.cap {
            return Err(self.span - now.duration_since(*h.front().unwrap_or(&now)));
        }
        h.push_back(now);
        Ok(())
    }
}

/// Per-address limits for the open auth surface (per finding-6's
/// `client_addr` — the untrusted socket address unless a trusted proxy).
struct AuthLimits {
    signup_hour: Mutex<SlidingWindow>,
    signup_day: Mutex<SlidingWindow>,
    login: Mutex<SlidingWindow>,
    /// verify/reset redemption: 20/min per address.
    redeem: Mutex<SlidingWindow>,
    /// forgot/resend-verification: 5/hour per address.
    mail_req: Mutex<SlidingWindow>,
}

impl AuthLimits {
    fn new() -> Self {
        Self {
            signup_hour: Mutex::new(SlidingWindow::new(5, Duration::from_secs(3600))),
            signup_day: Mutex::new(SlidingWindow::new(20, Duration::from_secs(86400))),
            // Coarser than the (email, prefix) lockout so the lockout's
            // precise 429 — not this flood bound — answers the tripped
            // pair; this one only stops an address spraying many emails.
            login: Mutex::new(SlidingWindow::new(30, Duration::from_secs(60))),
            redeem: Mutex::new(SlidingWindow::new(20, Duration::from_secs(60))),
            mail_req: Mutex::new(SlidingWindow::new(5, Duration::from_secs(3600))),
        }
    }

    fn check(lim: &Mutex<SlidingWindow>, addr: std::net::IpAddr) -> Result<(), ApiError> {
        lim.lock()
            .unwrap_or_else(|e| e.into_inner())
            .check(addr, Instant::now())
            .map_err(|retry| {
                let mut e = ApiError::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limited",
                    "too many attempts; retry shortly",
                );
                e.retry_after = Some(retry.as_secs().max(1));
                e
            })
    }
}

/// The request's client address, inserted as an extension by the
/// service wrapper (computed once there through `client_addr`).
#[derive(Clone, Copy)]
struct ClientAddr(std::net::IpAddr);

/// Request extension: this connection arrived on the `--ops-listen`
/// socket — the only place env-key principals hold (finding 18).
#[derive(Clone, Copy)]
struct OpsListener;

fn client(parts: &Parts) -> std::net::IpAddr {
    parts
        .extensions
        .get::<ClientAddr>()
        .map(|c| c.0)
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
}

/// Origin/Fetch-Site defence for requests that carry no session yet
/// (review #17): with no synchroniser token possible, the origin check
/// IS the CSRF layer on the open auth POSTs — plus JSON-only bodies
/// (the `J` extractor's 415) and no CORS layer.
fn check_request_origin(parts: &Parts) -> Result<(), ApiError> {
    if let Some(site) = parts
        .headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
    {
        // ATTACK(csrf-open-post)
        if site != "same-origin" && site != "none" {
            return Err(csrf_failed());
        }
    }
    if let Some(origin) = parts
        .headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
    {
        let host = parts
            .headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let ok = url::Url::parse(origin).ok().is_some_and(|u| {
            u.host_str()
                .is_some_and(|h| host == h || host.starts_with(&format!("{h}:")))
        });
        if !ok {
            return Err(csrf_failed());
        }
    }
    Ok(())
}

fn csrf_failed() -> ApiError {
    ApiError::new(
        StatusCode::FORBIDDEN,
        "csrf_failed",
        "cross-site request refused",
    )
}

/// Proof of a signed-in session. Resolution order (review S3): any
/// `Authorization` header wins — bearer-only, no cookie fallback — so a
/// `drs_` bearer and a `dr_` cookie both die 401 rather than confuse
/// the two planes. The session surface answers 404 (not 401) when no
/// account store is configured: the dashboard's compat key-paste mode
/// keys off that exact status.
struct SessionAuth(std::sync::Arc<durable_accounts::SessionGrant>);

impl FromRequestParts<App> for SessionAuth {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let Some(store) = app.keyring.store.clone() else {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "unauthenticated",
                "no account store is configured",
            ));
        };
        if parts.headers.contains_key(header::AUTHORIZATION) {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "API keys cannot reach the session surface",
            ));
        }
        let grant = session_grant_for(app, parts, &store).await?;
        Ok(SessionAuth(grant))
    }
}

/// The session-cookie → grant resolution shared by `SessionAuth` and
/// the dual-auth flat routes (D-066): tag-checked `drs_`, cache-then-
/// store, idle+absolute expiry with the pending-touch floor, and the
/// mid-await bump drop — identical answers either way in.
async fn session_grant_for(
    app: &App,
    parts: &Parts,
    store: &Arc<dyn AccountStore>,
) -> Result<std::sync::Arc<durable_accounts::SessionGrant>, ApiError> {
    let token = session_cookie(parts)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated", "no session"))?;
    let Some(hash) = session_token_hash(&app.session_key, &token) else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "no session",
        ));
    };
    let now = app.now_ms();
    if let Some(grant) = app.sessions.get(&hash, now) {
        app.sessions.touch(hash, now);
        return Ok(grant);
    }
    let fill_start = Instant::now();
    let grant = match store.session_grant(&hash).await {
        Ok(Some(g)) => g,
        Ok(None) => {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "no session",
            ))
        }
        Err(_) => return Err(accounts_unavailable()),
    };
    // The idle check takes the pending-touch floor too — a slide
    // queued but not yet flushed still counts as use (D-071).
    let idle = grant.idle_expires_at_ms.max(app.sessions.touch_idle(&hash));
    // ATTACK(idle-timeout)
    if grant.expires_at_ms <= now || idle <= now {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "session expired",
        ));
    }
    let grant = std::sync::Arc::new(grant);
    // A revocation that landed inside the store await drops the
    // fill — deny rather than serve stale (review #10).
    if !app
        .sessions
        .put(hash, std::sync::Arc::clone(&grant), fill_start)
    {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "no session",
        ));
    }
    app.sessions.touch(hash, now);
    Ok(grant)
}

/// A session-authed mutation: SessionAuth plus the synchroniser CSRF
/// check (`x-durable-csrf` recomputed from the seed, constant-time).
struct SessionWrite(std::sync::Arc<durable_accounts::SessionGrant>);

impl FromRequestParts<App> for SessionWrite {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let auth = SessionAuth::from_request_parts(parts, app).await?;
        let presented = parts
            .headers
            .get("x-durable-csrf")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if !csrf_ok(&auth.0.csrf_seed, presented) {
            return Err(csrf_failed());
        }
        check_request_origin(parts)?;
        Ok(SessionWrite(auth.0))
    }
}

#[derive(serde::Deserialize)]
struct SignupReq {
    email: String,
    password: String,
    /// Required when the deployment runs invite-only (ruling 9).
    #[serde(default)]
    invite_token: Option<String>,
}

#[derive(serde::Deserialize)]
struct LoginReq {
    email: String,
    password: String,
}

#[derive(serde::Deserialize)]
struct VerifyReq {
    token: String,
}

#[derive(serde::Deserialize)]
struct ResetReq {
    token: String,
    password: String,
}

#[derive(serde::Deserialize)]
struct EmailReq {
    email: String,
}

fn normalize_email(raw: &str) -> Option<String> {
    let email = raw.trim().to_lowercase();
    (email.len() <= 254 && email.matches('@').count() == 1 && email.len() > 3).then_some(email)
}

/// Mail hardening (review #14): subjects are fixed strings per kind —
/// never user input; every interpolated value is HTML-escaped in the
/// html twin; `send_to` passes the strict addr-spec check at enqueue.
const SUBJECT_VERIFY: &str = "Verify your Durable Runner email";
const SUBJECT_RESET: &str = "Reset your Durable Runner password";
const SUBJECT_TAKEN: &str = "A Durable Runner sign-up used your address";
const SUBJECT_LOCKOUT: &str = "Unusual sign-in attempts on your Durable Runner account";

fn verify_mail_body(origin: &str, token: &str) -> (String, String) {
    let link = format!("{origin}/app#/verify?token={token}");
    let text = format!(
        "Verify your Durable Runner email\n\nOpen this link within 24 hours:\n{link}\n\n\
         If you did not sign up, ignore this mail."
    );
    let html = format!(
        "<p>Verify your Durable Runner email</p><p><a href=\"{0}\">{0}</a></p>\
         <p>If you did not sign up, ignore this mail.</p>",
        durable_accounts::mail::html_escape(&link)
    );
    (text, html)
}

fn reset_mail_body(origin: &str, token: &str) -> (String, String) {
    let link = format!("{origin}/app#/reset?token={token}");
    let text = format!(
        "Reset your Durable Runner password\n\nOpen this link within 1 hour:\n{link}\n\n\
         If you did not ask for a reset, ignore this mail — your password is unchanged."
    );
    let html = format!(
        "<p>Reset your Durable Runner password</p><p><a href=\"{0}\">{0}</a></p>\
         <p>If you did not ask for a reset, ignore this mail — your password is unchanged.</p>",
        durable_accounts::mail::html_escape(&link)
    );
    (text, html)
}

fn taken_mail_body() -> (String, String) {
    (
        "Someone tried to sign up with this address, which already has an account.\n\
         If it was you, sign in or reset your password; if not, ignore this mail."
            .to_string(),
        "<p>Someone tried to sign up with this address, which already has an account.</p>\
         <p>If it was you, sign in or reset your password; if not, ignore this mail.</p>"
            .to_string(),
    )
}

fn lockout_mail_body() -> (String, String) {
    (
        "Several failed sign-in attempts hit your account, so new attempts are \
         paused for a while.\nIf this was you, wait a few minutes or reset your \
         password; if not, your address may be getting probed — nothing was \
         signed in."
            .to_string(),
        "<p>Several failed sign-in attempts hit your account, so new attempts are \
         paused for a while.</p><p>If this was you, wait a few minutes or reset \
         your password; if not, your address may be getting probed — nothing \
         was signed in.</p>"
            .to_string(),
    )
}

/// The session cookie's account — resolved only so a token redeem can
/// tell "my session" from "someone else's". A dead or forged cookie is
/// `None`, same as absent.
async fn cookie_account(
    app: &App,
    parts: &Parts,
    store: &std::sync::Arc<dyn AccountStore>,
) -> Result<Option<u64>, ApiError> {
    let Some(token) = session_cookie(parts) else {
        return Ok(None);
    };
    let Some(hash) = session_token_hash(&app.session_key, &token) else {
        return Ok(None);
    };
    let now = app.now_ms();
    if let Some(grant) = app.sessions.get(&hash, now) {
        return Ok(Some(grant.account.id));
    }
    match store.session_grant(&hash).await {
        Ok(Some(g)) if g.expires_at_ms > now && g.idle_expires_at_ms > now => {
            Ok(Some(g.account.id))
        }
        Ok(_) => Ok(None),
        Err(_) => Err(accounts_unavailable()),
    }
}

/// Who gets a fresh session cookie after a token redeem (review #2,
/// #17): verify NEVER sets one — the mailbox owner signs in fresh;
/// reset sets one only when the requester is signed out or already
/// signed in as that account (never signs a different browser over).
enum TokenCookie {
    Never,
    UnlessForeignSession,
}

async fn finish_redeem(
    parts: &Parts,
    app: &App,
    store: &std::sync::Arc<dyn AccountStore>,
    account: &durable_accounts::Account,
    policy: TokenCookie,
    mut res: Response,
) -> Result<Response, ApiError> {
    let set_fresh = match policy {
        TokenCookie::Never => false,
        TokenCookie::UnlessForeignSession => match cookie_account(app, parts, store).await? {
            None => true,
            Some(other) => other == account.id,
        },
    };
    if !set_fresh {
        return Ok(res);
    }
    let addr = client(parts);
    let token = mint_session_token(&app.session_key);
    let hash = sha256(token.as_bytes());
    let mut seed = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
        .unwrap_or_else(|_| panic!("rng"));
    let now = app.now_ms();
    store
        .create_session(&durable_accounts::NewSession {
            account: account.id,
            token_hash: hash,
            csrf_seed: seed,
            expires_at_ms: now + SESSION_ABS_MS,
            idle_expires_at_ms: now + SESSION_IDLE_MS,
            replaces: None,
            user_agent: parts
                .headers
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .map(|s| s[..s.len().min(256)].to_string()),
            ip: Some(addr),
        })
        .await
        .map_err(|_| accounts_unavailable())?;
    res.headers_mut().append(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&session_set_cookie(&token))
            .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    res.headers_mut().append(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&known_device_set_cookie(&mint_known_device(
            &app.session_key,
            &account.public_id,
        )))
        .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    Ok(res)
}

async fn auth_signup(
    parts: Parts,
    State(app): State<App>,
    J(req): J<SignupReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    let addr = client(&parts);
    AuthLimits::check(&app.auth_limits.signup_hour, addr)?;
    AuthLimits::check(&app.auth_limits.signup_day, addr)?;
    let Some(email) = normalize_email(&req.email) else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "invalid email",
        ));
    };
    if let Some(why) = durable_accounts::password::policy_refusal(&req.password) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bad_input", why));
    }
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    if app.invite_only {
        let ok = match req.invite_token.as_deref().and_then(invite_token_hash) {
            Some(hash) => store
                .invite_grant(&hash, unix_ms())
                .await
                .map_err(|_| accounts_unavailable())?
                .is_some_and(|g| g.email == email),
            None => false,
        };
        // ATTACK(invite-required)
        if !ok {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "invite_required",
                "sign-up is invite-only right now",
            ));
        }
    }
    // The hash runs on every branch BEFORE the store call (review S3):
    // Created / VerifyResent / Taken all cost the same work.
    let password = req.password.clone();
    let phc = app
        .argon
        .run(addr, false, move || {
            durable_accounts::password::hash_password(&password)
        })
        .await?
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()))?;
    let verify_token = mint_email_token();
    let token_hash = sha256(verify_token.as_bytes());
    let expires = unix_ms() + VERIFY_TOKEN_TTL_MS;
    let origin = app.public_origin.as_deref().unwrap_or("http://localhost");
    let (text, html) = verify_mail_body(origin, &verify_token);
    let verify_mail = Mail {
        kind: "verify",
        send_to: email.clone(),
        subject: SUBJECT_VERIFY.to_string(),
        text,
        html,
        idempotency_key: format!("verify:{email}:{}", hex(&token_hash[..8])),
        expires_at_ms: Some(expires),
    };
    let (text, html) = taken_mail_body();
    let notice_mail = Mail {
        kind: "notice",
        send_to: email.clone(),
        subject: SUBJECT_TAKEN.to_string(),
        text,
        html,
        idempotency_key: format!("notice:{email}:{}", hex(&token_hash[..8])),
        expires_at_ms: Some(unix_ms() + 24 * 3600 * 1000),
    };
    // The outbox row commits inside the signup transaction; the drain
    // worker owns delivery — a racing immediate send could double-send
    // with a claimed row, so there is none.
    let outcome = store
        .signup(
            &email,
            &phc,
            &token_hash,
            expires,
            &verify_mail,
            &notice_mail,
        )
        .await
        .map_err(|_| accounts_unavailable())?;
    // An unverified re-sign-up revoked that account's sessions inside
    // the transaction — the cache agrees before the next request.
    if let durable_accounts::SignupOutcome::VerifyResent { account, .. } = outcome {
        app.sessions.bump_account(account);
    }
    Ok(no_store(
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status": "verification_sent"})),
        )
            .into_response(),
    ))
}

async fn auth_login(
    parts: Parts,
    State(app): State<App>,
    J(req): J<LoginReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    let addr = client(&parts);
    let Some(email) = normalize_email(&req.email) else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "invalid email",
        ));
    };
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    let account = store
        .account_by_email(&email)
        .await
        .map_err(|_| accounts_unavailable())?;
    let prefix = client_prefix(addr);
    let now = unix_ms();
    // A recognized device skips the cool-off (review #13): the cookie
    // is HMAC-tagged to this account's public_id, so it can't be
    // forged or replayed for another address — and it is checked
    // before the lockout so a cooling attacker can't keep the owner
    // out. Unknown accounts never bypass: no cookie can match them.
    let known_device = account
        .as_ref()
        .is_some_and(|a| known_device_ok(&app.session_key, &parts, &a.public_id));
    if !known_device {
        // ATTACK(cooloff)
        if let Some(retry) = app.lockouts.check(&email, prefix, now) {
            let mut e = ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many attempts; retry shortly",
            );
            e.retry_after = Some(retry);
            return Err(e);
        }
    }
    AuthLimits::check(&app.auth_limits.login, addr)?;
    // Unknown address: verify the dummy PHC — identical work, no oracle.
    let phc = account
        .as_ref()
        .and_then(|a| a.password_hash.clone())
        .unwrap_or_else(|| durable_accounts::password::DUMMY_PHC.to_string());
    let password = req.password.clone();
    let ok = app
        .argon
        .run(addr, known_device, move || {
            durable_accounts::password::verify_password(&phc, &password)
        })
        .await?;
    if !ok || account.is_none() {
        if !known_device {
            // Failures count against (email, prefix) whether or not the
            // account exists — keying only real accounts would make
            // 429-vs-401 a registration oracle (review #13).
            // ATTACK(lockout-shape)
            let tripped = app.lockouts.fail(&email, prefix, now);
            if tripped {
                if let Some(acct) = &account {
                    // The owner notice rides the outbox's own dedupe:
                    // the once-a-day idempotency key coalesces repeats.
                    let day = now / 86_400_000;
                    let (text, html) = lockout_mail_body();
                    let _ = store
                        .enqueue_mail(&Mail {
                            kind: "notice",
                            send_to: acct.email.clone(),
                            subject: SUBJECT_LOCKOUT.to_string(),
                            text,
                            html,
                            idempotency_key: format!("lockout:{}:{day}", acct.id),
                            expires_at_ms: Some(now + 24 * 3600 * 1000),
                        })
                        .await;
                }
            }
        }
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "invalid credentials",
        ));
    }
    let account = account.unwrap_or_else(|| panic!("checked"));
    app.lockouts.clear(&email, prefix);
    // Sign-in always mints a fresh token — a cookie value the client
    // arrived with can never become the session (no fixation).
    // ATTACK(session-fixation)
    let token = mint_session_token(&app.session_key);
    let hash = sha256(token.as_bytes());
    let mut seed = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
        .unwrap_or_else(|_| panic!("rng"));
    let now = app.now_ms();
    store
        .create_session(&durable_accounts::NewSession {
            account: account.id,
            token_hash: hash,
            csrf_seed: seed,
            expires_at_ms: now + SESSION_ABS_MS,
            idle_expires_at_ms: now + SESSION_IDLE_MS,
            replaces: None,
            user_agent: parts
                .headers
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .map(|s| s[..s.len().min(256)].to_string()),
            ip: Some(addr),
        })
        .await
        .map_err(|_| accounts_unavailable())?;
    let mut res = no_store(
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "account": {
                    "public_id": account.public_id,
                    "email": account.email,
                    "verified": account.email_verified,
                },
                "csrf": csrf_token(&seed),
            })),
        )
            .into_response(),
    );
    res.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&session_set_cookie(&token)) // ATTACK(cookie-attrs)
            .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    res.headers_mut().append(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&known_device_set_cookie(&mint_known_device(
            &app.session_key,
            &account.public_id,
        )))
        .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    Ok(res)
}

async fn auth_logout(
    SessionWrite(_grant): SessionWrite,
    State(app): State<App>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let Some(store) = app.keyring.store.clone() else {
        return Err(accounts_unavailable());
    };
    let token = session_cookie(&parts).unwrap_or_default();
    let hash = sha256(token.as_bytes());
    let _ = store.revoke_session(&hash).await;
    app.sessions.bump(hash);
    let mut res = no_store(StatusCode::NO_CONTENT.into_response());
    res.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_static(session_clear_cookie()),
    );
    Ok(res)
}

/// One 410 for every dead token state — consumed, superseded, expired,
/// wrong kind, unknown all look identical (review: token oracle).
fn token_gone() -> ApiError {
    ApiError::new(
        StatusCode::GONE,
        "token_gone",
        "this link is no longer valid — request a fresh one",
    )
}

async fn auth_verify(
    parts: Parts,
    State(app): State<App>,
    J(req): J<VerifyReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    AuthLimits::check(&app.auth_limits.redeem, client(&parts))?;
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    let Some(hash) = email_token_hash(&req.token) else {
        return Err(token_gone());
    };
    // The consume marks the token, installs the pending password hash,
    // sets verified_at and revokes every session — atomically, in the
    // store's single transaction (replay and takeover both die there).
    // ATTACK(token-replay)
    let account = store
        .consume_verify_token(&hash)
        .await
        .map_err(|_| accounts_unavailable())?
        .ok_or_else(token_gone)?;
    // The store revoked every session in that transaction — the cache
    // must agree immediately, not at grant TTL.
    app.sessions.bump_account(account.id);
    Ok(no_store(
        finish_redeem(
            &parts,
            &app,
            &store,
            &account,
            // ATTACK(verify-sets-cookie)
            TokenCookie::Never,
            Json(serde_json::json!({"verified": true})).into_response(),
        )
        .await?,
    ))
}

/// Always-202 envelope for the email-keyed routes: the response and its
/// timing must not leak whether the address is registered (review #3).
fn accepted_sent() -> Response {
    no_store(
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"status": "sent"})),
        )
            .into_response(),
    )
}

async fn auth_resend_verification(
    parts: Parts,
    State(app): State<App>,
    J(req): J<EmailReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    AuthLimits::check(&app.auth_limits.mail_req, client(&parts))?;
    let res = accepted_sent();
    let (Some(store), Some(email)) = (app.keyring.store.clone(), normalize_email(&req.email))
    else {
        return Ok(res);
    };
    // The store supersedes the pending verify token, inserts the fresh
    // one (carrying the account's current hash) and enqueues the mail
    // in one transaction — `None` (unknown / already verified) and the
    // 202 already chosen keep the envelope sealed.
    let token = mint_email_token();
    let hash = sha256(token.as_bytes());
    let expires = unix_ms() + VERIFY_TOKEN_TTL_MS;
    let origin = app.public_origin.as_deref().unwrap_or("http://localhost");
    let (text, html) = verify_mail_body(origin, &token);
    let _ = store
        .reissue_verify(
            &email,
            &hash,
            expires,
            &Mail {
                kind: "verify",
                send_to: email.clone(),
                subject: SUBJECT_VERIFY.to_string(),
                text,
                html,
                idempotency_key: format!("verify:{email}:{}", hex(&hash[..8])),
                expires_at_ms: Some(expires),
            },
        )
        .await
        .map_err(|_| accounts_unavailable())?;
    Ok(res)
}

async fn auth_forgot(
    parts: Parts,
    State(app): State<App>,
    J(req): J<EmailReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    AuthLimits::check(&app.auth_limits.mail_req, client(&parts))?;
    let res = accepted_sent();
    let (Some(store), Some(email)) = (app.keyring.store.clone(), normalize_email(&req.email))
    else {
        return Ok(res);
    };
    let token = mint_email_token();
    let hash = sha256(token.as_bytes());
    let expires = unix_ms() + RESET_TOKEN_TTL_MS;
    let origin = app.public_origin.as_deref().unwrap_or("http://localhost");
    let (text, html) = reset_mail_body(origin, &token);
    // Token + mail commit in one store transaction; `None` (unknown or
    // unverified address) enqueues nothing — the 202 went out either
    // way, so the envelope never registers (review #3).
    let issued = store
        .issue_reset_token(
            &email,
            &hash,
            expires,
            &Mail {
                kind: "reset",
                send_to: email.clone(),
                subject: SUBJECT_RESET.to_string(),
                text,
                html,
                idempotency_key: format!("reset:{email}:{}", hex(&hash[..8])),
                expires_at_ms: Some(expires),
            },
        )
        .await
        .map_err(|_| accounts_unavailable())?
        .is_some();
    // `issued` is deliberately dropped — whether a mail was enqueued
    // must never shape the response.
    // ATTACK(enumeration)
    let _ = issued;
    Ok(res)
}

async fn auth_reset(
    parts: Parts,
    State(app): State<App>,
    J(req): J<ResetReq>,
) -> Result<Response, ApiError> {
    check_request_origin(&parts)?;
    let addr = client(&parts);
    AuthLimits::check(&app.auth_limits.redeem, addr)?;
    if let Some(why) = durable_accounts::password::policy_refusal(&req.password) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bad_input", why));
    }
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    let Some(hash) = email_token_hash(&req.token) else {
        return Err(token_gone());
    };
    // Hash BEFORE the consume: a dead token still costs the work, and
    // the store only sees the digest.
    let password = req.password.clone();
    let phc = app
        .argon
        .run(addr, false, move || {
            durable_accounts::password::hash_password(&password)
        })
        .await?
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()))?;
    // One transaction: install the hash, consume the token, kill the
    // account's other pending tokens, revoke every session.
    // ATTACK(token-replay)
    let account = store
        .consume_reset_token(&hash, &phc)
        .await
        .map_err(|_| accounts_unavailable())?
        .ok_or_else(token_gone)?;
    // Every session died in that transaction — the cache agrees at
    // once, so a just-reset account's stolen cookie dies mid-flight.
    app.sessions.bump_account(account.id);
    app.lockouts.clear_email(&account.email);
    Ok(no_store(
        finish_redeem(
            &parts,
            &app,
            &store,
            &account,
            TokenCookie::UnlessForeignSession,
            Json(serde_json::json!({"reset": true})).into_response(),
        )
        .await?,
    ))
}

async fn auth_session(SessionAuth(grant): SessionAuth) -> Result<Response, ApiError> {
    Ok(no_store(
        Json(serde_json::json!({
            "account": {
                "public_id": grant.account.public_id,
                "email": grant.account.email,
                "verified": grant.account.email_verified,
            },
            "csrf": csrf_token(&grant.csrf_seed),
            "workspaces": [],
        }))
        .into_response(),
    ))
}

async fn me_account(SessionAuth(grant): SessionAuth) -> Result<Response, ApiError> {
    Ok(no_store(
        Json(serde_json::json!({
            "account": {
                "public_id": grant.account.public_id,
                "email": grant.account.email,
                "verified": grant.account.email_verified,
            },
        }))
        .into_response(),
    ))
}

async fn me_sessions(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
) -> Result<Response, ApiError> {
    let Some(store) = app.keyring.store.clone() else {
        return Err(accounts_unavailable());
    };
    let sessions = store
        .sessions_for_account(
            grant.account.id,
            durable_accounts::MAX_SESSIONS_PER_ACCOUNT as u32,
        )
        .await
        .map_err(|_| accounts_unavailable())?;
    let list: Vec<serde_json::Value> = sessions
        .iter()
        .map(|s| {
            serde_json::json!({
                // The opaque public_id is the only id that crosses —
                // the internal row id never does (S3-2).
                "id": s.public_id,
                "current": s.public_id == grant.public_id,
                "created_at_ms": s.created_at_ms,
                "last_seen_at_ms": s.last_seen_at_ms,
                "expires_at_ms": s.expires_at_ms,
                "idle_expires_at_ms": s.idle_expires_at_ms,
                "user_agent": s.user_agent,
                "ip": s.ip,
            })
        })
        .collect();
    Ok(no_store(
        Json(serde_json::json!({"sessions": list})).into_response(),
    ))
}

/// Revoke one session by its opaque public_id — the account binding
/// lives inside the store query, so a foreign (or unknown) id answers
/// the same 404 either way: no cross-account oracle (S3-2).
async fn me_revoke_one(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path(public_id): axum::extract::Path<uuid::Uuid>,
) -> Result<Response, ApiError> {
    let Some(store) = app.keyring.store.clone() else {
        return Err(accounts_unavailable());
    };
    match store
        .revoke_session_public(grant.account.id, public_id)
        .await
        .map_err(|_| accounts_unavailable())?
    {
        Some(hash) => {
            app.sessions.bump(hash);
            Ok(no_store(StatusCode::NO_CONTENT.into_response()))
        }
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such session",
        )),
    }
}

/// Revoke every session except the requester's — sign-out-everywhere-
/// else. The account bump kills the victims' cached grants at once;
/// the survivor is re-warmed so the caller stays signed in.
async fn me_revoke_all(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let Some(store) = app.keyring.store.clone() else {
        return Err(accounts_unavailable());
    };
    let revoked = store
        .revoke_sessions_except(grant.account.id, grant.public_id)
        .await
        .map_err(|_| accounts_unavailable())?;
    app.sessions.bump_account(grant.account.id);
    // The bump also shadows this session's entry — re-warm it.
    if let Some(token) = session_cookie(&parts) {
        app.sessions
            .put(sha256(token.as_bytes()), grant.clone(), Instant::now());
    }
    Ok(no_store(
        Json(serde_json::json!({"revoked": revoked})).into_response(),
    ))
}

#[derive(serde::Deserialize)]
struct PasswordChangeReq {
    current_password: String,
    new_password: String,
}

/// Password change with re-auth (S3-2): the current password is
/// verified through the same bounded argon queue as sign-in, then one
/// transaction installs the hash, supersedes every pending token and
/// revokes every session — the caller gets a fresh rotated session
/// (`replaces` links the old row for audit).
async fn me_password(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    parts: Parts,
    J(req): J<PasswordChangeReq>,
) -> Result<Response, ApiError> {
    let Some(store) = app.keyring.store.clone() else {
        return Err(accounts_unavailable());
    };
    if let Some(why) = durable_accounts::password::policy_refusal(&req.new_password) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bad_input", why));
    }
    let addr = client(&parts);
    // Re-auth: a found laptop can't rotate the password. One failure
    // shape — no "wrong current password" oracle beyond the 403.
    let phc = grant
        .account
        .password_hash
        .clone()
        .unwrap_or_else(|| durable_accounts::password::DUMMY_PHC.to_string());
    let password = req.current_password.clone();
    let ok = app
        .argon
        .run(addr, false, move || {
            durable_accounts::password::verify_password(&phc, &password)
        })
        .await?;
    if !ok || grant.account.password_hash.is_none() {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "invalid_credentials",
            "current password does not match",
        ));
    }
    let password = req.new_password.clone();
    let new_phc = app
        .argon
        .run(addr, false, move || {
            durable_accounts::password::hash_password(&password)
        })
        .await?
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()))?;
    // One transaction: new hash, pending tokens superseded, every
    // session revoked — then the cache agrees.
    store
        .change_password(grant.account.id, &new_phc)
        .await
        .map_err(|_| accounts_unavailable())?;
    app.sessions.bump_account(grant.account.id);
    app.lockouts.clear_email(&grant.account.email);
    // Rotation on privilege change (S3-2): a fresh session linked to
    // the one it replaces — the old token is dead in the same tx.
    let token = mint_session_token(&app.session_key);
    let hash = sha256(token.as_bytes());
    let mut seed = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
        .unwrap_or_else(|_| panic!("rng"));
    let now = app.now_ms();
    store
        .create_session(&durable_accounts::NewSession {
            account: grant.account.id,
            token_hash: hash,
            csrf_seed: seed,
            expires_at_ms: now + SESSION_ABS_MS,
            idle_expires_at_ms: now + SESSION_IDLE_MS,
            replaces: Some(grant.session_id),
            user_agent: parts
                .headers
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .map(|s| s[..s.len().min(256)].to_string()),
            ip: Some(addr),
        })
        .await
        .map_err(|_| accounts_unavailable())?;
    let mut res = no_store(
        Json(serde_json::json!({
            "changed": true,
            "csrf": csrf_token(&seed),
        }))
        .into_response(),
    );
    res.headers_mut().append(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&session_set_cookie(&token))
            .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    res.headers_mut().append(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&known_device_set_cookie(&mint_known_device(
            &app.session_key,
            &grant.account.public_id,
        )))
        .unwrap_or_else(|_| header::HeaderValue::from_static("")),
    );
    Ok(res)
}

// ---- S3-3: workspaces, members, invites (D-066, D-072) --------------

const INVITE_PREFIX: &str = "dri_";
const INVITE_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

/// Resolve membership for a `/workspaces/{pid}/*` route (D-066): the
/// path pid is the only authority — an `x-durable-workspace` header
/// that names another workspace is a 400. A deleted workspace answers
/// 410 `workspace_closed`; a non-member answers 404 (never 403 — the
/// workspace's existence is itself member knowledge); a role below
/// `need` answers 403.
async fn member_access(
    app: &App,
    grant: &durable_accounts::SessionGrant,
    pid: uuid::Uuid,
    parts: &Parts,
    need: Role,
) -> Result<(durable_accounts::Workspace, Role), ApiError> {
    if let Some(h) = parts
        .headers
        .get("x-durable-workspace")
        .and_then(|v| v.to_str().ok())
    {
        if h != pid.to_string() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "workspace_mismatch",
                "x-durable-workspace does not match the path workspace",
            ));
        }
    }
    let store = accounts(app)?;
    let Some(ws) = store.workspace_by_public_id(pid).await.map_err(store_err)? else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such workspace",
        ));
    };
    if ws.deleted_at_ms.is_some() {
        return Err(ApiError::new(
            StatusCode::GONE,
            "workspace_closed",
            "this workspace is deleted",
        ));
    }
    let account = grant.account.id;
    let role = match app.members.get(account, ws.id) {
        Some(role) => role,
        None => {
            let fill_start = Instant::now();
            let Some(role) = store.member_role(account, ws.id).await.map_err(store_err)? else {
                return Err(ApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "no such workspace",
                ));
            };
            app.members.put(account, ws.id, role, fill_start);
            role
        }
    };
    if role < need {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "this role may not do that",
        ));
    }
    Ok((ws, role))
}

/// A session's workspace on a FLAT route (`/handlers*` — D-066): the
/// `x-durable-workspace` header names it; absent or unparseable is 400
/// `workspace_required`, deleted is 410, non-member 404, low role 403.
async fn flat_member(
    app: &App,
    grant: &durable_accounts::SessionGrant,
    parts: &Parts,
    need: Role,
) -> Result<(durable_accounts::Workspace, Role), ApiError> {
    let raw = parts
        .headers
        .get("x-durable-workspace")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "workspace_required",
                "session callers name the workspace in x-durable-workspace",
            )
        })?;
    let pid = raw.parse::<uuid::Uuid>().map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "workspace_required",
            "x-durable-workspace must be a workspace public id",
        )
    })?;
    let store = accounts(app)?;
    let Some(ws) = store.workspace_by_public_id(pid).await.map_err(store_err)? else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such workspace",
        ));
    };
    if ws.deleted_at_ms.is_some() {
        return Err(ApiError::new(
            StatusCode::GONE,
            "workspace_closed",
            "this workspace is deleted",
        ));
    }
    let account = grant.account.id;
    let role = match app.members.get(account, ws.id) {
        Some(role) => role,
        None => {
            let fill_start = Instant::now();
            let Some(role) = store.member_role(account, ws.id).await.map_err(store_err)? else {
                return Err(ApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "no such workspace",
                ));
            };
            app.members.put(account, ws.id, role, fill_start);
            role
        }
    };
    if role < need {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "this role may not do that",
        ));
    }
    Ok((ws, role))
}

/// The `--ops-listen` gate around `keyring.verify`: with the flag set,
/// an env-key grant is honoured only on the ops socket — on `--listen`
/// the same bearer answers `Denied`, indistinguishable from a bad key
/// (finding 18). Store keys and sessions are unaffected everywhere.
/// Without the flag nothing changes.
async fn verify_key(app: &App, parts: &Parts, key: &str) -> Verified {
    match app.keyring.verify(key).await {
        Verified::Granted(p)
            if app.ops_gate
                && matches!(p, Principal::OpsKey | Principal::OpsRead)
                && parts.extensions.get::<OpsListener>().is_none() =>
        {
            Verified::Denied
        }
        v => v,
    }
}

/// The dual-auth resolution for flat tenant routes (D-066/D-073): a
/// bearer takes the key path (workspace from the grant, `key_need` the
/// permission the call requires); a session takes the member path
/// (workspace from `x-durable-workspace`, `role_need` the role the call
/// requires, `write` also binds the CSRF + Origin checks). Neither
/// credential answers 401.
async fn flat_access(
    app: &App,
    parts: &Parts,
    key_need: Permission,
    role_need: Role,
    write: bool,
) -> Result<u64, ApiError> {
    if let Some(key) = bearer(parts) {
        // ATTACK(auth)
        return match verify_key(app, parts, key).await {
            Verified::Granted(Principal::OpsKey) => Ok(0),
            Verified::Granted(Principal::OpsRead) if key_need == Permission::Read => Ok(0),
            Verified::Granted(Principal::StoreKey { ws, permission })
                if key_need == Permission::Read || permission == Permission::Write =>
            {
                Ok(ws)
            }
            Verified::Granted(_) => Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "this key may only read; this route needs write",
            )),
            Verified::Denied => Err(unauthorized()),
            Verified::Unavailable => Err(accounts_unavailable()),
        };
    }
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    let grant = session_grant_for(app, parts, &store).await?;
    if write {
        let presented = parts
            .headers
            .get("x-durable-csrf")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if !csrf_ok(&grant.csrf_seed, presented) {
            return Err(csrf_failed());
        }
        check_request_origin(parts)?;
    }
    let (ws, _) = flat_member(app, &grant, parts, role_need).await?;
    Ok(ws.id)
}

/// Read-side dual auth for the run-plane routes: bearer keys take
/// their old `ReadAuthorized` path — `--open-reads` on a loopback name
/// included — while a session takes the member path at viewer+
/// (x-durable-workspace required; the D-062 rules live in
/// `flat_member`). Downstream `auth.ws()` scopes exactly as before.
async fn tenant_read(app: &App, parts: &Parts) -> Result<u64, ApiError> {
    if app.open_reads && loopback_host(parts) {
        return Ok(0);
    }
    flat_access(app, parts, Permission::Read, Role::Viewer, false).await
}

/// Tiny shim: the flat handlers below were written against
/// `Authorized(auth)`'s `auth.ws()` accessor — resolved now by
/// `flat_access`/`flat_admin_access` into the same shape.
struct FlatWs(u64);
impl FlatWs {
    fn ws(&self) -> u64 {
        self.0
    }
}

/// `/handler-secret` (D-073): the ops key or a session admin/owner —
/// a data-plane key is refused outright even when it can write.
async fn flat_admin_access(app: &App, parts: &Parts) -> Result<u64, ApiError> {
    if let Some(key) = bearer(parts) {
        return match verify_key(app, parts, key).await {
            Verified::Granted(Principal::OpsKey) => Ok(0),
            Verified::Granted(_) => Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "handler-secret needs a session admin; data-plane keys cannot mint it",
            )),
            Verified::Denied => Err(unauthorized()),
            Verified::Unavailable => Err(accounts_unavailable()),
        };
    }
    let Some(store) = app.keyring.store.clone() else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unauthenticated",
            "no account store is configured",
        ));
    };
    let grant = session_grant_for(app, parts, &store).await?;
    let presented = parts
        .headers
        .get("x-durable-csrf")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !csrf_ok(&grant.csrf_seed, presented) {
        return Err(csrf_failed());
    }
    check_request_origin(parts)?;
    let (ws, _) = flat_member(app, &grant, parts, Role::Admin).await?;
    Ok(ws.id)
}

/// `dri_` + hex(32 B random). The store keeps sha256 only — the token
/// crosses once, in the inviter's response.
fn mint_invite_token() -> String {
    let mut raw = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut raw)
        .unwrap_or_else(|_| panic!("rng"));
    format!("{INVITE_PREFIX}{}", hex(&raw))
}

fn valid_ws_name(name: &str) -> bool {
    let n = name.trim();
    !n.is_empty() && n.len() <= 128 && n.chars().all(|c| c != '\u{0}')
}

async fn ws_create(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    if !grant.account.email_verified {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "email_unverified",
            "verify the account's email first",
        ));
    }
    let name = req.get("name").and_then(|v| v.as_str()).unwrap_or_default();
    if !valid_ws_name(name) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "name must be 1-128 chars",
        ));
    }
    let store = accounts(&app)?;
    let ws = store
        .create_workspace_for(grant.account.id, name.trim())
        .await
        .map_err(store_err)?;
    Ok(no_store(
        Json(serde_json::json!({
            "public_id": ws.public_id,
            "name": ws.name,
            "role": "owner",
            "handler_secret": hex(&ws.handler_secret),
        }))
        .into_response(),
    ))
}

async fn ws_list_mine(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
) -> Result<Response, ApiError> {
    let store = accounts(&app)?;
    let rows = store
        .workspaces_for_account(grant.account.id)
        .await
        .map_err(store_err)?;
    let list: Vec<serde_json::Value> = rows
        .iter()
        .map(|(ws, role)| {
            serde_json::json!({
                "public_id": ws.public_id,
                "name": ws.name,
                "role": role.as_str(),
            })
        })
        .collect();
    Ok(no_store(
        Json(serde_json::json!({ "workspaces": list })).into_response(),
    ))
}

async fn ws_get(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, role) = member_access(&app, &grant, pid, &parts, Role::Viewer).await?;
    Ok(no_store(
        Json(serde_json::json!({
            "public_id": ws.public_id,
            "name": ws.name,
            "role": role.as_str(),
        }))
        .into_response(),
    ))
}

async fn ws_patch(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    // PATCH is UPDATE-only: `{name}` and nothing else.
    let name = req.get("name").and_then(|v| v.as_str()).unwrap_or_default();
    if !valid_ws_name(name)
        || req
            .as_object()
            .is_none_or(|o| o.keys().any(|k| k != "name"))
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "PATCH carries {name} only",
        ));
    }
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    accounts(&app)?
        .rename_workspace(ws.id, name.trim())
        .await
        .map_err(store_err)?;
    Ok(no_store(
        Json(serde_json::json!({ "public_id": ws.public_id, "name": name.trim() })).into_response(),
    ))
}

async fn ws_delete(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Owner).await?;
    let confirm = req
        .get("confirm")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if confirm != format!("delete {}", ws.name) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "confirm must be \"delete <name>\"",
        ));
    }
    let store = accounts(&app)?;
    store
        .delete_workspace(ws.id, app.now_ms())
        .await
        .map_err(store_err)?;
    // Keys die now, not at the cache TTL (D-072): drop every cached
    // grant of this workspace, then every cached member role in it.
    app.keyring.invalidate_ws(ws.id);
    app.members.bump_ws(ws.id);
    // Live runs are cancelled, finished history untouched (D-072).
    if let Ok(page) = app
        .engine
        .list_in(ws.id, Order::Oldest { after: None }, usize::MAX)
        .await
    {
        for run in page.runs {
            if !run.state.is_terminal() {
                let _ = app.engine.cancel(run.id).await;
            }
        }
    }
    Ok(no_store(StatusCode::NO_CONTENT.into_response()))
}

async fn ws_members(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Viewer).await?;
    let rows = accounts(&app)?
        .list_members(ws.id)
        .await
        .map_err(store_err)?;
    let list: Vec<serde_json::Value> = rows
        .iter()
        .map(|m| {
            serde_json::json!({
                "account": { "public_id": m.account_public_id, "email": m.email },
                "role": m.role.as_str(),
                "since": m.since_ms,
            })
        })
        .collect();
    Ok(no_store(
        Json(serde_json::json!({ "members": list })).into_response(),
    ))
}

async fn ws_invite_member(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let email = req
        .get("email")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_lowercase();
    let role = req
        .get("role")
        .and_then(|v| v.as_str())
        .and_then(Role::parse);
    // Members join only by invite, and owner rows only from POST
    // /workspaces — `owner` is never a valid invite role.
    let Some(role) = role.filter(|r| *r != Role::Owner) else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "role must be admin, member or viewer",
        ));
    };
    if !durable_accounts::mail::valid_addr_spec(&email) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "not an email address",
        ));
    }
    let store = accounts(&app)?;
    let token = mint_invite_token();
    let expires = app.now_ms() + INVITE_TTL_MS;
    let Some(invite) = store
        .create_invite(
            ws.id,
            &email,
            role,
            grant.account.id,
            &sha256(token.as_bytes()),
            expires,
        )
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "invite_pending",
            "a live invite already covers this address",
        ));
    };
    // If the address already belongs to a member, refuse rather than
    // mail a meaningless invite (409 already-member per review).
    if let Some(acc) = store.account_by_email(&email).await.map_err(store_err)? {
        if store
            .member_role(acc.id, ws.id)
            .await
            .map_err(store_err)?
            .is_some()
        {
            let _ = store.revoke_invite(ws.id, invite.public_id).await;
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "already_member",
                "this account is already a member",
            ));
        }
    }
    // The link the app understands: a FRAGMENT route, so the token never
    // lands in a server log or a Referer — it appears once, here and in
    // the inviter's response, and the store keeps only its sha256.
    let origin = app.public_origin.as_deref().unwrap_or("http://localhost");
    let link = format!("{origin}/app#/accept-invite?token={token}");
    let (subject, body) = (
        format!("Invitation to {}", ws.name),
        format!(
            "You were invited to {} as {}. Accept the invitation: {link}\n\n\
             If you did not expect this, ignore the message — the link expires.",
            ws.name,
            role.as_str()
        ),
    );
    let mail = Mail {
        kind: "workspace-invite",
        send_to: email.clone(),
        subject,
        text: body,
        html: String::new(),
        idempotency_key: format!("invite-{}", invite.public_id),
        expires_at_ms: Some(expires),
    };
    let _ = store.enqueue_mail(&mail).await.map_err(store_err)?;
    Ok(no_store(
        Json(serde_json::json!({
            "invite": {
                "public_id": invite.public_id,
                "email": invite.email,
                "role": invite.role.as_str(),
                "expires_at": invite.expires_at_ms,
            },
            "invite_url": link,
        }))
        .into_response(),
    ))
}

async fn ws_member_patch(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path((pid, account_pid)): axum::extract::Path<(uuid::Uuid, uuid::Uuid)>,
    parts: Parts,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let role = req
        .get("role")
        .and_then(|v| v.as_str())
        .and_then(Role::parse)
        .filter(|r| *r != Role::Owner);
    let Some(role) = role else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "role must be admin, member or viewer",
        ));
    };
    let store = accounts(&app)?;
    let Some(acc) = store
        .account_by_public_id(account_pid)
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such member",
        ));
    };
    match store
        .set_member_role(ws.id, acc.id, role)
        .await
        .map_err(store_err)?
    {
        durable_accounts::MemberChange::Changed => {
            app.members.bump_ws(ws.id);
            Ok(no_store(
                Json(serde_json::json!({ "account": { "public_id": account_pid }, "role": role.as_str() }))
                    .into_response(),
            ))
        }
        durable_accounts::MemberChange::NotFound => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such member",
        )),
        durable_accounts::MemberChange::LastOwner => Err(ApiError::new(
            StatusCode::CONFLICT,
            "last_owner",
            "the owner row cannot be changed",
        )),
    }
}

async fn ws_member_remove(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path((pid, account_pid)): axum::extract::Path<(uuid::Uuid, uuid::Uuid)>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let store = accounts(&app)?;
    let Some(acc) = store
        .account_by_public_id(account_pid)
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such member",
        ));
    };
    match store
        .remove_member(ws.id, acc.id)
        .await
        .map_err(store_err)?
    {
        durable_accounts::MemberChange::Changed => {
            app.members.bump_ws(ws.id);
            Ok(no_store(StatusCode::NO_CONTENT.into_response()))
        }
        durable_accounts::MemberChange::NotFound => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such member",
        )),
        durable_accounts::MemberChange::LastOwner => Err(ApiError::new(
            StatusCode::CONFLICT,
            "last_owner",
            "the owner row cannot be removed",
        )),
    }
}

async fn ws_invites(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Viewer).await?;
    let rows = accounts(&app)?
        .list_invites(ws.id)
        .await
        .map_err(store_err)?;
    let list: Vec<serde_json::Value> = rows
        .iter()
        .map(|i| {
            serde_json::json!({
                "public_id": i.public_id,
                "email": i.email,
                "role": i.role.as_str(),
                "expires_at": i.expires_at_ms,
            })
        })
        .collect();
    Ok(no_store(
        Json(serde_json::json!({ "invites": list })).into_response(),
    ))
}

async fn ws_invite_revoke(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path((pid, iid)): axum::extract::Path<(uuid::Uuid, uuid::Uuid)>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    if !accounts(&app)?
        .revoke_invite(ws.id, iid)
        .await
        .map_err(store_err)?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such invite",
        ));
    }
    Ok(no_store(StatusCode::NO_CONTENT.into_response()))
}

// ---- S3-5: member-facing API keys -----------------------------------

/// A key as the member API shows it: `public_id` is the only id (the
/// sequential `id` never leaves the process), the secret never appears —
/// only its stored prefix.
fn key_json(k: &ApiKeyInfo) -> serde_json::Value {
    serde_json::json!({
        "public_id": k.public_id,
        "name": k.name,
        "prefix": k.prefix,
        "permission": k.permission.api_str(),
        "revoked": k.revoked,
        "last_used_at_ms": k.last_used_at_ms,
        "created_by": k.created_by,
    })
}

/// `GET /workspaces/{pid}/keys` — every member may list; the response
/// carries prefix + metadata only, never secret material.
async fn ws_keys(
    SessionAuth(grant): SessionAuth,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Viewer).await?;
    let keys = accounts(&app)?.list_keys(ws.id).await.map_err(store_err)?;
    let keys: Vec<serde_json::Value> = keys.iter().map(key_json).collect();
    Ok(no_store(
        Json(serde_json::json!({ "keys": keys })).into_response(),
    ))
}

#[derive(Deserialize)]
struct WsKeyReq {
    name: String,
    /// `write` (submit + cancel + read) or `read`.
    permission: String,
}

fn key_permission(raw: &str) -> Result<Permission, ApiError> {
    match raw {
        "admin" | "write" => Ok(Permission::Write),
        "read" => Ok(Permission::Read),
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            format!("permission must be write or read, not {other:?}"),
        )),
    }
}

/// `POST /workspaces/{pid}/keys` — admin+; mints a `drk_` secret shown
/// exactly once in this response.
async fn ws_key_create(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path(pid): axum::extract::Path<uuid::Uuid>,
    parts: Parts,
    J(req): J<WsKeyReq>,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let permission = key_permission(&req.permission)?;
    if req.name.trim().is_empty() || req.name.len() > 128 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "name must be 1..=128 characters",
        ));
    }
    let secret = new_key_secret();
    if secret.is_empty() {
        return Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "could not draw a key secret",
        ));
    }
    let prefix = &secret[..secret.len().min(11)];
    let info = accounts(&app)?
        .create_key(
            ws.id,
            req.name.trim(),
            permission,
            &sha256(secret.as_bytes()),
            prefix,
            Some(grant.account.id),
        )
        .await
        .map_err(store_err)?;
    let mut out = key_json(&info);
    out["key"] = serde_json::Value::String(secret);
    Ok(no_store(Json(out).into_response()))
}

/// `DELETE /workspaces/{pid}/keys/{key_pid}` — admin+; a foreign or
/// already-dead key answers 404 and the store row stays untouched.
async fn ws_key_delete(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path((pid, key_pid)): axum::extract::Path<(uuid::Uuid, uuid::Uuid)>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let store = accounts(&app)?;
    let Some(info) = store
        .key_by_public_id(ws.id, key_pid)
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such key",
        ));
    };
    let Some(hash) = store.revoke_key(ws.id, info.id).await.map_err(store_err)? else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such key",
        ));
    };
    app.keyring.invalidate(hash);
    Ok(no_store(StatusCode::NO_CONTENT.into_response()))
}

/// `POST /workspaces/{pid}/keys/{key_pid}/rotate` — admin+; the store
/// retires the old key and mints its replacement (same name + scope) in
/// one transaction, then the old hash is invalidated write-through so
/// it 401s on the very next call.
async fn ws_key_rotate(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    axum::extract::Path((pid, key_pid)): axum::extract::Path<(uuid::Uuid, uuid::Uuid)>,
    parts: Parts,
) -> Result<Response, ApiError> {
    let (ws, _) = member_access(&app, &grant, pid, &parts, Role::Admin).await?;
    let secret = new_key_secret();
    if secret.is_empty() {
        return Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "could not draw a key secret",
        ));
    }
    let prefix = &secret[..secret.len().min(11)];
    let store = accounts(&app)?;
    let Some(info) = store
        .key_by_public_id(ws.id, key_pid)
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such key",
        ));
    };
    let Some((new_info, old_hash)) = store
        .rotate_key(
            ws.id,
            info.id,
            &sha256(secret.as_bytes()),
            prefix,
            Some(grant.account.id),
        )
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such key",
        ));
    };
    app.keyring.invalidate(old_hash);
    let mut out = key_json(&new_info);
    out["key"] = serde_json::Value::String(secret);
    Ok(no_store(Json(out).into_response()))
}

/// `POST /invites/accept` — any verified session whose email matches
/// the invite's bound address; a mismatch is 403 `invite_email_mismatch`
/// before the token is consumed (the row is only revealed to the
/// address it was mailed to).
async fn invite_accept(
    SessionWrite(grant): SessionWrite,
    State(app): State<App>,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    if !grant.account.email_verified {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "email_unverified",
            "verify the account's email first",
        ));
    }
    let token = req
        .get("token")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !token.starts_with(INVITE_PREFIX) {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such invite",
        ));
    }
    let store = accounts(&app)?;
    let hash = sha256(token.as_bytes());
    let Some(live) = store
        .invite_grant(&hash, app.now_ms())
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such invite",
        ));
    };
    if !live.email.eq_ignore_ascii_case(&grant.account.email) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "invite_email_mismatch",
            "this invite was mailed to a different address",
        ));
    }
    let Some((ws, role)) = store
        .accept_invite(&hash, grant.account.id, app.now_ms())
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such invite",
        ));
    };
    app.members.bump_ws(ws);
    let ws = store
        .workspace_by_id(ws)
        .await
        .map_err(store_err)?
        .unwrap_or_else(|| unreachable!("an invite's workspace exists"));
    Ok(no_store(
        Json(serde_json::json!({
            "workspace": {
                "public_id": ws.public_id,
                "name": ws.name,
                "role": role.as_str(),
            }
        }))
        .into_response(),
    ))
}

async fn invite_decline(
    SessionWrite(_grant): SessionWrite,
    State(app): State<App>,
    J(req): J<serde_json::Value>,
) -> Result<Response, ApiError> {
    let token = req
        .get("token")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !token.starts_with(INVITE_PREFIX) {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such invite",
        ));
    }
    let store = accounts(&app)?;
    store
        .decline_invite(&sha256(token.as_bytes()), app.now_ms())
        .await
        .map_err(store_err)?;
    Ok(no_store(StatusCode::NO_CONTENT.into_response()))
}

/// `Cache-Control: no-store` + `Vary: Cookie` on every session-surface
/// response (review #18) — shared caches can never serve one tenant's
/// body to another.
fn no_store(mut res: Response) -> Response {
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    res.headers_mut()
        .append(header::VARY, header::HeaderValue::from_static("Cookie"));
    res
}

impl FromRequestParts<App> for Authorized {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let Some(key) = bearer(parts) else {
            return Err(unauthorized());
        };
        // ATTACK(auth)
        match verify_key(app, parts, key).await {
            Verified::Granted(auth)
                if auth.may_write() && !matches!(auth, Principal::Member { .. }) =>
            {
                Ok(Authorized(auth))
            }
            Verified::Granted(_) => Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "this key may only read (DURABLE_READ_KEY); this route needs DURABLE_API_KEY",
            )),
            Verified::Denied => Err(unauthorized()),
            Verified::Unavailable => Err(accounts_unavailable()),
        }
    }
}

/// Proof that the request may read: any verified key, or the server runs
/// with `--open-reads` (loopback only, resolving to workspace 0). Taken
/// first by every handler that shows runs (D-037) — the workspace in
/// `auth.ws()` scopes what the read may see.
struct ReadAuthorized(Auth);

impl FromRequestParts<App> for ReadAuthorized {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        // Open reads answer only requests addressed to a loopback name: a
        // page in the operator's browser can rebind its own name to
        // 127.0.0.1, and its requests then carry that name as Host (D-047).
        if app.open_reads && loopback_host(parts) {
            return Ok(ReadAuthorized(Principal::OpsRead));
        }
        match bearer(parts) {
            // ATTACK(auth)
            Some(key) => match verify_key(app, parts, key).await {
                Verified::Granted(auth) => Ok(ReadAuthorized(auth)),
                Verified::Denied => Err(unauthorized()),
                Verified::Unavailable => Err(accounts_unavailable()),
            },
            _ => Err(unauthorized()),
        }
    }
}

/// `Query` whose rejection is the JSON error envelope.
struct Q<T>(T);

impl<T: serde::de::DeserializeOwned, S: Send + Sync> FromRequestParts<S> for Q<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(v)| Q(v))
            .map_err(|r| ApiError::new(StatusCode::BAD_REQUEST, "bad_query", r.body_text()))
    }
}

/// `Path` whose rejection is the JSON error envelope.
struct P<T>(T);

impl<T: serde::de::DeserializeOwned + Send, S: Send + Sync> FromRequestParts<S> for P<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        AxPath::<T>::from_request_parts(parts, state)
            .await
            .map(|AxPath(v)| P(v))
            .map_err(|r| ApiError::new(StatusCode::BAD_REQUEST, "bad_path", r.body_text()))
    }
}

/// `Json` whose rejection is the JSON error envelope (keeping axum's
/// status: 400 malformed, 413 too large, 415 wrong type, 422 wrong shape).
struct J<T>(T);

impl<T: serde::de::DeserializeOwned, S: Send + Sync> axum::extract::FromRequest<S> for J<T> {
    type Rejection = ApiError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(J(v)),
            // The body did not arrive within its deadline (D-045): a slow
            // client, not a malformed request.
            Err(r) if caused_by_body_deadline(&r) => Err(ApiError::new(
                StatusCode::REQUEST_TIMEOUT,
                "body_timeout",
                "the request body did not arrive in time",
            )),
            Err(r) => {
                let status = r.status();
                let code = match status {
                    StatusCode::PAYLOAD_TOO_LARGE => "body_too_large",
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => "bad_content_type",
                    _ => "bad_body",
                };
                Err(ApiError::new(status, code, r.body_text()))
            }
        }
    }
}

/// Whether `e` (or anything it wraps) is the body deadline's timeout.
fn caused_by_body_deadline(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.is::<tower_http::timeout::TimeoutError>() {
            return true;
        }
        cur = err.source();
    }
    false
}

#[derive(Deserialize)]
struct SubmitReq {
    op_key: String,
    spec: JobSpecWire,
    not_before: Option<Millis>,
}

#[derive(Deserialize)]
struct JobSpecWire {
    version: u32,
    steps: Vec<StepSpec>,
    /// Base64 input — JSON can't carry raw bytes.
    input_b64: String,
}

/// The one error envelope: `{"error": <safe message>, "code": <stable
/// code>}`. Clients branch on `code`; internal causes are logged, never
/// returned.
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retry_after: Option<u64>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retry_after: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message, "code": self.code }));
        let mut res = (self.status, body).into_response();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            // A refusal the server chooses carries Retry-After (S2 N4).
            res.headers_mut()
                .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
        }
        if self.status == StatusCode::UNAUTHORIZED {
            res.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                header::HeaderValue::from_static("Bearer"),
            );
        }
        if let Some(secs) = self.retry_after {
            res.headers_mut().insert(
                header::RETRY_AFTER,
                header::HeaderValue::try_from(secs.max(1).to_string())
                    .unwrap_or_else(|_| header::HeaderValue::from_static("1")),
            );
        }
        res
    }
}

impl From<EngineError> for ApiError {
    fn from(e: EngineError) -> Self {
        use durable_domain::Refusal;
        match &e {
            EngineError::Refused(Refusal::ReserveExhausted) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "the server is at capacity; retry with the same op_key",
            ),
            EngineError::Refused(Refusal::AtCapacity) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "at_capacity",
                "the runner holds as many runs as it is configured to (--max-live-runs, \
                 --max-index-bytes); finished runs free their input at once, and their small \
                 dedup records leave when a compaction runs after seven days; retry later with \
                 the same op_key",
            ),
            EngineError::Refused(Refusal::WorkspaceQuota) => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "workspace_quota",
                "this workspace's share of the runner's bounds is full \
                 (--max-live-runs-per-ws, --max-index-bytes-per-ws); other \
                 workspaces still admit — free this workspace's runs or \
                 raise its share",
            ),
            EngineError::Refused(Refusal::OverLimits) => Self::new(
                StatusCode::CONFLICT,
                "over_limits",
                "job spec over limits (op_key 1-256 bytes without control characters, \
                 names 1-128 bytes, input 64 KiB, at most 64 steps)",
            ),
            EngineError::Refused(r) => Self::new(StatusCode::CONFLICT, "refused", r.to_string()),
            EngineError::UnregisteredHandler(_) => {
                Self::new(StatusCode::CONFLICT, "unregistered_handler", e.to_string())
            }
            EngineError::RegistryFull => Self::new(
                StatusCode::CONFLICT,
                "registry_full",
                "the workspace's handler registry is at its bound",
            ),
            EngineError::Stopped => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "engine_stopped",
                "the engine has stopped; the server is restarting",
            ),
            // Refused before anything was written, or a group commit that
            // failed after: either way nothing was acknowledged.
            EngineError::Journal(j) => Self::journal(j, &e),
            EngineError::CommitFailed(j) => Self::journal(j, &e),
            EngineError::CompactionRunning => Self::new(
                StatusCode::CONFLICT,
                "compaction_running",
                "a compaction is already running; wait for it to finish",
            ),
            EngineError::SnapshotOverBudget { .. } => {
                Self::new(StatusCode::CONFLICT, "snapshot_over_budget", e.to_string())
            }
            EngineError::Timeout => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "timeout",
                "the engine did not answer in time; a submit may still commit, so retry with the \
                 same op_key",
            ),
            EngineError::Serde(_)
            | EngineError::Piped
            | EngineError::Config(_)
            | EngineError::WorkspaceMismatch { .. } => Self::internal(&e),
        }
    }
}

impl ApiError {
    fn journal(j: &durable_journal::JournalError, e: &EngineError) -> Self {
        use durable_journal::JournalError as J;
        match j {
            J::NoSpace { .. } => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "no_space",
                "free disk space is below the reserve; retry later with the same op_key",
            ),
            // A real ENOSPC after the reserve check (compaction, backup).
            J::Io(source) if source.kind() == std::io::ErrorKind::StorageFull => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "no_space",
                "the disk filled during the operation; nothing was committed",
            ),
            J::SpaceProbe(_) => {
                eprintln!("durable-server: free-space probe failed: {e}");
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    "free disk space could not be read; nothing was written, retry later",
                )
            }
            J::WriteFailed { source, .. } if source.kind() == std::io::ErrorKind::StorageFull => {
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no_space",
                    "the disk filled during the write; nothing was committed",
                )
            }
            J::WriteFailed { .. }
            | J::SyncFailed(_)
            | J::Poisoned
            | J::ManifestSwapFailed(_)
            | J::ManifestMismatch { .. } => {
                eprintln!("durable-server: journal unavailable: {e}");
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "journal_unavailable",
                    "the journal could not commit; nothing was acknowledged, retry with the same op_key",
                )
            }
            _ => Self::internal(e),
        }
    }

    fn internal(e: &EngineError) -> Self {
        eprintln!("durable-server: internal error: {e}");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    }
}

async fn submit(
    parts: Parts,
    State(app): State<App>,
    J(req): J<SubmitReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(flat_access(&app, &parts, Permission::Write, Role::Member, true).await?);
    let input = base64_decode(&req.spec.input_b64)
        .map_err(|m| ApiError::new(StatusCode::BAD_REQUEST, "bad_input", m))?;
    let spec = JobSpec {
        version: req.spec.version,
        steps: req.spec.steps,
        input,
    };
    let key = OperationKey(req.op_key);
    let submitted = match app
        .engine
        .submit_in(auth.ws(), key, spec, req.not_before)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            if admission_refusal(&e) {
                app.obs.refused(Some(auth.ws()), observe::wall_ms());
            }
            return Err(e.into());
        }
    };
    if submitted.deduplicated {
        app.obs.deduplicated(auth.ws(), observe::wall_ms());
    }
    // The id leaves only in its opaque form (S2 #7).
    Ok(Json(
        serde_json::json!({ "run_id": app.pubid.encode(submitted.run) }),
    ))
}

/// A submit refused at admission — what `/metrics` counts as `refused`
/// with the 429s: at capacity, the command queue full (busy), over the
/// limits, an unregistered handler, or below the free-space reserve. A
/// malformed request, a missing key, or a timeout (the submit may still
/// commit) is not one.
fn admission_refusal(e: &EngineError) -> bool {
    use durable_domain::Refusal;
    matches!(
        e,
        EngineError::Refused(
            Refusal::AtCapacity
                | Refusal::ReserveExhausted
                | Refusal::OverLimits
                | Refusal::WorkspaceQuota
        ) | EngineError::UnregisteredHandler(_)
            | EngineError::Journal(durable_journal::JournalError::NoSpace { .. })
    )
}

/// Whether a request is a submit (a 429 for it counts as `refused`).
fn is_submit<B>(req: &hyper::Request<B>) -> bool {
    req.method() == axum::http::Method::POST && req.uri().path() == "/jobs"
}

/// The value of query parameter `name`: `Ok(None)` when absent, `Err`
/// when it is given more than once.
fn single_param<'a>(pairs: &'a [(String, String)], name: &str) -> Result<Option<&'a str>, ()> {
    let mut found = pairs
        .iter()
        .filter(|(k, _)| k == name)
        .map(|(_, v)| v.as_str());
    match (found.next(), found.next()) {
        (v, None) => Ok(v),
        (Some(_), Some(_)) => Err(()),
        (None, Some(_)) => Err(()),
    }
}

/// The metrics a key may see (D-060): a workspace-0 key (the operator's
/// env keys, the loopback `--open-reads`) sees every workspace; any other
/// key sees its own workspace only — its counters, latencies, gauges,
/// states and handlers, never another tenant's or the totals.
fn metrics_scope(ws: u64) -> observe::Scope {
    // ATTACK(obs-metrics-scope)
    if ws == 0 {
        observe::Scope::All
    } else {
        observe::Scope::Ws(ws)
    }
}

/// `GET /metrics?window=15m|1h|6h|24h` (D-058): the metrics ring for the
/// window, totals, current state counts and per-handler stats — for the
/// caller's scope ([`metrics_scope`]).
async fn metrics(
    parts: Parts,
    State(app): State<App>,
    Q(pairs): Q<Vec<(String, String)>>,
) -> Result<Json<observe::MetricsOut>, ApiError> {
    let auth = FlatWs(tenant_read(&app, &parts).await?);
    let invalid = || {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_window",
            "window must be one of 15m, 1h, 6h, 24h",
        )
    };
    let window = match single_param(&pairs, "window").map_err(|()| invalid())? {
        None => Window::H1,
        Some(w) => Window::parse(w).ok_or_else(invalid)?,
    };
    let scope = metrics_scope(auth.ws());
    let index = app.engine.metrics_index(scope).await?;
    Ok(Json(app.obs.metrics(
        scope,
        window,
        observe::wall_ms(),
        index.counts,
        &index.handlers,
    )))
}

/// `GET /jobs/{id}/events` (D-058): the run's timeline. 404 exactly when
/// `GET /jobs/{id}` is — another workspace's run is 404 (D-060).
async fn run_events(
    parts: Parts,
    State(app): State<App>,
    P(id): P<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(tenant_read(&app, &parts).await?);
    let id = RunId(decode_id(&app, auth.ws(), &id)?);
    let run = match app.engine.summary(id).await? {
        // ATTACK(obs-run-scope)
        Some(run) if run.ws == auth.ws() => run,
        _ => return Err(not_found()),
    };
    let t = app.obs.timeline(id);
    Ok(Json(serde_json::json!({
        "run": app.pubid.encode(id),
        "key": run.key.0,
        "state": state_name(&run.state),
        "complete": t.complete,
        "reason": t.reason,
        "events": t.events,
    })))
}

/// One activity-feed entry: the timeline event plus its run (the opaque
/// public id) and key.
#[derive(Serialize)]
struct FeedItem<'a> {
    at: Millis,
    run: String,
    /// `None` only for a run that has since left the index (expired by a
    /// compaction seven days after it finished).
    key: Option<&'a str>,
    #[serde(flatten)]
    kind: observe::Kind,
}

/// `GET /events?limit=1..200` (D-058): the caller's workspace's activity
/// feed, newest first — only its own runs (D-060).
async fn feed(
    parts: Parts,
    State(app): State<App>,
    Q(pairs): Q<Vec<(String, String)>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(tenant_read(&app, &parts).await?);
    let invalid = || {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_limit",
            "limit must be an integer from 1 to 200",
        )
    };
    let limit = match single_param(&pairs, "limit").map_err(|()| invalid())? {
        None => 50,
        Some(v) => v
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=observe::MAX_FEED_PAGE).contains(n))
            .ok_or_else(invalid)?,
    };
    // ATTACK(obs-feed-scope)
    let (since, events) = app.obs.feed(auth.ws(), limit, observe::wall_ms());
    // Keys come from the index at read time: the observer holds no keys,
    // so its memory stays fixed per event (D-058).
    let mut runs: Vec<RunId> = events.iter().map(|(r, _)| *r).collect();
    runs.sort_unstable();
    runs.dedup();
    let keys = app.engine.keys(auth.ws(), runs.clone()).await?;
    let key_of = |run: RunId| {
        runs.binary_search(&run)
            .ok()
            .and_then(|i| keys.get(i))
            .and_then(|k| k.as_ref())
            .map(|k| k.0.as_str())
    };
    let items: Vec<FeedItem<'_>> = events
        .iter()
        .map(|(run, ev)| FeedItem {
            at: ev.at,
            run: app.pubid.encode(*run),
            key: key_of(*run),
            kind: ev.kind,
        })
        .collect();
    Ok(Json(serde_json::json!({
        "since": since,
        "events": items,
    })))
}

/// The observability routes answer `Cache-Control: no-store`, errors too.
async fn obs_no_store(mut res: Response) -> Response {
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    res
}

fn state_name(s: &RunState) -> String {
    match s {
        RunState::Queued { .. } => "queued".into(),
        RunState::Running { step, attempt, .. } => {
            format!("running:step:{}:attempt:{}", step.0, attempt)
        }
        RunState::RetryWaiting { step, attempt, .. } => {
            format!("retry_waiting:step:{}:attempt:{}", step.0, attempt)
        }
        RunState::Reconciling { step, .. } => format!("reconciling:step:{}", step.0),
        RunState::Done => "done".into(),
        RunState::Failed { step } => format!("failed:step:{}", step.0),
        RunState::Cancelled => "cancelled".into(),
    }
}

/// The public id → the internal one. A malformed or forged string is
/// `None`, which answers exactly like an id that was never issued —
/// the permutation adds no new oracle (S2 #7).
fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such run")
}

fn decode_id(app: &App, ws: u64, s: &str) -> Result<u64, ApiError> {
    // Opaque first: an issued public id must always resolve to itself —
    // the permutation could in principle produce an all-digit string, and
    // a numeric-first read would misroute it to a different internal run.
    if let Some(id) = app.pubid.decode(s) {
        return Ok(id);
    }
    // v1.0.1 compat (N7): the compat workspace's legacy ids are the bare
    // internal numbers. A short digit string never decodes as an opaque
    // id (wrong byte count), so every realistic legacy id reaches here;
    // every other workspace's ids stay opaque-only.
    // ATTACK(numeric-id-ws0)
    if ws == 0 && !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s.parse::<u64>().map_err(|_| not_found());
    }
    Err(not_found())
}

/// The run if it exists AND belongs to the caller's workspace — a
/// cross-workspace id is 404, never 403: a foreign run id leaks nothing
/// (S2-2's no-oracle rule).
async fn scoped_run(app: &App, ws: u64, id: u64) -> Result<durable_domain::Run, ApiError> {
    match app.engine.inspect(RunId(id)).await? {
        // ATTACK(tenant-scope)
        Some(run) if run.ws == ws => Ok(run),
        _ => Err(not_found()),
    }
}

async fn inspect(
    parts: Parts,
    State(app): State<App>,
    P(id): P<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(tenant_read(&app, &parts).await?);
    let run = scoped_run(&app, auth.ws(), decode_id(&app, auth.ws(), &id)?).await?;
    Ok(Json(serde_json::json!({
        "id": app.pubid.encode(run.id),
        "key": run.key.0,
        "state": state_name(&run.state),
        "steps": run.spec.steps.len(),
        "finished_at": run.finished_at,
    })))
}

/// The spec a run was submitted with (its steps and input), for the API-key holder only
/// (D-051): a controller that resubmits the same work under a new op_key needs it, and
/// the input may be customer content, so the read key does not reveal it.
async fn spec(
    parts: Parts,
    State(app): State<App>,
    P(id): P<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Bearer stays write-gated (its old `Authorized`); a session
    // reads its workspace's spec at viewer+ like every other GET.
    let auth = FlatWs(flat_access(&app, &parts, Permission::Write, Role::Viewer, false).await?);
    use base64::Engine as _;
    let run = scoped_run(&app, auth.ws(), decode_id(&app, auth.ws(), &id)?).await?;
    Ok(Json(serde_json::json!({
        "id": app.pubid.encode(run.id),
        "key": run.key.0,
        "spec": {
            "version": run.spec.version,
            "steps": run.spec.steps,
            "input_b64": base64::engine::general_purpose::STANDARD.encode(&run.spec.input),
        },
    })))
}

async fn cancel(
    parts: Parts,
    State(app): State<App>,
    P(id): P<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(flat_access(&app, &parts, Permission::Write, Role::Member, true).await?);
    // The scope check happens before the cancel: a run's workspace is
    // fixed at acceptance, so inspecting first cannot race.
    let internal = decode_id(&app, auth.ws(), &id)?;
    scoped_run(&app, auth.ws(), internal).await?;
    app.engine.cancel(RunId(internal)).await?;
    Ok(Json(serde_json::json!({ "cancelled": id })))
}

#[derive(Deserialize)]
struct ListQuery {
    /// `asc` (default): oldest first, continue with `after`.
    /// `desc`: newest first, continue with `before`.
    order: Option<String>,
    /// Return runs after this public cursor (the previous `next_after`).
    after: Option<String>,
    /// Newest first: runs before this public cursor (`next_before`).
    before: Option<String>,
    /// Page size: default 100, clamped to 1..=1000.
    limit: Option<usize>,
}

async fn list(
    parts: Parts,
    State(app): State<App>,
    Q(q): Q<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = FlatWs(tenant_read(&app, &parts).await?);
    let newest = match q.order.as_deref() {
        None | Some("asc") => false,
        Some("desc") => true,
        Some(_) => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_query",
                "order must be asc or desc",
            ))
        }
    };
    // Cursors are public ids like every other boundary value (S2 #7):
    // a malformed one is a client bug — 400, never a lookup.
    let decode_cursor = |s: Option<String>| -> Result<Option<u64>, ApiError> {
        s.map(|c| decode_id(&app, auth.ws(), &c))
            .transpose()
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "bad_query", "malformed cursor"))
    };
    let order = if newest {
        Order::Newest {
            before: decode_cursor(q.before)?.map(RunId),
        }
    } else {
        Order::Oldest {
            after: decode_cursor(q.after)?.map(RunId),
        }
    };
    let page = app
        .engine
        // ATTACK(list-scope)
        .list_in(auth.ws(), order, q.limit.unwrap_or(100))
        .await?;
    let items: Vec<serde_json::Value> = page
        .runs
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": app.pubid.encode(r.id),
                "key": r.key.0,
                "state": state_name(&r.state),
                "steps": r.steps,
                "finished_at": r.finished_at,
            })
        })
        .collect();
    let cursor = if newest { "next_before" } else { "next_after" };
    let mut body = serde_json::json!({ "runs": items });
    body[cursor] = serde_json::json!(page.next.map(|id| app.pubid.encode(id)));
    Ok(Json(body))
}

/// Liveness: the process answers.
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

/// Readiness: should this server get new work now? False (503) while free
/// space is below the reserve or once the engine has stopped.
async fn ready(State(app): State<App>) -> Response {
    let r = app.engine.readiness();
    let status = if r.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({ "ready": r.ready, "code": r.reason })),
    )
        .into_response()
}

async fn diagnostics(
    Authorized(auth): Authorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let d = app.engine.diag().await?;
    let mut v = serde_json::to_value(d).unwrap_or_default();
    if let Some(o) = v.as_object_mut() {
        o.insert(
            "registry_stale".into(),
            serde_json::json!(app
                .registry_stale
                .load(std::sync::atomic::Ordering::Relaxed)),
        );
    }
    Ok(Json(v))
}

// ---- S2: the account graph (workspaces + API keys) ----------------

/// Workspace management is an ops surface (review #4c): only the env
/// ops key — a store key scoped to ws 0 can no longer reach `/admin/*`.
/// Tenant self-service is the member/role surface.
fn ops_admin(auth: &Auth) -> Result<(), ApiError> {
    // ATTACK(ops-scope)
    if !matches!(auth, Principal::OpsKey) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "this operation needs a workspace-0 admin key",
        ));
    }
    Ok(())
}

/// The tenant-facing counterpart of `/admin/diagnostics`: the calling
/// workspace's own counts — never another tenant's, never the global
/// totals (S1 review: diagnostics leaks tenant volume across the
/// boundary, so the global view is ops-only).
async fn workspace_diagnostics(
    parts: Parts,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Bearer stays write-gated (its old `Authorized`); a session sees
    // only its own workspace's counts at viewer+ — never the ops view.
    // ATTACK(diag-scope)
    let ws = flat_access(&app, &parts, Permission::Write, Role::Viewer, false).await?;
    let d = app.engine.diag_ws(ws).await?;
    Ok(Json(serde_json::to_value(d).unwrap_or_default()))
}

/// The configured account store — without one the workspace routes have
/// no graph to talk to: a 404 with the config reason, not a retryable
/// outage.
fn accounts(app: &App) -> Result<&Arc<dyn AccountStore>, ApiError> {
    app.keyring.store.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no account store configured (DATABASE_URL is unset)",
        )
    })
}

/// A store failure as an API error: `Unavailable` fails closed as
/// `accounts_unavailable`; anything else is a defect in the query or
/// schema and is 500.
fn store_err(e: durable_accounts::AccountError) -> ApiError {
    use durable_accounts::AccountError as E;
    match e {
        E::Unavailable(_) => accounts_unavailable(),
        // The store's atomic refusal — same shape the route's pre-check
        // and the engine's bound produce.
        E::RegistryFull => ApiError::new(
            StatusCode::CONFLICT,
            "registry_full",
            "the workspace's handler registry is at its bound",
        ),
        E::Store(m) => {
            eprintln!("durable-server: account store error: {m}");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal error",
            )
        }
    }
}

/// Resolve a workspace's public id to its row — the ONLY translation the
/// API does; nothing internal crosses back out (D/S2: opaque ids).
async fn workspace_by_public(
    store: &Arc<dyn AccountStore>,
    public_id: uuid::Uuid,
) -> Result<durable_accounts::Workspace, ApiError> {
    store
        .workspace_by_public_id(public_id)
        .await
        .map_err(store_err)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such workspace"))
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    const T: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(T[(b >> 4) as usize] as char);
        s.push(T[(b & 0xf) as usize] as char);
    }
    s
}

/// Parse hex back into bytes — `None` on odd length, bad chars or a
/// wrong size, so a corrupt snapshot entry degrades to "unsigned"
/// rather than panicking startup.
fn unhex(s: &str) -> Option<[u8; 32]> {
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    if b.len() != 64 {
        return None;
    }
    let (pairs, _) = b.as_chunks::<2>();
    for (i, pair) in pairs.iter().enumerate() {
        out[i] = (nib(pair[0])? << 4) | nib(pair[1])?;
    }
    Some(out)
}

/// A fresh API key: `dr_` + 48 hex chars of CSPRNG. Only its sha256 is
/// stored; the plaintext crosses exactly one boundary — this response.
fn new_key_secret() -> String {
    let mut b = [0u8; 24];
    use ring::rand::SecureRandom as _;
    // SystemRandom::fill only fails on catastrophic RNG loss; map it to a
    // zeroed buffer refusal rather than issuing a weak key.
    match ring::rand::SystemRandom::new().fill(&mut b) {
        Ok(()) => format!("drk_{}", hex(&b)),
        Err(_) => String::new(),
    }
}

/// Who the caller is, as externals see it: the workspace's public id and
/// the key's permission. No store configured (compat mode) means
/// workspace 0 — the nil UUID.
async fn whoami(
    ReadAuthorized(auth): ReadAuthorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let public_id = match &app.keyring.store {
        Some(store) => match store.workspace_by_id(auth.ws()).await {
            Ok(Some(ws)) => ws.public_id.to_string(),
            Ok(None) => uuid::Uuid::nil().to_string(),
            Err(e) => return Err(store_err(e)),
        },
        None => uuid::Uuid::nil().to_string(),
    };
    Ok(Json(serde_json::json!({
        "workspace": public_id,
        "permission": match &auth {
            Principal::OpsKey => "admin",
            Principal::OpsRead => "read",
            Principal::StoreKey { permission, .. } => permission.api_str(),
            Principal::Member { role, .. } => role.as_str(),
        },
    })))
}

async fn list_workspaces(
    Authorized(auth): Authorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let store = accounts(&app)?;
    let list = store.list_workspaces(1000, None).await.map_err(store_err)?;
    let items: Vec<serde_json::Value> = list
        .iter()
        .map(|ws| {
            serde_json::json!({
                "public_id": ws.public_id,
                "name": ws.name,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "workspaces": items })))
}

#[derive(Deserialize)]
struct WorkspaceReq {
    name: String,
}

async fn create_workspace(
    Authorized(auth): Authorized,
    State(app): State<App>,
    J(req): J<WorkspaceReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    if req.name.trim().is_empty() || req.name.len() > 128 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "name must be 1..=128 characters",
        ));
    }
    let store = accounts(&app)?;
    let ws = store
        .create_workspace(req.name.trim())
        .await
        .map_err(store_err)?;
    Ok(Json(serde_json::json!({
        "public_id": ws.public_id,
        "name": ws.name,
        // The signing secret crosses exactly one boundary — this
        // response. GETs and lists never carry it again (S2-6).
        "handler_secret": hex(&ws.handler_secret),
    })))
}

async fn get_workspace(
    Authorized(auth): Authorized,
    State(app): State<App>,
    P(public_id): P<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let store = accounts(&app)?;
    let ws = workspace_by_public(store, public_id).await?;
    Ok(Json(serde_json::json!({
        "public_id": ws.public_id,
        "name": ws.name,
    })))
}

async fn list_workspace_keys(
    Authorized(auth): Authorized,
    State(app): State<App>,
    P(public_id): P<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let store = accounts(&app)?;
    let ws = workspace_by_public(store, public_id).await?;
    let keys = store.list_keys(ws.id).await.map_err(store_err)?;
    let items: Vec<serde_json::Value> = keys
        .iter()
        .map(|k| {
            serde_json::json!({
                "id": k.id,
                "name": k.name,
                "prefix": k.prefix,
                "permission": k.permission.api_str(),
                "revoked": k.revoked,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "keys": items })))
}

#[derive(Deserialize)]
struct KeyReq {
    name: String,
    /// `admin` (submit + cancel + read) or `read`. Anything else is a
    /// spec error at the boundary.
    permission: String,
}

async fn create_workspace_key(
    Authorized(auth): Authorized,
    State(app): State<App>,
    P(public_id): P<uuid::Uuid>,
    J(req): J<KeyReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let permission = match req.permission.as_str() {
        "admin" | "write" => Permission::Write,
        "read" => Permission::Read,
        other => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_input",
                format!("permission must be admin or read, not {other:?}"),
            ))
        }
    };
    if req.name.trim().is_empty() || req.name.len() > 128 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_input",
            "name must be 1..=128 characters",
        ));
    }
    let secret = new_key_secret();
    if secret.is_empty() {
        return Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "could not draw a key secret",
        ));
    }
    let store = accounts(&app)?;
    let ws = workspace_by_public(store, public_id).await?;
    let prefix = &secret[..secret.len().min(11)];
    let info = store
        .create_key(
            ws.id,
            req.name.trim(),
            permission,
            &sha256(secret.as_bytes()),
            prefix,
            None,
        )
        .await
        .map_err(store_err)?;
    Ok(Json(serde_json::json!({
        "id": info.id,
        "name": info.name,
        "prefix": info.prefix,
        "permission": info.permission.api_str(),
        // Shown exactly once: only its sha256 is stored.
        "key": secret,
    })))
}

async fn delete_workspace_key(
    Authorized(auth): Authorized,
    State(app): State<App>,
    P((public_id, key_id)): P<(uuid::Uuid, u64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let store = accounts(&app)?;
    let ws = workspace_by_public(store, public_id).await?;
    match store.revoke_key(ws.id, key_id).await.map_err(store_err)? {
        Some(hash) => {
            app.keyring.invalidate(hash);
            Ok(Json(serde_json::json!({ "revoked": key_id })))
        }
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such key (or already revoked)",
        )),
    }
}

// ---- S2-4: per-workspace handler registrations --------------------------

/// Handler routes are tenant self-service: the caller's own workspace,
/// admin permission required (Authorized). The store is authoritative;
/// each write lands in the engine's dispatch registry and the snapshot.
async fn list_handlers(
    State(app): State<App>,
    parts: Parts,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ws = flat_access(&app, &parts, Permission::Read, Role::Viewer, false).await?;
    let store = accounts(&app)?;
    let regs = store.list_handlers(ws).await.map_err(store_err)?;
    // Health percentiles ride the ws-scoped observer ring (1 h window) —
    // the test-call columns come from the store row.
    let index = app
        .engine
        .metrics_index(observe::Scope::Ws(ws))
        .await
        .unwrap_or_else(|_| durable_engine::shell::MetricsIndex {
            counts: observe::IndexCounts::default(),
            handlers: Vec::new(),
        });
    let names: Vec<String> = regs.iter().map(|r| r.name.clone()).collect();
    let m = app.obs.metrics(
        observe::Scope::Ws(ws),
        Window::H1,
        observe::wall_ms(),
        index.counts,
        &names,
    );
    let mut handlers: Vec<serde_json::Value> = Vec::new();
    if ws == 0 {
        // Flag handlers are ws 0's too; a store row shadows them on a
        // name conflict (store rows push after, the map keeps the last).
        for (name, _) in app.flag_handlers.iter() {
            handlers.push(serde_json::json!({ "name": name, "source": "flag" }));
        }
    }
    for r in regs {
        handlers.retain(|h| h["name"] != r.name);
        let mstats = m.handlers.iter().find(|x| x.name == r.name);
        handlers.push(serde_json::json!({
            "name": r.name,
            "url": r.url,
            "health": {
                "last_test_at_ms": r.last_test_at_ms,
                "last_test_status": r.last_test_status,
                "last_test_latency_ms": r.last_test_latency_ms,
                "p50_ms": mstats.and_then(|x| x.step_ms.p50),
                "p95_ms": mstats.and_then(|x| x.step_ms.p95),
            },
        }));
    }
    Ok(Json(serde_json::json!({ "handlers": handlers })))
}

#[derive(Deserialize)]
struct PutHandlerReq {
    url: String,
}

/// Queue the write and wait for the writer's verdict — the actual
/// load/merge/fsync runs on the writer thread; the route waits for its
/// ack (bounded, no synchronous fsync on an async worker). Anything but
/// a durably-applied write is a retryable 503 — a mutation the file
/// never held would resurrect on restart while the client believed it
/// landed (re-review #8: the old code logged and returned 200).
async fn registry_write(
    app: &App,
    op: impl FnOnce(tokio::sync::oneshot::Sender<Result<()>>) -> RegistryWrite,
) -> Result<(), ApiError> {
    let (ack, rx) = tokio::sync::oneshot::channel();
    if app.registry_tx.try_send(op(ack)).is_err() {
        eprintln!(
            "durable-server: registry writer queue full; {:?} converges at the next reconcile",
            app.registry_file
        );
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "the registry writer's queue is full; retry",
        ));
    }
    match tokio::time::timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(_))) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "registry_write_failed",
            "the registry snapshot write failed; retry",
        )),
        _ => {
            eprintln!("durable-server: registry writer did not ack within 10 s");
            Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "the registry writer did not settle in 10 s; retry",
            ))
        }
    }
}

async fn registry_file_upsert(app: &App, entry: RegistryEntry) -> Result<(), ApiError> {
    registry_write(app, |ack| RegistryWrite::Upsert { entry, ack }).await
}

async fn registry_file_remove(app: &App, ws: u64, name: &str) -> Result<(), ApiError> {
    let name = name.to_string();
    registry_write(app, |ack| RegistryWrite::Remove { ws, name, ack }).await
}

async fn registry_file_replace_ws(
    app: &App,
    ws: u64,
    entries: Vec<RegistryEntry>,
) -> Result<(), ApiError> {
    registry_write(app, |ack| RegistryWrite::ReplaceWs { ws, entries, ack }).await
}

async fn put_handler(
    State(app): State<App>,
    parts: Parts,
    P(name): P<String>,
    Json(body): Json<PutHandlerReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ws_id = flat_access(&app, &parts, Permission::Write, Role::Member, true).await?;
    let auth = FlatWs(ws_id);
    let store = accounts(&app)?;
    let name = name.trim();
    if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_name",
            "handler name must be 1-128 chars with no control characters",
        ));
    }
    // ATTACK(tenant-register-policy)
    let url = if name == BUILTIN_ECHO {
        let u = body.url.trim();
        if !u.is_empty() && u != BUILTIN_ECHO {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_handler_url",
                "builtin:echo registers with url \"builtin:echo\" or an empty url",
            ));
        }
        BUILTIN_ECHO.to_string()
    } else {
        valid_handler_url(body.url.trim(), &app.handler_factory.store_policy)?
    };
    registry_rate(&app, auth.ws())?;
    // Finding 4: the bound check, the store write, the engine update and
    // the snapshot send are one critical section per workspace — a
    // reconcile's list+apply cannot interleave between them.
    let _ws_guard = app.registry_locks.stripe(auth.ws()).lock().await;
    // Re-review S2-4: exactly what is armed is a no-op — no store query,
    // no epoch bump, no engine update, no file write.
    // ATTACK(registry-noop-skip)
    if app.registry.armed(auth.ws(), name).as_deref() == Some(url.as_str()) {
        return Ok(Json(
            serde_json::json!({ "name": name, "url": url, "unchanged": true }),
        ));
    }
    // S3 re-review 2: the view claims only what the engine armed AND the
    // file holds. Drop the entry before anything changes; it comes back
    // only once the writer acks the save — so a PUT that fails anywhere
    // below (store, engine, file) leaves nothing for a retry to skip on.
    // ATTACK(registry-view-invalidate)
    app.registry.remove(auth.ws(), name);
    // The 256 bound lives in the engine, but the store must never hold a
    // row the engine refuses: one over-limit row would poison every
    // later reconcile of this workspace's set. Check before writing —
    // a concurrent PUT can still race past it, which the engine's own
    // bound catches and a DELETE or the next reconcile heals.
    let regs = store.list_handlers(auth.ws()).await.map_err(store_err)?;
    if !regs.iter().any(|r| r.name == name) && regs.len() >= MAX_HANDLERS_PER_WS {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "registry_full",
            "the workspace's handler registry is at its bound",
        ));
    }
    // Order is store → engine → snapshot: the store is the authority, so
    // a row it accepted is never silently dropped. If the engine call
    // fails the row stays and the next reconcile applies it; the client
    // gets the engine's error and can retry or DELETE. The epoch bumps
    // BEFORE the store write (S3): a reconcile pass overlapping this
    // mutation always sees the epoch move — bumping after the write
    // would leave a window where a stale pass still reads the old epoch.
    app.registry_epoch
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    store
        .put_handler(auth.ws(), name, &url)
        .await
        .map_err(store_err)?;
    let secret = store
        .workspace_by_id(auth.ws())
        .await
        .map_err(store_err)?
        .map(|w| Arc::new(w.handler_secret));
    // S3: a missing workspace row must not arm the handler *unsigned* —
    // the row stays in the store (the reconcile re-arms it if the
    // workspace heals), but this process refuses to dispatch unsigned.
    if auth.ws() != 0 && secret.is_none() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "registry_unavailable",
            "the workspace's signing secret is unavailable; the handler is stored but not armed",
        ));
    }
    app.engine
        .put_handler(
            auth.ws(),
            name,
            arm_url(&app.handler_factory, &url, secret.clone()),
        )
        .await?;
    let saved = registry_file_upsert(
        &app,
        RegistryEntry {
            ws: auth.ws(),
            name: name.to_string(),
            url: url.clone(),
            secret: secret.as_deref().map(|s| hex(s)),
        },
    )
    .await;
    // ATTACK(registry-set-after-save)
    if saved.is_ok() {
        app.registry.set(auth.ws(), name, &url);
    }
    saved?;
    Ok(Json(serde_json::json!({ "name": name, "url": url })))
}

/// `POST /handler-secret` — draw a fresh signing secret for the caller's
/// workspace (S2-6). The old secret dies here; every registration is
/// rebuilt signed with the new one (store → engine → snapshot), so a
/// rotate takes effect without waiting for the reconciler. The secret
/// is returned exactly once — verification needs it on the handler side.
async fn rotate_handler_secret(
    State(app): State<App>,
    parts: Parts,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ws_id = flat_admin_access(&app, &parts).await?;
    let auth = FlatWs(ws_id);
    let store = accounts(&app)?;
    registry_rate(&app, auth.ws())?;
    // Bump before the store write — same epoch ordering as PUT (S3).
    app.registry_epoch
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    // Finding 4: rotate + re-arm + snapshot under the ws stripe, same
    // section as PUT/DELETE and the reconciler's list+apply.
    let _ws_guard = app.registry_locks.stripe(auth.ws()).lock().await;
    // Same rule as PUT: nothing claims armed until the new set is saved
    // (a failed save left the view whole, so a retried PUT skipped and
    // the file kept the dead secret).
    app.registry.replace_ws(auth.ws(), std::iter::empty());
    let Some(secret) = store
        .rotate_handler_secret(auth.ws())
        .await
        .map_err(store_err)?
    else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such workspace",
        ));
    };
    // Rebuild this workspace's engine set signed with the new secret —
    // the same shape as a reconcile pass for one workspace.
    let regs = store.list_handlers(auth.ws()).await.map_err(store_err)?;
    let shared = Some(Arc::new(secret));
    let mut set: Vec<(String, Arc<dyn StepHandler>)> = if auth.ws() == 0 {
        app.flag_handlers.to_vec()
    } else {
        Vec::new()
    };
    let mut entries = Vec::with_capacity(regs.len());
    for r in &regs {
        set.push((
            r.name.clone(),
            arm_url(&app.handler_factory, &r.url, shared.clone()),
        ));
        entries.push(RegistryEntry {
            ws: auth.ws(),
            name: r.name.clone(),
            url: r.url.clone(),
            secret: Some(hex(&secret)),
        });
    }
    app.engine.set_workspace_handlers(auth.ws(), set).await?;
    let view: Vec<(String, String)> = entries
        .iter()
        .map(|e| (e.name.clone(), e.url.clone()))
        .collect();
    let saved = registry_file_replace_ws(&app, auth.ws(), entries).await;
    // ATTACK(rotate-set-after-save)
    if saved.is_ok() {
        app.registry.replace_ws(auth.ws(), view);
    }
    saved?;
    Ok(Json(serde_json::json!({ "secret": hex(&secret) })))
}

async fn delete_handler(
    State(app): State<App>,
    parts: Parts,
    P(name): P<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ws_id = flat_access(&app, &parts, Permission::Write, Role::Member, true).await?;
    let auth = FlatWs(ws_id);
    let store = accounts(&app)?;
    registry_rate(&app, auth.ws())?;
    // Bump before the store write — same epoch ordering as PUT (S3).
    app.registry_epoch
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    // Finding 4: the delete holds the ws stripe across store + engine +
    // file, so a reconcile pass can never be mid-list for this row.
    let _ws_guard = app.registry_locks.stripe(auth.ws()).lock().await;
    // Forget FIRST — PUT's "drop before anything changes" rule (S3
    // re-review 2, S2-3): a view kept past a store 404, an engine refusal
    // or a failed save made the next PUT of that URL answer `unchanged`
    // over a row the store no longer holds. A missing entry only ever
    // costs a PUT its no-op skip.
    // ATTACK(registry-delete-forgets)
    app.registry.remove(auth.ws(), &name);
    let existed = store
        .delete_handler(auth.ws(), &name)
        .await
        .map_err(store_err)?;
    if !existed {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such handler registration",
        ));
    }
    app.engine.delete_handler(auth.ws(), &name)?;
    registry_file_remove(&app, auth.ws(), &name).await?;
    Ok(Json(serde_json::json!({ "deleted": name })))
}

#[derive(Deserialize)]
struct TestCallReq {
    input_b64: String,
}

/// The D-070 probe, shared by the flat and member routes: ONE call
/// through the armed handler's own test path — the signed, SSRF-guarded
/// HTTP request for an HTTP handler, `execute` in-process for
/// `builtin:echo`, an honest `in_process` for anything else. Never
/// journaled, never a run. Budgets: 30/hour per workspace (Free's
/// bound; S3-4 reads the plan) + 10/minute per client address; the
/// second strike 429s.
async fn test_call_run(
    app: &App,
    ws: u64,
    name: &str,
    addr: std::net::IpAddr,
    body: &[u8],
) -> Result<Json<serde_json::Value>, ApiError> {
    let req: TestCallReq = serde_json::from_slice(body).map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "bad_json",
            "malformed JSON",
        )
    })?;
    let input = base64_decode(&req.input_b64)
        .map_err(|e| ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "bad_input", e))?;
    if input.len() > 8 * 1024 {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "input_too_large",
            "test input exceeds 8 KiB",
        ));
    }
    {
        let mut per = app.test_calls_ws.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some(q) = per.get_mut(&ws) {
            while q
                .front()
                .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(3600))
            {
                q.pop_front();
            }
        }
        if per.get(&ws).is_some_and(|q| q.len() >= 30) {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "test_call_limited",
                "test calls are bounded to 30 per workspace per hour",
            ));
        }
        if per.len() >= 10_000 && !per.contains_key(&ws) {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "test_call_limited",
                "the probe budget table is full; retry",
            ));
        }
        per.entry(ws).or_default().push_back(now);
    }
    if let Err(wait) = app
        .test_calls_addr
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .check(addr, Instant::now())
    {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "test_call_limited",
            format!(
                "test calls are bounded to 10 per client per minute; retry in {}s",
                wait.as_secs().max(1)
            ),
        ));
    }
    let Some(handler) = app.engine.handler(ws, name).await else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such handler in this workspace",
        ));
    };
    let step = StepSpec {
        name: "test".to_string(),
        handler: name.to_string(),
    };
    let idem = format!("probe/{ws}/{name}/{}", observe::wall_ms());
    let reply = handler.test_call(&step, &input, &idem).await;
    // The row's test columns update when the registry knows the name —
    // flag-registered names have no store row to write.
    if let Ok(store) = accounts(app) {
        let _ = store
            .record_handler_test(
                ws,
                name,
                observe::wall_ms() as i64,
                reply.status,
                reply.latency_ms as i64,
            )
            .await;
    }
    Ok(Json(serde_json::json!({
        "status": reply.status,
        "http_status": reply.http_status,
        "latency_ms": reply.latency_ms,
        "reply_excerpt_b64": base64::engine::general_purpose::STANDARD.encode(&reply.excerpt),
    })))
}

/// `POST /handlers/{name}/test` — flat: ops key, ws key, or a session
/// member of the `x-durable-workspace` workspace.
async fn test_call_flat(
    State(app): State<App>,
    axum::extract::Path(name): axum::extract::Path<String>,
    parts: Parts,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ws = flat_access(&app, &parts, Permission::Write, Role::Member, true).await?;
    test_call_run(&app, ws, &name, client(&parts), &body).await
}

async fn compact(
    Authorized(auth): Authorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let gen = app.engine.compact().await?;
    Ok(Json(serde_json::json!({ "generation": gen })))
}

#[derive(Deserialize)]
struct BackupReq {
    /// A plain directory name under `--backup-root` — never a path.
    name: String,
}

/// Resolve a backup name under the configured root. Only `[A-Za-z0-9_.-]`,
/// at most 64 characters, not starting with `.` — so no separators, no
/// `..`, no absolute paths, nothing that can leave the root.
fn backup_dest(root: &Path, name: &str) -> Result<PathBuf, ApiError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if !valid {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_backup_name",
            "backup name must be 1-64 of [A-Za-z0-9_.-], not starting with '.'",
        ));
    }
    Ok(root.join(name))
}

async fn backup(
    Authorized(auth): Authorized,
    State(app): State<App>,
    J(req): J<BackupReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    ops_admin(&auth)?;
    let Some(root) = app.backup_root.as_deref() else {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "backups_disabled",
            "backups are disabled: start the server with --backup-root",
        ));
    };
    let dest = backup_dest(root, &req.name)?;
    match app.engine.backup_to(dest).await {
        Ok(()) => Ok(Json(serde_json::json!({ "backup": req.name }))),
        Err(EngineError::Journal(durable_journal::JournalError::BackupExists)) => {
            Err(ApiError::new(
                StatusCode::CONFLICT,
                "backup_exists",
                "a backup with that name already exists",
            ))
        }
        Err(e) => Err(e.into()),
    }
}

/// Canonical, padded standard base64 (RFC 4648 §4); anything else is
/// refused with one stable message, not the decoder's own text (which
/// changes between versions).
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| "input_b64 must be canonical standard base64 (RFC 4648, padded)".to_string())
}

/// Every route. `/health`, `/ready` and the dashboard's static files are
/// open (they show no runs). `GET /jobs` and `GET /jobs/{id}` take
/// `ReadAuthorized` (either key, or `--open-reads`); every mutating and
/// admin handler takes `Authorized`, so it is unreachable without the API
/// key.
fn router(app: App, ui_dir: Option<&Path>) -> Router {
    // Read-only observability (D-058): `ReadAuthorized`, never cached.
    let observability = Router::new()
        .route("/metrics", get(metrics))
        .route("/events", get(feed))
        .route("/jobs/{id}/events", get(run_events))
        .layer(axum::middleware::map_response(obs_no_store));
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/jobs", post(submit).get(list))
        .route("/jobs/{id}", get(inspect))
        .route("/jobs/{id}/spec", get(spec))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/auth/signup", post(auth_signup))
        .route("/auth/login", post(auth_login))
        .route("/auth/logout", post(auth_logout))
        .route("/auth/session", get(auth_session))
        .route("/auth/verify", post(auth_verify))
        .route("/auth/resend-verification", post(auth_resend_verification))
        .route("/auth/forgot", post(auth_forgot))
        .route("/auth/reset", post(auth_reset))
        .route("/me", get(me_account))
        .route("/me/password", post(me_password))
        .route("/me/sessions", get(me_sessions).delete(me_revoke_all))
        .route("/me/sessions/{public_id}", delete(me_revoke_one))
        // S3-3: the tenant control plane. Path pid is the only workspace
        // authority on these routes; an x-durable-workspace header that
        // disagrees is a 400 (D-066). Members join by invite only.
        .route("/workspaces", get(ws_list_mine).post(ws_create))
        .route(
            "/workspaces/{pid}",
            get(ws_get).patch(ws_patch).delete(ws_delete),
        )
        .route(
            "/workspaces/{pid}/members",
            get(ws_members).post(ws_invite_member),
        )
        .route(
            "/workspaces/{pid}/members/{account_pid}",
            axum::routing::patch(ws_member_patch).delete(ws_member_remove),
        )
        .route("/workspaces/{pid}/invites", get(ws_invites))
        .route(
            "/workspaces/{pid}/invites/{iid}",
            axum::routing::delete(ws_invite_revoke),
        )
        // S3-5: members manage their own workspace's keys — list shows
        // prefix + metadata only, secrets are shown once at mint/rotate.
        .route("/workspaces/{pid}/keys", get(ws_keys).post(ws_key_create))
        .route(
            "/workspaces/{pid}/keys/{key_pid}",
            axum::routing::delete(ws_key_delete),
        )
        .route(
            "/workspaces/{pid}/keys/{key_pid}/rotate",
            post(ws_key_rotate),
        )
        .route("/invites/accept", post(invite_accept))
        .route("/invites/decline", post(invite_decline))
        // The old inspector mount: every URL under it lives at the same
        // tail under /app now — 308, query preserved. Nothing is served
        // under /inspector itself.
        .route("/inspector", get(inspector_redirect))
        .route("/inspector/", get(inspector_redirect))
        .route("/inspector/{*tail}", get(inspector_redirect))
        .route("/admin/compact", post(compact))
        .route("/admin/backup", post(backup))
        .route("/admin/diagnostics", get(diagnostics))
        // The tenant's own counts — any authenticated key reads its own
        // workspace's shape; another tenant's totals stay ops-only.
        .route("/diagnostics", get(workspace_diagnostics))
        // S2-2: the account graph. Workspaces and keys are addressed by
        // public_id only — internal u64 ids never leave the process.
        .route("/whoami", get(whoami))
        .route(
            "/admin/workspaces",
            get(list_workspaces).post(create_workspace),
        )
        .route("/admin/workspaces/{public_id}", get(get_workspace))
        .route(
            "/admin/workspaces/{public_id}/keys",
            get(list_workspace_keys).post(create_workspace_key),
        )
        .route(
            "/admin/workspaces/{public_id}/keys/{key_id}",
            axum::routing::delete(delete_workspace_key),
        )
        // S2-4: the caller's own workspace's handler registry.
        .route("/handlers", get(list_handlers))
        .route("/handler-secret", post(rotate_handler_secret))
        .route(
            "/handlers/{name}",
            axum::routing::put(put_handler).delete(delete_handler),
        )
        .route("/handlers/{name}/test", post(test_call_flat))
        .merge(observability)
        // A submit is at most 64 KiB of input (base64: ~88 KiB) plus its
        // spec; nothing legitimate comes near this.
        // ATTACK(body-limit)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(app);
    if let Some(ui) = ui_dir {
        let csp = header::HeaderValue::from_str(&ui_csp(ui)).unwrap_or(
            header::HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
        );
        let ui_routes = Router::new()
            .nest_service(
                "/app",
                tower_http::services::ServeDir::new(ui)
                    .append_index_html_on_directories(true)
                    .not_found_service(tower_http::services::ServeFile::new(ui.join("index.html"))),
            )
            .layer(axum::middleware::map_response(move |mut res: Response| {
                let csp = csp.clone();
                async move {
                    let h = res.headers_mut();
                    h.insert(header::CONTENT_SECURITY_POLICY, csp);
                    h.insert(
                        header::X_CONTENT_TYPE_OPTIONS,
                        header::HeaderValue::from_static("nosniff"),
                    );
                    h.insert(
                        header::REFERRER_POLICY,
                        header::HeaderValue::from_static("no-referrer"),
                    );
                    h.insert(
                        header::X_FRAME_OPTIONS,
                        header::HeaderValue::from_static("DENY"),
                    );
                    res
                }
            }));
        router = router.merge(ui_routes);
    }
    router
}

/// The dashboard's Content-Security-Policy: only its own files, and the
/// inline bootstrap script SvelteKit writes into `index.html` by hash (no
/// `'unsafe-inline'` scripts), calls to this origin only, no framing.
fn ui_csp(ui: &Path) -> String {
    use base64::Engine as _;
    let mut hashes = String::new();
    match std::fs::read_to_string(ui.join("index.html")) {
        Ok(html) => {
            let mut rest = html.as_str();
            while let Some(start) = rest.find("<script>") {
                let body = &rest[start + "<script>".len()..];
                let Some(end) = body.find("</script>") else {
                    break;
                };
                let digest = sha256(&body.as_bytes()[..end]);
                hashes.push_str(&format!(
                    " 'sha256-{}'",
                    base64::engine::general_purpose::STANDARD.encode(digest)
                ));
                rest = &body[end + "</script>".len()..];
            }
        }
        Err(e) => eprintln!(
            "durable-server: cannot read {}/index.html ({e}); the dashboard will not run",
            ui.display()
        ),
    }
    format!(
        "default-src 'none'; script-src 'self'{hashes}; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; \
         form-action 'none'; frame-ancestors 'none'"
    )
}

/// `/inspector` and every tail under it answer 308 to the same tail under
/// `/app`, query string preserved — `/inspector/x?y=1` -> `/app/x?y=1`,
/// `/inspector` -> `/app`. Nothing else under /inspector is served.
async fn inspector_redirect(uri: Uri) -> Response {
    let tail = uri
        .path_and_query()
        .map(|pq| pq.as_str().strip_prefix("/inspector").unwrap_or(""))
        .unwrap_or("");
    axum::response::Redirect::permanent(&format!("/app{tail}")).into_response()
}

fn api_key_from_env() -> Result<ApiKey> {
    let key = std::env::var(API_KEY_ENV).map_err(|_| {
        anyhow::anyhow!(
            "{API_KEY_ENV} is not set: mutating and admin routes need an API key \
             (generate one with `openssl rand -hex 32`)"
        )
    })?;
    if key.trim().len() < MIN_API_KEY_LEN {
        anyhow::bail!("{API_KEY_ENV} must be at least {MIN_API_KEY_LEN} characters");
    }
    Ok(ApiKey::new(key.trim()))
}

/// The optional read-only key: at least as long as the API key, and not
/// the same key (a read key that is also the admin key scopes nothing).
fn read_key_from_env(api_key: &ApiKey) -> Result<Option<ApiKey>> {
    let Ok(key) = std::env::var(READ_KEY_ENV) else {
        return Ok(None);
    };
    let key = key.trim();
    if key.len() < MIN_API_KEY_LEN {
        anyhow::bail!("{READ_KEY_ENV} must be at least {MIN_API_KEY_LEN} characters");
    }
    if api_key.verify(key) {
        anyhow::bail!("{READ_KEY_ENV} must differ from {API_KEY_ENV}");
    }
    Ok(Some(ApiKey::new(key)))
}

/// `DATABASE_URL`, when set, turns on the S2 account graph: API keys are
/// verified against it (through the keyring's cache) and workspaces other
/// than 0 become reachable. The pool is lazy — the server starts even if
/// Neon is momentarily down and store-dependent auth then fails closed as
/// `accounts_unavailable`. Migrations run best-effort at startup: a
/// failure is logged loudly and surfaces as `accounts_unavailable` on the
/// affected paths rather than a silent fallthrough to deny-all.
async fn account_store_from_env() -> Result<Option<Arc<dyn AccountStore>>> {
    const DATABASE_URL_ENV: &str = "DATABASE_URL";
    const BACKEND_ENV: &str = "DURABLE_ACCOUNTS_BACKEND";
    let backend = std::env::var(BACKEND_ENV).unwrap_or_default();
    let url = std::env::var(DATABASE_URL_ENV).ok();
    match (backend.trim(), url) {
        ("memory", None) => {
            eprintln!(
                "durable-server: DURABLE_ACCOUNTS_BACKEND=memory — the account \
                 graph is in-memory only (dev/test): keys and workspaces die \
                 with the process"
            );
            Ok(Some(Arc::new(durable_accounts::Memory::new())))
        }
        ("memory", Some(_)) => {
            anyhow::bail!("{BACKEND_ENV}=memory conflicts with {DATABASE_URL_ENV}: pick one")
        }
        ("", None) => Ok(None),
        ("", Some(url)) if !url.trim().is_empty() => {
            let store =
                Pg::connect(url.trim()).map_err(|e| anyhow::anyhow!("{DATABASE_URL_ENV}: {e}"))?;
            match store.migrate().await {
                Ok(()) => {
                    eprintln!("durable-server: account store connected, migrations applied")
                }
                Err(e) => eprintln!(
                    "durable-server: WARNING account store unreachable at startup ({e}); \
                     serving anyway — store-verified keys answer accounts_unavailable \
                     until it returns"
                ),
            }
            Ok(Some(Arc::new(store)))
        }
        ("", Some(_)) => anyhow::bail!("{DATABASE_URL_ENV} is set but empty"),
        (other, _) => anyhow::bail!("{BACKEND_ENV}={other:?} is unknown (want: memory)"),
    }
}

/// Whether the request's `Host` is a loopback name: `localhost`, or an IP
/// literal in 127.0.0.0/8 or `::1`, with or without a port. A missing or
/// unparsable Host is not.
fn loopback_host(parts: &Parts) -> bool {
    let Some(host) = parts
        .headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<axum::http::uri::Authority>().ok())
    else {
        return false;
    };
    let name = host.host();
    let name = name
        .strip_prefix('[')
        .and_then(|n| n.strip_suffix(']'))
        .unwrap_or(name);
    name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `--open-reads` is allowed only where nothing but this host can connect:
/// a loopback IP address (a name such as `localhost` is not trusted to
/// resolve to one).
/// `--ops-listen` must be a loopback socket address (finding 18): the
/// flag exists to keep env keys off the public socket, so a non-loopback
/// value would re-publish them — refuse the boot with the reason.
fn check_ops_listen(ops_listen: &Option<String>) -> Result<()> {
    let Some(addr) = ops_listen else {
        return Ok(());
    };
    match addr.parse::<std::net::SocketAddr>() {
        Ok(sa) if sa.ip().is_loopback() => Ok(()),
        Ok(_) => anyhow::bail!(
            "--ops-listen must be a loopback address (127.0.0.1 or [::1]);              {addr} would re-publish the env keys it exists to keep private"
        ),
        Err(e) => anyhow::bail!("--ops-listen {addr:?} is not a socket address: {e}"),
    }
}

/// `DURABLE_INVITE_ONLY` is a value, not a presence flag (deploy review):
/// `1/true/yes/on` turns the gate on, `0/false/no/off`/empty off; any
/// other spelling refuses the boot rather than silently gating sign-up.
fn invite_only_env() -> Result<bool> {
    invite_only_value(
        std::env::var_os("DURABLE_INVITE_ONLY")
            .map(|v| v.to_string_lossy().into_owned())
            .as_deref(),
    )
}

fn invite_only_value(raw: Option<&str>) -> Result<bool> {
    let Some(raw) = raw else { return Ok(false) };
    let v = raw.trim().to_ascii_lowercase();
    match v.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "" | "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!(
            "DURABLE_INVITE_ONLY={v:?} is not a boolean; use 1/true/yes/on or 0/false/no/off (or unset)"
        ),
    }
}

fn check_open_reads(listen: &str, open_reads: bool) -> Result<()> {
    if !open_reads {
        return Ok(());
    }
    match listen.parse::<std::net::SocketAddr>() {
        Ok(a) if a.ip().is_loopback() => Ok(()),
        _ => anyhow::bail!(
            "--open-reads is allowed only on a loopback --listen (127.0.0.1 or [::1]); \
             {listen} is reachable from elsewhere, so reads need DURABLE_API_KEY or DURABLE_READ_KEY"
        ),
    }
}

fn prepare_backup_root(dir: &Path) -> Result<Arc<Path>> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("create backup root {}", dir.display()))?;
    let canonical = std::fs::canonicalize(dir)
        .with_context(|| format!("resolve backup root {}", dir.display()))?;
    // A backup interrupted by a crash left `.<name>.partial`: never valid.
    let swept = durable_journal::sweep_partial_backups(&canonical)
        .map_err(|e| anyhow::anyhow!("sweep partial backups: {e}"))?;
    if swept > 0 {
        eprintln!("durable-server: removed {swept} interrupted backup(s) from the backup root");
    }
    Ok(Arc::from(canonical))
}

/// The in-flight cap one workspace may hold: the configured share for
/// tenants, the whole pool for a compat (account-store-free, ws-0 only)
/// install — v1.0.x gave ws 0 the pool and compat must not regress it
/// (S2 re-review N3).
fn ws_in_flight_cap(args: &Args, compat: bool) -> usize {
    if compat {
        args.max_in_flight.max(1)
    } else {
        args.max_in_flight_per_ws.max(1)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.journal_format {
        // "<records> <snapshots> <parks ws≠0>": a single integer before
        // v1.0.2 meant records only; the snapshot reader and the ws-park
        // guarantee are what a rollback needs to know (D-056, S2 review).
        println!(
            "{} {} 1",
            durable_engine::READER_VERSION,
            durable_engine::snapshot::VERSION
        );
        return Ok(());
    }
    let api_key = api_key_from_env()?;
    let read_key = read_key_from_env(&api_key)?;
    let store = account_store_from_env().await?;
    if store.is_some() && args.public_origin.is_none() {
        // Without the configured origin, mailed links would be built
        // from the request Host — attacker-controlled on the open auth
        // routes (review #18). Refuse rather than mint Host-derived
        // links.
        anyhow::bail!("--public-origin is required when an account store is configured");
    }
    check_open_reads(&args.listen, args.open_reads)?;
    check_ops_listen(&args.ops_listen)?;
    let backup_root = args
        .backup_root
        .as_deref()
        .map(prepare_backup_root)
        .transpose()?;
    let cfg = ShellConfig {
        journal: durable_journal::JournalOptions {
            reserve_bytes: args.reserve_bytes,
            ..Default::default()
        },
        compact_every: (args.compact_every_secs > 0)
            .then(|| Duration::from_secs(args.compact_every_secs)),
        max_snapshot_bytes: args.max_snapshot_bytes,
        max_in_flight: args.max_in_flight.max(1),
        max_in_flight_per_ws: ws_in_flight_cap(&args, store.is_none()),
        max_dispatch_per_tick: args.max_dispatch_per_tick.max(1),
        reply_timeout: Duration::from_millis(args.reply_timeout_ms.max(1)),
        limits: durable_domain::Limits {
            max_live_runs: args.max_live_runs,
            max_live_runs_per_ws: args.max_live_runs_per_ws,
            max_index_bytes: args.max_index_bytes,
            max_index_bytes_per_ws: args.max_index_bytes_per_ws,
            // Compat mode (no account store) is a ws-0-only install: it
            // admits to the global bounds exactly like v1.0.x, never the
            // tenant share an upgrade would otherwise impose (finding 6).
            ws0_share_exempt: store.is_none(),
            ..durable_domain::Limits::default()
        },
        ..Default::default()
    };
    let mut registry = Registry::default();
    // The SSRF policy (S2-5 + white-hat A): this host's own interface
    // addresses join the refused set — a handler URL naming the droplet's
    // public address would otherwise be delivered over lo to anything
    // listening on 0.0.0.0. Listing them fails closed: no start.
    let policy = ssrf::Policy::boot(false, args.deny_cidrs.clone())
        .context("list this host's interface addresses for the SSRF guard")?;
    // Two policies (re-review S1-1): `--allow-local-handlers` opens the
    // operator's own `--handler` flags only. Every API-registered handler
    // — any workspace — is checked against the full policy whatever that
    // flag says; only a lab build can open it, with its own flag.
    // ATTACK(tenant-strict-ssrf)
    let store_policy = policy.with_allow_local(lab_local_tenant_handlers(&args));
    let flag_policy = policy.with_allow_local(args.allow_local_handlers);
    let pubid =
        pubid::PubId::load_or_generate(&args.journal).with_context(|| "load public-id key")?;
    let factory = Arc::new(HandlerFactory::new(
        store_policy.clone(),
        flag_policy.clone(),
        Duration::from_millis(args.step_timeout_ms),
        cfg.limits.max_output_bytes,
        pubid.clone(),
    )?);
    // A `--handler` flag URL: literal-local refused unless
    // --allow-local-handlers.
    for (_, url) in &args.handlers {
        valid_handler_url(url, &flag_policy)
            .map_err(|e| anyhow::anyhow!("--handler {url}: {}", e.message))?;
    }
    let mut flag_handlers: Vec<(String, Arc<dyn StepHandler>)> = Vec::new();
    for (name, url) in &args.handlers {
        let h = factory.make(url);
        flag_handlers.push((name.clone(), h.clone()));
        registry.register(name, h);
    }
    let engine =
        spawn(&args.journal, registry, cfg).map_err(|e| anyhow::anyhow!("engine open: {e}"))?;
    if let Some(t) = engine.discarded_tail() {
        eprintln!(
            "durable-server: discarded a torn tail: {} bytes at offset {} (the log ended inside \
             its last frame, which was never acknowledged)",
            t.bytes, t.offset
        );
    }
    // S2-4: the last-known-good registry, loaded before serving — a
    // restart during an account-store outage still dispatches. The
    // reconciler (below, store mode only) refreshes it.
    let registry_file: Arc<Path> = Arc::from(args.journal.join("handlers.json").as_path());
    if let Ok(d) = engine.diag().await {
        if d.workspace_scoped_live_runs > 0 {
            eprintln!(
                "durable-server: {} workspace-scoped run(s) live in the journal",
                d.workspace_scoped_live_runs
            );
        }
    }
    let registry = Arc::new(RegistryState::default());
    if store.is_some() {
        let entries = load_registry_file(&registry_file)
            .with_context(|| format!("loading {registry_file:?}"))?;
        for (ws, name, url) in arm_snapshot(entries, &factory, &engine).await {
            registry.set(ws, &name, &url);
        }
    }
    let mailer = mailout::mailer_from_env()?;
    let stopped = engine.clone();
    let registry_stale = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let registry_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let registry_tx = spawn_registry_writer(
        registry_file.clone(),
        Arc::clone(&registry_epoch),
        Arc::clone(&registry.saves),
    );
    let obs = engine.observer().clone();
    let app = App {
        engine,
        keyring: Arc::new(Keyring::new(api_key, read_key, store.clone())),
        ops_gate: args.ops_listen.is_some(),
        open_reads: args.open_reads,
        backup_root,
        handler_factory: factory,
        flag_handlers: Arc::new(flag_handlers),
        registry_stale: Arc::clone(&registry_stale),
        registry_file,
        registry_tx,
        registry_epoch,
        registry_locks: Arc::new(RegistryLocks::default()),
        registry,
        session_key: std::sync::Arc::new(pubid.derive_key(b"session-token-v1")),
        sessions: std::sync::Arc::new(SessionCache::new()),
        members: std::sync::Arc::new(MemberCache::new()),
        test_calls_ws: std::sync::Arc::new(Mutex::new(HashMap::new())),
        test_calls_addr: std::sync::Arc::new(Mutex::new(SlidingWindow::new(
            10,
            Duration::from_secs(60),
        ))),
        argon: std::sync::Arc::new(ArgonQueue::new()),
        auth_limits: std::sync::Arc::new(AuthLimits::new()),
        lockouts: std::sync::Arc::new(LoginLockouts::new()),
        mailer: mailer.clone(),
        mail_budget: std::sync::Arc::new(MailBudget::new(mailout::mail_budget_from_env(
            MAIL_DAILY_BUDGET,
        ))),
        invite_only: invite_only_env()?,
        public_origin: args.public_origin.clone(),
        now_override_ms: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
        pubid,
        obs: obs.clone(),
    };
    if let Some(store) = &store {
        spawn_registry_reconcile(store.clone(), &app, registry_stale);
        // The outbox drain owns every send — the mail row committed in
        // the same transaction as its fact is the only truth (D-068).
        spawn_mail_drain(app.clone());
        // Idle-expiry slides in one batched store write per tick (D-071).
        spawn_session_flush(app.clone());
    }
    let router = router(app.clone(), args.ui_dir.as_deref());
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    if args.allow_local_handlers {
        // A visible opt-out must be visible at startup too.
        eprintln!(
            "durable-server: --allow-local-handlers set — the --handler flag \
             URLs may target local/private addresses; API-registered handlers \
             stay under the full SSRF policy"
        );
    }
    if lab_local_tenant_handlers(&args) {
        eprintln!(
            "durable-server: LAB BUILD with --allow-local-tenant-handlers — \
             API-registered handlers may target local addresses; never deploy this"
        );
    }
    let (own, denied) = policy.sizes();
    eprintln!(
        "durable-server: SSRF guard refuses this host's {own} interface addresses \
         and {denied} --deny-cidr range(s) besides the local blocks"
    );
    eprintln!("durable-server listening on {}", args.listen);
    let (stop_http, http_stopped) = tokio::sync::oneshot::channel::<()>();
    let health_serve = match &args.health_listen {
        Some(addr) => {
            let l = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind health listener {addr}"))?;
            let r = Router::new()
                .route("/health", get(health))
                .route("/ready", get(ready))
                .with_state(app.clone());
            let (hs_stop, hs_rx) = tokio::sync::oneshot::channel::<()>();
            eprintln!("durable-server liveness listening on {addr}");
            Some((
                hs_stop,
                tokio::spawn(serve_http(l, r, health_limits(), None, hs_rx, false)),
            ))
        }
        None => None,
    };
    let limits = HttpLimits {
        max_connections: args.max_connections.max(1),
        header_timeout: Duration::from_millis(args.header_timeout_ms.max(1)),
        body_deadline: Duration::from_millis(args.body_deadline_ms.max(1)),
        write_stall: Duration::from_millis(args.write_stall_ms.max(1)),
        max_lifetime: Duration::from_secs(args.max_connection_secs.max(1)),
        max_requests: args.max_requests_per_connection.max(1),
        rate_per_addr: args.rate_limit_per_addr.max(0.0),
        v6_prefix: args.rate_limit_v6_prefix,
        max_conns_per_addr: args.max_conns_per_addr,
        max_conns_per_proxy: args.max_conns_per_proxy,
        trusted_proxies: TrustedProxies(args.trusted_proxies),
        ..HttpLimits::default()
    };
    let serve = serve_http(
        listener,
        router.clone(),
        limits.clone(),
        Some(obs.clone()),
        http_stopped,
        false,
    );
    // The ops listener (finding 18): same router, same bounds, plus the
    // `OpsListener` request extension the env-key gate checks for.
    let ops_serve = match &args.ops_listen {
        Some(addr) => {
            let l = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind ops listener {addr}"))?;
            let (os_stop, os_rx) = tokio::sync::oneshot::channel::<()>();
            eprintln!("durable-server ops listening on {addr}");
            Some((
                os_stop,
                tokio::spawn(serve_http(l, router, limits, Some(obs), os_rx, true)),
            ))
        }
        None => None,
    };
    tokio::pin!(serve);
    tokio::select! {
        r = &mut serve => return r.context("serve"),
        () = stopped.closed() => {
            // The journal writer poisoned itself (a failed fsync is never
            // retried): exit non-zero so the supervisor restarts us and
            // startup replay decides what is durable.
            anyhow::bail!("engine stopped: journal writer poisoned; exiting so a restart replays");
        }
        () = terminate() => {}
    }
    // Drain (D-036): stop accepting, let requests in flight finish (each
    // ack still waits for its fsync), then let the engine make everything
    // it decided durable and release the journal. Exit 0 only if all of
    // that happened; otherwise a restart replays, as after a crash.
    eprintln!("durable-server: terminating: draining");
    let limit = Duration::from_secs(args.drain_timeout_secs.max(1));
    let _ = stop_http.send(());
    if let Some((hs_stop, hs_task)) = health_serve {
        let _ = hs_stop.send(());
        let _ = hs_task.await;
    }
    if let Some((os_stop, os_task)) = ops_serve {
        let _ = os_stop.send(());
        let _ = os_task.await;
    }
    let cut = match tokio::time::timeout(limit, &mut serve).await {
        Ok(r) => {
            r.context("serve")?;
            false
        }
        Err(_) => {
            eprintln!(
                "durable-server: requests still open after {}s; closing them",
                limit.as_secs()
            );
            true
        }
    };
    // The engine drains either way: what it decided becomes durable.
    stopped
        .shutdown(limit)
        .await
        .map_err(|e| anyhow::anyhow!("drain did not finish ({e}); a restart replays"))?;
    if cut {
        // Nothing durable is at risk, but a client lost its response:
        // that is not a clean stop (D-036).
        anyhow::bail!("drained the engine, but requests were still open at the drain timeout");
    }
    eprintln!("durable-server: drained; exiting");
    Ok(())
}

/// Bounds on what one client connection — or one client address — may
/// hold.
#[derive(Debug, Clone)]
struct HttpLimits {
    max_connections: usize,
    header_timeout: Duration,
    body_deadline: Duration,
    /// A response write that makes no progress for this long closes the
    /// connection (a client that does not read).
    write_stall: Duration,
    /// A connection is asked to close after this long (after the request
    /// in flight), however busy it is.
    max_lifetime: Duration,
    /// The last request a connection serves carries `Connection: close`:
    /// a client pipelining thousands of requests gets this many answers.
    max_requests: usize,
    /// Requests/second one client address may make (0 = unlimited).
    rate_per_addr: f64,
    /// Prefix bits one IPv6 client address is keyed by.
    v6_prefix: u8,
    /// Open connections one untrusted peer address may hold (0 = unlimited).
    max_conns_per_addr: usize,
    /// Open connections one trusted-proxy peer may hold (0 = unlimited).
    max_conns_per_proxy: usize,
    /// Who may name the client address in `CF-Connecting-IP`.
    trusted_proxies: TrustedProxies,
    /// The liveness reserve: conns that answer /health + /ready while
    /// the work pool is dry.
    ready_pool: usize,
    /// Reserve slots one address bucket may hold at once.
    ready_per_addr: usize,
    /// A reserve conn's request head must arrive inside this window.
    ready_head_timeout: Duration,
    /// Under slot pressure a conn whose head never produced a served
    /// request is evicted once it is this old.
    pressure_head_deadline: Duration,
    /// How often a work conn re-checks slot pressure.
    pressure_poll: Duration,
    /// Overload-lane slots one address bucket may hold at once.
    lane_per_addr: usize,
}

impl Default for HttpLimits {
    /// The server's flag defaults.
    fn default() -> Self {
        Self {
            max_connections: 512,
            header_timeout: Duration::from_secs(10),
            body_deadline: Duration::from_secs(30),
            write_stall: Duration::from_secs(10),
            max_lifetime: Duration::from_secs(600),
            max_requests: 100,
            rate_per_addr: 100.0,
            v6_prefix: DEFAULT_V6_PREFIX,
            max_conns_per_addr: 64,
            max_conns_per_proxy: 4096,
            trusted_proxies: TrustedProxies::default(),
            ready_pool: READY_CONNECTIONS,
            ready_per_addr: READY_PER_ADDR,
            ready_head_timeout: READY_HEAD_TIMEOUT,
            pressure_head_deadline: PRESSURE_HEAD_DEADLINE,
            pressure_poll: PRESSURE_POLL,
            lane_per_addr: LANE_PER_ADDR,
        }
    }
}

/// Serve `router` on `listener` until `stop` fires, then let the requests
/// in flight finish (D-045, D-049). Every connection holds one of the
/// `max_connections` permits, or one of the reserve below.
///
/// Liveness connections held in reserve: a flood that fills every
/// general slot cannot take these — they answer /health and /ready only.
/// The readiness listener and this reserve answer /health + /ready even
/// while the work pool is dry (S2 wedge: a flood once reset every probe
/// at the kernel backlog).
const READY_CONNECTIONS: usize = 8;
/// A liveness reserve slot is capped per address bucket and its head
/// must arrive inside a second: a flooder that piles onto the main
/// listener while the pool is dry can hold at most this many reserve
/// slots — and only by actually asking (S2 re-review F3).
const READY_PER_ADDR: usize = 2;
const READY_HEAD_TIMEOUT: Duration = Duration::from_secs(1);
/// The overload lane: while every work and reserve permit is held the
/// server keeps accepting into this bounded lane and answers each conn
/// `503 overloaded` + `Retry-After`, then shuts its write side and
/// drains briefly — the client reads a real HTTP refusal instead of a
/// kernel RST it cannot tell from a crash (S2 re-review ruling F1: a
/// TCP reset is not a bounded refusal). The lane is a fast lane, not a
/// queue: conns live at most a second, so it drains at ~256/s — and a
/// single host may not fill it, hence the per-address budget (N4).
const OVERLOAD_LANE_CONNECTIONS: usize = 256;
const OVERLOAD_LANE_LIFETIME: Duration = Duration::from_secs(1);
const LANE_PER_ADDR: usize = 8;
/// Under slot pressure a conn whose request head has not produced a
/// served request inside this window is closed — the attacker's
/// favourite shape (a parked or trickled head camping a work slot for
/// the whole header timeout) is evicted before eligible idle
/// keep-alives are touched (S2 re-review design item 2 + F2).
const PRESSURE_HEAD_DEADLINE: Duration = Duration::from_secs(1);
const PRESSURE_POLL: Duration = Duration::from_millis(250);

/// Served-and-idle work conns a starved accept may reclaim (S2
/// re-review N3). A conn registers once at accept; its served/in-flight
/// state is read live from the shared counters. When the pool is dry
/// and a conn from a not-over-share bucket still cannot get a slot,
/// the accept pops the longest-idle victim — over its fair share first
/// — under this one mutex, so each claim is atomic: one starved accept
/// kills at most one conn and two accepts can never pick the same one.
struct IdleRegistry {
    inner: std::sync::Mutex<HashMap<u64, IdleEntry>>,
}

struct IdleEntry {
    /// The conn's fair-share bucket (v4 exact, v6 /48 aggregate) — the
    /// ordering key: over-share buckets yield before at-share ones.
    bucket: std::net::IpAddr,
    served: Arc<std::sync::atomic::AtomicUsize>,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    /// Epoch ms of the last completed request's end — the idle age.
    idle_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Fired when this conn is claimed; the conn task yields its slot.
    evict: tokio::sync::oneshot::Sender<()>,
}

impl IdleRegistry {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn insert(&self, id: u64, entry: IdleEntry) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, entry);
    }

    fn remove(&self, id: u64) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
    }

    /// The claim: longest-idle served+idle conn, over-share buckets
    /// first. `held` reports a bucket's live work conns; `share` is the
    /// pool divided over live buckets. Popping under the lock is the
    /// atomic claim — the entry is gone before the lock releases, so a
    /// second starved accept can never pick the same conn (N3).
    fn take_victim(
        &self,
        held: impl Fn(std::net::IpAddr) -> usize,
        buckets: usize,
        pool: usize,
    ) -> Option<IdleEntry> {
        let share = (pool / buckets.max(1)).max(1);
        let mut map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut pick: Option<(u64, u64, bool)> = None;
        for (id, e) in map.iter() {
            if e.served.load(std::sync::atomic::Ordering::Relaxed) < 1
                || e.in_flight.load(std::sync::atomic::Ordering::Relaxed) != 0
            {
                continue;
            }
            let over = held(e.bucket) > share;
            let idle = e.idle_ms.load(std::sync::atomic::Ordering::Relaxed);
            let better = match pick {
                None => true,
                // Over-share buckets yield first; ties — and any bucket
                // once none are over share — go to the longest idle
                // (the SMALLEST last-activity stamp).
                Some((_, idle2, over2)) => (over && !over2) || (over == over2 && idle < idle2),
            };
            if better {
                pick = Some((*id, idle, over));
            }
        }
        pick.and_then(|(id, ..)| map.remove(&id))
    }
}

/// Removes the conn's registry entry however the conn task ends — an
/// entry left behind would sit in every victim scan until claimed.
struct RegGuard {
    id: u64,
    reg: Arc<IdleRegistry>,
}

impl Drop for RegGuard {
    fn drop(&mut self) {
        self.reg.remove(self.id);
    }
}

/// Which pool a freshly-accepted connection drew its permit from.
enum Slot {
    /// Full service; the permit is the bounded-work cap.
    Work(tokio::sync::OwnedSemaphorePermit),
    /// Liveness-only service from the reserve (S2: /health must answer
    /// while a tenant's connection storm fills every work slot).
    Ready(tokio::sync::OwnedSemaphorePermit),
    /// The overload lane: the conn gets a fixed `503 + Retry-After`, a
    /// write-half close and a brief drain — a refusal the client can
    /// read, never a bare RST.
    Lane(tokio::sync::OwnedSemaphorePermit),
}

/// How a pressured connection is evicted (S2 re-review design items
/// 2-3 + N3): a dead head is dropped outright — graceful shutdown would
/// wait out the whole header timeout for a head that never finishes —
/// while a claimed idle keep-alive drains first (its served work is
/// already done).
#[derive(Debug, PartialEq, Eq)]
enum Pressure {
    /// `served == 0` past the head deadline: evicted first, no drain.
    DeadHead,
    /// A starved accept claimed this conn in the idle registry
    /// (longest-idle, over-share buckets first): graceful shutdown.
    Yielded,
}

/// The dedicated liveness listener's bounds: a small conn pool, no rate
/// table (probes are bursts by design), the same deadline posture.
fn health_limits() -> HttpLimits {
    HttpLimits {
        max_connections: 32,
        header_timeout: Duration::from_secs(10),
        body_deadline: Duration::from_secs(30),
        write_stall: Duration::from_secs(10),
        max_lifetime: Duration::from_secs(300),
        max_requests: 100,
        rate_per_addr: 0.0,
        v6_prefix: 64,
        max_conns_per_addr: 32,
        max_conns_per_proxy: 0,
        trusted_proxies: TrustedProxies::default(),
        ..HttpLimits::default()
    }
}

/// `max_connections` permits (the rest wait in the kernel's accept queue);
/// a request head (and an idle keep-alive connection waiting for one) must
/// arrive within `header_timeout`; a request body within `body_deadline`;
/// a response write must make progress within `write_stall`; and a
/// connection is closed after `max_lifetime`, once its request in flight
/// is answered.
async fn serve_http(
    listener: tokio::net::TcpListener,
    router: Router,
    limits: HttpLimits,
    obs: Option<Arc<Observer>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    ops_side: bool,
) -> std::io::Result<()> {
    use hyper_util::rt::{TokioIo, TokioTimer};
    let permits = Arc::new(tokio::sync::Semaphore::new(limits.max_connections.max(1)));
    // Readiness keeps a reserved slice: a flood that fills every general
    // slot still leaves `ready_pool` conns that answer /health and
    // /ready — a tenant's connection storm can starve work, never probes
    // (S2 wedge: keep-alive hoarding froze /health behind a full pool).
    // ATTACK(liveness-flood): zero the reserve and a work-pool flood starves /health.
    let ready_permits = Arc::new(tokio::sync::Semaphore::new(limits.ready_pool.max(1)));
    // The overload lane behind both pools: a full backlog must not be the
    // answer — every accepted conn gets a real HTTP response, even when
    // that response is `503 overloaded` (S2 re-review ruling F1).
    let lane_permits = Arc::new(tokio::sync::Semaphore::new(OVERLOAD_LANE_CONNECTIONS));
    // The refusal bytes are one fixed response per conn — built once.
    let lane_body = br#"{"error":"overloaded","code":"overloaded","retry_after_ms":1000}"#;
    let lane_response = Arc::new(
        format!(
            "HTTP/1.1 503 Service Unavailable\r\n\
         content-type: application/json\r\n\
         retry-after: 1\r\n\
         content-length: {}\r\n\
         connection: close\r\n\r\n{}",
            lane_body.len(),
            String::from_utf8_lossy(lane_body),
        )
        .into_bytes(),
    );
    let (closing_tx, closing) = tokio::sync::watch::channel(false);
    // ATTACK(body-deadline)
    let service = tower_http::timeout::RequestBodyDeadline::new(router, limits.body_deadline);
    let mut conns = tokio::task::JoinSet::new();
    let mut accept_failing = false;
    // Per-address bounds (D-052): the rate table is one token bucket per
    // client address in a bounded map; the connection count is per TCP
    // peer (at accept time no request — and so no trusted-proxy header —
    // has been read yet).
    // ATTACK(rate-limit)
    let rate_limiter = (limits.rate_per_addr > 0.0).then(|| {
        Arc::new(std::sync::Mutex::new(RateLimit::new(
            limits.rate_per_addr,
            MAX_LIMITED_ADDRS,
            limits.v6_prefix,
        )))
    });
    // Always constructed: the counts feed fair-share eviction even when
    // no cap is configured (S2 re-review F3 + design item 3). With caps
    // at zero `acquire` only counts.
    let conn_limit = ConnLimit::new(
        limits.max_conns_per_addr,
        limits.max_conns_per_proxy,
        limits.v6_prefix,
    );
    // The liveness reserve is its own budget, capped per address — a
    // flooder may not hold more than `ready_per_addr` reserve slots.
    let ready_limit = ConnLimit::new(
        limits.ready_per_addr,
        limits.ready_per_addr,
        limits.v6_prefix,
    );
    // The overload lane is capped per address too (S2 re-review N4): a
    // client that ignores the 503 holds its lane slot for the whole
    // drain second — 256 fresh conns/s from one host would otherwise
    // park the whole lane and every later refusal degrade to a bare
    // drop.
    let lane_limit = ConnLimit::new(limits.lane_per_addr, limits.lane_per_addr, limits.v6_prefix);
    // The claim log for starved-accept evictions (N3).
    let idle_registry = Arc::new(IdleRegistry::new());
    let trusted = limits.trusted_proxies.clone();
    let pool_size = limits.max_connections.max(1);
    let mut conn_cap_refused = false;
    loop {
        // `biased`: general slots are preferred; a ready slot is spent
        // only while the general pool is empty — reserved capacity must
        // not drain during normal load (N7-re: a saturated pool is the
        // only moment /health would otherwise lose).
        let slot = tokio::select! {
            biased;
            p = permits.clone().acquire_owned() => match p {
                Ok(p) => Slot::Work(p),
                Err(_) => break, // the semaphore is never closed
            },
            p = ready_permits.clone().acquire_owned() => match p {
                Ok(p) => Slot::Ready(p),
                Err(_) => break,
            },
            p = lane_permits.clone().acquire_owned() => match p {
                Ok(p) => Slot::Lane(p),
                Err(_) => break,
            },
            _ = &mut stop => break,
            Some(done) = conns.join_next(), if !conns.is_empty() => {
                if let Err(e) = done {
                    eprintln!("durable-server: a connection task failed: {e}");
                }
                continue;
            }
        };
        let (stream, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(accepted) => {
                    accept_failing = false;
                    accepted
                }
                Err(e) => {
                    // Out of descriptors or a reset handshake: back off
                    // rather than spin (logged once per episode); the
                    // permit returns.
                    if !accept_failing {
                        eprintln!("durable-server: accept failed: {e}; retrying");
                        accept_failing = true;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = &mut stop => break,
        };
        // A dual-stack listener reports an IPv4 client as `::ffff:a.b.c.d`:
        // judged as IPv6, the loopback proxy hop (127.0.0.1/32) would not
        // be trusted and every IPv4 client would share one /64 bucket
        // (white-hat finding B). Every limit below sees the canonical form.
        // ATTACK(peer-canonical)
        let peer = std::net::SocketAddr::new(peer.ip().to_canonical(), peer.port());
        if let Slot::Lane(lane_permit) = slot {
            // The work pool is dry and this conn is waiting — real
            // demand may reclaim one idle served slot (N3).
            evict_for_demand(
                &idle_registry,
                &conn_limit,
                &ready_limit,
                &lane_limit,
                peer.ip(),
                pool_size,
            );
            // Both service pools are dry — the conn still gets a real
            // HTTP refusal rather than a slot in the kernel backlog it
            // would ride to a timeout or an RST. The write half closes
            // right after the response so the 503 is read, not raced by
            // a reset; the read side drains briefly because closing with
            // unread request bytes is what turns into an RST (S2
            // re-review design item 1).
            match lane_limit.acquire(peer.ip(), trusted.trusts(peer.ip())) {
                // The lane owes every refusal a real answer — a silent
                // drop is the reset the client cannot tell from a crash
                // (ruling F1).
                Some(lane_guard) => {
                    // ATTACK(overload-lane): drop this call and lane conns die unanswered.
                    lane_answer(&mut conns, stream, lane_permit, &lane_response, lane_guard)
                }
                // The waiter's own bucket parked the lane — a refusal
                // is still owed an answer (N4).
                None => answer_best_effort(stream, &lane_response),
            }
            continue;
        }
        if let Slot::Ready(ready_permit) = slot {
            // Reserved capacity: liveness paths only, never keep-alive —
            // a conn answers one probe then closes, so the slice cannot
            // be camped. The reserve is capped per address (F3): a
            // flooder piling onto the main listener holds at most
            // `ready_per_addr` slots; over that the conn falls to the
            // overload lane (or is dropped when even the lane is full).
            // This accept is real demand too: an idle served conn may
            // yield its work slot for it (N3).
            evict_for_demand(
                &idle_registry,
                &conn_limit,
                &ready_limit,
                &lane_limit,
                peer.ip(),
                pool_size,
            );
            let Some(ready_guard) = ready_limit.acquire(peer.ip(), trusted.trusts(peer.ip()))
            else {
                match lane_admit(
                    &lane_permits,
                    &lane_limit,
                    peer.ip(),
                    trusted.trusts(peer.ip()),
                ) {
                    Some((lane_permit, lane_guard)) => {
                        conns.spawn(overload_answer(
                            stream,
                            lane_permit,
                            lane_response.clone(),
                            lane_guard,
                        ));
                    }
                    None => answer_best_effort(stream, &lane_response),
                }
                continue;
            };
            let io = TokioIo::new(StallGuard::new(stream, limits.write_stall));
            let svc = service.clone();
            let handler =
                hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let svc = svc.clone();
                    async move {
                        let allowed = req.method() == hyper::Method::GET
                            && matches!(req.uri().path(), "/health" | "/ready");
                        let mut res = if allowed {
                            tower::ServiceExt::oneshot(svc, req).await?
                        } else {
                            // A refusal the server chooses is owed a
                            // Retry-After too (S2 re-review N4).
                            Response::builder()
                                .status(StatusCode::SERVICE_UNAVAILABLE)
                                .header(header::CONTENT_TYPE, "application/json")
                                .header(header::RETRY_AFTER, "1")
                                .body(axum::body::Body::from(
                                    r#"{"error":"reserved for liveness","code":"reserved"}"#,
                                ))
                                .unwrap_or_else(|_| Response::new(axum::body::Body::empty()))
                        };
                        res.headers_mut().insert(
                            header::CONNECTION,
                            header::HeaderValue::from_static("close"),
                        );
                        Ok::<_, std::convert::Infallible>(res)
                    }
                });
            let conn = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.ready_head_timeout)
                .serve_connection(io, handler);
            let mut closing = closing.clone();
            let lifetime = limits.max_lifetime;
            conns.spawn(async move {
                let _permit = ready_permit;
                let _ready_guard = ready_guard;
                let mut conn = std::pin::pin!(conn);
                tokio::select! {
                    _ = conn.as_mut() => return,
                    _ = closing.wait_for(|c| *c) => {}
                    () = tokio::time::sleep(lifetime) => {}
                }
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            });
            continue;
        }
        let Slot::Work(permit) = slot else {
            unreachable!("Lane and Ready are handled above")
        };
        // The per-peer connection cap: past it the conn falls to the
        // overload lane for a real 503, not a bare drop — a refused
        // connection must still be an answer (S2 re-review F3).
        let conn_guard = match conn_limit.acquire(peer.ip(), trusted.trusts(peer.ip())) {
            Some(g) => {
                conn_cap_refused = false;
                g
            }
            None => {
                if !conn_cap_refused {
                    eprintln!(
                        "durable-server: per-address connection cap reached; refusing {}",
                        peer.ip()
                    );
                    conn_cap_refused = true;
                }
                match lane_admit(
                    &lane_permits,
                    &lane_limit,
                    peer.ip(),
                    trusted.trusts(peer.ip()),
                ) {
                    Some((lane_permit, lane_guard)) => {
                        conns.spawn(overload_answer(
                            stream,
                            lane_permit,
                            lane_response.clone(),
                            lane_guard,
                        ));
                    }
                    None => answer_best_effort(stream, &lane_response),
                }
                continue; // the work permit returns with the task
            }
        };
        // A second handle on the socket: the DeadHead arm writes the
        // conn's refusal on it — hyper's half-parsed head cannot be
        // answered through the connection itself (ruling F1). The fd is
        // duplicated via std's try_clone — a round trip tokio -> std ->
        // tokio keeps the workspace's unsafe_code = "forbid".
        let Ok(std_stream) = stream.into_std() else {
            continue; // the conn is already gone with the stream
        };
        let refusal_handle = std_stream.try_clone().ok().and_then(|s| {
            s.set_nonblocking(true).ok()?;
            tokio::net::TcpStream::from_std(s).ok()
        });
        let Ok(stream) = tokio::net::TcpStream::from_std(std_stream) else {
            continue; // could not restore the stream for hyper
        };
        let io = TokioIo::new(StallGuard::new(stream, limits.write_stall));
        // The connection's `max_requests`-th answer says `Connection:
        // close`, so pipelining cannot keep one connection busy for ever.
        let svc = service.clone();
        // Per-connection eviction state (S2 re-review R2 + N3): `served`
        // and `in_flight` decide eligibility — a conn that never served
        // is never an eviction victim; `idle_ms` orders the victims
        // (longest-idle first).
        let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let idle_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let max_requests = limits.max_requests.max(1);
        let per_conn = Arc::new((svc, served.clone(), in_flight.clone(), idle_ms.clone()));
        let peer_ip = peer.ip();
        let rate_limiter = rate_limiter.clone();
        let trusted = trusted.clone();
        let obs = obs.clone();
        let handler =
            hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let per_conn = per_conn.clone();
                let rate_limiter = rate_limiter.clone();
                let trusted = trusted.clone();
                let obs = obs.clone();
                async move {
                    // A refused request is still a SERVED request (F2):
                    // the 429 path used to return before `served` moved,
                    // so a conn that only ever drew 429s stayed
                    // yield-ineligible and escaped the request cap —
                    // camping the slot until max_lifetime. Count first.
                    let (svc, served, in_flight, idle_ms) = &*per_conn;
                    let _flight = FlightGuard::enter(in_flight, idle_ms);
                    let n = served.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    // The client address once — the global limiter and
                    // the /auth limiters share it (an extension so route
                    // extractors never re-derive it differently).
                    let client = client_addr(
                        peer_ip,
                        &trusted,
                        req.headers()
                            .get("cf-connecting-ip")
                            .and_then(|v| v.to_str().ok()),
                    );
                    let mut req = req;
                    req.extensions_mut().insert(ClientAddr(client));
                    if ops_side {
                        req.extensions_mut().insert(OpsListener);
                    }
                    // The per-address request rate. Refused before auth:
                    // a flood without a key is still a flood, and the
                    // check is cheaper than the work behind it.
                    let mut res = if let Some(lim) = rate_limiter.as_ref() {
                        let refused = lim
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .check(client, std::time::Instant::now())
                            .err();
                        match refused {
                            Some(retry) => {
                                // A refused submit counts in `/metrics`
                                // (D-058) — the operator's view only: no
                                // workspace is known before the key is.
                                if let Some(o) = obs.as_ref().filter(|_| is_submit(&req)) {
                                    o.refused(None, observe::wall_ms());
                                }
                                let mut res = Response::builder()
                                    .status(StatusCode::TOO_MANY_REQUESTS)
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .body(axum::body::Body::from(
                                        r#"{"error":"rate limited","code":"rate_limited"}"#,
                                    ))
                                    .unwrap_or_else(|_| Response::new(axum::body::Body::empty()));
                                let secs = (retry.as_secs_f64().ceil() as u64).max(1);
                                res.headers_mut().insert(
                                    header::RETRY_AFTER,
                                    header::HeaderValue::try_from(secs.to_string())
                                        .unwrap_or_else(|_| header::HeaderValue::from_static("1")),
                                );
                                res
                            }
                            None => tower::ServiceExt::oneshot(svc.clone(), req).await?,
                        }
                    } else {
                        tower::ServiceExt::oneshot(svc.clone(), req).await?
                    };
                    // `Connection: close` at the request cap so
                    // pipelining cannot keep one connection busy for
                    // ever — a 429-only conn hits it too (idle-conn
                    // eviction under slot pressure is the yield arm's
                    // job, not the response's).
                    if n >= max_requests {
                        res.headers_mut().insert(
                            header::CONNECTION,
                            header::HeaderValue::from_static("close"),
                        );
                    }
                    Ok::<_, std::convert::Infallible>(res)
                }
            });
        let conn = hyper::server::conn::http1::Builder::new()
            .timer(TokioTimer::new())
            // ATTACK(head-timeout)
            .header_read_timeout(limits.header_timeout)
            .serve_connection(io, handler);
        let mut closing = closing.clone();
        let lifetime = limits.max_lifetime;
        let pool_pressure = permits.clone();
        let head_deadline = limits.pressure_head_deadline;
        let pressure_poll = limits.pressure_poll;
        // The conn joins the idle registry a starved accept reclaims
        // from (N3): the claim channel fires at most once, the entry is
        // removed however the conn ends.
        static NEXT_CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let conn_id = NEXT_CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (evict_tx, mut evict_rx) = tokio::sync::oneshot::channel::<()>();
        idle_registry.insert(
            conn_id,
            IdleEntry {
                bucket: conn_guard.share_bucket(),
                served: served.clone(),
                in_flight: in_flight.clone(),
                idle_ms: idle_ms.clone(),
                evict: evict_tx,
            },
        );
        let lane_response = lane_response.clone();
        let idle_reg = idle_registry.clone();
        conns.spawn(async move {
            let _permit = permit;
            let _conn_guard = conn_guard;
            let _idle_entry = RegGuard {
                id: conn_id,
                reg: idle_reg,
            };
            let mut conn = std::pin::pin!(conn);
            let accepted_at = tokio::time::Instant::now();
            // Under slot pressure a conn camping its permit starves every
            // other tenant's accept — the S2 wedge. A head that never
            // produced a served request dies at `pressure_head_deadline`
            // — the attacker's favourite shape (parked/trickled head) is
            // evicted first, and its refusal is still answered (ruling
            // F1). Idle served conns yield by claim, not self-check: a
            // starved accept pops the longest-idle victim from the
            // registry — over-share buckets first — so evictions are
            // demand-sized and atomic (N3). The first check lands a
            // grace window after the conn opened.
            let pressured = async {
                let mut poll = tokio::time::interval_at(
                    tokio::time::Instant::now() + pressure_poll,
                    pressure_poll,
                );
                poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    poll.tick().await;
                    if pool_pressure.available_permits() != 0 {
                        continue;
                    }
                    let served_n = served.load(std::sync::atomic::Ordering::Relaxed);
                    let in_flight_n = in_flight.load(std::sync::atomic::Ordering::Relaxed);
                    // ATTACK(slot-camp): no deadline and a parked head camps to the S2 wedge.
                    if served_n == 0 && in_flight_n == 0 && accepted_at.elapsed() >= head_deadline {
                        return Pressure::DeadHead; // evicted first
                    }
                }
            };
            // A connection error (a reset, a timed-out head, a stalled
            // write) ends only that connection.
            let evicted = tokio::select! {
                _ = conn.as_mut() => return,
                _ = closing.wait_for(|c| *c) => None,
                () = tokio::time::sleep(lifetime) => None,
                p = pressured => Some(p),
                v = &mut evict_rx => v.ok().map(|()| Pressure::Yielded),
            };
            if evicted == Some(Pressure::DeadHead) {
                // A head that never arrived is not graceful-drained —
                // hyper counts the half-parsed head as in-flight and
                // would wait out the whole header timeout for it. The
                // refusal is still a real answer (ruling F1): write the
                // 503 on the second handle, then drop the conn now —
                // its slot is the point of the eviction.
                if let Some(mut h) = refusal_handle {
                    use tokio::io::AsyncWriteExt as _;
                    let _ = h.write_all(&lane_response[..]).await;
                    let _ = h.shutdown().await;
                }
                return;
            }
            // Shutting down, claimed, or the connection is old: answer
            // the request in flight, then close.
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        });
    }
    drop(listener); // stop accepting: new connections are refused
    closing_tx.send_replace(true); // in-flight requests finish, idle ones close
    while let Some(done) = conns.join_next().await {
        if let Err(e) = done {
            eprintln!("durable-server: a connection task failed: {e}");
        }
    }
    Ok(())
}

/// Admit `peer` to the overload lane: the global lane permit AND the
/// per-address lane budget (S2 re-review N4 — one host may not park the
/// lane). Either missing means the conn is refused at the socket.
fn lane_admit(
    lane: &Arc<tokio::sync::Semaphore>,
    lane_limit: &ConnLimit,
    peer: std::net::IpAddr,
    trusted: bool,
) -> Option<(tokio::sync::OwnedSemaphorePermit, ConnGuard)> {
    let guard = lane_limit.acquire(peer, trusted)?;
    lane.clone()
        .try_acquire_owned()
        .ok()
        .map(|permit| (permit, guard))
}

/// No lane slot left for this conn: the refusal is still owed an answer
/// (S2 re-review ruling F1 — a silent drop is not a refusal the client
/// can read). One non-blocking write on the way out, then the socket
/// drops; a client that drains fast enough sees the 503.
fn answer_best_effort(stream: tokio::net::TcpStream, response: &Arc<Vec<u8>>) {
    let _ = stream.try_write(response);
    drop(stream);
}

/// Real demand reclaiming an idle work slot (S2 re-review N3): a conn
/// that drew a reserve or lane slot means the work pool is dry AND a
/// conn is waiting — evict the longest-idle served conn so the pool
/// turns over, unless the waiter's own bucket already holds more than
/// its share of every pool it touches (the flood's excess is not
/// legitimate demand — it may not kill anyone's conn). The claim inside
/// the registry is atomic: one waiter, at most one victim.
fn evict_for_demand(
    idle: &IdleRegistry,
    conn_limit: &ConnLimit,
    ready_limit: &ConnLimit,
    lane_limit: &ConnLimit,
    peer: std::net::IpAddr,
    pool_size: usize,
) {
    let buckets = conn_limit.buckets().max(1);
    let share = (pool_size / buckets).max(1);
    let held = conn_limit.held(peer) + ready_limit.held(peer) + lane_limit.held(peer);
    if held > share {
        return; // the waiter's bucket is over its fair share
    }
    if let Some(victim) = idle.take_victim(|b| conn_limit.held(b), buckets, pool_size) {
        let _ = victim.evict.send(());
    }
}

/// Spawn one lane conn onto the shared task set — a separate fn so the
/// accept arm's deciding call is a single line (a mutation lands on one
/// line only).
fn lane_answer(
    conns: &mut tokio::task::JoinSet<()>,
    stream: tokio::net::TcpStream,
    permit: tokio::sync::OwnedSemaphorePermit,
    response: &Arc<Vec<u8>>,
    guard: ConnGuard,
) {
    conns.spawn(overload_answer(stream, permit, response.clone(), guard));
}

/// One overload-lane conn: write the fixed `503 + Retry-After`, close
/// the write half so the client reads the answer as a clean EOF rather
/// than racing a reset, then drain the read side for at most
/// `OVERLOAD_LANE_LIFETIME` — closing a socket that still holds unread
/// request bytes is exactly what produces an RST, and an RST is a
/// refusal the client cannot tell from a crash (S2 re-review design
/// item 1). The permit and the per-address slot free with the task.
async fn overload_answer(
    mut stream: tokio::net::TcpStream,
    permit: tokio::sync::OwnedSemaphorePermit,
    response: Arc<Vec<u8>>,
    guard: ConnGuard,
) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let _permit = permit;
    let _lane_guard = guard;
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await; // half-close: FIN after the bytes
    let mut sink = [0u8; 1024];
    let _ = tokio::time::timeout(OVERLOAD_LANE_LIFETIME, async {
        while stream.read(&mut sink).await.unwrap_or(0) != 0 {}
    })
    .await;
}

/// Increments a connection's in-flight count on entry and decrements it
/// when the request future completes, however it completes — the
/// eviction arm's "none in flight" witness — and stamps the conn's
/// idle-age clock, the victim ordering's key.
struct FlightGuard(
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicU64>,
);

impl FlightGuard {
    fn enter(
        c: &Arc<std::sync::atomic::AtomicUsize>,
        idle_ms: &Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(c.clone(), idle_ms.clone())
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.1.store(now_ms, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A connection's stream whose writes fail with `TimedOut` once they have
/// made no progress for `stall`: a client that stops reading its
/// responses would otherwise hold the connection for ever, since hyper's
/// header timeout does not run while a write is pending (review V1).
/// Reads pass through; any write progress restarts the clock.
struct StallGuard<S> {
    inner: S,
    stall: Duration,
    timer: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl<S> StallGuard<S> {
    fn new(inner: S, stall: Duration) -> Self {
        Self {
            inner,
            stall,
            timer: None,
        }
    }

    fn guard<T>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        op: impl FnOnce(
            std::pin::Pin<&mut S>,
            &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<T>>,
    ) -> std::task::Poll<std::io::Result<T>>
    where
        S: Unpin,
    {
        use std::future::Future as _;
        use std::task::Poll;
        match op(std::pin::Pin::new(&mut self.inner), cx) {
            Poll::Ready(r) => {
                self.timer = None;
                Poll::Ready(r)
            }
            Poll::Pending => {
                let stall = self.stall;
                let timer = self
                    .timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(stall)));
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "the client stopped reading",
                    ))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for StallGuard<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for StallGuard<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.guard(cx, |io, cx| io.poll_write(cx, buf))
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.guard(cx, |io, cx| io.poll_write_vectored(cx, bufs))
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.guard(cx, |io, cx| io.poll_flush(cx))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.guard(cx, |io, cx| io.poll_shutdown(cx))
    }
}

/// Resolves on SIGTERM (a supervisor stopping us) or SIGINT (Ctrl-C).
async fn terminate() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    r = tokio::signal::ctrl_c() => {
                        if r.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("durable-server: cannot watch SIGTERM ({e}); only Ctrl-C drains");
                if tokio::signal::ctrl_c().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
    #[cfg(not(unix))]
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use serde_json::{json, Value};
    use std::collections::HashSet;
    use tower::ServiceExt as _;

    const KEY: &str = "dr_ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4";
    const READ_KEY: &str = "dr_aacaacaacaacaacaacaacaacaacaacaacaacaacaacaacaac";

    struct Echo;
    #[async_trait::async_trait]
    impl StepHandler for Echo {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            Outcome::Checkpoint(b"ok".to_vec())
        }
    }

    /// Answers with an output no observability response may contain.
    struct Secret;
    #[async_trait::async_trait]
    impl StepHandler for Secret {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            Outcome::Checkpoint(SECRET_OUTPUT.to_vec())
        }
    }
    const SECRET_OUTPUT: &[u8] = b"SECRET-OUTPUT-7f3a";
    const SECRET_INPUT: &[u8] = b"SECRET-INPUT-91c2";

    struct Harness {
        router: Router,
        keyring: Arc<Keyring>,
        app: App,
        backups: tempfile::TempDir,
        obs: Arc<Observer>,
        _journal: tempfile::TempDir,
    }

    fn harness(with_backup_root: bool) -> Harness {
        harness_with(with_backup_root, None)
    }

    /// `free`: an injected free-space probe over a tiny 4 KiB reserve.
    fn harness_with(
        with_backup_root: bool,
        free: Option<Arc<std::sync::atomic::AtomicU64>>,
    ) -> Harness {
        harness_store(with_backup_root, free, None)
    }

    /// `store`: an account store backing the keyring (S2-1 fixtures).
    fn harness_store(
        with_backup_root: bool,
        free: Option<Arc<std::sync::atomic::AtomicU64>>,
        store: Option<Arc<dyn AccountStore>>,
    ) -> Harness {
        harness_store_local(with_backup_root, free, store, true, false)
    }

    /// `harness_store` with the shipped strict SSRF policy — every other
    /// test wants loopback fixture handlers, this one wants the guard.
    fn harness_strict(store: Option<Arc<dyn AccountStore>>) -> Harness {
        harness_store_local(false, None, store, false, false)
    }

    /// `harness_store` with the `--ops-listen` gate armed (finding 18):
    /// env-key grants hold only when the request carries the
    /// `OpsListener` extension (`raw_ops`); everywhere else they 401.
    fn harness_ops(store: Option<Arc<dyn AccountStore>>) -> Harness {
        harness_store_local(false, None, store, true, true)
    }

    fn harness_store_local(
        with_backup_root: bool,
        free: Option<Arc<std::sync::atomic::AtomicU64>>,
        store: Option<Arc<dyn AccountStore>>,
        allow_local: bool,
        ops_gate: bool,
    ) -> Harness {
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let backups = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
        reg.register("secret", Arc::new(Secret));
        let mut opts = durable_journal::JournalOptions::default();
        if let Some(free) = free {
            opts.reserve_bytes = 4096;
            opts.space = durable_journal::SpaceProbe::new(move |_| {
                Ok(free.load(std::sync::atomic::Ordering::SeqCst))
            });
        }
        let cfg = ShellConfig {
            tick_ms: 5,
            journal: opts,
            ..Default::default()
        };
        let engine = spawn(journal.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let backup_root = with_backup_root.then(|| {
            prepare_backup_root(&backups.path().join("root")).unwrap_or_else(|e| panic!("{e}"))
        });
        let keyring = Arc::new(Keyring::new(
            ApiKey::new(KEY),
            Some(ApiKey::new(READ_KEY)),
            store,
        ));
        let app = test_app(
            engine,
            keyring.clone(),
            false,
            backup_root,
            journal.path(),
            allow_local,
            ops_gate,
        );
        Harness {
            router: router(app.clone(), None),
            keyring,
            obs: app.obs.clone(),
            app,
            backups,
            _journal: journal,
        }
    }

    /// Register `step` for `ws` the way a route PUT plus a reconcile
    /// would: the store row, then the engine's in-process `Echo` — the
    /// tests never want a real HTTP call. Workspaces other than 0 need
    /// this before any `step` spec can admit (S2-4's scoped registry).
    async fn register_step(store: &durable_accounts::Memory, ws: u64, eng: &Engine) {
        store
            .put_handler(ws, "step", "http://127.0.0.1:9/step")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(ws, "step", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// `call`, keeping the headers and the raw body text.
    async fn call_raw(
        router: &Router,
        uri: &str,
        key: Option<&str>,
    ) -> (StatusCode, axum::http::HeaderMap, String) {
        let mut req = Request::builder().method("GET").uri(uri);
        if let Some(k) = key {
            req = req.header("authorization", format!("Bearer {k}"));
        }
        let res = router
            .clone()
            .oneshot(req.body(Body::empty()).unwrap_or_else(|e| panic!("{e}")))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (status, headers) = (res.status(), res.headers().clone());
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        (
            status,
            headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    /// Poll `GET /jobs/{id}` until the run's state is `want`, against a
    /// wall-clock deadline (never a count of sleeps).
    async fn until_state(router: &Router, id: u64, want: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            let (_, v) = call(router, "GET", &format!("/jobs/{id}"), Some(KEY), None).await;
            if v["state"] == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {id} never reached {want}");
    }

    const OBS_ROUTES: [&str; 3] = ["/metrics", "/events", "/jobs/1/events"];

    /// D-058: the three observability reads take `ReadAuthorized`: the
    /// read key or the API key, never nothing — unless `--open-reads`,
    /// and then only for a loopback Host.
    #[tokio::test]
    async fn observability_reads_need_a_read_key() {
        let h = harness(false);
        let (status, _) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("o")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        for uri in OBS_ROUTES {
            for key in [None, Some("wrong"), Some(&KEY[..KEY.len() - 1])] {
                let (status, v) = call(&h.router, "GET", uri, key, None).await;
                assert_eq!(
                    (status, v["code"].clone()),
                    (StatusCode::UNAUTHORIZED, json!("unauthorized")),
                    "GET {uri} key={key:?}"
                );
            }
            for key in [KEY, READ_KEY] {
                let (status, v) = call(&h.router, "GET", uri, Some(key), None).await;
                assert_eq!(status, StatusCode::OK, "GET {uri}: {v}");
            }
        }
        // --open-reads: a loopback Host reads without a key; any other is 401.
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
        let engine =
            spawn(journal.path(), reg, ShellConfig::default()).unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                true,
                None,
                journal.path(),
                true,
                false,
            ),
            None,
        );
        for uri in ["/metrics", "/events"] {
            for (host, want) in [
                ("127.0.0.1:58300", StatusCode::OK),
                ("attacker.example", StatusCode::UNAUTHORIZED),
            ] {
                let req = Request::builder()
                    .uri(uri)
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap_or_else(|e| panic!("{e}"));
                let status = r
                    .clone()
                    .oneshot(req)
                    .await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .status();
                assert_eq!(status, want, "GET {uri} Host {host}");
            }
        }
    }

    /// Every observability answer — success or refusal — says
    /// `Cache-Control: no-store`.
    #[tokio::test]
    async fn observability_answers_are_never_cached() {
        let h = harness(false);
        call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("c")),
        )
        .await;
        for (uri, key, want) in [
            ("/metrics", Some(READ_KEY), StatusCode::OK),
            ("/events", Some(READ_KEY), StatusCode::OK),
            ("/jobs/1/events", Some(READ_KEY), StatusCode::OK),
            ("/metrics", None, StatusCode::UNAUTHORIZED),
            (
                "/metrics?window=2h",
                Some(READ_KEY),
                StatusCode::BAD_REQUEST,
            ),
            ("/events?limit=0", Some(READ_KEY), StatusCode::BAD_REQUEST),
            ("/jobs/999/events", Some(READ_KEY), StatusCode::NOT_FOUND),
        ] {
            let (status, headers, _) = call_raw(&h.router, uri, key).await;
            assert_eq!(status, want, "{uri}");
            assert_eq!(
                headers
                    .get(header::CACHE_CONTROL)
                    .and_then(|v| v.to_str().ok()),
                Some("no-store"),
                "{uri}"
            );
        }
        // Other reads are unchanged.
        let (_, headers, _) = call_raw(&h.router, "/jobs/1", Some(KEY)).await;
        assert!(headers.get(header::CACHE_CONTROL).is_none());
    }

    /// `window` selects the ring and bucket size; every bucket is present;
    /// anything else is 400 `invalid_window` in the error envelope.
    #[tokio::test]
    async fn metrics_windows_and_their_refusals() {
        let h = harness(false);
        for (q, window, bucket, points) in [
            ("", 3_600_000u64, 10_000u64, 360usize),
            ("?window=1h", 3_600_000, 10_000, 360),
            ("?window=15m", 900_000, 10_000, 90),
            ("?window=6h", 21_600_000, 60_000, 360),
            ("?window=24h", 86_400_000, 60_000, 1440),
            ("?window=15m&other=x", 900_000, 10_000, 90),
        ] {
            let (status, v) = call(
                &h.router,
                "GET",
                &format!("/metrics{q}"),
                Some(READ_KEY),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{q}: {v}");
            assert_eq!(v["window_ms"], window, "{q}");
            assert_eq!(v["bucket_ms"], bucket, "{q}");
            let pts = v["points"].as_array().map_or(0, Vec::len);
            assert_eq!(pts, points, "{q}");
            assert_eq!(v["states"].as_object().map_or(0, |o| o.len()), 7, "{q}");
            assert!(v["now"].as_u64() >= v["since"].as_u64(), "{q}");
        }
        for q in [
            "window=2h",
            "window=",
            "window=1H",
            "window=60m",
            "window=1h&window=1h",
            "window=%31h%00",
        ] {
            let (status, v) = call(
                &h.router,
                "GET",
                &format!("/metrics?{q}"),
                Some(READ_KEY),
                None,
            )
            .await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid_window")),
                "{q}"
            );
            assert!(v["error"].is_string(), "{q}: the error envelope");
        }
    }

    /// `limit` is 1–200 (default 50); anything else is 400 `invalid_limit`.
    #[tokio::test]
    async fn the_feed_limit_is_bounded() {
        let h = harness(false);
        for i in 0..60 {
            let mut b = submit_body(&format!("f-{i}"));
            b["not_before"] = json!(9_999_999_999_999u64); // one event each
            let (status, _) = call(&h.router, "POST", "/jobs", Some(KEY), Some(b)).await;
            assert_eq!(status, StatusCode::OK);
        }
        for (q, n) in [
            ("", 50usize),
            ("?limit=1", 1),
            ("?limit=200", 60),
            ("?limit=60", 60),
        ] {
            let (status, v) = call(
                &h.router,
                "GET",
                &format!("/events{q}"),
                Some(READ_KEY),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{q}: {v}");
            assert_eq!(v["events"].as_array().map_or(0, Vec::len), n, "{q}");
        }
        let (_, v) = call(&h.router, "GET", "/events?limit=2", Some(READ_KEY), None).await;
        let id = |n| json!(h.app.pubid.encode(RunId(n)));
        assert_eq!(v["events"][0]["run"], id(60), "newest first: {v}");
        assert_eq!(v["events"][0]["key"], "f-59");
        assert_eq!(v["events"][0]["kind"], "accepted");
        assert_eq!(v["events"][1]["run"], id(59));
        for q in [
            "limit=0",
            "limit=201",
            "limit=abc",
            "limit=-1",
            "limit=",
            "limit=1.5",
            "limit=5&limit=5",
        ] {
            let (status, v) = call(
                &h.router,
                "GET",
                &format!("/events?{q}"),
                Some(READ_KEY),
                None,
            )
            .await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid_limit")),
                "{q}"
            );
        }
    }

    /// `GET /jobs/{id}/events` is 404 exactly where `GET /jobs/{id}` is,
    /// and a malformed id is the same 400 for both.
    #[tokio::test]
    async fn run_events_404_exactly_like_inspect() {
        let h = harness(false);
        call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("n")),
        )
        .await;
        for (id, want) in [
            ("1", StatusCode::OK),
            ("2", StatusCode::NOT_FOUND),
            ("0", StatusCode::NOT_FOUND),
        ] {
            let (a, va) = call(&h.router, "GET", &format!("/jobs/{id}"), Some(KEY), None).await;
            let (b, vb) = call(
                &h.router,
                "GET",
                &format!("/jobs/{id}/events"),
                Some(KEY),
                None,
            )
            .await;
            assert_eq!((a, b), (want, want), "{id}");
            assert_eq!(va["code"], vb["code"], "{id}");
        }
        // An id that decodes to nothing is 404 for both (S2: no oracle).
        let (a, va) = call(&h.router, "GET", "/jobs/abc", Some(KEY), None).await;
        let (b, vb) = call(&h.router, "GET", "/jobs/abc/events", Some(KEY), None).await;
        assert_eq!((a, b), (StatusCode::NOT_FOUND, StatusCode::NOT_FOUND));
        assert_eq!(
            (va["code"].clone(), vb["code"].clone()),
            (json!("not_found"), json!("not_found"))
        );
        // The opaque id the submit returned works for both.
        let pid = h.app.pubid.encode(RunId(1));
        let (a, _) = call(&h.router, "GET", &format!("/jobs/{pid}"), Some(KEY), None).await;
        let (b, _) = call(
            &h.router,
            "GET",
            &format!("/jobs/{pid}/events"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((a, b), (StatusCode::OK, StatusCode::OK));
        let (_, v) = call(&h.router, "GET", "/jobs/404/events", Some(KEY), None).await;
        assert_eq!(v["code"], "not_found");
    }

    /// A run through a handler, then both event views: the timeline in
    /// commit order with the derived events, and neither view carries the
    /// job's input or the checkpointed output in any encoding.
    #[tokio::test]
    async fn event_views_never_show_inputs_or_outputs() {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let h = harness(false);
        let body = json!({
            "op_key": "secret-run",
            "spec": {
                "version": 1,
                "steps": [{ "name": "a", "handler": "secret" }, { "name": "b", "handler": "secret" }],
                "input_b64": b64.encode(SECRET_INPUT),
            },
        });
        let (status, v) = call(&h.router, "POST", "/jobs", Some(KEY), Some(body)).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        until_state(&h.router, 1, "done").await;

        let (status, _, timeline) = call_raw(&h.router, "/jobs/1/events", Some(READ_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        let (_, _, feed) = call_raw(&h.router, "/events?limit=200", Some(READ_KEY)).await;
        let as_numbers = |b: &[u8]| b.iter().map(u8::to_string).collect::<Vec<_>>().join(",");
        for text in [&timeline, &feed] {
            for needle in [
                "SECRET".to_string(),
                b64.encode(SECRET_INPUT),
                b64.encode(SECRET_OUTPUT),
                as_numbers(SECRET_INPUT),
                as_numbers(SECRET_OUTPUT),
            ] {
                assert!(!text.contains(&needle), "{needle} leaked: {text}");
            }
        }
        let t: Value = serde_json::from_str(&timeline).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            (t["run"].clone(), t["key"].clone(), t["state"].clone()),
            (
                json!(h.app.pubid.encode(RunId(1))),
                json!("secret-run"),
                json!("done")
            )
        );
        assert_eq!(
            (t["complete"].clone(), t["reason"].clone()),
            (json!(true), Value::Null)
        );
        let kinds: Vec<&str> = t["events"]
            .as_array()
            .map(|a| a.iter().filter_map(|e| e["kind"].as_str()).collect())
            .unwrap_or_default();
        assert_eq!(
            kinds,
            [
                "accepted",
                "dispatched",
                "checkpointed",
                "dispatched",
                "checkpointed",
                "finished"
            ]
        );
        assert_eq!(t["events"][5]["outcome"], "done");
        assert_eq!(t["events"][1]["attempt"], 1);
        assert!(t["events"][1]["deadline"].as_u64() > t["events"][1]["at"].as_u64());
        let f: Value = serde_json::from_str(&feed).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(f["events"][0]["kind"], "finished");
        assert_eq!(f["events"][0]["key"], "secret-run");
        assert_eq!(f["events"].as_array().map_or(0, Vec::len), 6);
        assert_eq!(f["since"], h.obs.since());

        // The metrics see the same run.
        let (_, m) = call(
            &h.router,
            "GET",
            "/metrics?window=15m",
            Some(READ_KEY),
            None,
        )
        .await;
        let t = &m["totals"];
        assert_eq!(
            (
                t["accepted"].clone(),
                t["completed"].clone(),
                t["steps_ok"].clone()
            ),
            (json!(1), json!(1), json!(2))
        );
        assert_eq!(m["states"]["done"], 1);
        let secret = m["handlers"]
            .as_array()
            .and_then(|a| a.iter().find(|x| x["name"] == "secret"))
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            (secret["calls"].clone(), secret["ok"].clone()),
            (json!(2), json!(2))
        );
        let names: Vec<&str> = m["handlers"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x["name"].as_str()).collect())
            .unwrap_or_default();
        assert_eq!(names, ["secret", "step"], "every handler, by name");
    }

    /// Deduplicated and refused submits are counted where they are
    /// answered.
    #[tokio::test]
    async fn submits_count_as_deduplicated_or_refused() {
        let h = harness(false);
        let mut later = submit_body("same");
        later["not_before"] = json!(9_999_999_999_999u64);
        for _ in 0..3 {
            let (status, v) =
                call(&h.router, "POST", "/jobs", Some(KEY), Some(later.clone())).await;
            assert_eq!(
                (status, v["run_id"].clone()),
                (StatusCode::OK, json!(h.app.pubid.encode(RunId(1))))
            );
        }
        let mut unknown = submit_body("u");
        unknown["spec"]["steps"][0]["handler"] = json!("nope");
        let (status, _) = call(&h.router, "POST", "/jobs", Some(KEY), Some(unknown)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body(&"k".repeat(300))),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        // Not refusals: a malformed body, no key.
        let mut bad = submit_body("b");
        bad["spec"]["input_b64"] = json!("***");
        let (status, _) = call(&h.router, "POST", "/jobs", Some(KEY), Some(bad)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = call(&h.router, "POST", "/jobs", None, Some(submit_body("x"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (_, m) = call(
            &h.router,
            "GET",
            "/metrics?window=15m",
            Some(READ_KEY),
            None,
        )
        .await;
        let t = &m["totals"];
        assert_eq!(
            (
                t["accepted"].clone(),
                t["deduplicated"].clone(),
                t["refused"].clone()
            ),
            (json!(1), json!(2), json!(2)),
            "{t}"
        );
    }

    // --- D-060: the observability reads answer the caller's workspace ----

    const OBS_KEY_A: &str = "dr_0b5a0b5a0b5a0b5a0b5a0b5a0b5a0b5a0b5a0b5a0b5a0b5a";
    const OBS_READ_A: &str = "dr_0b5e0b5e0b5e0b5e0b5e0b5e0b5e0b5e0b5e0b5e0b5e0b5e";
    const OBS_KEY_B: &str = "dr_0b5b0b5b0b5b0b5b0b5b0b5b0b5b0b5b0b5b0b5b0b5b0b5b";

    /// Two tenant workspaces beside workspace 0, each with `step`; A also
    /// has `a-only`. Runs to done: A two, B one, workspace 0 one (the env
    /// key's). Returns the harness and each workspace's run ids.
    async fn observed_tenants() -> (Harness, Vec<String>, Vec<String>, Vec<String>) {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("obs-a")
            .await
            .unwrap_or_else(|e| panic!("a: {e}"));
        let b = store
            .create_workspace("obs-b")
            .await
            .unwrap_or_else(|e| panic!("b: {e}"));
        for (ws, key, perm) in [
            (a.id, OBS_KEY_A, Permission::Write),
            (a.id, OBS_READ_A, Permission::Read),
            (b.id, OBS_KEY_B, Permission::Write),
        ] {
            store
                .create_key(ws, "k", perm, &sha256(key.as_bytes()), &key[..6], None)
                .await
                .unwrap_or_else(|e| panic!("key: {e}"));
        }
        let h = harness_store(false, None, Some(store.clone()));
        register_step(&store, a.id, &h.app.engine).await;
        register_step(&store, b.id, &h.app.engine).await;
        h.app
            .engine
            .put_handler(a.id, "a-only", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut ids = Vec::new();
        for (key, n) in [(OBS_KEY_A, 2), (OBS_KEY_B, 1), (KEY, 1)] {
            let mut mine = Vec::new();
            for i in 0..n {
                let (status, v) = call(
                    &h.router,
                    "POST",
                    "/jobs",
                    Some(key),
                    Some(submit_body(&format!("obs-{i}"))),
                )
                .await;
                assert_eq!(status, StatusCode::OK, "{v}");
                let id = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));
                mine.push(id.to_string());
            }
            for id in &mine {
                until_state_as(&h.router, key, id, "done").await;
            }
            ids.push(mine);
        }
        let ws0 = ids.pop().unwrap_or_default();
        let b_ids = ids.pop().unwrap_or_default();
        let a_ids = ids.pop().unwrap_or_default();
        (h, a_ids, b_ids, ws0)
    }

    /// `until_state` with a tenant's key and the opaque id.
    async fn until_state_as(router: &Router, key: &str, id: &str, want: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            let (_, v) = call(router, "GET", &format!("/jobs/{id}"), Some(key), None).await;
            if v["state"] == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {id} never reached {want}");
    }

    /// D-060: a tenant key's `/metrics` is its own workspace's — counters,
    /// latencies, states and handler stats; another tenant's (and the
    /// totals) never show. A workspace-0 key keeps the global view, with
    /// workspace 0's handlers.
    #[tokio::test]
    async fn metrics_answer_only_the_callers_workspace() {
        let (h, _, _, _) = observed_tenants().await;
        let m = |key: &'static str| {
            let r = h.router.clone();
            async move {
                let (status, v) = call(&r, "GET", "/metrics?window=15m", Some(key), None).await;
                assert_eq!(status, StatusCode::OK, "{v}");
                v
            }
        };
        let names = |v: &Value| -> Vec<String> {
            v["handlers"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x["name"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };
        let calls = |v: &Value, name: &str| {
            v["handlers"]
                .as_array()
                .and_then(|a| a.iter().find(|x| x["name"] == name))
                .map(|x| x["calls"].clone())
                .unwrap_or(Value::Null)
        };
        let (a, ar, b, ops) = (
            m(OBS_KEY_A).await,
            m(OBS_READ_A).await,
            m(OBS_KEY_B).await,
            m(KEY).await,
        );
        let t = |v: &Value| {
            (
                v["totals"]["accepted"].clone(),
                v["totals"]["completed"].clone(),
                v["states"]["done"].clone(),
            )
        };
        // DECISIVE(obs-metrics-scope)
        assert_eq!(t(&a), (json!(2), json!(2), json!(2)), "A: {a}");
        assert_eq!(t(&ar), t(&a), "A's read key sees A's view, not more");
        assert_eq!(t(&b), (json!(1), json!(1), json!(1)), "B: {b}");
        assert_eq!(t(&ops), (json!(4), json!(4), json!(4)), "ops: {ops}");
        // Handlers: each workspace lists its own registrations only.
        assert_eq!(names(&a), ["a-only", "step"]);
        assert_eq!(names(&b), ["step"], "B never learns A's handler names");
        assert_eq!(
            names(&ops),
            ["secret", "step"],
            "the operator's: workspace 0's"
        );
        // DECISIVE(obs-call-scope)
        assert_eq!(calls(&a, "step"), json!(2));
        assert_eq!(calls(&b, "step"), json!(1), "B's step is not A's step");
        assert_eq!(calls(&ops, "step"), json!(1), "workspace 0's step");
        // The gauge series and the points carry no other tenant's numbers.
        let accepted_in_points = |v: &Value| -> u64 {
            v["points"]
                .as_array()
                .map(|p| p.iter().filter_map(|x| x["accepted"].as_u64()).sum())
                .unwrap_or(0)
        };
        assert_eq!(accepted_in_points(&b), 1);
        assert_eq!(accepted_in_points(&a), 2);
    }

    /// D-060: `GET /jobs/{id}/events` for another workspace's run is the
    /// same 404 a missing run gets — for a tenant key and for the
    /// workspace-0 env key alike, exactly like `GET /jobs/{id}`.
    #[tokio::test]
    async fn run_events_answer_only_the_callers_workspace() {
        let (h, a_ids, b_ids, ws0) = observed_tenants().await;
        let get = |key: &'static str, id: String| {
            let r = h.router.clone();
            async move { call(&r, "GET", &format!("/jobs/{id}/events"), Some(key), None).await }
        };
        let (status, v) = get(OBS_KEY_A, a_ids[0].clone()).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["run"], json!(a_ids[0]), "the opaque id, never a number");
        assert_eq!(v["events"][0]["kind"], "accepted");
        let (missing_status, missing) = get(OBS_KEY_B, "AAAAAAAAAAA".into()).await;
        assert_eq!(missing_status, StatusCode::NOT_FOUND);
        for (key, foreign) in [
            (OBS_KEY_B, a_ids[0].clone()),
            (OBS_READ_A, b_ids[0].clone()),
            (OBS_KEY_A, ws0[0].clone()),
            (KEY, a_ids[1].clone()),
            (KEY, b_ids[0].clone()),
        ] {
            let (status, v) = get(key, foreign.clone()).await;
            // DECISIVE(obs-run-scope)
            assert_eq!(
                (status, &v),
                (StatusCode::NOT_FOUND, &missing),
                "{key} reading {foreign}: identical to a missing run"
            );
            let (inspect, _) = call(
                &h.router,
                "GET",
                &format!("/jobs/{foreign}"),
                Some(key),
                None,
            )
            .await;
            assert_eq!(
                inspect,
                StatusCode::NOT_FOUND,
                "exactly like GET /jobs/{{id}}"
            );
        }
    }

    /// D-060: `GET /events` is the caller's workspace's feed: every event
    /// is one of its own runs, by opaque id; none of another workspace's.
    #[tokio::test]
    async fn the_feed_answers_only_the_callers_workspace() {
        let (h, a_ids, b_ids, ws0) = observed_tenants().await;
        let runs = |key: &'static str| {
            let r = h.router.clone();
            async move {
                let (status, v) = call(&r, "GET", "/events?limit=200", Some(key), None).await;
                assert_eq!(status, StatusCode::OK, "{v}");
                let mut ids: Vec<String> = v["events"]
                    .as_array()
                    .map(|e| {
                        e.iter()
                            .map(|x| {
                                x["run"]
                                    .as_str()
                                    .unwrap_or_else(|| panic!("an opaque string id: {x}"))
                                    .to_string()
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                ids.sort();
                ids.dedup();
                ids
            }
        };
        let sorted = |mut v: Vec<String>| {
            v.sort();
            v
        };
        // DECISIVE(obs-feed-scope)
        assert_eq!(runs(OBS_KEY_A).await, sorted(a_ids.clone()));
        assert_eq!(runs(OBS_READ_A).await, sorted(a_ids));
        assert_eq!(runs(OBS_KEY_B).await, sorted(b_ids));
        assert_eq!(runs(KEY).await, sorted(ws0), "env keys: workspace 0's only");
    }

    /// White-hat finding B: behind Caddy every TCP peer is the loopback
    /// hop, so the deploy trusts 127.0.0.1/32 and ::1/128 as the proxy
    /// (deploy/durable-run.sh) and Caddy overwrites `CF-Connecting-IP`
    /// with the client it resolved. Then each visitor gets its own bucket
    /// (IPv6 by /64), a header from an untrusted peer is ignored, and a
    /// dual-stack listener — which sees `::ffff:127.0.0.1` — still trusts
    /// the hop. One token per 1000 s makes every refusal deterministic.
    #[tokio::test]
    async fn visitors_behind_the_loopback_proxy_hop_get_their_own_buckets() {
        let hop = |nets: &[&str]| HttpLimits {
            rate_per_addr: 0.001,
            trusted_proxies: TrustedProxies(
                nets.iter()
                    .map(|n| IpNet::parse(n).unwrap_or_else(|e| panic!("{e}")))
                    .collect(),
            ),
            ..HttpLimits::default()
        };
        let client = reqwest::Client::new();
        let get = |base: String, visitor: &'static str| {
            let c = client.clone();
            async move {
                c.get(format!("{base}/health"))
                    .header("cf-connecting-ip", visitor)
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .status()
                    .as_u16()
            }
        };
        // The deploy's trust: the loopback hop, both families.
        let (addr, _stop, _h) = serving(hop(&["127.0.0.1/32", "::1/128"])).await;
        let base = format!("http://{addr}");
        for (visitor, want) in [
            ("198.51.100.1", 200),
            ("198.51.100.1", 429), // its own bucket is spent…
            ("198.51.100.2", 200), // …another visitor's is not
            ("2001:db8:1:1::1", 200),
            ("2001:db8:1:1::2", 429), // the same /64: one bucket
            // (another /64 in the same /48 is charged to the /48's
            // aggregate too — the F3 bound — so the next visitor is a
            // different /48)
            ("2001:db8:2::1", 200),
            ("::ffff:198.51.100.3", 200),
            ("198.51.100.3", 429), // a mapped form is the same IPv4 client
        ] {
            assert_eq!(get(base.clone(), visitor).await, want, "{visitor}");
        }
        // A peer outside the trusted hop: its header is ignored, so every
        // "visitor" it claims shares the peer's one bucket.
        let (addr, _stop2, _h2) = serving(hop(&["::1/128"])).await;
        let base = format!("http://{addr}");
        assert_eq!(get(base.clone(), "198.51.100.10").await, 200);
        assert_eq!(
            get(base.clone(), "198.51.100.11").await,
            429,
            "spoofed header believed"
        );
        // Dual-stack: the IPv4 hop arrives as ::ffff:127.0.0.1 and must
        // still be the trusted 127.0.0.1.
        let (addr, _stop3, _h3) = serving_on(hop(&["127.0.0.1/32"]), "[::]:0").await;
        let base = format!("http://127.0.0.1:{}", addr.port());
        assert_eq!(get(base.clone(), "198.51.100.20").await, 200);
        // DECISIVE(peer-canonical)
        assert_eq!(
            get(base.clone(), "198.51.100.21").await,
            200,
            "a dual-stack listener must canonicalize the mapped peer"
        );
    }

    /// A submit refused 429 by the per-address limit counts as refused; a
    /// limited read does not.
    #[tokio::test]
    async fn a_rate_limited_submit_counts_as_refused() {
        // One request, then one token per 1000 s: deterministic refusals.
        let limits = HttpLimits {
            rate_per_addr: 0.001,
            ..HttpLimits::default()
        };
        let (addr, _stop, h) = serving(limits).await;
        let client = reqwest::Client::new();
        let post = || {
            client
                .post(format!("http://{addr}/jobs"))
                .bearer_auth(KEY)
                .json(&submit_body("rl"))
                .send()
        };
        let first = post().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(first.status().as_u16(), 200);
        let second = post().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(second.status().as_u16(), 429);
        let read = client
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(read.status().as_u16(), 429);
        let m = h.obs.metrics(
            observe::Scope::All,
            Window::M15,
            observe::wall_ms(),
            Default::default(),
            &[],
        );
        assert_eq!(m.totals.counters.refused, 1);
    }

    async fn call(
        router: &Router,
        method: &str,
        uri: &str,
        key: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(k) = key {
            req = req.header("authorization", format!("Bearer {k}"));
        }
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap_or_else(|e| panic!("{e}"));
        let res = router
            .clone()
            .oneshot(req)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let status = res.status();
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn submit_body(op_key: &str) -> Value {
        json!({
            "op_key": op_key,
            "spec": {
                "version": 1,
                "steps": [{ "name": "a", "handler": "step" }],
                "input_b64": "aGk=",
            },
        })
    }

    /// Every mutating and admin route refuses a request without the key
    /// or with a wrong one — before the body is even parsed.
    #[tokio::test]
    async fn mutating_and_admin_routes_require_the_api_key() {
        let h = harness(true);
        let routes: [(&str, &str, Option<Value>); 6] = [
            ("POST", "/jobs", Some(submit_body("k"))),
            ("GET", "/jobs/1/spec", None),
            ("POST", "/jobs/1/cancel", None),
            ("POST", "/admin/compact", None),
            ("POST", "/admin/backup", Some(json!({ "name": "b" }))),
            ("GET", "/admin/diagnostics", None),
        ];
        for (method, uri, body) in routes {
            for key in [None, Some("wrong"), Some(&KEY[..KEY.len() - 1])] {
                let (status, v) = call(&h.router, method, uri, key, body.clone()).await;
                assert_eq!(
                    status,
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} key={key:?}"
                );
                assert_eq!(v["code"], "unauthorized");
            }
        }
        let (_, list) = call(&h.router, "GET", "/jobs", Some(KEY), None).await;
        assert_eq!(list["runs"], json!([]), "refused submits journaled nothing");
        assert!(
            std::fs::read_dir(h.backups.path().join("root"))
                .map(|d| d.count())
                .unwrap_or_else(|e| panic!("{e}"))
                == 0,
            "a refused backup wrote nothing"
        );
    }

    #[tokio::test]
    async fn the_key_is_accepted_everywhere() {
        let h = harness(true);
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("k")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let id = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));
        let (status, _) = call(&h.router, "GET", &format!("/jobs/{id}"), Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/jobs", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/health", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/admin/diagnostics", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// N7 compat: workspace 0 still answers v1.0.1's bare numeric run
    /// ids — a v1.0.1 client holding `"1"` keeps working. The issued id
    /// stays opaque, and numeric resolution never opens for a nonzero
    /// workspace (their internal ids would leak across tenants).
    #[tokio::test]
    async fn workspace_zero_still_answers_numeric_run_ids() {
        let h = harness(true);
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("k")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let opaque = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));
        assert!(
            !opaque.chars().all(|c| c.is_ascii_digit()),
            "issued ids are opaque: {opaque}"
        );
        // RunId(1) is the first run — resolvable both ways.
        let (status, v2) = call(&h.router, "GET", "/jobs/1", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK, "{v2}");
        assert_eq!(v2["id"].as_str(), Some(opaque), "same run, either id");
        // A numeric id never issued is still an honest 404 — the
        // fallback resolves only real internal ids.
        let (status, _) = call(&h.router, "GET", "/jobs/424242", Some(KEY), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // The opaque form routes normally — and because the opaque
        // decode runs first, an issued id can never be misread as a
        // compat number.
        let (status, _) = call(
            &h.router,
            "GET",
            &format!("/jobs/{opaque}"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// Checklist (key scopes): the read-only key reads, and every
    /// mutating and admin route refuses it 403 — a valid key, wrong scope.
    #[tokio::test]
    async fn the_read_key_reads_and_nothing_else() {
        let h = harness(true);
        let mut later = submit_body("k");
        later["not_before"] = json!(9_999_999_999_999u64); // stays queued
        let (status, v) = call(&h.router, "POST", "/jobs", Some(KEY), Some(later)).await;
        assert_eq!(status, StatusCode::OK);
        let id = v["run_id"].as_str().unwrap_or_default().to_string();
        for uri in ["/jobs".to_string(), format!("/jobs/{id}")] {
            let (status, _) = call(&h.router, "GET", &uri, Some(READ_KEY), None).await;
            assert_eq!(status, StatusCode::OK, "GET {uri}");
        }
        let routes: [(&str, String, Option<Value>); 6] = [
            ("POST", "/jobs".into(), Some(submit_body("r"))),
            ("GET", format!("/jobs/{id}/spec"), None),
            ("POST", format!("/jobs/{id}/cancel"), None),
            ("POST", "/admin/compact".into(), None),
            ("POST", "/admin/backup".into(), Some(json!({ "name": "b" }))),
            ("GET", "/admin/diagnostics".into(), None),
        ];
        for (method, uri, body) in routes {
            let (status, v) = call(&h.router, method, &uri, Some(READ_KEY), body).await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::FORBIDDEN, json!("forbidden")),
                "{method} {uri}"
            );
        }
        let (_, v) = call(&h.router, "GET", &format!("/jobs/{id}"), Some(KEY), None).await;
        assert_eq!(v["state"], "queued", "the read key cancelled nothing");
    }

    /// D-051: the API key reads back exactly the spec a run was submitted with, so a
    /// controller can resubmit the same work under a new op_key; a missing run is 404.
    #[tokio::test]
    async fn the_api_key_reads_back_a_runs_submitted_spec() {
        let h = harness(true);
        let mut body = submit_body("spec-1");
        body["not_before"] = json!(9_999_999_999_999u64); // stays queued
        let (status, v) = call(&h.router, "POST", "/jobs", Some(KEY), Some(body.clone())).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let id = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));
        let (status, v) = call(
            &h.router,
            "GET",
            &format!("/jobs/{id}/spec"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["key"], "spec-1");
        assert_eq!(
            v["spec"], body["spec"],
            "the spec round-trips to a submit body"
        );
        let (status, _) = call(&h.router, "GET", "/jobs/999/spec", Some(KEY), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// S2-1: a store-verified key submits into its own workspace, and
    /// dedup is keyed on (workspace, op_key) — the same op_key under two
    /// workspaces lands two independent runs, while a repeat inside one
    /// workspace still dedups.
    #[tokio::test]
    async fn a_store_key_submits_into_its_workspace() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("alpha")
            .await
            .unwrap_or_else(|e| panic!("workspace: {e}"));
        let token = "dr_ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8ab8";
        store
            .create_key(
                ws.id,
                "main",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_al",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let h = harness_store(true, None, Some(store.clone()));
        register_step(&store, ws.id, &h.app.engine).await;

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(token),
            Some(submit_body("same-key")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let run_a = v["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{v}"))
            .to_string();

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("same-key")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let run_b = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));
        assert_ne!(
            run_a, run_b,
            "the same op_key in different workspaces must not dedup across"
        );

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(token),
            Some(submit_body("same-key")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(
            v["run_id"].as_str(),
            Some(run_a.as_str()),
            "the same key inside one workspace still dedups"
        );
    }

    /// S2-1 failure case: a down account store fails closed on uncached
    /// bearers (503 `accounts_unavailable`), while env ops keys and an
    /// already-cached grant keep working inside the grant TTL.
    #[tokio::test]
    async fn a_down_account_store_fails_closed_but_keeps_cached_keys() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("beta")
            .await
            .unwrap_or_else(|e| panic!("workspace: {e}"));
        let token = "dr_abbabbabbabbabbabbabbabbabbabbabbabbabbabbabbabb";
        store
            .create_key(
                ws.id,
                "main",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_be",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let h = harness_store(true, None, Some(store.clone()));

        // First verified read caches the grant.
        let (status, _) = call(&h.router, "GET", "/jobs", Some(token), None).await;
        assert_eq!(status, StatusCode::OK);

        store.set_unavailable(true);

        let (status, _) = call(&h.router, "GET", "/jobs", Some(token), None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a verified key inside its TTL survives the outage"
        );

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some("dr_ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2ab2"),
            Some(submit_body("x")),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(v["code"], "accounts_unavailable");

        // Env ops keys verify locally and never touch the store.
        let (status, _) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("y")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// S2-1: revocation performed through this process invalidates the
    /// cached grant write-through — the key denies immediately rather
    /// than riding out its TTL.
    #[tokio::test]
    async fn a_revoked_key_denies_without_waiting_out_the_cache() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("gamma")
            .await
            .unwrap_or_else(|e| panic!("workspace: {e}"));
        let token = "dr_abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc";
        let info = store
            .create_key(
                ws.id,
                "main",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_ga",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let h = harness_store(true, None, Some(store.clone()));

        let (status, _) = call(&h.router, "GET", "/jobs", Some(token), None).await;
        assert_eq!(status, StatusCode::OK);

        store
            .revoke_key(ws.id, info.id)
            .await
            .unwrap_or_else(|e| panic!("revoke: {e}"));
        h.keyring.invalidate(sha256(token.as_bytes()));

        let (status, _) = call(&h.router, "GET", "/jobs", Some(token), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// `call`, keeping the headers: the no-u64-leak scan checks them too.
    async fn call_full(
        router: &Router,
        method: &str,
        uri: &str,
        key: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(k) = key {
            req = req.header("authorization", format!("Bearer {k}"));
        }
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap_or_else(|e| panic!("{e}"));
        let res = router
            .clone()
            .oneshot(req)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// S2-2's isolation matrix, end to end: workspace B's key can neither
    /// see nor cancel workspace A's run — every reach is 404, the same
    /// answer a nonexistent id gets, so a foreign run leaks nothing.
    #[tokio::test]
    async fn workspaces_are_isolated_end_to_end() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("a: {e}"));
        let b = store
            .create_workspace("b")
            .await
            .unwrap_or_else(|e| panic!("b: {e}"));
        let key_a = "dr_ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7ab7";
        let key_b = "dr_abaabaabaabaabaabaabaabaabaabaabaabaabaabaabaaba";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("ka: {e}"));
        store
            .create_key(
                b.id,
                "k",
                Permission::Write,
                &sha256(key_b.as_bytes()),
                "dr_b",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("kb: {e}"));
        let h = harness_store(true, None, Some(store.clone()));
        register_step(&store, a.id, &h.app.engine).await;
        register_step(&store, b.id, &h.app.engine).await;

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_a),
            Some(submit_body("s")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let run_a = v["run_id"].as_str().unwrap_or_else(|| panic!("{v}"));

        // B's reads of A's run look exactly like a nonexistent run.
        let (status, v) = call(
            &h.router,
            "GET",
            &format!("/jobs/{run_a}"),
            Some(key_b),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
        assert_eq!(v["code"], "not_found");
        let (status, v) = call(&h.router, "GET", "/jobs/999999", Some(key_b), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            v["code"], "not_found",
            "a foreign id must read identically to a missing one"
        );

        let (status, v) = call(
            &h.router,
            "GET",
            &format!("/jobs/{run_a}/spec"),
            Some(key_b),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
        let (status, v) = call(
            &h.router,
            "POST",
            &format!("/jobs/{run_a}/cancel"),
            Some(key_b),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");

        // B's list contains only B's runs.
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_b),
            Some(submit_body("s")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let (status, v) = call(&h.router, "GET", "/jobs", Some(key_b), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let ids: Vec<&str> = v["runs"]
            .as_array()
            .unwrap_or_else(|| panic!("{v}"))
            .iter()
            .filter_map(|r| r["id"].as_str())
            .collect();
        assert!(
            !ids.contains(&run_a),
            "B's list must not show A's run: {ids:?}"
        );
        // DECISIVE(list-scope)
        assert_eq!(ids.len(), 1, "B sees exactly its own run");

        // A sees A.
        let (status, v) = call(&h.router, "GET", "/jobs", Some(key_a), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["runs"].as_array().map(|r| r.len()), Some(1));
    }

    /// S2 review #7's required property: two submits from one workspace
    /// must reveal nothing about other workspaces' volume. Workspace A
    /// submits, workspace B submits fifty runs, A submits again — A's two
    /// public ids are opaque strings indistinguishable from any other
    /// pair (the test holds the key, so it *verifies* the internal gap is
    /// really 51 while asserting nothing on the wire betrays it).
    #[tokio::test]
    async fn public_ids_hide_other_workspaces_volume() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let b = store
            .create_workspace("b")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0";
        let key_b = "dr_1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d";
        for (ws, k) in [(a.id, key_a), (b.id, key_b)] {
            store
                .create_key(
                    ws,
                    "k",
                    Permission::Write,
                    &sha256(k.as_bytes()),
                    "dr_t",
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let h = harness_store(true, None, Some(store.clone()));
        register_step(&store, a.id, &h.app.engine).await;
        register_step(&store, b.id, &h.app.engine).await;

        async fn submit_n(router: &Router, key: &str, tag: &str, n: usize) -> Vec<String> {
            let mut ids = Vec::new();
            for i in 0..n {
                let (_, _, v) = call_full(
                    router,
                    "POST",
                    "/jobs",
                    Some(key),
                    Some(submit_body(&format!("{tag}-{i}"))),
                )
                .await;
                ids.push(
                    v["run_id"]
                        .as_str()
                        .unwrap_or_else(|| panic!("{v}"))
                        .to_string(),
                );
            }
            ids
        }
        let first = submit_n(&h.router, key_a, "a1", 1).await.remove(0);
        let _b = submit_n(&h.router, key_b, "b", 50).await; // B's volume between A's two submits
        let second = submit_n(&h.router, key_a, "a2", 1).await.remove(0);

        // The wire forms: fixed-width opaque strings, nothing numeric.
        for s in [&first, &second] {
            assert_eq!(s.len(), 11, "public id {s}");
            assert!(s.parse::<u64>().is_err(), "{s} must not be a bare integer");
            assert_ne!(first, second, "distinct runs, distinct public ids");
        }
        // What A could never see without the key: the ids really are 51
        // apart internally. The permutation hides it, not a mapping table
        // that could lag.
        let (ia, ib) = (
            h.app
                .pubid
                .decode(&first)
                .unwrap_or_else(|| panic!("{first}")),
            h.app
                .pubid
                .decode(&second)
                .unwrap_or_else(|| panic!("{second}")),
        );
        assert_eq!(ib - ia, 51, "B's 50 submits sit between A's two ids");
        // A's *other* endpoints agree: the public id is the only id it
        // ever sees — inspect, spec, cancel all take it verbatim.
        let (status, _) = call(
            &h.router,
            "GET",
            &format!("/jobs/{second}"),
            Some(key_a),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // And a numeric guess or a forged string still answers like a
        // missing run — the permutation adds no new oracle.
        for probe in ["1", &second[..5], "AAAAAAAAAAA"] {
            let (status, _) = call(
                &h.router,
                "GET",
                &format!("/jobs/{probe}"),
                Some(key_a),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{probe}");
        }
    }

    /// The coordinator's rule: no response body or header may carry the
    /// internal sequential workspace id. Walks every response's JSON and
    /// headers for a workspace-shaped field holding the u64.
    #[tokio::test]
    async fn no_response_or_header_carries_the_internal_ws_id() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("scan")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let token = "dr_aaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaaeaae";
        store
            .create_key(
                ws.id,
                "k",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_s",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store.clone()));
        register_step(&store, ws.id, &h.app.engine).await;
        let internal = ws.id; // the id that must never leave the process

        // Seed a run + a workspace so the routes have data to show.
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(token),
            Some(submit_body("r")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let jid = v["run_id"].as_str().unwrap_or_default().to_string();
        let pid = ws.public_id.to_string();

        let routes: Vec<(String, Option<&str>)> = vec![
            ("/whoami".into(), Some(token)),
            ("/admin/workspaces".into(), Some(KEY)),
            (format!("/admin/workspaces/{pid}"), Some(KEY)),
            (format!("/admin/workspaces/{pid}/keys"), Some(KEY)),
            ("/jobs".into(), Some(token)),
            (format!("/jobs/{jid}"), Some(token)),
            (format!("/jobs/{jid}/spec"), Some(token)),
            ("/admin/diagnostics".into(), Some(KEY)),
        ];
        fn scan_json(v: &Value, internal: u64, path: &str) {
            match v {
                Value::Object(map) => {
                    for (k, val) in map {
                        if matches!(
                            k.as_str(),
                            "ws" | "workspace_id" | "internal_id" | "workspace"
                        ) && val.as_u64() == Some(internal)
                        {
                            panic!(
                                "{path}: field {k:?} carries the internal workspace id {internal}"
                            );
                        }
                        // S3: run ids are strings now — a *numeric*
                        // run_id (or any ws-named value) is an internal
                        // sequence leaking. Key `id`s stay numeric:
                        // they are the management API's own handle.
                        if matches!(k.as_str(), "run_id" | "ws") && val.is_number() {
                            panic!(
                                "{path}: field {k:?} is numeric ({val}) — \
                                 an internal id, not the opaque public form"
                            );
                        }
                        scan_json(val, internal, path);
                    }
                }
                Value::Array(items) => items.iter().for_each(|v| scan_json(v, internal, path)),
                _ => {}
            }
        }
        for (uri, key) in routes {
            let (status, headers, body) = call_full(&h.router, "GET", &uri, key, None).await;
            assert_eq!(status, StatusCode::OK, "GET {uri}: {body}");
            scan_json(&body, internal, &uri);
            for (name, value) in headers.iter() {
                if name.as_str().contains("workspace")
                    || name.as_str() == "x-ws"
                    || name.as_str() == "ws"
                {
                    assert_ne!(
                        value.to_str().ok().and_then(|v| v.parse::<u64>().ok()),
                        Some(internal),
                        "GET {uri}: header {name} carries the internal workspace id"
                    );
                }
            }
        }
    }

    /// whoami exposes the workspace's public id and the key's permission —
    /// env ops keys read as the nil UUID (workspace 0 / compat mode).
    #[tokio::test]
    async fn whoami_reports_public_id_and_permission() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("who")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let token = "dr_ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5ab5";
        store
            .create_key(
                ws.id,
                "k",
                Permission::Read,
                &sha256(token.as_bytes()),
                "dr_w",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store));

        let (status, v) = call(&h.router, "GET", "/whoami", Some(token), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["workspace"], json!(ws.public_id.to_string()));
        assert_eq!(v["permission"], json!("read"));

        let (status, v) = call(&h.router, "GET", "/whoami", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["workspace"], json!(uuid::Uuid::nil().to_string()));
        assert_eq!(v["permission"], json!("admin"));
    }

    /// The full ops path over HTTP: create workspace → create key → the
    /// key submits → revoke → the key denies immediately (write-through).
    #[tokio::test]
    async fn the_key_lifecycle_over_http() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let h = harness_store(true, None, Some(store.clone()));

        let (status, v) = call(
            &h.router,
            "POST",
            "/admin/workspaces",
            Some(KEY),
            Some(json!({ "name": "tenant" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let pid = v["public_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{v}"))
            .to_string();
        assert!(
            uuid::Uuid::parse_str(&pid).is_ok(),
            "public_id must be opaque: {v}"
        );

        let (status, v) = call(
            &h.router,
            "POST",
            &format!("/admin/workspaces/{pid}/keys"),
            Some(KEY),
            Some(json!({ "name": "ci", "permission": "admin" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let key_id = v["id"].as_u64().unwrap_or_else(|| panic!("{v}"));
        let issued = v["key"]
            .as_str()
            .unwrap_or_else(|| panic!("{v}"))
            .to_string();
        // The workspace's internal id never left the process — the test
        // finds it through the store to give the ws a `step` handler.
        let ws_id = store
            .list_workspaces(100, None)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .into_iter()
            .find(|w| w.public_id.to_string() == pid)
            .unwrap_or_else(|| panic!("workspace {pid}"))
            .id;
        register_step(&store, ws_id, &h.app.engine).await;
        assert!(issued.starts_with("drk_"));

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(&issued),
            Some(submit_body("k")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v} — the issued key must submit");

        let (status, v) = call(
            &h.router,
            "GET",
            &format!("/admin/workspaces/{pid}/keys"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(v["keys"].as_array().map(|k| k.len()), Some(1));
        assert_eq!(v["keys"][0]["prefix"].as_str().unwrap_or(""), &issued[..11]);

        let (status, _) = call(
            &h.router,
            "DELETE",
            &format!("/admin/workspaces/{pid}/keys/{key_id}"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/jobs", Some(&issued), None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "revoked denies at once, no TTL wait"
        );
    }

    /// Ops scope: a non-ws-0 admin cannot reach the management surface.
    #[tokio::test]
    async fn workspace_management_needs_the_ops_scope() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("tenant")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let token = "dr_ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3ab3";
        store
            .create_key(
                ws.id,
                "k",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_t",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store));

        let (status, _) = call(&h.router, "GET", "/admin/workspaces", Some(token), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = call(
            &h.router,
            "POST",
            "/admin/workspaces",
            Some(token),
            Some(json!({ "name": "escalate" })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// Unknown or malformed public ids: the unknown one is 404, the
    /// unparseable one is the path extractor's 400 — never an oracle.
    #[tokio::test]
    async fn unknown_public_ids_are_404() {
        use durable_accounts::Memory;
        let h = harness_store(true, None, Some(Arc::new(Memory::new())));
        let pid = uuid::Uuid::from_u128(0xfeed);
        let (status, v) = call(
            &h.router,
            "GET",
            &format!("/admin/workspaces/{pid}"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
        assert_eq!(v["code"], "not_found");
        let (status, v) = call(
            &h.router,
            "GET",
            "/admin/workspaces/not-a-uuid",
            Some(KEY),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(v["code"], "bad_path");
    }

    /// Compat mode (no DATABASE_URL): the workspace surface does not
    /// exist — 404 with the config reason, not a retryable outage.
    #[tokio::test]
    async fn compat_mode_workspace_routes_are_404() {
        let h = harness(true);
        let (status, v) = call(&h.router, "GET", "/admin/workspaces", Some(KEY), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
        assert!(v["error"].as_str().unwrap_or("").contains("DATABASE_URL"));
        // Rotation is meaningless without a store — the same 404.
        let (status, v) = call(&h.router, "POST", "/handler-secret", Some(KEY), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
    }

    /// S2-4's isolation: a handler registered in workspace A is invisible
    /// to workspace B — B's spec naming it is `unregistered_handler`, B's
    /// list is empty, and B cannot delete it. Registering/deleting is
    /// scoped to the caller's own workspace.
    #[tokio::test]
    async fn a_workspaces_handlers_are_isolated() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let b = store
            .create_workspace("b")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6ab6";
        let key_b = "dr_ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9ab9";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        store
            .create_key(
                b.id,
                "k",
                Permission::Write,
                &sha256(key_b.as_bytes()),
                "dr_b",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store));

        let spec_with = |handler: &str| {
            json!({"op_key": "o", "spec": {"version": 1,
                "steps": [{"name": "s", "handler": handler}], "input_b64": "aGk="}})
        };
        // B's spec naming what only A will register → unregistered_handler.
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_b),
            Some(spec_with("h")),
        )
        .await;
        assert_eq!(
            (status, v["code"].as_str().unwrap_or("")),
            (StatusCode::CONFLICT, "unregistered_handler"),
            "{v}"
        );

        // A registers it — then A's spec submits while B's still refuses.
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/h",
            Some(key_a),
            Some(json!({ "url": "http://127.0.0.1:9/h" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_a),
            Some(spec_with("h")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_b),
            Some(spec_with("h")),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{v}");

        // Lists are scoped; deletes are scoped.
        let (status, v) = call(&h.router, "GET", "/handlers", Some(key_b), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["handlers"].as_array().map(|h| h.len()), Some(0), "{v}");
        let (status, v) = call(&h.router, "GET", "/handlers", Some(key_a), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["handlers"][0]["url"], "http://127.0.0.1:9/h", "{v}");
        let (status, _) = call(&h.router, "DELETE", "/handlers/h", Some(key_b), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "B cannot delete A's handler");

        // Deleting under A removes it for A — the spec fails admission again.
        let (status, _) = call(&h.router, "DELETE", "/handlers/h", Some(key_a), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_a),
            Some(spec_with("h")),
        )
        .await;
        assert_eq!(
            (status, v["code"].as_str().unwrap_or("")),
            (StatusCode::CONFLICT, "unregistered_handler"),
            "{v}"
        );
    }

    /// A route PUT persists to the store, the engine AND the snapshot —
    /// and the snapshot rebuilds a fresh engine's registry exactly the
    /// way startup's load path does (the restart-during-outage case).
    #[tokio::test]
    async fn the_snapshot_rebuilds_the_registry_on_restart() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0ab0";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store));
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/h",
            Some(key_a),
            Some(json!({ "url": "http://127.0.0.1:9/h" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");

        // The snapshot on disk carries the entry…
        let entries = load_registry_file(&h.app.registry_file).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            entries,
            vec![RegistryEntry {
                ws: a.id,
                name: "h".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: Some(hex(&a.handler_secret)),
            }]
        );

        // …and replaying it into a fresh engine — main()'s startup path —
        // registers `h` in a's workspace so its spec admits.
        let j2 = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let engine2 = spawn(
            j2.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        for e in entries {
            let secret = e.secret.as_deref().and_then(unhex).map(Arc::new);
            engine2
                .put_handler(
                    e.ws,
                    &e.name,
                    h.app.handler_factory.make_signed(&e.url, secret),
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let spec = JobSpec {
            version: 1,
            steps: vec![StepSpec {
                name: "s".into(),
                handler: "h".into(),
            }],
            input: b"hi".to_vec(),
        };
        engine2
            .submit_scoped(a.id, OperationKey("k".into()), spec)
            .await
            .unwrap_or_else(|e| panic!("snapshot-restored registry must admit: {e}"));
    }

    /// The 256 bound is enforced before the store write: seeded to the
    /// limit, a 257th registration answers 409 `registry_full` and never
    /// reaches the store — an update to an existing name still passes.
    #[tokio::test]
    async fn the_registry_bound_refuses_a_257th_name() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        for i in 0..MAX_HANDLERS_PER_WS {
            store
                .put_handler(a.id, &format!("h{i}"), "http://127.0.0.1:9/h")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let h = harness_store(true, None, Some(store));
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/one-too-many",
            Some(key_a),
            Some(json!({ "url": "http://127.0.0.1:9/x" })),
        )
        .await;
        assert_eq!(
            (status, v["code"].as_str().unwrap_or("")),
            (StatusCode::CONFLICT, "registry_full"),
            "{v}"
        );
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/h0",
            Some(key_a),
            Some(json!({ "url": "http://127.0.0.1:9/other" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
    }

    /// S3: every registry mutation bumps `registry_epoch` — the marker a
    /// running reconcile pass checks before each apply, so a pass read
    /// before the mutation discards itself instead of resurrecting a
    /// rotated secret or a deleted handler for up to 30 s.
    #[tokio::test]
    async fn registry_mutations_bump_the_epoch() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_cccccccccccccccccccccccccccccccccccccccccccccccc";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_c",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store.clone()));
        let epoch = || {
            h.app
                .registry_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        };
        // D-073: rotate is session-admin only — seat one on ws `a`.
        let (cookie, csrf, pid) = session_admin_on(&h.router, &store, &a, "adm@x.co").await;
        let e0 = epoch();
        for (method, uri, body) in [
            (
                "PUT",
                "/handlers/h",
                Some(json!({ "url": "http://127.0.0.1:9/x" })),
            ),
            ("DELETE", "/handlers/h", None),
        ] {
            let (status, v) = call(&h.router, method, uri, Some(key_a), body).await;
            assert_eq!(status, StatusCode::OK, "{method} {uri}: {v}");
        }
        let (status, _, v) = raw(
            &h.router,
            "POST",
            "/handler-secret",
            &ws_headers(&cookie, &csrf, &pid),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_eq!(epoch() - e0, 3, "PUT + DELETE + rotate each bump once");
    }

    /// A strict app over a Memory store holding one tenant whose store rows
    /// were planted directly (no route check): `evil` names a local
    /// target, `good` a public one. Returns the app, the router, the
    /// tenant's key and its workspace.
    async fn planted_rows_app() -> (
        App,
        Router,
        &'static str,
        durable_accounts::Workspace,
        tempfile::TempDir,
    ) {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("planted")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key = "dr_9ab19ab19ab19ab19ab19ab19ab19ab19ab19ab19ab19ab1";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key.as_bytes()),
                "dr_9",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        for (name, url) in [
            ("evil", "http://127.0.0.1:9/h"),
            ("good", "http://8.8.8.8/h"),
        ] {
            store
                .put_handler(a.id, name, url)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let engine = spawn(
            journal.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let keyring = Arc::new(Keyring::new(ApiKey::new(KEY), None, Some(store)));
        // The shipped store policy: strict.
        let app = test_app(engine, keyring, false, None, journal.path(), false, false);
        (app.clone(), router(app, None), key, a, journal)
    }

    /// Submit `handler` as the tenant (a far `not_before`: admission only).
    async fn admits(r: &Router, key: &str, handler: &str) -> StatusCode {
        let mut b = submit_body(&format!("admit-{handler}"));
        b["spec"]["steps"][0]["handler"] = json!(handler);
        b["not_before"] = json!(9_999_999_999_999u64);
        call(r, "POST", "/jobs", Some(key), Some(b)).await.0
    }

    /// Re-review #10: the reconcile pass re-checks every stored URL — a
    /// row planted past the route (a direct store edit, another build)
    /// never arms; a public row does.
    #[tokio::test]
    async fn reconcile_never_arms_a_planted_local_row() {
        let (app, r, key, _ws, _j) = planted_rows_app().await;
        registry_sync(
            &app.keyring.store.clone().unwrap_or_else(|| panic!("store")),
            &app.engine,
            &app.handler_factory,
            &app.flag_handlers,
            &app.registry_tx,
            &app.registry_epoch,
            &app.registry_locks,
            &app.registry,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(admits(&r, key, "good").await, StatusCode::OK, "control");
        // DECISIVE(reconcile-recheck)
        assert_eq!(
            admits(&r, key, "evil").await,
            StatusCode::CONFLICT,
            "a planted local row was armed by the reconcile pass"
        );
    }

    /// Re-review #10: the startup snapshot is re-checked the same way —
    /// handlers.json may come from another build or an offline edit.
    #[tokio::test]
    async fn the_startup_snapshot_never_arms_a_planted_local_row() {
        let (app, r, key, ws, _j) = planted_rows_app().await;
        let secret = Some(hex(&ws.handler_secret));
        let entry = |name: &str, url: &str| RegistryEntry {
            ws: ws.id,
            name: name.to_string(),
            url: url.to_string(),
            secret: secret.clone(),
        };
        let armed = arm_snapshot(
            vec![
                entry("evil", "http://169.254.169.254/latest"),
                entry("good", "http://8.8.8.8/h"),
            ],
            &app.handler_factory,
            &app.engine,
        )
        .await;
        assert_eq!(admits(&r, key, "good").await, StatusCode::OK, "control");
        // DECISIVE(startup-recheck)
        assert_eq!(
            admits(&r, key, "evil").await,
            StatusCode::CONFLICT,
            "a planted local snapshot row was armed at startup"
        );
        assert_eq!(armed.len(), 1);
    }

    /// The reconciler's outage path: with the store down the engine keeps
    /// the last-known-good set — submits still admit and diagnostics says
    /// `registry_stale` (the loop sets the flag on the same error).
    #[tokio::test]
    async fn a_store_outage_keeps_the_last_known_good_registry() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_aabaabaabaabaabaabaabaabaabaabaabaabaabaabaabaab";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        store
            .put_handler(a.id, "h", "http://127.0.0.1:9/h")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_store(true, None, Some(store.clone()));

        // A healthy sync pushes store truth → engine + snapshot file.
        registry_sync(
            &h.app
                .keyring
                .store
                .clone()
                .unwrap_or_else(|| panic!("store")),
            &h.app.engine,
            &h.app.handler_factory,
            &h.app.flag_handlers,
            &h.app.registry_tx,
            &h.app.registry_epoch,
            &h.app.registry_locks,
            &h.app.registry,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            load_registry_file(&h.app.registry_file)
                .unwrap_or_else(|e| panic!("{e}"))
                .len(),
            1
        );

        // Cache key_a's verification — a *cached* key keeps working
        // through the outage; an unknown bearer would fail closed.
        let (status, v) = call(&h.router, "GET", "/handlers", Some(key_a), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");

        // The store dies: the next sync fails — the loop's stale flag —
        // but the in-memory registry still admits under workspace a.
        store.set_unavailable(true);
        assert!(
            registry_sync(
                &h.app
                    .keyring
                    .store
                    .clone()
                    .unwrap_or_else(|| panic!("store")),
                &h.app.engine,
                &h.app.handler_factory,
                &h.app.flag_handlers,
                &h.app.registry_tx,
                &h.app.registry_epoch,
                &h.app.registry_locks,
                &h.app.registry,
            )
            .await
            .is_err(),
            "a down store must fail the reconcile, loudly"
        );
        h.app
            .registry_stale
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let spec = json!({"op_key": "o", "spec": {"version": 1,
            "steps": [{"name": "s", "handler": "h"}], "input_b64": "aGk="}});
        let (status, v) = call(&h.router, "POST", "/jobs", Some(key_a), Some(spec)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{v} — LKG registry must still admit"
        );
        let (status, v) = call(&h.router, "GET", "/admin/diagnostics", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["registry_stale"], json!(true), "{v}");
    }

    /// A one-shot tripwire + release latch that cannot miss a signal:
    /// the wire is an atomic flag re-checked *after* the notify is
    /// armed, so a release before the trip still ends the park.
    struct Gate {
        tripped: std::sync::atomic::AtomicBool,
        trip_n: tokio::sync::Notify,
        released: std::sync::atomic::AtomicBool,
        release_n: tokio::sync::Notify,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                tripped: std::sync::atomic::AtomicBool::new(false),
                trip_n: tokio::sync::Notify::new(),
                released: std::sync::atomic::AtomicBool::new(false),
                release_n: tokio::sync::Notify::new(),
            }
        }

        /// Fire the wire, then park until `release` is called.
        async fn trip(&self) {
            self.tripped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.trip_n.notify_waiters();
            loop {
                let n = self.release_n.notified();
                tokio::pin!(n);
                n.as_mut().enable();
                if self.released.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                n.await;
            }
        }

        /// Wait for the wire to fire.
        async fn wait(&self) {
            loop {
                let n = self.trip_n.notified();
                tokio::pin!(n);
                n.as_mut().enable();
                if self.tripped.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                n.await;
            }
        }

        fn release(&self) {
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.release_n.notify_waiters();
        }
    }

    /// A `Memory` store whose `list_handlers`/`delete_handler` for `ws`
    /// park mid-call — `listed` fires with the stale result already
    /// computed, `deleting` fires inside the delete before the row is
    /// removed: the "listed before the commit" window finding 4 names.
    struct GateStore {
        inner: Arc<durable_accounts::Memory>,
        ws: u64,
        listed: Gate,
        deleting: Gate,
    }

    use durable_accounts::{
        ApiKeyInfo, HandlerRegistration, InviteGrant, KeyGrant, Mail, MailOutcome, OutboxMail,
        SessionGrant, SignupOutcome, Workspace,
    };

    #[async_trait::async_trait]
    impl AccountStore for GateStore {
        async fn verify_key(&self, h: &[u8; 32]) -> Result<Option<KeyGrant>, AccountError> {
            self.inner.verify_key(h).await
        }
        async fn workspace_by_id(&self, id: u64) -> Result<Option<Workspace>, AccountError> {
            self.inner.workspace_by_id(id).await
        }
        async fn workspace_by_public_id(
            &self,
            public_id: uuid::Uuid,
        ) -> Result<Option<Workspace>, AccountError> {
            self.inner.workspace_by_public_id(public_id).await
        }
        async fn create_workspace(&self, name: &str) -> Result<Workspace, AccountError> {
            self.inner.create_workspace(name).await
        }
        async fn list_workspaces(
            &self,
            limit: u32,
            after: Option<u64>,
        ) -> Result<Vec<Workspace>, AccountError> {
            self.inner.list_workspaces(limit, after).await
        }
        async fn create_key(
            &self,
            workspace: u64,
            name: &str,
            permission: Permission,
            key_hash: &[u8; 32],
            prefix: &str,
            created_by: Option<u64>,
        ) -> Result<ApiKeyInfo, AccountError> {
            self.inner
                .create_key(workspace, name, permission, key_hash, prefix, created_by)
                .await
        }
        async fn key_by_public_id(
            &self,
            workspace: u64,
            public_id: uuid::Uuid,
        ) -> Result<Option<ApiKeyInfo>, AccountError> {
            self.inner.key_by_public_id(workspace, public_id).await
        }
        async fn rotate_key(
            &self,
            workspace: u64,
            key_id: u64,
            new_key_hash: &[u8; 32],
            new_prefix: &str,
            created_by: Option<u64>,
        ) -> Result<Option<(ApiKeyInfo, [u8; 32])>, AccountError> {
            self.inner
                .rotate_key(workspace, key_id, new_key_hash, new_prefix, created_by)
                .await
        }
        async fn touch_keys(
            &self,
            touches: &[durable_accounts::KeyTouch],
        ) -> Result<(), AccountError> {
            self.inner.touch_keys(touches).await
        }
        async fn revoke_key(
            &self,
            workspace: u64,
            key_id: u64,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.inner.revoke_key(workspace, key_id).await
        }
        async fn list_keys(&self, workspace: u64) -> Result<Vec<ApiKeyInfo>, AccountError> {
            self.inner.list_keys(workspace).await
        }
        async fn list_handlers(
            &self,
            workspace: u64,
        ) -> Result<Vec<HandlerRegistration>, AccountError> {
            let r = self.inner.list_handlers(workspace).await;
            if workspace == self.ws && r.is_ok() {
                self.listed.trip().await;
            }
            r
        }
        async fn put_handler(
            &self,
            workspace: u64,
            name: &str,
            url: &str,
        ) -> Result<(), AccountError> {
            self.inner.put_handler(workspace, name, url).await
        }
        async fn record_handler_test(
            &self,
            workspace: u64,
            name: &str,
            at_ms: i64,
            status: &str,
            latency_ms: i64,
        ) -> Result<bool, AccountError> {
            self.inner
                .record_handler_test(workspace, name, at_ms, status, latency_ms)
                .await
        }
        async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError> {
            if workspace == self.ws {
                self.deleting.trip().await;
            }
            self.inner.delete_handler(workspace, name).await
        }
        async fn rotate_handler_secret(
            &self,
            workspace: u64,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.inner.rotate_handler_secret(workspace).await
        }
        async fn signup(
            &self,
            email: &str,
            password_hash: &str,
            token_hash: &[u8; 32],
            token_expires_at_ms: i64,
            verify_mail: &Mail,
            notice_mail: &Mail,
        ) -> Result<SignupOutcome, AccountError> {
            self.inner
                .signup(
                    email,
                    password_hash,
                    token_hash,
                    token_expires_at_ms,
                    verify_mail,
                    notice_mail,
                )
                .await
        }
        async fn account_by_email(
            &self,
            email: &str,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.account_by_email(email).await
        }
        async fn session_grant(
            &self,
            token_hash: &[u8; 32],
        ) -> Result<Option<SessionGrant>, AccountError> {
            self.inner.session_grant(token_hash).await
        }
        async fn create_session(
            &self,
            session: &durable_accounts::NewSession,
        ) -> Result<(), AccountError> {
            self.inner.create_session(session).await
        }
        async fn revoke_session(&self, token_hash: &[u8; 32]) -> Result<bool, AccountError> {
            self.inner.revoke_session(token_hash).await
        }
        async fn revoke_sessions_for_account(&self, account: u64) -> Result<(), AccountError> {
            self.inner.revoke_sessions_for_account(account).await
        }
        async fn member_role(
            &self,
            account: u64,
            workspace: u64,
        ) -> Result<Option<Role>, AccountError> {
            self.inner.member_role(account, workspace).await
        }
        async fn create_workspace_for(
            &self,
            account: u64,
            name: &str,
        ) -> Result<Workspace, AccountError> {
            self.inner.create_workspace_for(account, name).await
        }
        async fn rename_workspace(&self, id: u64, name: &str) -> Result<bool, AccountError> {
            self.inner.rename_workspace(id, name).await
        }
        async fn delete_workspace(&self, id: u64, now_ms: i64) -> Result<bool, AccountError> {
            self.inner.delete_workspace(id, now_ms).await
        }
        async fn workspaces_for_account(
            &self,
            account: u64,
        ) -> Result<Vec<(Workspace, Role)>, AccountError> {
            self.inner.workspaces_for_account(account).await
        }
        async fn list_members(
            &self,
            workspace: u64,
        ) -> Result<Vec<durable_accounts::MemberInfo>, AccountError> {
            self.inner.list_members(workspace).await
        }
        async fn set_member_role(
            &self,
            workspace: u64,
            account: u64,
            role: Role,
        ) -> Result<durable_accounts::MemberChange, AccountError> {
            self.inner.set_member_role(workspace, account, role).await
        }
        async fn remove_member(
            &self,
            workspace: u64,
            account: u64,
        ) -> Result<durable_accounts::MemberChange, AccountError> {
            self.inner.remove_member(workspace, account).await
        }
        async fn account_by_public_id(
            &self,
            public_id: uuid::Uuid,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.account_by_public_id(public_id).await
        }
        async fn create_invite(
            &self,
            workspace: u64,
            email: &str,
            role: Role,
            invited_by: u64,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
        ) -> Result<Option<durable_accounts::WorkspaceInvite>, AccountError> {
            self.inner
                .create_invite(
                    workspace,
                    email,
                    role,
                    invited_by,
                    token_hash,
                    expires_at_ms,
                )
                .await
        }
        async fn list_invites(
            &self,
            workspace: u64,
        ) -> Result<Vec<durable_accounts::WorkspaceInvite>, AccountError> {
            self.inner.list_invites(workspace).await
        }
        async fn revoke_invite(
            &self,
            workspace: u64,
            public_id: uuid::Uuid,
        ) -> Result<bool, AccountError> {
            self.inner.revoke_invite(workspace, public_id).await
        }
        async fn accept_invite(
            &self,
            token_hash: &[u8; 32],
            account: u64,
            now_ms: i64,
        ) -> Result<Option<(u64, Role)>, AccountError> {
            self.inner.accept_invite(token_hash, account, now_ms).await
        }
        async fn decline_invite(
            &self,
            token_hash: &[u8; 32],
            now_ms: i64,
        ) -> Result<bool, AccountError> {
            self.inner.decline_invite(token_hash, now_ms).await
        }
        async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
            self.inner.outbox_mark(id, outcome).await
        }
        async fn sessions_for_account(
            &self,
            account: u64,
            limit: u32,
        ) -> Result<Vec<durable_accounts::SessionInfo>, AccountError> {
            self.inner.sessions_for_account(account, limit).await
        }
        async fn revoke_session_public(
            &self,
            account: u64,
            public_id: uuid::Uuid,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.inner.revoke_session_public(account, public_id).await
        }
        async fn revoke_sessions_except(
            &self,
            account: u64,
            keep_public_id: uuid::Uuid,
        ) -> Result<u64, AccountError> {
            self.inner
                .revoke_sessions_except(account, keep_public_id)
                .await
        }
        async fn change_password(
            &self,
            account: u64,
            new_password_hash: &str,
        ) -> Result<(), AccountError> {
            self.inner.change_password(account, new_password_hash).await
        }
        async fn touch_sessions(
            &self,
            touches: &[durable_accounts::SessionTouch],
        ) -> Result<(), AccountError> {
            self.inner.touch_sessions(touches).await
        }
        async fn consume_verify_token(
            &self,
            token_hash: &[u8; 32],
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.consume_verify_token(token_hash).await
        }
        async fn consume_reset_token(
            &self,
            token_hash: &[u8; 32],
            new_password_hash: &str,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner
                .consume_reset_token(token_hash, new_password_hash)
                .await
        }
        async fn issue_reset_token(
            &self,
            email: &str,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
            reset_mail: &Mail,
        ) -> Result<Option<u64>, AccountError> {
            self.inner
                .issue_reset_token(email, token_hash, expires_at_ms, reset_mail)
                .await
        }
        async fn reissue_verify(
            &self,
            email: &str,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
            verify_mail: &Mail,
        ) -> Result<Option<u64>, AccountError> {
            self.inner
                .reissue_verify(email, token_hash, expires_at_ms, verify_mail)
                .await
        }
        async fn invite_grant(
            &self,
            token_hash: &[u8; 32],
            now_ms: i64,
        ) -> Result<Option<InviteGrant>, AccountError> {
            self.inner.invite_grant(token_hash, now_ms).await
        }
        async fn enqueue_mail(&self, mail: &Mail) -> Result<Option<u64>, AccountError> {
            self.inner.enqueue_mail(mail).await
        }
        async fn outbox_claim(
            &self,
            now_ms: i64,
            limit: u32,
            claim_ms: i64,
        ) -> Result<Vec<OutboxMail>, AccountError> {
            self.inner.outbox_claim(now_ms, limit, claim_ms).await
        }
        async fn outbox_sweep(&self, now_ms: i64) -> Result<u64, AccountError> {
            self.inner.outbox_sweep(now_ms).await
        }
        async fn outbox_purge(&self, now_ms: i64) -> Result<u64, AccountError> {
            self.inner.outbox_purge(now_ms).await
        }
    }

    /// A `Memory` store that counts the registry's store work.
    struct CountStore {
        inner: Arc<durable_accounts::Memory>,
        registry_calls: std::sync::atomic::AtomicU64,
    }

    impl CountStore {
        fn bump(&self) {
            self.registry_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn calls(&self) -> u64 {
            self.registry_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl AccountStore for CountStore {
        async fn verify_key(&self, h: &[u8; 32]) -> Result<Option<KeyGrant>, AccountError> {
            self.inner.verify_key(h).await
        }
        async fn workspace_by_id(&self, id: u64) -> Result<Option<Workspace>, AccountError> {
            self.bump();
            self.inner.workspace_by_id(id).await
        }
        async fn workspace_by_public_id(
            &self,
            public_id: uuid::Uuid,
        ) -> Result<Option<Workspace>, AccountError> {
            self.inner.workspace_by_public_id(public_id).await
        }
        async fn create_workspace(&self, name: &str) -> Result<Workspace, AccountError> {
            self.inner.create_workspace(name).await
        }
        async fn list_workspaces(
            &self,
            limit: u32,
            after: Option<u64>,
        ) -> Result<Vec<Workspace>, AccountError> {
            self.inner.list_workspaces(limit, after).await
        }
        async fn create_key(
            &self,
            workspace: u64,
            name: &str,
            permission: Permission,
            key_hash: &[u8; 32],
            prefix: &str,
            created_by: Option<u64>,
        ) -> Result<ApiKeyInfo, AccountError> {
            self.inner
                .create_key(workspace, name, permission, key_hash, prefix, created_by)
                .await
        }
        async fn key_by_public_id(
            &self,
            workspace: u64,
            public_id: uuid::Uuid,
        ) -> Result<Option<ApiKeyInfo>, AccountError> {
            self.inner.key_by_public_id(workspace, public_id).await
        }
        async fn rotate_key(
            &self,
            workspace: u64,
            key_id: u64,
            new_key_hash: &[u8; 32],
            new_prefix: &str,
            created_by: Option<u64>,
        ) -> Result<Option<(ApiKeyInfo, [u8; 32])>, AccountError> {
            self.inner
                .rotate_key(workspace, key_id, new_key_hash, new_prefix, created_by)
                .await
        }
        async fn touch_keys(
            &self,
            touches: &[durable_accounts::KeyTouch],
        ) -> Result<(), AccountError> {
            self.inner.touch_keys(touches).await
        }
        async fn revoke_key(
            &self,
            workspace: u64,
            key_id: u64,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.inner.revoke_key(workspace, key_id).await
        }
        async fn list_keys(&self, workspace: u64) -> Result<Vec<ApiKeyInfo>, AccountError> {
            self.inner.list_keys(workspace).await
        }
        async fn list_handlers(
            &self,
            workspace: u64,
        ) -> Result<Vec<HandlerRegistration>, AccountError> {
            self.bump();
            self.inner.list_handlers(workspace).await
        }
        async fn put_handler(
            &self,
            workspace: u64,
            name: &str,
            url: &str,
        ) -> Result<(), AccountError> {
            self.bump();
            self.inner.put_handler(workspace, name, url).await
        }
        async fn record_handler_test(
            &self,
            workspace: u64,
            name: &str,
            at_ms: i64,
            status: &str,
            latency_ms: i64,
        ) -> Result<bool, AccountError> {
            self.inner
                .record_handler_test(workspace, name, at_ms, status, latency_ms)
                .await
        }
        async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError> {
            self.bump();
            self.inner.delete_handler(workspace, name).await
        }
        async fn rotate_handler_secret(
            &self,
            workspace: u64,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.bump();
            self.inner.rotate_handler_secret(workspace).await
        }
        async fn signup(
            &self,
            email: &str,
            password_hash: &str,
            token_hash: &[u8; 32],
            token_expires_at_ms: i64,
            verify_mail: &Mail,
            notice_mail: &Mail,
        ) -> Result<SignupOutcome, AccountError> {
            self.inner
                .signup(
                    email,
                    password_hash,
                    token_hash,
                    token_expires_at_ms,
                    verify_mail,
                    notice_mail,
                )
                .await
        }
        async fn account_by_email(
            &self,
            email: &str,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.account_by_email(email).await
        }
        async fn session_grant(
            &self,
            token_hash: &[u8; 32],
        ) -> Result<Option<SessionGrant>, AccountError> {
            self.inner.session_grant(token_hash).await
        }
        async fn create_session(
            &self,
            session: &durable_accounts::NewSession,
        ) -> Result<(), AccountError> {
            self.inner.create_session(session).await
        }
        async fn revoke_session(&self, token_hash: &[u8; 32]) -> Result<bool, AccountError> {
            self.inner.revoke_session(token_hash).await
        }
        async fn revoke_sessions_for_account(&self, account: u64) -> Result<(), AccountError> {
            self.inner.revoke_sessions_for_account(account).await
        }
        async fn member_role(
            &self,
            account: u64,
            workspace: u64,
        ) -> Result<Option<Role>, AccountError> {
            self.inner.member_role(account, workspace).await
        }
        async fn create_workspace_for(
            &self,
            account: u64,
            name: &str,
        ) -> Result<Workspace, AccountError> {
            self.inner.create_workspace_for(account, name).await
        }
        async fn rename_workspace(&self, id: u64, name: &str) -> Result<bool, AccountError> {
            self.inner.rename_workspace(id, name).await
        }
        async fn delete_workspace(&self, id: u64, now_ms: i64) -> Result<bool, AccountError> {
            self.inner.delete_workspace(id, now_ms).await
        }
        async fn workspaces_for_account(
            &self,
            account: u64,
        ) -> Result<Vec<(Workspace, Role)>, AccountError> {
            self.inner.workspaces_for_account(account).await
        }
        async fn list_members(
            &self,
            workspace: u64,
        ) -> Result<Vec<durable_accounts::MemberInfo>, AccountError> {
            self.inner.list_members(workspace).await
        }
        async fn set_member_role(
            &self,
            workspace: u64,
            account: u64,
            role: Role,
        ) -> Result<durable_accounts::MemberChange, AccountError> {
            self.inner.set_member_role(workspace, account, role).await
        }
        async fn remove_member(
            &self,
            workspace: u64,
            account: u64,
        ) -> Result<durable_accounts::MemberChange, AccountError> {
            self.inner.remove_member(workspace, account).await
        }
        async fn account_by_public_id(
            &self,
            public_id: uuid::Uuid,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.account_by_public_id(public_id).await
        }
        async fn create_invite(
            &self,
            workspace: u64,
            email: &str,
            role: Role,
            invited_by: u64,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
        ) -> Result<Option<durable_accounts::WorkspaceInvite>, AccountError> {
            self.inner
                .create_invite(
                    workspace,
                    email,
                    role,
                    invited_by,
                    token_hash,
                    expires_at_ms,
                )
                .await
        }
        async fn list_invites(
            &self,
            workspace: u64,
        ) -> Result<Vec<durable_accounts::WorkspaceInvite>, AccountError> {
            self.inner.list_invites(workspace).await
        }
        async fn revoke_invite(
            &self,
            workspace: u64,
            public_id: uuid::Uuid,
        ) -> Result<bool, AccountError> {
            self.inner.revoke_invite(workspace, public_id).await
        }
        async fn accept_invite(
            &self,
            token_hash: &[u8; 32],
            account: u64,
            now_ms: i64,
        ) -> Result<Option<(u64, Role)>, AccountError> {
            self.inner.accept_invite(token_hash, account, now_ms).await
        }
        async fn decline_invite(
            &self,
            token_hash: &[u8; 32],
            now_ms: i64,
        ) -> Result<bool, AccountError> {
            self.inner.decline_invite(token_hash, now_ms).await
        }
        async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
            self.inner.outbox_mark(id, outcome).await
        }
        async fn sessions_for_account(
            &self,
            account: u64,
            limit: u32,
        ) -> Result<Vec<durable_accounts::SessionInfo>, AccountError> {
            self.inner.sessions_for_account(account, limit).await
        }
        async fn revoke_session_public(
            &self,
            account: u64,
            public_id: uuid::Uuid,
        ) -> Result<Option<[u8; 32]>, AccountError> {
            self.inner.revoke_session_public(account, public_id).await
        }
        async fn revoke_sessions_except(
            &self,
            account: u64,
            keep_public_id: uuid::Uuid,
        ) -> Result<u64, AccountError> {
            self.inner
                .revoke_sessions_except(account, keep_public_id)
                .await
        }
        async fn change_password(
            &self,
            account: u64,
            new_password_hash: &str,
        ) -> Result<(), AccountError> {
            self.inner.change_password(account, new_password_hash).await
        }
        async fn touch_sessions(
            &self,
            touches: &[durable_accounts::SessionTouch],
        ) -> Result<(), AccountError> {
            self.inner.touch_sessions(touches).await
        }
        async fn consume_verify_token(
            &self,
            token_hash: &[u8; 32],
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner.consume_verify_token(token_hash).await
        }
        async fn consume_reset_token(
            &self,
            token_hash: &[u8; 32],
            new_password_hash: &str,
        ) -> Result<Option<durable_accounts::Account>, AccountError> {
            self.inner
                .consume_reset_token(token_hash, new_password_hash)
                .await
        }
        async fn issue_reset_token(
            &self,
            email: &str,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
            reset_mail: &Mail,
        ) -> Result<Option<u64>, AccountError> {
            self.inner
                .issue_reset_token(email, token_hash, expires_at_ms, reset_mail)
                .await
        }
        async fn reissue_verify(
            &self,
            email: &str,
            token_hash: &[u8; 32],
            expires_at_ms: i64,
            verify_mail: &Mail,
        ) -> Result<Option<u64>, AccountError> {
            self.inner
                .reissue_verify(email, token_hash, expires_at_ms, verify_mail)
                .await
        }
        async fn invite_grant(
            &self,
            token_hash: &[u8; 32],
            now_ms: i64,
        ) -> Result<Option<InviteGrant>, AccountError> {
            self.inner.invite_grant(token_hash, now_ms).await
        }
        async fn enqueue_mail(&self, mail: &Mail) -> Result<Option<u64>, AccountError> {
            self.inner.enqueue_mail(mail).await
        }
        async fn outbox_claim(
            &self,
            now_ms: i64,
            limit: u32,
            claim_ms: i64,
        ) -> Result<Vec<OutboxMail>, AccountError> {
            self.inner.outbox_claim(now_ms, limit, claim_ms).await
        }
        async fn outbox_sweep(&self, now_ms: i64) -> Result<u64, AccountError> {
            self.inner.outbox_sweep(now_ms).await
        }
        async fn outbox_purge(&self, now_ms: i64) -> Result<u64, AccountError> {
            self.inner.outbox_purge(now_ms).await
        }
    }

    /// A Memory-backed harness with two tenants, A and B, each with an
    /// admin key, behind a store that counts registry work.
    async fn two_tenant_registry() -> (Harness, Arc<CountStore>, &'static str, &'static str) {
        let inner = Arc::new(durable_accounts::Memory::new());
        let key_a = "dr_3c13c13c13c13c13c13c13c13c13c13c13c13c13c13c13c1";
        let key_b = "dr_3c23c23c23c23c23c23c23c23c23c23c23c23c23c23c23c2";
        for (name, key) in [("ra", key_a), ("rb", key_b)] {
            let w = inner
                .create_workspace(name)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            inner
                .create_key(
                    w.id,
                    "k",
                    Permission::Write,
                    &sha256(key.as_bytes()),
                    "dr_3",
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let store = Arc::new(CountStore {
            inner,
            registry_calls: std::sync::atomic::AtomicU64::new(0),
        });
        let h = harness_store(true, None, Some(store.clone()));
        (h, store, key_a, key_b)
    }

    /// Re-review S2-4: a PUT of exactly what is armed is a no-op — no
    /// store query, no epoch bump, no file save; a changed URL is not.
    #[tokio::test]
    async fn a_noop_put_does_no_store_epoch_or_disk_work() {
        let (h, store, key_a, _) = two_tenant_registry().await;
        let put = |url: &'static str| {
            let r = h.router.clone();
            async move {
                call(
                    &r,
                    "PUT",
                    "/handlers/h",
                    Some(key_a),
                    Some(json!({ "url": url })),
                )
                .await
            }
        };
        let (status, v) = put("http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let saves = || {
            h.app
                .registry
                .saves
                .load(std::sync::atomic::Ordering::SeqCst)
        };
        let epoch = || {
            h.app
                .registry_epoch
                .load(std::sync::atomic::Ordering::SeqCst)
        };
        let (calls0, epoch0, saves0) = (store.calls(), epoch(), saves());
        assert_eq!(saves0, 1, "the first PUT saved once");
        for _ in 0..5 {
            let (status, v) = put("http://127.0.0.1:9/a").await;
            // DECISIVE(registry-noop-skip)
            assert_eq!(
                (status, v["unchanged"].clone()),
                (StatusCode::OK, json!(true))
            );
        }
        assert_eq!(
            (store.calls(), epoch(), saves()),
            (calls0, epoch0, saves0),
            "a no-op PUT did store, epoch or disk work"
        );
        let (status, _) = put("http://127.0.0.1:9/b").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(saves(), saves0 + 1, "a changed URL is saved");
    }

    /// Makes the registry writer's saves fail: a directory where its temp
    /// file goes (holds as root too, unlike a read-only directory).
    struct SaveBlock(PathBuf);
    impl SaveBlock {
        fn on(h: &Harness) -> Self {
            let p = h.app.registry_file.with_extension("json.tmp");
            std::fs::create_dir(&p).unwrap_or_else(|e| panic!("{e}"));
            Self(p)
        }
    }
    impl Drop for SaveBlock {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    async fn put_h(h: &Harness, key: &str, url: &str) -> (StatusCode, serde_json::Value) {
        call(
            &h.router,
            "PUT",
            "/handlers/h",
            Some(key),
            Some(json!({ "url": url })),
        )
        .await
    }

    fn file_url(h: &Harness) -> Option<String> {
        load_registry_file(&h.app.registry_file)
            .unwrap_or_else(|e| panic!("{e}"))
            .into_iter()
            .find(|e| e.name == "h")
            .map(|e| e.url)
    }

    /// S3 re-review 2: a PUT whose file save failed answers 503 and arms
    /// nothing in the view — its retry writes instead of `unchanged`.
    #[tokio::test]
    async fn a_put_whose_save_failed_is_retried_not_skipped() {
        let (h, _store, key_a, _) = two_tenant_registry().await;
        {
            let _block = SaveBlock::on(&h);
            let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
            assert_eq!(
                (status, v["code"].clone()),
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!("registry_write_failed")
                ),
                "{v}"
            );
        }
        assert_eq!(file_url(&h), None, "control: the failed save wrote nothing");
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(registry-set-after-save)
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the retry was skipped: the failed save left the view armed"
        );
        assert_eq!(file_url(&h).as_deref(), Some("http://127.0.0.1:9/a"));
    }

    /// S3 re-review 2: after a good PUT of A, a PUT of B that armed B but
    /// failed its save must not leave A in the view — a PUT back to A
    /// would answer `unchanged` while B is what dispatch calls.
    #[tokio::test]
    async fn a_failed_put_never_leaves_the_old_url_claiming_armed() {
        let (h, _store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        {
            let _block = SaveBlock::on(&h);
            let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/b").await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        }
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(registry-view-invalidate)
        assert_ne!(
            v["unchanged"],
            json!(true),
            "a PUT back to A was skipped while the failed PUT's B stayed armed"
        );
        assert_eq!(file_url(&h).as_deref(), Some("http://127.0.0.1:9/a"));
    }

    /// S3 re-review 2: a DELETE forgets the view before its save, so a
    /// failed save never makes the next PUT of the same URL a no-op while
    /// nothing is armed (the order the review proposed would).
    #[tokio::test]
    async fn a_delete_whose_save_failed_lets_the_same_put_rearm() {
        let (h, _store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        {
            let _block = SaveBlock::on(&h);
            let (status, v) = call(&h.router, "DELETE", "/handlers/h", Some(key_a), None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        }
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(registry-delete-forgets)
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the PUT after a deleted handler was skipped: nothing is armed"
        );
    }

    /// Re-review S2-3: a DELETE the store answers not-found (the row went
    /// behind the API) still forgets the view — the next PUT of the same
    /// URL writes instead of confirming a registration the store lacks.
    #[tokio::test]
    async fn a_delete_the_store_did_not_find_still_forgets_the_view() {
        let (h, store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let ws = load_registry_file(&h.app.registry_file)
            .unwrap_or_else(|e| panic!("{e}"))
            .into_iter()
            .find(|e| e.name == "h")
            .map(|e| e.ws)
            .unwrap_or_else(|| panic!("the PUT saved its row"));
        assert!(store
            .delete_handler(ws, "h")
            .await
            .unwrap_or_else(|e| panic!("{e}")));
        let (status, v) = call(&h.router, "DELETE", "/handlers/h", Some(key_a), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{v}");
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the PUT was skipped on the view a 404 DELETE left behind"
        );
        let rows = store
            .list_handlers(ws)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            rows.iter().any(|r| r.name == "h"),
            "the API confirmed a registration the store does not hold"
        );
    }

    /// Re-review S2-3: a DELETE whose engine call failed after the store
    /// row went still forgets the view — a later PUT of the same URL is
    /// never a no-op over a row the store no longer has.
    #[tokio::test]
    async fn a_delete_the_engine_refused_still_forgets_the_view() {
        let (h, _store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        h.app
            .engine
            .shutdown(Duration::from_secs(5))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (status, v) = call(&h.router, "DELETE", "/handlers/h", Some(key_a), None).await;
        assert_ne!(status, StatusCode::OK, "control: the engine refused: {v}");
        let (_, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the PUT was skipped on the view a failed DELETE left behind"
        );
    }

    /// S3 re-review 2: a rotate whose save failed keeps nothing in the
    /// view, so the next PUT writes the new secret instead of skipping
    /// and leaving the dead one in the file.
    #[tokio::test]
    async fn a_rotate_whose_save_failed_lets_a_put_save_the_new_secret() {
        let (h, _store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let secret_in_file = || {
            load_registry_file(&h.app.registry_file)
                .unwrap_or_else(|e| panic!("{e}"))
                .into_iter()
                .find(|e| e.name == "h")
                .and_then(|e| e.secret)
        };
        let old = secret_in_file();
        // D-073: rotate is session-admin only — seat one on ws A.
        let a_ws = {
            let inner = &_store.inner;
            inner
                .list_workspaces(100, None)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .into_iter()
                .find(|w| w.name == "ra")
                .unwrap_or_else(|| panic!("ws A"))
        };
        let (cookie, csrf, pid) =
            session_admin_on(&h.router, &_store.inner, &a_ws, "adm@x.co").await;
        {
            let _block = SaveBlock::on(&h);
            let (status, _, v) = raw(
                &h.router,
                "POST",
                "/handler-secret",
                &ws_headers(&cookie, &csrf, &pid),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        }
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(rotate-set-after-save)
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the PUT after a failed rotate was skipped: the file keeps the dead secret"
        );
        assert_ne!(
            secret_in_file(),
            old,
            "the file still carries the rotated-out secret"
        );
    }

    /// S3 re-review 2: a reconcile pass whose snapshot did not land
    /// forgets the views it set, so the PUT of what it armed still writes.
    #[tokio::test]
    async fn a_reconcile_pass_that_did_not_land_forgets_its_views() {
        let (h, store, key_a, _) = two_tenant_registry().await;
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/a").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let ws = load_registry_file(&h.app.registry_file)
            .unwrap_or_else(|e| panic!("{e}"))
            .into_iter()
            .find(|e| e.name == "h")
            .map(|e| e.ws)
            .unwrap_or_else(|| panic!("the PUT saved its row"));
        // The store moves to B behind the API (another instance's write).
        store
            .put_handler(ws, "h", "http://127.0.0.1:9/b")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let r = {
            let _block = SaveBlock::on(&h);
            registry_sync(
                &h.app
                    .keyring
                    .store
                    .clone()
                    .unwrap_or_else(|| panic!("store")),
                &h.app.engine,
                &h.app.handler_factory,
                &h.app.flag_handlers,
                &h.app.registry_tx,
                &h.app.registry_epoch,
                &h.app.registry_locks,
                &h.app.registry,
            )
            .await
        };
        assert!(r.is_err(), "control: the pass's snapshot could not land");
        let (status, v) = put_h(&h, key_a, "http://127.0.0.1:9/b").await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(reconcile-forgets)
        assert_ne!(
            v["unchanged"],
            json!(true),
            "the PUT was skipped on a view the failed pass left behind"
        );
        assert_eq!(file_url(&h).as_deref(), Some("http://127.0.0.1:9/b"));
    }

    /// Re-review S2-4: each workspace's registry mutations are rate
    /// limited (2/s, burst 20) — past the burst a mutation is 429 with
    /// Retry-After and no store work; another workspace is unaffected.
    #[tokio::test]
    async fn registry_mutations_are_rate_limited_per_workspace() {
        let (h, store, key_a, key_b) = two_tenant_registry().await;
        let mut refused = None;
        for i in 0..100 {
            let (status, v) = call(
                &h.router,
                "PUT",
                &format!("/handlers/h{i}"),
                Some(key_a),
                Some(json!({ "url": format!("http://127.0.0.1:9/{i}") })),
            )
            .await;
            if status != StatusCode::OK {
                refused = Some((i, status, v["code"].clone()));
                break;
            }
        }
        // The exact count is the unit test's (an injected clock); here the
        // clock is real, so tokens may refill while the burst runs.
        // DECISIVE(registry-rate)
        assert!(
            matches!(&refused, Some((i, s, c))
                if *i >= 20 && *s == StatusCode::TOO_MANY_REQUESTS
                    && *c == json!("registry_rate_limited")),
            "the burst is 20, then 429 registry_rate_limited: {refused:?}"
        );
        let calls = store.calls();
        let (status, headers, _) = {
            let req = Request::builder()
                .method("DELETE")
                .uri("/handlers/h0")
                .header("authorization", format!("Bearer {key_a}"))
                .body(Body::empty())
                .unwrap_or_else(|e| panic!("{e}"));
            let res = h
                .router
                .clone()
                .oneshot(req)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            (res.status(), res.headers().clone(), ())
        };
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(headers.get(header::RETRY_AFTER).is_some(), "Retry-After");
        assert_eq!(store.calls(), calls, "a refused mutation did store work");
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/x",
            Some(key_b),
            Some(json!({ "url": "http://127.0.0.1:9/x" })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "another workspace is unaffected: {v}"
        );
    }

    /// The registry limiter's arithmetic on an injected clock: a burst of
    /// 20, then 2 per second, per workspace; idle full buckets make room
    /// at the table's cap.
    #[test]
    fn the_registry_limiter_is_a_per_workspace_bucket() {
        let r = RegistryState::default();
        let t0 = Instant::now();
        for i in 0..20 {
            assert!(r.admit(7, t0).is_ok(), "burst {i}");
        }
        let wait = r.admit(7, t0).err().unwrap_or_default();
        assert!(
            wait > Duration::ZERO && wait <= Duration::from_millis(500),
            "{wait:?}"
        );
        assert!(
            r.admit(8, t0).is_ok(),
            "another workspace has its own bucket"
        );
        let t1 = t0 + Duration::from_millis(500);
        assert!(r.admit(7, t1).is_ok(), "one token per 500 ms");
        assert!(r.admit(7, t1).is_err());
    }

    /// Re-review S2-4: the writer saves once per batch of queued writes —
    /// 100 upserts queued before it wakes are one tmp-write and two fsyncs.
    #[test]
    fn the_registry_writer_saves_once_per_batch() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let path = d.path().join("handlers.json");
        let (tx, rx) = std::sync::mpsc::sync_channel::<RegistryWrite>(512);
        let mut acks = Vec::new();
        for i in 0..100u64 {
            let (ack, rx_ack) = tokio::sync::oneshot::channel();
            tx.send(RegistryWrite::Upsert {
                entry: RegistryEntry {
                    ws: 1,
                    name: format!("h{i}"),
                    url: format!("http://example.com/{i}"),
                    secret: None,
                },
                ack,
            })
            .unwrap_or_else(|e| panic!("{e}"));
            acks.push(rx_ack);
        }
        drop(tx);
        let saves = Arc::new(std::sync::atomic::AtomicU64::new(0));
        registry_writer(
            path.clone(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            rx,
            Arc::clone(&saves),
        );
        // DECISIVE(registry-batch)
        assert_eq!(
            saves.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one save per batch"
        );
        assert_eq!(load_registry_file(&path).map(|v| v.len()).ok(), Some(100));
        for mut a in acks {
            assert!(matches!(a.try_recv(), Ok(Ok(()))));
        }
    }

    /// Finding 4 (S2): the epoch bumps *before* the store write, so a
    /// pass that read the new epoch could still list the pre-commit row
    /// — its apply then re-armed a deleted handler in the engine and
    /// its snapshot put the row back in `handlers.json`. A DELETE and
    /// a pass's list+apply of the same workspace are each one critical
    /// section now: whichever runs second sees the other's result, so
    /// the row can never come back.
    ///
    /// The gate store makes the interleave deterministic: the delete is
    /// parked inside `store.delete_handler` while a pass is started —
    /// under the fix that pass cannot reach its own `list_handlers`
    /// until the delete releases the workspace's stripe.
    #[tokio::test]
    async fn a_deleted_handler_stays_deleted_through_a_reconcile_window() {
        use durable_accounts::Memory;
        let inner = Arc::new(Memory::new());
        let a = inner
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_cccccccccccccccccccccccccccccccccccccccccccccccc";
        inner
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_c",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        inner
            .put_handler(a.id, "h", "http://127.0.0.1:9/h")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let store = Arc::new(GateStore {
            inner,
            ws: a.id,
            listed: Gate::new(),
            deleting: Gate::new(),
        });
        let dyn_store: Arc<dyn AccountStore> = store.clone();
        let h = harness_store(true, None, Some(dyn_store.clone()));
        // The pre-delete state: `h` armed in the engine and present in
        // the snapshot file — what a restart would see.
        h.app
            .engine
            .put_handler(a.id, "h", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        registry_file_upsert(
            &h.app,
            RegistryEntry {
                ws: a.id,
                name: "h".into(),
                url: "http://127.0.0.1:9/h".into(),
                secret: None,
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e:?}"));

        // The DELETE through its real route: bump → stripe → store →
        // engine → file. `deleting` trips inside the store write.
        let del = {
            let router = h.router.clone();
            tokio::spawn(
                async move { call(&router, "DELETE", "/handlers/h", Some(key_a), None).await },
            )
        };
        store.deleting.wait().await;

        // The property under test: while a mutation of this workspace
        // is in flight, its stripe is held — an unfixed tree lets a
        // reconcile read through right here.
        let held = h.app.registry_locks.stripe(a.id).try_lock().is_err();

        let pass = {
            let engine = h.app.engine.clone();
            let factory = Arc::clone(&h.app.handler_factory);
            let flags = Arc::clone(&h.app.flag_handlers);
            let tx = h.app.registry_tx.clone();
            let epoch = Arc::clone(&h.app.registry_epoch);
            let locks = Arc::clone(&h.app.registry_locks);
            let view = Arc::clone(&h.app.registry);
            tokio::spawn(async move {
                registry_sync(
                    &dyn_store, &engine, &factory, &flags, &tx, &epoch, &locks, &view,
                )
                .await
            })
        };
        if held {
            // Serialized: finish the delete first — the pass waits on
            // the stripe, then lists the post-delete truth.
            store.deleting.release();
            let (status, v) = del.await.unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(status, StatusCode::OK, "{v}");
            store.listed.wait().await;
            store.listed.release();
            pass.await
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|e| panic!("{e}"));
        } else {
            // Unfixed behaviour, driven deterministically: the pass
            // lists the pre-delete row and parks; the delete commits
            // beneath it; the stale apply re-arms `h` and the stale
            // snapshot rewrites the row.
            store.listed.wait().await;
            store.deleting.release();
            let (status, v) = del.await.unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(status, StatusCode::OK, "{v}");
            store.listed.release();
            pass.await
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|e| panic!("{e}"));
        }

        // Store truth and file mirror are identical under both
        // orderings — what must never diverge is the engine: a deleted
        // handler must not re-admit work.
        assert!(
            store
                .inner
                .list_handlers(a.id)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .is_empty(),
            "the store row is gone"
        );
        let rows = load_registry_file(&h.app.registry_file).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            !rows.iter().any(|e| e.ws == a.id && e.name == "h"),
            "handlers.json resurrected the deleted row: {rows:?}"
        );
        let spec = json!({"op_key": "after", "spec": {"version": 1,
            "steps": [{"name": "s", "handler": "h"}], "input_b64": "aGk="}});
        let (status, v) = call(&h.router, "POST", "/jobs", Some(key_a), Some(spec)).await;
        assert_eq!(
            (status, v["code"].as_str()),
            (StatusCode::CONFLICT, Some("unregistered_handler")),
            "the deleted handler admitted work again: {v}"
        );
    }

    /// Re-review #8: a mutation the registry writer could not durably
    /// apply to handlers.json must not answer 200 — the row would
    /// resurrect on restart while the client believed it gone. The
    /// answer is a retryable 503 with a code.
    #[tokio::test]
    async fn a_registry_write_the_file_refused_is_503_not_200() {
        use std::os::unix::fs::PermissionsExt as _;
        let h = harness_store(true, None, Some(Arc::new(durable_accounts::Memory::new())));
        let store = h.keyring.store.clone().unwrap_or_else(|| panic!("store"));
        let ws = store
            .create_workspace("t")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key = "dr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        store
            .create_key(
                ws.id,
                "k",
                Permission::Write,
                &sha256(key.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            store
                .verify_key(&sha256(key.as_bytes()))
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .is_some(),
            "probe: the store does not see the seeded key"
        );
        // The row is in the file (the writer skips a write that changes
        // nothing — re-review S2-4 — so the DELETE must change it).
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/h",
            Some(key),
            Some(json!({ "url": "http://127.0.0.1:9/h" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // The writer cannot persist: the file's directory refuses writes.
        let dir = h
            .app
            .registry_file
            .parent()
            .unwrap_or_else(|| panic!("parent"))
            .to_path_buf();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500))
            .unwrap_or_else(|e| panic!("{e}"));
        let (status, v) = call(&h.router, "DELETE", "/handlers/h", Some(key), None).await;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            (status, v["code"].as_str()),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some("registry_write_failed")
            ),
            "a write the file refused answered {status}: {v}"
        );
    }

    /// The companion failure: an unreadable or corrupt handlers.json is
    /// an error, never an empty registry — the next write would erase
    /// every workspace's last-known-good rows.
    #[test]
    fn an_unreadable_or_corrupt_registry_file_is_an_error() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let p = d.path().join("handlers.json");
        std::fs::write(&p, b"not json{").unwrap_or_else(|e| panic!("{e}"));
        assert!(
            load_registry_file(&p).is_err(),
            "a corrupt file loaded as an empty registry"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000))
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(
                load_registry_file(&p).is_err(),
                "an unreadable file loaded as an empty registry"
            );
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))
                .unwrap_or_else(|e| panic!("{e}"));
        }
        assert!(
            load_registry_file(&d.path().join("absent.json"))
                .unwrap_or_else(|e| panic!("{e}"))
                .is_empty(),
            "a missing file is a fresh registry, not an error"
        );
    }

    /// S2-5, the packet assertion: a handler URL whose *name* resolves
    /// to a local address is refused inside the resolver — the loopback
    /// listener sees zero connections. `localhost` always maps to a
    /// loopback; a host that does not resolve at all fails the same way.
    #[tokio::test]
    async fn a_name_resolving_local_never_sends_a_packet() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let port = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("{e}"))
            .port();
        // The strict client — no --allow-local-handlers.
        let url = format!("http://localhost:{port}/step");
        let h = HttpHandler {
            client: handler_client(&ssrf::Policy::strict()).unwrap_or_else(|e| panic!("{e}")),
            policy_url: reqwest::Url::parse(&url).ok(),
            url,
            timeout: Duration::from_secs(2),
            max_output: 1024,
            secret: None,
            policy: ssrf::Policy::strict(),
            pubid: pubid::PubId::ephemeral(),
        };
        let spec = StepSpec {
            name: "s".into(),
            handler: "h".into(),
        };
        let outcome = h.execute(&spec, b"hi", "k").await;
        assert!(
            matches!(outcome, Outcome::Failed),
            "a refused target is a failed attempt, not unknown: {outcome:?}"
        );
        // Zero packets: if even one escaped the guard it lands here.
        let escaped = tokio::time::timeout(Duration::from_millis(400), listener.accept()).await;
        assert!(
            escaped.is_err(),
            "a packet reached a refused target — the resolver let a local address through"
        );

        // Positive control: the same URL through the opt-out client
        // dials — the flag is what fixtures run under.
        let url = format!("http://localhost:{port}/step");
        let open = HttpHandler {
            client: handler_client(&ssrf::Policy::open()).unwrap_or_else(|e| panic!("{e}")),
            policy_url: reqwest::Url::parse(&url).ok(),
            url,
            timeout: Duration::from_secs(2),
            max_output: 1024,
            secret: None,
            policy: ssrf::Policy::open(),
            pubid: pubid::PubId::ephemeral(),
        };
        let call = tokio::spawn(async move { open.execute(&spec, b"hi", "k").await });
        let dialed = tokio::time::timeout(Duration::from_secs(2), listener.accept()).await;
        assert!(
            dialed.is_ok(),
            "the allow-local client must reach a loopback target"
        );
        let _ = call.await;
    }

    /// S2 review #3: a *literal* local IP never reaches the resolver —
    /// hyper dials it directly — so the call-time policy check is what
    /// refuses it. A planted literal (store row, snapshot, file edit)
    /// fails the attempt and sends zero packets.
    #[tokio::test]
    async fn a_literal_local_url_is_refused_at_call_time() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        // A handler built the way a planted store/snapshot row builds
        // it: strict policy, literal loopback URL.
        let url = format!("http://{addr}/step");
        let h = HttpHandler {
            client: handler_client(&ssrf::Policy::strict()).unwrap_or_else(|e| panic!("{e}")),
            policy_url: reqwest::Url::parse(&url).ok(),
            url,
            timeout: Duration::from_secs(2),
            max_output: 1024,
            secret: None,
            policy: ssrf::Policy::strict(),
            pubid: pubid::PubId::ephemeral(),
        };
        let spec = StepSpec {
            name: "s".into(),
            handler: "h".into(),
        };
        let outcome = h.execute(&spec, b"hi", "k").await;
        // DECISIVE(call-time-literal)
        assert!(
            matches!(outcome, Outcome::Failed),
            "a literal local target must fail the attempt: {outcome:?}"
        );
        assert!(
            !h.ask(None, "k").await,
            "a probe to a literal local target is refused too"
        );
        let escaped = tokio::time::timeout(Duration::from_millis(400), listener.accept()).await;
        assert!(
            escaped.is_err(),
            "a packet reached a literal local target — the call-time check failed"
        );
    }

    /// S3 re-review 4: an API-registered (signed) handler is built with the
    /// store's client and policy, never the flag pair — even when the
    /// operator's `--allow-local-handlers` opens the flag pair. A name
    /// resolving to loopback is the client's to refuse (its resolver); a
    /// loopback literal is the policy's (the call-time check). The flag
    /// pair reaching the same listener is the control.
    #[tokio::test]
    async fn a_signed_handler_uses_the_store_pair_not_the_flag_pair() {
        let factory = HandlerFactory::new(
            ssrf::Policy::strict(),
            ssrf::Policy::open(),
            Duration::from_secs(2),
            1024,
            pubid::PubId::ephemeral(),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let secret = Some(Arc::new([7u8; 32]));
        let bind = || async {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let port = l.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
            (l, port)
        };
        let reached = |l: tokio::net::TcpListener| async move {
            tokio::time::timeout(Duration::from_millis(400), l.accept())
                .await
                .is_ok()
        };
        // The control: the flag pair (open) reaches a loopback name.
        let (l, port) = bind().await;
        let flag = factory.make(&format!("http://localhost:{port}/step"));
        let reach = tokio::spawn(async move { flag.execute(&spec_of("s"), b"hi", "k").await });
        assert!(
            tokio::time::timeout(Duration::from_secs(5), l.accept())
                .await
                .is_ok_and(|r| r.is_ok()),
            "control: the open flag pair must reach localhost here"
        );
        reach.abort();
        // A name: only the store client's resolver stands in the way.
        let (l, port) = bind().await;
        let h = factory.make_signed(&format!("http://localhost:{port}/step"), secret.clone());
        let outcome = h.execute(&spec_of("s"), b"hi", "k").await;
        // DECISIVE(signed-store-client)
        assert!(
            !reached(l).await,
            "a signed handler dialed loopback by name: it has the flag client"
        );
        assert!(matches!(outcome, Outcome::Failed), "{outcome:?}");
        // A literal: no resolver runs, so the store policy's check must.
        let (l, port) = bind().await;
        let h = factory.make_signed(&format!("http://127.0.0.1:{port}/step"), secret);
        let outcome = h.execute(&spec_of("s"), b"hi", "k").await;
        // DECISIVE(signed-store-policy)
        assert!(
            !reached(l).await,
            "a signed handler dialed a loopback literal: it has the flag policy"
        );
        assert!(matches!(outcome, Outcome::Failed), "{outcome:?}");
    }

    /// White-hat finding A, end to end: a handler URL naming a
    /// `--deny-cidr` address (as a literal, and as a name resolving to it)
    /// or this host's own interface address fails its attempt and probe,
    /// refuses at registration, and the listener there never sees a
    /// connection. The same listener answers an open policy — the control
    /// that makes "no connection" mean the guard, not a dead listener.
    #[tokio::test]
    async fn a_denied_or_own_address_gets_no_packet() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        let spec = StepSpec {
            name: "s".into(),
            handler: "h".into(),
        };
        let factory = |policy: &ssrf::Policy| {
            HandlerFactory::new(
                policy.clone(),
                policy.clone(),
                Duration::from_secs(2),
                1024,
                pubid::PubId::ephemeral(),
            )
            .unwrap_or_else(|e| panic!("{e}"))
        };
        let quiet = |l: tokio::net::TcpListener| async move {
            tokio::time::timeout(Duration::from_millis(400), l.accept())
                .await
                .is_err()
        };
        // 1. --deny-cidr, under --allow-local-handlers: the deny still holds.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let deny = ssrf::Policy::new(
            true,
            Vec::new(),
            vec![
                (IpAddr::V4(Ipv4Addr::LOCALHOST), 32),
                (IpAddr::V6(Ipv6Addr::LOCALHOST), 128),
            ],
        );
        let literal = format!("http://{addr}/step");
        for url in [
            literal.clone(),
            format!("http://localhost:{}/step", addr.port()),
        ] {
            let h = factory(&deny).make(&url);
            let outcome = h.execute(&spec, b"hi", "k").await;
            // DECISIVE(ssrf-deny-cidr)
            assert!(matches!(outcome, Outcome::Failed), "{url}: {outcome:?}");
            assert!(!h.ask(None, "k").await, "{url}: the probe is refused too");
        }
        assert!(valid_handler_url(&literal, &deny).is_err());
        assert!(
            quiet(listener).await,
            "a packet reached a --deny-cidr address"
        );
        // The control: the open policy reaches a listener there.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let call = factory(&ssrf::Policy::open()).make(&format!("http://{addr}/step"));
        let reach = tokio::spawn(async move { call.execute(&spec_of("s"), b"hi", "k").await });
        let got = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await;
        assert!(
            matches!(got, Ok(Ok(_))),
            "control: the open policy must connect"
        );
        reach.abort();

        // 2. This host's own interface address, with the shipped policy.
        let own = ssrf::host_addresses()
            .unwrap_or_else(|e| panic!("{e}"))
            .into_iter()
            .find(|ip| ip.is_ipv4() && !ip.is_loopback());
        let Some(own) = own else {
            eprintln!(
                "NOTICE: no non-loopback IPv4 interface address here; the own-address \
                 path is proved by ssrf::tests::the_hosts_own_addresses_are_refused_by_both_layers"
            );
            return;
        };
        let listener = tokio::net::TcpListener::bind((own, 0))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let shipped = ssrf::Policy::boot(false, Vec::new()).unwrap_or_else(|e| panic!("{e}"));
        let url = format!("http://{addr}/step");
        let h = factory(&shipped).make(&url);
        assert!(matches!(
            h.execute(&spec, b"hi", "k").await,
            Outcome::Failed
        ));
        assert!(valid_handler_url(&url, &shipped).is_err());
        assert!(
            quiet(listener).await,
            "a packet reached this host's own address {own}"
        );
    }

    fn spec_of(name: &str) -> StepSpec {
        StepSpec {
            name: name.into(),
            handler: "h".into(),
        }
    }

    /// S2-5 at registration: without `--allow-local-handlers` a literal
    /// local target refuses at PUT with 422 `local_target_forbidden`;
    /// a domain defers to the resolver and registers fine.
    #[tokio::test]
    async fn registration_refuses_literal_local_targets() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1ab1";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let engine = spawn(
            journal.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        // The strict app — allow_local false, as the shipped default runs.
        let keyring = Arc::new(Keyring::new(ApiKey::new(KEY), None, Some(store)));
        let r = router(
            test_app(engine, keyring, false, None, journal.path(), false, false),
            None,
        );
        for bad in [
            "http://127.0.0.1:9/h",
            "http://[::1]:9/h",
            "http://169.254.169.254/latest/meta-data",
            "http://2130706433/", // decimal loopback — normalized by the parser
        ] {
            let (status, v) = call(
                &r,
                "PUT",
                "/handlers/h",
                Some(key_a),
                Some(json!({ "url": bad })),
            )
            .await;
            assert_eq!(
                (status, v["code"].as_str().unwrap_or("")),
                (StatusCode::UNPROCESSABLE_ENTITY, "local_target_forbidden"),
                "{bad} must refuse: {v}"
            );
        }
        // A domain registers — the resolver judges it at call time.
        let (status, v) = call(
            &r,
            "PUT",
            "/handlers/h",
            Some(key_a),
            Some(json!({ "url": "https://handler.example/h" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
    }

    /// `--open-reads` serves reads without a key; it is refused at startup
    /// unless the listen address is a loopback IP.
    #[tokio::test]
    async fn open_reads_are_loopback_only() {
        for ok in ["127.0.0.1:58300", "[::1]:58300", "127.0.0.2:1"] {
            check_open_reads(ok, true).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in [
            "0.0.0.0:58300",
            "[::]:58300",
            "10.0.0.5:58300",
            "localhost:58300",
        ] {
            assert!(check_open_reads(bad, true).is_err(), "{bad}");
            check_open_reads(bad, false).unwrap_or_else(|e| panic!("{bad}: {e}"));
        }
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
        let engine =
            spawn(journal.path(), reg, ShellConfig::default()).unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                true,
                None,
                journal.path(),
                true,
                false,
            ),
            None,
        );
        let get = |host: Option<&'static str>| {
            let r = r.clone();
            async move {
                let mut req = Request::builder().uri("/jobs");
                if let Some(h) = host {
                    req = req.header("host", h);
                }
                r.oneshot(req.body(Body::empty()).unwrap_or_else(|e| panic!("{e}")))
                    .await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .status()
            }
        };
        for host in [
            "127.0.0.1:58300",
            "localhost:58300",
            "localhost",
            "[::1]:58300",
        ] {
            assert_eq!(get(Some(host)).await, StatusCode::OK, "Host {host}");
        }
        // Review U6: DNS rebinding. A page in the operator's browser can
        // point its own name at 127.0.0.1; its requests then carry that
        // name as Host. Open reads answer only loopback Hosts.
        for host in [
            "attacker.example",
            "attacker.example:58300",
            "127.0.0.1.attacker.example",
            "10.0.0.5:58300",
        ] {
            assert_eq!(
                get(Some(host)).await,
                StatusCode::UNAUTHORIZED,
                "Host {host}"
            );
        }
        assert_eq!(get(None).await, StatusCode::UNAUTHORIZED, "no Host");
        let (status, _) = call(&r, "GET", "/jobs", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK, "the key reads whatever the Host");
        let (status, _) = call(&r, "POST", "/jobs", None, Some(submit_body("x"))).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "writes still need the key"
        );
    }

    /// Checklist P0: reads (`GET /jobs`, `GET /jobs/{id}`), which show every
    /// op_key and state, were open on any listen address. They need the
    /// key unless the server was started with `--open-reads` (loopback
    /// only); liveness and readiness stay open.
    #[tokio::test]
    async fn reads_need_the_key_by_default() {
        let h = harness(false);
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("r")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let id = v["run_id"].as_str().unwrap_or_default().to_string();
        for uri in [
            "/jobs".to_string(),
            format!("/jobs/{id}"),
            "/jobs?order=desc&limit=5".to_string(),
        ] {
            for key in [None, Some("wrong")] {
                let (status, v) = call(&h.router, "GET", &uri, key, None).await;
                assert_eq!(
                    (status, v["code"].clone()),
                    (StatusCode::UNAUTHORIZED, json!("unauthorized")),
                    "GET {uri} key={key:?}"
                );
            }
            let (status, _) = call(&h.router, "GET", &uri, Some(KEY), None).await;
            assert_eq!(status, StatusCode::OK, "GET {uri} with the key");
        }
        for uri in ["/health", "/ready"] {
            let (status, _) = call(&h.router, "GET", uri, None, None).await;
            assert_eq!(status, StatusCode::OK, "{uri} stays open");
        }
    }

    /// A backup goes only to `<backup-root>/<name>`: separators, `..`,
    /// absolute paths and hidden names are refused, and an existing
    /// backup is never overwritten or merged into.
    #[tokio::test]
    async fn backups_land_only_under_the_configured_root() {
        let h = harness(true);
        let outside = h.backups.path().join("outside");
        for name in [
            "../escape",
            "..",
            ".",
            ".hidden",
            "a/b",
            "",
            outside.to_str().unwrap_or_else(|| panic!("utf-8 path")),
            &"x".repeat(65),
        ] {
            let (status, v) = call(
                &h.router,
                "POST",
                "/admin/backup",
                Some(KEY),
                Some(json!({ "name": name })),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "name {name:?}: {v}");
            assert_eq!(v["code"], "invalid_backup_name");
        }
        assert!(!outside.exists(), "nothing may be written outside the root");
        assert!(!h.backups.path().join("escape").exists());

        let ok = json!({ "name": "nightly-1" });
        let (status, v) = call(
            &h.router,
            "POST",
            "/admin/backup",
            Some(KEY),
            Some(ok.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert!(h
            .backups
            .path()
            .join("root/nightly-1/manifest.json")
            .exists());
        // Checklist: a symlink planted under the root at a backup's name is
        // an existing name (409), and nothing is written through it.
        #[cfg(unix)]
        {
            let elsewhere = h.backups.path().join("elsewhere");
            std::fs::create_dir(&elsewhere).unwrap_or_else(|e| panic!("{e}"));
            std::os::unix::fs::symlink(&elsewhere, h.backups.path().join("root/planted"))
                .unwrap_or_else(|e| panic!("{e}"));
            let (status, v) = call(
                &h.router,
                "POST",
                "/admin/backup",
                Some(KEY),
                Some(json!({ "name": "planted" })),
            )
            .await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::CONFLICT, json!("backup_exists"))
            );
            assert_eq!(
                std::fs::read_dir(&elsewhere)
                    .map(|d| d.count())
                    .unwrap_or(1),
                0,
                "a backup was written through the symlink"
            );
        }
        let (status, v) = call(&h.router, "POST", "/admin/backup", Some(KEY), Some(ok)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{v}");
        assert_eq!(v["code"], "backup_exists");
    }

    #[tokio::test]
    async fn backups_are_disabled_without_a_root() {
        let h = harness(false);
        let (status, v) = call(
            &h.router,
            "POST",
            "/admin/backup",
            Some(KEY),
            Some(json!({ "name": "b" })),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(v["code"], "backups_disabled");
    }

    /// A full disk is a 503 with the stable code `no_space` on every write
    /// route (submit, compact, backup), readiness flips false, and both
    /// recover when space returns.
    #[tokio::test]
    async fn full_disk_is_503_no_space_and_readiness_false() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        let free = Arc::new(AtomicU64::new(1 << 20));
        let h = harness_with(true, Some(free.clone()));
        let (status, v) = call(&h.router, "GET", "/ready", None, None).await;
        assert_eq!((status, v["ready"].clone()), (StatusCode::OK, json!(true)));
        let (status, _) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("a")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        free.store(100, SeqCst); // far below the 4 KiB reserve
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("b")),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(v["code"], "no_space");
        let (status, v) = call(&h.router, "GET", "/ready", None, None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            (v["ready"].clone(), v["code"].clone()),
            (json!(false), json!("no_space"))
        );
        let (status, v) = call(&h.router, "POST", "/admin/compact", Some(KEY), None).await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("no_space"))
        );
        let (status, v) = call(
            &h.router,
            "POST",
            "/admin/backup",
            Some(KEY),
            Some(json!({ "name": "full" })),
        )
        .await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("no_space"))
        );
        assert!(!h.backups.path().join("root/full").exists());

        free.store(1 << 20, SeqCst);
        let mut ready = false;
        for _ in 0..100 {
            let (status, _) = call(&h.router, "GET", "/ready", None, None).await;
            if status == StatusCode::OK {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(ready, "readiness recovers once space returns");
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("b")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert!(
            v["run_id"].as_str().is_some_and(|s| s.len() == 11),
            "the second submit returns an opaque public id: {v}"
        );
    }

    /// `GET /jobs` is paged by run id: `limit` (default 100, at most 1000)
    /// and `after`; `next_after` points at the next page or is null.
    #[tokio::test]
    async fn list_is_paged() {
        let h = harness(false);
        for i in 0..5 {
            let (status, _) = call(
                &h.router,
                "POST",
                "/jobs",
                Some(KEY),
                Some(submit_body(&format!("page-{i}"))),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        // Public ids are opaque strings; the test holds the key, so it
        // checks the *decoded* sequence — a client sees only the strings.
        let ids = |v: &Value| -> Vec<u64> {
            v["runs"]
                .as_array()
                .unwrap_or_else(|| panic!("runs array: {v}"))
                .iter()
                .filter_map(|r| r["id"].as_str())
                .map(|s| h.app.pubid.decode(s).unwrap_or_else(|| panic!("{s}")))
                .collect()
        };
        let cursor = |v: &Value| -> Option<u64> {
            v["next_after"]
                .as_str()
                .map(|s| h.app.pubid.decode(s).unwrap_or_else(|| panic!("{s}")))
        };
        let (_, v) = call(&h.router, "GET", "/jobs?limit=2", Some(KEY), None).await;
        assert_eq!((ids(&v), cursor(&v)), (vec![1, 2], Some(2)));
        let after2 = v["next_after"].as_str().unwrap_or_default().to_string();
        let (_, v) = call(
            &h.router,
            "GET",
            &format!("/jobs?after={after2}&limit=2"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), cursor(&v)), (vec![3, 4], Some(4)));
        let after4 = v["next_after"].as_str().unwrap_or_default().to_string();
        let (_, v) = call(
            &h.router,
            "GET",
            &format!("/jobs?after={after4}&limit=2"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), cursor(&v)), (vec![5], None));
        let (status, v) = call(&h.router, "GET", "/jobs?limit=100000", Some(KEY), None).await;
        assert_eq!(
            (status, ids(&v).len()),
            (StatusCode::OK, 5),
            "limit is clamped, not refused"
        );
    }

    /// Newest first, for the inspector: `order=desc` pages down from the
    /// newest run with a `before` cursor and `next_before`.
    #[tokio::test]
    async fn list_pages_newest_first() {
        let h = harness(false);
        for i in 0..5 {
            let (status, _) = call(
                &h.router,
                "POST",
                "/jobs",
                Some(KEY),
                Some(submit_body(&format!("desc-{i}"))),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        let ids = |v: &Value| -> Vec<u64> {
            v["runs"]
                .as_array()
                .unwrap_or_else(|| panic!("runs array: {v}"))
                .iter()
                .filter_map(|r| r["id"].as_str())
                .map(|s| h.app.pubid.decode(s).unwrap_or_else(|| panic!("{s}")))
                .collect()
        };
        let before =
            |v: &Value| -> Option<String> { v["next_before"].as_str().map(str::to_string) };
        let dec = |v: &Value| -> Option<u64> {
            v["next_before"]
                .as_str()
                .map(|s| h.app.pubid.decode(s).unwrap_or_else(|| panic!("{s}")))
        };
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&limit=2",
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), dec(&v)), (vec![5, 4], Some(4)));
        let b4 = before(&v).unwrap_or_default();
        let (_, v) = call(
            &h.router,
            "GET",
            &format!("/jobs?order=desc&before={b4}&limit=2"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), dec(&v)), (vec![3, 2], Some(2)));
        let b2 = before(&v).unwrap_or_default();
        let (_, v) = call(
            &h.router,
            "GET",
            &format!("/jobs?order=desc&before={b2}&limit=2"),
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), dec(&v)), (vec![1], None));
    }

    /// Request bodies are capped well below axum's 2 MB default: a submit
    /// can never legitimately exceed the input limit plus its spec.
    #[tokio::test]
    async fn oversized_request_body_is_413() {
        let h = harness(false);
        let mut body = submit_body("big");
        body["spec"]["input_b64"] = json!("A".repeat(1 << 20));
        let (status, _) = call(&h.router, "POST", "/jobs", Some(KEY), Some(body)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// A raw HTTP handler that answers every request with `reply`, then
    /// (for a streamed reply) keeps writing `chunk` forever.
    async fn raw_handler(head: &'static str, chunk: Option<&'static [u8]>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await; // the request; contents irrelevant
                    if sock.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    match chunk {
                        Some(c) => loop {
                            let frame = format!("{:x}\r\n", c.len());
                            if sock.write_all(frame.as_bytes()).await.is_err()
                                || sock.write_all(c).await.is_err()
                                || sock.write_all(b"\r\n").await.is_err()
                            {
                                return; // the client hung up: the bound worked
                            }
                        },
                        None => tokio::time::sleep(Duration::from_secs(30)).await,
                    }
                });
            }
        });
        format!("http://{addr}/step")
    }

    fn http_handler(url: String) -> HttpHandler {
        HttpHandler {
            client: handler_client(&ssrf::Policy::open()).unwrap_or_else(|e| panic!("{e}")),
            policy_url: reqwest::Url::parse(&url).ok(),
            url,
            secret: None,
            timeout: Duration::from_secs(10),
            max_output: 1024,
            policy: ssrf::Policy::open(),
            pubid: pubid::PubId::ephemeral(),
        }
    }

    async fn execute(h: &HttpHandler) -> Outcome {
        let step = StepSpec {
            name: "s".into(),
            handler: "h".into(),
        };
        tokio::time::timeout(Duration::from_secs(3), h.execute(&step, b"in", "k/0"))
            .await
            .unwrap_or_else(|_| panic!("a bounded read must not wait for the whole body"))
    }

    /// A reply that declares more than the output limit is refused from its
    /// headers: not a byte of the body is buffered.
    #[tokio::test]
    async fn reply_declared_over_the_limit_is_refused_unread() {
        let url = raw_handler("HTTP/1.1 200 OK\r\ncontent-length: 10485760\r\n\r\n{", None).await;
        assert!(matches!(execute(&http_handler(url)).await, Outcome::Failed));
    }

    /// A chunked reply with no length is read only up to the limit.
    #[tokio::test]
    async fn endless_chunked_reply_is_cut_at_the_limit() {
        static CHUNK: [u8; 512] = [b'x'; 512];
        let url = raw_handler(
            "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
            Some(&CHUNK),
        )
        .await;
        assert!(matches!(execute(&http_handler(url)).await, Outcome::Failed));
    }

    /// A listener that answers every connection with `head` and counts
    /// the connections it accepted.
    async fn counting_handler(head: String) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let head = head.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let _ = sock.write_all(head.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}/step"), seen)
    }

    /// Checklist: the handler client followed redirects, so a handler's
    /// 307 re-POSTed the job's input and idempotency key to wherever it
    /// pointed. A redirect is now an answer (a failed attempt), never
    /// followed.
    #[tokio::test]
    async fn a_handler_redirect_is_not_followed() {
        let ok = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}".to_string();
        let (elsewhere, seen) = counting_handler(ok).await;
        for code in [
            "301 Moved Permanently",
            "302 Found",
            "307 Temporary Redirect",
            "308 Permanent Redirect",
        ] {
            let head =
                format!("HTTP/1.1 {code}\r\nlocation: {elsewhere}\r\ncontent-length: 0\r\n\r\n");
            let (url, _) = counting_handler(head).await;
            let h = http_handler(url);
            assert!(matches!(execute(&h).await, Outcome::Failed), "{code}");
            let step = StepSpec {
                name: "s".into(),
                handler: "h".into(),
            };
            assert!(
                !h.probe(&step, "k/0").await,
                "{code}: a redirected probe is not found"
            );
        }
        assert_eq!(
            seen.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the redirect target was contacted"
        );
    }

    /// D-050: every handler call says which run, step and attempt it is
    /// answering. Driven through the real engine: step 0 fails once (so its
    /// retry is attempt 2), step 1's reply is lost (it answers after the
    /// timeout), and the probe that reconciles it names that attempt.
    #[tokio::test]
    async fn handler_calls_name_their_run_step_and_attempt() {
        type Seen = Arc<std::sync::Mutex<Vec<[String; 4]>>>;
        fn seen_call(seen: &Seen, method: &str, h: &axum::http::HeaderMap) -> [String; 4] {
            let get = |n: &str| {
                h.get(n)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string()
            };
            let call = [
                method.to_string(),
                get(RUN_ID_HEADER),
                get(STEP_HEADER),
                get(ATTEMPT_HEADER),
            ];
            seen.lock()
                .unwrap_or_else(|e| panic!("{e}"))
                .push(call.clone());
            call
        }
        async fn post(
            axum::extract::State(seen): axum::extract::State<Seen>,
            h: axum::http::HeaderMap,
        ) -> (StatusCode, &'static str) {
            let [_, _, step, attempt] = seen_call(&seen, "POST", &h);
            match (step.as_str(), attempt.as_str()) {
                ("0", "1") => (StatusCode::INTERNAL_SERVER_ERROR, "{}"),
                ("1", "1") => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    (StatusCode::OK, "{}")
                }
                _ => (StatusCode::OK, "{}"),
            }
        }
        async fn probe(
            axum::extract::State(seen): axum::extract::State<Seen>,
            h: axum::http::HeaderMap,
        ) -> StatusCode {
            seen_call(&seen, "GET", &h);
            StatusCode::OK
        }
        let seen: Seen = Arc::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let handler_app = Router::new()
            .route("/step", axum::routing::post(post).get(probe))
            .with_state(seen.clone());
        tokio::spawn(async move { axum::serve(listener, handler_app).await });

        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        let pid = pubid::PubId::ephemeral();
        reg.register(
            "step",
            Arc::new(HttpHandler {
                client: handler_client(&ssrf::Policy::open()).unwrap_or_else(|e| panic!("{e}")),
                policy_url: reqwest::Url::parse(&format!("http://{addr}/step")).ok(),
                url: format!("http://{addr}/step"),
                timeout: Duration::from_millis(300),
                max_output: 1024,
                secret: None,
                policy: ssrf::Policy::open(),
                pubid: pid.clone(),
            }),
        );
        let cfg = ShellConfig {
            tick_ms: 5,
            ..Default::default()
        };
        let engine = spawn(journal.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let step = |name: &str| durable_domain::StepSpec {
            name: name.into(),
            handler: "step".into(),
        };
        let run = engine
            .submit(
                durable_domain::OperationKey("calls/1".into()),
                durable_domain::JobSpec {
                    version: 1,
                    steps: vec![step("a"), step("b")],
                    input: b"in".to_vec(),
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let state = engine
                .inspect(run)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .map(|r| r.state);
            if state == Some(durable_domain::RunState::Done) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the run did not finish: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The header carries the public id — the internal sequence
        // would tell the handler how much other workspaces submitted.
        let id = pid.encode(run);
        let row = |m: &str, step: &str, attempt: &str| {
            [
                m.to_string(),
                id.clone(),
                step.to_string(),
                attempt.to_string(),
            ]
        };
        assert_eq!(
            *seen.lock().unwrap_or_else(|e| panic!("{e}")),
            vec![
                row("POST", "0", "1"),
                row("POST", "0", "2"),
                row("POST", "1", "1"),
                row("GET", "1", "1"),
            ]
        );
    }

    /// Handler URLs must be http(s) with a host: a job can only ever reach
    /// what the operator registered, over HTTP.
    #[test]
    fn handler_urls_must_be_http_or_https() {
        for ok in [
            "step=http://127.0.0.1:9/step",
            "s=https://handlers.example/step",
        ] {
            parse_handler(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in [
            "s=file:///etc/passwd",
            "s=ftp://h/x",
            "s=unix:/tmp/sock",
            "s=http://",
            "s=not a url",
            "=http://h/x",
        ] {
            assert!(parse_handler(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn reply_within_the_limit_is_checkpointed() {
        let url = raw_handler(
            "HTTP/1.1 200 OK\r\ncontent-length: 11\r\n\r\n{\"ok\":true}",
            None,
        )
        .await;
        match execute(&http_handler(url)).await {
            Outcome::Checkpoint(b) => assert_eq!(b, br#"{"ok":true}"#),
            other => panic!("expected a checkpoint, got {other:?}"),
        }
    }

    #[test]
    fn server_does_not_schedule_compaction_by_default() {
        let args = Args::try_parse_from(["durable-server"]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            args.compact_every_secs, 0,
            "scheduled compaction must be opt-in"
        );
    }

    /// Checklist: the dashboard at /app holds a key and renders op_keys.
    /// Its pages are served with a CSP that allows only its own scripts
    /// (the inline bootstrap by hash), no framing, nosniff and no
    /// referrer; /inspector now 308s to it and is never served.
    #[tokio::test]
    async fn the_inspector_is_served_with_security_headers() {
        let ui = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let script = "\n\t\t\t\tconsole.log('boot');\n\t\t\t";
        std::fs::write(
            ui.path().join("index.html"),
            format!("<!doctype html><html><body><script>{script}</script></body></html>"),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(ui.path().join("app.js"), "export {}").unwrap_or_else(|e| panic!("{e}"));
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let engine = spawn(journal.path(), Registry::default(), ShellConfig::default())
            .unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                false,
                None,
                journal.path(),
                true,
                false,
            ),
            Some(ui.path()),
        );
        use base64::Engine as _;
        let hash = base64::engine::general_purpose::STANDARD.encode(sha256(script.as_bytes()));
        for uri in ["/app/", "/app/app.js", "/app/runs/deep-link"] {
            let res = r
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap_or_else(|e| panic!("{e}")),
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            // An unknown path gets the app shell (with ServeDir's 404).
            assert!(
                res.status().is_success() || uri.ends_with("deep-link"),
                "{uri}"
            );
            let h = res.headers();
            let get = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            assert_eq!(get("x-content-type-options"), "nosniff", "{uri}");
            assert_eq!(get("referrer-policy"), "no-referrer", "{uri}");
            let csp = get("content-security-policy");
            for part in [
                "default-src 'none'",
                "frame-ancestors 'none'",
                "connect-src 'self'",
                &format!("script-src 'self' 'sha256-{hash}'"),
            ] {
                assert!(csp.contains(part), "{uri}: CSP {csp:?} lacks {part:?}");
            }
            let scripts = csp.split(';').find(|d| d.trim().starts_with("script-src"));
            assert!(
                scripts.is_some_and(|d| !d.contains("unsafe-inline")),
                "{uri}: scripts must not be unsafe-inline: {csp}"
            );
        }

        // The CSP hashes match the inline script bytes on disk.
        let csp_uri = r
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app/")
                    .body(Body::empty())
                    .unwrap_or_else(|e| panic!("{e}")),
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let csp = csp_uri
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(
            csp.contains(&format!("'sha256-{hash}'")),
            "CSP must hash the file's inline script: {csp}"
        );

        // /inspector[/tail] 308s to /app[/tail] with the query preserved.
        for (uri, want) in [
            ("/inspector", "/app"),
            ("/inspector/", "/app/"),
            ("/inspector/x?y=1", "/app/x?y=1"),
            (
                "/inspector/runs/deep?key=abc&p=2",
                "/app/runs/deep?key=abc&p=2",
            ),
        ] {
            let res = r
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap_or_else(|e| panic!("{e}")),
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(res.status(), StatusCode::PERMANENT_REDIRECT, "{uri}");
            assert_eq!(
                res.headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok()),
                Some(want),
                "{uri}"
            );
        }

        // A traversal tail cannot escape the ui dir.
        for uri in [
            "/app/../Cargo.toml",
            "/app/%2e%2e/%2e%2e/Cargo.toml",
            "/app/..%2f..%2fetc%2fpasswd",
        ] {
            let res = r
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap_or_else(|e| panic!("{e}")),
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let body = res
                .into_body()
                .collect()
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .to_bytes();
            let text = String::from_utf8_lossy(&body);
            assert!(
                !text.contains("[package]") && !text.contains("root:"),
                "{uri} escaped the ui dir"
            );
        }
    }

    /// Without `--ui-dir` there is no /app at all — 404, not an empty
    /// mount; /inspector still redirects so old links land on an honest
    /// 404 rather than a hang.
    #[tokio::test]
    async fn app_without_ui_dir_is_404() {
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let engine = spawn(journal.path(), Registry::default(), ShellConfig::default())
            .unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                false,
                None,
                journal.path(),
                true,
                false,
            ),
            None,
        );
        let res = r
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app/")
                    .body(Body::empty())
                    .unwrap_or_else(|e| panic!("{e}")),
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let res = r
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/inspector/x")
                    .body(Body::empty())
                    .unwrap_or_else(|e| panic!("{e}")),
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(res.status(), StatusCode::PERMANENT_REDIRECT);
    }

    /// The per-address bounds lifted the way the load battery lifts them
    /// (`--rate-limit-per-addr 100000 --max-conns-per-addr 0`): the
    /// limiter's code path still runs on every request, but a test that is
    /// not about D-052 cannot be flunked by its own burst. The D-052
    /// tests below name their own rate/conn caps explicitly.
    fn lifted_limits() -> HttpLimits {
        HttpLimits {
            rate_per_addr: 100_000.0,
            max_conns_per_addr: 0,
            max_conns_per_proxy: 0,
            ..HttpLimits::default()
        }
    }

    /// A served router with `limits` on 127.0.0.1; its address and a stop
    /// sender.
    async fn serving(
        limits: HttpLimits,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        Harness,
    ) {
        serving_on(limits, "127.0.0.1:0").await
    }

    /// `serving` bound on `bind` — `"[::]:0"` dual-stacks so one test can
    /// drive both 127.0.0.1 and ::1 clients at distinct address buckets.
    async fn serving_on(
        limits: HttpLimits,
        bind: &str,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        Harness,
    ) {
        let h = harness(false);
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(serve_http(
            listener,
            h.router.clone(),
            limits,
            Some(h.obs.clone()),
            stopped,
            false,
        ));
        (addr, stop, h)
    }

    /// Whether the server closes `sock` within `limit` (reads to EOF).
    async fn closed_within(sock: &mut tokio::net::TcpStream, limit: Duration) -> bool {
        use tokio::io::AsyncReadExt as _;
        let mut buf = [0u8; 1024];
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            match tokio::time::timeout_at(deadline, sock.read(&mut buf)).await {
                Err(_) => return false,
                Ok(Ok(0)) | Ok(Err(_)) => return true,
                Ok(Ok(_)) => {} // a response (e.g. 408) before the close
            }
        }
    }

    /// Review U2: with no header timeout, a client that sends part of a
    /// request (or opens a keep-alive connection and goes quiet) held its
    /// connection forever; 300 of them made `/health` time out. A head
    /// not finished within the header timeout is cut off.
    #[tokio::test]
    async fn a_slow_request_head_is_cut_off() {
        use tokio::io::AsyncWriteExt as _;
        let limits = HttpLimits {
            header_timeout: Duration::from_millis(300),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut partial = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        partial
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            closed_within(&mut partial, Duration::from_secs(5)).await,
            "a half-sent request head held its connection"
        );
        let mut silent = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            closed_within(&mut silent, Duration::from_secs(5)).await,
            "a silent connection was kept"
        );
    }

    /// A request body that trickles is cut off at the body deadline, so a
    /// client holding a key cannot hold a connection open forever either.
    #[tokio::test]
    async fn a_request_body_has_a_deadline() {
        use tokio::io::AsyncWriteExt as _;
        let limits = HttpLimits {
            body_deadline: Duration::from_millis(300),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut slow = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let head = format!(
            "POST /jobs HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {KEY}\r\n\
             content-type: application/json\r\ncontent-length: 1000\r\n\r\n{{\"op"
        );
        slow.write_all(head.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            closed_within(&mut slow, Duration::from_secs(5)).await,
            "a stalled body held its connection"
        );
    }

    /// Review V2: a body cut off at its deadline was answered 400, as if it
    /// were malformed. It is 408 `body_timeout`.
    #[tokio::test]
    async fn a_body_past_its_deadline_is_408() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            body_deadline: Duration::from_millis(300),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut slow = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let head = format!(
            "POST /jobs HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {KEY}\r\n\
             content-type: application/json\r\ncontent-length: 1000\r\n\r\n{{\"op"
        );
        slow.write_all(head.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), slow.read_to_end(&mut reply)).await;
        let text = String::from_utf8_lossy(&reply);
        assert!(text.starts_with("HTTP/1.1 408"), "{text}");
        assert!(text.contains("\"code\":\"body_timeout\""), "{text}");
    }

    /// At most `max_connections` are served at once; the rest wait to be
    /// accepted, and a slot frees when a held connection times out.
    #[tokio::test]
    async fn connections_are_capped_and_slots_come_back() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 4,
            header_timeout: Duration::from_millis(500),
            pressure_head_deadline: Duration::from_millis(800),
            pressure_poll: Duration::from_millis(50),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut held = Vec::new();
        for _ in 0..8 {
            let mut s = tokio::net::TcpStream::connect(addr)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // A probe from the SAME address as the flood cannot spend a
        // reserve slot (READY_PER_ADDR — F3): it draws the overload lane
        // and reads a real `503 + Retry-After`, never a bare RST. Once
        // the parked heads die at the pressure head deadline (~1 s) a
        // fresh probe gets a work slot and a 200.
        let mut probe = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("/health never answered while connections were held"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 200") || reply.starts_with(b"HTTP/1.1 503"),
            "the probe got no real answer: {}",
            String::from_utf8_lossy(&reply)
        );
        // The parked heads hold no head → they die at the pressure
        // deadline; a work slot frees and the next probe is served.
        for s in &mut held {
            assert!(
                closed_within(s, Duration::from_secs(10)).await,
                "a head that never arrived outlived the pressure deadline"
            );
        }
        let served = reqwest::Client::new()
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(served.status().as_u16(), 200);
        let _ = served.text().await;
    }

    /// S2 wedge: a conn's permit outlived its request for the whole
    /// keep-alive life, so `max_connections` parked sockets starved every
    /// later accept — /health included. Liveness keeps a reserved slice.
    #[tokio::test]
    async fn health_survives_a_pinned_connection_pool() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 4,
            // Long enough that an idle keep-alive still holds its slot
            // when the probe arrives.
            header_timeout: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        // Park every work slot on an unfinished request head: no response
        // is ever served, so the pressure-close rotation cannot free one
        // — any probe served at all came through the reserve.
        let mut held = Vec::new();
        for _ in 0..4 {
            let mut s = tokio::net::TcpStream::connect(addr)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // Every work slot is pinned: /health answers on the reserve.
        let mut probe = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("/health starved behind a pinned connection pool"))
            .unwrap_or_else(|e| panic!("{e}"));
        // DECISIVE(liveness-flood)
        assert!(
            reply.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&reply)
        );
        // The reserve serves liveness only: work gets a fast 503.
        let mut probe = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /jobs HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("the reserve must refuse non-liveness work fast"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 503"),
            "{}",
            String::from_utf8_lossy(&reply)
        );
    }

    /// Demand-driven eviction (S2 re-review N3): while the pool is dry
    /// and a conn waits, the longest-idle served conn yields — but only
    /// demand from a bucket that is NOT already over its share may
    /// claim a victim (the flood's excess is not legitimate demand).
    /// Over-share buckets are ordered before at-share ones.
    #[tokio::test]
    async fn an_idle_keep_alive_yields_to_real_demand_over_share_first() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 4,
            // Long enough that nothing here outlives the test on its own
            // deadline — only the eviction arms may close these conns:
            // the parked heads must still hold their slots when the
            // waiter arrives.
            header_timeout: Duration::from_secs(60),
            pressure_head_deadline: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving_on(limits, "[::]:0").await;
        let v4 = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port(),
        );
        let v6 = std::net::SocketAddr::new(
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            addr.port(),
        );
        // 127.0.0.1 bucket: one camper (served once, now idle) and two
        // parked heads — three conns of the four-slot pool.
        let mut camper = tokio::net::TcpStream::connect(v4)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        camper
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut buf = [0u8; 512];
        loop {
            match tokio::time::timeout(Duration::from_millis(400), camper.read(&mut buf)).await {
                Ok(Ok(0)) => panic!("the conn closed instead of camping keep-alive"),
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => panic!("{e}"),
                Err(_) => break, // quiet socket: response fully consumed
            }
        }
        let mut held = Vec::new();
        for _ in 0..2 {
            let mut s = tokio::net::TcpStream::connect(v4)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // The lone ::1 client also serves once then camps: its bucket
        // holds one conn — under its fair share (pool 4 / 2 buckets).
        let mut bystander = tokio::net::TcpStream::connect(v6)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        bystander
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        loop {
            match tokio::time::timeout(Duration::from_millis(400), bystander.read(&mut buf)).await {
                Ok(Ok(0)) => panic!("the bystander closed instead of camping"),
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => panic!("{e}"),
                Err(_) => break,
            }
        }
        // The pool is dry. A waiter from the over-share bucket is the
        // flood's excess: it may not claim anyone's conn.
        let flood_waiter = tokio::net::TcpStream::connect(v4)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        tokio::time::sleep(Duration::from_millis(500)).await;
        match tokio::time::timeout(Duration::from_millis(500), camper.read(&mut buf)).await {
            Err(_) => {} // still open: over-share demand evicts nobody
            Ok(Ok(0)) => panic!("a flood conn claimed the camper's slot"),
            Ok(Ok(_)) => panic!("unexpected bytes on the camper"),
            Ok(Err(e)) => panic!("{e}"),
        }
        // A waiter from an under-share bucket is real demand: it
        // reclaims the longest-idle conn, over-share buckets first —
        // the camper dies, the bystander keeps its slot.
        let waiter = tokio::net::TcpStream::connect(v6)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let n = tokio::time::timeout(Duration::from_secs(5), camper.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("an over-share camper kept its slot under demand"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(n, 0, "the over-share camper did not yield");
        // The at-share bystander must still be open and servable.
        bystander
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let n = tokio::time::timeout(Duration::from_secs(5), bystander.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("a client at its fair share was evicted"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(n > 0, "the at-share conn was closed, not just evicted");
        drop(waiter);
        drop(flood_waiter);
    }

    /// The other half of fair-share (S2 re-review design item 3): with
    /// every bucket exactly at its share, nothing may yield — the
    /// pressure endures (the two conns themselves hold the whole pool),
    /// so this discriminates "no eviction" from "pressure lifted". A
    /// mutant that yields any served idle conn goes red here.
    #[tokio::test]
    async fn clients_at_their_fair_share_are_not_evicted_under_pressure() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 2,
            header_timeout: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving_on(limits, "[::]:0").await;
        let v4 = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port(),
        );
        let v6 = std::net::SocketAddr::new(
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            addr.port(),
        );
        let mut campers = Vec::new();
        for peer in [v4, v6] {
            let mut s = tokio::net::TcpStream::connect(peer)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let mut buf = [0u8; 512];
            loop {
                match tokio::time::timeout(Duration::from_millis(400), s.read(&mut buf)).await {
                    Ok(Ok(0)) => panic!("the conn closed instead of camping keep-alive"),
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => panic!("{e}"),
                    Err(_) => break,
                }
            }
            campers.push(s);
        }
        // Pool of two, one conn per bucket, both at their share of one:
        // pressure is continuous for well over a head deadline, yet both
        // must still answer a second request.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        for (i, s) in campers.iter_mut().enumerate() {
            s.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let mut buf = [0u8; 512];
            let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
                .await
                .unwrap_or_else(|_| panic!("at-share conn {i} was evicted under pressure"))
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(n > 0, "at-share conn {i} was closed, not just idle");
        }
    }

    /// The yield's grace window, not amnesty (S2 re-review design item
    /// 2): a fresh conn that never sent a byte keeps a one-second head
    /// grace under pressure — it is not evicted at the first tick — but
    /// past PRESSURE_HEAD_DEADLINE a head that never arrived is evicted
    /// FIRST, before any eligible idle keep-alive.
    #[tokio::test]
    async fn a_fresh_head_gets_a_one_second_grace_then_dies_under_pressure() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 2,
            header_timeout: Duration::from_secs(60),
            // A 1.5 s deadline the grace window sits well inside, and a
            // 50 ms poll so the death lands promptly after it — not a
            // race against the default second.
            pressure_head_deadline: Duration::from_millis(1500),
            pressure_poll: Duration::from_millis(50),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut held = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        held.write_all(b"GET /health HTTP/1.1\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // The last slot goes to a conn that has sent nothing — it is in
        // its head grace, not camping. With the pool dry the parked
        // head's deadline is ~1.5 s from its own accept.
        let mut fresh = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // Inside the grace window both conns must still be open —
        // an instant eviction would read EOF or an error here.
        let mut buf = [0u8; 64];
        for _ in 0..5 {
            match held.try_read(&mut buf) {
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Ok(0) => panic!("the parked head was cut inside its grace second"),
                Ok(_) => panic!("unsolicited bytes on a conn mid-head"),
                Err(e) => panic!("the parked head errored inside its grace second: {e}"),
            }
            match tokio::time::timeout(Duration::from_millis(100), fresh.read(&mut buf)).await {
                Ok(Ok(0)) => panic!("a conn in its head grace was evicted under pressure"),
                Ok(Ok(_)) => panic!("unsolicited bytes on an idle conn"),
                Ok(Err(e)) => panic!("{e}"),
                Err(_) => {}
            }
        }
        fresh
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let n = tokio::time::timeout(Duration::from_secs(5), fresh.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("the grace conn was left unusable"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            buf[..n].starts_with(b"HTTP/1.1 200"),
            "expected 200, got {:?}",
            String::from_utf8_lossy(&buf[..n])
        );
        // Past the deadline, under a still-dry pool, the dead head is
        // evicted first — long before the 60 s header timeout.
        // DECISIVE(slot-camp)
        assert!(
            closed_within(&mut held, Duration::from_secs(4)).await,
            "a head that never arrived outlived the pressure deadline"
        );
    }

    /// S2 re-review design item 1: a full backlog is never the answer.
    /// Work and liveness pools dry, a conn must read a real
    /// `503 overloaded` + `Retry-After` — not ride the kernel backlog to
    /// a reset the client cannot tell from a crash.
    #[tokio::test]
    async fn an_overloaded_connection_reads_a_503_not_a_reset() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 1,
            header_timeout: Duration::from_secs(60),
            // The parked reserve conns must still hold their slots when
            // the probe arrives — a parked ready head outliving the
            // default second would open a reserve slot and send the
            // probe to a 200 instead of the lane.
            ready_head_timeout: Duration::from_secs(60),
            pressure_head_deadline: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        // Dry the work pool with a parked head…
        let mut pin = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        pin.write_all(b"GET /health HTTP/1.1\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // …and the liveness reserve with READY_PER_ADDR parked heads
        // from the same address bucket.
        let mut held = Vec::new();
        for _ in 0..READY_PER_ADDR {
            let mut s = tokio::net::TcpStream::connect(addr)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // The next conn must get a REAL answer fast — the lane's 503 —
        // not silence or a reset.
        let mut probe = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("an overloaded conn got no answer — it rode the backlog"))
            .unwrap_or_else(|e| panic!("{e}"));
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("HTTP/1.1 503"),
            "expected 503 overloaded, got: {text}"
        );
        assert!(
            text.contains("retry-after") || text.contains("Retry-After"),
            "no Retry-After in the refusal: {text}"
        );
        assert!(
            text.contains("\"overloaded\""),
            "body was not the lane's: {text}"
        );
    }

    /// N1: the PRIMARY lane arm — work pool AND the whole liveness
    /// reserve both dry — owes every conn a readable 503, never an
    /// unanswered close (the committed-mutant shape: the lane took the
    /// permit and answered nothing). Four distinct address buckets drive
    /// the branch so it cannot be reached by the per-address fallbacks.
    #[tokio::test]
    async fn the_overload_lane_answers_conns_from_four_buckets() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 1,
            ready_pool: 4,
            ready_per_addr: 1,
            header_timeout: Duration::from_secs(60),
            // The parked heads must still hold their slots when the
            // probes arrive.
            ready_head_timeout: Duration::from_secs(60),
            pressure_head_deadline: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving_on(limits, "0.0.0.0:0").await;
        // Find at least four distinct local source addresses — each is
        // its own bucket. 127/8 works on Linux; a macOS host answers for
        // 127.0.0.1, any lo0 aliases, and its own interface address —
        // found by asking the kernel which source it would use for a
        // few destinations (a UDP connect sends no packet; a VPN may own
        // the default route, so the private ranges are asked too), never
        // a hard-coded LAN address that changes with the network.
        let primary = [
            "192.0.2.1:9",
            "192.168.0.1:9",
            "192.168.1.1:9",
            "10.0.0.1:9",
            "172.16.0.1:9",
        ]
        .into_iter()
        .filter_map(|dst| {
            let u = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
            u.connect(dst).ok()?;
            Some(u.local_addr().ok()?.ip().to_string())
        })
        .collect::<Vec<_>>();
        let mut candidates: Vec<String> = [
            "127.0.0.1",
            "127.0.0.2",
            "127.0.0.3",
            "127.0.0.4",
            "127.0.2.2",
            "127.0.2.3",
        ]
        .map(String::from)
        .to_vec();
        for a in primary {
            if !candidates.contains(&a) {
                candidates.push(a);
            }
        }
        let connect_to = |src: String, dst: std::net::SocketAddr| async move {
            let local =
                std::net::SocketAddr::new(src.parse().unwrap_or_else(|_| panic!("{src}")), 0);
            let sock = tokio::net::TcpSocket::new_v4().ok()?;
            sock.bind(local).ok()?;
            // A source the host cannot route from (a VPN tunnel's
            // address) must not stall the search.
            tokio::time::timeout(Duration::from_secs(1), sock.connect(dst))
                .await
                .ok()?
                .ok()
        };
        let connect_from = |src: String| connect_to(src, addr);
        // Probe the candidates against a throwaway listener, never the
        // server under test: a probe conn it is still serving would hold
        // a slot (or its address's reserve share) when the real conns
        // arrive, and the scenario below would race it.
        let scout = tokio::net::TcpListener::bind("0.0.0.0:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let scout_addr = scout.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let mut addrs = Vec::new();
        for src in candidates {
            if let Some(c) = connect_to(src.clone(), scout_addr).await {
                drop(c);
                addrs.push(src);
                if addrs.len() == 4 {
                    break;
                }
            }
        }
        drop(scout);
        assert!(
            addrs.len() >= 4,
            "need >= 4 distinct local source addresses, found {addrs:?}"
        );
        // Dry the work pool and all four reserve slots, one per bucket —
        // every further accept must take the primary lane arm.
        let mut pinned = connect_from(addrs[0].parse().unwrap_or_else(|_| panic!("addr")))
            .await
            .unwrap_or_else(|| panic!("the pinned conn failed to connect"));
        pinned
            .write_all(b"GET /health HTTP/1.1\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut held = Vec::new();
        for src in &addrs {
            let mut s = connect_from(src.parse().unwrap_or_else(|_| panic!("{src}")))
                .await
                .unwrap_or_else(|| panic!("{src}: reserve conn failed to connect"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // Now the primary lane arm: a conn from each of the four buckets
        // must READ `503 overloaded` + `Retry-After` — never an
        // unanswered close.
        for src in &addrs {
            let mut probe = connect_from(src.parse().unwrap_or_else(|_| panic!("{src}")))
                .await
                .unwrap_or_else(|| panic!("{src}: probe conn failed to connect"));
            probe
                .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let mut reply = Vec::new();
            let read =
                tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply)).await;
            // An unanswered conn shows up three ways, all the same failure:
            // a reset (the close found our request bytes queued — usual on
            // macOS), a bare EOF (closed before they arrived — usual on
            // Linux), or a hang. Each lands on the one assertion the lane
            // guards: the conn READ a 503.
            let text = match read {
                Ok(Ok(_)) => String::from_utf8_lossy(&reply).into_owned(),
                Ok(Err(e)) => format!("<reset, no answer: {e}>"),
                Err(_) => "<no answer within 5 s>".to_string(),
            };
            // DECISIVE(overload-lane)
            assert!(
                text.starts_with("HTTP/1.1 503"),
                "{src}: expected the lane's 503, got: {text}"
            );
            assert!(
                text.to_lowercase().contains("retry-after"),
                "{src}: no Retry-After in the refusal: {text}"
            );
            assert!(
                text.contains("\"overloaded\""),
                "{src}: body was not the lane's: {text}"
            );
        }
    }

    /// S2 re-review F2: a refused request is still a SERVED request — a
    /// conn that only draws 429s must hit the request cap and close like
    /// any other, not camp yield-ineligible to max_lifetime.
    #[tokio::test]
    async fn a_429_only_connection_still_hits_the_request_cap() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 4,
            max_requests: 3,
            rate_per_addr: 0.001, // every request 429s
            header_timeout: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut sock = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut buf = [0u8; 1024];
        // The bucket bursts one token: request 0 is served (200) and the
        // rest draw 429. The cap must still fire on answers 1..=3 —
        // under the bug a 429 never incremented `served`, so this conn
        // would ride to max_lifetime.
        for i in 0..3 {
            sock.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            // Read the response before sending the next — HTTP/1.1
            // keep-alive is sequential here.
            let mut got = Vec::new();
            loop {
                let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf))
                    .await
                    .unwrap_or_else(|_| panic!("no answer to request {i}"))
                    .unwrap_or_else(|e| panic!("{e}"));
                if n == 0 {
                    panic!("conn closed after only {i} answers");
                }
                got.extend_from_slice(&buf[..n]);
                if got.windows(4).any(|w| w == b"\r\n\r\n") && got.len() > 40 {
                    break;
                }
            }
            let want = if i == 0 {
                b"HTTP/1.1 200"
            } else {
                b"HTTP/1.1 429"
            };
            assert!(
                got.starts_with(want),
                "request {i}: {}",
                String::from_utf8_lossy(&got)
            );
        }
        // Three answers hit the same request cap as three 200s: the conn
        // must close rather than keep a slot on refused requests.
        assert!(
            closed_within(&mut sock, Duration::from_secs(5)).await,
            "a conn whose later answers were 429s escaped the request cap"
        );
    }

    /// S2 re-review F3: the liveness reserve is per-address — one
    /// flooding address may hold at most READY_PER_ADDR reserve slots,
    /// so a probe from a DIFFERENT address still reaches /health while
    /// the flood's extra conns take the 503 lane.
    #[tokio::test]
    async fn the_liveness_reserve_is_capped_per_address() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 1,
            header_timeout: Duration::from_secs(60),
            // The parked work/reserve heads must still hold their slots
            // when the probes arrive — same race as the lane test.
            ready_head_timeout: Duration::from_secs(60),
            pressure_head_deadline: Duration::from_secs(60),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving_on(limits, "[::]:0").await;
        let v4 = std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port(),
        );
        let v6 = std::net::SocketAddr::new(
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            addr.port(),
        );
        let mut pin = tokio::net::TcpStream::connect(v4)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        pin.write_all(b"GET /health HTTP/1.1\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // READY_PER_ADDR parked heads from the flooding bucket.
        let mut held = Vec::new();
        for _ in 0..READY_PER_ADDR {
            let mut s = tokio::net::TcpStream::connect(v4)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            s.write_all(b"GET /health HTTP/1.1\r\n")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            held.push(s);
        }
        // The flooder's next conn is over its reserve budget → the lane.
        let mut extra = tokio::net::TcpStream::connect(v4)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        extra
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), extra.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("the over-budget conn got no answer"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 503"),
            "expected the lane's 503, got: {}",
            String::from_utf8_lossy(&reply)
        );
        // A probe from a different bucket still draws a reserve slot.
        let mut probe = tokio::net::TcpStream::connect(v6)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("the ::1 probe got no answer"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 200"),
            "the other bucket's probe lost liveness: {}",
            String::from_utf8_lossy(&reply)
        );
    }

    /// D-052: one flooding address is refused 429 `rate_limited` with a
    /// Retry-After while another address proceeds. The test runs with
    /// loopback "trusted" so `CF-Connecting-IP` stands in for distinct
    /// client addresses behind a proxy.
    #[tokio::test]
    async fn a_flooding_address_is_limited_while_another_proceeds() {
        let limits = HttpLimits {
            rate_per_addr: 5.0,
            trusted_proxies: TrustedProxies(vec![
                IpNet::parse("127.0.0.0/8").unwrap_or_else(|e| panic!("{e}"))
            ]),
            ..HttpLimits::default()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let client = reqwest::Client::new();
        let get = |from: &str| {
            client
                .get(format!("http://{addr}/health"))
                .header("cf-connecting-ip", from)
                .send()
        };
        for i in 0..5 {
            let res = get("198.51.100.7").await.unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(res.status().as_u16(), 200, "request {i}");
        }
        let refused = get("198.51.100.7").await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(refused.status().as_u16(), 429);
        let retry = refused
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        assert_eq!(retry, Some(1), "Retry-After seconds");
        let body: serde_json::Value = refused.json().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(body["code"], json!("rate_limited"), "{body}");
        let other = get("203.0.113.9").await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            other.status().as_u16(),
            200,
            "a different address is not punished for the flood"
        );
    }

    /// D-052: `CF-Connecting-IP` from a peer outside the trusted ranges is
    /// ignored — the requests are counted against the peer's own address.
    /// Once the peer's bucket is drained the spoofed-header requests are
    /// refused; had the header been believed, each would have landed in a
    /// fresh bucket and sailed through.
    #[tokio::test]
    async fn a_spoofed_client_header_from_an_untrusted_peer_is_ignored() {
        let limits = HttpLimits {
            rate_per_addr: 5.0,
            trusted_proxies: TrustedProxies(vec![
                IpNet::parse("10.0.0.0/8").unwrap_or_else(|e| panic!("{e}"))
            ]),
            ..HttpLimits::default()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let client = reqwest::Client::new();
        let health = format!("http://{addr}/health");
        for i in 0..5 {
            let res = client
                .get(&health)
                .send()
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(res.status().as_u16(), 200, "request {i}");
        }
        // The peer's bucket is now drained. A spoofed header does not move
        // the count to a different address.
        let res = client
            .get(&health)
            .header("cf-connecting-ip", "198.51.100.7")
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(res.status().as_u16(), 429, "the header was believed");
    }

    /// D-052: from a trusted peer the header names the client address, so
    /// two "clients" on one connection get separate buckets.
    #[tokio::test]
    async fn a_trusted_proxy_names_the_client() {
        let limits = HttpLimits {
            rate_per_addr: 5.0,
            trusted_proxies: TrustedProxies(vec![
                IpNet::parse("127.0.0.0/8").unwrap_or_else(|e| panic!("{e}"))
            ]),
            ..HttpLimits::default()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let client = reqwest::Client::new();
        let health = format!("http://{addr}/health");
        for _ in 0..6 {
            client
                .get(&health)
                .header("cf-connecting-ip", "198.51.100.7")
                .send()
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        // The flood drained 198.51.100.7's bucket, not the peer's: a
        // headerless request (counted by peer address) still proceeds.
        let res = client
            .get(&health)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            res.status().as_u16(),
            200,
            "the flood was counted against the peer, not the header"
        );
    }

    /// D-052: past the per-peer connection cap the socket is closed at
    /// accept — it never reaches a request — while the connections under
    /// the cap keep serving.
    #[tokio::test]
    async fn the_per_peer_connection_cap_closes_the_excess() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_conns_per_addr: 2,
            ..HttpLimits::default()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let a = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let _b = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut extra = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            closed_within(&mut extra, Duration::from_secs(5)).await,
            "a third connection from one peer was kept"
        );
        // A slot frees when a held connection closes.
        drop(_b);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut c = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        c.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), c.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("/health never answered"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&reply)
        );
        drop(a);
    }

    /// D-052: a peer inside the trusted-proxy ranges gets the larger
    /// proxy cap — a proxy's edge legitimately carries many clients, so
    /// the untrusted-peer cap must not throttle it.
    #[tokio::test]
    async fn a_trusted_proxy_peer_gets_the_higher_connection_cap() {
        let limits = HttpLimits {
            max_conns_per_addr: 2,
            max_conns_per_proxy: 4,
            trusted_proxies: TrustedProxies(vec![
                IpNet::parse("127.0.0.0/8").unwrap_or_else(|e| panic!("{e}"))
            ]),
            ..HttpLimits::default()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(
                tokio::net::TcpStream::connect(addr)
                    .await
                    .unwrap_or_else(|e| panic!("{e}")),
            );
        }
        let mut extra = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            closed_within(&mut extra, Duration::from_secs(5)).await,
            "a fifth connection from a trusted peer was kept"
        );
        drop(held);
    }

    /// Review U11: base64 behaviour was checked by one valid input. Every
    /// byte value round-trips, and non-canonical input (missing padding,
    /// non-zero trailing bits, stray characters) is 400 `bad_input` with
    /// a stable message that does not echo the decoder's own text.
    #[tokio::test]
    async fn input_is_canonical_standard_base64() {
        use base64::Engine as _;
        let all: Vec<u8> = (0..=255).collect();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&all);
        assert_eq!(
            base64_decode(&encoded).unwrap_or_else(|e| panic!("{e}")),
            all
        );
        let h = harness(false);
        let mut ok = submit_body("b64-all");
        ok["spec"]["input_b64"] = json!(encoded);
        let (status, v) = call(&h.router, "POST", "/jobs", Some(KEY), Some(ok)).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        for (i, bad) in ["aGk", "aGl=", "aG k=", "aGk==", "####", "aGk=\n"]
            .iter()
            .enumerate()
        {
            let mut body = submit_body(&format!("b64-bad-{i}"));
            body["spec"]["input_b64"] = json!(bad);
            let (status, v) = call(&h.router, "POST", "/jobs", Some(KEY), Some(body)).await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("bad_input")),
                "{bad:?}: {v}"
            );
            assert_eq!(
                v["error"], "input_b64 must be canonical standard base64 (RFC 4648, padded)",
                "{bad:?}"
            );
        }
    }

    /// Review V1: the header timeout does not run while a response write is
    /// pending, so a client that pipelines requests and never reads held
    /// its connection, and its slot, for ever. A write that makes no
    /// progress within the write-stall deadline closes the connection.
    #[tokio::test]
    async fn a_client_that_never_reads_does_not_hold_a_slot() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_connections: 1,
            header_timeout: Duration::from_secs(60),
            write_stall: Duration::from_millis(300),
            max_requests: usize::MAX, // the stall alone must free the slot
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let sock = tokio::net::TcpSocket::new_v4().unwrap_or_else(|e| panic!("{e}"));
        sock.set_recv_buffer_size(1024)
            .unwrap_or_else(|e| panic!("{e}"));
        let stalled = sock.connect(addr).await.unwrap_or_else(|e| panic!("{e}"));
        let (_reader, mut writer) = stalled.into_split(); // never read
        let flood = tokio::spawn(async move {
            let one = b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n";
            let many: Vec<u8> = one
                .iter()
                .copied()
                .cycle()
                .take(one.len() * 20_000)
                .collect();
            let _ = writer.write_all(&many).await; // blocks once the server stops reading
            writer
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut probe = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        probe
            .write_all(b"GET /health HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut reply))
            .await
            .unwrap_or_else(|_| panic!("a client that never reads held the only slot"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(
            reply.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&reply)
        );
        flood.abort();
    }

    /// A connection that keeps making progress is still closed at its
    /// lifetime (after the request in flight), so a trickling reader
    /// cannot keep a slot for ever either.
    #[tokio::test]
    async fn a_connection_is_closed_at_its_lifetime() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_lifetime: Duration::from_millis(500),
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut s = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut buf = [0u8; 4096];
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a busy connection outlived its lifetime"
            );
            if s.write_all(b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n")
                .await
                .is_err()
            {
                break; // closed
            }
            match tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => break, // closed
                Ok(Ok(_)) => {}
                Err(_) => panic!("no answer"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A client that pipelines many requests on one connection gets at most
    /// `max_requests` answers; the last says `Connection: close`.
    #[tokio::test]
    async fn requests_per_connection_are_capped() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let limits = HttpLimits {
            max_requests: 5,
            ..lifted_limits()
        };
        let (addr, _stop, _h) = serving(limits).await;
        let mut s = tokio::net::TcpStream::connect(addr)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let one = b"GET /health HTTP/1.1\r\nhost: x\r\n\r\n";
        let many: Vec<u8> = one.iter().copied().cycle().take(one.len() * 20).collect();
        s.write_all(&many).await.unwrap_or_else(|e| panic!("{e}"));
        let mut all = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut all))
            .await
            .unwrap_or_else(|_| panic!("the connection stayed open past its request cap"))
            .unwrap_or_else(|e| panic!("{e}"));
        let text = String::from_utf8_lossy(&all);
        assert_eq!(text.matches("HTTP/1.1 200").count(), 5, "{text}");
        assert!(
            text.to_ascii_lowercase().contains("connection: close"),
            "{text}"
        );
    }

    /// A full command channel is 503 `busy` (retry with the same op_key),
    /// never a queue that grows.
    #[test]
    fn full_channel_refusal_is_503_busy() {
        let e = ApiError::from(EngineError::Refused(
            durable_domain::Refusal::ReserveExhausted,
        ));
        assert_eq!(
            (e.status, e.code),
            (StatusCode::SERVICE_UNAVAILABLE, "busy")
        );
    }

    /// Checklist: the key is checked before the body is read. An
    /// unauthenticated submit whose 250 KiB body never ends is refused 401
    /// at once: the handler never waits on it.
    #[tokio::test]
    async fn an_unauthenticated_body_is_refused_unread() {
        let h = harness(false);
        let chunk = axum::body::Bytes::from(vec![b' '; 250 * 1024]);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("content-type", "application/json")
            .body(Body::new(Endless(Some(chunk))))
            .unwrap_or_else(|e| panic!("{e}"));
        let res = tokio::time::timeout(Duration::from_secs(5), h.router.clone().oneshot(req))
            .await
            .unwrap_or_else(|_| panic!("the server waited on an unauthenticated body"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    /// A request body that sends one chunk and then never ends.
    struct Endless(Option<axum::body::Bytes>);

    impl http_body::Body for Endless {
        type Data = axum::body::Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match self.0.take() {
                Some(b) => std::task::Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
                None => std::task::Poll::Pending,
            }
        }
    }

    #[test]
    fn capacity_refusal_is_503_at_capacity() {
        let e = ApiError::from(EngineError::Refused(durable_domain::Refusal::AtCapacity));
        assert_eq!(
            (e.status, e.code),
            (StatusCode::SERVICE_UNAVAILABLE, "at_capacity")
        );
    }

    /// A tenant over its share is told ITS quota is full — a distinct
    /// 429, never the retry-the-runner 503 that names --max-live-runs
    /// (S2 re-review finding 6).
    #[test]
    fn workspace_quota_refusal_is_429() {
        let e = ApiError::from(EngineError::Refused(
            durable_domain::Refusal::WorkspaceQuota,
        ));
        assert_eq!(
            (e.status, e.code),
            (StatusCode::TOO_MANY_REQUESTS, "workspace_quota")
        );
    }

    #[test]
    fn startup_sweeps_interrupted_backups() {
        let root = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        std::fs::create_dir(root.path().join(".b1.partial")).unwrap_or_else(|e| panic!("{e}"));
        std::fs::create_dir(root.path().join("b0")).unwrap_or_else(|e| panic!("{e}"));
        prepare_backup_root(root.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(!root.path().join(".b1.partial").exists());
        assert!(root.path().join("b0").exists());
    }

    /// Review finding 13: a real ENOSPC inside compaction or backup (after
    /// the reserve check) is an `Io` error, and was a 500.
    #[test]
    fn storage_full_io_error_is_503_no_space() {
        let e = ApiError::from(EngineError::Journal(durable_journal::JournalError::Io(
            std::io::Error::from(std::io::ErrorKind::StorageFull),
        )));
        assert_eq!(
            (e.status, e.code),
            (StatusCode::SERVICE_UNAVAILABLE, "no_space")
        );
    }

    /// A free-space probe that cannot answer (statvfs failing) refuses the
    /// submit as unavailable, not a 500.
    #[tokio::test]
    async fn failing_space_probe_is_503_not_500() {
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe_fail = fail.clone();
        let cfg = ShellConfig {
            tick_ms: 5,
            journal: durable_journal::JournalOptions {
                space: durable_journal::SpaceProbe::new(move |_| {
                    if probe_fail.load(std::sync::atomic::Ordering::SeqCst) {
                        Err(std::io::Error::other("statvfs failed"))
                    } else {
                        Ok(u64::MAX)
                    }
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = spawn(journal.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                false,
                None,
                journal.path(),
                true,
                false,
            ),
            None,
        );
        fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let (status, v) = call(&r, "POST", "/jobs", Some(KEY), Some(submit_body("p"))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    }

    /// Review (still open): with the journal's fsync stuck, every request
    /// hung, open reads included. Submit and list answer 503 `timeout`
    /// within the reply deadline instead.
    #[tokio::test]
    async fn a_stuck_journal_answers_503_timeout_within_the_deadline() {
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(durable_journal::faults::Faults::default());
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
        let cfg = ShellConfig {
            tick_ms: 5,
            reply_timeout: Duration::from_millis(300),
            journal: durable_journal::JournalOptions {
                faults: Some(faults.clone()),
                ..Default::default()
            },
            ..Default::default()
        };
        let engine = spawn(journal.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let r = router(
            test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                false,
                None,
                journal.path(),
                true,
                false,
            ),
            None,
        );
        faults.hold_syncs();
        let within = |f| tokio::time::timeout(Duration::from_secs(5), f);
        let (status, v) = within(call(&r, "POST", "/jobs", Some(KEY), Some(submit_body("s"))))
            .await
            .unwrap_or_else(|_| panic!("submit hung"));
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("timeout"))
        );
        let (status, v) = within(call(&r, "GET", "/jobs", Some(KEY), None))
            .await
            .unwrap_or_else(|_| panic!("list hung"));
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("timeout"))
        );
        faults.release_syncs();
    }

    /// Auth schemes are case-insensitive (RFC 9110 §11.1), and the token
    /// may be surrounded by extra whitespace.
    #[tokio::test]
    async fn bearer_scheme_is_case_insensitive() {
        let h = harness(false);
        for (i, header) in [format!("bearer {KEY}"), format!("BEARER   {KEY} ")]
            .into_iter()
            .enumerate()
        {
            let req = axum::http::Request::builder()
                .method("POST")
                .uri("/jobs")
                .header("authorization", header)
                .header("content-type", "application/json")
                .body(Body::from(submit_body(&format!("case-{i}")).to_string()))
                .unwrap_or_else(|e| panic!("{e}"));
            let res = h
                .router
                .clone()
                .oneshot(req)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(res.status(), StatusCode::OK, "header variant {i}");
        }
    }

    /// Bad query strings and bodies answer in the one error envelope, not
    /// axum's plain-text rejections.
    #[tokio::test]
    async fn rejections_use_the_json_envelope() {
        let h = harness(false);
        let (status, v) = call(&h.router, "GET", "/jobs?limit=-1", Some(KEY), None).await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("bad_query"))
        );
        let (status, v) = call(&h.router, "GET", "/jobs?after=abc", Some(KEY), None).await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("bad_query"))
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/jobs")
            .header("authorization", format!("Bearer {KEY}"))
            .header("content-type", "application/json")
            .body(Body::from("{not json"))
            .unwrap_or_else(|e| panic!("{e}"));
        let res = h
            .router
            .clone()
            .oneshot(req)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let status = res.status();
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("bad_body"))
        );
    }

    #[tokio::test]
    async fn over_long_key_is_refused_with_a_stable_code() {
        let h = harness(false);
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body(&"k".repeat(300))),
        )
        .await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::CONFLICT, json!("over_limits"))
        );
    }

    #[test]
    fn server_index_bound_defaults_to_the_sized_domain_default() {
        let args = Args::try_parse_from(["durable-server"]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            args.max_index_bytes,
            durable_domain::Limits::default().max_index_bytes
        );
        assert_eq!(args.max_index_bytes, 512 * 1024 * 1024);
    }

    /// S2 re-review N3: the shipped `--max-in-flight-per-ws` default is
    /// the engine's own (the fifth-share that leaves a fifth tenant
    /// headroom), and a compat install — no account store, ws 0 only —
    /// caps at the whole pool like v1.0.x did. Asserted on the parsed
    /// CLI, not `ShellConfig::default()`: the shell's default is what
    /// the CLI overrides, so only the parse proves what ships.
    #[test]
    fn the_shipped_in_flight_share_matches_the_engine_default() {
        let args = Args::try_parse_from(["durable-server"]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            args.max_in_flight_per_ws,
            ShellConfig::default().max_in_flight_per_ws
        );
        assert!(
            args.max_in_flight_per_ws * 4 < args.max_in_flight,
            "four maxed tenants must not tile the pool: {} × 4 vs {}",
            args.max_in_flight_per_ws,
            args.max_in_flight
        );
        // Compat (ws-0 only) gets the pool, like v1.0.x; a tenant
        // deployment keeps the configured share.
        assert_eq!(ws_in_flight_cap(&args, true), args.max_in_flight);
        assert_eq!(ws_in_flight_cap(&args, false), args.max_in_flight_per_ws);
    }

    #[test]
    fn api_key_debug_is_redacted() {
        let k = ApiKey::new(KEY);
        assert!(!format!("{k:?}").contains("test-key"));
        assert!(k.verify(KEY));
        assert!(!k.verify(""));
    }

    // ---- S2-6: signed handler calls -----------------------------------

    /// The handler-side check `docs/HANDLERS.md` prescribes, kept in
    /// code so the contract can never drift from what the server
    /// produces: a fresh timestamp (±300 s), the signature recomputed
    /// over the verbatim wire fields compared in constant time, and a
    /// signature seen at most once. The rejection reason is
    /// inspectable, never 200-with-a-flag.
    ///
    /// `expected_authority` is the authority the handler is REGISTERED
    /// under — the verifier's own configuration, never the request's
    /// `Host` header: that header is the replayer's to choose, so
    /// checking it lets a call minted for `a/step` replay at `b/step`
    /// (S2 re-review #4). The runner signs the registered URL's
    /// authority; a signature carrying any other value can never match
    /// a verifier configured for this one.
    #[allow(clippy::too_many_arguments)]
    fn verify_sig(
        secret: &[u8; 32],
        now: u64,
        expected_authority: &str,
        headers: &axum::http::HeaderMap,
        method: &str,
        path: &str,
        idem_key: &str,
        body_digest: &str,
        seen: &mut HashSet<String>,
    ) -> Result<(), &'static str> {
        let get = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
        let ts: u64 = get(TS_HEADER)
            .and_then(|v| v.parse().ok())
            .ok_or("missing timestamp")?;
        if now.abs_diff(ts) > 300 {
            return Err("stale timestamp");
        }
        let sig = get(SIG_HEADER).ok_or("missing signature")?;
        // step/attempt are integers or `-` for the compat path — anything
        // else is a field-smuggling attempt, refused outright. The run id
        // is the opaque public id, bound into the signature verbatim
        // (HTTP forbids the control bytes a re-split would need anyway).
        let num = |n: &str| match get(n) {
            None | Some("-") => Ok("-".to_string()),
            Some(v) if v.bytes().all(|b| b.is_ascii_digit()) => Ok(v.to_string()),
            Some(_) => Err("malformed call header"),
        };
        let (s, a) = (num(STEP_HEADER)?, num(ATTEMPT_HEADER)?);
        let r = match get(RUN_ID_HEADER) {
            None | Some("-") => "-".to_string(),
            Some(v) => v.to_string(),
        };
        let expected = {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
            let canonical = format!(
                "{ts}\n{method}\n{expected_authority}\n{path}\n{r}\n{s}\n{a}\n{idem_key}\n{body_digest}"
            );
            ring::hmac::sign(&key, canonical.as_bytes())
        };
        // Constant-time on the decoded tag: hex length leaks nothing
        // (a wrong-length sig is simply not a tag).
        use subtle::ConstantTimeEq as _;
        let presented: Option<[u8; 32]> = unhex(sig);
        let presented = presented.ok_or("signature mismatch")?;
        if !bool::from(presented.ct_eq(expected.as_ref())) {
            return Err("signature mismatch");
        }
        // Replay is keyed on the signature itself: a re-split or
        // re-minted variant is a new key, but an identical wire sig
        // never runs the effect twice.
        if !seen.insert(sig.to_string()) {
            return Err("replay");
        }
        Ok(())
    }

    /// A verifying handler: the testbed `docs/HANDLERS.md` describes —
    /// every POST is signature-checked before the body is believed, and
    /// the probe endpoint does the same with the query-carried key.
    struct Verifier {
        secret: std::sync::RwLock<[u8; 32]>,
        /// The authority this handler is registered under — its own
        /// configuration, verified instead of the request's `Host`.
        authority: String,
        seen: Mutex<HashSet<String>>,
        rejects: Mutex<Vec<String>>,
        posts: std::sync::atomic::AtomicUsize,
    }

    impl Verifier {
        fn reject(&self, reason: &'static str) -> (StatusCode, String) {
            self.rejects
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(reason.to_string());
            (
                StatusCode::UNAUTHORIZED,
                format!("{{\"reason\":\"{reason}\"}}"),
            )
        }
    }

    async fn verified_post(
        axum::extract::State(st): axum::extract::State<Arc<Verifier>>,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> (StatusCode, String) {
        st.posts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // The idem key is taken from the *body* — what the effect will
        // dedupe on — and the header must equal it. A header/body split
        // is a smuggling attempt, refused before the signature check.
        let header_idem = headers
            .get("x-idempotency-key")
            .and_then(|v| v.to_str().ok());
        let body_idem = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v["idem_key"].as_str().map(str::to_string));
        let (Some(h_idem), Some(idem)) = (header_idem, body_idem) else {
            return st.reject("missing idempotency key");
        };
        if h_idem != idem {
            return st.reject("idempotency key mismatch");
        }
        let digest = hex(&sha256(&body));
        let secret = *st.secret.read().unwrap_or_else(|e| e.into_inner());
        let mut seen = st.seen.lock().unwrap_or_else(|e| e.into_inner());
        match verify_sig(
            &secret,
            unix_secs(),
            &st.authority,
            &headers,
            "POST",
            "/step",
            &idem,
            &digest,
            &mut seen,
        ) {
            Ok(()) => (StatusCode::OK, "{}".to_string()),
            Err(r) => st.reject(r),
        }
    }

    async fn verified_probe(
        axum::extract::State(st): axum::extract::State<Arc<Verifier>>,
        axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
        headers: axum::http::HeaderMap,
    ) -> StatusCode {
        let Some(idem) = q.get("idem_key") else {
            return StatusCode::UNAUTHORIZED;
        };
        let secret = *st.secret.read().unwrap_or_else(|e| e.into_inner());
        let mut seen = st.seen.lock().unwrap_or_else(|e| e.into_inner());
        // A probe's signed body digest is empty — GET bound to the path.
        match verify_sig(
            &secret,
            unix_secs(),
            &st.authority,
            &headers,
            "GET",
            "/step",
            idem,
            "",
            &mut seen,
        ) {
            Ok(()) => StatusCode::OK,
            Err(r) => st.reject(r).0,
        }
    }

    async fn spawn_verifier(secret: [u8; 32]) -> (String, Arc<Verifier>) {
        // Bound first: the verifier's configured authority is the address
        // it is registered under — never a header the caller controls.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let st = Arc::new(Verifier {
            secret: std::sync::RwLock::new(secret),
            authority: addr.to_string(),
            seen: Mutex::new(HashSet::new()),
            rejects: Mutex::new(Vec::new()),
            posts: std::sync::atomic::AtomicUsize::new(0),
        });
        let app = Router::new()
            .route(
                "/step",
                axum::routing::post(verified_post).get(verified_probe),
            )
            .with_state(st.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), st)
    }

    /// The signed wire format, end to end: `PUT /handlers` arms the
    /// engine with the workspace's secret, a submitted run dispatches
    /// through it, and the verifying handler accepts — recompute,
    /// freshness and the idem-replay bound all pass on the real path.
    #[tokio::test]
    async fn a_signed_call_reaches_a_verifying_handler() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_aafaafaafaafaafaafaafaafaafaafaafaafaafaafaafaaf";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (url, verifier) = spawn_verifier(a.handler_secret).await;
        let h = harness_store(true, None, Some(store));

        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/step",
            Some(key_a),
            Some(json!({ "url": format!("{url}/step") })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");

        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(key_a),
            Some(submit_body("sig-ok")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let id = v["run_id"].as_str().unwrap_or_default().to_string();

        // The run finishes only if the verifier accepted a call — poll
        // the scoped read for `done` within a bounded window.
        let mut state = String::new();
        for _ in 0..400 {
            let (_, v) = call(&h.router, "GET", &format!("/jobs/{id}"), Some(key_a), None).await;
            state = v["state"].as_str().unwrap_or("").to_string();
            if state == "done" || state == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(state, "done", "run must complete through a valid signature");
        assert_eq!(
            verifier.posts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the verifying handler saw exactly one call"
        );
        assert!(
            verifier
                .rejects
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty(),
            "the signed wire format failed verification"
        );
    }

    /// The reference check itself: each rejection class the contract
    /// names — stale timestamp, tampered body, wrong secret, replay —
    /// is refused for its own reason, in order.
    #[test]
    fn the_reference_verifier_rejects_each_tamper_class() {
        let secret = [7u8; 32];
        let now = 1_700_000_000u64;
        let idem = "k/0/1";
        let body = b"{\"input\":\"aGk=\",\"idem_key\":\"k/0/1\"}";
        let digest = hex(&sha256(body));
        let mut headers = axum::http::HeaderMap::new();
        let mut seen: HashSet<String> = HashSet::new();
        let sign_for = |secret: &[u8; 32], ts: u64, body: &[u8]| {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
            hex(ring::hmac::sign(
                &key,
                format!(
                    "{ts}\nPOST\nh.test:8080\n/step\n5\n0\n1\n{idem}\n{}",
                    hex(&sha256(body))
                )
                .as_bytes(),
            )
            .as_ref())
        };
        headers.insert(
            "host",
            "h.test:8080".parse().unwrap_or_else(|e| panic!("{e}")),
        );
        headers.insert(
            TS_HEADER,
            now.to_string().parse().unwrap_or_else(|e| panic!("{e}")),
        );
        headers.insert(
            SIG_HEADER,
            sign_for(&secret, now, body)
                .parse()
                .unwrap_or_else(|e| panic!("{e}")),
        );
        headers.insert(RUN_ID_HEADER, "5".parse().unwrap_or_else(|e| panic!("{e}")));
        headers.insert(STEP_HEADER, "0".parse().unwrap_or_else(|e| panic!("{e}")));
        headers.insert(
            ATTEMPT_HEADER,
            "1".parse().unwrap_or_else(|e| panic!("{e}")),
        );

        // The verifier is configured for the authority it is registered
        // under — the value the runner signed.
        let home = "h.test:8080";
        // A well-formed call passes…
        assert!(verify_sig(
            &secret, now, home, &headers, "POST", "/step", idem, &digest, &mut seen
        )
        .is_ok());
        // …but its replay does not — the signature is spent.
        assert_eq!(
            verify_sig(&secret, now, home, &headers, "POST", "/step", idem, &digest, &mut seen),
            Err("replay")
        );
        // The same sig under a different path or method does not replay.
        assert_eq!(
            verify_sig(&secret, now, home, &headers, "GET", "/step", "other", "", &mut seen),
            Err("signature mismatch")
        );
        // Nor at a different destination — S3's cross-handler replay.
        // The captured request is replayed to another verifier with the
        // ORIGINAL Host header intact: the replayer can send `Host:
        // h.test:8080` to any target, so only the configured authority —
        // never the received header — can bind the signature (S2
        // re-review #4).
        assert_eq!(
            verify_sig(
                &secret,
                now,
                "other.test:9",
                &headers,
                "POST",
                "/step",
                idem,
                &digest,
                &mut seen
            ),
            Err("signature mismatch")
        );
        // The same replay carrying a forged `Host: other.test:9` header
        // — the destination's real authority — is refused identically:
        // no value the header carries is consulted.
        let mut forged = headers.clone();
        forged.insert(
            "host",
            "other.test:9".parse().unwrap_or_else(|e| panic!("{e}")),
        );
        assert_eq!(
            verify_sig(
                &secret,
                now,
                "other.test:9",
                &forged,
                "POST",
                "/step",
                idem,
                &digest,
                &mut seen
            ),
            Err("signature mismatch")
        );
        // A stale timestamp refuses before the signature is judged.
        let mut stale = headers.clone();
        stale.insert(
            TS_HEADER,
            (now - 301)
                .to_string()
                .parse()
                .unwrap_or_else(|e| panic!("{e}")),
        );
        assert_eq!(
            verify_sig(&secret, now, home, &stale, "POST", "/step", "other", &digest, &mut seen),
            Err("stale timestamp")
        );
        // A tampered body breaks the digest the signature binds.
        assert_eq!(
            verify_sig(
                &secret,
                now,
                home,
                &headers,
                "POST",
                "/step",
                "other",
                &hex(&sha256(b"{\"input\":\"eW8=\"}")),
                &mut seen
            ),
            Err("signature mismatch")
        );
        // A signature under another workspace's secret refuses.
        let mut wrong = headers.clone();
        wrong.insert(
            SIG_HEADER,
            sign_for(&[9u8; 32], now, body)
                .parse()
                .unwrap_or_else(|e| panic!("{e}")),
        );
        assert_eq!(
            verify_sig(&secret, now, home, &wrong, "POST", "/step", "other", &digest, &mut seen),
            Err("signature mismatch")
        );
        // Field smuggling: a non-integer attempt header refuses outright.
        let mut smuggled = headers.clone();
        smuggled.insert(
            ATTEMPT_HEADER,
            "1.x".parse().unwrap_or_else(|e| panic!("{e}")),
        );
        assert_eq!(
            verify_sig(&secret, now, home, &smuggled, "POST", "/step", "other", &digest, &mut seen),
            Err("malformed call header")
        );
    }

    /// Rotation: `POST /handler-secret` returns the new secret once,
    /// the engine's set is resynced to it immediately, and calls signed
    /// under the retired secret fail honestly at the handler.
    #[tokio::test]
    async fn rotation_retires_the_old_secret_immediately() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_aadaadaadaadaadaadaadaadaadaadaadaadaadaadaadaad";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (url, verifier) = spawn_verifier(a.handler_secret).await;
        let h = harness_store(true, None, Some(store.clone()));
        let (status, v) = call(
            &h.router,
            "PUT",
            "/handlers/step",
            Some(key_a),
            Some(json!({ "url": format!("{url}/step") })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");

        // Rotate (D-073: session admin on ws `a`): the new secret comes
        // back once, and differs.
        let (cookie, csrf, pid) = session_admin_on(&h.router, &store, &a, "adm@x.co").await;
        let (status, _, v) = raw(
            &h.router,
            "POST",
            "/handler-secret",
            &ws_headers(&cookie, &csrf, &pid),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let new_secret = unhex(v["secret"].as_str().unwrap_or(""))
            .unwrap_or_else(|| panic!("rotation must return the new secret once: {v}"));
        assert_ne!(new_secret, a.handler_secret);

        // The engine now signs with the new secret: a call it produces
        // fails at a handler still holding the retired one — honestly,
        // as a definitive failed attempt, not a hang.
        let signed = h.app.handler_factory.make_signed(
            &format!("{url}/step"),
            store
                .workspace_by_id(a.id)
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .map(|w| Arc::new(w.handler_secret)),
        );
        let outcome = signed
            .execute(
                &StepSpec {
                    name: "s".into(),
                    handler: "step".into(),
                },
                b"hi",
                "k/rot",
            )
            .await;
        assert!(
            matches!(outcome, Outcome::Failed),
            "a call under the retired secret must fail: {outcome:?}"
        );
        assert_eq!(
            verifier
                .rejects
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_slice(),
            &["signature mismatch"]
        );

        // The handler learns the new secret; the engine's resynced set
        // (built by the rotate route) now verifies.
        *verifier.secret.write().unwrap_or_else(|e| e.into_inner()) = new_secret;
        let outcome = signed
            .execute(
                &StepSpec {
                    name: "s".into(),
                    handler: "step".into(),
                },
                b"hi",
                "k/rot2",
            )
            .await;
        assert!(
            matches!(outcome, Outcome::Checkpoint(_)),
            "the rotated secret must verify: {outcome:?}"
        );
    }

    /// S2-7's drain investigation, at the level the load run observed it:
    /// the same engine, same caps, same submit burst — once against a
    /// fast axum fixture and once against the single-threaded node
    /// fixture `perf/load.mjs` spawns. Whatever differs is the bound.
    /// `cargo test -p durable-server drain_rate -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "measurement, not a regression test"]
    async fn drain_rate_against_http_fixtures() {
        async fn axum_echo() -> String {
            async fn ok() -> &'static str {
                "{}"
            }
            let app = Router::new().route("/step", axum::routing::post(ok).get(ok));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
            format!("http://{addr}")
        }
        // The exact fixture load.mjs spawns — one node process, one
        // thread, JSON parse + reply per request. The URL comes from
        // `DRAIN_NODE_URL` (spawn it outside: nothing in src/ may start a
        // process, arch gate) — unset, the node comparisons are skipped.
        //   node -e "require('node:http').createServer((q,r)=>{let b='';\
        //     q.on('data',c=>b+=c);q.on('end',()=>{r.writeHead(200,\
        //     {'content-type':'application/json'});r.end('{}')})})\
        //     .listen(63999,'127.0.0.1')" &
        let node_url = std::env::var("DRAIN_NODE_URL").ok();
        async fn measure(url: &str, n: usize, in_flight: usize, per_tick: usize) -> f64 {
            let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let engine = spawn(
                journal.path(),
                Registry::default(),
                ShellConfig {
                    tick_ms: 50,
                    max_in_flight: in_flight,
                    max_dispatch_per_tick: per_tick,
                    ..Default::default()
                },
            )
            .unwrap_or_else(|e| panic!("{e}"));
            let h = http_handler(url.to_string());
            engine
                .put_handler(0, "step", Arc::new(h))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            // Concurrent submits — submit latency must not hide drain.
            // Bounded well under cmd_capacity (1024): more in flight is
            // refused ReserveExhausted, not queued.
            let gate = Arc::new(tokio::sync::Semaphore::new(256));
            let mut pending = Vec::with_capacity(n);
            for i in 0..n {
                let eng = engine.clone();
                let g = gate.clone();
                pending.push(tokio::spawn(async move {
                    let _permit = g.acquire().await;
                    eng.submit(
                        OperationKey(format!("dr-{i}")),
                        JobSpec {
                            version: 1,
                            input: b"x".to_vec(),
                            steps: vec![StepSpec {
                                name: "s".into(),
                                handler: "step".into(),
                            }],
                        },
                    )
                    .await
                }));
            }
            for p in pending {
                p.await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .unwrap_or_else(|e| panic!("submit: {e}"));
            }
            let start = std::time::Instant::now();
            let deadline = start + Duration::from_secs(240);
            loop {
                let d = engine.diag().await.unwrap_or_else(|e| panic!("{e}"));
                let done = d
                    .states
                    .iter()
                    .filter(|(k, _)| matches!(**k, "done" | "failed" | "cancelled"))
                    .map(|(_, c)| *c as usize)
                    .sum::<usize>();
                if done >= n {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "stalled at {done}/{n}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            let secs = start.elapsed().as_secs_f64();
            eprintln!(
                "{url} in_flight={in_flight} per_tick={per_tick}: \
                 {n} done in {secs:.1}s = {:.0} runs/s",
                n as f64 / secs
            );
            n as f64 / secs
        }
        // Sequential calls — no concurrency, so this is the fixture's
        // own round-trip latency (what one permit-slot actually pays).
        async fn latency(url: &str, n: usize) -> f64 {
            let h = http_handler(url.to_string());
            let spec = StepSpec {
                name: "s".into(),
                handler: "step".into(),
            };
            h.execute(&spec, b"w", "warm").await;
            let start = std::time::Instant::now();
            for i in 0..n {
                h.execute(&spec, b"x", &format!("seq-{i}")).await;
            }
            let us = start.elapsed().as_micros() as f64 / n as f64 / 1000.0;
            eprintln!("{url}: {us:.2} ms/call sequential (n={n})");
            us
        }
        let url_fast = axum_echo().await;
        latency(&url_fast, 200).await;
        let fast = measure(&url_fast, 2000, 32, 64).await;
        // axum at the raised caps — an in-process fixture shares this
        // single-threaded test runtime with engine+client, so it
        // understates; the node fixture (own process) is the fair one.
        let axum_fast = measure(&url_fast, 2000, 256, 512).await;
        eprintln!("summary: axum32={fast:.0}/s axum256={axum_fast:.0}/s");
        if let Some(url_node) = node_url {
            latency(&url_node, 200).await;
            let slow = measure(&url_node, 2000, 32, 64).await;
            // If the bound is in-flight call latency × permits, raising
            // the permit count scales the rate; the writer/tick, it won't.
            let more = measure(&url_node, 2000, 256, 512).await;
            // in_flight alone, per_tick at its default — is the permit
            // the whole story?
            let alone = measure(&url_node, 2000, 256, 64).await;
            eprintln!(
                "node: 32/64={slow:.0}/s 256/512={more:.0}/s \
                 in_flight256-only={alone:.0}/s"
            );
        } else {
            eprintln!("node comparisons skipped: DRAIN_NODE_URL unset");
        }
    }

    /// Signing is per-call overhead the dispatch path pays: measure the
    /// HMAC itself so the cost is evidence, not a feeling. A wall-clock
    /// bound inside the unit suite is flaky by construction under
    /// parallel load, so it runs as a measurement probe:
    /// `cargo test -p durable-server signing_overhead -- --ignored`.
    #[test]
    #[ignore = "measurement, not a regression test"]
    fn signing_overhead_is_trivial_per_call() {
        let secret = [3u8; 32];
        let call = Call {
            run: RunId(1),
            step: durable_domain::StepIndex(0),
            attempt: 1,
        };
        let digest = hex(&sha256(b"{}"));
        let start = Instant::now();
        for i in 0..10_000u64 {
            let sig = sign_call(
                &secret,
                1_700_000_000 + i,
                "POST",
                "127.0.0.1:9",
                "/step",
                &call.run.0.to_string(),
                &call.step.0.to_string(),
                &call.attempt.to_string(),
                "k",
                &digest,
            );
            std::hint::black_box(sig);
        }
        let mean = start.elapsed().as_nanos() as f64 / 10_000.0;
        assert!(
            mean < 10_000.0,
            "HMAC signing must stay microseconds, not {mean:.0} ns/call"
        );
    }

    /// S1 #1: a tenant's admin key is admin *of its workspace* — never
    /// of the host. The global ops routes refuse it: diagnostics (every
    /// tenant's totals), compact (engine-wide maintenance), backup
    /// (whole-journal copies). Its own workspace's counts come from
    /// GET /diagnostics — scoped, never another tenant's.
    #[tokio::test]
    async fn a_tenant_admin_key_never_reaches_the_ops_routes() {
        use durable_accounts::Memory;
        let store = Arc::new(Memory::new());
        let ws = store
            .create_workspace("tenant")
            .await
            .unwrap_or_else(|e| panic!("workspace: {e}"));
        let token = "dr_abdabdabdabdabdabdabdabdabdabdabdabdabdabdabdabd";
        store
            .create_key(
                ws.id,
                "main",
                Permission::Write,
                &sha256(token.as_bytes()),
                "dr_te",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let h = harness_store(true, None, Some(store.clone()));
        register_step(&store, ws.id, &h.app.engine).await;
        // Give the tenant a run so its scoped diagnostics have a shape.
        let (status, v) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(token),
            Some(submit_body("tenant-job")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        for (method, uri, body) in [
            ("GET", "/admin/diagnostics", None),
            ("POST", "/admin/compact", None),
            ("POST", "/admin/backup", Some(json!({ "name": "b" }))),
        ] {
            let (status, v) = call(&h.router, method, uri, Some(token), body).await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::FORBIDDEN, json!("forbidden")),
                "{method} {uri} must refuse a tenant key"
            );
        }
        // The scoped route answers the tenant's own shape only.
        let (status, v) = call(&h.router, "GET", "/diagnostics", Some(token), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        // DECISIVE(diag-scope)
        assert_eq!(v["live_runs"], json!(1), "{v}");
        assert!(
            v.get("workspaces_active").is_none() && v.get("records_written").is_none(),
            "tenant diagnostics must not carry global fields: {v}"
        );
        // …and the ops key keeps the global view.
        let (status, v) = call(&h.router, "GET", "/admin/diagnostics", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        assert!(v["records_written"].is_u64(), "{v}");
    }

    /// Never-panics fuzzing for every outside-input decoder this binary
    /// owns: public ids and route ids, request JSON bodies + the input
    /// base64 and admission rules behind them, signed-call headers,
    /// `--handler`/`--trusted-proxies` strings, and `handlers.json`.
    /// Seed corpora live under `fuzz/corpus/<target>/`; a panic is a
    /// red-first bug.
    mod fuzz {
        use super::*;
        use proptest::prelude::*;

        fn corpus(name: &str) -> Vec<Vec<u8>> {
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../fuzz/corpus")
                .join(name);
            let mut out: Vec<Vec<u8>> = std::fs::read_dir(&dir)
                .unwrap_or_else(|e| panic!("corpus {dir:?}: {e}"))
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
                .filter_map(|e| std::fs::read(e.path()).ok())
                .collect();
            out.sort();
            out
        }

        fn corpus_str(name: &str) -> Vec<String> {
            corpus(name)
                .into_iter()
                .filter_map(|b| String::from_utf8(b).ok())
                .collect()
        }

        fn pubid_decode(s: &str) {
            let p = pubid::PubId::ephemeral();
            let _ = p.decode(s);
        }

        #[test]
        fn fuzz_pubid_seed_corpus() {
            for s in corpus_str("pubid") {
                pubid_decode(&s);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_pubid_never_panics(s in any::<String>()) {
                pubid_decode(&s);
            }
        }

        /// `decode_id` on the real App: opaque-first for every workspace,
        /// the numeric fallback only at ws 0.
        fn decode_id_at(app: &App, s: &str) {
            for ws in [0u64, 1, u64::MAX] {
                let _ = decode_id(app, ws, s);
            }
        }

        #[tokio::test]
        async fn fuzz_decode_id_seed_corpus_and_prop() {
            let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let mut reg = Registry::default();
            reg.register("step", Arc::new(Echo));
            let engine = spawn(journal.path(), reg, ShellConfig::default())
                .unwrap_or_else(|e| panic!("{e}"));
            let app = test_app(
                engine,
                Arc::new(Keyring::new(ApiKey::new(KEY), None, None)),
                false,
                None,
                journal.path(),
                true,
                false,
            );
            for s in corpus_str("decode-id") {
                decode_id_at(&app, &s);
            }
            let mut runner = proptest::test_runner::TestRunner::default();
            runner
                .run(&any::<String>(), |s| {
                    decode_id_at(&app, &s);
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("{e:?}"));
        }

        /// The signed-call verifier over arbitrary header bytes (the
        /// values HTTP admits): ts parse, host, digit fields, hex sig.
        fn sig_headers(parts: &[&str]) {
            let secret = [7u8; 32];
            let mut seen = HashSet::new();
            let mut h = axum::http::HeaderMap::new();
            for (name, v) in [
                TS_HEADER,
                SIG_HEADER,
                RUN_ID_HEADER,
                STEP_HEADER,
                ATTEMPT_HEADER,
            ]
            .into_iter()
            .zip(parts.iter().cycle())
            {
                if let Ok(v) = axum::http::HeaderValue::from_str(v) {
                    h.insert(name, v);
                }
            }
            let _ = verify_sig(
                &secret,
                1_700_000_000_000,
                parts.get(2).copied().unwrap_or(""),
                &h,
                "POST",
                "/step",
                "i",
                "d",
                &mut seen,
            );
        }

        #[test]
        fn fuzz_sig_headers_seed_corpus() {
            for b in corpus("sig-headers") {
                let text = String::from_utf8_lossy(&b);
                let mut lines = text.lines();
                let ts = lines.next().unwrap_or("");
                let sig = lines.next().unwrap_or("");
                sig_headers(&[ts, sig]);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_sig_headers_never_panics(
                ts in any::<String>(),
                sig in any::<String>(),
                host in any::<String>(),
                run in any::<String>(),
                step in any::<String>(),
                attempt in any::<String>(),
            ) {
                sig_headers(&[&ts, &sig, &host, &run, &step, &attempt]);
            }

            #[test]
            fn fuzz_unhex_never_panics(s in any::<String>()) {
                let _ = unhex(&s);
            }
        }

        /// The submit body chain: JSON → base64 → JobSpec → admission.
        fn submit_body_bytes(bytes: &[u8]) {
            if let Ok(req) = serde_json::from_slice::<SubmitReq>(bytes) {
                if let Ok(input) = base64_decode(&req.spec.input_b64) {
                    let spec = JobSpec {
                        version: req.spec.version,
                        steps: req.spec.steps,
                        input,
                    };
                    let l = durable_domain::Limits::default();
                    let _ = l.admits(&spec);
                    let _ = l.admits_key(&OperationKey(req.op_key));
                }
            }
            if let Ok(req) = serde_json::from_slice::<PutHandlerReq>(bytes) {
                let _ = reqwest::Url::parse(&req.url);
            }
        }

        #[test]
        fn fuzz_bodies_seed_corpus() {
            for b in corpus("http-bodies") {
                submit_body_bytes(&b);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_bodies_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..=4096)) {
                submit_body_bytes(&bytes);
            }
        }

        #[test]
        fn fuzz_handler_parse_seed_corpus() {
            for s in corpus_str("handler-parse") {
                let _ = parse_handler(&s);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_handler_parse_never_panics(s in any::<String>()) {
                let _ = parse_handler(&s);
            }
        }

        #[test]
        fn fuzz_ipnet_seed_corpus() {
            for s in corpus_str("ipnet") {
                let _ = addr_limits::IpNet::parse(&s);
                let _ = parse_v6_prefix(&s);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_ipnet_never_panics(s in any::<String>()) {
                let _ = addr_limits::IpNet::parse(&s);
                let _ = parse_v6_prefix(&s);
            }
        }

        fn registry_file_bytes(bytes: &[u8]) {
            let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let p = d.path().join("handlers.json");
            std::fs::write(&p, bytes).unwrap_or_else(|e| panic!("{e}"));
            let _ = load_registry_file(&p); // returns Result now — ignored deliberately
        }

        #[test]
        fn fuzz_registry_json_seed_corpus() {
            for b in corpus("registry-json") {
                registry_file_bytes(&b);
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::default())]
            #[test]
            fn fuzz_registry_json_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..=8192)) {
                registry_file_bytes(&bytes);
            }
        }
    }

    // ---- S3-0: session + auth surface ------------------------------
    use durable_accounts::Memory;

    /// `call` plus arbitrary headers and the full response (cookie and
    /// cache-header assertions need more than status+body).
    async fn raw(
        router: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body: Option<Value>,
    ) -> (StatusCode, header::HeaderMap, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, v.as_str());
        }
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap_or_else(|e| panic!("{e}"));
        let res = router
            .clone()
            .oneshot(req)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let status = res.status();
        let heads = res.headers().clone();
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        (
            status,
            heads,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// `raw` carrying the `OpsListener` extension — what a request on
    /// the `--ops-listen` socket looks like to the router.
    async fn raw_ops(
        router: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body: Option<Value>,
    ) -> (StatusCode, header::HeaderMap, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, v.as_str());
        }
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap_or_else(|e| panic!("{e}"));
        let res = router
            .clone()
            .oneshot({
                let mut req = req;
                req.extensions_mut().insert(OpsListener);
                req
            })
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let status = res.status();
        let heads = res.headers().clone();
        let bytes = res
            .into_body()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .to_bytes();
        (
            status,
            heads,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn cookie_header(set_cookie: &str) -> String {
        set_cookie.split(';').next().unwrap_or_default().to_string()
    }

    /// Sign up then sign in; returns `(cookie_value, csrf)` for a live
    /// session. The password must satisfy the policy.
    async fn sign_in(router: &Router, email: &str, password: &str) -> (String, String) {
        let (st, _, _) = raw(
            router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": email, "password": password})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED, "signup");
        let (st, heads, v) = raw(
            router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": email, "password": password})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "login: {v}");
        let set = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        (
            cookie_header(&set),
            v["csrf"].as_str().unwrap_or_default().to_string(),
        )
    }

    /// The whole S3-0 round trip: sign-up → 202 + unverified account +
    /// a queued verify mail; login → cookie + csrf; the cookie
    /// resolves `GET /auth/session`; logout revokes.
    #[tokio::test]
    async fn s30_signup_login_session_round_trip() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));

        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": "User@Example.com", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        assert_eq!(v, json!({"status": "verification_sent"}));
        let acct = store
            .account_by_email("user@example.com")
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("account row"));
        assert!(!acct.email_verified, "unverified until the link is used");
        assert_eq!(
            store
                .outbox()
                .iter()
                .filter(|r| r.mail.kind == "verify")
                .count(),
            1,
            "one verify mail queued"
        );

        // The same address again: identical answer, identical work —
        // every branch pays the hash (review S3).
        let ran_before = h.app.argon.ran.load(std::sync::atomic::Ordering::Relaxed);
        let (st, _, v2) = raw(
            &h.router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": "user@example.com", "password": "another-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        assert_eq!(v2, v, "identical 202 envelope, no oracle");
        let ran_after = h.app.argon.ran.load(std::sync::atomic::Ordering::Relaxed);
        assert!(ran_after > ran_before, "the re-sign-up still paid the hash");

        let (cookie, csrf) =
            sign_in(&h.router, "user@example.com", "another-long-passphrase").await;
        let (st, heads, v) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["account"]["email"], "user@example.com");
        assert!(v["csrf"].is_string());
        assert_eq!(
            heads
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store"),
            "session responses are never cached"
        );
        assert!(
            heads
                .get_all(header::VARY)
                .iter()
                .any(|v| v.to_str().is_ok_and(|s| s.contains("Cookie"))),
            "Vary: Cookie"
        );
        // Logout without the synchroniser token → 403 csrf_failed…
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/logout",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "csrf_failed");
        // …and with a wrong one → same; with the right one → 204.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/logout",
            &[
                ("cookie", cookie.clone()),
                ("x-durable-csrf", "drc_bad".into()),
            ],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        let (st, heads, _) = raw(
            &h.router,
            "POST",
            "/auth/logout",
            &[("cookie", cookie.clone()), ("x-durable-csrf", csrf)],
            None,
        )
        .await;
        // DECISIVE(csrf)
        assert_eq!(st, StatusCode::NO_CONTENT);
        let expired = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(expired.contains("Max-Age=0"), "the cookie is cleared");
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "the session is dead");
    }

    /// Cookie attributes are asserted exactly: HttpOnly, Secure,
    /// SameSite=Lax, Path=/, no Domain.
    #[tokio::test]
    async fn s30_cookie_attributes() {
        let h = harness_store(false, None, Some(Arc::new(Memory::new())));
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": "a@b.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        let (st, heads, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "a@b.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let set = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        for attr in [
            "__Host-drs=drs_",
            "Secure",
            "HttpOnly",
            "SameSite=Lax",
            "Path=/",
        ] {
            // DECISIVE(cookie-attrs)
            assert!(set.contains(attr), "Set-Cookie missing {attr}: {set}");
        }
        assert!(!set.contains("Domain"), "__Host- forbids Domain: {set}");
    }

    /// A forged `drs_` value dies on the tag check — before any store
    /// I/O (review #8). Proved at the check itself: the mutation that
    /// removes it makes this assertion red.
    #[test]
    fn s30_session_forge_dies_before_the_store() {
        let pubid = pubid::PubId::ephemeral();
        let key = pubid.derive_key(b"session-token-v1");
        let forged = format!("drs_{}", hex(&[0xabu8; 32]));
        // DECISIVE(session-forge)
        assert!(
            session_token_hash(&key, &forged).is_none(),
            "a token this process never minted must fail the tag"
        );
        let minted = mint_session_token(&key);
        assert!(session_token_hash(&key, &minted).is_some());
    }

    /// Unknown `drs_` values — even well-formed ones — answer 401, and
    /// `GET /auth/session` answers 404 when no store is configured
    /// (the dashboard's compat-mode signal).
    #[tokio::test]
    async fn s30_session_unknown_and_no_store() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store));
        let forged = format!("drs_{}", hex(&[7u8; 32]));
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", format!("__Host-drs={forged}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);

        let h2 = harness_store(false, None, None);
        let (st, _, _) = raw(&h2.router, "GET", "/auth/session", &[], None).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "no store → 404, not 401");
    }

    /// The origin checks guard the unauthenticated POSTs too (review
    /// #17): cross-site fetch metadata or a foreign Origin refuse 403.
    #[tokio::test]
    async fn s30_csrf_open_post() {
        let h = harness_store(false, None, Some(Arc::new(Memory::new())));
        for bad in [
            ("sec-fetch-site", "cross-site"),
            ("sec-fetch-site", "same-site"),
            ("origin", "https://evil.example"),
        ] {
            let hs = [
                ("host", "durablerunnerapp.404human.dev".to_string()),
                (bad.0, bad.1.to_string()),
            ];
            let (st, _, v) = raw(
                &h.router,
                "POST",
                "/auth/login",
                &hs,
                Some(json!({"email": "a@b.co", "password": "x"})),
            )
            .await;
            // DECISIVE(csrf-open-post)
            assert_eq!(st, StatusCode::FORBIDDEN, "{bad:?}: {v}");
            assert_eq!(v["code"], "csrf_failed");
        }
        // Same-origin metadata passes the gate (then fails the login).
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[
                ("host", "durablerunnerapp.404human.dev".to_string()),
                ("sec-fetch-site", "same-origin".to_string()),
                (
                    "origin",
                    "https://durablerunnerapp.404human.dev".to_string(),
                ),
            ],
            Some(json!({"email": "a@b.co", "password": "x"})),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
    }

    /// Re-sign-up of an unverified address kills sessions minted with
    /// the earlier credential and installs the new password — the
    /// pre-registration takeover's second leg (review #2).
    #[tokio::test]
    async fn s30_prereg_resignup_revokes() {
        let h = harness_store(false, None, Some(Arc::new(Memory::new())));
        // Attacker signs up the victim's address and logs in.
        let (attacker_cookie, _) =
            sign_in(&h.router, "victim@example.com", "attacker-passphrase").await;
        // The victim re-signs up with their own password.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": "victim@example.com", "password": "victim-passphrase-ok"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        // The attacker's session is dead and their password no longer
        // works; the victim's does.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", attacker_cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "victim@example.com", "password": "attacker-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "{v}");
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "victim@example.com", "password": "victim-passphrase-ok"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
    }

    /// The argon2 queue is bounded (review #9): a second concurrent job
    /// from the same address is refused 503 busy + Retry-After.
    #[tokio::test]
    async fn s30_argon_queue_is_bounded() {
        let h = harness_store(false, None, Some(Arc::new(Memory::new())));
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": "q@q.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        let body = || Some(json!({"email": "q@q.co", "password": "a-long-enough-passphrase"}));
        let r1 = h.router.clone();
        let r2 = h.router.clone();
        let (a, b) = tokio::join!(
            raw(&r1, "POST", "/auth/login", &[], body()),
            raw(&r2, "POST", "/auth/login", &[], body())
        );
        let mut busy = [a, b]
            .into_iter()
            .filter(|(st, heads, _)| {
                *st == StatusCode::SERVICE_UNAVAILABLE && heads.contains_key(header::RETRY_AFTER)
            })
            .count();
        assert!(busy <= 1, "at most one concurrent job may be refused");
        // Saturate directly: two queued calls from the same address
        // always refuse the second.
        let slow = || std::thread::sleep(Duration::from_millis(50));
        let addr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        let (r1, r2) = tokio::join!(
            h.app.argon.run(addr, false, slow),
            h.app.argon.run(addr, false, slow)
        );
        busy = [r1, r2].into_iter().filter(|r| r.is_err()).count();
        assert_eq!(busy, 1, "one job per address");
    }

    /// Store-down drill: a cached session keeps working through an
    /// outage; an uncached token fails closed 503 accounts_unavailable
    /// (never a silent deny).
    #[tokio::test]
    async fn s30_store_down_cached_sessions_survive() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let (cookie, _) = sign_in(&h.router, "d@d.co", "a-long-enough-passphrase").await;
        // Prime the cache, then drop the store.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        store.set_unavailable(true);
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "cached grant survives the outage");
        // A real (well-tagged) token the cache has never seen → 503.
        let other = mint_session_token(&h.app.session_key.clone());
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", format!("__Host-drs={other}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(v["code"], "accounts_unavailable");
    }

    // ---- S3-1: verify / reset / outbox drain / lockout -------------

    /// The `drt_` token out of the newest pending outbox row of `kind`
    /// for `email` — the link the mailbox owner would click.
    fn mailed_token(store: &Memory, kind: &'static str, email: &str) -> String {
        store
            .outbox()
            .into_iter()
            .rev()
            .find(|r| r.status == "pending" && r.kind == kind && r.send_to == email)
            .and_then(|r| {
                r.mail.text.split("token=").nth(1).map(|t| {
                    t.chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                        .collect()
                })
            })
            .unwrap_or_else(|| panic!("no pending {kind} mail for {email}"))
    }

    async fn signup_only(router: &Router, email: &str, password: &str) {
        let (st, _, v) = raw(
            router,
            "POST",
            "/auth/signup",
            &[],
            Some(json!({"email": email, "password": password})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED, "{v}");
    }

    /// Sign up, then click the mailed verify link.
    async fn signup_and_verify(router: &Router, store: &Memory, email: &str, password: &str) {
        signup_only(router, email, password).await;
        let token = mailed_token(store, "verify", email);
        let (st, _, v) = raw(
            router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
    }

    /// Verify: the mailed link verifies the account, answers
    /// `{verified: true}` — and NEVER sets a session cookie: the
    /// mailbox owner signs in fresh (review #2/#17). Proves
    /// `verify-sets-cookie`.
    #[tokio::test]
    async fn s31_verify_round_trip() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "v@v.co", "a-long-enough-passphrase").await;
        // A session minted before verify dies with it (prereg proof's
        // other half — the revoke lives in the consume).
        let (cookie, _) = sign_in(&h.router, "v@v.co", "a-long-enough-passphrase").await;
        let token = mailed_token(&store, "verify", "v@v.co");
        let (st, heads, v) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[("cookie", cookie.clone())],
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["verified"], true);
        // DECISIVE(verify-sets-cookie)
        assert!(
            heads.get_all(header::SET_COOKIE).iter().next().is_none(),
            "verify must never set a cookie: {heads:?}"
        );
        let acct = store
            .account_by_email("v@v.co")
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("account"));
        assert!(acct.email_verified);
        // The pre-verify session is dead; a fresh login works.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (_, _) = sign_in(&h.router, "v@v.co", "a-long-enough-passphrase").await;
    }

    /// A used `drt_` token is dead — consumed, superseded, expired and
    /// unknown all read as the one 410. Proves `token-replay`.
    #[tokio::test]
    async fn s31_verify_token_replay_410() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "r@r.co", "a-long-enough-passphrase").await;
        let token = mailed_token(&store, "verify", "r@r.co");
        let body = || Some(json!({"token": token}));
        let (st, _, _) = raw(&h.router, "POST", "/auth/verify", &[], body()).await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, v) = raw(&h.router, "POST", "/auth/verify", &[], body()).await;
        // DECISIVE(token-replay)
        assert_eq!(st, StatusCode::GONE, "{v}");
        assert_eq!(v["code"], "token_gone");
        // Garbage shaped nothing like a token: same 410, no oracle.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": "not-a-token"})),
        )
        .await;
        assert_eq!(st, StatusCode::GONE);
    }

    /// A `reset` token presented to `/auth/verify` is a dead verify
    /// token — the kind is bound inside the consume. Proves
    /// `token-kind-confusion`.
    #[tokio::test]
    async fn s31_reset_token_cannot_verify() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "k@k.co", "a-long-enough-passphrase").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/forgot",
            &[],
            Some(json!({"email": "k@k.co"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        let reset = mailed_token(&store, "reset", "k@k.co");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": reset})),
        )
        .await;
        // DECISIVE(token-kind-confusion)
        assert_eq!(st, StatusCode::GONE, "{v}");
        // And the reset link still works for its own route.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[],
            Some(json!({"token": reset, "password": "another-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
    }

    /// Forgot → mailed reset link → reset installs the password,
    /// revokes every session and sibling token, and mints a fresh
    /// session only for an unaffiliated requester.
    #[tokio::test]
    async fn s31_forgot_reset_round_trip() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "f@f.co", "first-long-passphrase").await;
        let (cookie, _) = sign_in(&h.router, "f@f.co", "first-long-passphrase").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/forgot",
            &[],
            Some(json!({"email": "f@f.co"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED, "{v}");
        assert_eq!(v, json!({"status": "sent"}));
        let token = mailed_token(&store, "reset", "f@f.co");
        // Unaffiliated requester (no cookie) → fresh session minted.
        let (st, heads, v) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[],
            Some(json!({"token": token, "password": "second-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let fresh = heads
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|s| s.starts_with("__Host-drs="))
            .map(cookie_header)
            .unwrap_or_else(|| panic!("reset mints a session: {heads:?}"));
        // The pre-reset session is dead; the fresh one resolves.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", fresh)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["account"]["email"], "f@f.co");
        // Old password dead, new password live, replay dead.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "f@f.co", "password": "first-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "f@f.co", "password": "second-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[],
            Some(json!({"token": token, "password": "third-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::GONE);
    }

    /// A foreign session is never signed over by someone else's reset
    /// link (review #17): B's browser redeeming A's link keeps B's
    /// session — no new cookie is issued.
    #[tokio::test]
    async fn s31_reset_keeps_foreign_session() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "a@a.co", "a-first-passphrase").await;
        signup_and_verify(&h.router, &store, "b@b.co", "b-first-passphrase").await;
        let (cookie_b, _) = sign_in(&h.router, "b@b.co", "b-first-passphrase").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/forgot",
            &[],
            Some(json!({"email": "a@a.co"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        let token = mailed_token(&store, "reset", "a@a.co");
        let (st, heads, v) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[("cookie", cookie_b.clone())],
            Some(json!({"token": token, "password": "a-second-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert!(
            heads
                .get_all(header::SET_COOKIE)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .all(|s| !s.starts_with("__Host-drs=")),
            "a foreign session must not be signed over: {heads:?}"
        );
        // B's session survives and still resolves as B.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie_b)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["account"]["email"], "b@b.co");
    }

    /// forgot + resend-verification answer one envelope for known,
    /// unverified and unknown addresses (review #3). Proves
    /// `enumeration` — the response may not depend on `issued`.
    #[tokio::test]
    async fn s31_forgot_resend_envelopes_match() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "known@k.co", "a-long-enough-passphrase").await;
        signup_only(&h.router, "unverified@u.co", "a-long-enough-passphrase").await;
        let forgot = |email: &'static str| {
            let r = h.router.clone();
            async move {
                raw(
                    &r,
                    "POST",
                    "/auth/forgot",
                    &[],
                    Some(json!({"email": email})),
                )
                .await
            }
        };
        let (st1, _, v1) = forgot("known@k.co").await;
        let (st2, _, v2) = forgot("nobody@nowhere.co").await;
        // DECISIVE(enumeration)
        assert_eq!((st1, &v1), (st2, &v2), "forgot must not register");
        assert_eq!(st1, StatusCode::ACCEPTED);
        // And the outbox really did differ — a mail went only to the
        // known address (the envelope hid it).
        assert!(
            store
                .outbox()
                .iter()
                .any(|r| r.kind == "reset" && r.send_to == "known@k.co"),
            "a reset mail was enqueued for the known address"
        );
        let resend = |email: &'static str| {
            let r = h.router.clone();
            async move {
                raw(
                    &r,
                    "POST",
                    "/auth/resend-verification",
                    &[],
                    Some(json!({"email": email})),
                )
                .await
            }
        };
        let (st1, _, v1) = resend("unverified@u.co").await;
        let (st2, _, v2) = resend("ghost@nowhere.co").await;
        assert_eq!((st1, &v1), (st2, &v2), "resend must not register");
        let (st3, _, v3) = resend("known@k.co").await;
        assert_eq!(
            (st1, &v1),
            (st3, &v3),
            "an already-verified address reads the same"
        );
    }

    /// `invite_only`: the gate asks the store for a live invite whose
    /// bound email matches the sign-up's — anything less is 403
    /// `invite_required`. Proves `invite-required`.
    #[tokio::test]
    async fn s31_invite_only_gate() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let mut app = h.app.clone();
        app.invite_only = true;
        let r = router(app, None);
        let body = |tok: Option<&str>| {
            let mut b = json!({"email": "inv@i.co", "password": "a-long-enough-passphrase"});
            if let Some(t) = tok {
                b["invite_token"] = json!(t);
            }
            Some(b)
        };
        let (st, _, v) = raw(&r, "POST", "/auth/signup", &[], body(None)).await;
        // DECISIVE(invite-required)
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "invite_required");
        // A drt_-kind mail token can never open the gate — the parser
        // is the dri_ kind, not the email-token kind (first-deploy bug).
        let (st, _, _) = raw(&r, "POST", "/auth/signup", &[], body(Some("drt_bogus"))).await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        // A live invite for a DIFFERENT email is not a grant for this one.
        let ws = store
            .create_workspace("w")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let other = mint_invite_token();
        store.add_invite(
            ws.id,
            "other@o.co",
            durable_accounts::Role::Member,
            invite_token_hash(&other).unwrap_or_else(|| panic!("shape")),
            unix_ms() + 86_400_000,
        );
        let (st, _, _) = raw(&r, "POST", "/auth/signup", &[], body(Some(&other))).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "the invite binds its email");
        // An expired invite is dead even for its own email.
        let dead = mint_invite_token();
        store.add_invite(
            ws.id,
            "inv@i.co",
            durable_accounts::Role::Member,
            invite_token_hash(&dead).unwrap_or_else(|| panic!("shape")),
            unix_ms() - 1,
        );
        let (st, _, _) = raw(&r, "POST", "/auth/signup", &[], body(Some(&dead))).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "expired invite");
        // A live invite for THIS email opens the gate.
        let tok = mint_invite_token();
        store.add_invite(
            ws.id,
            "inv@i.co",
            durable_accounts::Role::Member,
            invite_token_hash(&tok).unwrap_or_else(|| panic!("shape")),
            unix_ms() + 86_400_000,
        );
        let (st, _, v) = raw(&r, "POST", "/auth/signup", &[], body(Some(&tok))).await;
        assert_eq!(st, StatusCode::ACCEPTED, "{v}");
    }

    /// The mailbox owner's click kills the pre-registration attacker's
    /// session (review #2): sign up a victim address, hold a session,
    /// verify — the held session dies. Proves `prereg-takeover`.
    #[tokio::test]
    async fn s31_verify_revokes_prereg_session() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "victim@x.co", "attacker-passphrase").await;
        let (attacker, _) = sign_in(&h.router, "victim@x.co", "attacker-passphrase").await;
        // The victim clicks the mailed link.
        let token = mailed_token(&store, "verify", "victim@x.co");
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", attacker)],
            None,
        )
        .await;
        // DECISIVE(prereg-takeover)
        assert_eq!(
            st,
            StatusCode::UNAUTHORIZED,
            "the prereg session dies at verify"
        );
    }

    /// Failures on a (normalized email, client prefix) pair cool off at
    /// the 10th: the next attempt — right or wrong, and whether or not
    /// the account exists — answers the same 429 + Retry-After (review
    /// #13). Proves `lockout-shape`.
    #[tokio::test]
    async fn s31_lockout_cools_off() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "l@l.co", "a-long-enough-passphrase").await;
        let login = |email: &'static str, pw: &'static str| {
            let r = h.router.clone();
            async move {
                raw(
                    &r,
                    "POST",
                    "/auth/login",
                    &[],
                    Some(json!({"email": email, "password": pw})),
                )
                .await
            }
        };
        for i in 0..10 {
            let (st, _, _) = login("l@l.co", "wrong-passphrase").await;
            assert_eq!(st, StatusCode::UNAUTHORIZED, "attempt {i}");
        }
        // The ten failures were recorded — the lockout's state, not
        // its answer (lockout-shape's own decisive line).
        // DECISIVE(lockout-shape)
        assert!(
            !h.app
                .lockouts
                .map
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty(),
            "the ten failures were recorded"
        );
        // The 11th cools off — even the right password — and the
        // Retry-After is the lockout's minutes, not the window's ~60 s.
        let (st, heads, v) = login("l@l.co", "a-long-enough-passphrase").await;
        // DECISIVE(cooloff)
        assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{v}");
        assert_eq!(v["code"], "rate_limited");
        let retry: u64 = heads
            .get(header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();
        assert!(retry > 60, "the lockout's cool-off, not the addr window");
        // An address that does not exist cools off identically — a 429
        // cannot answer "is this email registered" (review #13).
        for _ in 0..10 {
            let (st, _, _) = login("ghost@nowhere.co", "wrong-passphrase").await;
            assert_eq!(st, StatusCode::UNAUTHORIZED);
        }
        let (st, _, v) = login("ghost@nowhere.co", "wrong-passphrase").await;
        assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{v}");
    }

    /// The HMAC-tagged known-device cookie is the owner's bypass (review
    /// #13): it skips the cool-off but never the password check — and a
    /// cookie minted for another account skips nothing.
    #[tokio::test]
    async fn s31_known_device_bypasses_cooloff() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "d@d.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "e@e.co", "a-long-enough-passphrase").await;
        // Sign in and capture the device cookie the login minted.
        let (st, heads, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "d@d.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let drd = heads
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|s| s.starts_with("__Host-drd="))
            .map(cookie_header)
            .unwrap_or_else(|| panic!("login mints __Host-drd: {heads:?}"));
        assert!(drd.starts_with("__Host-drd=drd_"));
        for _ in 0..10 {
            let (st, _, _) = raw(
                &h.router,
                "POST",
                "/auth/login",
                &[],
                Some(json!({"email": "d@d.co", "password": "wrong-passphrase"})),
            )
            .await;
            assert_eq!(st, StatusCode::UNAUTHORIZED);
        }
        // Without the device cookie the pair is in cool-off…
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "d@d.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::TOO_MANY_REQUESTS);
        // …the device cookie carries them through — but still must
        // answer the password, it is no auth factor.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[("cookie", drd.clone())],
            Some(json!({"email": "d@d.co", "password": "wrong-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "{v}");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[("cookie", drd.clone())],
            Some(json!({"email": "d@d.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // Another account's lockout is untouched by d@d.co's cookie.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[("cookie", drd)],
            Some(json!({"email": "e@e.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "e@e.co was never locked");
    }

    /// A bad addr-spec never reaches the outbox — CRLF, display names,
    /// second `@`s are refused at enqueue (review #14). Proves
    /// `addr-spec-injection`.
    #[tokio::test]
    async fn s31_addr_spec_refused() {
        let store = Arc::new(Memory::new());
        let mail = |to: &str| Mail {
            kind: "notice",
            send_to: to.to_string(),
            subject: "s".into(),
            text: "t".into(),
            html: "h".into(),
            idempotency_key: format!("k:{to}"),
            expires_at_ms: None,
        };
        for bad in [
            "a@b.co\r\nBcc: x@y.co",
            "a@b.co\nBcc: x@y.co",
            "Display <a@b.co>",
            "a@b.co, x@y.co",
            "a@@b.co",
            "a b@c.co",
            "",
        ] {
            let r = store.enqueue_mail(&mail(bad)).await;
            // DECISIVE(addr-spec-injection)
            assert!(matches!(r, Ok(None)), "{bad:?} must be refused");
        }
        assert!(store.outbox().is_empty(), "nothing enqueued");
        assert_eq!(store.outbox().len(), 0);
        assert!(
            store
                .enqueue_mail(&mail("a+b@sub.d.co"))
                .await
                .is_ok_and(|id| id.is_some()),
            "a real addr-spec enqueues"
        );
    }

    /// The drain claims due rows, sends through the Mailer under the
    /// deadline and marks `sent` — never re-claims a sent row (D-068).
    #[tokio::test]
    async fn s31_outbox_drain_sends() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        assert_eq!(
            store
                .outbox()
                .iter()
                .filter(|r| r.status == "pending")
                .count(),
            1
        );
        let sent = drain_outbox_once(&h.app).await;
        assert_eq!(sent, 1);
        let mailer = h
            .app
            .mailer
            .as_any()
            .downcast_ref::<durable_accounts::MemoryMailer>()
            .unwrap_or_else(|| panic!("memory mailer"));
        let captured = mailer.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].kind, "verify");
        assert_eq!(captured[0].send_to, "o@o.co");
        assert!(
            store.outbox().iter().all(|r| r.status == "sent"),
            "the row closed"
        );
        assert_eq!(
            drain_outbox_once(&h.app).await,
            0,
            "a sent row is never re-claimed"
        );
    }

    /// The global send budget stops the drain mid-batch (review #14):
    /// a claimed row waits for budget rather than send uncapped.
    /// Proves `mail-global`.
    #[tokio::test]
    async fn s31_mail_global_budget_stops_drain() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let mut app = h.app.clone();
        app.mail_budget = std::sync::Arc::new(MailBudget::new(0));
        let r = router(app.clone(), None);
        signup_only(&r, "cap@c.co", "a-long-enough-passphrase").await;
        let sent = drain_outbox_once(&app).await;
        // DECISIVE(mail-global)
        assert_eq!(sent, 0, "a zero budget sends nothing");
        let mailer = app
            .mailer
            .as_any()
            .downcast_ref::<durable_accounts::MemoryMailer>()
            .unwrap_or_else(|| panic!("memory mailer"));
        assert!(mailer.captured().is_empty());
    }

    /// The per-(address, kind) hourly cap coalesces a spam loop: the
    /// 4th mail inside the hour is dropped at enqueue — newest still
    /// replaces a pending row for free (review #14). Proves
    /// `mail-budget`.
    #[tokio::test]
    async fn s31_outbox_hourly_cap() {
        let store = Arc::new(Memory::new());
        let mail = |n: u32| Mail {
            kind: "notice",
            send_to: "cap@d.co".into(),
            subject: "s".into(),
            text: "t".into(),
            html: "h".into(),
            idempotency_key: format!("n:{n}"),
            expires_at_ms: None,
        };
        // Three enqueues land when each prior row is no longer pending.
        for n in 0..3 {
            let id = store
                .enqueue_mail(&mail(n))
                .await
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("enqueue {n}"));
            store
                .outbox_mark(id, durable_accounts::MailOutcome::Sent)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let fourth = store
            .enqueue_mail(&mail(3))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // DECISIVE(mail-budget)
        assert_eq!(fourth, None, "the 4th mail inside the hour coalesces");
        assert_eq!(store.outbox().len(), 3);
    }

    /// resend-verification supersedes the pending token and mails a
    /// fresh link — the old link 410s, the new one verifies.
    #[tokio::test]
    async fn s31_resend_verification_supersedes() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "rs@r.co", "a-long-enough-passphrase").await;
        let first = mailed_token(&store, "verify", "rs@r.co");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/resend-verification",
            &[],
            Some(json!({"email": "rs@r.co"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED, "{v}");
        let second = mailed_token(&store, "verify", "rs@r.co");
        assert_ne!(first, second, "a fresh token was mailed");
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": first})),
        )
        .await;
        assert_eq!(st, StatusCode::GONE, "the superseded link is dead");
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/verify",
            &[],
            Some(json!({"token": second})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
    }

    // ---- S3-2: session lifecycle + /me ------------------------------

    /// A pure login (no signup) — the second+ session on one account.
    async fn login_only(router: &Router, email: &str, password: &str) -> (String, String) {
        let (st, heads, v) = raw(
            router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": email, "password": password})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "login: {v}");
        let set = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        (
            cookie_header(&set),
            v["csrf"].as_str().unwrap_or_default().to_string(),
        )
    }

    /// The opaque ids the sessions list reports for `cookie`.
    async fn session_public_ids(router: &Router, cookie: &str) -> Vec<String> {
        let (st, _, v) = raw(
            router,
            "GET",
            "/me/sessions",
            &[("cookie", cookie.to_string())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v["sessions"]
            .as_array()
            .unwrap_or_else(|| panic!("sessions array: {v}"))
            .iter()
            .map(|s| s["id"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// `/me` needs a session (401 without); the mutation routes need
    /// the synchroniser CSRF (403 csrf_failed without it).
    #[tokio::test]
    async fn s32_me_requires_session_and_csrf() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let (st, _, _) = raw(&h.router, "GET", "/me", &[], None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (st, _, _) = raw(&h.router, "GET", "/me/sessions", &[], None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        signup_only(&h.router, "me@me.co", "a-long-enough-passphrase").await;
        let (cookie, _) = login_only(&h.router, "me@me.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(&h.router, "GET", "/me", &[("cookie", cookie.clone())], None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["account"]["email"], "me@me.co");
        // No CSRF header on the mutations → 403.
        let (st, _, v) = raw(
            &h.router,
            "DELETE",
            "/me/sessions",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "csrf_failed");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/me/password",
            &[("cookie", cookie)],
            Some(json!({"current_password": "x", "new_password": "y"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "csrf_failed");
    }

    /// The sessions list carries opaque public_ids only — the internal
    /// row id never serializes — and `current` marks the requester's
    /// own row.
    #[tokio::test]
    async fn s32_me_lists_sessions() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "l@l.co", "a-long-enough-passphrase").await;
        let (c1, _) = login_only(&h.router, "l@l.co", "a-long-enough-passphrase").await;
        let (c2, _) = login_only(&h.router, "l@l.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(&h.router, "GET", "/me/sessions", &[("cookie", c1)], None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let rows = v["sessions"]
            .as_array()
            .unwrap_or_else(|| panic!("list: {v}"));
        assert_eq!(rows.len(), 2, "two live sessions");
        assert!(
            rows.iter()
                .all(|r| uuid::Uuid::parse_str(r["id"].as_str().unwrap_or_default()).is_ok()),
            "opaque public ids only: {rows:?}"
        );
        assert!(
            rows.iter().all(|r| r.get("session_id").is_none()),
            "the internal id never serializes: {rows:?}"
        );
        let currents: Vec<String> = rows
            .iter()
            .filter(|r| r["current"] == true)
            .map(|r| r["id"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(currents.len(), 1, "exactly one row is the requester's");
        // From c2's view a different row is current.
        let (st, _, v) = raw(&h.router, "GET", "/me/sessions", &[("cookie", c2)], None).await;
        assert_eq!(st, StatusCode::OK);
        let other: Vec<String> = v["sessions"]
            .as_array()
            .unwrap_or_else(|| panic!("list: {v}"))
            .iter()
            .filter(|r| r["current"] == true)
            .map(|r| r["id"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(other.len(), 1);
        assert_ne!(other[0], currents[0], "the flag follows the requester");
    }

    /// Revoke-one by public_id is bound to the account inside the
    /// store's lookup: another account's id answers the same 404 as an
    /// unknown one — and the foreign session lives on. Proves
    /// `session-idor`.
    #[tokio::test]
    async fn s32_revoke_one_session() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        signup_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        let (ca, csrf_a) = login_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        let (cb, csrf_b) = login_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        let sa = session_public_ids(&h.router, &ca).await;
        let sb = session_public_ids(&h.router, &cb).await;
        assert_eq!(sa.len(), 1);
        assert_eq!(sb.len(), 1);
        // A revoking B's id → 404, and B's session still resolves.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/me/sessions/{}", sb[0]),
            &[("cookie", ca.clone()), ("x-durable-csrf", csrf_a.clone())],
            None,
        )
        .await;
        // DECISIVE(session-idor)
        assert_eq!(st, StatusCode::NOT_FOUND, "no cross-account session write");
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cb.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "the victim session lives");
        // An unknown id reads identically.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/me/sessions/{}", uuid::Uuid::now_v7()),
            &[("cookie", ca), ("x-durable-csrf", csrf_a)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        // B revoking its own → 204 and B's cached grant dies at once.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/me/sessions/{}", sb[0]),
            &[("cookie", cb.clone()), ("x-durable-csrf", csrf_b)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NO_CONTENT);
        let (st, _, _) = raw(&h.router, "GET", "/auth/session", &[("cookie", cb)], None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
    }

    /// Revoke-all-but-current: the victims' cached grants die with the
    /// account bump, the requester's is re-warmed and keeps working.
    #[tokio::test]
    async fn s32_revoke_all_but_current() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "all@all.co", "a-long-enough-passphrase").await;
        let (c1, csrf1) = login_only(&h.router, "all@all.co", "a-long-enough-passphrase").await;
        let (c2, _) = login_only(&h.router, "all@all.co", "a-long-enough-passphrase").await;
        let (c3, _) = login_only(&h.router, "all@all.co", "a-long-enough-passphrase").await;
        // Prime every grant into the cache — the victims must die in
        // cache too, not just in the store.
        for c in [&c1, &c2, &c3] {
            let (st, _, _) = raw(
                &h.router,
                "GET",
                "/auth/session",
                &[("cookie", c.clone())],
                None,
            )
            .await;
            assert_eq!(st, StatusCode::OK);
        }
        let (st, _, v) = raw(
            &h.router,
            "DELETE",
            "/me/sessions",
            &[("cookie", c1.clone()), ("x-durable-csrf", csrf1)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["revoked"], 2);
        for c in [c2, c3] {
            let (st, _, _) = raw(&h.router, "GET", "/auth/session", &[("cookie", c)], None).await;
            assert_eq!(st, StatusCode::UNAUTHORIZED, "the victim dies in cache too");
        }
        let (st, _, _) = raw(&h.router, "GET", "/auth/session", &[("cookie", c1)], None).await;
        assert_eq!(st, StatusCode::OK, "the requester keeps its session");
    }

    /// Password change: re-auth gates it (wrong current → 403, nothing
    /// mutated), then one transaction installs the hash and revokes
    /// every session — the caller leaves with a rotated cookie and the
    /// account's other session dies too.
    #[tokio::test]
    async fn s32_password_change_reauths_and_rotates() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "pw@pw.co", "a-long-enough-passphrase").await;
        let (c1, csrf1) = login_only(&h.router, "pw@pw.co", "a-long-enough-passphrase").await;
        let (c2, _) = login_only(&h.router, "pw@pw.co", "a-long-enough-passphrase").await;
        // Prime both grants in the cache.
        for c in [&c1, &c2] {
            let (st, _, _) = raw(
                &h.router,
                "GET",
                "/auth/session",
                &[("cookie", c.clone())],
                None,
            )
            .await;
            assert_eq!(st, StatusCode::OK);
        }
        // Re-auth: the wrong current password is refused and mutates
        // nothing.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/me/password",
            &[("cookie", c1.clone()), ("x-durable-csrf", csrf1.clone())],
            Some(json!({"current_password": "the-wrong-passphrase", "new_password": "a-new-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "invalid_credentials");
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", c1.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "a refused change mutates nothing");
        // The right pair: new hash + every session revoked in one
        // transaction; the response carries the rotated cookie.
        let (st, heads, v) = raw(
            &h.router,
            "POST",
            "/me/password",
            &[("cookie", c1.clone()), ("x-durable-csrf", csrf1)],
            Some(json!({"current_password": "a-long-enough-passphrase", "new_password": "a-new-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["changed"], true);
        assert!(v["csrf"].is_string(), "the new session's csrf");
        let new_cookie = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .map(cookie_header)
            .unwrap_or_else(|| panic!("a rotated cookie is set: {heads:?}"));
        assert_ne!(new_cookie, c1, "the session token rotated");
        let new_cookie_for_list = new_cookie.clone();
        // Every old grant is dead — in store AND cache — immediately.
        for c in [c1, c2] {
            let (st, _, _) = raw(&h.router, "GET", "/auth/session", &[("cookie", c)], None).await;
            assert_eq!(st, StatusCode::UNAUTHORIZED);
        }
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", new_cookie)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // The old password can't sign in; the new one can.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[],
            Some(json!({"email": "pw@pw.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (_, _) = login_only(&h.router, "pw@pw.co", "a-new-long-passphrase").await;
        // And the sessions list shows only the live rows — the revoked
        // ones never list.
        let ids = session_public_ids(&h.router, &new_cookie_for_list).await;
        assert_eq!(ids.len(), 2, "the rotated session + the fresh login");
    }

    /// The injected clock proves idle expiry: use slides the deadline
    /// (the flush lands it in the store), disuse lets it die — even
    /// inside the absolute window. Proves `idle-timeout`.
    #[tokio::test]
    async fn s32_idle_slides_then_times_out() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let t0 = 1_700_000_000_000i64;
        h.app
            .now_override_ms
            .store(t0, std::sync::atomic::Ordering::Relaxed);
        let (cookie, _) = sign_in(&h.router, "i@i.co", "a-long-enough-passphrase").await;
        let set_now = |t: i64| {
            h.app
                .now_override_ms
                .store(t, std::sync::atomic::Ordering::Relaxed)
        };
        let probe = |cookie: &str| {
            let r = h.router.clone();
            let c = cookie.to_string();
            async move {
                raw(&r, "GET", "/auth/session", &[("cookie", c)], None)
                    .await
                    .0
            }
        };
        // Use at t0+1 keeps it alive to t0+IDLE+1 — a pending touch is
        // a live floor before the store ever sees it.
        set_now(t0 + 1);
        assert_eq!(probe(&cookie).await, StatusCode::OK);
        set_now(t0 + SESSION_IDLE_MS);
        assert_eq!(
            probe(&cookie).await,
            StatusCode::OK,
            "the touch slid the idle deadline"
        );
        // Flush lands the slide in the store.
        flush_session_touches(&h.app).await;
        let account = store
            .account_by_email("i@i.co")
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("account"));
        let rows = store
            .sessions_for_account(account.id, 10)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].idle_expires_at_ms,
            t0 + 2 * SESSION_IDLE_MS,
            "the flushed slide moved the store's deadline"
        );
        // Disuse past that deadline dies — inside the absolute window.
        set_now(t0 + 2 * SESSION_IDLE_MS + 1);
        // DECISIVE(idle-timeout)
        assert_eq!(probe(&cookie).await, StatusCode::UNAUTHORIZED);
    }

    /// Absolute expiry caps the slide: use right up to the boundary
    /// keeps a session alive, but past `expires_at` no amount of use
    /// revives it.
    #[tokio::test]
    async fn s32_absolute_expiry_caps_the_slide() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        let t0 = 1_700_000_000_000i64;
        h.app
            .now_override_ms
            .store(t0, std::sync::atomic::Ordering::Relaxed);
        let (cookie, _) = sign_in(&h.router, "abs@abs.co", "a-long-enough-passphrase").await;
        let probe = |cookie: &str, at: i64| {
            h.app
                .now_override_ms
                .store(at, std::sync::atomic::Ordering::Relaxed);
            let r = h.router.clone();
            let c = cookie.to_string();
            async move {
                raw(&r, "GET", "/auth/session", &[("cookie", c)], None)
                    .await
                    .0
            }
        };
        // Stay active in steps under the idle window until just inside
        // the absolute deadline — each step lands strictly inside the
        // slide floor the previous touch set (a deadline equal to now
        // is dead).
        for at in [
            t0 + SESSION_IDLE_MS / 2,
            t0 + SESSION_IDLE_MS * 3 / 2 - 1,
            t0 + SESSION_ABS_MS - 1,
        ] {
            assert_eq!(probe(&cookie, at).await, StatusCode::OK, "at {at}");
        }
        // One millisecond past: the pending slide floor outlives the
        // absolute deadline, and the absolute deadline wins.
        assert_eq!(
            probe(&cookie, t0 + SESSION_ABS_MS + 1).await,
            StatusCode::UNAUTHORIZED
        );
    }

    /// Sign-in always mints a fresh server-chosen token — a cookie the
    /// client arrived with can never become the session. Proves
    /// `session-fixation`.
    #[tokio::test]
    async fn s32_login_never_adopts_a_planted_cookie() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_only(&h.router, "f@f.co", "a-long-enough-passphrase").await;
        // A structurally valid drs_ token the attacker prepared — it
        // passes the tag check but the server never minted it.
        let planted = mint_session_token(&h.app.session_key);
        assert!(
            session_token_hash(&h.app.session_key, &planted).is_some(),
            "a valid planted value for the fixation attempt"
        );
        let (st, heads, v) = raw(
            &h.router,
            "POST",
            "/auth/login",
            &[("cookie", format!("{SESSION_COOKIE}={planted}"))],
            Some(json!({"email": "f@f.co", "password": "a-long-enough-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let issued = heads
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .map(cookie_header)
            .unwrap_or_else(|| panic!("login mints a cookie: {heads:?}"));
        // DECISIVE(session-fixation)
        assert_ne!(
            issued,
            format!("{SESSION_COOKIE}={planted}"),
            "the server chooses the session value"
        );
        // The planted value resolves no session — it was never minted
        // server-side.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", format!("{SESSION_COOKIE}={planted}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
    }

    /// A reset revokes every session in the store's transaction and the
    /// account bump makes the cache agree at once — the stale cookie
    /// dies mid-flight, not at grant TTL. And the token is single-use:
    /// the replay answers the one 410. Proves `reset-token-replay`.
    #[tokio::test]
    async fn s32_reset_kills_cached_sessions_and_replay_dies() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "rs@rs.co", "a-long-enough-passphrase").await;
        let (cookie, _) = login_only(&h.router, "rs@rs.co", "a-long-enough-passphrase").await;
        // Prime the grant into the cache.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/auth/forgot",
            &[],
            Some(json!({"email": "rs@rs.co"})),
        )
        .await;
        assert_eq!(st, StatusCode::ACCEPTED);
        let token = mailed_token(&store, "reset", "rs@rs.co");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[],
            Some(json!({"token": token, "password": "a-new-long-passphrase"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // The cached grant died with the account bump — instantly.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            "/auth/session",
            &[("cookie", cookie)],
            None,
        )
        .await;
        assert_eq!(
            st,
            StatusCode::UNAUTHORIZED,
            "reset kills the cached grant at once"
        );
        // And the consumed token replays to the one 410.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/auth/reset",
            &[],
            Some(json!({"token": token, "password": "another-long-passphrase"})),
        )
        .await;
        // DECISIVE(reset-token-replay)
        assert_eq!(st, StatusCode::GONE, "{v}");
        assert_eq!(v["code"], "token_gone");
    }

    // ---- S3-3: workspaces, members, roles, invites -------------------

    /// Sign up + verify + log in `email`, then seat it as an ADMIN member
    /// of `ws` — a store-level invite + accept, the D-073 way a rotate
    /// targets a store-made workspace.
    async fn session_role_on(
        router: &Router,
        store: &Memory,
        ws: &durable_accounts::Workspace,
        email: &str,
        role: Role,
    ) -> (String, String, String) {
        signup_and_verify(router, store, email, "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(router, email, "a-long-enough-passphrase").await;
        let acc = store
            .account_by_email(email)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("account"));
        let token = mint_invite_token();
        let now = observe::wall_ms() as i64;
        let invite = store
            .create_invite(
                ws.id,
                email,
                role,
                acc.id,
                &sha256(token.as_bytes()),
                now + 60_000,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(invite.is_some(), "the invite row must mint");
        let seated = store
            .accept_invite(&sha256(token.as_bytes()), acc.id, now)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(seated.is_some(), "the accept must seat the row");
        (cookie, csrf, ws.public_id.to_string())
    }

    async fn session_admin_on(
        router: &Router,
        store: &Memory,
        ws: &durable_accounts::Workspace,
        email: &str,
    ) -> (String, String, String) {
        session_role_on(router, store, ws, email, Role::Admin).await
    }

    /// Create `name` as `cookie`'s workspace; returns its public_id.
    async fn make_ws(router: &Router, cookie: &str, name: &str) -> String {
        let (st, _, v) = raw(
            router,
            "POST",
            "/workspaces",
            &[("cookie", cookie.to_string())],
            Some(json!({"name": name})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["role"], "owner");
        assert!(v["handler_secret"].as_str().is_some_and(|s| s.len() == 64));
        v["public_id"].as_str().unwrap_or_default().to_string()
    }

    /// Invite `email` to `pid` as `role` under `cookie`/`csrf`; returns
    /// the mailed token string from the one-time invite_url.
    async fn make_invite(
        router: &Router,
        cookie: &str,
        csrf: &str,
        pid: &str,
        email: &str,
        role: &str,
    ) -> (StatusCode, String, String) {
        let (st, _, v) = raw(
            router,
            "POST",
            &format!("/workspaces/{pid}/members"),
            &[
                ("cookie", cookie.to_string()),
                ("x-durable-csrf", csrf.to_string()),
            ],
            Some(json!({"email": email, "role": role})),
        )
        .await;
        let iid = v["invite"]["public_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let url = v["invite_url"].as_str().unwrap_or_default().to_string();
        let token = url.split("token=").nth(1).unwrap_or_default().to_string();
        (st, iid, token)
    }

    fn session_headers(cookie: &str, csrf: &str) -> [(&'static str, String); 2] {
        [
            ("cookie", cookie.to_string()),
            ("x-durable-csrf", csrf.to_string()),
        ]
    }

    #[tokio::test]
    async fn s33_workspace_lifecycle() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;
        // Lists it as owner; GET shows the same role.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/workspaces",
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["workspaces"][0]["role"], "owner");
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}"),
            &[("cookie", cookie.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let (st, _, v) = raw(
            &h.router,
            "PATCH",
            &format!("/workspaces/{pid}"),
            &session_headers(&cookie, &csrf),
            Some(json!({"name": "beta"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // Unverified account cannot create.
        signup_only(&h.router, "u@u.co", "a-long-enough-passphrase").await;
        let (uc, _) = login_only(&h.router, "u@u.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/workspaces",
            &[("cookie", uc)],
            Some(json!({"name": "nope"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "email_unverified");
    }

    /// idor-workspace: a member of A on B's pid answers 404 — never a
    /// hint the workspace exists.
    #[tokio::test]
    async fn s33_idor_workspace() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "a@a.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "b@b.co", "a-long-enough-passphrase").await;
        let (ac, _) = login_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        let (bc, _) = login_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        let pid_b = make_ws(&h.router, &bc, "bspace").await;
        let (st, _, _) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid_b}"),
            &[("cookie", ac.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        // Same answer for a pid that does not exist at all.
        let ghost = uuid::Uuid::now_v7();
        let (st2, _, _) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{ghost}"),
            &[("cookie", ac)],
            None,
        )
        .await;
        assert_eq!(st, st2, "non-member and missing must be indistinguishable");
    }

    /// ws-header-path-mismatch: a header naming another workspace is a
    /// 400; matching the path is fine.
    #[tokio::test]
    async fn s33_ws_header_path_mismatch() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, _) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;
        let other = make_ws(&h.router, &cookie, "beta").await;
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}"),
            &[
                ("cookie", cookie.clone()),
                ("x-durable-workspace", other.clone()),
            ],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(v["code"], "workspace_mismatch");
        let (st, _, _) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}"),
            &[
                ("cookie", cookie.clone()),
                ("x-durable-workspace", pid.clone()),
            ],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
    }

    /// child-id-idor, one case per child kind under the path: a foreign
    /// invite and a foreign member addressed under one's own pid answer
    /// 404 and leave the foreign row untouched.
    #[tokio::test]
    async fn s33_child_id_idor() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "a@a.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "b@b.co", "a-long-enough-passphrase").await;
        let (ac, acsrf) = login_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        let (bc, bcsrf) = login_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        let pid_a = make_ws(&h.router, &ac, "aspace").await;
        let pid_b = make_ws(&h.router, &bc, "bspace").await;
        // B holds a live invite for c@c.co; A tries to revoke it under A's pid.
        let (ist, iid_b, _) = make_invite(&h.router, &bc, &bcsrf, &pid_b, "c@c.co", "member").await;
        assert_eq!(ist, StatusCode::OK);
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid_a}/invites/{iid_b}"),
            &session_headers(&ac, &acsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        // B's invite is untouched — still listed for B.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid_b}/invites"),
            &[("cookie", bc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["invites"][0]["public_id"], iid_b);
        // Same for a member row: A tries to remove B's owner under A's pid.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid_b}/members"),
            &[("cookie", bc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let owner_pid = v["members"][0]["account"]["public_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid_a}/members/{owner_pid}"),
            &session_headers(&ac, &acsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid_b}/members"),
            &[("cookie", bc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            v["members"].as_array().map(|m| m.len()),
            Some(1),
            "foreign member row unchanged"
        );
    }

    /// role-escalation: a viewer cannot PATCH a member, a member cannot
    /// DELETE the workspace, and an admin cannot demote the owner.
    #[tokio::test]
    async fn s33_role_escalation() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "v@v.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "m@m.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "x@x.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        // Seat them: viewer, member, admin — via real invites.
        for (email, role) in [
            ("v@v.co", "viewer"),
            ("m@m.co", "member"),
            ("x@x.co", "admin"),
        ] {
            let (ist, _, token) = make_invite(&h.router, &oc, &ocsrf, &pid, email, role).await;
            assert_eq!(ist, StatusCode::OK);
            let (c, csrf) = login_only(&h.router, email, "a-long-enough-passphrase").await;
            let (st, _, _) = raw(
                &h.router,
                "POST",
                "/invites/accept",
                &[("cookie", c), ("x-durable-csrf", csrf)],
                Some(json!({"token": token})),
            )
            .await;
            assert_eq!(st, StatusCode::OK);
        }
        let (vc, vcsrf) = login_only(&h.router, "v@v.co", "a-long-enough-passphrase").await;
        let (mc, mcsrf) = login_only(&h.router, "m@m.co", "a-long-enough-passphrase").await;
        let (xc, xcsrf) = login_only(&h.router, "x@x.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}/members"),
            &[("cookie", vc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let owner_pid = v["members"]
            .as_array()
            .unwrap_or_else(|| panic!("{v}"))
            .iter()
            .find(|m| m["role"] == "owner")
            .unwrap_or_else(|| panic!("{v}"))["account"]["public_id"]
            .as_str()
            .unwrap_or_else(|| panic!("{v}"))
            .to_string();
        // Viewer PATCHes a member → 403.
        let (st, _, _) = raw(
            &h.router,
            "PATCH",
            &format!("/workspaces/{pid}/members/{owner_pid}"),
            &session_headers(&vc, &vcsrf),
            Some(json!({"role": "viewer"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        // Member DELETEs the workspace → 403.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}"),
            &session_headers(&mc, &mcsrf),
            Some(json!({"confirm": "delete alpha"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        // Admin demotes the owner → 409 last_owner.
        let (st, _, v) = raw(
            &h.router,
            "PATCH",
            &format!("/workspaces/{pid}/members/{owner_pid}"),
            &session_headers(&xc, &xcsrf),
            Some(json!({"role": "member"})),
        )
        .await;
        assert_eq!(st, StatusCode::CONFLICT, "{v}");
        assert_eq!(v["code"], "last_owner");
    }

    /// invite-hijack: a session whose email is not the invite's bound
    /// address answers 403 invite_email_mismatch; the invite stays live.
    #[tokio::test]
    async fn s33_invite_hijack() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "n@n.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "w@w.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (ist, _, token) = make_invite(&h.router, &oc, &ocsrf, &pid, "w@w.co", "admin").await;
        assert_eq!(ist, StatusCode::OK);
        // The wrong account presents the token → 403, invite unconsumed.
        let (nc, ncsrf) = login_only(&h.router, "n@n.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/invites/accept",
            &session_headers(&nc, &ncsrf),
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "invite_email_mismatch");
        // The bound account accepts → member with the invited role.
        let (wc, wcsrf) = login_only(&h.router, "w@w.co", "a-long-enough-passphrase").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/invites/accept",
            &session_headers(&wc, &wcsrf),
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["workspace"]["role"], "admin");
    }

    /// last-owner: removing the owner answers 409 and the row stays.
    #[tokio::test]
    async fn s33_last_owner() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}/members"),
            &[("cookie", oc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let owner_pid = v["members"][0]["account"]["public_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let (st, _, v) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}/members/{owner_pid}"),
            &session_headers(&oc, &ocsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::CONFLICT, "{v}");
        assert_eq!(v["code"], "last_owner");
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}/members"),
            &[("cookie", oc)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["members"].as_array().map(|m| m.len()), Some(1));
    }

    /// deleted-ws-key: a key of a deleted workspace answers 401 on the
    /// very next request, even though it was granted moments ago; the
    /// workspace's member surface answers 410.
    #[tokio::test]
    async fn s33_deleted_ws_key() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        // A store key for this ws, warmed into the key cache.
        let ws = store
            .workspace_by_public_id(uuid::Uuid::parse_str(&pid).unwrap_or_else(|e| panic!("{e}")))
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("ws missing"));
        let key = format!("dr_{}", "f".repeat(48));
        store
            .create_key(
                ws.id,
                "k",
                Permission::Write,
                &sha256(key.as_bytes()),
                "dr_f",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key), None).await;
        assert_eq!(st, StatusCode::OK, "key live before delete");
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}"),
            &session_headers(&oc, &ocsrf),
            Some(json!({"confirm": "delete alpha"})),
        )
        .await;
        assert_eq!(st, StatusCode::NO_CONTENT);
        // The cached grant is gone — 401 on this same request.
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key), None).await;
        assert_eq!(
            st,
            StatusCode::UNAUTHORIZED,
            "deleted ws's key is dead at once"
        );
        // Members see 410, outsiders still see nothing.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}"),
            &[("cookie", oc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::GONE, "{v}");
        assert_eq!(v["code"], "workspace_closed");
        // And it is gone from the list.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/workspaces",
            &[("cookie", oc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["workspaces"].as_array().map(|w| w.len()), Some(0), "{v}");
    }

    // ---- S3-5: member-facing keys -----------------------------------

    /// Create a `write` key in `pid` as `cookie`; returns `(public_id,
    /// plaintext)` — the plaintext appears in exactly this response.
    async fn make_key(
        router: &Router,
        cookie: &str,
        csrf: &str,
        pid: &str,
        perm: &str,
    ) -> (String, String) {
        let (st, _, v) = raw(
            router,
            "POST",
            &format!("/workspaces/{pid}/keys"),
            &session_headers(cookie, csrf),
            Some(json!({"name": "cli", "permission": perm})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let key = v["key"].as_str().unwrap_or_default().to_string();
        assert!(key.starts_with("drk_"), "new keys are tagged: {key}");
        (v["public_id"].as_str().unwrap_or_default().to_string(), key)
    }

    /// Lifecycle: mint shows `drk_` once, the list carries prefix but
    /// never secret material, rotate kills the old key on the next call
    /// and DELETE does the same to the new one.
    #[tokio::test]
    async fn s35_key_lifecycle() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (key_pid, key) = make_key(&h.router, &oc, &ocsrf, &pid, "write").await;
        // The list shows prefix, never the secret.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}/keys"),
            &[("cookie", oc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["keys"][0]["prefix"], &key[..11]);
        let body = serde_json::to_string(&v).unwrap_or_default();
        assert!(
            !body.contains(&key[4..]),
            "list leaked key material: {body}"
        );
        // The key works on the data plane.
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key), None).await;
        assert_eq!(st, StatusCode::OK);
        // Rotate: new secret once; the OLD key 401s on the next call.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            &format!("/workspaces/{pid}/keys/{key_pid}/rotate"),
            &session_headers(&oc, &ocsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let key2 = v["key"].as_str().unwrap_or_default().to_string();
        let new_pid = v["public_id"].as_str().unwrap_or_default().to_string();
        assert!(key2.starts_with("drk_") && key2 != key);
        assert_ne!(new_pid, key_pid, "rotate mints a fresh row");
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key), None).await;
        assert_eq!(
            st,
            StatusCode::UNAUTHORIZED,
            "rotated-out key must 401 at once"
        );
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key2), None).await;
        assert_eq!(st, StatusCode::OK);
        // Delete: the new key 401s on its next call too.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}/keys/{new_pid}"),
            &session_headers(&oc, &ocsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NO_CONTENT);
        let (st, _) = call(&h.router, "GET", "/jobs", Some(&key2), None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "deleted key must 401 at once");
        // And a second delete is a 404 indistinguishable from IDOR.
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}/keys/{new_pid}"),
            &session_headers(&oc, &ocsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
    }

    /// key-self-escalation: a data-plane key — even write — cannot reach
    /// any member route; those need a session cookie.
    #[tokio::test]
    async fn s35_key_self_escalation() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (_, key) = make_key(&h.router, &oc, &ocsrf, &pid, "write").await;
        for (method, uri) in [
            ("GET", "/workspaces".to_string()),
            ("POST", "/workspaces".to_string()),
            ("GET", format!("/workspaces/{pid}/keys")),
            ("POST", format!("/workspaces/{pid}/keys")),
            ("GET", format!("/workspaces/{pid}/members")),
            ("DELETE", format!("/workspaces/{pid}")),
            ("GET", "/auth/session".to_string()),
            ("GET", "/me".to_string()),
        ] {
            let (st, _) = call(&h.router, method, &uri, Some(&key), None).await;
            assert_eq!(
                st,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} answered {st} to a bare API key"
            );
        }
    }

    /// cross-ws-key-create: a member of A POSTing a key under B's pid
    /// answers 404 — B's existence is not confirmed.
    #[tokio::test]
    async fn s35_cross_ws_key_create() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "a@a.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "b@b.co", "a-long-enough-passphrase").await;
        let (ac, acsrf) = login_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        let (bc, _) = login_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        make_ws(&h.router, &ac, "aspace").await;
        let pid_b = make_ws(&h.router, &bc, "bspace").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            &format!("/workspaces/{pid_b}/keys"),
            &session_headers(&ac, &acsrf),
            Some(json!({"name": "stolen", "permission": "write"})),
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        // B's key list is untouched.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid_b}/keys"),
            &[("cookie", bc)],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(v["keys"].as_array().map(|k| k.len()), Some(0));
    }

    /// Roles on the key routes: a viewer may list but POSTs/DELETEs are
    /// 403; a member (non-admin) is refused the same way.
    #[tokio::test]
    async fn s35_key_roles() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        signup_and_verify(&h.router, &store, "v@v.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (key_pid_owner, _) = make_key(&h.router, &oc, &ocsrf, &pid, "write").await;
        // Seat v@v.co as a viewer via a real invite.
        let (ist, _, token) = make_invite(&h.router, &oc, &ocsrf, &pid, "v@v.co", "viewer").await;
        assert_eq!(ist, StatusCode::OK);
        let (vc, vcsrf) = login_only(&h.router, "v@v.co", "a-long-enough-passphrase").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/invites/accept",
            &session_headers(&vc, &vcsrf),
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        // Viewer lists.
        let (st, _, _) = raw(
            &h.router,
            "GET",
            &format!("/workspaces/{pid}/keys"),
            &[("cookie", vc.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        // Viewer cannot mint or revoke.
        let (st, _, _) = raw(
            &h.router,
            "POST",
            &format!("/workspaces/{pid}/keys"),
            &session_headers(&vc, &vcsrf),
            Some(json!({"name": "esc", "permission": "write"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        let (st, _, _) = raw(
            &h.router,
            "DELETE",
            &format!("/workspaces/{pid}/keys/{key_pid_owner}"),
            &session_headers(&vc, &vcsrf),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
    }

    /// last-used-amplification: N verifies through the hot path coalesce
    /// to at most one last_used UPDATE per key per flush (D-071).
    #[tokio::test]
    async fn s35_last_used_amplification() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (_, key) = make_key(&h.router, &oc, &ocsrf, &pid, "write").await;
        // N verifies — cached or not, each lands <=1 pending touch.
        for _ in 0..8 {
            let (st, _) = call(&h.router, "GET", "/jobs", Some(&key), None).await;
            assert_eq!(st, StatusCode::OK);
        }
        let pending = h.app.keyring.drain_key_touches();
        assert_eq!(pending.len(), 1, "8 verifies coalesce to one touch");
        store
            .touch_keys(&pending)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let ws = store
            .workspace_by_public_id(uuid::Uuid::parse_str(&pid).unwrap_or_else(|e| panic!("{e}")))
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("ws"));
        let keys = store
            .list_keys(ws.id)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(keys[0].last_used_at_ms.is_some(), "the flush landed");
    }

    // ---- S3-6: handlers management + the builtin:echo test call ------

    /// A session caller on a flat route names its workspace in
    /// `x-durable-workspace`.
    fn ws_headers(cookie: &str, csrf: &str, pid: &str) -> [(&'static str, String); 3] {
        [
            ("cookie", cookie.to_string()),
            ("x-durable-csrf", csrf.to_string()),
            ("x-durable-workspace", pid.to_string()),
        ]
    }

    /// The D-070 probe on the in-process builtin: a member registers
    /// `builtin:echo` by name, the test call echoes the input in-process
    /// (no network hop), and a ws write key can probe the same handler.
    #[tokio::test]
    async fn s36_testcall_echo() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;

        let (st, _, v) = raw(
            &h.router,
            "PUT",
            "/handlers/builtin:echo",
            &ws_headers(&cookie, &csrf, &pid),
            Some(json!({"url": "builtin:echo"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // GET lists it without any secret material.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/handlers",
            &[
                ("cookie", cookie.clone()),
                ("x-durable-workspace", pid.clone()),
            ],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["handlers"][0]["name"], "builtin:echo");
        assert!(v["handlers"][0].get("handler_secret").is_none(), "{v}");

        // Session probe: echoes the input, bounded excerpt, no run.
        let input = base64::engine::general_purpose::STANDARD.encode(b"ping-body");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handlers/builtin:echo/test",
            &ws_headers(&cookie, &csrf, &pid),
            Some(json!({"input_b64": input})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["status"], "ok", "{v}");
        assert!(v["latency_ms"].is_number(), "{v}");
        let excerpt = base64::engine::general_purpose::STANDARD
            .decode(v["reply_excerpt_b64"].as_str().unwrap_or_default())
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(excerpt, b"ping-body");
        // A write key probes the same armed handler.
        let (_, key) = make_key(&h.router, &cookie, &csrf, &pid, "write").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handlers/builtin:echo/test",
            &[("authorization", format!("Bearer {key}"))],
            Some(json!({"input_b64": input})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["status"], "ok", "{v}");
        // The row's test columns updated: GET shows last_test_status ok.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/handlers",
            &[("authorization", format!("Bearer {key}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["handlers"][0]["health"]["last_test_status"], "ok", "{v}");
        assert!(
            v["handlers"][0]["health"]["last_test_at_ms"].is_number(),
            "{v}"
        );
    }

    /// ssrf-testcall: a private target is refused at registration —
    /// nothing is ever armed, so zero packets can reach it — and a test
    /// call on an unarmed name answers 404.
    #[tokio::test]
    async fn s36_ssrf_testcall() {
        let store = Arc::new(Memory::new());
        let h = harness_strict(Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;

        for target in [
            "http://127.0.0.1:9/h",
            "http://169.254.169.254/latest",
            "http://[::1]:9/h",
            "http://10.0.0.1/h",
        ] {
            let (st, _, v) = raw(
                &h.router,
                "PUT",
                "/handlers/probe",
                &ws_headers(&cookie, &csrf, &pid),
                Some(json!({"url": target})),
            )
            .await;
            assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY, "{target}: {v}");
            assert_eq!(v["code"], "local_target_forbidden", "{target}: {v}");
        }
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/handlers",
            &[
                ("cookie", cookie.clone()),
                ("x-durable-workspace", pid.clone()),
            ],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["handlers"].as_array().map(|x| x.len()), Some(0), "{v}");
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handlers/probe/test",
            &ws_headers(&cookie, &csrf, &pid),
            Some(json!({"input_b64": "aGk="})),
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    }

    /// testcall-abuse: the per-client bound strikes before the
    /// workspace's hourly one — the 11th call in a burst 429s with a
    /// Retry-After-shaped refusal, and an already-registered handler is
    /// unaffected.
    #[tokio::test]
    async fn s36_testcall_abuse() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;
        let (_, key) = make_key(&h.router, &cookie, &csrf, &pid, "write").await;
        let (st, _, v) = raw(
            &h.router,
            "PUT",
            "/handlers/builtin:echo",
            &[("authorization", format!("Bearer {key}"))],
            Some(json!({"url": "builtin:echo"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        for i in 0..10 {
            let (st, _, v) = raw(
                &h.router,
                "POST",
                "/handlers/builtin:echo/test",
                &[("authorization", format!("Bearer {key}"))],
                Some(json!({"input_b64": "aGk="})),
            )
            .await;
            assert_eq!(st, StatusCode::OK, "call {i}: {v}");
        }
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handlers/builtin:echo/test",
            &[("authorization", format!("Bearer {key}"))],
            Some(json!({"input_b64": "aGk="})),
        )
        .await;
        assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{v}");
        assert_eq!(v["code"], "test_call_limited", "{v}");
    }

    /// secret-in-get: the workspace signing secret is shown exactly once
    /// (create/rotate); no GET surface can be made to carry it.
    #[tokio::test]
    async fn s36_secret_in_get() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;
        // Rotate once so the live secret is a value we hold.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handler-secret",
            &ws_headers(&cookie, &csrf, &pid),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let secret = v["secret"].as_str().unwrap_or_default().to_string();
        assert!(secret.len() >= 32, "{v}");
        for uri in ["/handlers", "/workspaces", &format!("/workspaces/{pid}")] {
            let (st, _, v) = raw(
                &h.router,
                "GET",
                uri,
                &[
                    ("cookie", cookie.clone()),
                    ("x-durable-workspace", pid.clone()),
                ],
                None,
            )
            .await;
            assert_eq!(st, StatusCode::OK, "{uri}: {v}");
            assert!(
                !v.to_string().contains(&secret),
                "{uri} carried the signing secret"
            );
        }
    }

    /// handler-secret-key: data-plane keys — even write — can never
    /// rotate the signing secret; a session admin can.
    #[tokio::test]
    async fn s36_handler_secret_key() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;
        let (_, key) = make_key(&h.router, &cookie, &csrf, &pid, "write").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/handler-secret",
            &[("authorization", format!("Bearer {key}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        // A read key gets the same refusal, not a weaker one.
        let (_, rkey) = make_key(&h.router, &cookie, &csrf, &pid, "read").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/handler-secret",
            &[("authorization", format!("Bearer {rkey}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
    }

    /// Member roles on the flat surface: a member PUTs and GETs; a
    /// viewer can GET but not PUT/DELETE; a non-member session gets 404
    /// (the pid never leaks).
    #[tokio::test]
    async fn s36_member_handlers() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (cookie, csrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &cookie, "alpha").await;

        signup_and_verify(&h.router, &store, "m@m.co", "a-long-enough-passphrase").await;
        let (mc, mcsrf) = login_only(&h.router, "m@m.co", "a-long-enough-passphrase").await;
        let (_, _, token) = make_invite(&h.router, &cookie, &csrf, &pid, "m@m.co", "member").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/invites/accept",
            &[("cookie", mc.clone()), ("x-durable-csrf", mcsrf.clone())],
            Some(json!({"token": token})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let (st, _, v) = raw(
            &h.router,
            "PUT",
            "/handlers/builtin:echo",
            &ws_headers(&mc, &mcsrf, &pid),
            Some(json!({"url": "builtin:echo"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");

        signup_and_verify(&h.router, &store, "v@v.co", "a-long-enough-passphrase").await;
        let (vc, vcsrf) = login_only(&h.router, "v@v.co", "a-long-enough-passphrase").await;
        let (_, _, vtok) = make_invite(&h.router, &cookie, &csrf, &pid, "v@v.co", "viewer").await;
        let (st, _, _) = raw(
            &h.router,
            "POST",
            "/invites/accept",
            &[("cookie", vc.clone()), ("x-durable-csrf", vcsrf.clone())],
            Some(json!({"token": vtok})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/handlers",
            &[("cookie", vc.clone()), ("x-durable-workspace", pid.clone())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let (st, _, v) = raw(
            &h.router,
            "PUT",
            "/handlers/viewer-put",
            &ws_headers(&vc, &vcsrf, &pid),
            Some(json!({"url": "builtin:echo"})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        let (st, _, v) = raw(
            &h.router,
            "DELETE",
            "/handlers/builtin:echo",
            &ws_headers(&vc, &vcsrf, &pid),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");

        // A session with no membership in pid: every flat route 404s the
        // workspace, never 403s the route.
        signup_and_verify(&h.router, &store, "x@x.co", "a-long-enough-passphrase").await;
        let (xc, xcsrf) = login_only(&h.router, "x@x.co", "a-long-enough-passphrase").await;
        for (method, uri) in [
            ("GET", "/handlers"),
            ("PUT", "/handlers/x"),
            ("DELETE", "/handlers/builtin:echo"),
        ] {
            let (st, _, v) = raw(
                &h.router,
                method,
                uri,
                &ws_headers(&xc, &xcsrf, &pid),
                if method == "PUT" {
                    Some(json!({"url": "builtin:echo"}))
                } else {
                    None
                },
            )
            .await;
            assert_eq!(st, StatusCode::NOT_FOUND, "{method} {uri}: {v}");
        }
    }

    // ---- finding 18: --ops-listen gates env keys to the ops socket ----

    /// With the gate armed, an env key on the public socket answers 401
    /// on every auth path it could take — flat member routes,
    /// `Authorized` admin routes, read routes — indistinguishable from a
    /// bad bearer, while the same key on the ops socket works and
    /// store-key/session callers are unaffected on either side.
    #[tokio::test]
    async fn ops_listen_confines_env_keys_to_the_ops_socket() {
        let store = Arc::new(Memory::new());
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let key_a = "dr_a51a51a51a51a51a51a51a51a51a51a51a51a51a51a51a51";
        store
            .create_key(
                a.id,
                "k",
                Permission::Write,
                &sha256(key_a.as_bytes()),
                "dr_a",
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let h = harness_ops(Some(store));
        let env = [("authorization", format!("Bearer {KEY}"))];

        // Public side: env key is just a bad bearer — 401 everywhere it
        // could authenticate.
        for (method, uri) in [
            ("GET", "/whoami"),
            ("GET", "/admin/workspaces"),
            ("GET", "/handlers"),
            ("POST", "/handler-secret"),
        ] {
            let (st, _, _) = raw(&h.router, method, uri, &env, None).await;
            assert_eq!(st, StatusCode::UNAUTHORIZED, "{method} {uri} on --listen");
        }
        // The ops socket answers the same key normally.
        let (st, _, v) = raw_ops(&h.router, "GET", "/whoami", &env, None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let (st, _, v) = raw_ops(&h.router, "GET", "/admin/workspaces", &env, None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let (st, _, v) = raw_ops(&h.router, "GET", "/handlers", &env, None).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // The read env key is confined the same way.
        let renv = [("authorization", format!("Bearer {READ_KEY}"))];
        let (st, _, _) = raw(&h.router, "GET", "/whoami", &renv, None).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
        let (st, _, _) = raw_ops(&h.router, "GET", "/whoami", &renv, None).await;
        assert_eq!(st, StatusCode::OK);
        // A store key is unaffected on the public side (data plane).
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/handlers",
            &[("authorization", format!("Bearer {key_a}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
    }

    /// Without the flag nothing changes — env keys work on --listen as
    /// always (the self-host shape).
    #[tokio::test]
    async fn no_ops_listen_leaves_env_keys_as_before() {
        let h = harness_store(false, None, None);
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/whoami",
            &[("authorization", format!("Bearer {KEY}"))],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
    }

    /// Boot refuses a non-loopback --ops-listen and a non-address, and
    /// takes every loopback spelling.
    #[test]
    fn ops_listen_must_be_loopback() {
        for good in ["127.0.0.1:9100", "[::1]:9100", "127.42.0.9:1"] {
            assert!(
                check_ops_listen(&Some(good.to_string())).is_ok(),
                "{good} must be accepted"
            );
        }
        for bad in [
            "0.0.0.0:9100",
            "8.8.8.8:9100",
            "[::]:9100",
            "junk",
            "127.0.0.1",
        ] {
            assert!(
                check_ops_listen(&Some(bad.to_string())).is_err(),
                "{bad} must be refused"
            );
        }
        assert!(check_ops_listen(&None).is_ok());
    }

    /// `DURABLE_INVITE_ONLY` is parsed, not presence-checked: every on
    /// spelling gates, every off spelling doesn't, junk refuses to boot.
    #[test]
    fn invite_only_env_parses_values() {
        for (v, want) in [
            ("1", true),
            ("true", true),
            ("TRUE", true),
            ("yes", true),
            ("on", true),
            (" ON ", true),
            ("0", false),
            ("false", false),
            ("no", false),
            ("off", false),
            ("", false),
            ("  ", false),
        ] {
            let got = invite_only_value(Some(v));
            assert_eq!(got.ok(), Some(want), "DURABLE_INVITE_ONLY={v:?}");
        }
        for bad in ["2", "enable", "enabled", "y", "n", "t", "f"] {
            assert!(
                invite_only_value(Some(bad)).is_err(),
                "DURABLE_INVITE_ONLY={bad:?} must refuse the boot"
            );
        }
        assert_eq!(invite_only_value(None).ok(), Some(false));
    }

    // ---- first-deploy blocker: sessions reach the run plane ----------

    /// Every run-plane route answers a session exactly like a flat
    /// route: x-durable-workspace names the ws, member roles decide,
    /// foreign workspaces are 404, never data.
    #[tokio::test]
    async fn s3d_sessions_on_the_run_plane() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        // Owner A + workspace A; owner B + workspace B.
        signup_and_verify(&h.router, &store, "a@a.co", "a-long-enough-passphrase").await;
        let (ac, acsrf) = login_only(&h.router, "a@a.co", "a-long-enough-passphrase").await;
        let pid_a = make_ws(&h.router, &ac, "alpha").await;
        signup_and_verify(&h.router, &store, "b@b.co", "a-long-enough-passphrase").await;
        let (bc, _) = login_only(&h.router, "b@b.co", "a-long-enough-passphrase").await;
        let pid_b = make_ws(&h.router, &bc, "beta").await;
        let ws_hdr = |pid: &str| ("x-durable-workspace", pid.to_string());
        let sess = |c: &str, csrf: &str, pid: &str| {
            vec![
                ("cookie", c.to_string()),
                ("x-durable-csrf", csrf.to_string()),
                ws_hdr(pid),
            ]
        };
        // Arm `builtin:echo` on ws A via the session — flat handler auth.
        let echo = |k: &str| {
            json!({
                "op_key": k,
                "spec": {
                    "version": 1,
                    "steps": [{ "name": "a", "handler": "builtin:echo" }],
                    "input_b64": "aGk=",
                },
            })
        };
        let (st, _, v) = raw(
            &h.router,
            "PUT",
            "/handlers/builtin:echo",
            &sess(&ac, &acsrf, &pid_a),
            Some(json!({"url": "builtin:echo"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        // Session member (owner) submits.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/jobs",
            &sess(&ac, &acsrf, &pid_a),
            Some(echo("s-1")),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let run = v["run_id"].as_str().unwrap_or_default().to_string();
        assert!(!run.is_empty());
        // Every read answers for the session owner.
        for path in [
            "/jobs".to_string(),
            format!("/jobs/{run}"),
            format!("/jobs/{run}/spec"),
            format!("/jobs/{run}/events"),
            "/metrics".to_string(),
            "/events".to_string(),
            "/diagnostics".to_string(),
        ] {
            let (st, _, v) = raw(&h.router, "GET", &path, &sess(&ac, &acsrf, &pid_a), None).await;
            assert_eq!(st, StatusCode::OK, "{path}: {v}");
        }
        // No header at all -> 400 workspace_required (D-062).
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/jobs",
            &[("cookie", ac.to_string())],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(v["code"], "workspace_required");
        // A's session naming B's ws -> 404 on every route, never B data.
        for (method, path) in [
            ("GET", "/jobs".to_string()),
            ("POST", "/jobs".to_string()),
            ("GET", format!("/jobs/{run}")),
            ("GET", format!("/jobs/{run}/spec")),
            ("POST", format!("/jobs/{run}/cancel")),
            ("GET", format!("/jobs/{run}/events")),
            ("GET", "/metrics".to_string()),
            ("GET", "/events".to_string()),
            ("GET", "/diagnostics".to_string()),
        ] {
            let (st, _, v) = raw(
                &h.router,
                method,
                &path,
                &sess(&ac, &acsrf, &pid_b),
                if method == "POST" {
                    Some(echo("x"))
                } else {
                    None
                },
            )
            .await;
            assert_eq!(st, StatusCode::NOT_FOUND, "{method} {path}: {v}");
        }
        // The owner cancels a parked run; a viewer may not submit/cancel.
        let ws_a = store
            .workspace_by_public_id(pid_a.parse().unwrap_or_else(|_| panic!("pid")))
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("ws"));
        let (vc, vcsrf, _) =
            session_role_on(&h.router, &store, &ws_a, "v@v.co", Role::Viewer).await;
        for (method, path) in [
            ("POST", "/jobs".to_string()),
            ("POST", format!("/jobs/{run}/cancel")),
        ] {
            let (st, _, v) = raw(
                &h.router,
                method,
                &path,
                &sess(&vc, &vcsrf, &pid_a),
                Some(echo("x")),
            )
            .await;
            assert_eq!(st, StatusCode::FORBIDDEN, "{method} {path}: {v}");
        }
        // A member+ session without the CSRF header -> 403 csrf_failed.
        let (st, _, v) = raw(
            &h.router,
            "POST",
            "/jobs",
            &[
                ("cookie", ac.to_string()),
                ("x-durable-workspace", pid_a.clone()),
            ],
            Some(echo("x")),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(v["code"], "csrf_failed");
        // /diagnostics for a session is the ws view — the ops global's
        // `registry_stale` key is the tell it isn't.
        let (st, _, v) = raw(
            &h.router,
            "GET",
            "/diagnostics",
            &sess(&ac, &acsrf, &pid_a),
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(v.get("registry_stale").is_none(), "ops shape leaked: {v}");
        // Bearer keys behave exactly as before.
        let (st, _) = call(&h.router, "GET", "/jobs", Some(READ_KEY), None).await;
        assert_eq!(st, StatusCode::OK);
        let (st, _) = call(&h.router, "POST", "/jobs", Some(READ_KEY), Some(echo("x"))).await;
        assert_eq!(st, StatusCode::FORBIDDEN);
    }

    /// The invite carries a usable app link, in the response AND the
    /// mail: `{origin}/app#/accept-invite?token=dri_…` — a fragment, so
    /// the token never reaches a server log.
    #[tokio::test]
    async fn s3d_invite_link_is_an_app_fragment() {
        let store = Arc::new(Memory::new());
        let h = harness_store(false, None, Some(store.clone()));
        signup_and_verify(&h.router, &store, "o@o.co", "a-long-enough-passphrase").await;
        let (oc, ocsrf) = login_only(&h.router, "o@o.co", "a-long-enough-passphrase").await;
        let pid = make_ws(&h.router, &oc, "alpha").await;
        let (st, _, v) = raw(
            &h.router,
            "POST",
            &format!("/workspaces/{pid}/members"),
            &session_headers(&oc, &ocsrf),
            Some(json!({"email": "n@n.co", "role": "member"})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        let url = v["invite_url"].as_str().unwrap_or_default().to_string();
        assert!(
            url.starts_with("http://localhost/app#/accept-invite?token=dri_"),
            "invite_url: {url}"
        );
        // The mailed body carries the same link — the only other place
        // the token ever appears.
        let mail = store
            .outbox_claim(unix_ms(), 8, 60_000)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let invite_mail = mail
            .iter()
            .find(|m| m.mail.kind == "workspace-invite")
            .unwrap_or_else(|| panic!("invite mail queued"));
        assert!(
            invite_mail.mail.text.contains(&url),
            "mail body lacks the app link: {}",
            invite_mail.mail.text
        );
    }
}
