//! durable-server: the Axum coordinator. One engine, one writer, HTTP on
//! top — submit/inspect/cancel/compact/backup. Handlers are allowlisted
//! HTTP endpoints configured at startup; the registry is the trust edge.

mod addr_limits;

use addr_limits::{
    client_addr, ConnLimit, IpNet, RateLimit, TrustedProxies, DEFAULT_V6_PREFIX, MAX_LIMITED_ADDRS,
};
use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, FromRequestParts, Path as AxPath, Query, State},
    http::{header, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use durable_domain::{JobSpec, Millis, OperationKey, RunId, RunState, StepSpec};
use durable_engine::shell::{
    spawn, Call, Engine, Order, Outcome, Registry, ShellConfig, StepHandler,
};
use durable_engine::EngineError;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    /// HTTP handler registrations: name=URL (repeatable).
    #[arg(long = "handler", value_parser = parse_handler)]
    handlers: Vec<(String, String)>,
    /// Attempt timeout per step call, ms (bounded by domain limits).
    #[arg(long, default_value_t = 20_000)]
    step_timeout_ms: u64,
    /// Static inspection UI directory, served at /inspector (optional).
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
    /// Approximate bytes of all retained runs; beyond it submits are 503
    /// `at_capacity`. Default 512 MiB, sized for a 2 GB host (D-031).
    #[arg(long, default_value_t = durable_domain::Limits::default().max_index_bytes)]
    max_index_bytes: u64,
    /// Handler calls (attempts and probes) in flight at once.
    #[arg(long, default_value_t = 32)]
    max_in_flight: usize,
    /// Dispatches per scheduler tick (ticks every 50 ms, so the first-step
    /// dispatch rate is at most 20 × this per second).
    #[arg(long, default_value_t = 64)]
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
fn handler_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
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

fn with_call(req: reqwest::RequestBuilder, call: Option<Call>) -> reqwest::RequestBuilder {
    match call {
        Some(c) => req
            .header(RUN_ID_HEADER, c.run.0)
            .header(STEP_HEADER, c.step.0)
            .header(ATTEMPT_HEADER, c.attempt),
        None => req,
    }
}

impl HttpHandler {
    async fn call(&self, call: Option<Call>, input: &[u8], idem_key: &str) -> Outcome {
        let body = serde_json::json!({
            "input": input,
            "idem_key": idem_key,
        });
        let req = self
            .client
            .post(&self.url)
            .header("x-idempotency-key", idem_key)
            .json(&body)
            .timeout(self.timeout);
        let res = with_call(req, call).send().await;
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
            Err(e) if e.is_connect() => Outcome::Failed,
            Err(_) => Outcome::Lost, // genuinely unknown — reconcile
        }
    }

    /// Probe asks the handler whether the call under `idem_key` landed.
    /// `GET {url}?idem_key=…` → 200 means found; anything else is unknown
    /// → reported as not-found honestly (default trait behaviour).
    async fn ask(&self, call: Option<Call>, idem_key: &str) -> bool {
        let req = self
            .client
            .get(&self.url)
            .query(&[("idem_key", idem_key)])
            .timeout(self.timeout);
        with_call(req, call)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
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

#[derive(Clone)]
struct App {
    engine: Engine,
    api_key: ApiKey,
    /// A key that may only read; `None` if none is configured.
    read_key: Option<ApiKey>,
    /// Reads need no key (`--open-reads`, loopback listeners only).
    open_reads: bool,
    /// Canonical backup directory; `None` disables `/admin/backup`.
    backup_root: Option<Arc<Path>>,
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

/// Proof that the request carried the API key. Every mutating and admin
/// handler takes this as its first extractor, so it runs before the body
/// is parsed and the route cannot be reached without the key. The
/// read-only key is recognised and refused 403: it is valid, but not here.
struct Authorized;

impl FromRequestParts<App> for Authorized {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let Some(key) = bearer(parts) else {
            return Err(unauthorized());
        };
        if app.api_key.verify(key) {
            return Ok(Authorized);
        }
        if app.read_key.as_ref().is_some_and(|k| k.verify(key)) {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "this key may only read (DURABLE_READ_KEY); this route needs DURABLE_API_KEY",
            ));
        }
        Err(unauthorized())
    }
}

/// Proof that the request may read: the API key or the read-only key, or
/// the server runs with `--open-reads` (loopback only). Taken first by
/// every handler that shows runs (D-037).
struct ReadAuthorized;

impl FromRequestParts<App> for ReadAuthorized {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        // Open reads answer only requests addressed to a loopback name: a
        // page in the operator's browser can rebind its own name to
        // 127.0.0.1, and its requests then carry that name as Host (D-047).
        if app.open_reads && loopback_host(parts) {
            return Ok(ReadAuthorized);
        }
        match bearer(parts) {
            Some(key)
                if app.api_key.verify(key)
                    || app.read_key.as_ref().is_some_and(|k| k.verify(key)) =>
            {
                Ok(ReadAuthorized)
            }
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
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message, "code": self.code }));
        let mut res = (self.status, body).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            res.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                header::HeaderValue::from_static("Bearer"),
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
    _: Authorized,
    State(app): State<App>,
    J(req): J<SubmitReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let input = base64_decode(&req.spec.input_b64)
        .map_err(|m| ApiError::new(StatusCode::BAD_REQUEST, "bad_input", m))?;
    let spec = JobSpec {
        version: req.spec.version,
        steps: req.spec.steps,
        input,
    };
    let key = OperationKey(req.op_key);
    let run = match req.not_before {
        Some(at) => app.engine.submit_at(key, spec, at).await,
        None => app.engine.submit(key, spec).await,
    }?;
    Ok(Json(serde_json::json!({ "run_id": run.0 })))
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

async fn inspect(
    _: ReadAuthorized,
    State(app): State<App>,
    P(id): P<u64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let run = app
        .engine
        .inspect(RunId(id))
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such run"))?;
    Ok(Json(serde_json::json!({
        "id": run.id.0,
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
    _: Authorized,
    State(app): State<App>,
    P(id): P<u64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use base64::Engine as _;
    let run = app
        .engine
        .inspect(RunId(id))
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such run"))?;
    Ok(Json(serde_json::json!({
        "id": run.id.0,
        "key": run.key.0,
        "spec": {
            "version": run.spec.version,
            "steps": run.spec.steps,
            "input_b64": base64::engine::general_purpose::STANDARD.encode(&run.spec.input),
        },
    })))
}

async fn cancel(
    _: Authorized,
    State(app): State<App>,
    P(id): P<u64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    app.engine.cancel(RunId(id)).await?;
    Ok(Json(serde_json::json!({ "cancelled": id })))
}

#[derive(Deserialize)]
struct ListQuery {
    /// `asc` (default): oldest first, continue with `after`.
    /// `desc`: newest first, continue with `before`.
    order: Option<String>,
    /// Return runs with id greater than this (the previous `next_after`).
    after: Option<u64>,
    /// Newest first: runs with id less than this (the previous `next_before`).
    before: Option<u64>,
    /// Page size: default 100, clamped to 1..=1000.
    limit: Option<usize>,
}

async fn list(
    _: ReadAuthorized,
    State(app): State<App>,
    Q(q): Q<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
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
    let order = if newest {
        Order::Newest {
            before: q.before.map(RunId),
        }
    } else {
        Order::Oldest {
            after: q.after.map(RunId),
        }
    };
    let page = app.engine.list_in(order, q.limit.unwrap_or(100)).await?;
    let items: Vec<serde_json::Value> = page
        .runs
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id.0,
                "key": r.key.0,
                "state": state_name(&r.state),
                "steps": r.steps,
                "finished_at": r.finished_at,
            })
        })
        .collect();
    let cursor = if newest { "next_before" } else { "next_after" };
    let mut body = serde_json::json!({ "runs": items });
    body[cursor] = serde_json::json!(page.next.map(|id| id.0));
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
    _: Authorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let d = app.engine.diag().await?;
    Ok(Json(serde_json::to_value(d).unwrap_or_default()))
}

async fn compact(
    _: Authorized,
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, ApiError> {
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
    _: Authorized,
    State(app): State<App>,
    J(req): J<BackupReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
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

/// Every route. `/health`, `/ready` and the inspector's static files are
/// open (they show no runs). `GET /jobs` and `GET /jobs/{id}` take
/// `ReadAuthorized` (either key, or `--open-reads`); every mutating and
/// admin handler takes `Authorized`, so it is unreachable without the API
/// key.
fn router(app: App, ui_dir: Option<&Path>) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/jobs", post(submit).get(list))
        .route("/jobs/{id}", get(inspect))
        .route("/jobs/{id}/spec", get(spec))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/admin/compact", post(compact))
        .route("/admin/backup", post(backup))
        .route("/admin/diagnostics", get(diagnostics))
        // A submit is at most 64 KiB of input (base64: ~88 KiB) plus its
        // spec; nothing legitimate comes near this.
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(app);
    if let Some(ui) = ui_dir {
        let csp = header::HeaderValue::from_str(&inspector_csp(ui)).unwrap_or(
            header::HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
        );
        let ui_routes = Router::new()
            .nest_service(
                "/inspector",
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

/// The inspector's Content-Security-Policy: only its own files, and the
/// inline bootstrap script SvelteKit writes into `index.html` by hash (no
/// `'unsafe-inline'` scripts), calls to this origin only, no framing.
fn inspector_csp(ui: &Path) -> String {
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
            "durable-server: cannot read {}/index.html ({e}); the inspector will not run",
            ui.display()
        ),
    }
    format!(
        "default-src 'none'; script-src 'self'{hashes}; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; \
         form-action 'none'; frame-ancestors 'none'"
    )
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
    check_open_reads(&args.listen, args.open_reads)?;
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
        max_dispatch_per_tick: args.max_dispatch_per_tick.max(1),
        reply_timeout: Duration::from_millis(args.reply_timeout_ms.max(1)),
        limits: durable_domain::Limits {
            max_live_runs: args.max_live_runs,
            max_index_bytes: args.max_index_bytes,
            ..durable_domain::Limits::default()
        },
        ..Default::default()
    };
    let mut registry = Registry::default();
    let client = handler_client()?;
    for (name, url) in &args.handlers {
        registry.register(
            name,
            Arc::new(HttpHandler {
                client: client.clone(),
                url: url.clone(),
                timeout: Duration::from_millis(args.step_timeout_ms),
                max_output: cfg.limits.max_output_bytes,
            }),
        );
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
    let stopped = engine.clone();
    let app = App {
        engine,
        api_key,
        read_key,
        open_reads: args.open_reads,
        backup_root,
    };
    let router = router(app, args.ui_dir.as_deref());
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    eprintln!("durable-server listening on {}", args.listen);
    let (stop_http, http_stopped) = tokio::sync::oneshot::channel::<()>();
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
    };
    let serve = serve_http(listener, router, limits, http_stopped);
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
        }
    }
}

/// Serve `router` on `listener` until `stop` fires, then let the requests
/// in flight finish (D-045, D-049). Every connection holds one of
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
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> std::io::Result<()> {
    use hyper_util::rt::{TokioIo, TokioTimer};
    let permits = Arc::new(tokio::sync::Semaphore::new(limits.max_connections.max(1)));
    let (closing_tx, closing) = tokio::sync::watch::channel(false);
    let service = tower_http::timeout::RequestBodyDeadline::new(router, limits.body_deadline);
    let mut conns = tokio::task::JoinSet::new();
    let mut accept_failing = false;
    // Per-address bounds (D-052): the rate table is one token bucket per
    // client address in a bounded map; the connection count is per TCP
    // peer (at accept time no request — and so no trusted-proxy header —
    // has been read yet).
    let rate_limiter = (limits.rate_per_addr > 0.0).then(|| {
        Arc::new(std::sync::Mutex::new(RateLimit::new(
            limits.rate_per_addr,
            MAX_LIMITED_ADDRS,
            limits.v6_prefix,
        )))
    });
    let conn_limit = (limits.max_conns_per_addr > 0 || limits.max_conns_per_proxy > 0)
        .then(|| ConnLimit::new(limits.max_conns_per_addr, limits.max_conns_per_proxy));
    let trusted = limits.trusted_proxies.clone();
    let mut conn_cap_refused = false;
    loop {
        let permit = tokio::select! {
            p = permits.clone().acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break, // the semaphore is never closed
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
        // The per-peer connection cap: past it the socket is dropped at
        // accept — the client sees a refused connection, not a queue slot
        // held while one address spends them all.
        let conn_guard = match &conn_limit {
            Some(l) => match l.acquire(peer.ip(), trusted.trusts(peer.ip())) {
                Some(g) => {
                    conn_cap_refused = false;
                    Some(g)
                }
                None => {
                    if !conn_cap_refused {
                        eprintln!(
                            "durable-server: per-address connection cap reached; refusing {}",
                            peer.ip()
                        );
                        conn_cap_refused = true;
                    }
                    continue; // the semaphore permit returns with the socket
                }
            },
            None => None,
        };
        let io = TokioIo::new(StallGuard::new(stream, limits.write_stall));
        // The connection's `max_requests`-th answer says `Connection:
        // close`, so pipelining cannot keep one connection busy for ever.
        let svc = service.clone();
        let served = std::sync::atomic::AtomicUsize::new(0);
        let max_requests = limits.max_requests.max(1);
        let per_conn = Arc::new((svc, served));
        let peer_ip = peer.ip();
        let rate_limiter = rate_limiter.clone();
        let trusted = trusted.clone();
        let handler =
            hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let per_conn = per_conn.clone();
                let rate_limiter = rate_limiter.clone();
                let trusted = trusted.clone();
                async move {
                    // The per-address request rate. Refused before auth:
                    // a flood without a key is still a flood, and the
                    // check is cheaper than the work behind it.
                    if let Some(lim) = rate_limiter.as_ref() {
                        let client = client_addr(
                            peer_ip,
                            &trusted,
                            req.headers()
                                .get("cf-connecting-ip")
                                .and_then(|v| v.to_str().ok()),
                        );
                        let refused = lim
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .check(client, std::time::Instant::now())
                            .err();
                        if let Some(retry) = refused {
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
                            return Ok(res);
                        }
                    }
                    let (svc, served) = &*per_conn;
                    let n = served.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    let mut res = tower::ServiceExt::oneshot(svc.clone(), req).await?;
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
            .header_read_timeout(limits.header_timeout)
            .serve_connection(io, handler);
        let mut closing = closing.clone();
        let lifetime = limits.max_lifetime;
        conns.spawn(async move {
            let _permit = permit;
            let _conn_guard = conn_guard;
            let mut conn = std::pin::pin!(conn);
            // A connection error (a reset, a timed-out head, a stalled
            // write) ends only that connection.
            tokio::select! {
                _ = conn.as_mut() => return,
                _ = closing.wait_for(|c| *c) => {}
                () = tokio::time::sleep(lifetime) => {}
            }
            // Shutting down, or the connection is old: answer the request
            // in flight (bounded by the deadlines above), then close.
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
    use tower::ServiceExt as _;

    const KEY: &str = "test-key-0123456789abcdef0123456789abcdef";
    const READ_KEY: &str = "read-key-0123456789abcdef0123456789abcdef";

    struct Echo;
    #[async_trait::async_trait]
    impl StepHandler for Echo {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            Outcome::Checkpoint(b"ok".to_vec())
        }
    }

    struct Harness {
        router: Router,
        backups: tempfile::TempDir,
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
        let journal = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let backups = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("step", Arc::new(Echo));
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
        let app = App {
            engine,
            api_key: ApiKey::new(KEY),
            read_key: Some(ApiKey::new(READ_KEY)),
            open_reads: false,
            backup_root,
        };
        Harness {
            router: router(app, None),
            backups,
            _journal: journal,
        }
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
        assert_eq!(v["run_id"], 1);
        let (status, _) = call(&h.router, "GET", "/jobs/1", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/jobs", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/health", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/admin/diagnostics", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// Checklist (key scopes): the read-only key reads, and every
    /// mutating and admin route refuses it 403 — a valid key, wrong scope.
    #[tokio::test]
    async fn the_read_key_reads_and_nothing_else() {
        let h = harness(true);
        let mut later = submit_body("k");
        later["not_before"] = json!(9_999_999_999_999u64); // stays queued
        let (status, _) = call(&h.router, "POST", "/jobs", Some(KEY), Some(later)).await;
        assert_eq!(status, StatusCode::OK);
        for uri in ["/jobs", "/jobs/1"] {
            let (status, _) = call(&h.router, "GET", uri, Some(READ_KEY), None).await;
            assert_eq!(status, StatusCode::OK, "GET {uri}");
        }
        let routes: [(&str, &str, Option<Value>); 6] = [
            ("POST", "/jobs", Some(submit_body("r"))),
            ("GET", "/jobs/1/spec", None),
            ("POST", "/jobs/1/cancel", None),
            ("POST", "/admin/compact", None),
            ("POST", "/admin/backup", Some(json!({ "name": "b" }))),
            ("GET", "/admin/diagnostics", None),
        ];
        for (method, uri, body) in routes {
            let (status, v) = call(&h.router, method, uri, Some(READ_KEY), body).await;
            assert_eq!(
                (status, v["code"].clone()),
                (StatusCode::FORBIDDEN, json!("forbidden")),
                "{method} {uri}"
            );
        }
        let (_, v) = call(&h.router, "GET", "/jobs/1", Some(KEY), None).await;
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
        let id = v["run_id"].as_u64().unwrap_or_else(|| panic!("{v}"));
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
            App {
                engine,
                api_key: ApiKey::new(KEY),
                read_key: None,
                open_reads: true,
                backup_root: None,
            },
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
        let (status, _) = call(
            &h.router,
            "POST",
            "/jobs",
            Some(KEY),
            Some(submit_body("r")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        for uri in ["/jobs", "/jobs/1", "/jobs?order=desc&limit=5"] {
            for key in [None, Some("wrong")] {
                let (status, v) = call(&h.router, "GET", uri, key, None).await;
                assert_eq!(
                    (status, v["code"].clone()),
                    (StatusCode::UNAUTHORIZED, json!("unauthorized")),
                    "GET {uri} key={key:?}"
                );
            }
            let (status, _) = call(&h.router, "GET", uri, Some(KEY), None).await;
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
        assert_eq!((status, v["run_id"].clone()), (StatusCode::OK, json!(2)));
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
        let ids = |v: &Value| -> Vec<u64> {
            v["runs"]
                .as_array()
                .unwrap_or_else(|| panic!("runs array: {v}"))
                .iter()
                .filter_map(|r| r["id"].as_u64())
                .collect()
        };
        let (_, v) = call(&h.router, "GET", "/jobs?limit=2", Some(KEY), None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![1, 2], json!(2)));
        let (_, v) = call(&h.router, "GET", "/jobs?after=2&limit=2", Some(KEY), None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![3, 4], json!(4)));
        let (_, v) = call(&h.router, "GET", "/jobs?after=4&limit=2", Some(KEY), None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![5], Value::Null));
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
                .filter_map(|r| r["id"].as_u64())
                .collect()
        };
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&limit=2",
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), v["next_before"].clone()), (vec![5, 4], json!(4)));
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&before=4&limit=2",
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), v["next_before"].clone()), (vec![3, 2], json!(2)));
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&before=2&limit=2",
            Some(KEY),
            None,
        )
        .await;
        assert_eq!((ids(&v), v["next_before"].clone()), (vec![1], Value::Null));
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
            client: handler_client().unwrap_or_else(|e| panic!("{e}")),
            url,
            timeout: Duration::from_secs(10),
            max_output: 1024,
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
        reg.register(
            "step",
            Arc::new(HttpHandler {
                client: handler_client().unwrap_or_else(|e| panic!("{e}")),
                url: format!("http://{addr}/step"),
                timeout: Duration::from_millis(300),
                max_output: 1024,
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
        let id = run.0.to_string();
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

    /// Checklist: the inspector holds a key and renders op_keys. Its pages
    /// are served with a CSP that allows only its own scripts (the inline
    /// bootstrap by hash), no framing, nosniff and no referrer.
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
            App {
                engine,
                api_key: ApiKey::new(KEY),
                read_key: None,
                open_reads: false,
                backup_root: None,
            },
            Some(ui.path()),
        );
        use base64::Engine as _;
        let hash = base64::engine::general_purpose::STANDARD.encode(sha256(script.as_bytes()));
        for uri in [
            "/inspector/",
            "/inspector/app.js",
            "/inspector/runs/deep-link",
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
        let h = harness(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("{e}"));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        tokio::spawn(serve_http(listener, h.router.clone(), limits, stopped));
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
            reply.starts_with(b"HTTP/1.1 200"),
            "{}",
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
            App {
                engine,
                api_key: ApiKey::new(KEY),
                read_key: None,
                open_reads: false,
                backup_root: None,
            },
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
            App {
                engine,
                api_key: ApiKey::new(KEY),
                read_key: None,
                open_reads: false,
                backup_root: None,
            },
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

    #[test]
    fn api_key_debug_is_redacted() {
        let k = ApiKey::new(KEY);
        assert!(!format!("{k:?}").contains("test-key"));
        assert!(k.verify(KEY));
        assert!(!k.verify(""));
    }
}
