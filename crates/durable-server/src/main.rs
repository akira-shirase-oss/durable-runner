//! durable-server: the Axum coordinator. One engine, one writer, HTTP on
//! top — submit/inspect/cancel/compact/backup. Handlers are allowlisted
//! HTTP endpoints configured at startup; the registry is the trust edge.

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
use durable_engine::shell::{spawn, Engine, Order, Outcome, Registry, ShellConfig, StepHandler};
use durable_engine::EngineError;
use serde::Deserialize;
use std::future::IntoFuture;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "durable-server", about = "Durable workflow runner coordinator")]
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
    /// `at_capacity`.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    max_index_bytes: u64,
    /// Handler calls (attempts and probes) in flight at once.
    #[arg(long, default_value_t = 32)]
    max_in_flight: usize,
    /// Dispatches per scheduler tick (ticks every 50 ms, so the first-step
    /// dispatch rate is at most 20 × this per second).
    #[arg(long, default_value_t = 64)]
    max_dispatch_per_tick: usize,
}

/// The environment variable holding the API key. Read from the
/// environment, never from argv (argv is visible to every local user).
const API_KEY_ENV: &str = "DURABLE_API_KEY";
const MIN_API_KEY_LEN: usize = 32;
/// Largest request body accepted on any route.
const MAX_REQUEST_BYTES: usize = 256 * 1024;

fn parse_handler(s: &str) -> Result<(String, String), String> {
    let (name, url) = s.split_once('=').ok_or("expected name=URL")?;
    Ok((name.to_string(), url.to_string()))
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

#[async_trait::async_trait]
impl StepHandler for HttpHandler {
    async fn execute(&self, _step: &StepSpec, input: &[u8], idem_key: &str) -> Outcome {
        let body = serde_json::json!({
            "input": input,
            "idem_key": idem_key,
        });
        let res = self
            .client
            .post(&self.url)
            .header("x-idempotency-key", idem_key)
            .json(&body)
            .timeout(self.timeout)
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
            Err(e) if e.is_connect() => Outcome::Failed,
            Err(_) => Outcome::Lost, // genuinely unknown — reconcile
        }
    }

    /// Probe asks the handler whether the call under `idem_key` landed.
    /// `GET {url}?idem_key=…` → 200 means found; anything else is unknown
    /// → reported as not-found honestly (default trait behaviour).
    async fn probe(&self, _step: &StepSpec, idem_key: &str) -> bool {
        self.client
            .get(&self.url)
            .query(&[("idem_key", idem_key)])
            .timeout(self.timeout)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
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
    /// Canonical backup directory; `None` disables `/admin/backup`.
    backup_root: Option<Arc<Path>>,
}

/// Proof that the request carried the API key. Every mutating and admin
/// handler takes this as its first extractor, so it runs before the body
/// is parsed and the route cannot be reached without the key.
struct Authorized;

impl FromRequestParts<App> for Authorized {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        // `<scheme> <token>`; the scheme is case-insensitive (RFC 9110
        // §11.1) and surrounding whitespace is ignored.
        let presented = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().split_once(char::is_whitespace))
            .and_then(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
            });
        match presented {
            Some(key) if app.api_key.verify(key) => Ok(Authorized),
            _ => Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid API key (Authorization: Bearer <key>)",
            )),
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
                "the runner holds as many runs as it is configured to; retry later with the same op_key",
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
            EngineError::SnapshotOverBudget { .. } => {
                Self::new(StatusCode::CONFLICT, "snapshot_over_budget", e.to_string())
            }
            EngineError::Serde(_) | EngineError::Piped => Self::internal(&e),
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

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| e.to_string())
}

/// Every route. Reads (`/health`, `/ready`, `GET /jobs`, `GET /jobs/{id}`, the
/// inspector) are open; every mutating and admin handler takes
/// `Authorized`, so it is unreachable without the API key.
fn router(app: App, ui_dir: Option<&Path>) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/jobs", post(submit).get(list))
        .route("/jobs/{id}", get(inspect))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/admin/compact", post(compact))
        .route("/admin/backup", post(backup))
        .route("/admin/diagnostics", get(diagnostics))
        // A submit is at most 64 KiB of input (base64: ~88 KiB) plus its
        // spec; nothing legitimate comes near this.
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(app);
    if let Some(ui) = ui_dir {
        router = router.nest_service(
            "/inspector",
            tower_http::services::ServeDir::new(ui)
                .append_index_html_on_directories(true)
                .not_found_service(tower_http::services::ServeFile::new(ui.join("index.html"))),
        );
    }
    router
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
    let api_key = api_key_from_env()?;
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
        limits: durable_domain::Limits {
            max_live_runs: args.max_live_runs,
            max_index_bytes: args.max_index_bytes,
            ..durable_domain::Limits::default()
        },
        ..Default::default()
    };
    let mut registry = Registry::default();
    let client = reqwest::Client::new();
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
    let stopped = engine.clone();
    let app = App {
        engine,
        api_key,
        backup_root,
    };
    let router = router(app, args.ui_dir.as_deref());
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    eprintln!("durable-server listening on {}", args.listen);
    tokio::select! {
        r = axum::serve(listener, router).into_future() => r?,
        () = stopped.closed() => {
            // The journal writer poisoned itself (a failed fsync is never
            // retried): exit non-zero so the supervisor restarts us and
            // startup replay decides what is durable.
            anyhow::bail!("engine stopped: journal writer poisoned; exiting so a restart replays");
        }
    }
    Ok(())
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
        let routes: [(&str, &str, Option<Value>); 5] = [
            ("POST", "/jobs", Some(submit_body("k"))),
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
        let (_, list) = call(&h.router, "GET", "/jobs", None, None).await;
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
    async fn the_key_is_accepted_and_reads_stay_open() {
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
        let (status, _) = call(&h.router, "GET", "/jobs/1", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/jobs", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/health", None, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call(&h.router, "GET", "/admin/diagnostics", Some(KEY), None).await;
        assert_eq!(status, StatusCode::OK);
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
        let (_, v) = call(&h.router, "GET", "/jobs?limit=2", None, None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![1, 2], json!(2)));
        let (_, v) = call(&h.router, "GET", "/jobs?after=2&limit=2", None, None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![3, 4], json!(4)));
        let (_, v) = call(&h.router, "GET", "/jobs?after=4&limit=2", None, None).await;
        assert_eq!((ids(&v), v["next_after"].clone()), (vec![5], Value::Null));
        let (status, v) = call(&h.router, "GET", "/jobs?limit=100000", None, None).await;
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
        let (_, v) = call(&h.router, "GET", "/jobs?order=desc&limit=2", None, None).await;
        assert_eq!((ids(&v), v["next_before"].clone()), (vec![5, 4], json!(4)));
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&before=4&limit=2",
            None,
            None,
        )
        .await;
        assert_eq!((ids(&v), v["next_before"].clone()), (vec![3, 2], json!(2)));
        let (_, v) = call(
            &h.router,
            "GET",
            "/jobs?order=desc&before=2&limit=2",
            None,
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
            client: reqwest::Client::new(),
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
                backup_root: None,
            },
            None,
        );
        fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let (status, v) = call(&r, "POST", "/jobs", Some(KEY), Some(submit_body("p"))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
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
        let (status, v) = call(&h.router, "GET", "/jobs?limit=-1", None, None).await;
        assert_eq!(
            (status, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("bad_query"))
        );
        let (status, v) = call(&h.router, "GET", "/jobs?after=abc", None, None).await;
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

    #[test]
    fn api_key_debug_is_redacted() {
        let k = ApiKey::new(KEY);
        assert!(!format!("{k:?}").contains("test-key"));
        assert!(k.verify(KEY));
        assert!(!k.verify(""));
    }
}
