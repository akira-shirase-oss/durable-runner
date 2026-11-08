//! durable-server: the Axum coordinator. One engine, one writer, HTTP on
//! top — submit/inspect/cancel/compact/backup. Handlers are allowlisted
//! HTTP endpoints configured at startup; the registry is the trust edge.

use anyhow::{Context, Result};
use axum::{
    extract::{Path as AxPath, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use durable_domain::{JobSpec, Millis, OperationKey, RunId, RunState, StepSpec};
use durable_engine::shell::{spawn, Engine, Outcome, Registry, ShellConfig, StepHandler};
use durable_engine::EngineError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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
}

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
            Ok(r) if r.status().is_success() => match r.bytes().await {
                Ok(b) => Outcome::Checkpoint(b.to_vec()),
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

#[derive(Clone)]
struct App {
    engine: Engine,
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

#[derive(Serialize)]
struct ErrBody {
    error: String,
}

fn engine_err(e: EngineError) -> (StatusCode, Json<ErrBody>) {
    let code = match &e {
        EngineError::Refused(durable_domain::Refusal::ReserveExhausted) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        EngineError::Refused(_) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        code,
        Json(ErrBody {
            error: e.to_string(),
        }),
    )
}

async fn submit(
    State(app): State<App>,
    Json(req): Json<SubmitReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrBody>)> {
    let input = base64_decode(&req.spec.input_b64)
        .map_err(|m| (StatusCode::BAD_REQUEST, Json(ErrBody { error: m })))?;
    let spec = JobSpec {
        version: req.spec.version,
        steps: req.spec.steps,
        input,
    };
    let key = OperationKey(req.op_key);
    let run = match req.not_before {
        Some(at) => app.engine.submit_at(key, spec, at).await,
        None => app.engine.submit(key, spec).await,
    }
    .map_err(engine_err)?;
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
    AxPath(id): AxPath<u64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrBody>)> {
    let run = app
        .engine
        .inspect(RunId(id))
        .await
        .map_err(engine_err)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrBody {
                    error: "no such run".into(),
                }),
            )
        })?;
    Ok(Json(serde_json::json!({
        "id": run.id.0,
        "key": run.key.0,
        "state": state_name(&run.state),
        "steps": run.spec.steps.len(),
        "finished_at": run.finished_at,
    })))
}

async fn cancel(
    State(app): State<App>,
    AxPath(id): AxPath<u64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrBody>)> {
    app.engine.cancel(RunId(id)).await.map_err(engine_err)?;
    Ok(Json(serde_json::json!({ "cancelled": id })))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn compact(
    State(app): State<App>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrBody>)> {
    let gen = app.engine.compact().await.map_err(engine_err)?;
    Ok(Json(serde_json::json!({ "generation": gen })))
}

#[derive(Deserialize)]
struct BackupReq {
    dest: String,
}

async fn backup(
    State(app): State<App>,
    Json(req): Json<BackupReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrBody>)> {
    app.engine
        .backup_to(std::path::PathBuf::from(&req.dest))
        .await
        .map_err(engine_err)?;
    Ok(Json(serde_json::json!({ "backup": req.dest })))
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut registry = Registry::default();
    let client = reqwest::Client::new();
    for (name, url) in &args.handlers {
        registry.register(
            name,
            Arc::new(HttpHandler {
                client: client.clone(),
                url: url.clone(),
                timeout: Duration::from_millis(args.step_timeout_ms),
            }),
        );
    }
    let engine = spawn(&args.journal, registry, ShellConfig::default())
        .map_err(|e| anyhow::anyhow!("engine open: {e}"))?;
    let app = App { engine };
    let router = Router::new()
        .route("/health", get(health))
        .route("/jobs", post(submit))
        .route("/jobs/{id}", get(inspect))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/admin/compact", post(compact))
        .route("/admin/backup", post(backup))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    eprintln!("durable-server listening on {}", args.listen);
    axum::serve(listener, router).await?;
    Ok(())
}
