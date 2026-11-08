//! durable: the operator CLI. Talks to a running durable-server over HTTP
//! for live operations; `restore` is an offline filesystem operation on a
//! stopped journal (the same layout backup produces).

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "durable", about = "Durable workflow runner CLI")]
struct Args {
    /// durable-server base URL.
    #[arg(long, global = true, default_value = "http://127.0.0.1:58300")]
    server: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Submit a job: `--op-key` dedupes retries; `--step name=handler` in order.
    Submit {
        #[arg(long)]
        op_key: String,
        #[arg(long = "step", required = true)]
        steps: Vec<String>,
        /// Input bytes inline (use @path to read a file).
        #[arg(long, default_value = "")]
        input: String,
        /// Delay the start by N milliseconds.
        #[arg(long)]
        delay_ms: Option<u64>,
    },
    /// Show one run's state.
    Inspect {
        #[arg(long)]
        run: u64,
    },
    /// Cancel a run (stops scheduling; cannot undo remote effects).
    Cancel {
        #[arg(long)]
        run: u64,
    },
    /// List every run in the live index.
    List,
    /// Bounded diagnostics: generation, records, runs by state kind.
    Diag,
    /// Compact the journal into a new generation.
    Compact,
    /// Consistent server-side backup into a destination directory.
    Backup {
        #[arg(long)]
        to: PathBuf,
    },
    /// Restore a backup into a journal directory (server must be stopped).
    Restore {
        #[arg(long)]
        from: PathBuf,
        #[arg(long)]
        to: PathBuf,
    },
}

fn parse_step(s: &str) -> Result<serde_json::Value> {
    let (name, handler) = s
        .split_once('=')
        .with_context(|| format!("step '{s}' must be name=handler"))?;
    Ok(json!({ "name": name, "handler": handler }))
}

fn input_bytes(arg: &str) -> Result<Vec<u8>> {
    if let Some(path) = arg.strip_prefix('@') {
        std::fs::read(path).with_context(|| format!("read input {path}"))
    } else {
        Ok(arg.as_bytes().to_vec())
    }
}

async fn check(resp: reqwest::Response) -> Result<serde_json::Value> {
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(json!({}));
    if status.is_success() {
        Ok(body)
    } else {
        bail!(
            "{status}: {}",
            body.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("error")
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = reqwest::Client::new();
    match args.cmd {
        Cmd::Submit {
            op_key,
            steps,
            input,
            delay_ms,
        } => {
            let step_objs: Result<Vec<_>> = steps.iter().map(|s| parse_step(s)).collect();
            let bytes = input_bytes(&input)?;
            use base64::Engine as _;
            let body = json!({
                "op_key": op_key,
                "spec": {
                    "version": 1,
                    "steps": step_objs?,
                    "input_b64": base64::engine::general_purpose::STANDARD.encode(&bytes),
                },
                "not_before": delay_ms.map(|d| now_ms() + d),
            });
            let v = check(
                client
                    .post(format!("{}/jobs", args.server))
                    .json(&body)
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Inspect { run } => {
            let v = check(
                client
                    .get(format!("{}/jobs/{run}", args.server))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Cancel { run } => {
            let v = check(
                client
                    .post(format!("{}/jobs/{run}/cancel", args.server))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::List => {
            let v = check(client.get(format!("{}/jobs", args.server)).send().await?).await?;
            println!("{v}");
        }
        Cmd::Diag => {
            let v = check(
                client
                    .get(format!("{}/admin/diagnostics", args.server))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Compact => {
            let v = check(
                client
                    .post(format!("{}/admin/compact", args.server))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Backup { to } => {
            let v = check(
                client
                    .post(format!("{}/admin/backup", args.server))
                    .json(&json!({ "dest": to.display().to_string() }))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Restore { from, to } => {
            restore(&from, &to)?;
            println!("restored {} -> {}", from.display(), to.display());
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Restore mirrors backup's ordering: generation data first, manifest
/// last — so an interrupted restore still opens a complete generation.
fn restore(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    if !from.join("manifest.json").exists() {
        bail!("{from:?} has no manifest.json — not a durable journal backup");
    }
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_str().is_some_and(|n| n.starts_with("gen-")) && entry.file_type()?.is_dir() {
            let gd = to.join(&name);
            std::fs::create_dir_all(&gd)?;
            for f in std::fs::read_dir(entry.path())? {
                let f = f?;
                std::fs::copy(f.path(), gd.join(f.file_name()))?;
            }
        }
    }
    std::fs::copy(from.join("manifest.json"), to.join("manifest.json"))?;
    Ok(())
}
