//! durable: the operator CLI. Talks to a running durable-server over HTTP
//! for live operations; `restore` is an offline filesystem operation on a
//! stopped journal (the same layout backup produces).
//!
//! Every command sends `Authorization: Bearer $DURABLE_API_KEY` — the same
//! variable the server reads its key from. Reads (`inspect`, `list`,
//! `metrics`, `events`) fall back to `$DURABLE_READ_KEY` when no API key is
//! set.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "durable", version, about = "Durable workflow runner CLI")]
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
        /// The opaque run id the server returned at submit.
        #[arg(long)]
        run: String,
    },
    /// Cancel a run (stops scheduling; cannot undo remote effects).
    Cancel {
        /// The opaque run id the server returned at submit.
        #[arg(long)]
        run: String,
    },
    /// List one page of the live index (ids ascending, or newest first).
    List {
        /// Newest first; continue with `--before <next_before>`.
        #[arg(long)]
        newest: bool,
        /// Newest first: start before this run id.
        #[arg(long)]
        before: Option<String>,
        /// Start after this run id (the previous page's `next_after`).
        #[arg(long)]
        after: Option<String>,
        /// Page size (server clamps to 1..=1000; default 100).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Bounded diagnostics: generation, records, runs by state kind.
    Diag,
    /// Metrics over a window (15m, 1h, 6h or 24h; default 1h), as JSON.
    Metrics {
        #[arg(long)]
        window: Option<String>,
    },
    /// The activity feed, newest first — or one run's timeline (`--run`).
    Events {
        /// The opaque run id the server returned at submit.
        #[arg(long)]
        run: Option<String>,
        /// Feed page size, 1-200 (default 50).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Compact the journal into a new generation.
    Compact,
    /// Consistent server-side backup into `<server --backup-root>/<name>`.
    Backup {
        /// A plain name ([A-Za-z0-9_.-]), never a path.
        #[arg(long)]
        name: String,
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

/// Attach the API key when one is configured; the server refuses
/// mutating and admin calls without it (401).
fn authed(rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match std::env::var("DURABLE_API_KEY") {
        Ok(key) => rb.bearer_auth(key.trim()),
        Err(_) => rb,
    }
}

/// Attach a key that may read: the API key, else the read-only key. The
/// server refuses reads without one (401) unless it runs `--open-reads`.
fn read_authed(rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match std::env::var("DURABLE_API_KEY").or_else(|_| std::env::var("DURABLE_READ_KEY")) {
        Ok(key) => rb.bearer_auth(key.trim()),
        Err(_) => rb,
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
                authed(client.post(format!("{}/jobs", args.server)))
                    .json(&body)
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Inspect { run } => {
            let v = check(
                read_authed(client.get(format!("{}/jobs/{run}", args.server)))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Cancel { run } => {
            let v = check(
                authed(client.post(format!("{}/jobs/{run}/cancel", args.server)))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::List {
            newest,
            before,
            after,
            limit,
        } => {
            let mut q: Vec<(&str, String)> = Vec::new();
            if newest || before.is_some() {
                q.push(("order", "desc".into()));
            }
            if let Some(b) = before {
                q.push(("before", b));
            }
            if let Some(a) = after {
                q.push(("after", a));
            }
            if let Some(l) = limit {
                q.push(("limit", l.to_string()));
            }
            let v = check(
                read_authed(client.get(format!("{}/jobs", args.server)))
                    .query(&q)
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Diag => {
            let v = check(
                authed(client.get(format!("{}/admin/diagnostics", args.server)))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Metrics { window } => {
            let q: Vec<(&str, String)> = window.map(|w| ("window", w)).into_iter().collect();
            let v = check(
                read_authed(client.get(format!("{}/metrics", args.server)))
                    .query(&q)
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Events { run, limit } => {
            let url = match run {
                Some(id) => format!("{}/jobs/{id}/events", args.server),
                None => format!("{}/events", args.server),
            };
            let q: Vec<(&str, String)> = limit
                .map(|l| ("limit", l.to_string()))
                .into_iter()
                .collect();
            let v = check(read_authed(client.get(url)).query(&q).send().await?).await?;
            println!("{v}");
        }
        Cmd::Compact => {
            let v = check(
                authed(client.post(format!("{}/admin/compact", args.server)))
                    .send()
                    .await?,
            )
            .await?;
            println!("{v}");
        }
        Cmd::Backup { name } => {
            let v = check(
                authed(client.post(format!("{}/admin/backup", args.server)))
                    .json(&json!({ "name": name }))
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
/// last — so an interrupted restore never opens as a half journal. It
/// takes the target's journal lock (a server running there refuses it),
/// refuses a target that already holds a journal, and copies only the
/// backup's own regular files.
fn restore(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    durable_journal::restore_backup(from, to).map_err(|e| match e {
        durable_journal::JournalError::SecondWriter => anyhow::anyhow!(
            "{} is in use: a server holds its journal lock; stop it, or restore into a new directory",
            to.display()
        ),
        e => anyhow::anyhow!("restore {} -> {}: {e}", from.display(), to.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_journal::Journal;
    use std::path::Path;

    /// A backup of a journal holding one record.
    fn backup_of_one(root: &Path) -> PathBuf {
        let src = root.join("src");
        let dest = root.join("backup");
        let mut j = Journal::open(&src).unwrap_or_else(|e| panic!("{e}"));
        j.append(br#"{"run":1}"#).unwrap_or_else(|e| panic!("{e}"));
        j.backup_to(&dest).unwrap_or_else(|e| panic!("{e}"));
        dest
    }

    fn log_of(dir: &Path) -> Vec<u8> {
        std::fs::read(durable_journal::gen_dir_path(dir, 1).join("journal.log"))
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Review (still open): restore took no lock, so it copied generations
    /// and a manifest into a journal a running server was appending to.
    #[test]
    fn restore_refuses_a_journal_a_server_holds() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let backup = backup_of_one(d.path());
        let live = d.path().join("live");
        let _server = Journal::open(&live).unwrap_or_else(|e| panic!("{e}"));
        let before = log_of(&live);
        assert!(
            restore(&backup, &live).is_err(),
            "restored under a live writer"
        );
        assert_eq!(log_of(&live), before, "the live journal was written to");
    }

    /// A restore never merges into an existing journal (generations from
    /// two histories would mix), even a stopped one.
    #[test]
    fn restore_refuses_to_merge_into_an_existing_journal() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let backup = backup_of_one(d.path());
        let stopped = d.path().join("stopped");
        drop(Journal::open(&stopped).unwrap_or_else(|e| panic!("{e}")));
        let before = log_of(&stopped);
        assert!(restore(&backup, &stopped).is_err());
        assert_eq!(log_of(&stopped), before);
        // Into a fresh directory it works and opens.
        let fresh = d.path().join("fresh");
        restore(&backup, &fresh).unwrap_or_else(|e| panic!("{e}"));
        let j = Journal::open(&fresh).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(j.records_written(), 1);
    }

    /// A symlink planted in a backup is refused, never followed: restore
    /// copies only the backup's own regular files.
    #[cfg(unix)]
    #[test]
    fn restore_does_not_follow_symlinks_in_the_backup() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let backup = backup_of_one(d.path());
        let secret = d.path().join("secret");
        std::fs::write(&secret, b"not journal data").unwrap_or_else(|e| panic!("{e}"));
        let planted = durable_journal::gen_dir_path(&backup, 1).join("snapshot.bin");
        std::os::unix::fs::symlink(&secret, &planted).unwrap_or_else(|e| panic!("{e}"));
        let to = d.path().join("to");
        assert!(restore(&backup, &to).is_err(), "a symlink was followed");
        assert!(
            !durable_journal::gen_dir_path(&to, 1)
                .join("snapshot.bin")
                .exists(),
            "the symlink's target was copied"
        );
    }
}

/// Never-panics fuzzing for the CLI's outside-input parser (`--step`
/// specs). A panic is a red-first bug.
#[cfg(test)]
mod fuzz {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::default())]
        #[test]
        fn fuzz_parse_step_never_panics(s in any::<String>()) {
            let _ = parse_step(&s);
        }
    }
}
