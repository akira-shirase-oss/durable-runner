//! Shared harness for the dynamic pentest suite: spawn a real
//! `durable-server` binary (fresh journal, in-memory account graph,
//! per-test limit flags) and drive attack requests at it over real
//! sockets. Every attack waits on a signal — `/health` for readiness,
//! a polled run state for engine progress — never on a sleep-as-proof.
//!
//! The binary is located via `$DURABLE_SERVER_BIN`, else
//! `target/{debug,release}/durable-server` under the workspace root; a
//! missing binary triggers a one-shot `cargo build -p durable-server`.

#![allow(
    dead_code,
    reason = "shared harness: helpers are used per test binary, not all at once"
)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The ops key the spawned server is configured with (`DURABLE_API_KEY`):
/// workspace-0 admin — the honest user's key in every attack.
pub const OPS_KEY: &str = "dr_ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4ab4";
/// The read-only env key (`DURABLE_READ_KEY`).
pub const READ_KEY: &str = "dr_aacaacaacaacaacaacaacaacaacaacaacaacaacaacaacaac";
/// A key that *looks* like a store key (`dr_` + 48 lowercase hex) but was
/// never issued — it passes the cheap shape check and must still be denied.
pub const FORGED_KEY: &str = "dr_000000000000000000000000000000000000000000000000";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The server binary this suite attacks.
pub fn server_bin() -> PathBuf {
    if let Ok(p) = std::env::var("DURABLE_SERVER_BIN") {
        let p = PathBuf::from(p);
        assert!(p.exists(), "DURABLE_SERVER_BIN {p:?} does not exist");
        return p;
    }
    let root = workspace_root();
    for profile in ["debug", "release"] {
        let p = root.join(format!("target/{profile}/durable-server"));
        if p.exists() {
            return p;
        }
    }
    // `cargo test --workspace` builds the bin anyway, but a cold
    // `cargo test -p durable-attacks` may not have it yet.
    let status = Command::new("cargo")
        .args(["build", "-p", "durable-server"])
        .current_dir(&root)
        .status()
        .unwrap_or_else(|e| panic!("cargo build -p durable-server: {e}"));
    assert!(status.success(), "cargo build -p durable-server failed");
    let p = root.join("target/debug/durable-server");
    assert!(p.exists(), "durable-server binary missing after build");
    p
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr().map(|a| a.port()))
        .unwrap_or_else(|e| panic!("free_port: {e}"))
}

/// A running server under attack. Killed (not drained) on drop — it is a
/// fixture, not production.
pub struct AttackServer {
    pub base: String,
    _dir: tempfile::TempDir,
    child: Child,
}

impl Drop for AttackServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn a server on a fresh journal with the harness's per-address
/// limits lifted (each attack sets the flag it is probing via `extra`),
/// the account graph in-memory, and the env keys above. Waits on
/// `/health` — the readiness signal — for up to 15 s per attempt.
pub async fn spawn(extra: &[&str]) -> AttackServer {
    let bin = server_bin();
    let mut last_err = String::new();
    for _attempt in 0..3 {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
        let port = free_port();
        let mut cmd = Command::new(&bin);
        cmd.arg("--journal")
            .arg(dir.path().join("journal"))
            .args(["--listen", &format!("127.0.0.1:{port}")]);
        // Harness defaults, yielded to any flag `extra` sets itself —
        // clap refuses a repeated scalar flag.
        for flag in ["--rate-limit-per-addr", "--max-conns-per-addr"] {
            if !extra.contains(&flag) {
                cmd.args([
                    flag,
                    if flag == "--rate-limit-per-addr" {
                        "100000"
                    } else {
                        "0"
                    },
                ]);
            }
        }
        let child = cmd
            // Submit specs name `step`; the flag handler on a global (but
            // unreachable) IP passes check_url, and parked runs never dial.
            // A ws-0 PUT /handlers/step store row shadows it where a test
            // needs its own target.
            .args(["--handler", "step=http://8.8.8.8/unused"])
            .args(extra)
            .env("DURABLE_API_KEY", OPS_KEY)
            .env("DURABLE_READ_KEY", READ_KEY)
            .env("DURABLE_ACCOUNTS_BACKEND", "memory")
            .env_remove("DATABASE_URL")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        let mut child = match child {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("spawn: {e}");
                continue;
            }
        };
        let base = format!("http://127.0.0.1:{port}");
        let c = http();
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut up = false;
        while Instant::now() < deadline {
            if let Ok(Some(s)) = child.try_wait() {
                last_err = format!("server exited during startup ({s})");
                break;
            }
            if c.get(format!("{base}/health")).send().await.is_ok() {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if up {
            return AttackServer {
                base,
                _dir: dir,
                child,
            };
        }
        if last_err.is_empty() {
            last_err = "server never answered /health within 15 s".into();
        }
        let _ = child.kill();
        let _ = child.wait();
    }
    panic!("durable-server did not come up: {last_err}");
}

/// A client with the server's own discipline: no redirects followed, no
/// proxy env honoured, every request inside a deadline.
pub fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .unwrap_or_else(|e| panic!("client: {e}"))
}

/// The smallest submit body that passes spec validation.
pub fn submit_body(op_key: &str) -> serde_json::Value {
    serde_json::json!({
        "op_key": op_key,
        "not_before": 9_999_999_999_999u64,
        "spec": {
            "version": 1,
            "steps": [{ "name": "a", "handler": "step" }],
            "input_b64": "aGk="
        },
    })
}

/// POST a submit; returns (status code, run id if one was issued).
pub async fn submit(
    c: &reqwest::Client,
    base: &str,
    key: &str,
    op_key: &str,
) -> (u16, Option<String>) {
    let r = c
        .post(format!("{base}/jobs"))
        .bearer_auth(key)
        .json(&submit_body(op_key))
        .send()
        .await
        .unwrap_or_else(|e| panic!("submit: {e}"));
    let status = r.status().as_u16();
    let v: serde_json::Value = r
        .json()
        .await
        .unwrap_or_else(|e| panic!("submit json: {e}"));
    (status, v["run_id"].as_str().map(str::to_string))
}

/// The JSON error envelope's stable code.
pub async fn code(r: reqwest::Response) -> (u16, String) {
    let status = r.status().as_u16();
    let v: serde_json::Value = r
        .json()
        .await
        .unwrap_or_else(|e| panic!("envelope json: {e}"));
    (status, v["code"].as_str().unwrap_or("<none>").to_string())
}

/// Create a workspace via the ops key; returns its public_id.
pub async fn create_workspace(c: &reqwest::Client, base: &str, name: &str) -> String {
    let r = c
        .post(format!("{base}/admin/workspaces"))
        .bearer_auth(OPS_KEY)
        .json(&serde_json::json!({ "name": name }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("create_workspace: {e}"));
    assert_eq!(r.status().as_u16(), 200, "create_workspace refused");
    let v: serde_json::Value = r.json().await.unwrap_or_else(|e| panic!("{e}"));
    v["public_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no public_id in {v}"))
        .to_string()
}

/// Mint a store key on a workspace via the ops key; returns the secret.
pub async fn create_key(
    c: &reqwest::Client,
    base: &str,
    ws_public: &str,
    permission: &str,
) -> String {
    let r = c
        .post(format!("{base}/admin/workspaces/{ws_public}/keys"))
        .bearer_auth(OPS_KEY)
        .json(&serde_json::json!({ "name": "k", "permission": permission }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("create_key: {e}"));
    assert_eq!(r.status().as_u16(), 200, "create_key refused");
    let v: serde_json::Value = r.json().await.unwrap_or_else(|e| panic!("{e}"));
    v["key"]
        .as_str()
        .unwrap_or_else(|| panic!("no key in {v}"))
        .to_string()
}

/// Poll `GET /jobs/{id}` until the run's `state` equals `want` or the
/// deadline passes. This is the signal a liveness-style attack waits on —
/// an observed state, never a bare sleep.
pub async fn wait_state(
    c: &reqwest::Client,
    base: &str,
    key: &str,
    run_id: &str,
    want: &str,
    within: Duration,
) -> serde_json::Value {
    let deadline = Instant::now() + within;
    let mut last = serde_json::Value::Null;
    while Instant::now() < deadline {
        let r = c
            .get(format!("{base}/jobs/{run_id}"))
            .bearer_auth(key)
            .send()
            .await;
        if let Ok(r) = r {
            if r.status().as_u16() == 200 {
                last = r.json().await.unwrap_or_else(|e| panic!("{e}"));
                // `state` carries detail ("failed:step:0") — `want` is a
                // prefix family ("failed").
                if last["state"]
                    .as_str()
                    .is_some_and(|s| s == want || s.starts_with(&format!("{want}:")))
                {
                    return last;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("run {run_id} never reached state {want:?} within {within:?}; last: {last}");
}
