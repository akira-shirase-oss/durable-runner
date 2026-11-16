//! Process-level proof for the compaction swap rule (D-019): a real OS
//! error at the manifest swap makes `durable-server` exit non-zero, nothing
//! is acknowledged after it, and a restart replays everything acknowledged
//! before it.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const KEY: &str = "process-test-key-0123456789abcdef0123456789";

/// A free loopback port outside the ranges other local services use.
fn free_port() -> u16 {
    loop {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("{e}"));
        let port = l.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
        if port != 55432 && !(58000..=58099).contains(&port) {
            return port;
        }
    }
}

fn start(journal: &Path, port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_durable-server"))
        .arg("--journal")
        .arg(journal)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .args(["--handler", "step=http://127.0.0.1:9/unused"])
        .env("DURABLE_API_KEY", KEY)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("{e}"))
}

async fn wait_healthy(c: &reqwest::Client, base: &str) {
    for _ in 0..200 {
        if c.get(format!("{base}/health")).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server never became healthy");
}

/// Submit a delayed job; its run id if acknowledged.
async fn submit(c: &reqwest::Client, base: &str, key: &str) -> Option<u64> {
    let body = serde_json::json!({
        "op_key": key,
        "not_before": 9_999_999_999_999u64,
        "spec": { "version": 1, "steps": [{ "name": "a", "handler": "step" }], "input_b64": "aGk=" },
    });
    let r = c
        .post(format!("{base}/jobs"))
        .bearer_auth(KEY)
        .json(&body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json::<serde_json::Value>().await.ok()?["run_id"].as_u64()
}

struct Restore<'a>(&'a Path);
impl Drop for Restore<'_> {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
    }
}

#[tokio::test]
async fn failed_manifest_swap_exits_the_process_and_loses_nothing() {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let journal = d.path().join("journal");
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let c = reqwest::Client::new();
    let mut server = start(&journal, port);
    wait_healthy(&c, &base).await;

    let mut acked = Vec::new();
    for i in 0..3 {
        acked.extend(submit(&c, &base, &format!("pre-{i}")).await);
    }
    assert_eq!(acked.len(), 3);

    let guard = Restore(&journal);
    std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o300))
        .unwrap_or_else(|e| panic!("{e}"));
    if std::fs::File::open(&journal).is_ok() {
        drop(guard);
        let _ = server.kill();
        let _ = server.wait();
        eprintln!("skipped: this user ignores directory permissions");
        return;
    }
    let compact = c
        .post(format!("{base}/admin/compact"))
        .bearer_auth(KEY)
        .send()
        .await
        .map(|r| r.status().as_u16());
    assert_ne!(compact.ok(), Some(200), "the compaction failed");
    let mut after = 0;
    for i in 0..30 {
        if submit(&c, &base, &format!("post-{i}")).await.is_some() {
            after += 1;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(s) = server.try_wait().unwrap_or_else(|e| panic!("{e}")) {
            break s;
        }
        if Instant::now() > deadline {
            let _ = server.kill();
            let _ = server.wait();
            panic!("the server kept running after a failed manifest swap");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    drop(guard);
    assert_eq!(
        after, 0,
        "{after} submits acknowledged after the failed swap"
    );
    assert!(!status.success(), "the server must exit non-zero: {status}");

    let mut server = start(&journal, port);
    wait_healthy(&c, &base).await;
    for id in &acked {
        let s = c
            .get(format!("{base}/jobs/{id}"))
            .send()
            .await
            .map(|r| r.status().as_u16())
            .unwrap_or(0);
        assert_eq!(s, 200, "acknowledged run {id} lost on restart");
    }
    let _ = server.kill();
    let _ = server.wait();
}
