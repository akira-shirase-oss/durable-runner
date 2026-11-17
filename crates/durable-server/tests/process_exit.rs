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

/// Set to skip the permission-based proofs where permissions are ignored.
const SKIP_ENV: &str = "DURABLE_ALLOW_PERMISSION_SKIP";

/// A free loopback port outside the ranges other local services use
/// (55432, 56000-62999, which holds 58000-58099 and 58400).
fn free_port() -> u16 {
    loop {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("{e}"));
        let port = l.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
        if port != 55432 && !(56000..=62999).contains(&port) {
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
        // Root (e.g. a CI container) ignores directory permissions, so the
        // EACCES this proof relies on never happens. Never pass silently:
        // fail unless the skip is explicitly asked for.
        assert!(
            std::env::var_os(SKIP_ENV).is_some(),
            "this test needs a user that honours directory permissions (not root); \
             run it unprivileged, or set {SKIP_ENV}=1 to skip it explicitly"
        );
        eprintln!("SKIPPED (explicitly, {SKIP_ENV}=1): directory permissions are ignored here");
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

/// Wait for the child to exit, at most `limit`.
async fn exit_within(child: &mut Child, limit: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if let Some(s) = child.try_wait().unwrap_or_else(|e| panic!("{e}")) {
            return Some(s);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

fn signal(child: &Child, sig: &str) {
    let ok = Command::new("kill")
        .args([sig, &child.id().to_string()])
        .status()
        .unwrap_or_else(|e| panic!("{e}"))
        .success();
    assert!(ok, "kill {sig} failed");
}

/// Review (still open): SIGTERM killed the server like a crash. It must
/// drain: stop accepting, let every request in flight finish (each
/// acknowledged submit durable), flush the batch the writer holds, and exit
/// 0 — and nothing acknowledged may be missing after a restart.
#[tokio::test]
async fn sigterm_drains_in_flight_work_and_exits_zero() {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let journal = d.path().join("journal");
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let c = reqwest::Client::new();
    let mut server = start(&journal, port);
    wait_healthy(&c, &base).await;

    let mut acked = Vec::new();
    for i in 0..5 {
        acked.extend(submit(&c, &base, &format!("before-{i}")).await);
    }
    assert_eq!(acked.len(), 5);
    // Clients keep submitting while the signal lands.
    let mut clients = tokio::task::JoinSet::new();
    for t in 0..4 {
        let (c, base) = (c.clone(), base.clone());
        clients.spawn(async move {
            let mut ids = Vec::new();
            for i in 0..10_000 {
                match submit(&c, &base, &format!("during-{t}-{i}")).await {
                    Some(id) => ids.push(id),
                    None => break, // refused or connection closed: the drain began
                }
            }
            ids
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    signal(&server, "-TERM");
    let status = match exit_within(&mut server, Duration::from_secs(20)).await {
        Some(s) => s,
        None => {
            let _ = server.kill();
            let _ = server.wait();
            panic!("the server did not exit within 20 s of SIGTERM");
        }
    };
    while let Some(ids) = clients.join_next().await {
        acked.extend(ids.unwrap_or_else(|e| panic!("{e}")));
    }
    assert!(status.success(), "SIGTERM must drain and exit 0: {status}");
    assert!(
        acked.len() > 5,
        "no submit was in flight when the signal landed"
    );

    let mut server = start(&journal, port);
    wait_healthy(&c, &base).await;
    for id in &acked {
        let s = c
            .get(format!("{base}/jobs/{id}"))
            .bearer_auth(KEY)
            .send()
            .await
            .map(|r| r.status().as_u16())
            .unwrap_or(0);
        assert_eq!(s, 200, "acknowledged run {id} lost across SIGTERM");
    }
    let _ = server.kill();
    let _ = server.wait();
}
