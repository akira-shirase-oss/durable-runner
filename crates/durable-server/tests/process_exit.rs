//! Process-level proof for the compaction swap rule (D-019): a real OS
//! error BEFORE the manifest swap — the unwritable journal root fails
//! the `journal.shape` write — is an ordinary compaction error: the
//! process keeps serving, SIGTERM drains it cleanly, and a restart
//! replays everything acknowledged. A failure AT or after the rename
//! still poisons; the in-process fault drill covers that (an external
//! permissions trick cannot reach past the sidecar write anymore).
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

/// The spawn every serving server in this file goes through: the
/// per-address limits are lifted exactly as the load battery lifts them
/// (`perf/load.mjs`), so a test's own burst can never be refused by D-052
/// — the limiter's own tests exercise it in the crate's unit tests.
/// `DURABLE_TEST_RATE_LIMIT` lowers the rate to reproduce a refusal on
/// purpose.
fn server_cmd(journal: &Path, port: u16) -> Command {
    let rate = std::env::var("DURABLE_TEST_RATE_LIMIT").unwrap_or_else(|_| "100000".into());
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_durable-server"));
    cmd.arg("--journal")
        .arg(journal)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .args(["--rate-limit-per-addr", &rate])
        .args(["--max-conns-per-addr", "0"])
        // Every fixture handler lives on loopback — the S2-5 opt-out.
        .arg("--allow-local-handlers")
        .env("DURABLE_API_KEY", KEY)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

fn start(journal: &Path, port: u16) -> Child {
    server_cmd(journal, port)
        .args(["--handler", "step=http://127.0.0.1:9/unused"])
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

/// The readiness signal a deployment probes: 200, not just a listener.
async fn wait_ready(c: &reqwest::Client, base: &str) {
    for _ in 0..200 {
        if let Ok(r) = c.get(format!("{base}/ready")).send().await {
            if r.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server never became ready");
}

/// Submit a delayed job; its run id if acknowledged.
async fn submit(c: &reqwest::Client, base: &str, key: &str) -> Option<String> {
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
    Some(
        r.json::<serde_json::Value>().await.ok()?["run_id"]
            .as_str()?
            .to_string(),
    )
}

/// Read an acknowledged run back and name what the answer means. Only a
/// 404 (or a 200 naming a different run) is "lost". A 429 or 503 is the
/// reader refused by a limit or by capacity — the run may be perfectly
/// fine, and calling it lost costs a wrong diagnosis.
async fn assert_run_survives(c: &reqwest::Client, base: &str, id: &str, across: &str) {
    let Ok(r) = c
        .get(format!("{base}/jobs/{id}"))
        .bearer_auth(KEY)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    else {
        panic!("read-back of acknowledged run {id} got no answer {across}");
    };
    let s = r.status().as_u16();
    match s {
        200 => {
            let body = r
                .json::<serde_json::Value>()
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(
                body["id"].as_str(),
                Some(id),
                "acknowledged run {id} lost {across}: /jobs/{id} names a different run: {body}"
            );
        }
        404 => panic!("acknowledged run {id} lost {across}: /jobs/{id} is 404"),
        429 | 503 => panic!(
            "read-back of acknowledged run {id} refused {across} ({s}: rate or capacity), \
             not lost — the burst tripped a bound, the run may be fine"
        ),
        other => panic!("read-back of acknowledged run {id} answered {other} {across}"),
    }
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
    // The unwritable root fails the journal.shape write — BEFORE the
    // manifest swap, which is an ordinary compaction error, not a
    // poison: the server keeps serving and drains cleanly on SIGTERM.
    // (A post-rename failure still exits non-zero; the in-process fault
    // injection at compaction step 3 proves that path.)
    let mut after = 0;
    for i in 0..30 {
        if submit(&c, &base, &format!("post-{i}")).await.is_some() {
            after += 1;
        }
    }
    assert_eq!(
        after, 30,
        "a pre-swap compaction error must not stop the engine"
    );
    drop(guard);
    signal(&server, "-TERM");
    let status = exit_within(&mut server, Duration::from_secs(15))
        .await
        .unwrap_or_else(|| panic!("the server did not drain after SIGTERM"));
    assert!(status.success(), "a live engine drains to exit 0: {status}");

    let mut server = start(&journal, port);
    wait_healthy(&c, &base).await;
    for id in &acked {
        assert_run_survives(&c, &base, id, "on restart").await;
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
        assert_run_survives(&c, &base, id, "across SIGTERM").await;
    }
    let _ = server.kill();
    let _ = server.wait();
}

/// Run the server with `env` (and nothing inherited for the keys) and
/// extra `args`; it must exit non-zero on its own, before serving, with
/// `why` on stderr.
async fn refuses_to_start(env: &[(&str, &str)], args: &[&str], why: &str) {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let port = free_port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_durable-server"));
    cmd.arg("--journal").arg(d.path().join("journal"));
    if !args.contains(&"--listen") {
        cmd.args(["--listen", &format!("127.0.0.1:{port}")]);
    }
    cmd.args(args)
        .env_remove("DURABLE_API_KEY")
        .env_remove("DURABLE_READ_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap_or_else(|e| panic!("{e}"));
    let Some(status) = exit_within(&mut child, Duration::from_secs(10)).await else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("the server kept running ({args:?}); it must refuse: {why}");
    };
    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut e, &mut err);
    }
    assert!(!status.success(), "{args:?}: exit {status}");
    assert!(err.contains(why), "stderr does not say {why:?}: {err}");
    let listened = args
        .iter()
        .position(|a| *a == "--listen")
        .and_then(|i| args.get(i + 1))
        .and_then(|l| l.rsplit(':').next())
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(port);
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", listened)).is_err(),
        "something listens on the refused server's port"
    );
}

/// Contract case: a second local writer fails startup rather than share the
/// journal. The first server holds LOCK for life; the second exits non-zero
/// before listening, and the first keeps serving.
#[tokio::test]
async fn a_second_server_on_the_same_journal_refuses_startup() {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let journal = d.path().join("journal");
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let c = reqwest::Client::new();
    let mut first = start(&journal, port);
    wait_healthy(&c, &base).await;

    let mut second = server_cmd(&journal, free_port())
        .args(["--handler", "step=http://127.0.0.1:9/unused"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("{e}"));
    let status = match exit_within(&mut second, Duration::from_secs(10)).await {
        Some(s) => s,
        None => {
            let _ = second.kill();
            let _ = second.wait();
            panic!("a second server on the same journal kept running");
        }
    };
    let mut err = String::new();
    if let Some(mut e) = second.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut e, &mut err);
    }
    assert!(!status.success(), "second writer exited {status}");
    assert!(
        err.contains("writer") || err.contains("LOCK"),
        "stderr does not name the writer lock: {err}"
    );

    let healthy = c
        .get(format!("{base}/health"))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);
    assert!(healthy, "the lock holder stopped serving");
    let _ = first.kill();
    let _ = first.wait();
}

/// Checklist: a missing or short API key refuses startup — the server
/// never serves with no key or a guessable one; and a read-only key must
/// be long and different from the admin key.
#[tokio::test]
async fn a_missing_or_short_key_refuses_startup() {
    refuses_to_start(&[], &[], "DURABLE_API_KEY is not set").await;
    let short = "k".repeat(31);
    refuses_to_start(&[("DURABLE_API_KEY", &short)], &[], "at least 32").await;
    refuses_to_start(
        &[("DURABLE_API_KEY", KEY), ("DURABLE_READ_KEY", &short)],
        &[],
        "DURABLE_READ_KEY must be at least 32",
    )
    .await;
    refuses_to_start(
        &[("DURABLE_API_KEY", KEY), ("DURABLE_READ_KEY", KEY)],
        &[],
        "must differ",
    )
    .await;
}

/// Finding 2 (S1): a v1.0.x journal has committed runs but no public-id
/// key — no v1.0.x binary ever wrote one. The S2 server must mint the
/// key and come up on it: before the era check, every existing install
/// crash-looped on upgrade (the key loader refused any non-empty
/// journal). The same server must still refuse a pubid-era journal that
/// lost its key — that refusal is the rename guard, kept by the unit
/// test next to `load_or_generate`.
#[tokio::test]
async fn a_v1_journal_boots_and_reaches_ready() {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let journal = d.path().join("journal");
    // A journal exactly as v1.0.1 leaves it: manifest, writer lock and
    // gen-000001 holding v2-envelope records — no journal.shape (that
    // sidecar is v1.0.2) and never a public_id.key.
    {
        let mut j = durable_journal::Journal::open(&journal).unwrap_or_else(|e| panic!("{e}"));
        j.append(
            br#"{"v":2,"run":1,"at":1000,"event":{"Accepted":{"key":"legacy","spec":{"version":1,"steps":[{"name":"a","handler":"step"}],"input":"aGk="},"not_before":9999999999999}}}"#,
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }
    assert!(
        !journal.join("journal.shape").exists(),
        "the fixture claims v1.0.1 — no sidecar"
    );
    assert!(
        !journal.join("public_id.key").exists(),
        "the fixture claims v1.0.1 — no key"
    );

    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let c = reqwest::Client::new();
    let mut server = start(&journal, port);
    wait_ready(&c, &base).await;

    // The key was minted at boot, owner-only, and the upgrade mint is
    // stable: the run the v1.0.1 writer committed can be read back by
    // its S2-era public id.
    assert_eq!(
        std::fs::read(journal.join("public_id.key"))
            .unwrap_or_else(|e| panic!("{e}"))
            .len(),
        32,
        "upgrade minted the key"
    );
    let listed = c
        .get(format!("{base}/jobs"))
        .bearer_auth(KEY)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(listed.status().as_u16(), 200);
    let ids = listed
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let run = &ids["runs"][0];
    let id = run["id"].as_str().unwrap_or_else(|| panic!("{ids}")).to_string();
    assert_eq!(
        run["key"].as_str(),
        Some("legacy"),
        "the replayed run is listed under a public id: {ids}"
    );
    assert_run_survives(&c, &base, &id, "after the v1.0.x upgrade").await;
    let _ = server.kill();
    let _ = server.wait();
}

/// The other half of finding 2's contract at process level: a journal
/// the S2 binary already wrote (v3 envelopes prove the era) that lost
/// its public_id.key must refuse startup — minting would rename every
/// public id clients hold.
#[tokio::test]
async fn a_pubid_era_journal_missing_its_key_refuses_to_start() {
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let journal = d.path().join("journal");
    {
        let mut j = durable_journal::Journal::open(&journal).unwrap_or_else(|e| panic!("{e}"));
        j.append(
            br#"{"v":3,"ws":0,"run":1,"at":1000,"event":{"Accepted":{"key":"era","spec":{"version":1,"steps":[{"name":"a","handler":"step"}],"input":[104,105]},"not_before":9999999999999}}}"#,
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }
    let port = free_port();
    let mut cmd = server_cmd(&journal, port);
    cmd.args(["--handler", "step=http://127.0.0.1:9/unused"])
        .stderr(Stdio::piped());
    let mut server = cmd.spawn().unwrap_or_else(|e| panic!("{e}"));
    let status = match exit_within(&mut server, Duration::from_secs(10)).await {
        Some(s) => s,
        None => {
            let _ = server.kill();
            let _ = server.wait();
            panic!("a key-less pubid-era journal let the server start");
        }
    };
    let mut err = String::new();
    if let Some(mut e) = server.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut e, &mut err);
    }
    assert!(!status.success(), "exit {status}");
    assert!(
        err.contains("no public-id key"),
        "stderr names the missing key: {err}"
    );
}

/// Checklist P0: unauthenticated reads only on a loopback listener.
#[tokio::test]
async fn open_reads_off_loopback_refuses_startup() {
    let port = free_port();
    let listen = format!("0.0.0.0:{port}");
    refuses_to_start(
        &[("DURABLE_API_KEY", KEY)],
        &["--open-reads", "--listen", &listen],
        "--open-reads is allowed only on a loopback",
    )
    .await;
}

/// A listener that answers every connection with `reply` and counts them.
async fn counting(reply: &'static str) -> (u16, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let port = l.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(reply.as_bytes()).await;
            });
        }
    });
    (port, seen)
}

/// Checklist: the handler client honoured HTTP_PROXY and friends, so an
/// environment variable could route every job's input through a proxy.
/// Handler calls go straight to the registered URL.
#[tokio::test]
async fn handler_calls_ignore_proxy_environment_variables() {
    use std::sync::atomic::Ordering::SeqCst;
    let (proxy, via_proxy) =
        counting("HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n").await;
    let (handler, direct) = counting("HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}").await;
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let proxy_url = format!("http://127.0.0.1:{proxy}");
    let mut server = server_cmd(&d.path().join("journal"), port)
        .args([
            "--handler",
            &format!("step=http://127.0.0.1:{handler}/step"),
        ])
        .env("HTTP_PROXY", &proxy_url)
        .env("http_proxy", &proxy_url)
        .env("HTTPS_PROXY", &proxy_url)
        .env("ALL_PROXY", &proxy_url)
        .env("all_proxy", &proxy_url)
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .spawn()
        .unwrap_or_else(|e| panic!("{e}"));
    let c = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap_or_else(|e| panic!("{e}"));
    wait_healthy(&c, &base).await;
    let body = serde_json::json!({
        "op_key": "proxy-check",
        "spec": { "version": 1, "steps": [{ "name": "a", "handler": "step" }], "input_b64": "aGk=" },
    });
    let r = c
        .post(format!("{base}/jobs"))
        .bearer_auth(KEY)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(r.status().is_success(), "{}", r.status());
    let deadline = Instant::now() + Duration::from_secs(15);
    while direct.load(SeqCst) + via_proxy.load(SeqCst) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = server.kill();
    let _ = server.wait();
    assert_eq!(
        via_proxy.load(SeqCst),
        0,
        "a handler call went through the proxy"
    );
    assert!(direct.load(SeqCst) >= 1, "the handler was never called");
}

/// Review U7: SIGTERM with a request still open when the drain timeout
/// ran out exited 0, though OPERATING says that is a non-zero exit. The
/// engine still drains (nothing durable is at risk), but a response was
/// cut, so the exit says so.
#[tokio::test]
async fn sigterm_with_a_request_still_open_exits_non_zero() {
    use tokio::io::AsyncWriteExt as _;
    let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let c = reqwest::Client::new();
    let mut server = server_cmd(&d.path().join("journal"), port)
        .args(["--handler", "step=http://127.0.0.1:9/unused"])
        .args(["--drain-timeout-secs", "1"])
        .args([
            "--body-deadline-ms",
            "60000",
            "--header-timeout-ms",
            "60000",
        ])
        .spawn()
        .unwrap_or_else(|e| panic!("{e}"));
    wait_healthy(&c, &base).await;
    // A submit whose body never finishes: its handler is in flight.
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let head = format!(
        "POST /jobs HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {KEY}\r\n\
         content-type: application/json\r\ncontent-length: 1000\r\n\r\n{{\"op"
    );
    sock.write_all(head.as_bytes())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    tokio::time::sleep(Duration::from_millis(200)).await;
    signal(&server, "-TERM");
    let status = match exit_within(&mut server, Duration::from_secs(20)).await {
        Some(s) => s,
        None => {
            let _ = server.kill();
            let _ = server.wait();
            panic!("the server did not exit within 20 s of SIGTERM");
        }
    };
    drop(sock);
    assert!(
        !status.success(),
        "a drain that timed out with a request open must not exit 0: {status}"
    );
}

/// D-056: `--journal-format` prints the binary's journal-format
/// capabilities — `<max record version> <max DRIX version> <parks ws≠0>`
/// since v1.0.2 — and exits. No keys needed: upgrade.sh and rollback.sh
/// call it while making a decision, not while serving.
#[test]
fn journal_format_reports_the_reader_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_durable-server"))
        .arg("--journal-format")
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(out.status.success(), "status: {:?}", out.status);
    let nums: Vec<u64> = String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(|n| {
            n.parse()
                .unwrap_or_else(|_| panic!("stdout: {:?}", out.stdout))
        })
        .collect();
    assert_eq!(
        nums,
        vec![
            durable_engine::READER_VERSION as u64,
            durable_engine::snapshot::VERSION as u64,
            1
        ],
        "records, snapshots, parks-ws≠0"
    );
}
