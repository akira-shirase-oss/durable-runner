//! SSRF attacks: the handler registry is the server's only outbound-call
//! surface, so it is the SSRF boundary. Two protections, two prove-tests:
//!
//! - `ATTACK(ssrf-register)` — `check_url` at PUT time refuses
//!   literal-local hosts and non-http schemes.
//! - `ATTACK(ssrf-resolve)` — `validate_addrs` inside the dial-time
//!   resolver refuses a domain that resolves local (no TOCTOU).
//!
//! The spawned server has NO `--allow-local-handlers`, so both layers are
//! armed.

use durable_attacks::*;
use std::io::Write as _;
use std::net::TcpListener as StdListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Prove-test for `ATTACK(ssrf-register)`.
#[tokio::test]
async fn local_handler_urls_are_refused_at_registration() {
    let s = spawn(&[]).await;
    let c = http();
    // url | expected refusal code (422 for local targets, 400 for non-http)
    let cases: &[(&str, &str)] = &[
        ("http://127.0.0.1/", "local_target_forbidden"),
        ("http://127.0.0.1:8080/internal", "local_target_forbidden"),
        ("http://127.1/", "local_target_forbidden"), // shorthand form of 127.0.0.1
        ("http://2130706433/", "local_target_forbidden"), // decimal form of 127.0.0.1
        ("http://0x7f000001/", "local_target_forbidden"), // hex form of 127.0.0.1
        ("http://[::1]/", "local_target_forbidden"),
        ("http://[::ffff:7f00:1]/", "local_target_forbidden"), // v4-mapped loopback
        ("http://0.0.0.0/", "local_target_forbidden"),
        ("http://10.9.8.7/", "local_target_forbidden"),
        ("http://192.168.7.7/", "local_target_forbidden"),
        (
            "http://169.254.169.254/latest/meta-data",
            "local_target_forbidden",
        ),
        ("http://[fe80::1]/", "local_target_forbidden"),
        ("file:///etc/passwd", "bad_handler_url"),
        ("gopher://127.0.0.1/x", "bad_handler_url"),
        ("ftp://10.1.2.3/f", "bad_handler_url"),
        ("http://", "bad_handler_url"),
    ];
    for (url, want) in cases {
        let r = c
            .put(format!("{}/handlers/evil", s.base))
            .bearer_auth(OPS_KEY)
            .json(&serde_json::json!({ "url": url }))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{url}: {e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            if *want == "bad_handler_url" {
                (400, *want)
            } else {
                (422, *want)
            },
            "{url} was not refused as {want}"
        );
    }
    // Re-read the registry: nothing refused made it in.
    let l = c
        .get(format!("{}/handlers", s.base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    let names: Vec<&str> = v["handlers"]
        .as_array()
        .map(|a| a.iter().filter_map(|h| h["name"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        !names.contains(&"evil"),
        "a refused URL was registered anyway: {v}"
    );
}

/// Prove-test for `ATTACK(ssrf-resolve)`: a *domain* cannot be judged at
/// PUT time — the dial-time resolver must refuse a name that resolves
/// local, before any packet leaves. A listener on loopback counts
/// connections; a refused resolution never reaches it.
#[tokio::test]
async fn a_domain_that_resolves_local_never_gets_a_packet() {
    // The victim listener: answers 200 to anything that reaches it.
    let victim = StdListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("{e}"));
    victim
        .set_nonblocking(true)
        .unwrap_or_else(|e| panic!("{e}"));
    let lport = victim.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = hits.clone();
        std::thread::spawn(move || {
            for mut conn in victim.incoming().flatten() {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            }
        });
    }

    let s = spawn(&[]).await;
    let c = http();
    // `localhost` is a domain: check_url accepts it at PUT and defers the
    // verdict to the dial-time resolver.
    let url = format!("http://localhost:{lport}/step");
    let r = c
        .put(format!("{}/handlers/step", s.base))
        .bearer_auth(OPS_KEY)
        .json(&serde_json::json!({ "url": url }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 200, "a domain handler should register");

    // Dispatch a run at it — not_before 0 so the engine fires now.
    let body = serde_json::json!({
        "op_key": "ssrf-probe",
        "spec": {
            "version": 1,
            "steps": [{ "name": "a", "handler": "step" }],
            "input_b64": "aGk="
        },
    });
    let r = c
        .post(format!("{}/jobs", s.base))
        .bearer_auth(OPS_KEY)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 200, "setup submit failed");
    let run_id = r
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|e| panic!("{e}"))["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no run id"))
        .to_string();

    // The signal: the run *attempted* its step and the refusal parked it
    // (retrying while attempts remain, then failed). Wait for either —
    // both prove the dispatch tried to dial.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut state = String::new();
    while std::time::Instant::now() < deadline {
        let r = c
            .get(format!("{}/jobs/{run_id}", s.base))
            .bearer_auth(OPS_KEY)
            .send()
            .await;
        if let Ok(r) = r {
            if r.status().as_u16() == 200 {
                let v: serde_json::Value = r.json().await.unwrap_or_else(|e| panic!("{e}"));
                state = v["state"].as_str().unwrap_or("").to_string();
                if state.starts_with("retry_waiting")
                    || state.starts_with("failed")
                    || state == "done"
                {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        state.starts_with("retry_waiting") || state.starts_with("failed"),
        "the refused call should park the run, got state {state:?}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "the resolver let a packet reach the local listener"
    );
}

/// Prove-test for `ATTACK(tenant-strict-ssrf)` (re-review S1-1): the
/// operator's `--allow-local-handlers` opens their own `--handler` flags
/// only. With it set, a tenant still cannot register a local target —
/// literal forms refuse at PUT — and a name resolving local, accepted at
/// PUT, never sends a packet at call time. Workspace 0's API
/// registrations are store rows too: strict as well.
#[tokio::test]
async fn the_operators_local_opt_out_never_reaches_a_tenant() {
    let victim = StdListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("{e}"));
    victim
        .set_nonblocking(true)
        .unwrap_or_else(|e| panic!("{e}"));
    let lport = victim.local_addr().unwrap_or_else(|e| panic!("{e}")).port();
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = hits.clone();
        std::thread::spawn(move || {
            for mut conn in victim.incoming().flatten() {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            }
        });
    }
    // The operator follows the deploy guide for a same-host handler.
    let s = spawn(&["--allow-local-handlers"]).await;
    let c = http();
    let ws = create_workspace(&c, &s.base, "tenant").await;
    let tenant = create_key(&c, &s.base, &ws, "admin").await;
    let put = |key: String, url: String| {
        let c = c.clone();
        let base = s.base.clone();
        async move {
            let r = c
                .put(format!("{base}/handlers/evil"))
                .bearer_auth(key)
                .json(&serde_json::json!({ "url": url }))
                .send()
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            code(r).await
        }
    };
    for url in [
        format!("http://127.0.0.1:{lport}/step"),
        "http://169.254.169.254/latest/meta-data".to_string(),
        "http://10.0.0.7/h".to_string(),
        "http://[::1]:22/".to_string(),
    ] {
        for key in [tenant.clone(), OPS_KEY.to_string()] {
            let (status, code) = put(key, url.clone()).await;
            assert_eq!(
                (status, code.as_str()),
                (422, "local_target_forbidden"),
                "{url}: an API registration used the operator's local opt-out"
            );
        }
    }
    // A name resolving local registers (names resolve at dial) — and the
    // tenant's call must still be refused before a packet leaves.
    let (status, _) = put(tenant.clone(), format!("http://localhost:{lport}/step")).await;
    assert_eq!(status, 200, "a domain registers; the dial decides");
    let body = serde_json::json!({
        "op_key": "tenant-local",
        "spec": {
            "version": 1,
            "steps": [{ "name": "a", "handler": "evil" }],
            "input_b64": "aGk="
        },
    });
    let r = c
        .post(format!("{}/jobs", s.base))
        .bearer_auth(&tenant)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 200, "setup submit failed");
    let v: serde_json::Value = r.json().await.unwrap_or_else(|e| panic!("{e}"));
    let run = v["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("{v}"))
        .to_string();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut state = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(r) = c
            .get(format!("{}/jobs/{run}", s.base))
            .bearer_auth(&tenant)
            .send()
            .await
        {
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            state = v["state"].as_str().unwrap_or("").to_string();
            if state.starts_with("retry_waiting") || state.starts_with("failed") || state == "done"
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        state.starts_with("retry_waiting") || state.starts_with("failed"),
        "the tenant's local call should be refused, got state {state:?}"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "a tenant handler reached a local listener through the operator's opt-out"
    );
}
