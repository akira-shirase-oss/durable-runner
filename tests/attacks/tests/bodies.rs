//! Body attacks: oversized (declared and chunked), slow (dribbled past
//! its deadline), and stalled heads — all at the socket, all asserting
//! the specific refusal.
//!
//! Prove-tests: `an_oversized_body_is_refused_413` (`ATTACK(body-limit)`),
//! `a_dribbled_body_is_cut_at_its_deadline` (`ATTACK(body-deadline)`),
//! `a_partial_head_is_cut` (`ATTACK(head-timeout)`).

use durable_attacks::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

fn addr(base: &str) -> String {
    base.trim_start_matches("http://").to_string()
}

/// Read until response headers end or `within` passes; the status code.
async fn status_line(s: &mut TcpStream, within: Duration) -> u16 {
    let mut buf = Vec::with_capacity(1024);
    let deadline = std::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(!left.is_zero(), "no response head within {within:?}");
        let n = match timeout(left, s.read_buf(&mut buf)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => panic!("socket read: {e}"),
            // DECISIVE(body-deadline)
            Err(_) => panic!("no response head within {within:?}"),
        };
        if n == 0 {
            panic!("connection closed before a response head");
        }
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    head.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("bad status line: {}", head.lines().next().unwrap_or("")))
}

/// Prove-test for `ATTACK(body-limit)`.
#[tokio::test]
async fn an_oversized_body_is_refused_413() {
    let s = spawn(&[]).await;
    let c = http();
    // Declared length past the 256 KiB cap, valid JSON shape otherwise.
    let mut body = submit_body("pad-1");
    body["pad"] = serde_json::json!("x".repeat(300 * 1024));
    let r = c
        .post(format!("{}/jobs", s.ops_base))
        .bearer_auth(OPS_KEY)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, code) = code(r).await;
    // DECISIVE(body-limit)
    assert_eq!((status, code.as_str()), (413, "body_too_large"));
}

/// The same refusal for a chunked body that only becomes too large as it
/// arrives — no Content-Length to pre-judge by.
#[tokio::test]
async fn a_chunked_body_past_the_cap_is_refused_413() {
    let s = spawn(&[]).await;
    let mut conn = TcpStream::connect(addr(&s.ops_base))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    conn.write_all(
        format!(
            "POST /jobs HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {OPS_KEY}\r\n\
             Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n",
            addr(&s.ops_base)
        )
        .as_bytes(),
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    // Five 64 KiB chunks = 320 KiB > the 256 KiB cap.
    let chunk = "x".repeat(64 * 1024);
    for _ in 0..5 {
        conn.write_all(format!("{:x}\r\n{}\r\n", chunk.len(), chunk).as_bytes())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let status = status_line(&mut conn, Duration::from_secs(10)).await;
    assert_eq!(status, 413, "chunked overflow must be 413");
}

/// A lying Content-Length cannot smuggle a body past the cap: the server
/// parses only the declared bytes, which must fail — it never trusts the
/// declaration enough to let the rest through as one request.
#[tokio::test]
async fn a_lying_content_length_cannot_smuggle_the_body() {
    let s = spawn(&[]).await;
    let mut conn = TcpStream::connect(addr(&s.ops_base))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    // Declares 8 bytes, then streams a full submit: the parsed prefix
    // '{"op_key' is malformed JSON and must be refused, and the remainder
    // must not be read as a second valid request that sneaks the job in.
    let body = serde_json::to_vec(&submit_body("smuggle")).unwrap_or_else(|e| panic!("{e}"));
    conn.write_all(
        format!(
            "POST /jobs HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {OPS_KEY}\r\n\
             Content-Type: application/json\r\nContent-Length: 8\r\n\r\n",
            addr(&s.ops_base)
        )
        .as_bytes(),
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    conn.write_all(&body)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let status = status_line(&mut conn, Duration::from_secs(10)).await;
    assert_eq!(
        status, 400,
        "a truncated declared body must be 400 bad_body"
    );
    // The smuggled remainder created nothing.
    let l = http()
        .get(format!("{}/jobs", s.ops_base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        v["runs"].as_array().map(Vec::len),
        Some(0),
        "the lie still recorded a run"
    );
}

/// Prove-test for `ATTACK(body-deadline)`.
#[tokio::test]
async fn a_dribbled_body_is_cut_at_its_deadline() {
    let s = spawn(&["--body-deadline-ms", "700"]).await;
    let mut conn = TcpStream::connect(addr(&s.ops_base))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    conn.write_all(
        format!(
            "POST /jobs HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {OPS_KEY}\r\n\
             Content-Type: application/json\r\nContent-Length: 4096\r\n\r\n{{",
            addr(&s.ops_base)
        )
        .as_bytes(),
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    // Dribble a byte a while — slower than the 700 ms deadline overall.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        if conn.write_all(b" ").await.is_err() {
            break; // already cut — that's the refusal too
        }
    }
    let status = status_line(&mut conn, Duration::from_secs(10)).await;
    assert_eq!(status, 408, "a dribbled body must be 408 at its deadline");
}

/// Prove-test for `ATTACK(head-timeout)`: a partial request head held
/// past the deadline must get the connection cut — the close (EOF) is the
/// signal, asserted within the test's own deadline.
#[tokio::test]
async fn a_partial_head_is_cut() {
    let s = spawn(&["--header-timeout-ms", "700"]).await;
    let mut conn = TcpStream::connect(addr(&s.ops_base))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    conn.write_all(b"POST /jobs HT")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let mut buf = [0u8; 64];
    let n = timeout(Duration::from_secs(10), conn.read(&mut buf))
        .await
        // DECISIVE(head-timeout)
        .unwrap_or_else(|_| panic!("the server held a stalled head past its deadline"))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(n, 0, "expected the connection closed (EOF), read {n} bytes");
}
