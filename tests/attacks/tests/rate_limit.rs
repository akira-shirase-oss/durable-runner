//! Rate-limit evasion attempts: reconnects and floods must not drain the
//! bucket for anyone else and must not reset their own.
//!
//! Prove-test: `the_bucket_refuses_after_its_tokens` goes red when
//! `ATTACK(rate-limit)` stops building the limiter.

use durable_attacks::*;

/// Prove-test for `ATTACK(rate-limit)`.
#[tokio::test]
async fn the_bucket_refuses_after_its_tokens() {
    // 2/s with a 1 s burst → ~4 requests pass, the rest are 429.
    let s = spawn(&["--rate-limit-per-addr", "2"]).await;
    let c = http();
    let mut refusals = 0usize;
    let mut retry_seen = false;
    for _ in 0..40 {
        let r = c
            .get(format!("{}/health", s.base))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if r.status().as_u16() == 429 {
            refusals += 1;
            retry_seen |= r.headers().get("retry-after").is_some();
        }
    }
    // DECISIVE(rate-limit)
    assert!(refusals >= 20, "the bucket never refused ({refusals}/40)");
    assert!(retry_seen, "a 429 without Retry-After");
}

/// A refused client that reconnects does not get a fresh bucket: the
/// table keys on the address, not the connection.
#[tokio::test]
async fn reconnecting_does_not_reset_the_bucket() {
    let s = spawn(&["--rate-limit-per-addr", "3"]).await;
    // Force one request per connection (Connection: close each time) so a
    // fresh bucket would mean reconnect evasion works.
    let c = http();
    let mut refusals = 0usize;
    for _ in 0..30 {
        let r = c
            .get(format!("{}/health", s.base))
            .header("connection", "close")
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if r.status().as_u16() == 429 {
            refusals += 1;
        }
    }
    assert!(
        refusals >= 10,
        "reconnects reset the bucket: only {refusals} refusals in 30 connections"
    );
}
