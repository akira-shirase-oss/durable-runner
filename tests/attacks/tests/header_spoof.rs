//! Header-spoofing attacks: `cf-connecting-ip` is believed only from a
//! trusted-proxy peer. From an untrusted peer it must not move the rate
//! bucket — the flood still hits 429 keyed on the real address.
//!
//! Prove-test: `spoofed_client_header_does_not_evade_the_bucket` goes red
//! when `ATTACK(cf-header)` is mutated to believe any peer's header.

use durable_attacks::*;
use std::time::Duration;

/// Prove-test for `ATTACK(cf-header)`: the server runs WITHOUT any
/// --trusted-proxy, so every cf-connecting-ip is spoofed. If the header
/// were believed, each request would land in a fresh bucket and the
/// flood would never be limited.
#[tokio::test]
async fn spoofed_client_header_does_not_evade_the_bucket() {
    // ~1 token/s with a 1 s burst: the first few requests pass, then 429.
    let s = spawn(&["--rate-limit-per-addr", "2"]).await;
    let c = http();
    let mut refusals = 0usize;
    for i in 0..60 {
        let fake = format!("203.0.113.{}", i + 1); // a fresh "client" per request
        let r = c
            .get(format!("{}/health", s.base))
            .header("cf-connecting-ip", fake)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if r.status().as_u16() == 429 {
            refusals += 1;
            assert!(
                r.headers().get("retry-after").is_some(),
                "a rate_limited answer must carry Retry-After"
            );
        }
    }
    assert!(
        refusals >= 20,
        "rotating cf-connecting-ip evaded the bucket: only {refusals} refusals in 60 requests"
    );
}

/// When a proxy IS trusted, the header legitimately re-keys the bucket —
/// but a malformed value must not be a free pass either: it falls back to
/// the real peer, which still has its own bucket.
#[tokio::test]
async fn a_malformed_header_is_no_free_pass_behind_a_trusted_proxy() {
    let s = spawn(&[
        "--rate-limit-per-addr",
        "2",
        "--trusted-proxy",
        "127.0.0.0/8",
    ])
    .await;
    let c = http();
    // Behind a trusted proxy the good header re-keys: fresh addresses get
    // their own (small) buckets, so they pass while garbage falls back to
    // the peer's bucket and is limited.
    let mut garbage_refusals = 0usize;
    let mut fresh_passed = 0usize;
    for i in 0..30 {
        let fresh = c
            .get(format!("{}/health", s.base))
            .header("cf-connecting-ip", format!("203.0.113.{}", i + 100))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if fresh.status().is_success() {
            fresh_passed += 1;
        }
        let g = c
            .get(format!("{}/health", s.base))
            .header("cf-connecting-ip", "not-an-ip-address")
            .send()
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if g.status().as_u16() == 429 {
            garbage_refusals += 1;
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    assert!(
        fresh_passed >= 20,
        "trusted cf header did not re-key ({fresh_passed} passed)"
    );
    assert!(
        garbage_refusals >= 10,
        "a malformed header got a free pass ({garbage_refusals} refusals)"
    );
}
