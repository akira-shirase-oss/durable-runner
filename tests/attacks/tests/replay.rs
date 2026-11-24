//! Replay attacks: a resent request must not create a second durable
//! effect. The submit's idempotency key dedups per (ws, key); a replayed
//! PUT is idempotent by construction — both asserted by re-reading state.
//!
//! Prove-test: `a_replayed_submit_returns_the_same_run` goes red when
//! `ATTACK(submit-dedup)` removes the `by_op` check.

use durable_attacks::*;

/// Prove-test for `ATTACK(submit-dedup)`.
#[tokio::test]
async fn a_replayed_submit_returns_the_same_run() {
    let s = spawn(&[]).await;
    let c = http();
    let (s1, run1) = submit(&c, &s.base, OPS_KEY, "replay-me").await;
    assert_eq!(s1, 200, "first submit failed");
    let run1 = run1.unwrap_or_else(|| panic!("no run id"));

    // The replay: same op_key, same spec — byte-for-byte the first POST.
    let (s2, run2) = submit(&c, &s.base, OPS_KEY, "replay-me").await;
    assert_eq!(s2, 200, "the replayed submit must still be answered");
    // DECISIVE(submit-dedup)
    assert_eq!(
        run2.as_deref(),
        Some(run1.as_str()),
        "a replay created a second run"
    );

    // And again, harder — a third replay proves it stays deduplicated.
    let (_, run3) = submit(&c, &s.base, OPS_KEY, "replay-me").await;
    assert_eq!(run3.as_deref(), Some(run1.as_str()));

    // Re-read the workspace: exactly one run exists for the replayed key.
    let l = c
        .get(format!("{}/jobs", s.base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    let runs = v["runs"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(runs, 1, "a replayed submit stored {runs} runs");
}

/// A replayed PUT of the same handler is idempotent: the registry holds
/// exactly one entry for the name, not one per request.
#[tokio::test]
async fn a_replayed_handler_put_stays_one_entry() {
    let s = spawn(&[]).await;
    let c = http();
    for i in 0..3 {
        let r = c
            .put(format!("{}/handlers/rep", s.base))
            .bearer_auth(OPS_KEY)
            .json(&serde_json::json!({ "url": "http://8.8.8.8/hook" }))
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {i}: {e}"));
        assert_eq!(r.status().as_u16(), 200, "replayed PUT must stay answered");
    }
    let l = c
        .get(format!("{}/handlers", s.base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    let count = v["handlers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|h| h["name"].as_str() == Some("rep"))
                .count()
        })
        .unwrap_or(0);
    assert_eq!(count, 1, "three PUTs stored {count} registry rows");
}
