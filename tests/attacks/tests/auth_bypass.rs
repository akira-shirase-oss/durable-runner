//! Auth bypass attacks against a running server. Every refusal is
//! asserted on its specific status + envelope code, then the workspace is
//! re-read to prove nothing was recorded.
//!
//! Prove-test: `a_garbage_bearer_is_refused_and_nothing_is_recorded` goes
//! red when `ATTACK(auth)` (the `Keyring::verify` match in both extractors)
//! is mutated to grant every bearer.

use durable_attacks::*;

/// Prove-test for `ATTACK(auth)`.
#[tokio::test]
async fn a_garbage_bearer_is_refused_and_nothing_is_recorded() {
    let s = spawn(&[]).await;
    let c = http();
    // `dr_` + 48 hex: the cheap store-key shape check passes, the store
    // (and the env keys) must still deny it.
    let r = c
        .post(format!("{}/jobs", s.base))
        .bearer_auth(FORGED_KEY)
        .json(&submit_body("forged-1"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 401, "forged bearer must be denied");
    let (_, code) = code(r).await;
    assert_eq!(code, "unauthorized");
    // Nothing was recorded behind the refusal.
    let l = c
        .get(format!("{}/jobs", s.base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        v["runs"].as_array().map(Vec::len),
        Some(0),
        "a denied submit still recorded a run"
    );
}

#[tokio::test]
async fn a_missing_bearer_is_refused_401() {
    let s = spawn(&[]).await;
    let c = http();
    let r = c
        .post(format!("{}/jobs", s.base))
        .json(&submit_body("nobearer"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 401);
    assert_eq!(
        r.headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer"),
        "a 401 must name the scheme it wants"
    );
    let (_, code) = code(r).await;
    assert_eq!(code, "unauthorized");
}

#[tokio::test]
async fn the_wrong_scheme_and_mangled_bearer_are_refused() {
    let s = spawn(&[]).await;
    let c = http();
    for (name, value) in [
        ("basic scheme", format!("Basic {}", OPS_KEY)),
        ("key without scheme", OPS_KEY.to_string()),
        ("lower-cased but wrong token", "bearer dr_nope".to_string()),
        ("empty bearer", "Bearer".to_string()),
    ] {
        let r = c
            .post(format!("{}/jobs", s.base))
            .header("authorization", value)
            .json(&submit_body("scheme"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            (401, "unauthorized"),
            "{name} must be denied"
        );
    }
}

/// A key in the query string or a custom header is not authentication:
/// only `Authorization: Bearer` counts.
#[tokio::test]
async fn a_key_outside_the_authorization_header_does_not_count() {
    let s = spawn(&[]).await;
    let c = http();
    for (name, req) in [
        (
            "query string",
            c.post(format!("{}/jobs?key={OPS_KEY}", s.base))
                .json(&submit_body("q1")),
        ),
        ("x-api-key", {
            c.post(format!("{}/jobs", s.base))
                .header("x-api-key", OPS_KEY)
                .json(&submit_body("q2"))
        }),
    ] {
        let r = req.send().await.unwrap_or_else(|e| panic!("{name}: {e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            (401, "unauthorized"),
            "{name} must not authenticate"
        );
    }
}

/// The read key is a real grant — for reads. Every mutating and ops route
/// must refuse it 403, not 401: it is recognised and out of scope.
#[tokio::test]
async fn the_read_key_cannot_mutate_or_administrate() {
    let s = spawn(&[]).await;
    let c = http();
    for (name, req) in [
        (
            "submit",
            c.post(format!("{}/jobs", s.base))
                .bearer_auth(READ_KEY)
                .json(&submit_body("rk")),
        ),
        (
            "compact",
            c.post(format!("{}/admin/compact", s.base))
                .bearer_auth(READ_KEY)
                .json(&serde_json::json!({})),
        ),
        (
            "put handler",
            c.put(format!("{}/handlers/x", s.base))
                .bearer_auth(READ_KEY)
                .json(&serde_json::json!({ "url": "http://203.0.113.9/h" })),
        ),
        (
            "ops diagnostics",
            c.get(format!("{}/admin/diagnostics", s.base))
                .bearer_auth(READ_KEY),
        ),
        (
            "workspace create",
            c.post(format!("{}/admin/workspaces", s.base))
                .bearer_auth(READ_KEY)
                .json(&serde_json::json!({ "name": "x" })),
        ),
    ] {
        let r = req.send().await.unwrap_or_else(|e| panic!("{name}: {e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            (403, "forbidden"),
            "{name}: read key must be refused 403"
        );
    }
}

/// The same ops routes are 401 (not 404) without a key — no route
/// information leaks to the unauthenticated.
#[tokio::test]
async fn ops_routes_deny_before_disclosing() {
    let s = spawn(&[]).await;
    let c = http();
    for (path, req) in [
        (
            "/admin/workspaces",
            c.get(format!("{}/admin/workspaces", s.base)),
        ),
        (
            "/admin/diagnostics",
            c.get(format!("{}/admin/diagnostics", s.base)),
        ),
        (
            "/admin/backup",
            c.post(format!("{}/admin/backup", s.base))
                .json(&serde_json::json!({ "name": "b" })),
        ),
        (
            "/admin/compact",
            c.post(format!("{}/admin/compact", s.base))
                .json(&serde_json::json!({})),
        ),
    ] {
        let r = req.send().await.unwrap_or_else(|e| panic!("{e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            (401, "unauthorized"),
            "{path} must deny, not disclose"
        );
    }
}
