//! Cross-tenant attacks: a second workspace's admin key probing the first
//! tenant's runs, and a tenant key probing ops routes. Refusals are the
//! no-oracle 404 for ids and 403 for ops scope; state is re-read as the
//! owner afterwards.
//!
//! Prove-tests: `a_second_workspace_cannot_read_anothers_run` goes red
//! when `ATTACK(tenant-scope)` is mutated; `a_tenant_admin_cannot_reach_
//! ops_routes` when `ATTACK(ops-scope)` is mutated.

use durable_attacks::*;

/// Prove-test for `ATTACK(tenant-scope)`.
#[tokio::test]
async fn a_second_workspace_cannot_read_anothers_run() {
    let s = spawn(&[]).await;
    let c = http();
    let (status, run) = submit(&c, &s.base, OPS_KEY, "ws0-run").await;
    assert_eq!(status, 200, "setup submit failed");
    let run_id = run.unwrap_or_else(|| panic!("no run id"));

    let ws_b = create_workspace(&c, &s.base, "tenant-b").await;
    let key_b = create_key(&c, &s.base, &ws_b, "admin").await;

    for (name, req) in [
        (
            "inspect",
            c.get(format!("{}/jobs/{run_id}", s.base))
                .bearer_auth(&key_b),
        ),
        (
            "spec",
            c.get(format!("{}/jobs/{run_id}/spec", s.base))
                .bearer_auth(&key_b),
        ),
        (
            "cancel",
            c.post(format!("{}/jobs/{run_id}/cancel", s.base))
                .bearer_auth(&key_b),
        ),
    ] {
        let r = req.send().await.unwrap_or_else(|e| panic!("{name}: {e}"));
        let (status, code) = code(r).await;
        // 404, never 403: a foreign id must leak nothing (S2-2).
        assert_eq!(
            (status, code.as_str()),
            (404, "not_found"),
            "{name}: cross-tenant id must 404"
        );
    }

    // B's list stays empty of A's run even when it exists.
    let l = c
        .get(format!("{}/jobs", s.base))
        .bearer_auth(&key_b)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let v: serde_json::Value = l.json().await.unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        v["runs"].as_array().map(Vec::len),
        Some(0),
        "tenant B saw A's run"
    );

    // The owner still reads its run — nothing was disturbed.
    let r = c
        .get(format!("{}/jobs/{run_id}", s.base))
        .bearer_auth(OPS_KEY)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(r.status().as_u16(), 200, "owner lost its run");
}

/// Prove-test for `ATTACK(ops-scope)`.
#[tokio::test]
async fn a_tenant_admin_cannot_reach_ops_routes() {
    let s = spawn(&[]).await;
    let c = http();
    let ws_b = create_workspace(&c, &s.base, "tenant-b").await;
    let key_b = create_key(&c, &s.base, &ws_b, "admin").await;

    for (name, req) in [
        (
            "create workspace",
            c.post(format!("{}/admin/workspaces", s.base))
                .bearer_auth(&key_b)
                .json(&serde_json::json!({ "name": "y" })),
        ),
        (
            "list workspaces",
            c.get(format!("{}/admin/workspaces", s.base))
                .bearer_auth(&key_b),
        ),
        (
            "ops diagnostics",
            c.get(format!("{}/admin/diagnostics", s.base))
                .bearer_auth(&key_b),
        ),
        (
            "backup",
            c.post(format!("{}/admin/backup", s.base))
                .bearer_auth(&key_b)
                .json(&serde_json::json!({ "name": "b" })),
        ),
    ] {
        let r = req.send().await.unwrap_or_else(|e| panic!("{name}: {e}"));
        let (status, code) = code(r).await;
        assert_eq!(
            (status, code.as_str()),
            (403, "forbidden"),
            "{name}: tenant admin must be refused"
        );
    }
}

/// A numeric run id — the ws-0 compat path — must not be an oracle for
/// other tenants either: ws-B's decode refuses it before any ws check.
#[tokio::test]
async fn a_numeric_run_id_is_no_oracle_for_other_tenants() {
    let s = spawn(&[]).await;
    let c = http();
    let (status, run) = submit(&c, &s.base, OPS_KEY, "ws0-run").await;
    assert_eq!(status, 200);
    assert!(run.is_some(), "setup submit failed");
    let ws_b = create_workspace(&c, &s.base, "tenant-b").await;
    let key_b = create_key(&c, &s.base, &ws_b, "admin").await;

    // Internal ids are never issued externally, but a tenant asking for
    // run 1 numerically must get 404, never a decode of another's run.
    let r = c
        .get(format!("{}/jobs/1", s.base))
        .bearer_auth(&key_b)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, code) = code(r).await;
    assert_eq!((status, code.as_str()), (404, "not_found"));
}
