//! Probes for the observer's bounds (D-058, D-060): what it really costs
//! at its caps, and per event. Ignored in the suite (they measure, they do not
//! assert speed); run them in release:
//!
//! ```sh
//! cargo test --release -p durable-engine --test observe_probe -- --ignored --nocapture
//! ```

use durable_domain::RunId;
use durable_engine::observe::{
    memory_bound, CallGuard, CallResult, Kind, Observed, Observer, Scope, Window, COARSE_BUCKETS,
    COARSE_BUCKET_MS, MAX_HANDLER_STATS_PER_WS, MAX_TIMELINES, MAX_TIMELINE_EVENTS,
    MAX_TRACKED_WORKSPACES,
};
use std::sync::Arc;
use std::time::Instant;

const T0: u64 = 1_764_000_000_000;

/// This process's resident set size in KiB, from `ps` (macOS and Linux).
fn rss_kib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap_or_else(|e| panic!("ps: {e}"));
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("ps output: {e}"))
}

fn failed(run: u64, at: u64) -> Observed {
    Observed {
        // Runs spread over every tracked tenant plus workspace 0.
        ws: run % (MAX_TRACKED_WORKSPACES as u64 + 1),
        run: RunId(run),
        at,
        first: Kind::StepFailed { step: 0 },
        then: None,
    }
}

/// Fill every structure to its cap — 20,000 full timelines, every
/// workspace's feed and both rings in every bucket (workspace 0 and all
/// tracked tenants), ten workspace-0 handlers and every tenant's handler
/// stats — and report the RSS growth beside the size-based bound
/// OPERATING.md quotes. Handler calls go through `CallGuard`, so their
/// latencies are the probe's real elapsed times; the buckets are filled
/// by sampling across the day.
#[test]
#[ignore = "probe: run in release with --nocapture"]
fn observer_memory_at_its_caps() {
    let handlers: Vec<String> = (0..10).map(|i| format!("h{i}")).collect();
    let before = rss_kib();
    let obs = Arc::new(Observer::new(T0));
    let empty = rss_kib();
    // Every bucket of the day-long coarse ring (and so every fine one).
    let span = COARSE_BUCKETS as u64 * COARSE_BUCKET_MS;
    let mut batch = Vec::with_capacity(1024);
    let mut n = 0u64;
    for run in 1..=MAX_TIMELINES as u64 {
        for i in 0..MAX_TIMELINE_EVENTS as u64 + 4 {
            let at = T0 + (n * 7_919) % span;
            n += 1;
            batch.push(if i == 0 {
                Observed {
                    ws: run % (MAX_TRACKED_WORKSPACES as u64 + 1),
                    run: RunId(run),
                    at,
                    first: Kind::Accepted { not_before: at },
                    then: None,
                }
            } else {
                failed(run, at)
            });
            if batch.len() == batch.capacity() {
                obs.committed(&batch, true);
                batch.clear();
            }
        }
    }
    obs.committed(&batch, true);
    // Every handler-stats slot: ten in workspace 0, the cap in each tenant.
    for h in &handlers {
        CallGuard::start(&obs, 0, Some(h)).finish(CallResult::Ok);
    }
    for ws in 1..=MAX_TRACKED_WORKSPACES as u64 {
        for i in 0..MAX_HANDLER_STATS_PER_WS {
            CallGuard::start(&obs, ws, Some(&format!("t{i}"))).finish(CallResult::Ok);
        }
    }
    obs.set_ws_index_gauges(|_| (1, 1));
    for k in 0..(COARSE_BUCKETS as u64 * 6) {
        obs.sample(T0 + k * (COARSE_BUCKET_MS / 6));
    }
    let full = rss_kib();
    let m = obs.metrics(
        Scope::All,
        Window::H24,
        T0 + span - 1,
        Default::default(),
        &handlers,
    );
    assert_eq!(m.points.len(), COARSE_BUCKETS);
    assert_eq!(obs.timeline_stats().0, MAX_TIMELINES);
    println!(
        "observer RSS: empty {} KiB over the process, full {} KiB over the process \
         ({} KiB filling); size-based bound {} KiB ({} handlers)",
        empty.saturating_sub(before),
        full.saturating_sub(before),
        full.saturating_sub(empty),
        memory_bound(handlers.len()) / 1024,
        handlers.len()
    );
}

/// What one committed event costs the actor: replay (timelines + feed)
/// and live (plus the metrics ring), in batches as the engine hands them.
#[test]
#[ignore = "probe: run in release with --nocapture"]
fn observer_cost_per_event() {
    const EVENTS: u64 = 2_000_000;
    for live in [false, true] {
        let obs = Observer::new(T0);
        let mut batch = Vec::with_capacity(1024);
        let started = Instant::now();
        for i in 0..EVENTS {
            // 40,000 distinct runs: twice the store, so evictions happen.
            batch.push(failed(i % 40_000 + 1, T0 + i / 100));
            if batch.len() == batch.capacity() {
                obs.committed(&batch, live);
                batch.clear();
            }
        }
        obs.committed(&batch, live);
        let ns = started.elapsed().as_nanos() as f64 / EVENTS as f64;
        println!(
            "observer cost: {ns:.0} ns/event ({}), {} evictions",
            if live { "live" } else { "replay" },
            obs.timeline_stats().1
        );
    }
}
