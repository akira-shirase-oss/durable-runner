//! Probes for the observer's bounds (D-058): what it really costs at its
//! caps, and per event. Ignored in the suite (they measure, they do not
//! assert speed); run them in release:
//!
//! ```sh
//! cargo test --release -p durable-engine --test observe_probe -- --ignored --nocapture
//! ```

use durable_domain::RunId;
use durable_engine::observe::{
    memory_bound, CallResult, Kind, Observed, Observer, Window, COARSE_BUCKETS, COARSE_BUCKET_MS,
    MAX_TIMELINES, MAX_TIMELINE_EVENTS,
};
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
        run: RunId(run),
        at,
        first: Kind::StepFailed { step: 0 },
        then: None,
    }
}

/// Fill every structure to its cap — 20,000 full timelines, the whole
/// feed, every bucket of both rings, and ten handlers' rings — and report
/// the RSS growth beside the size-based bound OPERATING.md quotes.
#[test]
#[ignore = "probe: run in release with --nocapture"]
fn observer_memory_at_its_caps() {
    let handlers: Vec<String> = (0..10).map(|i| format!("h{i}")).collect();
    let before = rss_kib();
    let obs = Observer::new(T0, handlers.clone());
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
    for (h, _) in handlers.iter().enumerate() {
        for k in 0..(COARSE_BUCKETS as u64 * 6) {
            let at = T0 + k * (COARSE_BUCKET_MS / 6);
            obs.call_finished(h, CallResult::Ok, k % 70_000, at);
            obs.sample(at);
        }
    }
    let full = rss_kib();
    let m = obs.metrics(Window::H24, T0 + span - 1, Default::default());
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
        let obs = Observer::new(T0, ["h".to_string()]);
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
