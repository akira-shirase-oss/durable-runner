//! Observability (D-058): derived, in memory, bounded. The journal stays
//! the only authority; nothing here is ever read back into the engine.
//!
//! Four structures, each with a fixed cap:
//!
//! - **metrics rings**: 360 × 10 s buckets (one hour) and 1440 × 60 s
//!   buckets (one day) of counters, sampled gauges and a fixed log-bucket
//!   latency histogram. They start empty at `since` (the observer's
//!   creation): replay never backfills them.
//! - **per-handler stats**: the same two rings per registered handler
//!   (the registry is a static allowlist, so the set is bounded).
//! - **run timelines**: at most [`MAX_TIMELINES`] runs, LRU on the last
//!   event, each holding at most [`TIMELINE_HEAD`] + [`TIMELINE_TAIL`]
//!   events (the first 16 and the last 80).
//! - **activity feed**: the last [`FEED_CAP`] events across runs.
//!
//! Timelines and the feed are rebuilt from journal replay at startup
//! (records carry `at`). Who feeds what: the actor, for every committed
//! record once it is durable (and for every replayed one); the executor,
//! for each handler call's latency and outcome; the server, for refused and
//! deduplicated submits; a one-second sampler task, for the gauges.
//!
//! Every method is O(1) per event (a window read is O(window)), takes a
//! `std::sync::Mutex` for a short section with no I/O and no await inside,
//! and is clock-injected: callers pass `now`, so tests are deterministic.
//! Nothing here holds or shows a job's input or a checkpoint's output.

use durable_domain::{Event, Millis, RunId, RunState};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// The fine ring: 10-second buckets, one hour of them.
pub const FINE_BUCKET_MS: u64 = 10_000;
pub const FINE_BUCKETS: usize = 360;
/// The coarse ring: 60-second buckets, one day of them.
pub const COARSE_BUCKET_MS: u64 = 60_000;
pub const COARSE_BUCKETS: usize = 1440;
/// Runs whose timelines are kept (LRU on the last event).
pub const MAX_TIMELINES: usize = 20_000;
/// A timeline past [`MAX_TIMELINE_EVENTS`] keeps its first this-many events…
pub const TIMELINE_HEAD: usize = 16;
/// …and its last this-many.
pub const TIMELINE_TAIL: usize = 80;
pub const MAX_TIMELINE_EVENTS: usize = TIMELINE_HEAD + TIMELINE_TAIL;
/// Events the activity feed keeps across runs.
pub const FEED_CAP: usize = 1_000;
/// The most feed events one `GET /events` returns.
pub const MAX_FEED_PAGE: usize = 200;
/// Gauges are sampled this often (the last sample in a bucket wins, so a
/// bucket's gauges are its value within this long of the bucket's end).
pub const SAMPLE_EVERY_MS: u64 = 1_000;

/// Upper bounds, in ms, of the latency histogram's buckets: a 1-2-3-4-5-6-8
/// log scale, so a bucket is at most a third wider than the one below
/// (above 4 ms; 1 ms resolution below). A latency above the last bound
/// lands in one overflow bucket. A percentile is the upper bound of the
/// bucket that holds it, capped at the exact maximum — an approximation
/// that over-reports by at most one bucket width, never under-reports.
pub const LATENCY_BOUNDS_MS: [u64; 46] = [
    1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 80, 100, 120, 150, 200, 250, 300, 400,
    500, 600, 800, 1_000, 1_200, 1_500, 2_000, 2_500, 3_000, 4_000, 5_000, 6_000, 8_000, 10_000,
    12_000, 15_000, 20_000, 25_000, 30_000, 40_000, 50_000, 60_000,
];
const HIST_BUCKETS: usize = LATENCY_BOUNDS_MS.len() + 1;

/// A bucket slot that has never been written.
const EMPTY: Millis = u64::MAX;

/// The wall clock in Unix milliseconds (0 if the clock is before 1970).
pub fn wall_ms() -> Millis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Millis)
        .unwrap_or(0)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic under one of these locks leaves plain counters behind: the
    // data is still meaningful, so poisoning is not a reason to stop.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ------------------------------------------------------------------ windows

/// The windows `GET /metrics` serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    M15,
    H1,
    H6,
    H24,
}

impl Window {
    /// `15m`, `1h`, `6h` or `24h`; anything else is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "15m" => Some(Self::M15),
            "1h" => Some(Self::H1),
            "6h" => Some(Self::H6),
            "24h" => Some(Self::H24),
            _ => None,
        }
    }

    pub fn window_ms(self) -> u64 {
        match self {
            Self::M15 => 15 * 60_000,
            Self::H1 => 60 * 60_000,
            Self::H6 => 6 * 60 * 60_000,
            Self::H24 => 24 * 60 * 60_000,
        }
    }

    /// 10 s buckets (the fine ring) for 15m and 1h, 60 s (the coarse ring)
    /// for 6h and 24h.
    pub fn bucket_ms(self) -> u64 {
        match self {
            Self::M15 | Self::H1 => FINE_BUCKET_MS,
            Self::H6 | Self::H24 => COARSE_BUCKET_MS,
        }
    }

    /// Buckets in the window: the last of them is the one holding `now`.
    pub fn points(self) -> usize {
        (self.window_ms() / self.bucket_ms()) as usize
    }

    fn fine(self) -> bool {
        self.bucket_ms() == FINE_BUCKET_MS
    }
}

// ------------------------------------------------------------------- events

/// How a run ended (the derived `finished` event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Done,
    Failed,
    Cancelled,
}

/// What a timeline or the feed shows of a committed record: kind and the
/// small numeric fields only — never an input, an output or a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Kind {
    Accepted {
        not_before: Millis,
    },
    /// An attempt started: a `StepDispatched` record, or a checkpoint (or
    /// a found reconcile) that moved the run onto its next step — that
    /// record IS the next step's durable dispatch, so it is shown as one.
    Dispatched {
        step: u32,
        attempt: u32,
        deadline: Millis,
    },
    Checkpointed {
        step: u32,
    },
    StepFailed {
        step: u32,
    },
    ReplyLost {
        step: u32,
    },
    Reconciled {
        step: u32,
        effect_found: bool,
    },
    CancelRequested,
    /// Derived, never journaled: emitted with the record that made the run
    /// terminal.
    Finished {
        outcome: RunOutcome,
    },
}

/// One timeline or feed event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ObsEvent {
    pub at: Millis,
    #[serde(flatten)]
    pub kind: Kind,
}

/// What one committed record contributes: its own event and at most one
/// derived one (`dispatched` for a chained step, or `finished`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    pub run: RunId,
    pub at: Millis,
    pub first: Kind,
    pub then: Option<Kind>,
}

impl Observed {
    /// Derive the record's events from the event and the run's state
    /// before (`None` for an acceptance) and after it applied.
    pub fn derive(
        run: RunId,
        at: Millis,
        event: &Event,
        before: Option<&RunState>,
        after: &RunState,
    ) -> Self {
        let first = match event {
            Event::Accepted { not_before, .. } => Kind::Accepted {
                not_before: *not_before,
            },
            Event::StepDispatched {
                step,
                attempt,
                deadline,
            } => Kind::Dispatched {
                step: step.0,
                attempt: *attempt,
                deadline: *deadline,
            },
            Event::StepCheckpointed { step, .. } => Kind::Checkpointed { step: step.0 },
            Event::StepFailed { step } => Kind::StepFailed { step: step.0 },
            Event::ReplyLost { step } => Kind::ReplyLost { step: step.0 },
            Event::Reconciled { step, effect_found } => Kind::Reconciled {
                step: step.0,
                effect_found: *effect_found,
            },
            Event::CancelRequested => Kind::CancelRequested,
        };
        let was_terminal = before.is_some_and(RunState::is_terminal);
        let then = match after {
            RunState::Done if !was_terminal => Some(Kind::Finished {
                outcome: RunOutcome::Done,
            }),
            RunState::Failed { .. } if !was_terminal => Some(Kind::Finished {
                outcome: RunOutcome::Failed,
            }),
            RunState::Cancelled if !was_terminal => Some(Kind::Finished {
                outcome: RunOutcome::Cancelled,
            }),
            RunState::Running {
                step,
                attempt,
                deadline,
            } if !matches!(event, Event::StepDispatched { .. }) => Some(Kind::Dispatched {
                step: step.0,
                attempt: *attempt,
                deadline: *deadline,
            }),
            _ => None,
        };
        Self {
            run,
            at,
            first,
            then,
        }
    }

    fn events(&self) -> impl Iterator<Item = ObsEvent> + '_ {
        std::iter::once(self.first)
            .chain(self.then)
            .map(|kind| ObsEvent { at: self.at, kind })
    }
}

// ---------------------------------------------------------------- histogram

/// A fixed log-bucket latency histogram plus the exact maximum.
#[derive(Debug, Clone, Copy)]
struct Hist {
    counts: [u32; HIST_BUCKETS],
    n: u32,
    max: u32,
}

impl Default for Hist {
    fn default() -> Self {
        Self {
            counts: [0; HIST_BUCKETS],
            n: 0,
            max: 0,
        }
    }
}

impl Hist {
    fn add(&mut self, ms: u64) {
        let i = LATENCY_BOUNDS_MS.partition_point(|&b| b < ms);
        if let Some(c) = self.counts.get_mut(i) {
            *c = c.saturating_add(1);
        }
        self.n = self.n.saturating_add(1);
        self.max = self.max.max(u32::try_from(ms).unwrap_or(u32::MAX));
    }

    fn merge(&mut self, o: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(o.counts.iter()) {
            *a = a.saturating_add(*b);
        }
        self.n = self.n.saturating_add(o.n);
        self.max = self.max.max(o.max);
    }

    /// The `q` quantile (0 < q ≤ 1): the upper bound of the bucket holding
    /// the ⌈q·n⌉-th smallest latency, capped at the exact max.
    fn quantile(&self, q: f64) -> Option<u64> {
        if self.n == 0 {
            return None;
        }
        let rank = ((q * f64::from(self.n)).ceil() as u64).clamp(1, u64::from(self.n));
        let mut seen = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            seen += u64::from(*c);
            if seen >= rank {
                let bound = LATENCY_BOUNDS_MS.get(i).copied().unwrap_or(u64::MAX);
                return Some(bound.min(u64::from(self.max)));
            }
        }
        Some(u64::from(self.max))
    }

    fn summary(&self) -> StepMs {
        StepMs {
            p50: self.quantile(0.50),
            p95: self.quantile(0.95),
            max: (self.n > 0).then_some(u64::from(self.max)),
        }
    }
}

/// Dispatch → answer latency of the attempts in a window, in ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StepMs {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub max: Option<u64>,
}

// -------------------------------------------------------------------- rings

/// A ring of time buckets: a slot holds the bucket whose start it is
/// tagged with; a slot tagged with an older start is stale and reads as
/// zero, so idle gaps zero-fill without any sweep.
#[derive(Debug)]
struct Ring<T> {
    bucket_ms: u64,
    slots: Vec<(Millis, T)>,
}

impl<T: Default + Copy> Ring<T> {
    fn new(bucket_ms: u64, len: usize) -> Self {
        Self {
            bucket_ms,
            slots: vec![(EMPTY, T::default()); len.max(1)],
        }
    }

    fn start_of(&self, t: Millis) -> Millis {
        t - t % self.bucket_ms
    }

    fn index(&self, start: Millis) -> usize {
        ((start / self.bucket_ms) % self.slots.len() as u64) as usize
    }

    /// The bucket for `t`, reset if its slot held an older bucket. `None`
    /// if the slot already holds a newer one (`t` is older than the ring
    /// remembers, e.g. the wall clock stepped back past it).
    fn at(&mut self, t: Millis) -> Option<&mut T> {
        let start = self.start_of(t);
        let i = self.index(start);
        let slot = self.slots.get_mut(i)?;
        if slot.0 != start {
            if slot.0 != EMPTY && slot.0 > start {
                return None;
            }
            *slot = (start, T::default());
        }
        Some(&mut slot.1)
    }

    /// The `n` buckets ending with the one holding `now`, oldest first:
    /// `(start, Some(data))`, or `None` for a bucket nothing was recorded in.
    fn window(&self, now: Millis, n: usize) -> Vec<(Millis, Option<T>)> {
        let n = n.min(self.slots.len());
        let last = self.start_of(now);
        let first = last.saturating_sub((n as u64).saturating_sub(1) * self.bucket_ms);
        (0..n as u64)
            .map(|k| {
                let start = first + k * self.bucket_ms;
                let data = self
                    .slots
                    .get(self.index(start))
                    .filter(|s| s.0 == start)
                    .map(|s| s.1);
                (start, data)
            })
            .collect()
    }
}

// ------------------------------------------------------------------ metrics

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Counters {
    pub accepted: u64,
    pub deduplicated: u64,
    pub refused: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub dispatched: u64,
    pub steps_ok: u64,
    pub steps_failed: u64,
    pub replies_lost: u64,
    pub reconciled: u64,
}

impl Counters {
    fn count(&mut self, kind: &Kind) {
        let c = match kind {
            Kind::Accepted { .. } => &mut self.accepted,
            Kind::Dispatched { .. } => &mut self.dispatched,
            Kind::Checkpointed { .. } => &mut self.steps_ok,
            Kind::StepFailed { .. } => &mut self.steps_failed,
            Kind::ReplyLost { .. } => &mut self.replies_lost,
            Kind::Reconciled { .. } => &mut self.reconciled,
            Kind::CancelRequested => return,
            Kind::Finished { outcome } => match outcome {
                RunOutcome::Done => &mut self.completed,
                RunOutcome::Failed => &mut self.failed,
                RunOutcome::Cancelled => &mut self.cancelled,
            },
        };
        *c += 1;
    }

    fn add(&mut self, o: &Counters) {
        self.accepted += o.accepted;
        self.deduplicated += o.deduplicated;
        self.refused += o.refused;
        self.completed += o.completed;
        self.failed += o.failed;
        self.cancelled += o.cancelled;
        self.dispatched += o.dispatched;
        self.steps_ok += o.steps_ok;
        self.steps_failed += o.steps_failed;
        self.replies_lost += o.replies_lost;
        self.reconciled += o.reconciled;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Gauges {
    live_runs: u64,
    active_runs: u64,
    in_flight: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct Point {
    c: Counters,
    gauges: Option<Gauges>,
    hist: Hist,
}

/// The fine and coarse rings, recorded together.
#[derive(Debug)]
struct Rings<T> {
    fine: Ring<T>,
    coarse: Ring<T>,
}

impl<T: Default + Copy> Rings<T> {
    fn new() -> Self {
        Self {
            fine: Ring::new(FINE_BUCKET_MS, FINE_BUCKETS),
            coarse: Ring::new(COARSE_BUCKET_MS, COARSE_BUCKETS),
        }
    }

    fn record(&mut self, t: Millis, mut f: impl FnMut(&mut T)) {
        if let Some(b) = self.fine.at(t) {
            f(b);
        }
        if let Some(b) = self.coarse.at(t) {
            f(b);
        }
    }

    fn window(&self, w: Window, now: Millis) -> Vec<(Millis, Option<T>)> {
        let ring = if w.fine() { &self.fine } else { &self.coarse };
        ring.window(now, w.points())
    }
}

/// A handler call's result, as the executor saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallResult {
    Ok,
    Failed,
    /// The reply vanished, the call timed out, or it was cut off at its
    /// deadline (the run reconciles).
    Lost,
}

#[derive(Debug, Default, Clone, Copy)]
struct HandlerBucket {
    calls: u64,
    ok: u64,
    failed: u64,
    lost: u64,
    hist: Hist,
}

#[derive(Debug)]
struct HandlerStats {
    rings: Rings<HandlerBucket>,
    last_ok_at: Option<Millis>,
    last_failure_at: Option<Millis>,
}

// ---------------------------------------------------------------- timelines

/// Why a timeline is not the run's whole history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gap {
    /// Earlier events were folded into a snapshot before this process
    /// replayed the journal.
    Compacted,
    /// The timeline store dropped this run (LRU) at least once.
    Evicted,
    /// The run passed [`MAX_TIMELINE_EVENTS`]: the middle is gone.
    Truncated,
}

impl Gap {
    pub fn as_str(self) -> &'static str {
        match self {
            Gap::Compacted => "compacted",
            Gap::Evicted => "evicted",
            Gap::Truncated => "truncated",
        }
    }
}

#[derive(Debug)]
struct Timeline {
    /// At most [`MAX_TIMELINE_EVENTS`]: `[..TIMELINE_HEAD]` are the first
    /// events; once full, `[TIMELINE_HEAD..]` is a ring of the last ones.
    events: Vec<ObsEvent>,
    /// Events written over in the tail ring (0 = nothing dropped).
    overwritten: u64,
    /// Set when the timeline began after the run's start.
    gap: Option<Gap>,
}

impl Timeline {
    fn new(gap: Option<Gap>) -> Self {
        Self {
            events: Vec::new(),
            overwritten: 0,
            gap,
        }
    }

    fn push(&mut self, ev: ObsEvent) {
        if self.events.len() < MAX_TIMELINE_EVENTS {
            // Grow by doubling but never past the cap: a full timeline
            // costs exactly MAX_TIMELINE_EVENTS events.
            if self.events.len() == self.events.capacity() {
                let want = (self.events.capacity() * 2).clamp(4, MAX_TIMELINE_EVENTS);
                self.events.reserve_exact(want - self.events.len());
            }
            self.events.push(ev);
            return;
        }
        let i = TIMELINE_HEAD + (self.overwritten % TIMELINE_TAIL as u64) as usize;
        if let Some(slot) = self.events.get_mut(i) {
            *slot = ev;
        }
        self.overwritten += 1;
    }

    /// Commit order: the head, then the tail ring from its oldest event.
    fn ordered(&self) -> Vec<ObsEvent> {
        if self.overwritten == 0 {
            return self.events.clone();
        }
        let (head, tail) = self.events.split_at(TIMELINE_HEAD.min(self.events.len()));
        let start = (self.overwritten % TIMELINE_TAIL as u64) as usize;
        let start = start.min(tail.len());
        let mut out = Vec::with_capacity(self.events.len());
        out.extend_from_slice(head);
        out.extend_from_slice(&tail[start..]);
        out.extend_from_slice(&tail[..start]);
        out
    }

    fn gap(&self) -> Option<Gap> {
        self.gap
            .or((self.overwritten > 0).then_some(Gap::Truncated))
    }
}

const NIL: u32 = u32::MAX;

#[derive(Debug)]
struct Slot {
    run: RunId,
    newer: u32,
    older: u32,
    tl: Timeline,
}

/// Timelines in an LRU (an intrusive list over a slab: O(1) per event),
/// plus the activity feed.
#[derive(Debug)]
struct RunStore {
    max_timelines: usize,
    feed_cap: usize,
    slots: Vec<Slot>,
    index: HashMap<RunId, u32>,
    newest: u32,
    oldest: u32,
    /// Runs with an id below this came from the snapshot the journal was
    /// opened with: their history before it is gone (0 = no snapshot).
    compacted_below: u64,
    feed: VecDeque<(RunId, ObsEvent)>,
    evictions: u64,
}

impl RunStore {
    fn new(max_timelines: usize, feed_cap: usize) -> Self {
        let max_timelines = max_timelines.clamp(1, NIL as usize - 1);
        let feed_cap = feed_cap.max(1);
        // Sized once for the caps: the store never reallocates, and its
        // footprint is the same whether 10 or 20,000 runs are held.
        Self {
            max_timelines,
            feed_cap,
            slots: Vec::with_capacity(max_timelines),
            index: HashMap::with_capacity(max_timelines),
            newest: NIL,
            oldest: NIL,
            compacted_below: 0,
            feed: VecDeque::with_capacity(feed_cap),
            evictions: 0,
        }
    }

    fn unlink(&mut self, i: u32) {
        let Some(s) = self.slots.get(i as usize) else {
            return;
        };
        let (newer, older) = (s.newer, s.older);
        match self.slots.get_mut(newer as usize) {
            Some(n) => n.older = older,
            None => self.newest = older,
        }
        match self.slots.get_mut(older as usize) {
            Some(o) => o.newer = newer,
            None => self.oldest = newer,
        }
    }

    fn link_newest(&mut self, i: u32) {
        let old_newest = self.newest;
        if let Some(s) = self.slots.get_mut(i as usize) {
            s.newer = NIL;
            s.older = old_newest;
        }
        match self.slots.get_mut(old_newest as usize) {
            Some(n) => n.newer = i,
            None => self.oldest = i,
        }
        self.newest = i;
    }

    /// The slot for a run with no timeline: a fresh one while under the
    /// cap, else the least recently updated run's, evicted.
    fn insert(&mut self, run: RunId, tl: Timeline) -> u32 {
        if self.slots.len() < self.max_timelines {
            let i = self.slots.len() as u32;
            self.slots.push(Slot {
                run,
                newer: NIL,
                older: NIL,
                tl,
            });
            self.link_newest(i);
            self.index.insert(run, i);
            return i;
        }
        let i = self.oldest;
        self.unlink(i);
        if let Some(s) = self.slots.get_mut(i as usize) {
            self.index.remove(&s.run);
            s.run = run;
            s.tl = tl;
        }
        self.evictions += 1;
        self.link_newest(i);
        self.index.insert(run, i);
        i
    }

    fn record(&mut self, run: RunId, ev: ObsEvent) {
        let i = match self.index.get(&run).copied() {
            Some(i) => {
                if self.newest != i {
                    self.unlink(i);
                    self.link_newest(i);
                }
                i
            }
            None => {
                // A timeline that does not begin with the acceptance began
                // late: folded into the snapshot this process opened, or
                // dropped from the store earlier.
                let gap = match ev.kind {
                    Kind::Accepted { .. } => None,
                    _ if run.0 < self.compacted_below => Some(Gap::Compacted),
                    _ => Some(Gap::Evicted),
                };
                self.insert(run, Timeline::new(gap))
            }
        };
        if let Some(s) = self.slots.get_mut(i as usize) {
            s.tl.push(ev);
        }
        if self.feed.len() >= self.feed_cap {
            self.feed.pop_front();
        }
        self.feed.push_back((run, ev));
    }

    fn timeline(&self, run: RunId) -> TimelineView {
        let Some(s) = self
            .index
            .get(&run)
            .and_then(|&i| self.slots.get(i as usize))
        else {
            // Every run this process accepted or replayed from the log had
            // a timeline; one without came from the snapshot or was evicted.
            let gap = if run.0 < self.compacted_below {
                Gap::Compacted
            } else {
                Gap::Evicted
            };
            return TimelineView {
                complete: false,
                reason: Some(gap.as_str()),
                events: Vec::new(),
            };
        };
        let gap = s.tl.gap();
        TimelineView {
            complete: gap.is_none(),
            reason: gap.map(Gap::as_str),
            events: s.tl.ordered(),
        }
    }
}

/// One run's timeline as the API shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineView {
    /// `true` ⇒ `reason` is `None` and `events` is the run's whole history.
    pub complete: bool,
    pub reason: Option<&'static str>,
    pub events: Vec<ObsEvent>,
}

// ------------------------------------------------------------- state counts

/// Runs by state kind, all seven always present (the same counts
/// `/admin/diagnostics` reports).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StateCounts {
    pub queued: u64,
    pub running: u64,
    pub retry_waiting: u64,
    pub reconciling: u64,
    pub done: u64,
    pub failed: u64,
    pub cancelled: u64,
}

impl StateCounts {
    fn slot(&mut self, s: &RunState) -> &mut u64 {
        match s {
            RunState::Queued { .. } => &mut self.queued,
            RunState::Running { .. } => &mut self.running,
            RunState::RetryWaiting { .. } => &mut self.retry_waiting,
            RunState::Reconciling { .. } => &mut self.reconciling,
            RunState::Done => &mut self.done,
            RunState::Failed { .. } => &mut self.failed,
            RunState::Cancelled => &mut self.cancelled,
        }
    }

    pub fn add(&mut self, s: &RunState) {
        *self.slot(s) += 1;
    }

    pub fn remove(&mut self, s: &RunState) {
        let c = self.slot(s);
        *c = c.saturating_sub(1);
    }

    /// `(kind, count)` for every kind, in the order `RunState::kind` lists them.
    pub fn pairs(&self) -> [(&'static str, u64); 7] {
        [
            ("queued", self.queued),
            ("running", self.running),
            ("retry_waiting", self.retry_waiting),
            ("reconciling", self.reconciling),
            ("done", self.done),
            ("failed", self.failed),
            ("cancelled", self.cancelled),
        ]
    }
}

/// The engine's index counts at one moment: what the actor answers for
/// `GET /metrics` (after the records it reflects are durable).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexCounts {
    pub states: StateCounts,
    /// Retained runs, terminal ones included (diagnostics' `live_runs`).
    pub live_runs: u64,
    /// Non-terminal runs (diagnostics' `active_runs`).
    pub active_runs: u64,
}

// ------------------------------------------------------------- API shapes

#[derive(Debug, Clone, Serialize)]
pub struct PointOut {
    pub t: Millis,
    #[serde(flatten)]
    pub counters: Counters,
    pub live_runs: Option<u64>,
    pub active_runs: Option<u64>,
    pub in_flight: Option<u64>,
    pub step_ms: StepMs,
}

#[derive(Debug, Clone, Serialize)]
pub struct TotalsOut {
    #[serde(flatten)]
    pub counters: Counters,
    pub step_ms: StepMs,
}

#[derive(Debug, Clone, Serialize)]
pub struct HandlerOut {
    pub name: String,
    pub calls: u64,
    pub ok: u64,
    pub failed: u64,
    pub replies_lost: u64,
    pub step_ms: StepMs,
    /// The handler's latest ok / non-ok call since `since` (not limited to
    /// the window: a "last" time outside it is still the last one).
    pub last_ok_at: Option<Millis>,
    pub last_failure_at: Option<Millis>,
}

/// The `GET /metrics` body.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsOut {
    pub now: Millis,
    pub since: Millis,
    pub window_ms: u64,
    pub bucket_ms: u64,
    pub points: Vec<PointOut>,
    pub totals: TotalsOut,
    pub states: StateCounts,
    pub handlers: Vec<HandlerOut>,
}

// ----------------------------------------------------------------- observer

/// The shared observer: one per engine, fed by the actor, the executor,
/// the server and the gauge sampler; read by the API.
#[derive(Debug)]
pub struct Observer {
    since: Millis,
    /// Registered handler names, sorted; `handler_stats` is parallel.
    handlers: Vec<String>,
    handler_ix: HashMap<String, usize>,
    points: Mutex<Rings<Point>>,
    /// One lock per handler: a reader aggregating one handler's window
    /// never stalls another's recording, nor the actor (which never
    /// touches these).
    handler_stats: Vec<Mutex<HandlerStats>>,
    runs: Mutex<RunStore>,
    in_flight: AtomicU64,
    live_runs: AtomicU64,
    active_runs: AtomicU64,
}

impl Observer {
    /// An observer whose metrics begin at `since`, for these handlers.
    pub fn new(since: Millis, handlers: impl IntoIterator<Item = String>) -> Self {
        Self::with_caps(since, handlers, MAX_TIMELINES, FEED_CAP)
    }

    fn with_caps(
        since: Millis,
        handlers: impl IntoIterator<Item = String>,
        max_timelines: usize,
        feed_cap: usize,
    ) -> Self {
        let mut names: Vec<String> = handlers.into_iter().collect();
        names.sort();
        names.dedup();
        let handler_ix = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i))
            .collect();
        let handler_stats = names
            .iter()
            .map(|_| {
                Mutex::new(HandlerStats {
                    rings: Rings::new(),
                    last_ok_at: None,
                    last_failure_at: None,
                })
            })
            .collect();
        Self {
            since,
            handlers: names,
            handler_ix,
            points: Mutex::new(Rings::new()),
            handler_stats,
            runs: Mutex::new(RunStore::new(max_timelines, feed_cap)),
            in_flight: AtomicU64::new(0),
            live_runs: AtomicU64::new(0),
            active_runs: AtomicU64::new(0),
        }
    }

    /// When this observer started: metrics begin here.
    pub fn since(&self) -> Millis {
        self.since
    }

    /// The index of a registered handler, for [`CallGuard::start`].
    pub fn handler_index(&self, name: &str) -> Option<usize> {
        self.handler_ix.get(name).copied()
    }

    // --- feeding ----------------------------------------------------------

    /// Records whose fsync has returned (`live`), or replayed at startup
    /// (`!live`: timelines and the feed only; metrics never backfill).
    pub fn committed(&self, batch: &[Observed], live: bool) {
        if batch.is_empty() {
            return;
        }
        {
            let mut runs = lock(&self.runs);
            for o in batch {
                for ev in o.events() {
                    runs.record(o.run, ev);
                }
            }
        }
        if live {
            let mut points = lock(&self.points);
            for o in batch {
                points.record(o.at, |p| {
                    p.c.count(&o.first);
                    if let Some(then) = &o.then {
                        p.c.count(then);
                    }
                });
            }
        }
    }

    /// The journal was opened from a snapshot covering runs below `next_run`.
    pub fn set_compacted_below(&self, next_run: u64) {
        let mut runs = lock(&self.runs);
        runs.compacted_below = runs.compacted_below.max(next_run);
    }

    /// A submit was refused (rate limit, capacity, admission).
    pub fn refused(&self, now: Millis) {
        lock(&self.points).record(now, |p| p.c.refused += 1);
    }

    /// A submit was answered with an existing run (its `op_key`).
    pub fn deduplicated(&self, now: Millis) {
        lock(&self.points).record(now, |p| p.c.deduplicated += 1);
    }

    /// One handler call ended after `ms` (dispatch → answer).
    pub fn call_finished(&self, handler: usize, result: CallResult, ms: u64, now: Millis) {
        lock(&self.points).record(now, |p| p.hist.add(ms));
        let Some(stats) = self.handler_stats.get(handler) else {
            return;
        };
        let mut s = lock(stats);
        s.rings.record(now, |b| {
            b.calls += 1;
            match result {
                CallResult::Ok => b.ok += 1,
                CallResult::Failed => b.failed += 1,
                CallResult::Lost => b.lost += 1,
            }
            b.hist.add(ms);
        });
        let last = match result {
            CallResult::Ok => &mut s.last_ok_at,
            CallResult::Failed | CallResult::Lost => &mut s.last_failure_at,
        };
        *last = Some(last.map_or(now, |t| t.max(now)));
    }

    /// The actor's index sizes, published for the gauge sampler.
    pub fn set_index_gauges(&self, live_runs: u64, active_runs: u64) {
        self.live_runs.store(live_runs, Ordering::Relaxed);
        self.active_runs.store(active_runs, Ordering::Relaxed);
    }

    /// Handler calls in flight now.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Sample the gauges into the buckets holding `now` (the last sample
    /// in a bucket wins).
    pub fn sample(&self, now: Millis) {
        let g = Gauges {
            live_runs: self.live_runs.load(Ordering::Relaxed),
            active_runs: self.active_runs.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
        };
        lock(&self.points).record(now, |p| p.gauges = Some(g));
    }

    // --- reading ----------------------------------------------------------

    /// The `GET /metrics` body for `window` at `now`, with the engine's
    /// current state counts.
    pub fn metrics(&self, window: Window, now: Millis, index: IndexCounts) -> MetricsOut {
        // Copy the window out under the lock; aggregate outside it.
        let raw = lock(&self.points).window(window, now);
        let mut totals = Counters::default();
        let mut hist = Hist::default();
        let points = raw
            .into_iter()
            .map(|(t, p)| {
                let p = p.unwrap_or_default();
                totals.add(&p.c);
                hist.merge(&p.hist);
                PointOut {
                    t,
                    counters: p.c,
                    live_runs: p.gauges.map(|g| g.live_runs),
                    active_runs: p.gauges.map(|g| g.active_runs),
                    in_flight: p.gauges.map(|g| g.in_flight),
                    step_ms: p.hist.summary(),
                }
            })
            .collect();
        let handlers = self
            .handlers
            .iter()
            .zip(&self.handler_stats)
            .map(|(name, stats)| {
                let (raw, last_ok_at, last_failure_at) = {
                    let s = lock(stats);
                    (s.rings.window(window, now), s.last_ok_at, s.last_failure_at)
                };
                let mut agg = HandlerBucket::default();
                for b in raw.iter().filter_map(|(_, b)| b.as_ref()) {
                    agg.calls += b.calls;
                    agg.ok += b.ok;
                    agg.failed += b.failed;
                    agg.lost += b.lost;
                    agg.hist.merge(&b.hist);
                }
                HandlerOut {
                    name: name.clone(),
                    calls: agg.calls,
                    ok: agg.ok,
                    failed: agg.failed,
                    replies_lost: agg.lost,
                    step_ms: agg.hist.summary(),
                    last_ok_at,
                    last_failure_at,
                }
            })
            .collect();
        MetricsOut {
            now,
            since: self.since,
            window_ms: window.window_ms(),
            bucket_ms: window.bucket_ms(),
            points,
            totals: TotalsOut {
                counters: totals,
                step_ms: hist.summary(),
            },
            states: index.states,
            handlers,
        }
    }

    /// One run's timeline (the caller has checked that the run exists).
    pub fn timeline(&self, run: RunId) -> TimelineView {
        lock(&self.runs).timeline(run)
    }

    /// The newest `limit` feed events (at most [`MAX_FEED_PAGE`]), newest
    /// first.
    pub fn feed(&self, limit: usize) -> Vec<(RunId, ObsEvent)> {
        let limit = limit.clamp(1, MAX_FEED_PAGE);
        lock(&self.runs)
            .feed
            .iter()
            .rev()
            .take(limit)
            .copied()
            .collect()
    }

    /// Timelines held now and evictions so far (diagnostic, tests).
    pub fn timeline_stats(&self) -> (usize, u64) {
        let runs = lock(&self.runs);
        (runs.index.len(), runs.evictions)
    }
}

/// One handler call in flight: counts toward `in_flight` while alive and,
/// for an attempt (a handler index), records its latency and result when
/// it ends. Dropped without [`CallGuard::finish`] — the call was aborted at
/// its deadline or abandoned at shutdown — it counts as a lost reply, which
/// is what the engine journals for it.
pub struct CallGuard {
    obs: Arc<Observer>,
    handler: Option<usize>,
    started: Instant,
    result: Option<CallResult>,
}

impl CallGuard {
    pub fn start(obs: &Arc<Observer>, handler: Option<usize>) -> Self {
        obs.in_flight.fetch_add(1, Ordering::Relaxed);
        Self {
            obs: Arc::clone(obs),
            handler,
            started: Instant::now(),
            result: None,
        }
    }

    pub fn finish(mut self, result: CallResult) {
        self.result = Some(result);
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.obs.in_flight.fetch_sub(1, Ordering::Relaxed);
        if let Some(h) = self.handler {
            let ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let result = self.result.unwrap_or(CallResult::Lost);
            self.obs.call_finished(h, result, ms, wall_ms());
        }
    }
}

// -------------------------------------------------------------------- memory

/// An upper bound on the observer's memory at its caps with `handlers`
/// registered handlers, from the structures' sizes (allocator overhead
/// and the hash map's spare capacity are approximated, not measured).
pub fn memory_bound(handlers: usize) -> usize {
    use std::mem::size_of;
    let rings = |entry: usize| (FINE_BUCKETS + COARSE_BUCKETS) * entry;
    let points = rings(size_of::<(Millis, Point)>());
    let per_handler = rings(size_of::<(Millis, HandlerBucket)>()) + size_of::<HandlerStats>();
    // A full timeline: its slot, MAX_TIMELINE_EVENTS events, and an index
    // entry ((RunId, u32) padded to 16 bytes + a control byte, at most
    // doubled by hashbrown's power-of-two bucket count).
    let timeline = size_of::<Slot>()
        + MAX_TIMELINE_EVENTS * size_of::<ObsEvent>()
        + 2 * (size_of::<(RunId, u32)>() + 1);
    let feed = FEED_CAP * size_of::<(RunId, ObsEvent)>();
    points + handlers * per_handler + MAX_TIMELINES * timeline + feed
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_domain::StepIndex;

    /// A base time far from the epoch, aligned to a coarse bucket.
    const T0: Millis = 1_764_000_000_000;

    fn obs(handlers: &[&str]) -> Observer {
        Observer::new(T0, handlers.iter().map(|s| s.to_string()))
    }

    fn accepted(run: u64, at: Millis) -> Observed {
        Observed {
            run: RunId(run),
            at,
            first: Kind::Accepted { not_before: at },
            then: None,
        }
    }

    fn ev(run: u64, at: Millis, kind: Kind) -> Observed {
        Observed {
            run: RunId(run),
            at,
            first: kind,
            then: None,
        }
    }

    fn idx() -> IndexCounts {
        IndexCounts::default()
    }

    #[test]
    fn windows_map_to_bucket_sizes() {
        let cases = [
            ("15m", 900_000, 10_000, 90),
            ("1h", 3_600_000, 10_000, 360),
            ("6h", 21_600_000, 60_000, 360),
            ("24h", 86_400_000, 60_000, 1440),
        ];
        for (s, window, bucket, points) in cases {
            let w = Window::parse(s).unwrap_or_else(|| panic!("{s}"));
            assert_eq!(
                (w.window_ms(), w.bucket_ms(), w.points()),
                (window, bucket, points),
                "{s}"
            );
            let m = obs(&[]).metrics(w, T0 + 5_000, idx());
            assert_eq!(m.points.len(), points, "{s}: every bucket present");
            assert_eq!((m.window_ms, m.bucket_ms), (window, bucket));
        }
        for bad in ["", "1H", "2h", "15", "60m", "1d", "24h ", "15m,1h"] {
            assert_eq!(Window::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn buckets_roll_over_and_zero_fill_across_idle_gaps() {
        let o = obs(&[]);
        // Two in the first bucket, one in the next, then 5 idle minutes.
        o.committed(&[accepted(1, T0 + 1), accepted(2, T0 + 9_999)], true);
        o.committed(&[accepted(3, T0 + 10_000)], true);
        let later = T0 + 5 * 60_000 + 3_000;
        o.committed(&[accepted(4, later)], true);
        let m = o.metrics(Window::M15, later, idx());
        assert_eq!(m.points.len(), 90);
        // Oldest first, contiguous, the last one holding `now`.
        for w in m.points.windows(2) {
            assert_eq!(w[1].t - w[0].t, 10_000);
        }
        let last = m.points.last().unwrap_or_else(|| panic!("points"));
        assert_eq!(last.t, T0 + 5 * 60_000);
        assert_eq!(last.counters.accepted, 1);
        let at = |t: Millis| {
            m.points
                .iter()
                .find(|p| p.t == t)
                .unwrap_or_else(|| panic!("no bucket {t}"))
        };
        assert_eq!(at(T0).counters.accepted, 2);
        assert_eq!(at(T0 + 10_000).counters.accepted, 1);
        // The idle gap is zero-filled, and so is everything before `since`.
        let idle: u64 = m
            .points
            .iter()
            .filter(|p| p.t > T0 + 10_000 && p.t < T0 + 5 * 60_000)
            .map(|p| p.counters.accepted)
            .sum();
        assert_eq!(idle, 0);
        assert!(m
            .points
            .iter()
            .filter(|p| p.t < T0)
            .all(|p| p.counters.accepted == 0 && p.live_runs.is_none()));
        assert_eq!(m.totals.counters.accepted, 4);

        // Past the ring's hour the fine slots are reused: the old buckets
        // read as zero, never as the new bucket's numbers.
        let much_later = T0 + 3_600_000;
        o.committed(&[accepted(5, much_later)], true);
        let m = o.metrics(Window::H1, much_later, idx());
        assert_eq!(m.points.len(), 360);
        // Runs 1 and 2 (T0's bucket) fell out of the hour; 3, 4, 5 remain.
        assert_eq!(m.totals.counters.accepted, 3);
        assert_eq!(m.points.first().map(|p| p.t), Some(T0 + 10_000));
        // The coarse ring still has the whole day.
        let d = o.metrics(Window::H24, much_later, idx());
        assert_eq!(d.totals.counters.accepted, 5);
        assert_eq!(d.points.len(), 1440);
    }

    #[test]
    fn a_record_older_than_the_ring_is_dropped_not_misfiled() {
        let o = obs(&[]);
        o.committed(&[accepted(1, T0 + 3_600_000)], true);
        // The same fine slot, an hour earlier: it must not reset the newer
        // bucket nor be counted as it.
        o.committed(&[accepted(2, T0)], true);
        let m = o.metrics(Window::M15, T0 + 3_600_000, idx());
        assert_eq!(m.points.last().map(|p| p.counters.accepted), Some(1));
    }

    #[test]
    fn percentiles_are_bucket_upper_bounds_capped_at_the_max() {
        let mut h = Hist::default();
        assert_eq!(
            h.summary(),
            StepMs {
                p50: None,
                p95: None,
                max: None
            }
        );
        for ms in 1..=100 {
            h.add(ms);
        }
        // Rank 50 is 50 ms, in (40, 50]; rank 95 is 95 ms, in (80, 100].
        assert_eq!(
            h.summary(),
            StepMs {
                p50: Some(50),
                p95: Some(100),
                max: Some(100)
            }
        );
        // One latency: every percentile is capped at it.
        let mut one = Hist::default();
        one.add(7);
        assert_eq!(
            one.summary(),
            StepMs {
                p50: Some(7),
                p95: Some(7),
                max: Some(7)
            }
        );
        // 0 ms is the first bucket (upper bound 1 ms); past the last bound
        // is the overflow, reported as the exact max.
        let mut wide = Hist::default();
        for ms in [0, 0, 0, 90_000] {
            wide.add(ms);
        }
        assert_eq!(wide.quantile(0.5), Some(1));
        assert_eq!(wide.quantile(0.95), Some(90_000));
        // 90 at 10 ms and 10 at 1 s: p50 10, p95 in (800, 1000].
        let mut bi = Hist::default();
        for _ in 0..90 {
            bi.add(10);
        }
        for _ in 0..10 {
            bi.add(1_000);
        }
        assert_eq!(bi.quantile(0.5), Some(10));
        assert_eq!(bi.quantile(0.95), Some(1_000));
        // Merging is adding.
        let mut m = Hist::default();
        m.merge(&bi);
        m.merge(&one);
        assert_eq!((m.n, m.max), (101, 1_000));
    }

    #[test]
    fn step_latency_lands_in_points_and_handlers() {
        let o = obs(&["b", "a"]);
        let a = o.handler_index("a").unwrap_or_else(|| panic!("a"));
        let b = o.handler_index("b").unwrap_or_else(|| panic!("b"));
        o.call_finished(a, CallResult::Ok, 120, T0 + 1_000);
        o.call_finished(a, CallResult::Failed, 480, T0 + 2_000);
        o.call_finished(b, CallResult::Lost, 900, T0 + 3_000);
        let m = o.metrics(Window::M15, T0 + 5_000, idx());
        let names: Vec<&str> = m.handlers.iter().map(|h| h.name.as_str()).collect();
        assert_eq!(names, ["a", "b"], "sorted by name");
        let ha = &m.handlers[0];
        assert_eq!((ha.calls, ha.ok, ha.failed, ha.replies_lost), (2, 1, 1, 0));
        assert_eq!(ha.last_ok_at, Some(T0 + 1_000));
        assert_eq!(ha.last_failure_at, Some(T0 + 2_000));
        assert_eq!(m.handlers[1].replies_lost, 1);
        assert_eq!(m.handlers[1].last_ok_at, None);
        assert_eq!(m.totals.step_ms.max, Some(900));
        assert_eq!(m.points.last().map(|p| p.step_ms.max), Some(Some(900)));
        // A window with no attempts has null latencies; the "last" times
        // are since the start, not the window.
        let quiet = o.metrics(Window::M15, T0 + 7_200_000, idx());
        assert_eq!(quiet.handlers[0].calls, 0);
        assert_eq!(quiet.handlers[0].step_ms.p50, None);
        assert_eq!(quiet.handlers[0].last_ok_at, Some(T0 + 1_000));
        assert_eq!(quiet.totals.step_ms.max, None);
    }

    #[test]
    fn gauges_are_null_until_sampled_and_the_last_sample_wins() {
        let o = obs(&[]);
        o.set_index_gauges(10, 3);
        o.sample(T0 + 1_000);
        o.set_index_gauges(12, 4);
        o.sample(T0 + 9_000); // same bucket: this one stays
        let m = o.metrics(Window::M15, T0 + 15_000, idx());
        let n = m.points.len();
        let (prev, last) = (&m.points[n - 2], &m.points[n - 1]);
        assert_eq!(prev.t, T0);
        assert_eq!(
            (prev.live_runs, prev.active_runs, prev.in_flight),
            (Some(12), Some(4), Some(0))
        );
        assert_eq!(last.live_runs, None, "not sampled yet");
    }

    #[test]
    fn counters_come_from_committed_records() {
        let o = obs(&[]);
        let records = [
            accepted(1, T0),
            Observed {
                run: RunId(1),
                at: T0 + 1,
                first: Kind::Checkpointed { step: 0 },
                then: Some(Kind::Dispatched {
                    step: 1,
                    attempt: 1,
                    deadline: T0 + 30_001,
                }),
            },
            ev(1, T0 + 2, Kind::ReplyLost { step: 1 }),
            Observed {
                run: RunId(1),
                at: T0 + 3,
                first: Kind::Reconciled {
                    step: 1,
                    effect_found: true,
                },
                then: Some(Kind::Finished {
                    outcome: RunOutcome::Done,
                }),
            },
        ];
        o.committed(&records, true);
        o.refused(T0 + 4);
        o.deduplicated(T0 + 5);
        let c = o.metrics(Window::M15, T0 + 6, idx()).totals.counters;
        assert_eq!(
            c,
            Counters {
                accepted: 1,
                deduplicated: 1,
                refused: 1,
                completed: 1,
                dispatched: 1,
                steps_ok: 1,
                replies_lost: 1,
                reconciled: 1,
                ..Counters::default()
            }
        );
        // Replayed records feed timelines and the feed, never metrics.
        let r = obs(&[]);
        r.committed(&records, false);
        assert_eq!(
            r.metrics(Window::M15, T0 + 6, idx()).totals.counters,
            Counters::default()
        );
        assert_eq!(r.timeline(RunId(1)).events.len(), 6);
        assert_eq!(r.feed(50).len(), 6);
    }

    #[test]
    fn a_long_timeline_keeps_the_first_16_and_the_last_80() {
        let o = obs(&[]);
        o.committed(&[accepted(1, T0)], false);
        for i in 1..200u64 {
            o.committed(&[ev(1, T0 + i, Kind::StepFailed { step: 0 })], false);
        }
        let t = o.timeline(RunId(1));
        assert_eq!((t.complete, t.reason), (false, Some("truncated")));
        let ats: Vec<Millis> = t.events.iter().map(|e| e.at - T0).collect();
        let want: Vec<Millis> = (0..16).chain(120..200).collect();
        assert_eq!(ats, want);
        // At exactly the cap nothing is dropped.
        o.committed(&[accepted(2, T0)], false);
        for i in 1..MAX_TIMELINE_EVENTS as u64 {
            o.committed(&[ev(2, T0 + i, Kind::StepFailed { step: 0 })], false);
        }
        let t = o.timeline(RunId(2));
        assert_eq!((t.complete, t.reason, t.events.len()), (true, None, 96));
    }

    #[test]
    fn the_least_recently_updated_timeline_is_evicted() {
        let o = Observer::with_caps(T0, Vec::new(), 3, FEED_CAP);
        for run in 1..=3 {
            o.committed(&[accepted(run, T0 + run)], false);
        }
        // Touch 1, so 2 is now the least recently updated.
        o.committed(&[ev(1, T0 + 10, Kind::CancelRequested)], false);
        o.committed(&[accepted(4, T0 + 11)], false);
        assert_eq!(o.timeline_stats(), (3, 1));
        let gone = o.timeline(RunId(2));
        assert_eq!(
            (gone.complete, gone.reason, gone.events.len()),
            (false, Some("evicted"), 0)
        );
        for run in [1, 3, 4] {
            assert!(o.timeline(RunId(run)).complete, "run {run}");
        }
        assert_eq!(o.timeline(RunId(1)).events.len(), 2);
        // A later event of the evicted run starts a new timeline that
        // says what it is: whatever is left, marked evicted.
        o.committed(&[ev(2, T0 + 12, Kind::CancelRequested)], false);
        let back = o.timeline(RunId(2));
        assert_eq!((back.complete, back.reason), (false, Some("evicted")));
        assert_eq!(back.events.len(), 1);
        // …which evicted 3, the least recent of 1, 3, 4.
        assert_eq!(o.timeline(RunId(3)).reason, Some("evicted"));
        assert!(o.timeline(RunId(4)).complete);
    }

    #[test]
    fn the_feed_is_capped_and_newest_first() {
        let o = obs(&[]);
        for i in 0..(FEED_CAP as u64 + 5) {
            o.committed(&[accepted(i + 1, T0 + i)], false);
        }
        let all: Vec<u64> = {
            let runs = lock(&o.runs);
            runs.feed.iter().map(|(r, _)| r.0).collect()
        };
        assert_eq!(all.len(), FEED_CAP);
        assert_eq!(all.first(), Some(&6), "the five oldest left");
        let page = o.feed(50);
        assert_eq!(page.len(), 50);
        let ids: Vec<u64> = page.iter().map(|(r, _)| r.0).collect();
        let want: Vec<u64> = (956..=1005).rev().collect();
        assert_eq!(ids, want);
        assert_eq!(o.feed(10_000).len(), MAX_FEED_PAGE, "a page is capped");
        assert_eq!(o.feed(0).len(), 1, "clamped to at least one");
    }

    #[test]
    fn runs_from_the_opened_snapshot_are_marked_compacted() {
        let o = obs(&[]);
        o.set_compacted_below(10);
        // A snapshot run with a later event: what is left, marked compacted.
        o.committed(&[ev(5, T0, Kind::CancelRequested)], false);
        let t = o.timeline(RunId(5));
        assert_eq!(
            (t.complete, t.reason, t.events.len()),
            (false, Some("compacted"), 1)
        );
        // A snapshot run with no event since: nothing, marked compacted.
        let t = o.timeline(RunId(7));
        assert_eq!(
            (t.complete, t.reason, t.events.len()),
            (false, Some("compacted"), 0)
        );
        // A run accepted after the snapshot is complete.
        o.committed(&[accepted(10, T0)], false);
        assert!(o.timeline(RunId(10)).complete);
        // Truncation does not hide that the start is gone.
        for i in 0..200 {
            o.committed(&[ev(5, T0 + i, Kind::StepFailed { step: 0 })], false);
        }
        assert_eq!(o.timeline(RunId(5)).reason, Some("compacted"));
    }

    #[test]
    fn finished_is_derived_from_the_terminal_transition() {
        use RunState as S;
        let r = RunId(1);
        let running = |step| S::Running {
            step: StepIndex(step),
            attempt: 1,
            deadline: 99,
        };
        // Last step checkpointed → done.
        let o = Observed::derive(
            r,
            5,
            &Event::StepCheckpointed {
                step: StepIndex(1),
                output: b"secret output".to_vec(),
            },
            Some(&running(1)),
            &S::Done,
        );
        assert_eq!(o.first, Kind::Checkpointed { step: 1 });
        assert_eq!(
            o.then,
            Some(Kind::Finished {
                outcome: RunOutcome::Done
            })
        );
        // Retries exhausted → failed.
        let o = Observed::derive(
            r,
            5,
            &Event::StepFailed { step: StepIndex(0) },
            Some(&running(0)),
            &S::Failed { step: StepIndex(0) },
        );
        assert_eq!(
            o.then,
            Some(Kind::Finished {
                outcome: RunOutcome::Failed
            })
        );
        // Not found on the last attempt → failed too.
        let o = Observed::derive(
            r,
            5,
            &Event::Reconciled {
                step: StepIndex(0),
                effect_found: false,
            },
            Some(&S::Reconciling {
                step: StepIndex(0),
                attempt: 5,
            }),
            &S::Failed { step: StepIndex(0) },
        );
        assert_eq!(
            o.then,
            Some(Kind::Finished {
                outcome: RunOutcome::Failed
            })
        );
        // Cancelled.
        let o = Observed::derive(
            r,
            5,
            &Event::CancelRequested,
            Some(&S::Queued { next_run_at: 0 }),
            &S::Cancelled,
        );
        assert_eq!(o.first, Kind::CancelRequested);
        assert_eq!(
            o.then,
            Some(Kind::Finished {
                outcome: RunOutcome::Cancelled
            })
        );
        // A checkpoint that chains to the next step shows its dispatch.
        let o = Observed::derive(
            r,
            5,
            &Event::StepCheckpointed {
                step: StepIndex(0),
                output: Vec::new(),
            },
            Some(&running(0)),
            &running(1),
        );
        assert_eq!(
            o.then,
            Some(Kind::Dispatched {
                step: 1,
                attempt: 1,
                deadline: 99
            })
        );
        // An explicit dispatch is not shown twice; a retry wait is nothing.
        let o = Observed::derive(
            r,
            5,
            &Event::StepDispatched {
                step: StepIndex(0),
                attempt: 1,
                deadline: 99,
            },
            Some(&S::Queued { next_run_at: 0 }),
            &running(0),
        );
        assert_eq!(o.then, None);
        let o = Observed::derive(
            r,
            5,
            &Event::StepFailed { step: StepIndex(0) },
            Some(&running(0)),
            &S::RetryWaiting {
                step: StepIndex(0),
                attempt: 1,
                next_run_at: 9,
            },
        );
        assert_eq!(o.then, None);
    }

    #[test]
    fn events_serialize_to_the_contract_shapes() {
        let cases = [
            (
                Kind::Accepted { not_before: 7 },
                r#"{"at":1,"kind":"accepted","not_before":7}"#,
            ),
            (
                Kind::Dispatched {
                    step: 0,
                    attempt: 2,
                    deadline: 9,
                },
                r#"{"at":1,"kind":"dispatched","step":0,"attempt":2,"deadline":9}"#,
            ),
            (
                Kind::Checkpointed { step: 3 },
                r#"{"at":1,"kind":"checkpointed","step":3}"#,
            ),
            (
                Kind::StepFailed { step: 3 },
                r#"{"at":1,"kind":"step_failed","step":3}"#,
            ),
            (
                Kind::ReplyLost { step: 3 },
                r#"{"at":1,"kind":"reply_lost","step":3}"#,
            ),
            (
                Kind::Reconciled {
                    step: 3,
                    effect_found: true,
                },
                r#"{"at":1,"kind":"reconciled","step":3,"effect_found":true}"#,
            ),
            (
                Kind::CancelRequested,
                r#"{"at":1,"kind":"cancel_requested"}"#,
            ),
            (
                Kind::Finished {
                    outcome: RunOutcome::Cancelled,
                },
                r#"{"at":1,"kind":"finished","outcome":"cancelled"}"#,
            ),
        ];
        for (kind, want) in cases {
            let got =
                serde_json::to_string(&ObsEvent { at: 1, kind }).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(got, want);
        }
    }

    #[test]
    fn the_metrics_body_has_the_contract_fields() {
        let o = obs(&["step"]);
        let v = serde_json::to_value(o.metrics(Window::H1, T0, idx()))
            .unwrap_or_else(|e| panic!("{e}"));
        for k in [
            "now",
            "since",
            "window_ms",
            "bucket_ms",
            "points",
            "totals",
            "states",
            "handlers",
        ] {
            assert!(v.get(k).is_some(), "{k}");
        }
        let p = &v["points"][0];
        for k in [
            "t",
            "accepted",
            "deduplicated",
            "refused",
            "completed",
            "failed",
            "cancelled",
            "dispatched",
            "steps_ok",
            "steps_failed",
            "replies_lost",
            "reconciled",
            "live_runs",
            "active_runs",
            "in_flight",
            "step_ms",
        ] {
            assert!(p.get(k).is_some(), "point.{k}");
        }
        assert_eq!(
            p["step_ms"],
            serde_json::json!({"p50":null,"p95":null,"max":null})
        );
        assert!(
            v["totals"].get("live_runs").is_none(),
            "no gauges in totals"
        );
        assert_eq!(
            v["states"],
            serde_json::json!({"queued":0,"running":0,"retry_waiting":0,"reconciling":0,
                               "done":0,"failed":0,"cancelled":0})
        );
        assert_eq!(
            v["handlers"][0],
            serde_json::json!({"name":"step","calls":0,"ok":0,"failed":0,"replies_lost":0,
                               "step_ms":{"p50":null,"p95":null,"max":null},
                               "last_ok_at":null,"last_failure_at":null})
        );
    }

    #[test]
    fn a_dropped_call_guard_counts_as_a_lost_reply() {
        let o = Arc::new(obs(&["h"]));
        let h = o.handler_index("h");
        let g = CallGuard::start(&o, h);
        let p = CallGuard::start(&o, None); // a probe: in flight only
        assert_eq!(o.in_flight(), 2);
        drop(g); // aborted at its deadline
        drop(p);
        let done = CallGuard::start(&o, h);
        done.finish(CallResult::Ok);
        assert_eq!(o.in_flight(), 0);
        let m = o.metrics(Window::M15, wall_ms(), idx());
        let s = &m.handlers[0];
        assert_eq!((s.calls, s.ok, s.replies_lost), (2, 1, 1));
    }

    /// The caps bound memory: the estimate OPERATING.md quotes, from the
    /// structures' real sizes.
    #[test]
    fn memory_at_the_caps_is_bounded() {
        use std::mem::size_of;
        // The sizes OPERATING.md's table quotes.
        assert_eq!(size_of::<ObsEvent>(), 32);
        assert_eq!(size_of::<(Millis, Point)>(), 328);
        assert_eq!(size_of::<(Millis, HandlerBucket)>(), 240);
        assert_eq!(size_of::<(u64, Observed)>(), 72);
        assert_eq!(size_of::<(RunId, ObsEvent)>(), 40);
        assert_eq!(size_of::<Slot>(), 56);
        let none = memory_bound(0);
        let ten = memory_bound(10);
        assert!(none < 72 * 1024 * 1024, "{none}");
        assert!(ten - none < 8 * 1024 * 1024, "{}", ten - none);
    }
}
