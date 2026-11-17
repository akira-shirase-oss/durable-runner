//! The Tokio shell: one actor task owns `EngineCore`; handler invocations
//! run in bounded spawned tasks and answer through the same loop. Ordering
//! guarantee: `commit` fsyncs the journal before any handler is invoked, so
//! a crash can only lose what the journal already recorded.
//!
//! Idempotency: every call for a step — attempt 1 through the last retry —
//! carries the same key `{operation_key}/{step}`, so a repeating remote call
//! can be de-duplicated on the far side (contract §04, at-least-once).

use crate::writer::{self, GroupCommit, Writer, WriterMsg};
use crate::{EngineCore, EngineError};
use durable_domain::{
    Effect, JobSpec, Limits, Millis, OperationKey, Run, RunId, RunState, StepIndex, StepSpec,
};
use durable_journal::{JournalError, JournalOptions};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinSet};

/// What a handler can honestly report about an attempt.
#[derive(Debug)]
pub enum Outcome {
    /// Step finished; this output is checkpointed (bounded by limits).
    Checkpoint(Vec<u8>),
    /// The call ran and failed or timed out — a retry is safe.
    Failed,
    /// The attempt's fate is unknown (reply vanished, connection cut) —
    /// reconcile before any repeat.
    Lost,
}

/// An allowlisted step handler. Only registered names may execute — the
/// registry, not the job spec, is the trust boundary.
#[async_trait::async_trait]
pub trait StepHandler: Send + Sync {
    async fn execute(&self, step: &StepSpec, input: &[u8], idem_key: &str) -> Outcome;
    /// Reconciliation probe: did the call under `idem_key` land remotely?
    /// Default `false` — honest "unknown is not found"; handlers that can
    /// really check override this.
    async fn probe(&self, _step: &StepSpec, _idem_key: &str) -> bool {
        false
    }
}

/// Named handlers, populated by configuration. A step naming a handler not
/// in the registry fails its attempt — it never silently runs.
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<String, Arc<dyn StepHandler>>,
}

impl Registry {
    pub fn register(&mut self, name: &str, h: Arc<dyn StepHandler>) {
        self.handlers.insert(name.to_string(), h);
    }
    fn get(&self, name: &str) -> Option<Arc<dyn StepHandler>> {
        self.handlers.get(name).cloned()
    }
    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }
}

fn wall_ms() -> Millis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Millis)
        .unwrap_or(0)
}

/// The business idempotency key for a step: constant across its attempts.
pub fn idem_key(key: &OperationKey, step: StepIndex) -> String {
    format!("{}/{}", key.0, step.0)
}

enum Cmd {
    Submit {
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
        reply: oneshot::Sender<Result<RunId, EngineError>>,
    },
    Cancel {
        run: RunId,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    Compact {
        reply: oneshot::Sender<Result<u32, EngineError>>,
    },
    Backup {
        dest: std::path::PathBuf,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

/// Reads travel on their own bounded channel: a burst of (open,
/// unauthenticated) reads can fill it and be refused busy, but can never
/// take a slot a submit or cancel needs.
enum Read {
    Inspect {
        run: RunId,
        reply: oneshot::Sender<Option<Run>>,
    },
    List {
        order: Order,
        limit: usize,
        reply: oneshot::Sender<RunPage>,
    },
    Diag {
        reply: oneshot::Sender<crate::Diagnostics>,
    },
}

/// Handle to the running engine. Cheap to clone; the channel capacities
/// are the back-pressure bounds — a full channel refuses honestly.
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Cmd>,
    read_tx: mpsc::Sender<Read>,
    health: Arc<Health>,
}

/// State the actor publishes for readiness, readable without a round trip
/// through the (possibly full) command channel.
#[derive(Debug, Default)]
struct Health {
    space_low: AtomicBool,
}

/// The most runs one list call returns, whatever the caller asks for.
pub const MAX_PAGE: usize = 1000;

/// Which end a list page starts from, with its cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Ids ascending, after the cursor (oldest first).
    Oldest { after: Option<RunId> },
    /// Ids descending, before the cursor (newest first).
    Newest { before: Option<RunId> },
}

/// A page of runs and the cursor for the next one.
#[derive(Debug, Clone, Default)]
pub struct RunPage {
    pub runs: Vec<crate::RunSummary>,
    /// The cursor for the next page in the same order (`after` when oldest
    /// first, `before` when newest first); `None` on the last page.
    pub next: Option<RunId>,
}

/// Whether the engine should receive new work now, and if not, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readiness {
    pub ready: bool,
    /// A stable code: `engine_stopped` or `no_space`.
    pub reason: Option<&'static str>,
}

impl Engine {
    /// Enqueue a command without waiting: a full channel is honest
    /// back-pressure (busy, retry later); a closed one means the actor has
    /// stopped and nothing will ever answer.
    fn send(&self, cmd: Cmd) -> Result<(), EngineError> {
        self.tx.try_send(cmd).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                EngineError::Refused(durable_domain::Refusal::ReserveExhausted)
            }
            mpsc::error::TrySendError::Closed(_) => EngineError::Stopped,
        })
    }

    fn send_read(&self, read: Read) -> Result<(), EngineError> {
        self.read_tx.try_send(read).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                EngineError::Refused(durable_domain::Refusal::ReserveExhausted)
            }
            mpsc::error::TrySendError::Closed(_) => EngineError::Stopped,
        })
    }

    pub async fn submit(&self, key: OperationKey, spec: JobSpec) -> Result<RunId, EngineError> {
        self.submit_inner(key, spec, None).await
    }

    /// Submit with a delayed start: the run queues until `not_before`.
    pub async fn submit_at(
        &self,
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
    ) -> Result<RunId, EngineError> {
        self.submit_inner(key, spec, Some(not_before)).await
    }

    async fn submit_inner(
        &self,
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
    ) -> Result<RunId, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Submit {
            key,
            spec,
            not_before,
            reply,
        })?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    pub async fn cancel(&self, run: RunId) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Cancel { run, reply })?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    /// Compact the journal: snapshot live state into a new generation,
    /// atomically swap the manifest. Returns the new generation.
    pub async fn compact(&self) -> Result<u32, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Compact { reply })?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    /// Consistent backup of the journal to `dest` (manifest copied last).
    pub async fn backup_to(&self, dest: std::path::PathBuf) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Backup { dest, reply })?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    /// One page of the live index in id order: at most
    /// `min(limit, MAX_PAGE)` runs with id > `after`.
    pub async fn list(&self, after: Option<RunId>, limit: usize) -> Result<RunPage, EngineError> {
        self.list_in(Order::Oldest { after }, limit).await
    }

    /// One page in either order, at most `min(limit, MAX_PAGE)` runs.
    pub async fn list_in(&self, order: Order, limit: usize) -> Result<RunPage, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::List {
            order,
            limit: limit.clamp(1, MAX_PAGE),
            reply,
        })?;
        rx.await.map_err(|_| EngineError::Stopped)
    }

    /// Bounded diagnostics — counts by state kind, never rows.
    pub async fn diag(&self) -> Result<crate::Diagnostics, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Diag { reply })?;
        rx.await.map_err(|_| EngineError::Stopped)
    }

    /// Readiness: false once the actor has stopped or while free space is
    /// below the journal reserve (submits would be refused `NoSpace`).
    pub fn readiness(&self) -> Readiness {
        let reason = if self.tx.is_closed() {
            Some("engine_stopped")
        } else if self.health.space_low.load(Ordering::SeqCst) {
            Some("no_space")
        } else {
            None
        };
        Readiness {
            ready: reason.is_none(),
            reason,
        }
    }

    /// Resolves once the actor has stopped: a poisoned journal writer (a
    /// failed fsync is never retried) or a panic. The server exits on it so
    /// a supervisor restarts the process and startup replay decides what
    /// is durable.
    pub async fn closed(&self) {
        self.tx.closed().await;
    }

    pub async fn inspect(&self, run: RunId) -> Result<Option<Run>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Inspect { run, reply })?;
        rx.await.map_err(|_| EngineError::Stopped)
    }
}

/// Runtime knobs, all bounded.
#[derive(Debug, Clone)]
pub struct ShellConfig {
    /// Maximum handler calls (attempts and reconcile probes) in flight.
    /// A dispatch is journaled only while holding one of these permits.
    pub max_in_flight: usize,
    /// Upper bound on dispatches (and on deadline expiries) per tick, so a
    /// backlog of due runs cannot monopolise the actor.
    pub max_dispatch_per_tick: usize,
    /// Scheduler/deadline-sweep cadence.
    pub tick_ms: u64,
    /// Command channel capacity (submit back-pressure bound).
    pub cmd_capacity: usize,
    /// Read channel capacity (inspect/list/diagnostics), separate so reads
    /// cannot crowd out writes.
    pub read_capacity: usize,
    /// Domain bounds — deadline, payload sizes, attempts. Tests shrink the
    /// step deadline so crash-recovery paths don't wait 30s.
    pub limits: Limits,
    /// Journal options (tests inject storage faults here).
    pub journal: JournalOptions,
    /// Group-commit bounds for the writer thread.
    pub group_commit: GroupCommit,
    /// Records the actor may hold for the writer before it stops taking
    /// new commands (they then wait in, and are refused by, the bounded
    /// command channel).
    pub max_outbox_records: usize,
    /// The same bound in payload bytes.
    pub max_outbox_bytes: usize,
    /// Refuse a compaction whose snapshot would exceed this many bytes
    /// (`SnapshotOverBudget`) instead of writing it.
    pub max_snapshot_bytes: u64,
    /// Compact on this schedule (expiring 7-day-old terminal runs and
    /// deleting superseded generations); `None` (the default) = only on
    /// request.
    pub compact_every: Option<Duration>,
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 32,
            max_dispatch_per_tick: 64,
            tick_ms: 50,
            cmd_capacity: 1024,
            read_capacity: 256,
            limits: Limits::default(),
            journal: JournalOptions::default(),
            group_commit: GroupCommit::default(),
            max_outbox_records: 16 * 1024,
            max_outbox_bytes: 64 * 1024 * 1024,
            max_snapshot_bytes: 2 * 1024 * 1024 * 1024,
            // Off until compaction is cheap and its error path is fatal
            // (D-018): an operator schedules it explicitly.
            compact_every: None,
        }
    }
}

/// Open the journal, replay to rebuild, hand the journal to the
/// group-commit writer thread, then spawn the actor task. The returned
/// handle is the only public interface — there is no path to the journal
/// except through it.
pub fn spawn(dir: &Path, registry: Registry, cfg: ShellConfig) -> Result<Engine, EngineError> {
    let mut core = EngineCore::open_with(dir, cfg.journal.clone())?;
    core.limits = cfg.limits;
    let (core, journal) = core.into_piped();
    let journal = journal.ok_or(EngineError::Piped)?;
    let writer = writer::spawn(journal, cfg.group_commit.clone())
        .map_err(|e| EngineError::Journal(JournalError::Io(e)))?;
    let (tx, rx) = mpsc::channel::<Cmd>(cfg.cmd_capacity);
    let (read_tx, read_rx) = mpsc::channel::<Read>(cfg.read_capacity.max(1));
    let health = Arc::new(Health::default());
    health.space_low.store(!core.space_ok(), Ordering::SeqCst);
    let mon = Monitor {
        health: health.clone(),
        errors: BTreeMap::new(),
    };
    tokio::spawn(actor(core, registry, cfg, (rx, read_rx), mon, writer));
    Ok(Engine {
        tx,
        read_tx,
        health,
    })
}

/// Errors the runtime handles itself — a handler outcome or a deadline
/// expiry that could not be journaled has no caller to answer — are
/// counted by kind (exposed in diagnostics) and drive readiness. Nothing
/// is dropped silently.
struct Monitor {
    health: Arc<Health>,
    errors: BTreeMap<&'static str, u64>,
}

impl Monitor {
    fn kind(e: &EngineError) -> &'static str {
        fn journal(e: &JournalError) -> &'static str {
            match e {
                JournalError::NoSpace { .. } => "no_space",
                JournalError::SpaceProbe(_) => "space_unknown",
                JournalError::WriteFailed { .. } => "journal_write_failed",
                JournalError::SyncFailed(_)
                | JournalError::Poisoned
                | JournalError::ManifestSwapFailed(_)
                | JournalError::ManifestMismatch { .. } => "journal_poisoned",
                _ => "internal",
            }
        }
        match e {
            EngineError::Refused(_) => "stale_event",
            EngineError::SnapshotOverBudget { .. } => "snapshot_over_budget",
            EngineError::Journal(j) => journal(j),
            EngineError::CommitFailed(j) => journal(j),
            _ => "internal",
        }
    }

    /// Record the free-space state; log only on a transition.
    fn space(&mut self, ok: bool) {
        let was_low = self.health.space_low.swap(!ok, Ordering::SeqCst);
        if was_low == ok {
            if ok {
                eprintln!("durable: free space back above the reserve; admitting work");
            } else {
                eprintln!(
                    "durable: free space below the reserve; refusing new work (503 no_space)"
                );
            }
        }
    }

    /// An error answered to a caller: it is theirs to handle, but a full
    /// disk still flips readiness.
    fn returned(&mut self, e: &EngineError) {
        if Self::kind(e) == "no_space" {
            self.space(false);
        }
    }

    /// An error with no caller: count it, log anything unexpected.
    fn handled(&mut self, e: &EngineError) {
        let kind = Self::kind(e);
        *self.errors.entry(kind).or_insert(0) += 1;
        match kind {
            "no_space" => self.space(false),
            // A late reply for a run that moved on is expected, not news.
            "stale_event" => {}
            _ => eprintln!("durable: {kind}: {e}"),
        }
    }

    fn effects(&mut self, r: Result<Vec<Effect>, EngineError>) -> Vec<Effect> {
        r.unwrap_or_else(|e| {
            self.handled(&e);
            Vec::new()
        })
    }
}

/// An attempt's result comes back with the permit it ran under, so a
/// chained step (the checkpoint that moves the run to its next step) can
/// inherit it instead of racing other runs for capacity.
type AttemptJoin = (RunId, StepIndex, Outcome, OwnedSemaphorePermit);
type ProbeJoin = (RunId, StepIndex, bool, OwnedSemaphorePermit);

/// Sends a held answer: `None` once durable, `Some(err)` if the commit failed.
type HeldReply = Box<dyn FnOnce(Option<&Arc<JournalError>>) + Send>;

/// Something a record decided that may not leave the process until the
/// fsync covering that record has returned (D-014 rule a).
enum Held {
    /// An answer to a caller. `Some(err)`: the commit failed — the caller
    /// gets `CommitFailed` (or, for a read, the engine stops).
    Reply(HeldReply),
    Fire {
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    },
    Probe {
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    },
}

/// The actor's side of group commit: records waiting for the writer,
/// what has been sent and made durable, and everything held until then.
struct Pipeline {
    tx: Option<SyncSender<WriterMsg>>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Messages not yet handed to the writer, in commit order.
    outbound: VecDeque<WriterMsg>,
    outbound_records: usize,
    outbound_bytes: usize,
    /// Commit index of the last record handed to the writer.
    sent_upto: u64,
    /// Commit index the writer has reported durable.
    durable: u64,
    held: VecDeque<(u64, Held)>,
    max_batch: usize,
    max_batch_bytes: usize,
}

impl Pipeline {
    fn new(
        writer: Option<(SyncSender<WriterMsg>, std::thread::JoinHandle<()>)>,
        max_batch: usize,
        max_batch_bytes: usize,
    ) -> Self {
        let (tx, thread) = match writer {
            Some((tx, t)) => (Some(tx), Some(t)),
            None => (None, None),
        };
        Self {
            tx,
            thread,
            outbound: VecDeque::new(),
            outbound_records: 0,
            outbound_bytes: 0,
            sent_upto: 0,
            durable: 0,
            held: VecDeque::new(),
            max_batch: max_batch.max(1),
            max_batch_bytes,
        }
    }

    /// Move the core's newly committed records into the outbound queue,
    /// merging into the last append so the writer sees few, large
    /// messages — each within `max_batch` records and `max_batch_bytes`.
    fn collect(&mut self, core: &mut EngineCore) {
        while let Some(b) = core.take_batch(self.max_batch, self.max_batch_bytes) {
            self.outbound_records += b.records.len();
            self.outbound_bytes += b.bytes;
            match self.outbound.back_mut() {
                Some(WriterMsg::Append {
                    records: queued,
                    bytes,
                    upto: u,
                }) if queued.len() + b.records.len() <= self.max_batch
                    && *bytes + b.bytes <= self.max_batch_bytes =>
                {
                    queued.extend(b.records);
                    *bytes += b.bytes;
                    *u = b.upto;
                }
                _ => self.outbound.push_back(WriterMsg::Append {
                    records: b.records,
                    bytes: b.bytes,
                    upto: b.upto,
                }),
            }
        }
    }

    /// Hand queued messages to the writer, but only once everything sent
    /// before is durable: one batch is in flight while the next one
    /// accumulates (that is the group commit). False if the writer is gone.
    fn flush(&mut self) -> bool {
        if self.sent_upto > self.durable || self.outbound.is_empty() {
            return true;
        }
        let Some(tx) = &self.tx else { return true };
        while let Some(msg) = self.outbound.pop_front() {
            let (n, nb) = (msg.records(), msg.bytes());
            let upto = match &msg {
                WriterMsg::Append { upto, .. } => Some(*upto),
                _ => None,
            };
            match tx.try_send(msg) {
                Ok(()) => {
                    self.outbound_records -= n;
                    self.outbound_bytes -= nb;
                    if let Some(u) = upto {
                        self.sent_upto = u;
                    }
                }
                Err(TrySendError::Full(m)) => {
                    self.outbound.push_front(m);
                    break; // the writer reports when it frees up
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }
        true
    }

    /// Records and payload bytes decided but not yet handed to the writer.
    fn backlog(&self, core: &EngineCore) -> (usize, usize) {
        (
            self.outbound_records + core.pending_records(),
            self.outbound_bytes + core.pending_bytes(),
        )
    }

    /// Answer a caller once `needs` is durable.
    fn answer<T: Send + 'static>(
        &mut self,
        needs: u64,
        tx: oneshot::Sender<Result<T, EngineError>>,
        value: Result<T, EngineError>,
    ) {
        if needs <= self.durable {
            let _ = tx.send(value); // a caller that went away still got its commit
            return;
        }
        self.held.push_back((
            needs,
            Held::Reply(Box::new(move |failed| {
                let _ = tx.send(match failed {
                    None => value,
                    Some(e) => Err(EngineError::CommitFailed(e.clone())),
                });
            })),
        ));
    }

    /// Answer a read once everything it reflects is durable — a read never
    /// shows state a crash could take back. On failure the caller sees the
    /// engine stop.
    fn answer_read<T: Send + 'static>(&mut self, needs: u64, tx: oneshot::Sender<T>, value: T) {
        if needs <= self.durable {
            let _ = tx.send(value);
            return;
        }
        self.held.push_back((
            needs,
            Held::Reply(Box::new(move |failed| {
                if failed.is_none() {
                    let _ = tx.send(value);
                }
            })),
        ));
    }

    /// The writer failed: everything still held fails — callers get
    /// `CommitFailed`, held handler calls and probes never happen.
    fn fail_held(&mut self, err: &Arc<JournalError>) {
        while let Some((_, h)) = self.held.pop_front() {
            if let Held::Reply(f) = h {
                f(Some(err));
            }
        }
    }

    /// Stop the writer and wait for it to release the journal.
    async fn shutdown(&mut self) {
        self.outbound.clear();
        self.tx = None; // the writer's recv fails once the sender is gone
        if let Some(t) = self.thread.take() {
            if tokio::task::spawn_blocking(move || t.join()).await.is_err() {
                eprintln!("durable: journal writer thread could not be joined");
            }
        }
    }
}

/// A compaction handed to the writer, finished only when it reports back.
/// `reply` is `None` for a scheduled compaction (nobody to answer: its
/// outcome is counted instead).
struct PendingCompaction {
    done: oneshot::Receiver<Result<u32, JournalError>>,
    expired: Vec<RunId>,
    reply: Option<CompactReply>,
}

type CompactReply = oneshot::Sender<Result<u32, EngineError>>;

/// Plan a compaction and queue it behind every record committed so far —
/// the snapshot never covers a record the log lacks.
fn start_compaction(
    core: &mut EngineCore,
    pipe: &mut Pipeline,
    budget: u64,
    reply: Option<CompactReply>,
) -> Result<PendingCompaction, (EngineError, Option<CompactReply>)> {
    let plan = match core.plan_compaction(wall_ms()) {
        Ok(p) => p,
        Err(e) => return Err((e, reply)),
    };
    if plan.bytes > budget {
        let e = EngineError::SnapshotOverBudget {
            bytes: plan.bytes,
            budget,
        };
        return Err((e, reply));
    }
    pipe.collect(core);
    let (done_tx, done) = oneshot::channel();
    pipe.outbound.push_back(WriterMsg::Compact {
        view: plan.view,
        bytes: plan.bytes,
        reply: done_tx,
    });
    Ok(PendingCompaction {
        done,
        expired: plan.expired,
        reply,
    })
}

async fn scheduled(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending().await,
    }
}

async fn compaction_done(
    p: &mut Option<PendingCompaction>,
) -> Result<Result<u32, JournalError>, oneshot::error::RecvError> {
    match p {
        Some(c) => (&mut c.done).await,
        None => std::future::pending().await,
    }
}

async fn actor(
    mut core: EngineCore,
    registry: Registry,
    cfg: ShellConfig,
    (mut rx, mut read_rx): (mpsc::Receiver<Cmd>, mpsc::Receiver<Read>),
    mut mon: Monitor,
    writer: Writer,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.tick_ms));
    let mut attempts: JoinSet<AttemptJoin> = JoinSet::new();
    let mut probes: JoinSet<ProbeJoin> = JoinSet::new();
    let mut live: HashMap<(RunId, StepIndex), AbortHandle> = HashMap::new();
    let mut waiting_probes = ProbeQueue::default();
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight));
    let mut status = writer.status;
    let mut pipe = Pipeline::new(
        Some((writer.tx, writer.thread)),
        cfg.group_commit.max_batch_records,
        cfg.group_commit.max_batch_bytes,
    );
    let mut pending_compaction: Option<PendingCompaction> = None;
    // First run one period after start, then every period.
    let mut compactor = cfg.compact_every.map(|every| {
        let mut t = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        t
    });
    // Set when the writer is gone without reporting a failure (it panicked).
    let mut writer_lost = false;

    macro_rules! exec {
        () => {
            Exec {
                registry: &registry,
                sem: sem.clone(),
                attempts: &mut attempts,
                probes: &mut probes,
                live: &mut live,
                waiting_probes: &mut waiting_probes,
                per_tick: cfg.max_dispatch_per_tick,
                mon: &mut mon,
                pipe: &mut pipe,
            }
        };
    }

    // Crash-window orphans: a run journaled Reconciling whose probe died
    // with the last process gets a fresh probe. Reconcile verdicts are
    // idempotent — a run that moved on refuses NothingToReconcile.
    let orphaned: Vec<(RunId, StepIndex)> = core
        .runs()
        .filter_map(|r| match r.state {
            RunState::Reconciling { step, .. } => Some((r.id, step)),
            _ => None,
        })
        .collect();
    if !orphaned.is_empty() {
        let mut ex = exec!();
        for (run, step) in orphaned {
            ex.probe(&mut core, run, step);
        }
    }

    loop {
        if core.is_poisoned() || writer_lost {
            // A write or fsync failed (never retried) or the writer died:
            // memory is ahead of disk. Fail everything still held, stop the
            // writer, and exit so a restart replays what is durable.
            let err = status
                .borrow()
                .failed
                .clone()
                .unwrap_or_else(|| Arc::new(JournalError::Poisoned));
            eprintln!(
                "durable: journal commit failed ({err}); engine stopping so a restart replays"
            );
            pipe.fail_held(&err);
            if let Some(reply) = pending_compaction.take().and_then(|c| c.reply) {
                let _ = reply.send(Err(EngineError::CommitFailed(err.clone())));
            }
            pipe.shutdown().await;
            return;
        }
        let (backlog_records, backlog_bytes) = pipe.backlog(&core);
        let accepting =
            backlog_records < cfg.max_outbox_records && backlog_bytes < cfg.max_outbox_bytes;
        tokio::select! {
            cmd = rx.recv(), if accepting => match cmd {
                Some(Cmd::Submit { key, spec, not_before, reply }) => {
                    let now = wall_ms();
                    // Admission is where the allowlist bites: a step naming a
                    // handler outside the registry is a spec error, refused
                    // before anything is journaled.
                    let missing = spec
                        .steps
                        .iter()
                        .find(|s| !registry.contains(&s.handler))
                        .map(|s| s.handler.clone());
                    let r = match missing {
                        Some(name) => Err(EngineError::UnregisteredHandler(name)),
                        None => core.submit_at(key, spec, not_before.unwrap_or(now), now),
                    };
                    if let Err(e) = &r {
                        mon.returned(e);
                    }
                    pipe.answer(core.last_index(), reply, r);
                }
                Some(Cmd::Cancel { run, reply }) => {
                    let r = core.cancel(run, wall_ms()).map(|_| ());
                    if let Err(e) = &r {
                        mon.returned(e);
                    }
                    pipe.answer(core.last_index(), reply, r);
                }
                Some(Cmd::Compact { reply }) => {
                    if pending_compaction.is_some() {
                        let _ = reply.send(Err(EngineError::Refused(
                            durable_domain::Refusal::ReserveExhausted,
                        )));
                    } else {
                        match start_compaction(&mut core, &mut pipe, cfg.max_snapshot_bytes, Some(reply)) {
                            Ok(p) => pending_compaction = Some(p),
                            Err((e, reply)) => {
                                if let Some(reply) = reply {
                                    let _ = reply.send(Err(e));
                                }
                            }
                        }
                    }
                }
                Some(Cmd::Backup { dest, reply }) => {
                    pipe.collect(&mut core);
                    pipe.outbound.push_back(WriterMsg::Backup { dest, reply });
                }
                None => {
                    pipe.shutdown().await;
                    return;
                }
            },
            read = read_rx.recv() => match read {
                Some(Read::Inspect { run, reply }) => {
                    pipe.answer_read(core.last_index(), reply, core.run(run).cloned());
                }
                Some(Read::List { order, limit, reply }) => {
                    let (runs, next) = match order {
                        Order::Oldest { after } => core.page(after, limit),
                        Order::Newest { before } => core.page_newest(before, limit),
                    };
                    pipe.answer_read(core.last_index(), reply, RunPage { runs, next });
                }
                Some(Read::Diag { reply }) => {
                    let mut d = core.diagnostics();
                    d.errors = mon.errors.clone();
                    pipe.answer_read(core.last_index(), reply, d);
                }
                // Every handle is gone (the command channel closes with it).
                None => {
                    pipe.shutdown().await;
                    return;
                }
            },
            Some(Ok((run, step, outcome, permit))) = attempts.join_next() => {
                live.remove(&(run, step));
                let (out, fail) = match outcome {
                    Outcome::Checkpoint(o) => (Some(o), None),
                    Outcome::Failed => (None, Some("failed".to_string())),
                    Outcome::Lost => (None, None),
                };
                let mut ex = exec!();
                // Not journaled (e.g. NoSpace): counted; the run stays
                // Running and its deadline sweep reconciles the effect.
                let effects = ex.mon.effects(core.step_finished(run, step, out, fail, wall_ms()));
                run_effects(&mut core, &mut ex, effects, run, Some(permit));
            }
            Some(Ok((run, step, found, permit))) = probes.join_next() => {
                let mut ex = exec!();
                let effects = ex.mon.effects(core.reconciled(run, step, found, wall_ms()));
                run_effects(&mut core, &mut ex, effects, run, Some(permit));
            }
            r = compaction_done(&mut pending_compaction) => {
                if let Some(c) = pending_compaction.take() {
                    let answer = match r {
                        Ok(Ok(generation)) => {
                            // The new generation is durable: only now does
                            // expiry reach the live index (all or nothing).
                            core.finish_compaction(&c.expired);
                            Ok(generation)
                        }
                        Ok(Err(e)) => Err(EngineError::Journal(e)),
                        Err(_) => Err(EngineError::Stopped),
                    };
                    match (c.reply, answer) {
                        (Some(reply), answer) => {
                            if let Err(e) = &answer {
                                mon.returned(e);
                            }
                            let _ = reply.send(answer);
                        }
                        // Scheduled: nobody to answer, so a failure is counted.
                        (None, Err(e)) => mon.handled(&e),
                        (None, Ok(_)) => {}
                    }
                }
            }
            changed = status.changed() => match changed {
                Ok(()) => {
                    let st = status.borrow_and_update().clone();
                    core.observe(&st);
                    pipe.durable = st.durable;
                    let mut ex = exec!();
                    release(&mut core, &mut ex);
                }
                Err(_) => writer_lost = true,
            },
            _ = ticker.tick() => {
                let mut ex = exec!();
                tick(&mut core, &mut ex);
            }
            () = scheduled(&mut compactor) => {
                // Retention on a schedule; skipped while one is running.
                if pending_compaction.is_none() {
                    match start_compaction(&mut core, &mut pipe, cfg.max_snapshot_bytes, None) {
                        Ok(p) => pending_compaction = Some(p),
                        Err((e, _)) => mon.handled(&e),
                    }
                }
            }
        }
        pipe.collect(&mut core);
        if !pipe.flush() {
            writer_lost = true;
        }
    }
}

/// Release everything held whose records are now durable, in order.
fn release(core: &mut EngineCore, ex: &mut Exec<'_>) {
    while ex
        .pipe
        .held
        .front()
        .is_some_and(|(needs, _)| *needs <= ex.pipe.durable)
    {
        let Some((_, held)) = ex.pipe.held.pop_front() else {
            break;
        };
        match held {
            Held::Reply(answer) => answer(None),
            Held::Fire { run, step, permit } => ex.start_attempt(core, run, step, permit),
            Held::Probe { run, step, permit } => ex.start_probe(core, run, step, permit),
        }
    }
}

/// Reconcile probes waiting for a permit, oldest first, each run once.
/// Bounded by the live index: a run is queued at most once.
#[derive(Default)]
struct ProbeQueue {
    order: VecDeque<(RunId, StepIndex)>,
    queued: HashSet<(RunId, StepIndex)>,
}

impl ProbeQueue {
    fn push(&mut self, item: (RunId, StepIndex)) {
        if self.queued.insert(item) {
            self.order.push_back(item);
        }
    }
    fn pop(&mut self) -> Option<(RunId, StepIndex)> {
        let item = self.order.pop_front()?;
        self.queued.remove(&item);
        Some(item)
    }
}

/// The executor's plumbing, bundled so helpers stay under the arg limit.
struct Exec<'a> {
    registry: &'a Registry,
    sem: Arc<Semaphore>,
    attempts: &'a mut JoinSet<AttemptJoin>,
    probes: &'a mut JoinSet<ProbeJoin>,
    /// Attempts this process fired, so a deadline sweep can cancel the
    /// local call and free its permit (cancelling cannot undo a remote
    /// effect; the reconcile probe decides that).
    live: &'a mut HashMap<(RunId, StepIndex), AbortHandle>,
    waiting_probes: &'a mut ProbeQueue,
    per_tick: usize,
    mon: &'a mut Monitor,
    pipe: &'a mut Pipeline,
}

impl Exec<'_> {
    /// Invoke a step's handler for the run's current attempt under
    /// `permit` — once the record that made it `Running` (committed only
    /// because this permit was held) is durable. Until then it is held.
    fn fire(
        &mut self,
        core: &EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    ) {
        let needs = core.last_index();
        if needs <= self.pipe.durable {
            self.start_attempt(core, run, step, permit);
        } else {
            self.pipe
                .held
                .push_back((needs, Held::Fire { run, step, permit }));
        }
    }

    /// The call itself, for a run still `Running` this step (a cancel
    /// committed while the dispatch was held means no call: the permit
    /// returns).
    fn start_attempt(
        &mut self,
        core: &EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    ) {
        let Some(r) = core.run(run) else { return };
        if !matches!(r.state, RunState::Running { step: s, .. } if s == step) {
            return;
        }
        let Some(sspec) = r.spec.steps.get(step.0 as usize).cloned() else {
            return;
        };
        let Some(handler) = self.registry.get(&sspec.handler) else {
            return;
        };
        let idem = idem_key(&r.key, step);
        let input = r.spec.input.clone();
        let handle = self.attempts.spawn(async move {
            let outcome = handler.execute(&sspec, &input, &idem).await;
            (run, step, outcome, permit)
        });
        self.live.insert((run, step), handle);
    }

    /// Fail a step's attempt (journaled; retry/terminal decided by domain).
    fn fail(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex, why: &str) {
        let effects =
            self.mon
                .effects(core.step_finished(run, step, None, Some(why.to_string()), wall_ms()));
        run_effects(core, self, effects, run, None);
    }

    /// Probe for a reconciling step; the verdict returns through the loop.
    /// A probe is a handler call, so it takes a permit like any other; with
    /// none free it waits in `waiting_probes` for the next tick.
    fn probe(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex) {
        match self.sem.clone().try_acquire_owned() {
            Ok(permit) => self.probe_with(core, run, step, permit),
            Err(_) => self.waiting_probes.push((run, step)),
        }
    }

    /// A probe is a handler call too: held until the record that made the
    /// run `Reconciling` is durable.
    fn probe_with(
        &mut self,
        core: &mut EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    ) {
        let needs = core.last_index();
        if needs <= self.pipe.durable {
            self.start_probe(core, run, step, permit);
        } else {
            self.pipe
                .held
                .push_back((needs, Held::Probe { run, step, permit }));
        }
    }

    fn start_probe(
        &mut self,
        core: &mut EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    ) {
        let Some(r) = core.run(run) else { return };
        if !matches!(r.state, RunState::Reconciling { step: s, .. } if s == step) {
            return; // the run moved on while the probe waited
        }
        let Some(sspec) = r.spec.steps.get(step.0 as usize).cloned() else {
            return;
        };
        let Some(handler) = self.registry.get(&sspec.handler) else {
            // Cannot probe an unregistered handler — reconcile as not-found
            // rather than leave the run stuck.
            let effects = self
                .mon
                .effects(core.reconciled(run, step, false, wall_ms()));
            run_effects(core, self, effects, run, Some(permit));
            return;
        };
        let idem = idem_key(&r.key, step);
        self.probes.spawn(async move {
            let found = handler.probe(&sspec, &idem).await;
            (run, step, found, permit)
        });
    }
}

/// The periodic pass: sweep expired attempts, start waiting probes, then
/// dispatch what's due — each step bounded by `max_dispatch_per_tick`.
/// A permit is taken **before** `StepDispatched` is journaled: with none
/// free the run stays queued (nothing happened, nothing is recorded).
fn tick(core: &mut EngineCore, exec: &mut Exec<'_>) {
    let now = wall_ms();
    // Below the reserve nothing new can be journaled: no expiry, no
    // dispatch (a call whose outcome cannot be recorded is not started).
    // In-flight work waits; readiness reports `no_space`.
    let space_ok = core.space_ok();
    exec.mon.space(space_ok);
    if !space_ok {
        return;
    }
    // Deadline sweep: an attempt out past its deadline is a lost reply —
    // it reconciles, never silently repeats. `live` only tracks attempts
    // this process fired; a run orphaned by a restart has no entry, and
    // gating on it would strand every crash-surviving Running run.
    for (run, step) in core.expired_attempts(now).into_iter().take(exec.per_tick) {
        if let Some(call) = exec.live.remove(&(run, step)) {
            call.abort(); // frees its permit; the remote effect is reconciled
        }
        let effects = exec
            .mon
            .effects(core.step_finished(run, step, None, None, now));
        run_effects(core, exec, effects, run, None);
    }
    // Probes that found no permit when their run started reconciling.
    while let Some((run, step)) = exec.waiting_probes.pop() {
        let Ok(permit) = exec.sem.clone().try_acquire_owned() else {
            exec.waiting_probes.push((run, step)); // back of the queue: no starvation of others
            break;
        };
        exec.probe_with(core, run, step, permit);
    }
    // Dispatch: permit first, then the journal commits StepDispatched,
    // then the handler is invoked — a crash between the commit and the
    // call can only lose a journaled fact, and no commit is ever made for
    // a call that capacity would not allow.
    for (run, step, attempt) in core.due(now).into_iter().take(exec.per_tick) {
        let Some(name) = core
            .run(run)
            .and_then(|r| r.spec.steps.get(step.0 as usize))
            .map(|s| s.handler.clone())
        else {
            continue;
        };
        if !exec.registry.contains(&name) {
            // The handler left the registry after this run was accepted:
            // it stays queued — registry repair or a cancel resolves it.
            // Refusing an event here would be noise; there is nothing to
            // journal because nothing happened.
            continue;
        }
        let Ok(permit) = exec.sem.clone().try_acquire_owned() else {
            break; // at capacity: the rest stay queued for a later tick
        };
        if let Err(e) = core.dispatch(run, step, attempt, now) {
            exec.mon.handled(&e); // stale claim or no space; the permit returns
            continue;
        }
        exec.fire(core, run, step, permit);
    }
}

/// Carry out the reducer's effects. `permit` is the capacity the event
/// arrived with (a finished attempt or probe); a `DispatchStep` effect —
/// the run already journaled Running on its next step — inherits it.
fn run_effects(
    core: &mut EngineCore,
    exec: &mut Exec<'_>,
    effects: Vec<Effect>,
    run: RunId,
    mut permit: Option<OwnedSemaphorePermit>,
) {
    for eff in effects {
        match eff {
            // The checkpoint commit already moved the run to Running — the
            // record IS this step's durable dispatch; just invoke it.
            Effect::DispatchStep { step, .. } => {
                let name = core
                    .run(run)
                    .and_then(|r| r.spec.steps.get(step.0 as usize))
                    .map(|s| s.handler.clone())
                    .unwrap_or_default();
                if !exec.registry.contains(&name) {
                    exec.fail(core, run, step, "unregistered handler");
                    continue;
                }
                // Every DispatchStep comes from an event carrying a permit,
                // so this always fires; if it ever cannot, the run stays
                // journaled Running and the deadline sweep reconciles it —
                // it is never silently skipped.
                if let Some(p) = permit
                    .take()
                    .or_else(|| exec.sem.clone().try_acquire_owned().ok())
                {
                    exec.fire(core, run, step, p);
                }
            }
            Effect::Reconcile { step } => match permit.take() {
                Some(p) => exec.probe_with(core, run, step, p),
                None => exec.probe(core, run, step),
            },
            // Wakes are covered by the ticker; checkpoints are already in
            // the journaled record; terminal transitions are recorded.
            Effect::ScheduleWake { .. }
            | Effect::CommitCheckpoint { .. }
            | Effect::Finish
            | Effect::EmitTerminal => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_domain::RunState;

    struct Echo;
    #[async_trait::async_trait]
    impl StepHandler for Echo {
        async fn execute(&self, _s: &StepSpec, input: &[u8], _k: &str) -> Outcome {
            Outcome::Checkpoint(input.to_vec())
        }
    }

    struct FlakyLost;
    #[async_trait::async_trait]
    impl StepHandler for FlakyLost {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            Outcome::Lost
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            true // the remote call did land
        }
    }

    /// Hangs mid-flight on its first call, answers every later one — the
    /// shape of a worker killed between dispatch and reply.
    struct HangOnce {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StepHandler for HangOnce {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                std::future::pending().await
            }
            Outcome::Checkpoint(b"ok".to_vec())
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            true // reconcile finds the effect landed — never re-fires
        }
    }

    /// Reports Lost, then its first probe hangs (in-flight at kill time);
    /// every later probe answers found.
    struct LostProbe {
        probes: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StepHandler for LostProbe {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            Outcome::Lost
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            if self
                .probes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                std::future::pending().await
            }
            true
        }
    }

    /// Holds every call until the gate gets permits; counts calls and the
    /// peak number of calls in flight. Its probe answers "not found", so a
    /// run journaled Running without a call would burn an attempt.
    #[derive(Clone)]
    struct Gated {
        gate: Arc<Semaphore>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Default for Gated {
        fn default() -> Self {
            Self {
                gate: Arc::new(Semaphore::new(0)),
                calls: Arc::default(),
                in_flight: Arc::default(),
                peak: Arc::default(),
            }
        }
    }
    #[async_trait::async_trait]
    impl StepHandler for Gated {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            let opened = self.gate.acquire().await.map(|p| p.forget());
            self.in_flight.fetch_sub(1, SeqCst);
            match opened {
                Ok(()) => Outcome::Checkpoint(b"ok".to_vec()),
                Err(_) => Outcome::Failed,
            }
        }
    }

    async fn running_count(eng: &Engine) -> usize {
        eng.list(None, MAX_PAGE)
            .await
            .unwrap_or_default()
            .runs
            .iter()
            .filter(|r| matches!(r.state, RunState::Running { .. }))
            .count()
    }

    fn spec1(handler: &str) -> JobSpec {
        JobSpec {
            version: 1,
            input: b"payload".to_vec(),
            steps: vec![StepSpec {
                name: "only".into(),
                handler: handler.into(),
            }],
        }
    }

    fn fast_limits() -> Limits {
        Limits {
            max_step_deadline_ms: 100,
            ..Limits::default()
        }
    }

    async fn wait_state(eng: &Engine, run: RunId, kind: &str) -> Run {
        for _ in 0..500 {
            if let Some(r) = eng.inspect(run).await.unwrap_or(None) {
                if r.state.kind() == kind {
                    return r;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run never reached state {kind}");
    }

    /// kill -9 between dispatch and reply, then restart on the same
    /// journal: the orphaned Running attempt must reconcile — never sit
    /// `running` forever, and never blindly re-fire the remote call.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_orphaned_running_run_reconciles() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cfg = ShellConfig {
            tick_ms: 5,
            limits: fast_limits(),
            ..Default::default()
        };
        let run;
        {
            let mut reg = Registry::default();
            reg.register(
                "hang",
                Arc::new(HangOnce {
                    calls: calls.clone(),
                }),
            );
            let eng = spawn(d.path(), reg, cfg.clone()).unwrap_or_else(|e| panic!("{e}"));
            run = eng
                .submit(OperationKey("orphan-running".into()), spec1("hang"))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            wait_state(&eng, run, "running").await;
            drop(eng); // the process dies mid-flight; the journal keeps Running
            tokio::time::sleep(Duration::from_millis(50)).await; // actor exits, LOCK frees
        }
        let mut reg = Registry::default();
        reg.register(
            "hang",
            Arc::new(HangOnce {
                calls: calls.clone(),
            }),
        );
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "reconcile must not blindly repeat the remote call"
        );
    }

    /// Crash after ReplyLost committed Reconciling but before the probe
    /// verdict: restart must re-probe, not strand the run in Reconciling.
    #[tokio::test(flavor = "current_thread")]
    async fn restart_orphaned_reconciling_run_reprobes() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cfg = ShellConfig {
            tick_ms: 5,
            limits: fast_limits(),
            ..Default::default()
        };
        let run;
        {
            let mut reg = Registry::default();
            reg.register(
                "lostprobe",
                Arc::new(LostProbe {
                    probes: probes.clone(),
                }),
            );
            let eng = spawn(d.path(), reg, cfg.clone()).unwrap_or_else(|e| panic!("{e}"));
            run = eng
                .submit(
                    OperationKey("orphan-reconciling".into()),
                    spec1("lostprobe"),
                )
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            wait_state(&eng, run, "reconciling").await;
            drop(eng); // probe#1 hangs forever — the crash window
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut reg = Registry::default();
        reg.register(
            "lostprobe",
            Arc::new(LostProbe {
                probes: probes.clone(),
            }),
        );
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        assert!(
            probes.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "restart must issue a fresh reconcile probe"
        );
    }

    fn spec(handler: &str) -> JobSpec {
        JobSpec {
            version: 1,
            input: b"payload".to_vec(),
            steps: vec![
                StepSpec {
                    name: "a".into(),
                    handler: handler.into(),
                },
                StepSpec {
                    name: "b".into(),
                    handler: handler.into(),
                },
            ],
        }
    }

    async fn until_done(eng: &Engine, run: RunId) -> RunState {
        for _ in 0..200 {
            if let Some(r) = eng.inspect(run).await.unwrap_or(None) {
                if r.state.is_terminal() {
                    return r.state;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run never reached a terminal state");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn submit_runs_to_done() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("echo", Arc::new(Echo));
        let eng = spawn(
            d.path(),
            reg,
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let run = eng
            .submit(OperationKey("k1".into()), spec("echo"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lost_reply_reconciles_and_advances() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("flaky", Arc::new(FlakyLost));
        let eng = spawn(
            d.path(),
            reg,
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let run = eng
            .submit(OperationKey("k2".into()), spec("flaky"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // every attempt is Lost, but the probe says the effect landed —
        // the run advances without ever repeating the remote call.
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unregistered_handler_refused_at_admission() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let eng = spawn(
            d.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        // The registry is the execution trust boundary: a step naming a
        // handler it doesn't know is a spec error, refused before the
        // journal sees anything — no run, no record, nothing to replay.
        let err = eng
            .submit(OperationKey("bad".into()), spec("ghost"))
            .await
            .err()
            .unwrap_or_else(|| panic!("unregistered handler must be refused"));
        assert!(matches!(err, EngineError::UnregisteredHandler(_)));
        assert!(eng
            .list(None, MAX_PAGE)
            .await
            .unwrap_or_default()
            .runs
            .is_empty());
    }

    /// Saturation above `max_in_flight`: a run may be journaled Running
    /// only when a handler call actually holds a permit. Before the fix
    /// every due run was journaled `StepDispatched` and the calls beyond
    /// capacity were silently skipped — the run sat Running until its
    /// deadline, reconciled "not found", and burned an attempt.
    #[tokio::test(flavor = "current_thread")]
    async fn saturation_never_journals_a_dispatch_without_a_permit() {
        use std::sync::atomic::Ordering::SeqCst;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let h = Gated::default();
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(h.clone()));
        let cfg = ShellConfig {
            tick_ms: 5,
            max_in_flight: 2,
            ..Default::default()
        };
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let mut runs = Vec::new();
        for i in 0..6 {
            runs.push(
                eng.submit(OperationKey(format!("sat-{i}")), spec1("gated"))
                    .await
                    .unwrap_or_else(|e| panic!("{e}")),
            );
        }
        tokio::time::sleep(Duration::from_millis(150)).await; // many ticks
        let running = running_count(&eng).await;
        assert!(
            running <= 2,
            "{running} runs journaled Running with only 2 permits"
        );
        assert_eq!(
            running,
            h.calls.load(SeqCst),
            "every Running run has a live call"
        );

        h.gate.add_permits(64);
        for run in runs {
            assert!(matches!(until_done(&eng, run).await, RunState::Done));
        }
        assert_eq!(h.calls.load(SeqCst), 6, "each step called exactly once");
        assert!(h.peak.load(SeqCst) <= 2);
    }

    /// One tick dispatches at most `max_dispatch_per_tick` runs, oldest
    /// first, even with permits to spare — a backlog cannot monopolise the
    /// actor (and every dispatch is an fsync).
    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_per_tick_is_capped() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let mut first = None;
        for i in 0..10 {
            let id = core
                .submit(OperationKey(format!("cap-{i}")), spec1("gated"), i)
                .unwrap_or_else(|e| panic!("{e}"));
            first.get_or_insert(id);
        }
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(Gated::default()));
        let sem = Arc::new(Semaphore::new(10));
        let (mut attempts, mut probes) = (JoinSet::new(), JoinSet::new());
        let (mut live, mut waiting) = (HashMap::new(), ProbeQueue::default());
        let mut mon = Monitor {
            health: Arc::default(),
            errors: BTreeMap::new(),
        };
        // Direct-mode core: every commit is durable when it returns.
        let mut pipe = Pipeline::new(None, 1024, usize::MAX);
        let mut ex = Exec {
            registry: &reg,
            sem: sem.clone(),
            attempts: &mut attempts,
            probes: &mut probes,
            live: &mut live,
            waiting_probes: &mut waiting,
            per_tick: 3,
            mon: &mut mon,
            pipe: &mut pipe,
        };
        let running = |c: &EngineCore| {
            c.runs()
                .filter(|r| matches!(r.state, RunState::Running { .. }))
                .count()
        };
        tick(&mut core, &mut ex);
        assert_eq!(running(&core), 3, "one tick dispatches at most the cap");
        assert_eq!(sem.available_permits(), 7, "each dispatch holds a permit");
        let oldest = first.unwrap_or_else(|| panic!("submitted"));
        assert!(matches!(
            core.run(oldest).map(|r| &r.state),
            Some(RunState::Running { .. })
        ));
        tick(&mut core, &mut ex);
        assert_eq!(running(&core), 6);
    }

    /// Never answers; its probe reports the effect landed.
    struct HangAlways;
    #[async_trait::async_trait]
    impl StepHandler for HangAlways {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            std::future::pending().await
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            true
        }
    }

    /// A handler that never answers must not keep its permit past the
    /// attempt deadline: the sweep cancels the local call (the remote
    /// effect is left to the reconcile probe), so capacity returns and the
    /// next run is dispatched.
    #[tokio::test(flavor = "current_thread")]
    async fn stalled_handler_cannot_hold_capacity_past_its_deadline() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("hang", Arc::new(HangAlways));
        let cfg = ShellConfig {
            tick_ms: 5,
            max_in_flight: 1,
            limits: fast_limits(),
            ..Default::default()
        };
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let a = eng
            .submit(OperationKey("stall-a".into()), spec1("hang"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let b = eng
            .submit(OperationKey("stall-b".into()), spec1("hang"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, a).await, RunState::Done));
        assert!(matches!(until_done(&eng, b).await, RunState::Done));
    }

    /// A chained step (the checkpoint commit that moves a run to its next
    /// step) inherits the finished attempt's permit, so capacity can never
    /// be taken by another run between the checkpoint and the call.
    #[tokio::test(flavor = "current_thread")]
    async fn chained_steps_keep_their_permit_under_saturation() {
        use std::sync::atomic::Ordering::SeqCst;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let h = Gated::default();
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(h.clone()));
        let cfg = ShellConfig {
            tick_ms: 1,
            max_in_flight: 1,
            ..Default::default()
        };
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let mut runs = Vec::new();
        for i in 0..3 {
            runs.push(
                eng.submit(OperationKey(format!("chain-{i}")), spec("gated"))
                    .await
                    .unwrap_or_else(|e| panic!("{e}")),
            );
        }
        // Open the gate one call at a time and check the invariant between.
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(running_count(&eng).await <= 1, "Running without a permit");
            h.gate.add_permits(1);
        }
        for run in runs {
            assert!(matches!(until_done(&eng, run).await, RunState::Done));
        }
        assert_eq!(
            h.calls.load(SeqCst),
            6,
            "3 runs x 2 steps, each called once"
        );
    }

    /// Waits for the gate, then succeeds; its probe finds the effect.
    struct GateThenFound(Arc<Semaphore>);
    #[async_trait::async_trait]
    impl StepHandler for GateThenFound {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            match self.0.acquire().await {
                Ok(p) => {
                    p.forget();
                    Outcome::Checkpoint(b"ok".to_vec())
                }
                Err(_) => Outcome::Failed,
            }
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            true
        }
    }

    /// A handler outcome that arrives while the disk is below the reserve
    /// cannot be journaled. It used to vanish into `unwrap_or_default()`;
    /// now it is counted, readiness flips, and once space returns the run
    /// reconciles (the probe finds the effect) instead of repeating it.
    #[tokio::test(flavor = "current_thread")]
    async fn outcome_during_full_disk_is_counted_then_reconciled() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let free = Arc::new(AtomicU64::new(1 << 20));
        let gate = Arc::new(Semaphore::new(0));
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(GateThenFound(gate.clone())));
        let probe_free = free.clone();
        let cfg = ShellConfig {
            tick_ms: 5,
            // A deadline long enough that no scheduler delay lets the sweep
            // reconcile the run before the disk is "filled" (review S5):
            // the sweep is what recovers it once space returns.
            limits: Limits {
                max_step_deadline_ms: 1500,
                ..Limits::default()
            },
            journal: JournalOptions {
                reserve_bytes: 4096,
                space: durable_journal::SpaceProbe::new(move |_| Ok(probe_free.load(SeqCst))),
                ..JournalOptions::default()
            },
            ..Default::default()
        };
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        assert!(eng.readiness().ready);
        let run = eng
            .submit(OperationKey("full-disk".into()), spec1("gated"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        wait_state(&eng, run, "running").await;

        free.store(100, SeqCst); // the disk fills while the call is in flight
        gate.add_permits(1); // the handler answers — its checkpoint cannot be journaled
        tokio::time::sleep(Duration::from_millis(40)).await;
        let diag = eng.diag().await.unwrap_or_else(|e| panic!("{e}"));
        assert!(
            diag.errors.get("no_space").copied().unwrap_or(0) >= 1,
            "an outcome that could not be journaled is counted: {:?}",
            diag.errors
        );
        assert!(!diag.space_ok);
        assert_eq!(
            eng.readiness(),
            Readiness {
                ready: false,
                reason: Some("no_space")
            }
        );
        assert!(matches!(
            eng.inspect(run).await.unwrap_or(None).map(|r| r.state),
            Some(RunState::Running { .. })
        ));

        free.store(1 << 20, SeqCst);
        // Reconciled after the 1.5 s deadline; allow generously for load.
        let mut state = None;
        for _ in 0..1000 {
            state = eng.inspect(run).await.unwrap_or(None).map(|r| r.state);
            if state.as_ref().is_some_and(RunState::is_terminal) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(matches!(state, Some(RunState::Done)), "{state:?}");
        assert!(eng.readiness().ready);
    }

    // ---- group commit (D-014 rules, D-015) --------------------------------

    use durable_journal::faults::Faults;

    /// A start time no tick reaches: runs stay queued, so the journal holds
    /// exactly one Accepted record per submit.
    const FAR: Millis = u64::MAX / 4;

    fn piped(faults: &Arc<Faults>, group_commit: GroupCommit) -> ShellConfig {
        ShellConfig {
            tick_ms: 5,
            journal: JournalOptions {
                faults: Some(faults.clone()),
                ..JournalOptions::default()
            },
            group_commit,
            ..Default::default()
        }
    }

    fn echo_registry() -> Registry {
        let mut reg = Registry::default();
        reg.register("echo", Arc::new(Echo));
        reg
    }

    /// Submit `n` delayed runs concurrently; their handles.
    fn submit_many(
        eng: &Engine,
        prefix: &str,
        n: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<RunId, EngineError>>> {
        (0..n)
            .map(|i| {
                let e = eng.clone();
                let key = OperationKey(format!("{prefix}-{i}"));
                tokio::spawn(async move { e.submit_at(key, spec("echo"), FAR).await })
            })
            .collect()
    }

    /// Await `f` for at most 5 s: a rule violation fails, it never hangs.
    async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), f)
            .await
            .unwrap_or_else(|_| panic!("no answer within 5 s"))
    }

    async fn join_all(
        hs: Vec<tokio::task::JoinHandle<Result<RunId, EngineError>>>,
    ) -> Vec<Result<RunId, EngineError>> {
        // Bounded: a waiter that never hears back is a failed rule, not a hang.
        let mut out = Vec::new();
        for h in hs {
            let r = tokio::time::timeout(Duration::from_secs(5), h)
                .await
                .unwrap_or_else(|_| panic!("a waiter never got an answer"));
            out.push(r.unwrap_or_else(|e| panic!("{e}")));
        }
        out
    }

    /// Rule (a): while the fsync covering a record is outstanding, nothing
    /// that record decided leaves the process — not the submit's ack, not
    /// a read that would show it, not the handler call it dispatches.
    #[tokio::test(flavor = "current_thread")]
    async fn nothing_escapes_before_the_covering_fsync() {
        use std::sync::atomic::Ordering::SeqCst;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let h = Gated::default();
        h.gate.add_permits(64); // answers at once when called
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(h.clone()));
        let eng = spawn(d.path(), reg, piped(&faults, GroupCommit::default()))
            .unwrap_or_else(|e| panic!("{e}"));

        faults.hold_syncs();
        let e = eng.clone();
        let submit =
            tokio::spawn(
                async move { e.submit(OperationKey("held".into()), spec1("gated")).await },
            );
        tokio::time::sleep(Duration::from_millis(30)).await; // its record is in the writer
        let e = eng.clone();
        let read = tokio::spawn(async move { e.inspect(RunId(1)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!submit.is_finished(), "no ack before the covering fsync");
        assert!(
            !read.is_finished(),
            "no read of a record that is not durable"
        );
        assert_eq!(
            h.calls.load(SeqCst),
            0,
            "no handler call before the covering fsync"
        );

        faults.release_syncs();
        let run = bounded(submit)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(run, RunId(1));
        let seen = bounded(read)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(seen.is_some(), "the held read answers once durable");
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        assert_eq!(h.calls.load(SeqCst), 1);
    }

    /// Answers Lost once its gate opens; counts its probes (which find the
    /// effect).
    #[derive(Clone)]
    struct LostAfterGate {
        gate: Arc<Semaphore>,
        probes: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StepHandler for LostAfterGate {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            match self.gate.acquire().await {
                Ok(p) => {
                    p.forget();
                    Outcome::Lost
                }
                Err(_) => Outcome::Failed,
            }
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            self.probes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }
    }

    /// Rule (a) for probes: a reconcile probe is a handler call, so it may
    /// not start until the record that made the run `Reconciling`
    /// (`ReplyLost`) is durable. Review: making probes start immediately
    /// passed every test.
    #[tokio::test(flavor = "current_thread")]
    async fn no_probe_before_the_reply_lost_record_is_durable() {
        use std::sync::atomic::Ordering::SeqCst;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let h = LostAfterGate {
            gate: Arc::new(Semaphore::new(0)),
            probes: Arc::default(),
        };
        let mut reg = Registry::default();
        reg.register("lost", Arc::new(h.clone()));
        let eng = spawn(d.path(), reg, piped(&faults, GroupCommit::default()))
            .unwrap_or_else(|e| panic!("{e}"));
        let run = bounded(eng.submit(OperationKey("probe-hold".into()), spec1("lost")))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        wait_state(&eng, run, "running").await; // dispatched and durable
        faults.hold_syncs();
        h.gate.add_permits(1); // the handler answers Lost -> ReplyLost queued
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            h.probes.load(SeqCst),
            0,
            "a probe started before ReplyLost was durable"
        );
        faults.release_syncs();
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        assert!(h.probes.load(SeqCst) >= 1);
    }

    /// Rule (b): commits that queue up behind an fsync share the next one,
    /// and the journal still holds contiguous sequence numbers in commit
    /// order (replay refuses any gap).
    #[tokio::test(flavor = "current_thread")]
    async fn queued_commits_share_one_fsync_with_contiguous_sequence() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let first = submit_many(&eng, "first", 1);
        tokio::time::sleep(Duration::from_millis(30)).await; // batch 1 is in its fsync
        let rest = submit_many(&eng, "queued", 50);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rest.iter().all(|h| !h.is_finished()),
            "none acked before durable"
        );
        faults.release_syncs();
        let mut ids: Vec<u64> = join_all(first)
            .await
            .into_iter()
            .chain(join_all(rest).await)
            .map(|r| r.unwrap_or_else(|e| panic!("{e}")).0)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (1..=51).collect::<Vec<u64>>());
        let syncs = faults.sync_calls();
        assert!(
            syncs <= 3,
            "51 commits took {syncs} fsyncs: they must share them"
        );
        let diag = bounded(eng.diag()).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(diag.largest_batch >= 50, "batch sizes: {diag:?}");

        drop(eng);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let records = durable_journal::replay_dir(&durable_journal::gen_dir_path(d.path(), 1))
            .unwrap_or_else(|e| panic!("contiguous history replays: {e}"));
        let seqs: Vec<u64> = records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, (1..=51).collect::<Vec<u64>>());
    }

    /// Rule (c): a failed group fsync fails every waiter in the batch and
    /// every later one, is never retried, and the restart replays a clean
    /// journal without any of them.
    #[tokio::test(flavor = "current_thread")]
    async fn failed_group_fsync_fails_every_waiter_and_is_never_retried() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let first = submit_many(&eng, "doomed", 1);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let rest = submit_many(&eng, "behind", 10);
        tokio::time::sleep(Duration::from_millis(100)).await;
        faults.fail_next_sync(); // the fsync now blocked fails when released
        faults.release_syncs();
        for r in join_all(first)
            .await
            .into_iter()
            .chain(join_all(rest).await)
        {
            assert!(
                matches!(&r, Err(EngineError::CommitFailed(e)) if matches!(**e, JournalError::SyncFailed(_))),
                "every waiter fails with the commit failure: {r:?}"
            );
        }
        assert_eq!(faults.sync_calls(), 1, "the failed fsync is never retried");
        tokio::time::timeout(Duration::from_secs(2), eng.closed())
            .await
            .unwrap_or_else(|_| panic!("the engine stops: memory is ahead of disk"));
        let core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("replay: {e}"));
        assert_eq!(
            core.runs().count(),
            0,
            "nothing unacknowledged survives as a run"
        );
    }

    /// Rule (c) for a failed write: rolled back, the batch fails, and the
    /// engine stops (memory already reflects the records).
    #[tokio::test(flavor = "current_thread")]
    async fn failed_group_write_fails_the_batch_and_stops() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.fail_next_write_after(5);
        let r = bounded(eng.submit_at(OperationKey("torn".into()), spec("echo"), FAR)).await;
        assert!(
            matches!(&r, Err(EngineError::CommitFailed(e)) if matches!(**e, JournalError::WriteFailed { rolled_back: true, .. })),
            "{r:?}"
        );
        tokio::time::timeout(Duration::from_secs(2), eng.closed())
            .await
            .unwrap_or_else(|_| panic!("the engine stops after a failed write"));
        let core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("replay: {e}"));
        assert_eq!(core.runs().count(), 0);
    }

    /// Rule (d): batches never exceed `max_batch_records`.
    #[tokio::test(flavor = "current_thread")]
    async fn group_commit_batches_are_bounded() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let gc = GroupCommit {
            max_batch_records: 4,
            ..GroupCommit::default()
        };
        let eng =
            spawn(d.path(), echo_registry(), piped(&faults, gc)).unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let first = submit_many(&eng, "b0", 1);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let rest = submit_many(&eng, "b", 20);
        tokio::time::sleep(Duration::from_millis(100)).await;
        faults.release_syncs();
        for r in join_all(first)
            .await
            .into_iter()
            .chain(join_all(rest).await)
        {
            r.unwrap_or_else(|e| panic!("{e}"));
        }
        let diag = bounded(eng.diag()).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(diag.largest_batch <= 4, "{diag:?}");
        assert!(
            diag.journal_batches >= 6,
            "21 records in batches of <= 4: {diag:?}"
        );
    }

    /// Retention on a schedule: compaction runs by itself, and the
    /// superseded generation is gone from disk afterwards.
    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_compaction_runs_and_prunes_old_generations() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let cfg = ShellConfig {
            tick_ms: 5,
            compact_every: Some(Duration::from_millis(60)),
            ..Default::default()
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        let run = eng
            .submit(OperationKey("retained".into()), spec("echo"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        let mut diag = bounded(eng.diag()).await.unwrap_or_else(|e| panic!("{e}"));
        for _ in 0..100 {
            if diag.generation >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            diag = bounded(eng.diag()).await.unwrap_or_else(|e| panic!("{e}"));
        }
        assert!(
            diag.generation >= 2,
            "no scheduled compaction ran: {diag:?}"
        );
        assert_eq!(diag.stale_generations, 0, "{diag:?}");
        // The fresh run (not 7 days old) survived the compaction.
        assert!(bounded(eng.inspect(run)).await.unwrap_or(None).is_some());
        // Stop the schedule before looking at the directory: a 60 ms
        // schedule can be mid-compaction (gen-(N+1) created, gen-N not yet
        // pruned). The writer finishes what it holds before it releases
        // the lock, so once the journal reopens the directory is settled.
        drop(eng);
        let core = reopen(d.path()).await;
        let gens: Vec<String> = std::fs::read_dir(d.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.starts_with("gen-"))
            .collect();
        assert_eq!(gens.len(), 1, "one generation on disk: {gens:?}");
        assert!(core.run(run).is_some());
    }

    /// Compaction is expensive (it snapshots the whole index) and its
    /// error path decides durability, so it never runs unasked by default.
    #[test]
    fn scheduled_compaction_is_off_by_default() {
        assert!(ShellConfig::default().compact_every.is_none());
    }

    /// Reopen once the stopped engine's writer has released the lock.
    async fn reopen(dir: &Path) -> EngineCore {
        for _ in 0..300 {
            match EngineCore::open(dir) {
                Ok(c) => return c,
                Err(EngineError::Journal(JournalError::SecondWriter)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("restart must replay: {e}"),
            }
        }
        panic!("the stopped engine never released the journal");
    }

    /// Submit `n` delayed runs one by one; the ids that were acknowledged.
    async fn submit_acked(eng: &Engine, prefix: &str, n: usize) -> Vec<RunId> {
        let mut acked = Vec::new();
        for i in 0..n {
            let r =
                bounded(eng.submit_at(OperationKey(format!("{prefix}-{i}")), spec("echo"), FAR))
                    .await;
            if let Ok(id) = r {
                acked.push(id);
            }
        }
        acked
    }

    /// Review finding (high): an error at or after the manifest swap used
    /// to leave the writer appending to the generation the manifest no
    /// longer names — every job acknowledged afterwards was lost on
    /// restart. Whatever compaction step fails on a *running* engine, no
    /// acknowledged job may be lost: before the swap the engine carries on,
    /// from the swap onward it stops and the restart replays.
    #[tokio::test(flavor = "current_thread")]
    async fn compaction_error_at_any_step_loses_no_acknowledged_job() {
        for step in 1..=3u64 {
            let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let faults = Arc::new(Faults::default());
            let eng = spawn(
                d.path(),
                echo_registry(),
                piped(&faults, GroupCommit::default()),
            )
            .unwrap_or_else(|e| panic!("{e}"));
            let mut acked = submit_acked(&eng, "pre", 5).await;
            assert_eq!(acked.len(), 5);
            faults.crash_compaction_at(step);
            assert!(
                bounded(eng.compact()).await.is_err(),
                "step {step}: injected error"
            );
            let after = submit_acked(&eng, "post", 10).await;
            if step >= 3 {
                assert!(
                    after.is_empty(),
                    "step {step}: nothing may be acked once the manifest moved"
                );
                bounded(eng.closed()).await;
            } else {
                assert_eq!(
                    after.len(),
                    10,
                    "step {step}: before the swap the engine carries on"
                );
            }
            acked.extend(after);
            drop(eng);
            let core = reopen(d.path()).await;
            for id in &acked {
                assert!(
                    core.run(*id).is_some(),
                    "step {step}: acknowledged {id:?} lost on restart"
                );
            }
        }
    }

    /// The same with a real OS error: a journal root the process can write
    /// into but not open for reading makes the post-rename directory fsync
    /// fail (EACCES), as the reviewer reproduced on the release binary.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn real_io_error_after_manifest_swap_stops_the_engine() {
        use std::os::unix::fs::PermissionsExt;
        struct Restore<'a>(&'a Path);
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o700));
            }
        }
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let eng = spawn(
            d.path(),
            echo_registry(),
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let mut acked = submit_acked(&eng, "pre", 3).await;
        let guard = Restore(d.path());
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o300))
            .unwrap_or_else(|e| panic!("{e}"));
        if std::fs::File::open(d.path()).is_ok() {
            eprintln!("skipped: this user ignores directory permissions");
            return;
        }
        assert!(bounded(eng.compact()).await.is_err());
        let after = submit_acked(&eng, "post", 30).await;
        drop(guard);
        assert!(
            after.is_empty(),
            "{} submits acked after the failed swap",
            after.len()
        );
        acked.extend(after);
        bounded(eng.closed()).await;
        drop(eng);
        let core = reopen(d.path()).await;
        for id in &acked {
            assert!(
                core.run(*id).is_some(),
                "acknowledged {id:?} lost on restart"
            );
        }
    }

    /// Compaction cost probe (review finding 3): the contract's 1 KiB
    /// inputs at `PROBE_RUNS` retained runs. Ignored by default; run it in
    /// release mode under `/usr/bin/time -l` for peak RSS:
    /// `PROBE_RUNS=100000 PROBE_INPUT=1024 cargo test --release -p durable-engine -- --ignored compaction_cost_probe --nocapture`
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "measurement probe: run explicitly, in release mode"]
    async fn compaction_cost_probe() {
        let env = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let (runs, input) = (env("PROBE_RUNS", 100_000), env("PROBE_INPUT", 1024));
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let cfg = ShellConfig {
            cmd_capacity: 4096,
            max_outbox_records: 1 << 20,
            // The probe holds every run queued on purpose.
            limits: Limits {
                max_live_runs: usize::MAX,
                max_index_bytes: u64::MAX,
                ..Limits::default()
            },
            ..Default::default()
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        let payload: Vec<u8> = (0..input).map(|i| (i % 251) as u8).collect();
        let job = move || JobSpec {
            version: 1,
            input: payload.clone(),
            steps: vec![StepSpec {
                name: "a".into(),
                handler: "echo".into(),
            }],
        };
        let t = std::time::Instant::now();
        let mut next = 0;
        while next < runs {
            let mut hs = Vec::new();
            for _ in 0..512.min(runs - next) {
                let (e, k, s) = (eng.clone(), OperationKey(format!("probe-{next}")), job());
                hs.push(tokio::spawn(async move { e.submit_at(k, s, FAR).await }));
                next += 1;
            }
            for h in hs {
                h.await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .unwrap_or_else(|e| panic!("{e}"));
            }
        }
        let fill = t.elapsed();
        let t = std::time::Instant::now();
        eng.submit_at(OperationKey("idle".into()), job(), FAR)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let idle = t.elapsed();
        let e2 = eng.clone();
        let t0 = std::time::Instant::now();
        let compaction = tokio::spawn(async move { (e2.compact().await, t0.elapsed()) });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let t = std::time::Instant::now();
        eng.submit_at(OperationKey("during".into()), job(), FAR)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let during = t.elapsed();
        let (r, compact_took) = compaction.await.unwrap_or_else(|e| panic!("{e}"));
        let generation = r.unwrap_or_else(|e| panic!("{e}"));
        let gdir = durable_journal::gen_dir_path(d.path(), generation);
        let snapshot_bytes: u64 = std::fs::read_dir(&gdir)
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot"))
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum();
        drop(eng);
        let mut reopen_took = Duration::ZERO;
        for _ in 0..3000 {
            let t = std::time::Instant::now();
            match EngineCore::open(d.path()) {
                Ok(c) => {
                    reopen_took = t.elapsed();
                    assert_eq!(c.runs().count(), runs + 2);
                    break;
                }
                Err(EngineError::Journal(JournalError::SecondWriter)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("{e}"),
            }
        }
        println!(
            "PROBE runs={runs} input={input} fill_s={:.2} idle_submit_ms={:.1} compact_s={:.2} \
             submit_during_compaction_ms={:.1} snapshot_mib={:.1} reopen_s={:.2}",
            fill.as_secs_f64(),
            idle.as_secs_f64() * 1e3,
            compact_took.as_secs_f64(),
            during.as_secs_f64() * 1e3,
            snapshot_bytes as f64 / (1024.0 * 1024.0),
            reopen_took.as_secs_f64(),
        );
    }

    /// A compaction whose snapshot would exceed the byte budget is refused
    /// with a stable error before anything is written, and the engine
    /// carries on.
    #[tokio::test(flavor = "current_thread")]
    async fn compaction_over_the_snapshot_budget_is_refused() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let cfg = ShellConfig {
            tick_ms: 5,
            max_snapshot_bytes: 1024,
            ..Default::default()
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(submit_acked(&eng, "budget", 20).await.len(), 20);
        let r = bounded(eng.compact()).await;
        assert!(
            matches!(r, Err(EngineError::SnapshotOverBudget { budget: 1024, .. })),
            "{r:?}"
        );
        assert!(
            !durable_journal::gen_dir_path(d.path(), 2).exists(),
            "nothing written"
        );
        assert_eq!(
            submit_acked(&eng, "after", 1).await.len(),
            1,
            "the engine carries on"
        );
    }

    /// Submit delayed runs with `input` bytes each, concurrently.
    fn submit_sized(
        eng: &Engine,
        prefix: &str,
        n: usize,
        input: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<RunId, EngineError>>> {
        (0..n)
            .map(|i| {
                let e = eng.clone();
                let key = OperationKey(format!("{prefix}-{i}"));
                let mut s = spec("echo");
                s.input = vec![b'x'; input];
                tokio::spawn(async move { e.submit_at(key, s, FAR).await })
            })
            .collect()
    }

    /// Review finding 4: batches were bounded only in records, so
    /// `max_batch_bytes` bounded nothing (40 records of 120 KB went in one
    /// write). With a 16 KiB bound and ~3 KB records, 41 commits need at
    /// least 8 writes + fsyncs.
    #[tokio::test(flavor = "current_thread")]
    async fn group_commit_batches_are_bounded_in_bytes() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let gc = GroupCommit {
            max_batch_bytes: 16 * 1024,
            ..GroupCommit::default()
        };
        let eng =
            spawn(d.path(), echo_registry(), piped(&faults, gc)).unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let first = submit_sized(&eng, "b0", 1, 1024);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let rest = submit_sized(&eng, "b", 40, 1024);
        tokio::time::sleep(Duration::from_millis(100)).await;
        faults.release_syncs();
        for r in join_all(first)
            .await
            .into_iter()
            .chain(join_all(rest).await)
        {
            r.unwrap_or_else(|e| panic!("{e}"));
        }
        let syncs = faults.sync_calls();
        assert!(
            syncs >= 8,
            "41 records of ~3 KB in {syncs} fsyncs under a 16 KiB bound"
        );
    }

    /// The actor's outbox is bounded in bytes too: once it holds
    /// `max_outbox_bytes` it stops taking commands, the bounded command
    /// channel fills, and further submits are refused busy instead of
    /// buffered without limit while the writer is slow.
    #[tokio::test(flavor = "current_thread")]
    async fn outbox_is_bounded_in_bytes() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let cfg = ShellConfig {
            cmd_capacity: 2,
            max_outbox_bytes: 32 * 1024,
            ..piped(&faults, GroupCommit::default())
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let first = submit_sized(&eng, "o-first", 1, 1024);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let mut handles = Vec::new();
        for i in 0..100 {
            handles.extend(submit_sized(&eng, &format!("o{i}"), 1, 1024));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (done, waiting): (Vec<_>, Vec<_>) = handles.into_iter().partition(|h| h.is_finished());
        let busy = join_all(done)
            .await
            .into_iter()
            .filter(|r| {
                matches!(
                    r,
                    Err(EngineError::Refused(
                        durable_domain::Refusal::ReserveExhausted
                    ))
                )
            })
            .count();
        assert!(
            busy >= 80,
            "only {busy} of 100 refused: the outbox kept buffering"
        );
        faults.release_syncs();
        for r in join_all(first)
            .await
            .into_iter()
            .chain(join_all(waiting).await)
        {
            r.unwrap_or_else(|e| panic!("held submits complete: {e}"));
        }
    }

    /// Review finding 5: open reads shared the writers' bounded command
    /// queue, so a burst of unauthenticated `GET /jobs` could crowd an
    /// authenticated submit out (refused busy). Reads have their own bound.
    #[tokio::test(flavor = "current_thread")]
    async fn a_read_burst_cannot_crowd_out_a_submit() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let cfg = ShellConfig {
            tick_ms: 5,
            cmd_capacity: 4,
            ..Default::default()
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        let reads: Vec<_> = (0..200)
            .map(|_| {
                let e = eng.clone();
                tokio::spawn(async move { e.list(None, MAX_PAGE).await.map(|p| p.runs.len()) })
            })
            .collect();
        let e = eng.clone();
        let submit = tokio::spawn(async move {
            e.submit_at(OperationKey("writer".into()), spec("echo"), FAR)
                .await
        });
        let r = bounded(submit).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(r.is_ok(), "the submit was crowded out by reads: {r:?}");
        for h in reads {
            let _ = bounded(h).await; // reads may be refused busy; never the submit
        }
    }

    /// Restart-memory probe, part 1 (review S1): fill `PROBE_DIR` with
    /// `PROBE_RUNS` queued runs of `PROBE_INPUT` bytes, compacting at the
    /// end if `PROBE_COMPACT=1`. Part 2 (`restart_probe_open`) runs in a
    /// separate process so `/usr/bin/time -l` sees only the restart.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "measurement probe: run explicitly, in release mode"]
    async fn restart_probe_fill() {
        let var = |k: &str| std::env::var(k).ok();
        let dir =
            std::path::PathBuf::from(var("PROBE_DIR").unwrap_or_else(|| panic!("set PROBE_DIR")));
        let runs: usize = var("PROBE_RUNS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);
        let input: usize = var("PROBE_INPUT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        let compact = var("PROBE_COMPACT").as_deref() == Some("1");
        let limits = Limits {
            max_live_runs: usize::MAX,
            max_index_bytes: u64::MAX,
            ..Limits::default()
        };
        let cfg = ShellConfig {
            cmd_capacity: 4096,
            max_outbox_records: 1 << 20,
            limits,
            ..Default::default()
        };
        let eng = spawn(&dir, echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        let payload: Vec<u8> = (0..input).map(|i| (i * 7 % 251) as u8).collect();
        let mut next = 0;
        while next < runs {
            let mut hs = Vec::new();
            for _ in 0..512.min(runs - next) {
                let e = eng.clone();
                let mut s = spec1("echo");
                s.input = payload.clone();
                let k = OperationKey(format!("restart-{next}"));
                hs.push(tokio::spawn(async move { e.submit_at(k, s, FAR).await }));
                next += 1;
            }
            for h in hs {
                h.await
                    .unwrap_or_else(|e| panic!("{e}"))
                    .unwrap_or_else(|e| panic!("{e}"));
            }
        }
        if var("PROBE_FINISH").as_deref() == Some("1") {
            // Finish every run (cancel), leaving only dedup records.
            let mut id = 1;
            while id <= runs as u64 {
                let mut hs = Vec::new();
                for _ in 0..512.min(runs as u64 + 1 - id) {
                    let e = eng.clone();
                    let run = RunId(id);
                    hs.push(tokio::spawn(async move { e.cancel(run).await }));
                    id += 1;
                }
                for h in hs {
                    h.await
                        .unwrap_or_else(|e| panic!("{e}"))
                        .unwrap_or_else(|e| panic!("{e}"));
                }
            }
        }
        if compact {
            eng.compact().await.unwrap_or_else(|e| panic!("{e}"));
        }
        let diag = eng.diag().await.unwrap_or_else(|e| panic!("{e}"));
        println!(
            "FILLED runs={runs} input={input} compacted={compact} index_bytes={} active={}",
            diag.index_bytes, diag.active_runs
        );
    }

    /// Restart-memory probe, part 2: open `PROBE_DIR` (replay) and report.
    #[test]
    #[ignore = "measurement probe: run explicitly, in release mode"]
    fn restart_probe_open() {
        let dir = std::env::var("PROBE_DIR").unwrap_or_else(|_| panic!("set PROBE_DIR"));
        let t = std::time::Instant::now();
        let core = EngineCore::open(Path::new(&dir)).unwrap_or_else(|e| panic!("{e}"));
        println!(
            "OPENED runs={} open_s={:.2}",
            core.runs().count(),
            t.elapsed().as_secs_f64()
        );
    }

    /// A failed fsync must stop the engine — never retry it, never keep
    /// serving on a writer whose memory may be ahead of disk — and the
    /// restart must replay a clean journal holding only durable records.
    #[tokio::test(flavor = "current_thread")]
    async fn fsync_failure_stops_the_engine_and_restart_replays() {
        use durable_journal::{faults::Faults, JournalError};
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let mut reg = Registry::default();
        reg.register("echo", Arc::new(Echo));
        let cfg = ShellConfig {
            tick_ms: 5,
            journal: JournalOptions {
                faults: Some(faults.clone()),
                ..JournalOptions::default()
            },
            ..Default::default()
        };
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let a = eng
            .submit(OperationKey("durable-a".into()), spec("echo"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, a).await, RunState::Done));

        faults.fail_next_sync();
        let b = eng
            .submit(OperationKey("unsynced-b".into()), spec("echo"))
            .await;
        assert!(
            matches!(&b, Err(EngineError::CommitFailed(e)) if matches!(**e, JournalError::SyncFailed(_))),
            "an unsynced submit is never acknowledged: {b:?}"
        );
        tokio::time::timeout(Duration::from_secs(2), eng.closed())
            .await
            .unwrap_or_else(|_| panic!("the engine must stop after a failed fsync"));
        assert!(matches!(eng.inspect(a).await, Err(EngineError::Stopped)));

        // Restart: replay succeeds and holds exactly the durable history.
        let core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("replay: {e}"));
        assert!(matches!(
            core.run(a).unwrap_or_else(|| panic!("run a")).state,
            RunState::Done
        ));
        assert_eq!(core.runs().count(), 1, "the unsynced run must not appear");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_channel_backpressures() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("none", Arc::new(Echo));
        let eng = spawn(
            d.path(),
            reg,
            ShellConfig {
                cmd_capacity: 1,
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        // Flood a channel of capacity 1: submits either land or refuse —
        // they never pile up unbounded, and nothing panics or hangs.
        for i in 0..50 {
            match eng
                .submit(OperationKey(format!("flood-{i}")), spec("none"))
                .await
            {
                Ok(_) | Err(EngineError::Refused(_)) => {}
                Err(e) => panic!("{e}"),
            }
        }
    }
}
