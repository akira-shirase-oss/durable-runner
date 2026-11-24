//! The Tokio shell: one actor task owns `EngineCore`; handler invocations
//! run in bounded spawned tasks and answer through the same loop. Ordering
//! guarantee: `commit` fsyncs the journal before any handler is invoked, so
//! a crash can only lose what the journal already recorded.
//!
//! Idempotency: every call for a step — attempt 1 through the last retry —
//! carries the same key `{operation_key}/{step}`, so a repeating remote call
//! can be de-duplicated on the far side (contract §04, at-least-once).

use crate::observe::{CallGuard, CallResult, IndexCounts, Observer, Scope};
use crate::writer::{self, GroupCommit, Writer, WriterMsg};
use crate::{EngineCore, EngineError, RunSummary, Submitted};
use durable_domain::{
    Effect, JobSpec, Limits, Millis, OperationKey, Run, RunId, RunState, StepIndex, StepSpec,
};
use durable_journal::{JournalError, JournalOptions};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
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

/// Which call a handler is answering: the run, the step and the attempt
/// (for a probe, the attempt being reconciled). The HTTP handler forwards it
/// as headers (D-050), so a handler, or a proxy in front of one, can tie a
/// call to the run that `GET /jobs/{id}` describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Call {
    pub run: RunId,
    pub step: StepIndex,
    pub attempt: u32,
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
    /// What the engine calls: [`StepHandler::execute`], told which call this
    /// is. Default: the call is ignored.
    async fn execute_call(
        &self,
        _call: Call,
        step: &StepSpec,
        input: &[u8],
        idem_key: &str,
    ) -> Outcome {
        self.execute(step, input, idem_key).await
    }
    /// What the engine calls to reconcile: [`StepHandler::probe`], told which
    /// call it reconciles. Default: the call is ignored.
    async fn probe_call(&self, _call: Call, step: &StepSpec, idem_key: &str) -> bool {
        self.probe(step, idem_key).await
    }
}

/// Handlers a single workspace may register (D-021 bound): enough for a
/// tenant's real surface, small enough that a registration flood cannot
/// grow the dispatch map without limit.
pub const MAX_HANDLERS_PER_WS: usize = 256;

/// Named handlers, populated by configuration, scoped per workspace
/// (S2-4): a step naming a handler not registered *in the submitting
/// workspace* fails its attempt — it never silently runs, and it never
/// borrows another workspace's registration.
#[derive(Default, Clone)]
pub struct Registry {
    /// Workspace → its own name → handler: a lookup, a count and a
    /// listing cost O(that workspace's registrations), never a walk of
    /// every tenant's (a `/metrics` read lists one workspace's names).
    handlers: HashMap<u64, HashMap<String, Arc<dyn StepHandler>>>,
}

impl Registry {
    /// Compat registration: `--handler` flags land in workspace 0.
    pub fn register(&mut self, name: &str, h: Arc<dyn StepHandler>) {
        self.handlers
            .entry(0)
            .or_default()
            .insert(name.to_string(), h);
    }
    fn get(&self, ws: u64, name: &str) -> Option<Arc<dyn StepHandler>> {
        self.handlers.get(&ws)?.get(name).cloned()
    }
    pub fn contains(&self, ws: u64, name: &str) -> bool {
        self.handlers.get(&ws).is_some_and(|m| m.contains_key(name))
    }
    /// Insert or replace one registration. Refuses past the per-workspace
    /// bound — the account store also bounds, this is the engine's own.
    fn put(&mut self, ws: u64, name: &str, h: Arc<dyn StepHandler>) -> Result<(), EngineError> {
        let m = self.handlers.entry(ws).or_default();
        if !m.contains_key(name) && m.len() >= MAX_HANDLERS_PER_WS {
            return Err(EngineError::RegistryFull);
        }
        m.insert(name.to_string(), h);
        Ok(())
    }
    fn remove(&mut self, ws: u64, name: &str) {
        if let Some(m) = self.handlers.get_mut(&ws) {
            m.remove(name);
            if m.is_empty() {
                self.handlers.remove(&ws);
            }
        }
    }
    /// Replace a workspace's whole set — how the store reconciler syncs.
    /// Refuses past the bound; an oversized store answer is refused whole.
    fn set_ws(
        &mut self,
        ws: u64,
        handlers: Vec<(String, Arc<dyn StepHandler>)>,
    ) -> Result<(), EngineError> {
        if handlers.len() > MAX_HANDLERS_PER_WS {
            return Err(EngineError::RegistryFull);
        }
        if handlers.is_empty() {
            self.handlers.remove(&ws);
            return Ok(());
        }
        self.handlers.insert(ws, handlers.into_iter().collect());
        Ok(())
    }
    /// The names registered in one workspace, sorted (what its `/metrics`
    /// lists; never another workspace's).
    pub fn names(&self, ws: u64) -> Vec<String> {
        let mut n: Vec<String> = self
            .handlers
            .get(&ws)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        n.sort();
        n
    }
}

fn wall_ms() -> Millis {
    crate::observe::wall_ms()
}

/// The business idempotency key for a step: constant across its attempts.
pub fn idem_key(key: &OperationKey, step: StepIndex) -> String {
    format!("{}/{}", key.0, step.0)
}

enum Cmd {
    Submit {
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
        reply: oneshot::Sender<Result<Submitted, EngineError>>,
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
    /// A handler registration written through the API (S2-4): the engine
    /// learns it on the command channel — never by the actor reading a
    /// database.
    PutHandler {
        ws: u64,
        name: String,
        handler: Arc<dyn StepHandler>,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    DeleteHandler {
        ws: u64,
        name: String,
    },
    /// The store reconciler's whole-set sync for one workspace.
    SetWorkspaceHandlers {
        ws: u64,
        handlers: Vec<(String, Arc<dyn StepHandler>)>,
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
        ws: u64,
        order: Order,
        limit: usize,
        reply: oneshot::Sender<RunPage>,
    },
    Diag {
        reply: oneshot::Sender<crate::Diagnostics>,
    },
    /// One workspace's diagnostics — the tenant-facing read.
    DiagWs {
        ws: u64,
        reply: oneshot::Sender<crate::WorkspaceDiagnostics>,
    },
    /// A run's list fields, never its spec or input.
    Summary {
        run: RunId,
        reply: oneshot::Sender<Option<RunSummary>>,
    },
    /// The operation keys of up to `MAX_PAGE` runs of workspace `ws`
    /// (`None`: not in the index, or another workspace's run).
    Keys {
        ws: u64,
        runs: Vec<RunId>,
        reply: oneshot::Sender<Vec<Option<OperationKey>>>,
    },
    /// State counts and index sizes, O(1).
    Counts { reply: oneshot::Sender<IndexCounts> },
    /// One scope's counts and registered handler names (`/metrics`).
    MetricsIndex {
        scope: Scope,
        reply: oneshot::Sender<MetricsIndex>,
    },
}

/// Handle to the running engine. Cheap to clone; the channel capacities
/// are the back-pressure bounds — a full channel refuses honestly.
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Cmd>,
    read_tx: mpsc::Sender<Read>,
    health: Arc<Health>,
    discarded_tail: Option<durable_journal::TornTail>,
    /// Set once the actor has returned and the writer thread has released
    /// the journal, with why it stopped.
    stopped: watch::Receiver<Option<StopReason>>,
    /// Set to ask the actor to drain and stop (`shutdown`).
    drain: Arc<watch::Sender<bool>>,
    reply_timeout: Duration,
    admin_timeout: Duration,
    /// Metrics, timelines and the activity feed (D-058).
    obs: Arc<Observer>,
}

/// Why the engine stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// `shutdown`: everything decided was made durable and answered, then
    /// the writer released the journal.
    Drained,
    /// A journal write or fsync failed (or the writer died): memory was
    /// ahead of disk, so held work failed and a restart replays.
    Failed,
    /// Every handle was dropped: the actor stopped without draining, like
    /// a crash (nothing undecided was acknowledged).
    Dropped,
}

/// Wait for the actor's answer, at most `limit`: a stuck journal must not
/// hang the caller. A timed-out request may still complete.
async fn answer<T>(rx: oneshot::Receiver<T>, limit: Duration) -> Result<T, EngineError> {
    match tokio::time::timeout(limit, rx).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_)) => Err(EngineError::Stopped),
        Err(_) => Err(EngineError::Timeout),
    }
}

/// State the actor publishes for readiness, readable without a round trip
/// through the (possibly full) command channel.
#[derive(Debug, Default)]
struct Health {
    space_low: AtomicBool,
}

/// The most runs one list call returns, whatever the caller asks for.
pub const MAX_PAGE: usize = 1000;

/// How long an in-grace dead-handler entry waits on `dead_retry`
/// between presence re-checks (S3 re-review N7): a 30 s grace costs ~30
/// examinations per entry instead of one per tick, and the heap's
/// `min(first + grace, now + this)` still parks on the exact deadline.
const DEAD_RECHECK_MS: u64 = 1_000;

/// While a batch rides the writer pipeline, outbound accumulates until
/// at least this many records are queued — fsyncs then carry real groups
/// and the writer's own gather merges what is already in its channel.
/// The moment the pipeline is idle, whatever has accumulated ships. The
/// strict one-batch gate serialized every fsync through the actor's wake
/// latency (~tens of ms under a saturated offer), capping durable
/// throughput near one tiny batch per cycle.
const MIN_SHIP_RECORDS: usize = 256;

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
        self.submit_scoped(0, key, spec).await
    }

    /// Submit into workspace 0 (delayed if `not_before` is set), saying
    /// whether the key named an existing run.
    pub async fn submit_with(
        &self,
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
    ) -> Result<Submitted, EngineError> {
        self.submit_in(0, key, spec, not_before).await
    }

    /// `submit_with` into workspace `ws` (dedup per `(ws, key)`).
    pub async fn submit_in(
        &self,
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
    ) -> Result<Submitted, EngineError> {
        self.submit_inner(ws, key, spec, not_before).await
    }

    /// Submit with a delayed start: the run queues until `not_before`.
    pub async fn submit_at(
        &self,
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
    ) -> Result<RunId, EngineError> {
        self.submit_at_scoped(0, key, spec, not_before).await
    }

    /// Submit into a workspace: dedup is scoped to `(ws, key)`, so the
    /// same key can run independently in different workspaces.
    pub async fn submit_scoped(
        &self,
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
    ) -> Result<RunId, EngineError> {
        self.submit_inner(ws, key, spec, None).await.map(|s| s.run)
    }

    /// `submit_at` scoped to a workspace.
    pub async fn submit_at_scoped(
        &self,
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
    ) -> Result<RunId, EngineError> {
        self.submit_inner(ws, key, spec, Some(not_before))
            .await
            .map(|s| s.run)
    }

    async fn submit_inner(
        &self,
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
        not_before: Option<Millis>,
    ) -> Result<Submitted, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Submit {
            ws,
            key,
            spec,
            not_before,
            reply,
        })?;
        answer(rx, self.reply_timeout).await?
    }

    pub async fn cancel(&self, run: RunId) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Cancel { run, reply })?;
        answer(rx, self.reply_timeout).await?
    }

    /// Compact the journal: snapshot live state into a new generation,
    /// atomically swap the manifest. Returns the new generation.
    pub async fn compact(&self) -> Result<u32, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Compact { reply })?;
        answer(rx, self.admin_timeout).await?
    }

    /// Consistent backup of the journal to `dest` (manifest copied last).
    pub async fn backup_to(&self, dest: std::path::PathBuf) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Backup { dest, reply })?;
        answer(rx, self.admin_timeout).await?
    }

    /// Register or replace a handler in one workspace's registry (S2-4).
    /// The caller (an API route) has already persisted it to the account
    /// store; this updates the dispatch view on the command channel.
    pub async fn put_handler(
        &self,
        ws: u64,
        name: &str,
        handler: Arc<dyn StepHandler>,
    ) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::PutHandler {
            ws,
            name: name.to_string(),
            handler,
            reply,
        })?;
        answer(rx, self.reply_timeout).await?
    }

    /// Remove one registration. Fire-and-forget: there is nothing to
    /// refuse — a run already in flight keeps its resolved handler.
    pub fn delete_handler(&self, ws: u64, name: &str) -> Result<(), EngineError> {
        self.send(Cmd::DeleteHandler {
            ws,
            name: name.to_string(),
        })
    }

    /// Replace a workspace's whole handler set — the store reconciler's
    /// sync point. Refuses an oversized set whole.
    pub async fn set_workspace_handlers(
        &self,
        ws: u64,
        handlers: Vec<(String, Arc<dyn StepHandler>)>,
    ) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::SetWorkspaceHandlers {
            ws,
            handlers,
            reply,
        })?;
        answer(rx, self.admin_timeout).await?
    }

    /// One page of `ws`'s runs oldest-first (scoped: the caller's
    /// workspace, never the global index).
    pub async fn list(
        &self,
        ws: u64,
        after: Option<RunId>,
        limit: usize,
    ) -> Result<RunPage, EngineError> {
        self.list_in(ws, Order::Oldest { after }, limit).await
    }

    /// One page in either order, at most `min(limit, MAX_PAGE)` runs.
    pub async fn list_in(
        &self,
        ws: u64,
        order: Order,
        limit: usize,
    ) -> Result<RunPage, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::List {
            ws,
            order,
            limit: limit.clamp(1, MAX_PAGE),
            reply,
        })?;
        answer(rx, self.reply_timeout).await
    }

    /// Bounded diagnostics — counts by state kind, never rows.
    pub async fn diag(&self) -> Result<crate::Diagnostics, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Diag { reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// One workspace's diagnostics — the tenant-facing subset, never
    /// another tenant's totals (S1 review).
    pub async fn diag_ws(&self, ws: u64) -> Result<crate::WorkspaceDiagnostics, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::DiagWs { ws, reply })?;
        answer(rx, self.reply_timeout).await
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

    /// Resolves once the engine has fully stopped: the actor has returned
    /// and the writer thread has been joined, so the journal's lock is
    /// free and another engine may open it. The future holds no handle, so
    /// awaiting it after dropping every `Engine` waits for that shutdown
    /// instead of keeping the engine alive. (If the actor panicked, it
    /// resolves when the actor is gone; the writer then releases the
    /// journal on its own thread.)
    pub fn on_stop(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut stopped = self.stopped.clone();
        async move {
            let _ = stopped.wait_for(Option::is_some).await;
        }
    }

    /// Drain and stop (SIGTERM): refuse new commands and anything still
    /// queued (`Stopped`, nothing of it was decided), start no new handler
    /// calls, hand every record already decided to the writer and wait for
    /// its fsync, answer every caller it covers, then join the writer. In-
    /// flight handler calls are abandoned: their runs stay journaled
    /// running or reconciling and reconcile after a restart (cancelling
    /// never undoes a remote effect). `Ok` once drained; `Timeout` if that
    /// took longer than `limit`; `Stopped` if the engine stopped any other
    /// way (a failed commit: restart replays).
    pub async fn shutdown(&self, limit: Duration) -> Result<(), EngineError> {
        self.drain.send_replace(true);
        let mut stopped = self.stopped.clone();
        let reason = match tokio::time::timeout(limit, stopped.wait_for(Option::is_some)).await {
            Ok(Ok(r)) => *r,
            Ok(Err(_)) => None, // the actor is gone without a report (a panic)
            Err(_) => return Err(EngineError::Timeout),
        };
        match reason {
            Some(StopReason::Drained) => Ok(()),
            _ => Err(EngineError::Stopped),
        }
    }

    /// The torn tail startup discarded (an incomplete final frame, never
    /// acknowledged), so the operator's log can say so.
    pub fn discarded_tail(&self) -> Option<durable_journal::TornTail> {
        self.discarded_tail
    }

    pub async fn inspect(&self, run: RunId) -> Result<Option<Run>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Inspect { run, reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// A run's key, state and step count — `None` exactly when `inspect`
    /// is — without copying its spec or input.
    pub async fn summary(&self, run: RunId) -> Result<Option<RunSummary>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Summary { run, reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// The operation keys of `runs` (at most `MAX_PAGE`; more are cut),
    /// in order; `None` for a run no longer in the index or not in `ws`
    /// (a workspace never learns another's keys).
    pub async fn keys(
        &self,
        ws: u64,
        mut runs: Vec<RunId>,
    ) -> Result<Vec<Option<OperationKey>>, EngineError> {
        runs.truncate(MAX_PAGE);
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Keys { ws, runs, reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// One scope's state counts, index sizes and registered handler names
    /// ([`Scope::All`]: every workspace's counts, workspace 0's names), in
    /// O(that workspace's handlers) on the actor.
    pub async fn metrics_index(&self, scope: Scope) -> Result<MetricsIndex, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::MetricsIndex { scope, reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// Runs by state kind and the index sizes (O(1) on the actor).
    pub async fn index_counts(&self) -> Result<IndexCounts, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send_read(Read::Counts { reply })?;
        answer(rx, self.reply_timeout).await
    }

    /// The observer: metrics, run timelines and the activity feed (D-058).
    pub fn observer(&self) -> &Arc<Observer> {
        &self.obs
    }
}

/// What `/metrics` needs from the actor for one scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsIndex {
    pub counts: IndexCounts,
    /// The scope's registered handler names, sorted.
    pub handlers: Vec<String>,
}

/// Runtime knobs, all bounded.
#[derive(Debug, Clone)]
pub struct ShellConfig {
    /// Maximum handler calls (attempts and reconcile probes) in flight.
    /// A dispatch is journaled only while holding one of these permits.
    /// Under a submit burst the permits saturate instantly and the rest
    /// defer to the tick scheduler — measured the drain bound (S2-7):
    /// 32 permits held ~640 runs/s against a fast fixture while 256 gave
    /// ~5k/s; the journal/fsync path itself sustains ~33k/s (Echo bench).
    pub max_in_flight: usize,
    /// The share of `max_in_flight` one workspace may hold at once (S2
    /// review #6): without it a single tenant's slow handler parks every
    /// permit and starves the rest — the measured default of 256 held
    /// entirely by one tenant was the cross-tenant DoS. 50 keeps one
    /// tenant under a fifth of the pool while it is full, so four
    /// saturated tenants leave a fifth able to dispatch; a ws below
    /// its cap can still use idle capacity up to the global bound.
    /// Compat (ws-0 only) deployments wanting more raise the flag.
    pub max_in_flight_per_ws: usize,
    /// Upper bound on dispatches (and on deadline expiries) per tick, so a
    /// backlog of due runs cannot monopolise the actor. This is the drain
    /// ceiling once permits stop throttling: 64 per 50 ms tick caps a
    /// deferred backlog at ~1,280 dispatches/s (measured ~1,356/s), which
    /// is what bounded the 50k-run workspaces load to ~1.3k/s (S2-7).
    pub max_dispatch_per_tick: usize,
    /// Scheduler/deadline-sweep cadence.
    pub tick_ms: u64,
    /// How long a run whose handler left the registry stays in the due
    /// rotation before it parks (S2 re-review finding 5): within grace a
    /// registration blip costs the run nothing; past grace it leaves the
    /// rotation until the handler returns, instead of being re-examined
    /// every tick forever.
    pub dead_handler_grace_ms: u64,
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
    /// How long a caller waits for the engine's answer to a submit, a
    /// cancel or a read before it gets `Timeout` (a stuck fsync must not
    /// hang every request).
    pub reply_timeout: Duration,
    /// The same for compaction and backup, which write a whole snapshot or
    /// copy the journal.
    pub admin_timeout: Duration,
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            max_in_flight_per_ws: 50,
            max_dispatch_per_tick: 512,
            tick_ms: 50,
            dead_handler_grace_ms: 30_000,
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
            reply_timeout: Duration::from_secs(10),
            admin_timeout: Duration::from_secs(600),
        }
    }
}

/// Open the journal, replay to rebuild, hand the journal to the
/// group-commit writer thread, then spawn the actor task. The returned
/// handle is the only public interface — there is no path to the journal
/// except through it.
pub fn spawn(dir: &Path, registry: Registry, mut cfg: ShellConfig) -> Result<Engine, EngineError> {
    cfg.group_commit = cfg.group_commit.clamped();
    cfg.limits.validate().map_err(EngineError::Config)?;
    // Replay feeds the observer's timelines and feed; its metrics begin now.
    let obs = Arc::new(Observer::new(wall_ms()));
    let mut core = EngineCore::open_observed(dir, cfg.journal.clone(), Some(obs.clone()))?;
    core.limits = cfg.limits;
    let counts = core.index_counts();
    obs.set_index_gauges(counts.live_runs, counts.active_runs);
    let discarded_tail = core.discarded_tail();
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
    let (stopped_tx, stopped) = watch::channel(None);
    let (drain, drain_rx) = watch::channel(false);
    let (reply_timeout, admin_timeout) = (cfg.reply_timeout, cfg.admin_timeout);
    let actor_obs = obs.clone();
    tokio::spawn(async move {
        // Every return path of the actor joins the writer first.
        let reason = actor(
            core,
            registry,
            cfg,
            (rx, read_rx, drain_rx),
            mon,
            writer,
            actor_obs,
        )
        .await;
        stopped_tx.send_replace(Some(reason));
    });
    tokio::spawn(sample_gauges(obs.clone(), stopped.clone()));
    Ok(Engine {
        tx,
        read_tx,
        health,
        discarded_tail,
        stopped,
        drain: Arc::new(drain),
        reply_timeout,
        admin_timeout,
        obs,
    })
}

/// The gauge sampler (D-058): once a second, the gauges go into the
/// buckets holding now — the last sample in a bucket is its value at the
/// bucket's end. Stops with the engine.
async fn sample_gauges(obs: Arc<Observer>, mut stopped: watch::Receiver<Option<StopReason>>) {
    let mut every = tokio::time::interval(Duration::from_millis(crate::observe::SAMPLE_EVERY_MS));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = every.tick() => obs.sample(wall_ms()),
            _ = stopped.wait_for(Option::is_some) => return,
        }
    }
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

/// An attempt's result comes back with the permit AND the workspace
/// slot it ran under, so a chained step (the checkpoint that moves the
/// run to its next step) inherits both instead of racing other runs for
/// capacity — a freed slot could otherwise be handed to a backlog run
/// before the finished call's effects ran (S1).
type AttemptJoin = (RunId, StepIndex, Outcome, OwnedSemaphorePermit, WsSlot);
type ProbeJoin = (RunId, StepIndex, bool, OwnedSemaphorePermit, WsSlot);

/// Sends a held answer: `None` once durable, `Some(err)` if the commit failed.
type HeldReply = Box<dyn FnOnce(Option<&Arc<JournalError>>) + Send>;

/// Something a record decided that may not leave the process until the
/// fsync covering that record has returned (D-014 rule a).
enum Held {
    /// An answer to a caller. `Some(err)`: the commit failed — the caller
    /// gets `CommitFailed` (or, for a read, the engine stops).
    Reply(HeldReply),
    /// A dispatch whose record is not yet durable. The workspace slot is
    /// charged from `dispatch`, through this wait, to the task's end —
    /// the share a run owns exists from the journal write, not the spawn
    /// (S2 re-review N3).
    Fire {
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
    },
    Probe {
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
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

    /// Hand queued messages to the writer the moment it is idle, and
    /// while it is busy once at least `MIN_SHIP_RECORDS` have accumulated:
    /// several batches ride the fsync pipeline so the writer never waits
    /// on the actor's wake latency between fsyncs (that is the group
    /// commit). False if the writer is gone.
    fn flush(&mut self) -> bool {
        if self.outbound.is_empty()
            || (self.sent_upto > self.durable && self.outbound_records < MIN_SHIP_RECORDS)
        {
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
    shape: durable_journal::Shape,
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
    // The shape the writer persists inside the manifest swap — after
    // pruning, the records that carried it are gone.
    let shape = core.planned_compaction_shape(&plan.view);
    pipe.collect(core);
    let (done_tx, done) = oneshot::channel();
    pipe.outbound.push_back(WriterMsg::Compact {
        view: plan.view,
        bytes: plan.bytes,
        shape,
        reply: done_tx,
    });
    Ok(PendingCompaction {
        done,
        expired: plan.expired,
        shape,
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

/// Resolves once a drain has been asked for; never if it cannot be (every
/// handle, and with them the sender, is gone: the channels' `None` ends
/// the actor then).
async fn drain_requested(rx: &mut watch::Receiver<bool>) {
    if rx.wait_for(|d| *d).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// A command still queued when the drain began: nothing of it was
/// decided, so it is refused, never half done.
fn refuse(cmd: Cmd) {
    match cmd {
        Cmd::Submit { reply, .. } => {
            let _ = reply.send(Err(EngineError::Stopped));
        }
        Cmd::Cancel { reply, .. } => {
            let _ = reply.send(Err(EngineError::Stopped));
        }
        Cmd::Compact { reply } => {
            let _ = reply.send(Err(EngineError::Stopped));
        }
        Cmd::Backup { reply, .. } => {
            let _ = reply.send(Err(EngineError::Stopped));
        }
        Cmd::PutHandler { reply, .. } | Cmd::SetWorkspaceHandlers { reply, .. } => {
            let _ = reply.send(Err(EngineError::Stopped));
        }
        Cmd::DeleteHandler { .. } => {}
    }
}

/// The drain (`Engine::shutdown`): take nothing new, make everything
/// already decided durable, answer what it covers, start no handler call.
/// True when everything decided reached disk.
async fn drain(
    core: &mut EngineCore,
    pipe: &mut Pipeline,
    status: &mut watch::Receiver<writer::WriterStatus>,
    (rx, read_rx): (&mut mpsc::Receiver<Cmd>, &mut mpsc::Receiver<Read>),
    pending_compaction: &mut Option<PendingCompaction>,
    mon: &mut Monitor,
) -> bool {
    rx.close();
    read_rx.close();
    while let Ok(cmd) = rx.try_recv() {
        refuse(cmd);
    }
    while read_rx.try_recv().is_ok() {} // their callers see `Stopped`
    loop {
        if core.is_poisoned() {
            let err = status
                .borrow()
                .failed
                .clone()
                .unwrap_or_else(|| Arc::new(JournalError::Poisoned));
            pipe.fail_held(&err);
            if let Some(reply) = pending_compaction.take().and_then(|c| c.reply) {
                let _ = reply.send(Err(EngineError::CommitFailed(err)));
            }
            return false;
        }
        pipe.collect(core);
        if !pipe.flush() {
            pipe.fail_held(&Arc::new(JournalError::Poisoned));
            return false;
        }
        // Held handler calls and probes are dropped (their permits return):
        // their records are durable, and restart reconciles them.
        while pipe
            .held
            .front()
            .is_some_and(|(needs, _)| *needs <= pipe.durable)
        {
            if let Some((_, Held::Reply(answer))) = pipe.held.pop_front() {
                answer(None);
            }
        }
        let flushed = pipe.outbound.is_empty() && pipe.durable >= core.last_index();
        if flushed && pending_compaction.is_none() {
            return true;
        }
        tokio::select! {
            changed = status.changed() => match changed {
                Ok(()) => {
                    let st = status.borrow_and_update().clone();
                    core.observe(&st);
                    pipe.durable = st.durable;
                    core.flush_observed(pipe.durable);
                }
                Err(_) => {
                    pipe.fail_held(&Arc::new(JournalError::Poisoned));
                    return false;
                }
            },
            r = compaction_done(pending_compaction) => {
                if let Some(c) = pending_compaction.take() {
                    let answer = match r {
                        Ok(Ok(generation)) => {
                            core.finish_compaction(&c.expired);
                            core.note_shape_persisted(c.shape);
                            Ok(generation)
                        }
                        Ok(Err(e)) => Err(EngineError::Journal(e)),
                        Err(_) => Err(EngineError::Stopped),
                    };
                    match (c.reply, answer) {
                        (Some(reply), answer) => {
                            let _ = reply.send(answer);
                        }
                        (None, Err(e)) => mon.handled(&e),
                        (None, Ok(_)) => {}
                    }
                }
            }
        }
    }
}

async fn actor(
    mut core: EngineCore,
    mut registry: Registry,
    cfg: ShellConfig,
    (mut rx, mut read_rx, mut drain_rx): (
        mpsc::Receiver<Cmd>,
        mpsc::Receiver<Read>,
        watch::Receiver<bool>,
    ),
    mut mon: Monitor,
    writer: Writer,
    obs: Arc<Observer>,
) -> StopReason {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.tick_ms));
    let mut attempts: JoinSet<AttemptJoin> = JoinSet::new();
    let mut probes: JoinSet<ProbeJoin> = JoinSet::new();
    let mut live: HashMap<(RunId, StepIndex), AbortHandle> = HashMap::new();
    let mut waiting_probes = ProbeQueue::default();
    let inflight = WsInflight::default();
    // The scheduler's per-workspace due queues: `pop_due` drains the
    // time-ordered index into these once, `due_order` rotates the
    // workspaces so a tick round-robins fairly, and a workspace at its
    // in-flight share is skipped whole — its queue is never examined
    // (S2 re-review finding 5). A run whose handler left the registry
    // parks out of the queues after `dead_handler_grace_ms`; the handler
    // re-registering re-queues it.
    let mut due_by_ws: BTreeMap<u64, VecDeque<(RunId, StepIndex, u32)>> = BTreeMap::new();
    let mut due_order: VecDeque<u64> = VecDeque::new();
    let mut parked: HashMap<u64, Vec<Parked>> = HashMap::new();
    let mut dead_since: HashMap<RunId, Millis> = HashMap::new();
    // In-grace dead-handler entries sit HERE between re-checks, not in
    // the dispatch queues: `(next_check, run, step, attempt)` pops back
    // into `due_by_ws` when due, so a dead entry costs one examination
    // per re-check interval instead of one per tick (S3 re-review N7).
    let mut dead_retry: BinaryHeap<Reverse<(u64, u64, u32, u32)>> = BinaryHeap::new();
    let mut spawned: HashMap<tokio::task::Id, (RunId, StepIndex)> = HashMap::new();
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
                inflight: &inflight,
                spawned: &mut spawned,
                ws_cap: cfg.max_in_flight_per_ws as u32,
                waiting_probes: &mut waiting_probes,
                due_by_ws: &mut due_by_ws,
                due_order: &mut due_order,
                parked: &mut parked,
                dead_since: &mut dead_since,
                dead_retry: &mut dead_retry,
                dead_grace_ms: cfg.dead_handler_grace_ms,
                per_tick: cfg.max_dispatch_per_tick,
                #[cfg(test)]
                examined: 0,
                mon: &mut mon,
                pipe: &mut pipe,
                obs: &obs,
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

    // DURABLE_ACTOR_TRACE: per-branch latency for wedge hunts — the env
    // is read once and only a slow arm or slow tick phase is logged.
    let trace = actor_trace();
    let mut arm_ms: std::collections::BTreeMap<&'static str, (u64, u128)> = Default::default();
    let mut ws_gauges_at: Millis = 0;

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
            return StopReason::Failed;
        }
        let (backlog_records, backlog_bytes) = pipe.backlog(&core);
        let accepting =
            backlog_records < cfg.max_outbox_records && backlog_bytes < cfg.max_outbox_bytes;
        let t_iter = trace.then(std::time::Instant::now);
        let mut arm = "idle";
        tokio::select! {
            cmd = rx.recv(), if accepting => { arm = "cmd"; match cmd {
                Some(Cmd::Submit { ws, key, spec, not_before, reply }) => {
                    let now = wall_ms();
                    // Admission is where the allowlist bites: a step naming a
                    // handler outside *this workspace's* registry is a spec
                    // error, refused before anything is journaled.
                    let missing = spec
                        .steps
                        .iter()
                        .find(|s| !registry.contains(ws, &s.handler))
                        .map(|s| s.handler.clone());
                    let r = match missing {
                        Some(name) => Err(EngineError::UnregisteredHandler(name)),
                        None => core.submit_checked(ws, key, spec, not_before.unwrap_or(now), now),
                    };
                    if let Err(e) = &r {
                        mon.returned(e);
                    }
                    pipe.answer(core.last_index(), reply, r);
                }
                Some(Cmd::Cancel { run, reply }) => {
                    // The run may hold scheduler memory: a parked entry
                    // (handler dead past grace) or a dead_since marker
                    // (in grace). Drop both now — a cancelled run's entry
                    // must not wait on its dead handler's return (S2
                    // re-review N2).
                    let ws = core.run(run).map(|r| r.ws);
                    let r = core.cancel(run, wall_ms()).map(|_| ());
                    if let Some(w) = ws {
                        exec!().untrack(w, run);
                    }
                    if let Err(e) = &r {
                        mon.returned(e);
                    }
                    pipe.answer(core.last_index(), reply, r);
                }
                Some(Cmd::Compact { reply }) => {
                    if pending_compaction.is_some() {
                        // Not "busy": retrying cannot help until it finishes.
                        let _ = reply.send(Err(EngineError::CompactionRunning));
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
                Some(Cmd::PutHandler { ws, name, handler, reply }) => {
                    pipe.answer(core.last_index(), reply, registry.put(ws, &name, handler));
                    exec!().unpark(&core, ws); // a handler's return revives parked runs
                }
                Some(Cmd::DeleteHandler { ws, name }) => {
                    registry.remove(ws, &name);
                }
                Some(Cmd::SetWorkspaceHandlers { ws, handlers, reply }) => {
                    pipe.answer(core.last_index(), reply, registry.set_ws(ws, handlers));
                    exec!().unpark(&core, ws);
                }
                None => {
                    pipe.shutdown().await;
                    return StopReason::Dropped;
                }
            }},
            read = read_rx.recv() => { arm = "read"; match read {
                Some(Read::Inspect { run, reply }) => {
                    pipe.answer_read(core.last_index(), reply, core.run(run).cloned());
                }
                Some(Read::List { ws, order, limit, reply }) => {
                    let (runs, next) = match order {
                        Order::Oldest { after } => core.page(ws, after, limit),
                        Order::Newest { before } => core.page_newest(ws, before, limit),
                    };
                    pipe.answer_read(core.last_index(), reply, RunPage { runs, next });
                }
                Some(Read::Diag { reply }) => {
                    let mut d = core.diagnostics();
                    d.errors = mon.errors.clone();
                    pipe.answer_read(core.last_index(), reply, d);
                }
                Some(Read::DiagWs { ws, reply }) => {
                    pipe.answer_read(core.last_index(), reply, core.diagnostics_for(ws));
                }
                Some(Read::Summary { run, reply }) => {
                    let s = core.run(run).map(RunSummary::of);
                    pipe.answer_read(core.last_index(), reply, s);
                }
                Some(Read::Keys { ws, runs, reply }) => {
                    let keys = runs
                        .iter()
                        .map(|r| core.run(*r).filter(|r| r.ws == ws).map(|r| r.key.clone()))
                        .collect();
                    pipe.answer_read(core.last_index(), reply, keys);
                }
                Some(Read::Counts { reply }) => {
                    pipe.answer_read(core.last_index(), reply, core.index_counts());
                }
                Some(Read::MetricsIndex { scope, reply }) => {
                    let m = match scope {
                        Scope::All => MetricsIndex {
                            counts: core.index_counts(),
                            handlers: registry.names(0),
                        },
                        Scope::Ws(ws) => MetricsIndex {
                            counts: core.index_counts_for(ws),
                            handlers: registry.names(ws),
                        },
                    };
                    pipe.answer_read(core.last_index(), reply, m);
                }
                // Every handle is gone (the command channel closes with it).
                None => {
                    pipe.shutdown().await;
                    return StopReason::Dropped;
                }
            }},
            Some(res) = attempts.join_next() => { arm = "attempt"; match res {
                Ok((run, step, outcome, permit, slot)) => {
                    live.remove(&(run, step));
                    spawned.retain(|_, k| *k != (run, step));
                    let (out, fail) = match outcome {
                        Outcome::Checkpoint(o) => (Some(o), None),
                        Outcome::Failed => (None, Some("failed".to_string())),
                        Outcome::Lost => (None, None),
                    };
                    let mut ex = exec!();
                    // Not journaled (e.g. NoSpace): counted; the run stays
                    // Running and its deadline sweep reconciles the effect.
                    let effects = ex.mon.effects(core.step_finished(run, step, out, fail, wall_ms()));
                    run_effects(&mut core, &mut ex, effects, run, Some(permit), Some(slot));
                }
                // A panicked or aborted attempt: the WsSlot already
                // returned the workspace's share on drop. The call's
                // outcome is unknown → journaled as Lost so
                // at-least-once retry reconciles it.
                Err(e) => {
                    eprintln!("durable: handler task died: {e}");
                    if let Some((run, step)) = spawned.remove(&e.id()) {
                        live.remove(&(run, step));
                        let mut ex = exec!();
                        let effects = ex.mon.effects(core.step_finished(
                            run,
                            step,
                            None,
                            None,
                            wall_ms(),
                        ));
                        run_effects(&mut core, &mut ex, effects, run, None, None);
                    }
                }
            }},
            Some(res) = probes.join_next() => { arm = "probe"; match res {
                Ok((run, step, found, permit, slot)) => {
                    spawned.retain(|_, k| *k != (run, step));
                    let mut ex = exec!();
                    let effects = ex.mon.effects(core.reconciled(run, step, found, wall_ms()));
                    run_effects(&mut core, &mut ex, effects, run, Some(permit), Some(slot));
                }
                // Same release rule for a dead probe; its verdict is
                // unknown, so it reconciles as not-found — at-least-once
                // retries the call under the same idempotency key.
                Err(e) => {
                    eprintln!("durable: probe task died: {e}");
                    if let Some((run, step)) = spawned.remove(&e.id()) {
                        let mut ex = exec!();
                        let effects =
                            ex.mon.effects(core.reconciled(run, step, false, wall_ms()));
                        run_effects(&mut core, &mut ex, effects, run, None, None);
                    }
                }
            }},
            r = compaction_done(&mut pending_compaction) => { arm = "compact_done";
                if let Some(c) = pending_compaction.take() {
                    let answer = match r {
                        Ok(Ok(generation)) => {
                            // The new generation is durable: only now does
                            // expiry reach the live index (all or nothing).
                            core.finish_compaction(&c.expired);
                            core.note_shape_persisted(c.shape);
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
            changed = status.changed() => { arm = "status"; match changed {
                Ok(()) => {
                    let st = status.borrow_and_update().clone();
                    core.observe(&st);
                    pipe.durable = st.durable;
                    // The observer first: a read released below must find
                    // what the records it waited for contributed.
                    core.flush_observed(pipe.durable);
                    let mut ex = exec!();
                    release(&mut core, &mut ex);
                }
                Err(_) => writer_lost = true,
            }},
            _ = ticker.tick() => {
                arm = "tick";
                let mut ex = exec!();
                tick(&mut core, &mut ex);
            }
            () = drain_requested(&mut drain_rx) => {
                let drained = drain(
                    &mut core,
                    &mut pipe,
                    &mut status,
                    (&mut rx, &mut read_rx),
                    &mut pending_compaction,
                    &mut mon,
                )
                .await;
                // In-flight calls are abandoned (the JoinSets abort them on
                // drop); their runs reconcile after a restart.
                attempts.abort_all();
                probes.abort_all();
                pipe.shutdown().await;
                return if drained {
                    StopReason::Drained
                } else {
                    StopReason::Failed
                };
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
        if let Some(t) = t_iter {
            let us = t.elapsed().as_micros();
            let e = arm_ms.entry(arm).or_default();
            e.0 += 1;
            e.1 += us;
            // Only a slow arm is worth the log line — normal work is
            // silent even under the flag.
            if us > 20_000 {
                eprintln!(
                    "actor-trace arm={arm} us={us} held={} backlog={backlog_records}B{backlog_bytes} [{}]",
                    pipe.held.len(),
                    arm_ms
                        .iter()
                        .map(|(k, (n, t))| format!("{k}:{n}x{:.0}us", t / (*n).max(1) as u128))
                        .collect::<Vec<_>>()
                        .join(" ")
                );
            }
        }
        let counts = core.index_counts();
        obs.set_index_gauges(counts.live_runs, counts.active_runs);
        // Per-workspace gauges: O(tracked workspaces), so at most once per
        // sample period, not every iteration.
        let now = wall_ms();
        if now.saturating_sub(ws_gauges_at) >= crate::observe::SAMPLE_EVERY_MS {
            ws_gauges_at = now;
            obs.set_ws_index_gauges(|ws| {
                let c = core.index_counts_for(ws);
                (c.live_runs, c.active_runs)
            });
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
            Held::Fire {
                run,
                step,
                permit,
                slot,
            } => ex.start_attempt(core, run, step, permit, slot),
            Held::Probe {
                run,
                step,
                permit,
                slot,
            } => ex.start_probe(core, run, step, permit, slot),
        }
    }
}

/// A parked dead-handler wait: the queued dispatch tuple plus the
/// handler name whose return revives it.
type Parked = (RunId, StepIndex, u32, Arc<str>);

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
    /// Entries waiting right now — the tick's drain is bounded by this
    /// so a queue that only refills cannot spin the actor (S2 N1).
    fn len(&self) -> usize {
        self.order.len()
    }
}

/// In-flight handler work per workspace (attempts + probes), shared
/// with the tasks it counts: a `WsSlot` owns one slot and releases it
/// on drop — a completed call, a panicking handler, or an aborted task
/// all give it back. The join arms only retire bookkeeping; the count
/// itself can never be stranded by a dead task (S2 re-review).
#[derive(Clone, Default)]
struct WsInflight(Arc<std::sync::Mutex<HashMap<u64, u32>>>);

impl WsInflight {
    fn count(&self, ws: u64) -> u32 {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ws)
            .copied()
            .unwrap_or(0)
    }
}

/// One workspace's in-flight slot: taken under the counter's lock (the
/// check and the claim are one step) and returned on drop.
struct WsSlot {
    shared: WsInflight,
    ws: u64,
}

impl WsSlot {
    fn take(shared: &WsInflight, ws: u64) -> Self {
        *shared
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(ws)
            .or_default() += 1;
        Self {
            shared: shared.clone(),
            ws,
        }
    }
}

impl Drop for WsSlot {
    fn drop(&mut self) {
        if let Some(n) = self
            .shared
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.ws)
        {
            *n = n.saturating_sub(1);
        }
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
    /// The fairness counter for `max_in_flight_per_ws` — each spawned
    /// task carries a `WsSlot` off it.
    inflight: &'a WsInflight,
    /// Task id → (run, step) for every spawned call and probe: the
    /// join-ERROR arm gets no tuple, so this is how it learns which
    /// step's task died.
    spawned: &'a mut HashMap<tokio::task::Id, (RunId, StepIndex)>,
    ws_cap: u32,
    waiting_probes: &'a mut ProbeQueue,
    /// Per-workspace dispatch queues + rotation — see the declarations
    /// the `exec!` macro wires these to.
    due_by_ws: &'a mut BTreeMap<u64, VecDeque<(RunId, StepIndex, u32)>>,
    due_order: &'a mut VecDeque<u64>,
    parked: &'a mut HashMap<u64, Vec<Parked>>,
    dead_since: &'a mut HashMap<RunId, Millis>,
    /// `(next_check, run, step, attempt)` — in-grace dead entries
    /// between re-checks; the field types are raw ints so the heap
    /// orders without `Ord` on the domain ids.
    dead_retry: &'a mut BinaryHeap<Reverse<(u64, u64, u32, u32)>>,
    dead_grace_ms: u64,
    per_tick: usize,
    /// Queue entries this tick examined — the test counter proving the
    /// rotation skips a full workspace's queue without touching it.
    #[cfg(test)]
    examined: usize,
    mon: &'a mut Monitor,
    pipe: &'a mut Pipeline,
    obs: &'a Arc<Observer>,
}

impl Exec<'_> {
    /// Invoke a step's handler for the run's current attempt under
    /// `permit` — once the record that made it `Running` (committed only
    /// because this permit was held) is durable. Until then it is held,
    /// and its workspace slot rides with it (S2 N3).
    fn fire(
        &mut self,
        core: &EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
    ) {
        let needs = core.last_index();
        if needs <= self.pipe.durable {
            self.start_attempt(core, run, step, permit, slot);
        } else {
            self.pipe.held.push_back((
                needs,
                Held::Fire {
                    run,
                    step,
                    permit,
                    slot,
                },
            ));
        }
    }

    /// The call itself, for a run still `Running` this step (a cancel
    /// committed while the dispatch was held means no call: the permit
    /// and the workspace slot return).
    fn start_attempt(
        &mut self,
        core: &EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
    ) {
        let Some(r) = core.run(run) else { return };
        let attempt = match r.state {
            RunState::Running {
                step: s, attempt, ..
            } if s == step => attempt,
            _ => return,
        };
        let Some(sspec) = r.spec.steps.get(step.0 as usize).cloned() else {
            return;
        };
        let Some(handler) = self.registry.get(r.ws, &sspec.handler) else {
            return;
        };
        let idem = idem_key(&r.key, step);
        let input = r.spec.input.clone();
        let call = Call { run, step, attempt };
        // Counts the call in flight; records its latency and result, or —
        // dropped by an abort at its deadline — a lost reply.
        let guard = CallGuard::start(self.obs, r.ws, Some(&sspec.handler));
        let handle = self.attempts.spawn(async move {
            let outcome = handler.execute_call(call, &sspec, &input, &idem).await;
            guard.finish(match &outcome {
                Outcome::Checkpoint(_) => CallResult::Ok,
                Outcome::Failed => CallResult::Failed,
                Outcome::Lost => CallResult::Lost,
            });
            // The slot returns WITH the outcome so the run's next step
            // can inherit it like the permit: dropping it here freed the
            // share to a backlog run before the effect pass, and the
            // journaled-Running next step was never called (S1).
            (run, step, outcome, permit, slot)
        });
        self.spawned.insert(handle.id(), (run, step));
        self.live.insert((run, step), handle);
    }

    /// Fail a step's attempt (journaled; retry/terminal decided by domain).
    fn fail(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex, why: &str) {
        let effects =
            self.mon
                .effects(core.step_finished(run, step, None, Some(why.to_string()), wall_ms()));
        run_effects(core, self, effects, run, None, None);
    }

    /// Probe for a reconciling step; the verdict returns through the loop.
    /// A probe is a handler call, so it takes a permit and a workspace
    /// slot like any other; with either missing it waits in
    /// `waiting_probes` for the next tick.
    fn probe(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex) {
        // A probe is a handler call: the workspace fairness cap applies
        // exactly like a dispatch's.
        let Some(ws) = core.run(run).map(|r| r.ws) else {
            return;
        };
        if self.inflight.count(ws) >= self.ws_cap {
            self.waiting_probes.push((run, step));
            return;
        }
        match self.sem.clone().try_acquire_owned() {
            Ok(permit) => self.probe_with(core, run, step, permit, WsSlot::take(self.inflight, ws)),
            Err(_) => self.waiting_probes.push((run, step)),
        }
    }

    /// A probe is a handler call too: held until the record that made the
    /// run `Reconciling` is durable — its workspace slot rides along.
    fn probe_with(
        &mut self,
        core: &mut EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
    ) {
        let needs = core.last_index();
        if needs <= self.pipe.durable {
            self.start_probe(core, run, step, permit, slot);
        } else {
            self.pipe.held.push_back((
                needs,
                Held::Probe {
                    run,
                    step,
                    permit,
                    slot,
                },
            ));
        }
    }

    fn start_probe(
        &mut self,
        core: &mut EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
        slot: WsSlot,
    ) {
        let Some(r) = core.run(run) else { return };
        let attempt = match r.state {
            RunState::Reconciling { step: s, attempt } if s == step => attempt,
            _ => return, // the run moved on while the probe waited
        };
        let Some(sspec) = r.spec.steps.get(step.0 as usize).cloned() else {
            return;
        };
        let Some(handler) = self.registry.get(r.ws, &sspec.handler) else {
            // Cannot probe an unregistered handler — reconcile as not-found
            // rather than leave the run stuck.
            let effects = self
                .mon
                .effects(core.reconciled(run, step, false, wall_ms()));
            run_effects(core, self, effects, run, Some(permit), Some(slot));
            return;
        };
        let idem = idem_key(&r.key, step);
        let call = Call { run, step, attempt };
        let guard = CallGuard::start(self.obs, r.ws, None); // in flight only
        let handle = self.probes.spawn(async move {
            let found = handler.probe_call(call, &sspec, &idem).await;
            drop(guard);
            // Same rule as an attempt: the slot rides home with the
            // verdict so a DispatchStep the reconcile produces keeps
            // the share the probe was run under.
            (run, step, found, permit, slot)
        });
        self.spawned.insert(handle.id(), (run, step));
    }

    /// Drop every scheduler entry a run holds — an in-grace `dead_since`
    /// marker, its `dead_retry` tuple, and a parked entry (S2 re-review
    /// N2). Called when the run leaves waiting for a reason no queue
    /// pass will see: a cancel.
    fn untrack(&mut self, ws: u64, run: RunId) {
        self.dead_since.remove(&run);
        self.dead_retry.retain(|Reverse((_, r, ..))| *r != run.0);
        if let Some(v) = self.parked.get_mut(&ws) {
            v.retain(|p| p.0 != run);
            if v.is_empty() {
                self.parked.remove(&ws);
            }
        }
    }

    /// A registry change on `ws` may bring a parked run's handler back:
    /// re-queue every parked entry whose name resolves again, still
    /// longest-waiting-first at the queue's head. Entries whose run left
    /// waiting while parked (a cancel lands nowhere else) are pruned
    /// here — the map is the only other place they could die.
    fn unpark(&mut self, core: &EngineCore, ws: u64) {
        let Some(mut entries) = self.parked.remove(&ws) else {
            return;
        };
        let mut i = 0;
        while i < entries.len() {
            // A run that left waiting while parked (cancelled, expired)
            // drops out here — parked entries must not outlive the run's
            // interest in dispatch (S2 re-review N2).
            let run = entries[i].0;
            let waiting = core.run(run).is_some_and(|r| {
                matches!(
                    r.state,
                    RunState::Queued { .. } | RunState::RetryWaiting { .. }
                )
            });
            if !waiting {
                entries.swap_remove(i);
                continue;
            }
            if self.registry.contains(ws, &entries[i].3) {
                let (run, step, attempt, _) = entries.swap_remove(i);
                // Absent-or-empty queue ⇔ the workspace is not in the
                // rotation (the invariant `requeue_ws` maintains).
                if self.due_by_ws.get(&ws).is_none_or(|q| q.is_empty()) {
                    self.due_order.push_back(ws);
                }
                self.due_by_ws
                    .entry(ws)
                    .or_default()
                    .push_front((run, step, attempt));
            } else {
                i += 1;
            }
        }
        if !entries.is_empty() {
            self.parked.insert(ws, entries);
        }
    }

    /// `ws` re-enters the due rotation iff its queue still holds entries —
    /// the invariant `due_order holds ws ⇔ due_by_ws[ws] is non-empty`,
    /// which is what makes the at-share skip O(1) and keeps drained
    /// workspaces out of the rotation entirely.
    fn requeue_ws(&mut self, ws: u64) {
        if self.due_by_ws.get(&ws).is_some_and(|q| !q.is_empty()) {
            self.due_order.push_back(ws);
        } else {
            self.due_by_ws.remove(&ws);
        }
    }
}

/// The periodic pass: sweep expired attempts, start waiting probes, then
/// dispatch what's due — each step bounded by `max_dispatch_per_tick`.
/// A permit is taken **before** `StepDispatched` is journaled: with none
/// free the run stays queued (nothing happened, nothing is recorded).
fn tick(core: &mut EngineCore, exec: &mut Exec<'_>) {
    tick_at(core, exec, wall_ms());
}

/// DURABLE_ACTOR_TRACE, resolved once for the process — the actor loop
/// and tick_at share the same answer instead of re-reading the env per
/// iteration.
fn actor_trace() -> bool {
    static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRACE.get_or_init(|| std::env::var_os("DURABLE_ACTOR_TRACE").is_some())
}

/// The scheduler's pass at an explicit `now` — production calls it via
/// `tick`; tests drive the re-check/grace clocks deterministically.
fn tick_at(core: &mut EngineCore, exec: &mut Exec<'_>, now: Millis) {
    let trace = actor_trace();
    let mut ph = [0u128; 4];
    let mut tp = std::time::Instant::now();
    let mut mark = |i: usize| {
        if trace {
            ph[i] = tp.elapsed().as_micros();
            tp = std::time::Instant::now();
        }
    };
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
    // The batch is popped before it is processed: on an early exit the
    // remainder was already de-indexed, so every entry the sweep never
    // reached loses its deadline until the next restart's recount (R1:
    // a run that broke the sweep requeued itself but dropped the rest).
    let mut popped = core.pop_expired(now, exec.per_tick).into_iter().peekable();
    while let Some((run, step)) = popped.next() {
        if let Some(call) = exec.live.remove(&(run, step)) {
            call.abort(); // frees its permit; the remote effect is reconciled
        }
        match core.step_finished(run, step, None, None, now) {
            Ok(effects) => run_effects(core, exec, effects, run, None, None),
            Err(e) => {
                let stale = matches!(e, EngineError::Refused(_));
                exec.mon.handled(&e);
                if stale {
                    continue; // the run moved on without the popped entry
                }
                // A real commit error (NoSpace, a failed group commit):
                // the attempt still owes its lost-reply record — put the
                // deadline back and stop the sweep; every step_finished
                // behind it fails the same way (S2 re-review N1).
                // pop_expired already removed the whole batch from the
                // index — re-index every remaining entry, not just the
                // failure, or crash-surviving Running runs strand here
                // until a restart's recount (R1).
                core.requeue_deadline(run);
                for (rest, _step) in popped.by_ref() {
                    // ATTACK(deadline-drop): without it the popped batch strands until restart.
                    core.requeue_deadline(rest);
                }
                break;
            }
        }
    }
    // Probes that found no permit or workspace share when their run
    // started reconciling. Each entry gets ONE try per tick, bounded by
    // the queue's length at entry (S2 re-review N1): a queue full of
    // over-share workspaces used to spin this loop forever — submits,
    // dispatch and the shutdown drain all starved behind it.
    mark(0);
    let mut probes_left = exec.waiting_probes.len();
    while probes_left > 0 {
        probes_left -= 1;
        let Some((run, step)) = exec.waiting_probes.pop() else {
            break;
        };
        exec.probe(core, run, step);
    }
    mark(1);
    // Dispatch: permit and workspace slot first, then the journal commits
    // StepDispatched, then the handler is invoked — a crash between the
    // commit and the call can only lose a journaled fact, and no commit
    // is ever made for a call that capacity would not allow.
    //
    // Newly-due work leaves the time-ordered index for the per-workspace
    // queues once (S2 re-review finding 5): a tick's scan cost is the
    // newly-due count, never the retained set, and entries already queued
    // are re-examined only while their workspace can still take work.
    for (ws, run, step, attempt) in core.pop_due(now) {
        if !exec.due_by_ws.contains_key(&ws) {
            exec.due_order.push_back(ws);
        }
        exec.due_by_ws
            .entry(ws)
            .or_default()
            .push_back((run, step, attempt));
    }
    // In-grace dead entries whose re-check is due come back through the
    // same rotation — a handler that returned dispatches this visit, a
    // still-absent one goes back to the heap once (S3 re-review N7: they
    // used to re-examine on every tick of their grace window).
    while exec
        .dead_retry
        .peek()
        .is_some_and(|Reverse((due, ..))| *due <= now)
    {
        let Some(Reverse((_, run, step, attempt))) = exec.dead_retry.pop() else {
            break;
        };
        let Some(ws) = core.run(RunId(run)).map(|r| r.ws) else {
            exec.dead_since.remove(&RunId(run)); // the run is gone entirely
            continue;
        };
        if !exec.due_by_ws.contains_key(&ws) {
            exec.due_order.push_back(ws);
        }
        exec.due_by_ws
            .entry(ws)
            .or_default()
            .push_back((RunId(run), StepIndex(step), attempt));
    }
    mark(2);
    // The per-tick budget counts only dispatch that actually happens
    // (S2 re-review N2): an entry that cannot dispatch — its handler left
    // the registry, its workspace is at share — is skipped for free, so
    // a tenant whose queue heads the due order can never starve the runs
    // behind it. Workspaces rotate: each live queue loses at most one
    // head entry per round, and a queue at share is passed over whole.
    let mut budget = exec.per_tick;
    // Workspace visits since the last dispatch: when every workspace in
    // the rotation was visited once with nothing dispatchable — full,
    // dead-handler-only, stale — a further pass can only repeat that,
    // so the tick ends without draining the budget.
    let mut idle = 0usize;
    while budget > 0 && idle < exec.due_order.len() {
        let Some(ws) = exec.due_order.pop_front() else {
            break;
        };
        if exec.inflight.count(ws) >= exec.ws_cap {
            idle += 1;
            exec.due_order.push_back(ws); // full: skipped whole, queue untouched
            continue;
        }
        // This visit examines each queued entry at most once (`pass` is
        // the queue's length at entry): a dead-handler or stale head
        // cannot hide the live work behind it, and a skipped entry that
        // re-queues waits for the workspace's next turn.
        let mut dispatched = false;
        let mut starved = false;
        {
            let Some(queue) = exec.due_by_ws.get_mut(&ws) else {
                idle += 1;
                continue; // emptied between rotations — leaves the rotation
            };
            let mut pass = queue.len();
            while pass > 0 {
                pass -= 1;
                let Some((run, step, attempt)) = queue.pop_front() else {
                    break;
                };
                #[cfg(test)]
                {
                    exec.examined += 1;
                }
                let Some((name, waiting)) = core.run(run).and_then(|r| {
                    r.spec.steps.get(step.0 as usize).map(|s| {
                        (
                            s.handler.clone(),
                            matches!(
                                r.state,
                                RunState::Queued { .. } | RunState::RetryWaiting { .. }
                            ),
                        )
                    })
                }) else {
                    exec.dead_since.remove(&run); // the run is gone entirely
                    continue; // stale queue entry — spec/step the run lost
                };
                if !waiting {
                    exec.dead_since.remove(&run); // in-grace marker dies with the wait
                    continue; // cancelled or already dispatched — stale
                }
                if !exec.registry.contains(ws, &name) {
                    // The handler left the registry after this run was
                    // accepted: it stays queued — registry repair or a
                    // cancel resolves it. Within grace it waits out the
                    // re-check interval on the `dead_retry` heap — not
                    // in this queue, so a dead backlog is not re-scanned
                    // every tick; past grace it parks out of the
                    // rotation until the handler returns (findings 5/N7).
                    let first = *exec.dead_since.entry(run).or_insert(now);
                    if now.saturating_sub(first) >= exec.dead_grace_ms {
                        exec.dead_since.remove(&run);
                        exec.parked
                            .entry(ws)
                            .or_default()
                            .push((run, step, attempt, name.into()));
                    } else {
                        let next = first
                            .saturating_add(exec.dead_grace_ms)
                            .min(now.saturating_add(DEAD_RECHECK_MS));
                        exec.dead_retry
                            .push(Reverse((next, run.0, step.0, attempt)));
                    }
                    continue;
                }
                let Ok(permit) = exec.sem.clone().try_acquire_owned() else {
                    // At capacity: the entry stays next-due for a later tick.
                    queue.push_front((run, step, attempt));
                    starved = true;
                    break;
                };
                // The workspace share is charged AT the dispatch (S2
                // re-review N3): the slot rides the held-fire through the
                // fsync wait, so a run waiting undurable already owns its
                // place — charging at spawn let one workspace take the
                // whole pool in a single tick.
                let slot = WsSlot::take(exec.inflight, ws);
                if let Err(e) = core.dispatch(run, step, attempt, now) {
                    let stale = matches!(e, EngineError::Refused(_));
                    exec.mon.handled(&e);
                    if stale {
                        continue; // stale claim; permit + slot return
                    }
                    // A real commit error (NoSpace, a failed group
                    // commit): the entry is live work, not a stale claim
                    // — put it back and end the tick; every dispatch
                    // behind it fails the same way (S2 re-review N1:
                    // dropping it left runs queued-with-no-index-entry
                    // until a restart's recount).
                    queue.push_front((run, step, attempt));
                    starved = true;
                    break;
                }
                exec.fire(core, run, step, permit, slot);
                // The run left the wait — its in-grace marker dies here,
                // AFTER the dispatch took. Dropping it earlier let a
                // PUT/DELETE flap restart the grace clock every time the
                // handler was seen present but the run stayed queued
                // (starved permit, full workspace share) — N7.
                exec.dead_since.remove(&run);
                budget -= 1;
                dispatched = true;
                break; // one dispatch per visit — the rotation stays fair
            }
        }
        idle = if dispatched { 0 } else { idle + 1 };
        exec.requeue_ws(ws);
        if starved {
            break; // global capacity — every later workspace starves too
        }
    }
    mark(3);
    // Only a slow phase earns the log line: a tick whose parts all fit
    // inside the arm-level bound stays silent under the flag.
    if trace && (ph.iter().sum::<u128>() > 20_000 || ph.iter().any(|p| *p > 5_000)) {
        eprintln!(
            "tick-phase sweep={}us probes={}us pour={}us rotate={}us dueq={}",
            ph[0],
            ph[1],
            ph[2],
            ph[3],
            exec.due_by_ws.values().map(|q| q.len()).sum::<usize>()
        );
    }
}

/// Carry out the reducer's effects. `permit` and `slot` are the
/// capacity the event arrived with (a finished attempt or probe); a
/// `DispatchStep` effect — the run already journaled Running on its
/// next step — inherits both. The slot MUST ride with the event: the
/// share count still includes it, so checking the cap against it would
/// see the run's own predecessor as a rival (S1: the next step stayed
/// journaled Running and was never called).
fn run_effects(
    core: &mut EngineCore,
    exec: &mut Exec<'_>,
    effects: Vec<Effect>,
    run: RunId,
    mut permit: Option<OwnedSemaphorePermit>,
    mut slot: Option<WsSlot>,
) {
    for eff in effects {
        match eff {
            // The checkpoint commit already moved the run to Running — the
            // record IS this step's durable dispatch; just invoke it.
            Effect::DispatchStep { step, .. } => {
                let (ws, name) = core
                    .run(run)
                    .and_then(|r| {
                        r.spec
                            .steps
                            .get(step.0 as usize)
                            .map(|s| (r.ws, s.handler.clone()))
                    })
                    .unwrap_or_default();
                if !exec.registry.contains(ws, &name) {
                    exec.fail(core, run, step, "unregistered handler");
                    continue;
                }
                // The inherited slot already owns its share — the cap
                // check is only for a DispatchStep that arrives with
                // none (a restart's effect, a sweep): then the fresh
                // slot must fit, else the run stays journaled Running
                // and the deadline sweep reconciles it (S2 N3).
                let slot = match slot.take() {
                    Some(s) => Some(s),
                    None if exec.inflight.count(ws) < exec.ws_cap => {
                        Some(WsSlot::take(exec.inflight, ws))
                    }
                    None => None,
                };
                // Every DispatchStep comes from an event carrying a permit,
                // so this always fires; if it ever cannot, the run stays
                // journaled Running and the deadline sweep reconciles it —
                // it is never silently skipped.
                if let (Some(p), Some(s)) = (
                    permit
                        .take()
                        .or_else(|| exec.sem.clone().try_acquire_owned().ok()),
                    slot,
                ) {
                    exec.fire(core, run, step, p, s);
                }
            }
            Effect::Reconcile { step } => {
                // The finishing call's permit and slot are spent on its
                // verdict, so the probe re-enters the normal path — it
                // queues if the workspace is at share or the pool is
                // empty (S2 N1/N3).
                drop(permit.take());
                drop(slot.take());
                exec.probe(core, run, step);
            }
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

    /// S2 re-review N4: the shipped in-flight share must leave headroom —
    /// four tenants each at their cap must not tile the permit pool, or a
    /// fifth workspace's dispatches starve on capacity it never touched.
    #[test]
    fn shipped_in_flight_share_leaves_the_fifth_tenant_headroom() {
        let c = ShellConfig::default();
        assert!(4 * c.max_in_flight_per_ws < c.max_in_flight);
    }

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
    /// shape of a worker killed between dispatch and reply. On that first
    /// call it can freeze the engine's journal (free space to 0: nothing
    /// more is journaled, the deadline sweep included) and say it was
    /// called. Its probe is honest: the effect landed iff a call reached it.
    struct HangOnce {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        freeze: Option<Arc<std::sync::atomic::AtomicU64>>,
        entered: Arc<tokio::sync::Notify>,
    }
    impl HangOnce {
        fn new(calls: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
            Self {
                calls: calls.clone(),
                freeze: None,
                entered: Arc::default(),
            }
        }
    }
    #[async_trait::async_trait]
    impl StepHandler for HangOnce {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                if let Some(free) = &self.freeze {
                    free.store(0, std::sync::atomic::Ordering::SeqCst);
                }
                self.entered.notify_one();
                std::future::pending().await
            }
            Outcome::Checkpoint(b"ok".to_vec())
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            self.calls.load(std::sync::atomic::Ordering::SeqCst) > 0
        }
    }

    /// Its first call panics inside the task; later calls answer. The
    /// probe keeps the default "not found", so a journaled Lost re-fires.
    struct PanicOnce {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StepHandler for PanicOnce {
        async fn execute(&self, _s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                panic!("handler blew up inside the task");
            }
            Outcome::Checkpoint(b"ok".to_vec())
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
        eng.list(0, None, MAX_PAGE)
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

    /// Poll until the run is terminal, against a wall-clock deadline (not
    /// a count of sleeps, which a loaded scheduler stretches).
    async fn until_terminal_within(eng: &Engine, run: RunId, limit: Duration) -> RunState {
        let deadline = tokio::time::Instant::now() + limit;
        while tokio::time::Instant::now() < deadline {
            if let Some(r) = eng.inspect(run).await.unwrap_or(None) {
                if r.state.is_terminal() {
                    return r.state;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run never reached a terminal state within {limit:?}");
    }

    /// kill -9 between dispatch and reply, then restart on the same
    /// journal: the orphaned Running attempt must reconcile — never sit
    /// `running` forever, and never blindly re-fire the remote call.
    ///
    /// Deterministic under load (review T2): the first engine is stopped
    /// on the handler's own signal that it was called (not by catching a
    /// 100 ms state window), its journal frozen at that moment so the
    /// deadline sweep cannot move the run on first; the restart waits for
    /// the first engine to release the journal (not a fixed sleep); and the
    /// probe answers from whether the call happened (not always "found").
    #[tokio::test(flavor = "current_thread")]
    async fn restart_orphaned_running_run_reconciles() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let free = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let probe_free = free.clone();
        let cfg = ShellConfig {
            tick_ms: 5,
            limits: fast_limits(),
            ..Default::default()
        };
        let first = ShellConfig {
            journal: JournalOptions {
                space: durable_journal::SpaceProbe::new(move |_| {
                    Ok(probe_free.load(std::sync::atomic::Ordering::SeqCst))
                }),
                ..JournalOptions::default()
            },
            ..cfg.clone()
        };
        let run;
        {
            let handler = HangOnce {
                freeze: Some(free),
                ..HangOnce::new(&calls)
            };
            let entered = handler.entered.clone();
            let mut reg = Registry::default();
            reg.register("hang", Arc::new(handler));
            let eng = spawn(d.path(), reg, first).unwrap_or_else(|e| panic!("{e}"));
            run = eng
                .submit(OperationKey("orphan-running".into()), spec1("hang"))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            // The call is in flight, so its dispatch is durable (D-014).
            tokio::time::timeout(Duration::from_secs(30), entered.notified())
                .await
                .unwrap_or_else(|_| panic!("the handler was never called"));
            let stopped = eng.on_stop();
            drop(eng); // the process dies mid-flight; the journal keeps Running
            tokio::time::timeout(Duration::from_secs(30), stopped)
                .await
                .unwrap_or_else(|_| panic!("the first engine never released the journal"));
        }
        let mut reg = Registry::default();
        reg.register("hang", Arc::new(HangOnce::new(&calls)));
        let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
        let state = until_terminal_within(&eng, run, Duration::from_secs(30)).await;
        assert!(matches!(state, RunState::Done), "{state:?}");
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
            let stopped = eng.on_stop();
            drop(eng); // probe#1 hangs forever — the crash window
            tokio::time::timeout(Duration::from_secs(30), stopped)
                .await
                .unwrap_or_else(|_| panic!("the first engine never released the journal"));
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

    /// Drain-rate measurement for the S2-7 investigation: N one-step Echo
    /// runs, time from last submit to all-Done. Run with
    /// `cargo test -p durable-engine drain_rate -- --ignored --nocapture`.
    /// Echo removes the network — whatever bounds this bounds the engine.
    #[tokio::test]
    #[ignore = "measurement, not a regression test"]
    async fn drain_rate_under_default_caps() {
        async fn measure(per_tick: usize, in_flight: usize, tick_ms: u64, n: usize) -> f64 {
            let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let mut reg = Registry::default();
            reg.register("e", Arc::new(Echo));
            let cfg = ShellConfig {
                tick_ms,
                max_dispatch_per_tick: per_tick,
                max_in_flight: in_flight,
                limits: Limits {
                    max_step_deadline_ms: 30_000,
                    ..Limits::default()
                },
                ..Default::default()
            };
            let eng = spawn(d.path(), reg, cfg).unwrap_or_else(|e| panic!("{e}"));
            for i in 0..n {
                eng.submit(
                    OperationKey(format!("drain-{i}")),
                    JobSpec {
                        version: 1,
                        input: b"x".to_vec(),
                        steps: vec![StepSpec {
                            name: "s".into(),
                            handler: "e".into(),
                        }],
                    },
                )
                .await
                .unwrap_or_else(|e| panic!("submit {i}: {e}"));
            }
            let start = std::time::Instant::now();
            let deadline = start + Duration::from_secs(120);
            loop {
                let d = eng.diag().await.unwrap_or_else(|e| panic!("{e}"));
                let done = d
                    .states
                    .iter()
                    .filter(|(k, _)| matches!(**k, "done" | "failed" | "cancelled"))
                    .map(|(_, c)| *c as usize)
                    .sum::<usize>();
                if done >= n {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "drain stalled at {done}/{n}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            let secs = start.elapsed().as_secs_f64();
            eprintln!(
                "per_tick={per_tick} in_flight={in_flight} tick_ms={tick_ms}: \
                 {n} done in {secs:.1}s = {:.0} runs/s",
                n as f64 / secs
            );
            n as f64 / secs
        }
        let base = measure(64, 32, 50, 2000).await;
        let raised = measure(512, 256, 50, 2000).await;
        let tight = measure(64, 128, 10, 2000).await;
        eprintln!("summary: default={base:.0}/s per_tick512={raised:.0}/s tick10={tight:.0}/s");
    }

    /// The per-workspace bound: `put` accepts 256 distinct names for one
    /// workspace and refuses the 257th — an update to an existing name
    /// never counts against the bound, and another workspace's names do
    /// not either.
    #[tokio::test(flavor = "current_thread")]
    async fn the_registry_bound_is_per_workspace() {
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
        for i in 0..MAX_HANDLERS_PER_WS {
            eng.put_handler(7, &format!("h{i}"), Arc::new(Echo))
                .await
                .unwrap_or_else(|e| panic!("h{i}: {e}"));
        }
        assert!(matches!(
            eng.put_handler(7, "one-too-many", Arc::new(Echo)).await,
            Err(EngineError::RegistryFull)
        ));
        // An update to an existing name is not a new registration.
        eng.put_handler(7, "h0", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // …and workspace 8's bound is untouched by 7's full set.
        eng.put_handler(8, "h0", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// S2 review #6: a tenant whose handler never answers may hold at
    /// most `max_in_flight_per_ws` calls — the rest of the pool stays
    /// free for everyone else. ws 7 parks its cap; ws 8's runs still
    /// dispatch and finish. Without the cap ws 7 would hold every
    /// permit in the global pool.
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_workspace_cannot_starve_another() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let eng = spawn(
            d.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                max_in_flight: 8,
                max_in_flight_per_ws: 2,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        // ws 7's handler blocks every call forever; ws 8's is instant.
        eng.put_handler(7, "block", Arc::new(Gated::default()))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(8, "fast", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        for i in 0..4 {
            eng.submit_scoped(7, OperationKey(format!("a-{i}")), spec("block"))
                .await
                .unwrap_or_else(|e| panic!("a-{i}: {e}"));
        }
        // Give ws 7's dispatches a moment to land and park.
        tokio::time::sleep(Duration::from_millis(100)).await;
        for i in 0..4 {
            let run = eng
                .submit_scoped(8, OperationKey(format!("b-{i}")), spec("fast"))
                .await
                .unwrap_or_else(|e| panic!("b-{i}: {e}"));
            assert!(
                matches!(until_done(&eng, run).await, RunState::Done),
                "b-{i} must finish while ws 7's calls hang forever"
            );
        }
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

    /// S2 re-review: a panicking handler task must not strand the
    /// workspace's in-flight share — the WsSlot returns on drop, so the
    /// journaled Lost can reconcile and retry. With
    /// `max_in_flight_per_ws = 1` a leaked slot would stall the probe
    /// and every later call forever: reaching `Done` proves the count
    /// came back to zero.
    #[tokio::test(flavor = "current_thread")]
    async fn panicking_handler_releases_the_workspace_slot() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut reg = Registry::default();
        reg.register(
            "panic-once",
            Arc::new(PanicOnce {
                calls: calls.clone(),
            }),
        );
        let eng = spawn(
            d.path(),
            reg,
            ShellConfig {
                tick_ms: 5,
                max_in_flight_per_ws: 1,
                limits: fast_limits(),
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let run = eng
            .submit(OperationKey("panic-slot".into()), spec1("panic-once"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, run).await, RunState::Done));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the panicked call was retried, not stranded"
        );
    }

    // --- D-058: the observer, fed by the actor and the executor ----------

    fn echo_engine(dir: &Path) -> Engine {
        let mut reg = Registry::default();
        reg.register("echo", Arc::new(Echo));
        reg.register("flaky", Arc::new(FlakyLost));
        spawn(
            dir,
            reg,
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    fn totals(eng: &Engine) -> crate::observe::MetricsOut {
        eng.observer().metrics(
            Scope::All,
            crate::observe::Window::M15,
            wall_ms(),
            IndexCounts::default(),
            &["echo".to_string(), "flaky".to_string()],
        )
    }

    /// A two-step run through the real executor: the commit path counts
    /// it, the executor times both calls under their handler, a lost reply
    /// reconciles, a resubmitted key is reported as deduplicated, and
    /// nothing is left in flight.
    #[tokio::test(flavor = "current_thread")]
    async fn the_observer_counts_runs_through_the_executor() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let eng = echo_engine(d.path());
        let first = eng
            .submit_with(OperationKey("obs-1".into()), spec("echo"), None)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!first.deduplicated);
        assert!(matches!(until_done(&eng, first.run).await, RunState::Done));
        let again = eng
            .submit_with(OperationKey("obs-1".into()), spec("echo"), None)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!((again.run, again.deduplicated), (first.run, true));
        let lost = eng
            .submit(OperationKey("obs-2".into()), spec1("flaky"))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(until_done(&eng, lost).await, RunState::Done));

        let m = totals(&eng);
        let c = m.totals.counters;
        assert_eq!(c.accepted, 2, "{c:?}");
        assert_eq!(c.completed, 2);
        assert_eq!(c.dispatched, 3, "two steps + one");
        assert_eq!(c.steps_ok, 2);
        assert_eq!((c.replies_lost, c.reconciled), (1, 1));
        let by = |n: &str| {
            m.handlers
                .iter()
                .find(|h| h.name == n)
                .cloned()
                .unwrap_or_else(|| panic!("{n}"))
        };
        let echo = by("echo");
        assert_eq!((echo.calls, echo.ok), (2, 2));
        assert!(echo.step_ms.max.is_some() && echo.last_ok_at.is_some());
        let flaky = by("flaky");
        assert_eq!((flaky.calls, flaky.replies_lost), (1, 1));
        assert!(flaky.last_failure_at.is_some());
        assert_eq!(eng.observer().in_flight(), 0);
        // The actor's own counts, through the read path.
        let counts = eng.index_counts().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            (counts.states.done, counts.live_runs, counts.active_runs),
            (2, 2, 0)
        );
        let t = eng.observer().timeline(first.run);
        assert!(t.complete);
        assert_eq!(t.events.len(), 6, "{t:?}");
    }

    /// A record's observation waits for its fsync like everything else it
    /// decided: while the covering fsync is held, the observer has nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn the_observer_sees_nothing_before_the_covering_fsync() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let e = eng.clone();
        let submit = tokio::spawn(async move {
            e.submit_at(OperationKey("held".into()), spec("echo"), FAR)
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!submit.is_finished());
        assert!(
            eng.observer().feed(0, 50).1.is_empty(),
            "nothing before the fsync"
        );
        assert_eq!(totals(&eng).totals.counters.accepted, 0);
        faults.release_syncs();
        let run = bounded(submit)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("{e}"));
        // The ack is released only after the observer has the record.
        let t = eng.observer().timeline(run);
        assert!(t.complete);
        assert_eq!(t.events.len(), 1);
        assert_eq!(totals(&eng).totals.counters.accepted, 1);
    }

    /// The shell's restart path: a new engine on the same journal rebuilds
    /// the timeline from replay; its metrics start empty.
    #[tokio::test(flavor = "current_thread")]
    async fn a_restarted_engine_rebuilds_timelines_from_replay() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let run;
        {
            let eng = echo_engine(d.path());
            run = eng
                .submit(OperationKey("replayed".into()), spec("echo"))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(matches!(until_done(&eng, run).await, RunState::Done));
            bounded(eng.shutdown(Duration::from_secs(5)))
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let eng = echo_engine(d.path());
        let t = eng.observer().timeline(run);
        assert!(t.complete, "{t:?}");
        assert_eq!(t.events.len(), 6);
        assert_eq!(eng.observer().feed(0, 1).1[0].0, run);
        assert_eq!(
            totals(&eng).totals.counters,
            crate::observe::Counters::default()
        );
    }

    /// Counts handler invocations — the evidence a parked run made none.
    struct Counting {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        probes: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StepHandler for Counting {
        async fn execute(&self, _s: &StepSpec, input: &[u8], _k: &str) -> Outcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Outcome::Checkpoint(input.to_vec())
        }
        async fn probe(&self, _s: &StepSpec, _k: &str) -> bool {
            self.probes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            true
        }
    }

    /// Append a v3 record as the S2 writer shapes it — the fixture for
    /// a journal written under workspace scoping.
    fn commit_v3(dir: &std::path::Path, run: u64, ws: u64, event: &str) {
        let mut j = durable_journal::Journal::open(dir).unwrap_or_else(|e| panic!("{e}"));
        j.append(
            format!("{{\"v\":3,\"ws\":{ws},\"run\":{run},\"at\":1000,\"event\":{event}}}")
                .as_bytes(),
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }

    fn v3_accepted(key: &str) -> String {
        format!(
            "{{\"Accepted\":{{\"key\":\"{key}\",\"spec\":{{\"version\":1,\
             \"steps\":[{{\"name\":\"only\",\"handler\":\"echo\"}}],\"input\":[]}},\
             \"not_before\":0}}}}"
        )
    }

    /// The S2-review scenario end to end on the workspace-aware binary:
    /// replay a journal carrying tenant runs — a queued one and a
    /// crash-orphaned Reconciling one — and each resolves its handler in
    /// its OWN workspace's registry, never borrowing workspace 0's.
    /// Diagnostics expose the journal shape the rollback guard reads.
    #[tokio::test(flavor = "current_thread")]
    async fn replayed_tenant_runs_dispatch_in_their_workspace() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("tenant"));
        commit_v3(d.path(), 2, 8, &v3_accepted("other-tenant"));
        commit_v3(
            d.path(),
            2,
            8,
            "{\"StepDispatched\":{\"step\":0,\"attempt\":1,\"deadline\":1000}}",
        );
        commit_v3(d.path(), 2, 8, "{\"ReplyLost\":{\"step\":0}}");
        commit_v3(d.path(), 3, 0, &v3_accepted("ops"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut reg = Registry::default();
        let h = Arc::new(Counting {
            calls: calls.clone(),
            probes: probes.clone(),
        });
        reg.register("echo", h.clone()); // workspace 0 (compat path)
        reg.put(7, "echo", h.clone())
            .unwrap_or_else(|e| panic!("{e}"));
        reg.put(8, "echo", h).unwrap_or_else(|e| panic!("{e}"));
        let eng = spawn(
            d.path(),
            reg,
            ShellConfig {
                tick_ms: 5,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        for run in [RunId(1), RunId(2), RunId(3)] {
            assert!(
                matches!(until_done(&eng, run).await, RunState::Done),
                "run {run:?} must finish in its own workspace"
            );
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2, // the two queued runs — ws-7's and ws-0's
            "each workspace's run called its own registration"
        );
        assert_eq!(
            probes.load(std::sync::atomic::Ordering::SeqCst),
            1, // ws-8's orphaned Reconciling run was re-probed
            "the tenant orphan reconciled through its own workspace"
        );
        let diag = eng.diag().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(diag.journal_max_ws, 8);
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
            .list(0, None, MAX_PAGE)
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

    /// Calls by step name — lets a test tell step 2's call from the
    /// backlog run's, where a global count would conflate them.
    #[derive(Clone, Default)]
    struct NamedCalls {
        calls: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    }
    #[async_trait::async_trait]
    impl StepHandler for NamedCalls {
        async fn execute(&self, s: &StepSpec, _i: &[u8], _k: &str) -> Outcome {
            *self
                .calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(s.name.clone())
                .or_default() += 1;
            Outcome::Checkpoint(b"ok".to_vec())
        }
    }

    fn spec2(handler: &str) -> JobSpec {
        JobSpec {
            version: 1,
            input: b"payload".to_vec(),
            steps: vec![
                StepSpec {
                    name: "one".into(),
                    handler: handler.into(),
                },
                StepSpec {
                    name: "two".into(),
                    handler: handler.into(),
                },
            ],
        }
    }

    /// S1 re-review (B, finding 1): a finishing call must hand its
    /// workspace slot to the run's next step — the journaled-Running
    /// claim for step n+1 was paid for by the call that just ended.
    /// Before the fix the task dropped its `WsSlot` before the join was
    /// processed; a tick could give the freed share to a backlog run,
    /// the `DispatchStep` then saw the workspace at cap and skipped —
    /// the step sat `Running` until its deadline and burned an attempt
    /// without ever being called.
    ///
    /// Drives `Exec`/`tick` directly (like `dispatch_per_tick_is_capped`):
    /// the interposition is exact — the tick runs between the finished
    /// call's join and its effect pass, which is the race window.
    #[tokio::test(flavor = "current_thread")]
    async fn a_finished_steps_workspace_slot_flows_to_the_next_step() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        // One workspace, cap 1: run A needs two steps, run B is the
        // backlog that would steal the freed share.
        let a = core
            .submit(OperationKey("a".into()), spec2("gated"), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let b = core
            .submit(OperationKey("b".into()), spec1("gated"), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let h = NamedCalls::default();
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(h.clone()));
        let sem = Arc::new(Semaphore::new(8));
        let (mut attempts, mut probes) = (JoinSet::new(), JoinSet::new());
        let (mut live, mut waiting) = (HashMap::new(), ProbeQueue::default());
        let (mut due_by_ws, mut due_order) = (BTreeMap::new(), VecDeque::new());
        let (mut parked, mut dead_since) = (HashMap::new(), HashMap::new());
        let mut dead_retry = BinaryHeap::new();
        let inflight = WsInflight::default();
        let mut spawned = HashMap::new();
        let mut mon = Monitor {
            health: Arc::default(),
            errors: BTreeMap::new(),
        };
        let mut pipe = Pipeline::new(None, 1024, usize::MAX);
        let obs = Arc::new(Observer::new(0));
        macro_rules! ex {
            () => {
                Exec {
                    registry: &reg,
                    sem: sem.clone(),
                    attempts: &mut attempts,
                    probes: &mut probes,
                    live: &mut live,
                    inflight: &inflight,
                    spawned: &mut spawned,
                    ws_cap: 1,
                    waiting_probes: &mut waiting,
                    due_by_ws: &mut due_by_ws,
                    due_order: &mut due_order,
                    parked: &mut parked,
                    dead_since: &mut dead_since,
                    dead_retry: &mut dead_retry,
                    dead_grace_ms: 0,
                    per_tick: 8,
                    examined: 0,
                    mon: &mut mon,
                    pipe: &mut pipe,
                    obs: &obs,
                }
            };
        }
        // The single slot dispatches A's step 1; B stays queued.
        {
            let mut ex = ex!();
            tick(&mut core, &mut ex);
        }
        assert_eq!(inflight.count(0), 1);
        assert_eq!(attempts.len(), 1);
        let calls = |h: &NamedCalls, name: &str| {
            h.calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(name)
                .copied()
                .unwrap_or(0)
        };
        // Take the finished call's result: its slot must still be held —
        // the count may not dip before the effect pass runs.
        let (run, step, outcome, permit, slot) = attempts
            .join_next()
            .await
            .unwrap_or_else(|| panic!("no attempt join"))
            .unwrap_or_else(|e| panic!("attempt join: {e}"));
        assert_eq!(run, a);
        assert_eq!(
            inflight.count(0),
            1,
            "the finished call returned its slot before its effects ran"
        );
        let (out, fail) = match outcome {
            Outcome::Checkpoint(o) => (Some(o), None),
            _ => panic!("step 1 did not checkpoint"),
        };
        let effects = mon.effects(core.step_finished(run, step, out, fail, wall_ms()));
        {
            let mut ex = ex!();
            // The race window: a tick between the call's end and its
            // effect pass must NOT give the slot to B — A still holds it.
            tick(&mut core, &mut ex);
            assert_eq!(
                calls(&h, "only"),
                0,
                "the backlog run stole the finished call's workspace slot"
            );
            assert!(!matches!(
                core.run(b).map(|r| &r.state),
                Some(RunState::Running { .. })
            ));
            // The effect pass: step 2's DispatchStep inherits the slot
            // and fires — it must not stop at the share check with a
            // count that still includes its own predecessor.
            run_effects(&mut core, &mut ex, effects, run, Some(permit), Some(slot));
        }
        assert_eq!(
            attempts.len(),
            1,
            "step 2 was journaled Running but never called"
        );
        assert_eq!(calls(&h, "one"), 1);
        // Step 2 completes; its returned permit+slot go back here.
        let (_, _, _, permit, slot) = attempts
            .join_next()
            .await
            .unwrap_or_else(|| panic!("no attempt join"))
            .unwrap_or_else(|e| panic!("attempt join: {e}"));
        drop((permit, slot));
        assert_eq!(calls(&h, "two"), 1, "step 2 was never called");
        assert_eq!(calls(&h, "only"), 0);
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
        let (mut due_by_ws, mut due_order) = (BTreeMap::new(), VecDeque::new());
        let (mut parked, mut dead_since) = (HashMap::new(), HashMap::new());
        let mut dead_retry = BinaryHeap::new();
        let inflight = WsInflight::default();
        let mut spawned = HashMap::new();
        let mut mon = Monitor {
            health: Arc::default(),
            errors: BTreeMap::new(),
        };
        // Direct-mode core: every commit is durable when it returns.
        let mut pipe = Pipeline::new(None, 1024, usize::MAX);
        let obs = Arc::new(Observer::new(0));
        let mut ex = Exec {
            registry: &reg,
            sem: sem.clone(),
            attempts: &mut attempts,
            probes: &mut probes,
            live: &mut live,
            inflight: &inflight,
            spawned: &mut spawned,
            ws_cap: 64,
            waiting_probes: &mut waiting,
            due_by_ws: &mut due_by_ws,
            due_order: &mut due_order,
            parked: &mut parked,
            dead_since: &mut dead_since,
            dead_retry: &mut dead_retry,
            dead_grace_ms: 0,
            per_tick: 3,
            examined: 0,
            mon: &mut mon,
            pipe: &mut pipe,
            obs: &obs,
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

    /// The scheduler state a `tick` test needs — the same fields the
    /// actor loop owns and lends to `Exec`.
    struct Sched {
        sem: Arc<Semaphore>,
        attempts: JoinSet<AttemptJoin>,
        probes: JoinSet<ProbeJoin>,
        live: HashMap<(RunId, StepIndex), AbortHandle>,
        inflight: WsInflight,
        spawned: HashMap<tokio::task::Id, (RunId, StepIndex)>,
        waiting: ProbeQueue,
        due_by_ws: BTreeMap<u64, VecDeque<(RunId, StepIndex, u32)>>,
        due_order: VecDeque<u64>,
        parked: HashMap<u64, Vec<Parked>>,
        dead_since: HashMap<RunId, Millis>,
        dead_retry: BinaryHeap<Reverse<(u64, u64, u32, u32)>>,
        mon: Monitor,
        pipe: Pipeline,
        obs: Arc<Observer>,
    }
    impl Sched {
        fn new(sem_permits: usize) -> Self {
            Self {
                sem: Arc::new(Semaphore::new(sem_permits)),
                attempts: JoinSet::new(),
                probes: JoinSet::new(),
                live: HashMap::new(),
                inflight: WsInflight::default(),
                spawned: HashMap::new(),
                waiting: ProbeQueue::default(),
                due_by_ws: BTreeMap::new(),
                due_order: VecDeque::new(),
                parked: HashMap::new(),
                dead_since: HashMap::new(),
                dead_retry: BinaryHeap::new(),
                mon: Monitor {
                    health: Arc::default(),
                    errors: BTreeMap::new(),
                },
                pipe: Pipeline::new(None, 1024, usize::MAX),
                obs: Arc::new(Observer::new(0)),
            }
        }
        fn exec<'a>(
            &'a mut self,
            registry: &'a Registry,
            ws_cap: u32,
            dead_grace_ms: u64,
            per_tick: usize,
        ) -> Exec<'a> {
            Exec {
                registry,
                sem: self.sem.clone(),
                attempts: &mut self.attempts,
                probes: &mut self.probes,
                live: &mut self.live,
                inflight: &self.inflight,
                spawned: &mut self.spawned,
                ws_cap,
                waiting_probes: &mut self.waiting,
                due_by_ws: &mut self.due_by_ws,
                due_order: &mut self.due_order,
                parked: &mut self.parked,
                dead_since: &mut self.dead_since,
                dead_retry: &mut self.dead_retry,
                dead_grace_ms,
                per_tick,
                examined: 0,
                mon: &mut self.mon,
                pipe: &mut self.pipe,
                obs: &self.obs,
            }
        }
    }

    /// A tick's index read is the due prefix, never the retained set
    /// (S2 re-review finding 5): forty not-yet-due runs behind three due
    /// ones cost three examinations, not forty-three.
    #[tokio::test(flavor = "current_thread")]
    async fn a_tick_examines_only_the_due_prefix() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let far = wall_ms() + 3_600_000;
        for i in 0..40 {
            core.submit_at(
                0,
                OperationKey(format!("later-{i}")),
                spec1("gated"),
                far,
                wall_ms(),
            )
            .unwrap_or_else(|e| panic!("{e}"));
        }
        for i in 0..3 {
            core.submit(OperationKey(format!("now-{i}")), spec1("gated"), wall_ms())
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(HangAlways));
        let mut sched = Sched::new(8);
        core.examined.set(0);
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
            assert_eq!(ex.examined, 3, "queue examinations beyond the due work");
        }
        assert_eq!(
            core.examined.get(),
            3,
            "the tick scanned runs that were not due"
        );
        assert!(!sched.due_by_ws.contains_key(&0), "all due work dispatched");
        assert_eq!(sched.attempts.len(), 3);
    }

    /// S2 re-review N1: a dispatch whose commit fails for a real reason
    /// (NoSpace) keeps its queue entry — it is live work, not a stale
    /// claim — and the run dispatches on a later tick once space is back,
    /// with no restart. The unfixed code dropped every queued entry on
    /// the error, leaving runs queued-but-unindexed until a recount.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_dispatch_keeps_its_queue_entry() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let free = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let probe_free = free.clone();
        let mut core = EngineCore::open_with(
            d.path(),
            JournalOptions {
                space: durable_journal::SpaceProbe::new(move |_| {
                    Ok(probe_free.load(std::sync::atomic::Ordering::SeqCst))
                }),
                ..JournalOptions::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        core.submit(OperationKey("k".into()), spec1("hang"), wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.register("hang", Arc::new(HangAlways));
        let mut sched = Sched::new(8);
        // Space at exactly the reserve: `space_ok` still admits the tick
        // but the commit is refused NoSpace — the entry must survive,
        // queued again, not dropped.
        free.store(
            durable_journal::DEFAULT_RESERVE_BYTES,
            std::sync::atomic::Ordering::SeqCst,
        );
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
        }
        assert_eq!(
            sched.due_by_ws.get(&0).map(VecDeque::len),
            Some(1),
            "the NoSpace-dropped entry left the queue"
        );
        assert_eq!(sched.attempts.len(), 0, "nothing dispatched while full");
        // Space back: the queued entry dispatches on the next tick.
        free.store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
        }
        assert_eq!(sched.attempts.len(), 1, "the run never dispatched");
        assert!(
            sched.due_by_ws.get(&0).is_none_or(VecDeque::is_empty),
            "the entry was not consumed by dispatch"
        );
    }

    /// S3 re-review N7: an in-grace dead-handler entry re-checks on the
    /// cadence, not every tick. Ticks inside the re-check interval
    /// examine nothing — the entry waits on `dead_retry` — and it still
    /// parks at the exact grace deadline.
    #[tokio::test(flavor = "current_thread")]
    async fn an_in_grace_dead_entry_rechecks_on_a_cadence() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let t0 = wall_ms();
        core.submit_at(0, OperationKey("dead".into()), spec1("gone"), t0, t0)
            .unwrap_or_else(|e| panic!("{e}"));
        let reg = Registry::default(); // "gone" never resolves
        let mut sched = Sched::new(8);
        {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0);
            assert_eq!(ex.examined, 1, "the first sighting marked it");
        }
        // Ticks inside the re-check interval re-examine nothing.
        for i in 1..=4u64 {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0 + i * 100);
            assert_eq!(
                ex.examined, 0,
                "tick {i} re-examined an in-grace dead entry"
            );
        }
        assert_eq!(
            sched.dead_retry.len(),
            1,
            "the in-grace entry waits on the heap"
        );
        // At the re-check boundary it is examined exactly once more…
        {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0 + 1_000);
            assert_eq!(ex.examined, 1, "the re-check did not happen");
        }
        // …and it parks on the grace deadline, not a tick early or late.
        {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0 + 10_000);
        }
        assert_eq!(sched.parked.get(&0).map(Vec::len), Some(1));
        assert!(sched.dead_since.is_empty(), "the marker outlived parking");
    }

    /// S3 re-review N7, the reconcile arm itself: a handler that returns
    /// DURING the grace window must see its waiters come off `dead_retry`
    /// at the re-check and dispatch — not wait out the whole grace and
    /// park with their handler already present. Without the pour back
    /// into the rotation the run strands until a restart's recount.
    #[tokio::test(flavor = "current_thread")]
    async fn an_in_grace_entry_dispatches_when_its_handler_returns() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let t0 = wall_ms();
        core.submit_at(0, OperationKey("revive".into()), spec1("gone"), t0, t0)
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default(); // absent at the first sighting
        let mut sched = Sched::new(8);
        {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0); // marker = t0, due back t0+1000
        }
        assert_eq!(sched.dead_retry.len(), 1, "the entry waits on the heap");
        // The handler returns mid-grace; at the next re-check tick the
        // entry must re-enter the rotation and dispatch.
        let h = NamedCalls::default();
        reg.put(0, "gone", Arc::new(h.clone()))
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut ex = sched.exec(&reg, 64, 10_000, 8);
            tick_at(&mut core, &mut ex, t0 + 1_000);
        }
        assert_eq!(sched.attempts.len(), 1, "the revived run dispatched");
        assert!(
            sched.dead_retry.is_empty(),
            "the re-check tuple stayed on the heap"
        );
        let (r, _, outcome, _permit, _slot) = sched
            .attempts
            .join_next()
            .await
            .unwrap_or_else(|| panic!("no attempt join"))
            .unwrap_or_else(|e| panic!("attempt join: {e}"));
        assert!(
            matches!(outcome, Outcome::Checkpoint(_)),
            "{r:?} {outcome:?}"
        );
    }

    /// S3 re-review N7: a PUT/DELETE flap must not restart the
    /// dead-handler grace clock. The old code dropped `dead_since` the
    /// moment a present handler was *seen* — even when the run stayed
    /// queued (here, a starved permit) — so a DELETE before the next
    /// absence check minted a fresh grace window and the entry never
    /// parked. The marker now dies only when the run leaves the wait.
    #[tokio::test(flavor = "current_thread")]
    async fn a_delete_put_flap_does_not_restart_the_dead_grace() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let t0 = wall_ms();
        core.submit_at(0, OperationKey("flap".into()), spec1("h"), t0, t0)
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default(); // absent at the first sighting
        let mut sched = Sched::new(0); // zero permits: present-but-starved
        {
            let mut ex = sched.exec(&reg, 64, 2_000, 8);
            tick_at(&mut core, &mut ex, t0); // marker = t0; due back t0+1000
        }
        assert_eq!(
            sched.dead_since.values().copied().collect::<Vec<_>>(),
            vec![t0],
            "the marker did not record the first absence"
        );
        // PUT: present at the re-check, but the run cannot dispatch.
        reg.put(0, "h", Arc::new(HangAlways))
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut ex = sched.exec(&reg, 64, 2_000, 8);
            tick_at(&mut core, &mut ex, t0 + 1_000);
        }
        assert_eq!(sched.attempts.len(), 0, "nothing may dispatch on 0 permits");
        assert_eq!(
            sched.dead_since.values().copied().collect::<Vec<_>>(),
            vec![t0],
            "a present sighting must not erase the marker while the run waits"
        );
        // DELETE again — the flap. Under the old marker policy this
        // minted a fresh grace at t0+1500.
        reg.remove(0, "h");
        {
            let mut ex = sched.exec(&reg, 64, 2_000, 8);
            tick_at(&mut core, &mut ex, t0 + 1_500);
        }
        {
            let mut ex = sched.exec(&reg, 64, 2_000, 8);
            tick_at(&mut core, &mut ex, t0 + 2_000);
        }
        assert_eq!(
            sched.parked.get(&0).map(Vec::len),
            Some(1),
            "the flap restarted the grace clock — the entry did not park"
        );
    }

    fn pipe(
        sent_upto: u64,
        durable: u64,
        records: usize,
    ) -> (Pipeline, std::sync::mpsc::Receiver<WriterMsg>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        let mut p = Pipeline::new(Some((tx, std::thread::spawn(|| ()))), 1024, 4 << 20);
        p.sent_upto = sent_upto;
        p.durable = durable;
        if records > 0 {
            let bytes = 8 * records;
            p.outbound.push_back(WriterMsg::Append {
                records: vec![vec![0u8; 8]; records],
                bytes,
                upto: sent_upto + records as u64,
            });
            p.outbound_records = records;
            p.outbound_bytes = bytes;
        }
        (p, rx)
    }

    #[test]
    fn a_busy_writer_pipeline_still_gets_fed() {
        // Regression for the one-batch-in-flight gate: it withheld every
        // later batch until the previous fsync reported durable, so each
        // fsync paid a full actor wake round trip (~tens of ms under a
        // saturated offer — the 1M battery's submit-latency collapse).
        // A busy pipeline must ship once MIN_SHIP_RECORDS accumulate.
        let (mut p, rx) = pipe(1, 0, MIN_SHIP_RECORDS + 44);
        assert!(p.flush());
        let shipped: usize = rx.try_iter().map(|m| m.records()).sum();
        assert_eq!(
            shipped,
            MIN_SHIP_RECORDS + 44,
            "a busy pipeline shipped {shipped} records"
        );
    }

    #[test]
    fn a_busy_writer_pipeline_coalesces_small_accumulations() {
        // The other half of the gate: below MIN_SHIP_RECORDS, keep
        // accumulating so fsyncs carry real groups (the
        // queued_commits_share_one_fsync property), then ship the moment
        // the pipeline drains.
        let (mut p, rx) = pipe(1, 0, MIN_SHIP_RECORDS - 156);
        assert!(p.flush());
        assert_eq!(
            rx.try_iter().count(),
            0,
            "small accumulations shipped early"
        );
        p.durable = p.sent_upto;
        assert!(p.flush());
        assert_eq!(rx.try_iter().count(), 1, "an idle pipeline did not ship");
    }

    /// A workspace at its in-flight share is skipped whole — its queued
    /// entries are not examined at all (finding 5). The O(1) skip is the
    /// evidence: a second tick over a full workspace's forty-deep queue
    /// examines zero entries.
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_workspace_skips_its_whole_queue() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        for i in 0..41 {
            core.submit_at(
                1,
                OperationKey(format!("w1-{i}")),
                spec1("gated"),
                0,
                wall_ms(),
            )
            .unwrap_or_else(|e| panic!("{e}"));
        }
        core.submit_at(2, OperationKey("w2".into()), spec1("gated"), 0, wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default();
        reg.put(1, "gated", Arc::new(HangAlways))
            .unwrap_or_else(|e| panic!("{e}"));
        reg.put(2, "gated", Arc::new(HangAlways))
            .unwrap_or_else(|e| panic!("{e}"));
        let mut sched = Sched::new(8);
        // ws_cap 1: tick 1 dispatches ws 1's first run (which hangs,
        // holding the share) and ws 2's run.
        {
            let mut ex = sched.exec(&reg, 1, 0, 8);
            tick(&mut core, &mut ex);
        }
        assert_eq!(sched.attempts.len(), 2);
        assert_eq!(sched.inflight.count(1), 1, "ws 1's share is held");
        assert_eq!(sched.due_by_ws[&1].len(), 40, "ws 1's backlog stays queued");
        // Tick 2: ws 1 is at share — its forty-entry queue must cost
        // zero examinations while it sits out the rotation.
        let mut ex = sched.exec(&reg, 1, 0, 8);
        tick(&mut core, &mut ex);
        assert_eq!(
            ex.examined, 0,
            "a full workspace's queue was examined entry by entry"
        );
        assert_eq!(sched.due_by_ws[&1].len(), 40, "skipped entries stay queued");
        assert_eq!(sched.attempts.len(), 2, "nothing else dispatched");
    }

    /// A run whose handler left the registry parks after the grace
    /// period — it leaves the due rotation entirely instead of being
    /// re-examined every tick — and re-dispatches when the handler
    /// returns (finding 5: dead-handler runs never leave the due set).
    #[tokio::test(flavor = "current_thread")]
    async fn a_dead_handler_run_parks_until_its_handler_returns() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let run = core
            .submit(OperationKey("orphaned".into()), spec1("ghost"), wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let mut reg = Registry::default(); // "ghost" is never registered — yet
        let mut sched = Sched::new(8);
        // Grace 0: the first examination parks it.
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
            assert_eq!(ex.examined, 1);
        }
        assert!(
            sched.parked[&0].iter().any(|(r, ..)| *r == run),
            "the dead-handler run did not park"
        );
        assert!(
            matches!(
                core.run(run).map(|r| &r.state),
                Some(RunState::Queued { .. })
            ),
            "a parked run stays queued — it was never called"
        );
        assert!(
            !sched.due_by_ws.contains_key(&0),
            "parked left the rotation"
        );
        // The next tick examines nothing: the parked run is out of the
        // rotation, not re-scanned on every pass.
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
            assert_eq!(
                ex.examined, 0,
                "the parked run was re-examined — it never left the due set"
            );
        }
        // The handler returning revives it: it re-queues and dispatches.
        let h = NamedCalls::default();
        reg.put(0, "ghost", Arc::new(h.clone()))
            .unwrap_or_else(|e| panic!("{e}"));
        let mut ex = sched.exec(&reg, 64, 0, 8);
        ex.unpark(&core, 0);
        tick(&mut core, &mut ex);
        assert_eq!(
            ex.examined, 1,
            "the revived run should be the only examination"
        );
        assert_eq!(sched.attempts.len(), 1, "the revived run dispatched");
        let (r, _, outcome, _permit, _slot) = sched
            .attempts
            .join_next()
            .await
            .unwrap_or_else(|| panic!("no attempt join"))
            .unwrap_or_else(|e| panic!("attempt join: {e}"));
        assert_eq!(r, run);
        assert!(matches!(outcome, Outcome::Checkpoint(_)));
        assert_eq!(
            h.calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get("only")
                .copied()
                .unwrap_or(0),
            1,
            "the parked run's handler was finally called"
        );
    }

    /// S2 re-review N2: a cancelled run drops its scheduler entries —
    /// the parked slot and the in-grace dead_since marker — at cancel
    /// time, not whenever its dead handler happens to return. Without
    /// the drop, a register/submit/delete/cancel cycle accumulates an
    /// entry per cancelled run forever.
    #[tokio::test(flavor = "current_thread")]
    async fn a_cancelled_dead_handler_run_leaves_no_scheduler_entry() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let parked_run = core
            .submit(OperationKey("parked".into()), spec1("ghost"), wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let grace_run = core
            .submit(OperationKey("grace".into()), spec1("ghost"), wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let reg = Registry::default(); // "ghost" never registered
        let mut sched = Sched::new(8);
        // Park the first run (grace 0) — the second is parked directly
        // for the unpark-prune check below.
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
        }
        assert_eq!(sched.parked.get(&0).map(Vec::len), Some(2));
        sched.dead_since.insert(grace_run, wall_ms()); // an in-grace marker survives
        sched
            .dead_retry
            .push(Reverse((wall_ms() + 1_000, grace_run.0, 0, 1))); // and its re-check tuple rides the heap
                                                                    // The cancel path drops every collection's entries for the run —
                                                                    // the heap's too (N2: dropping only the marker left the tuple to
                                                                    // re-queue the run at its re-check).
        core.cancel(parked_run, wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        core.cancel(grace_run, wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            ex.untrack(0, parked_run);
            ex.untrack(0, grace_run);
        }
        assert!(
            sched.parked.get(&0).is_none_or(Vec::is_empty),
            "a cancelled run's parked entry survived"
        );
        assert!(
            sched.dead_since.is_empty(),
            "a cancelled run's in-grace marker survived"
        );
        assert!(
            sched.dead_retry.is_empty(),
            "a cancelled run's re-check tuple survived on the heap"
        );
        // And the reconcile-time sweep prunes a cancelled parked run
        // even when nothing tracked the cancel (belt for the same leak).
        let stale = core
            .submit(OperationKey("stale".into()), spec1("ghost"), wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick(&mut core, &mut ex);
        }
        assert_eq!(sched.parked.get(&0).map(Vec::len), Some(1));
        core.cancel(stale, wall_ms())
            .unwrap_or_else(|e| panic!("{e}"));
        let mut ex = sched.exec(&reg, 64, 0, 8);
        ex.unpark(&core, 0);
        assert!(
            sched.parked.get(&0).is_none_or(Vec::is_empty),
            "unpark kept a cancelled run's entry"
        );
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

    /// R1: the deadline sweep pops a batch, then processes it — on a real
    /// commit error it stopped after re-indexing only the entry that
    /// failed, dropping every other popped deadline until the next
    /// restart's recount. Two expired attempts at exactly the reserve:
    /// the first's lost-reply commit fails NoSpace, and BOTH entries must
    /// survive to re-fire once space returns. S3 re-review F5: the clock
    /// is injected — `tick_at` and an explicit `now`, no sleeps, no
    /// machine-speed dependence.
    #[tokio::test(flavor = "current_thread")]
    async fn the_sweep_requeues_every_popped_deadline_on_commit_error() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let free = Arc::new(AtomicU64::new(u64::MAX));
        let probe_free = free.clone();
        let reserve = 4096u64;
        let mut core = EngineCore::open_with(
            d.path(),
            JournalOptions {
                reserve_bytes: reserve,
                space: durable_journal::SpaceProbe::new(move |_| Ok(probe_free.load(SeqCst))),
                ..JournalOptions::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        core.limits.max_step_deadline_ms = 60;
        // An injected epoch — the wall clock never enters this test.
        let t0: Millis = 1_000_000;
        let a = core
            .submit_at(0, OperationKey("sweep-a".into()), spec1("gated"), 0, t0)
            .unwrap_or_else(|e| panic!("{e}"));
        let b = core
            .submit_at(0, OperationKey("sweep-b".into()), spec1("gated"), 0, t0)
            .unwrap_or_else(|e| panic!("{e}"));
        let gate = Arc::new(Semaphore::new(0));
        let mut reg = Registry::default();
        reg.register("gated", Arc::new(GateThenFound(gate.clone())));
        let mut sched = Sched::new(8);
        // Dispatch both at t0: two attempts in flight, deadlines at
        // t0+60 in the index.
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick_at(&mut core, &mut ex, t0);
        }
        assert_eq!(sched.attempts.len(), 2, "both runs dispatched");
        // Exactly at the reserve: the sweep's space gate still passes
        // but every record commit refuses NoSpace — the break path.
        free.store(reserve + 1, SeqCst);
        // Both deadlines fire under the failed commit: the popped batch
        // holds a and b; a's lost-reply commit fails first and the fix
        // re-indexes EVERY remaining entry, not just a's.
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick_at(&mut core, &mut ex, t0 + 61);
        }
        // Space back: the requeued deadlines re-fire on the next sweep —
        // b's only existed if the requeue kept it.
        free.store(u64::MAX, SeqCst);
        {
            let mut ex = sched.exec(&reg, 64, 0, 8);
            tick_at(&mut core, &mut ex, t0 + 62);
        }
        // Both expired attempts reconcile: the probes find the effect
        // (GateThenFound::probe is true) — a reconcile that never fired
        // leaves its run Running until a restart's recount.
        let mut reconciled = 0;
        while let Some(join) = sched.probes.join_next().await {
            let (run, step, found, permit, slot) =
                join.unwrap_or_else(|e| panic!("probe died: {e}"));
            let mut ex = sched.exec(&reg, 64, 0, 8);
            let effects = ex.mon.effects(core.reconciled(run, step, found, t0 + 62));
            run_effects(&mut core, &mut ex, effects, run, Some(permit), Some(slot));
            reconciled += 1;
        }
        assert_eq!(reconciled, 2, "both popped deadlines re-fired");
        for run in [a, b] {
            assert!(
                matches!(core.run(run).map(|r| &r.state), Some(RunState::Done)),
                "run {:?} did not reconcile: {:?}",
                run,
                core.run(run).map(|r| &r.state)
            );
        }
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

    /// Review (still open): SIGTERM killed the process like a crash. A
    /// drain answers what was already decided once its fsync returns,
    /// refuses everything after, joins the writer, and loses nothing.
    #[tokio::test]
    async fn shutdown_drains_what_was_decided_and_refuses_the_rest() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let e = eng.clone();
        let held = tokio::spawn(async move {
            e.submit_at(OperationKey("held".into()), spec1("echo"), FAR)
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await; // its record is in the writer
        let e = eng.clone();
        let stop = tokio::spawn(async move { e.shutdown(Duration::from_secs(10)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !stop.is_finished(),
            "the drain waits for the covering fsync"
        );
        faults.release_syncs();
        bounded(stop)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("drain: {e}"));
        let run = bounded(held)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("the decided submit is acknowledged: {e}"));
        assert!(matches!(
            eng.submit_at(OperationKey("late".into()), spec1("echo"), FAR)
                .await,
            Err(EngineError::Stopped)
        ));
        let core = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("the lock is free: {e}"));
        assert!(core.run(run).is_some(), "the drained submit is durable");
        assert_eq!(
            core.runs().count(),
            1,
            "nothing after the drain was journaled"
        );
    }

    /// Review (still open): the engine's replies were awaited with no
    /// deadline, so a stuck fsync hung every request, reads included. A
    /// caller now gets `Timeout` within the reply deadline; the stuck
    /// submit may still commit later (its outcome was unknown, not
    /// refused), and a retry with the same op_key finds it.
    ///
    /// The clock is frozen for the whole test and `advance` is the only
    /// thing that moves it: a deadline's firing is asserted as an
    /// ordering fact (the task ends while syncs are still held, before
    /// `release_syncs`), never as a wall-clock race a loaded machine
    /// can lose.
    #[tokio::test]
    async fn replies_have_a_deadline_while_the_journal_is_stuck() {
        tokio::time::pause();
        // A paused clock still auto-advances to the next pending timer
        // whenever the runtime has no runnable work — left alone it would
        // fast-forward straight into every armed reply deadline while a
        // real fsync is still in flight. A parked blocking task inhibits
        // auto-advance, so `advance` below is the only thing that moves
        // time.
        let (uninhibit, inhibit_rx) = std::sync::mpsc::channel::<()>();
        let inhibit = tokio::task::spawn_blocking(move || {
            let _ = inhibit_rx.recv();
        });
        const DEADLINE: Duration = Duration::from_millis(300);
        // `j` must finish within `by` of paused time — i.e. its reply
        // deadline fired. A deadline-less implementation leaves it
        // running (red-first against a mutant that drops the deadline).
        async fn deadline_fires<Out: Send + 'static>(
            j: tokio::task::JoinHandle<Out>,
            by: Duration,
        ) -> Out {
            for _ in 0..4 {
                tokio::task::yield_now().await; // let it arm its deadline
            }
            tokio::time::advance(by).await;
            for _ in 0..20 {
                if j.is_finished() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                j.is_finished(),
                "the reply deadline did not fire — the caller is hung"
            );
            j.await.unwrap_or_else(|e| panic!("join: {e}"))
        }
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let cfg = ShellConfig {
            reply_timeout: DEADLINE,
            ..piped(&faults, GroupCommit::default())
        };
        let eng = spawn(d.path(), echo_registry(), cfg).unwrap_or_else(|e| panic!("{e}"));
        // Frozen + inhibited: this submit's deadline cannot fire — only
        // the real journal answer resolves it, however slow the fsync.
        let early = eng
            .submit_at(OperationKey("early".into()), spec1("echo"), FAR)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        // Every deadline error below is asserted while the commit is
        // still stuck — `release_syncs` has not been called.
        let r = deadline_fires(
            tokio::spawn({
                let e = eng.clone();
                async move {
                    e.submit_at(OperationKey("stuck".into()), spec1("echo"), FAR)
                        .await
                }
            }),
            DEADLINE + Duration::from_millis(1),
        )
        .await;
        assert!(matches!(r, Err(EngineError::Timeout)), "{r:?}");
        let r = deadline_fires(
            tokio::spawn({
                let e = eng.clone();
                async move { e.inspect(early).await }
            }),
            DEADLINE + Duration::from_millis(1),
        )
        .await;
        assert!(
            matches!(r, Err(EngineError::Timeout)),
            "a read waits on the stuck commit: {r:?}"
        );
        let r = deadline_fires(
            tokio::spawn({
                let e = eng.clone();
                async move { e.list(0, None, 10).await }
            }),
            DEADLINE + Duration::from_millis(1),
        )
        .await;
        assert!(matches!(r, Err(EngineError::Timeout)), "{r:?}");
        faults.release_syncs();
        // Still frozen: the retried submit cannot time out — it resolves
        // when the real fsync lands, and dedup proves the timed-out
        // original committed exactly once.
        let again = eng
            .submit_at(OperationKey("stuck".into()), spec1("echo"), FAR)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again, RunId(2), "the timed-out submit committed once");
        drop(uninhibit);
        tokio::time::resume();
        inhibit.await.unwrap_or_else(|e| panic!("join: {e}"));
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

        let stopped = eng.on_stop();
        drop(eng);
        bounded(stopped).await;
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::time::Instant::now() < deadline {
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

    /// A real OS error BEFORE the swap point — a journal root the process
    /// can write into but not open for reading fails the sidecar's
    /// directory fsync (EACCES) — is an ordinary compaction error:
    /// the engine serves on, nothing is poisoned. (A post-rename fault
    /// still stops it; `compaction_error_at_any_step` covers step 3, and
    /// a permissions trick cannot reach past the sidecar write anymore.)
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn real_io_error_before_manifest_swap_is_not_fatal() {
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
            // Root ignores directory permissions: never pass silently.
            drop(guard);
            assert!(
                std::env::var_os("DURABLE_ALLOW_PERMISSION_SKIP").is_some(),
                "this test needs a user that honours directory permissions (not root); \
                 run it unprivileged, or set DURABLE_ALLOW_PERMISSION_SKIP=1 to skip it explicitly"
            );
            eprintln!("SKIPPED (explicitly): directory permissions are ignored here");
            return;
        }
        assert!(bounded(eng.compact()).await.is_err());
        let after = submit_acked(&eng, "post", 30).await;
        drop(guard);
        assert_eq!(
            after.len(),
            30,
            "a pre-swap compaction error must not stop the engine"
        );
        acked.extend(after);
        assert!(
            bounded(eng.shutdown(Duration::from_secs(5))).await.is_ok(),
            "a live engine drains cleanly"
        );
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
        let rest = submit_sized(&eng, "b", 90, 1024);
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
            "91 records of ~1.6 KB (D-054 base64) in {syncs} fsyncs under a 16 KiB bound"
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
        for i in 0..130 {
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
            "only {busy} of 130 refused: the outbox kept buffering"
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
                tokio::spawn(async move { e.list(0, None, MAX_PAGE).await.map(|p| p.runs.len()) })
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

    /// A second compaction requested while one is still running is told so
    /// with a stable conflict, not "busy, retry": retrying cannot help until
    /// the first one finishes.
    #[tokio::test(flavor = "current_thread")]
    async fn second_compaction_while_one_runs_is_a_conflict() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let eng = spawn(
            d.path(),
            echo_registry(),
            piped(&faults, GroupCommit::default()),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        faults.hold_syncs();
        let held = submit_many(&eng, "hold", 1); // the writer waits in this fsync
        tokio::time::sleep(Duration::from_millis(30)).await;
        let e = eng.clone();
        let first = tokio::spawn(async move { e.compact().await });
        tokio::time::sleep(Duration::from_millis(30)).await; // queued behind the batch
        let second = bounded(eng.compact()).await;
        assert!(
            matches!(second, Err(EngineError::CompactionRunning)),
            "{second:?}"
        );
        faults.release_syncs();
        join_all(held).await;
        let generation = bounded(first)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(generation, 2);
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
        // Flood a channel of capacity 1 with 50 submits at once: each
        // lands or is refused busy at once — they never pile up unbounded,
        // and nothing panics or hangs. (On this single-threaded runtime the
        // 50 tasks run before the actor drains the channel, so most are
        // refused.)
        let mut flood = tokio::task::JoinSet::new();
        for i in 0..50 {
            let eng = eng.clone();
            flood.spawn(async move {
                eng.submit(OperationKey(format!("flood-{i}")), spec("none"))
                    .await
            });
        }
        let (mut landed, mut busy) = (0, 0);
        while let Some(r) = flood.join_next().await {
            match r.unwrap_or_else(|e| panic!("{e}")) {
                Ok(_) => landed += 1,
                Err(EngineError::Refused(durable_domain::Refusal::ReserveExhausted)) => busy += 1,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(landed + busy, 50);
        assert!(
            busy >= 1,
            "a full command channel must refuse, not queue ({landed} landed)"
        );
        assert!(landed >= 1, "the channel still admits what fits");
    }
    /// S2 re-review N1: a waiting-probe queue whose every entry belongs
    /// to a full workspace must not spin the actor — each entry gets one
    /// try per tick, then waits for the next. ws 7: one run's execute
    /// hangs holding the workspace's only share; the deadline sweep
    /// reconciles it and the probe queues on the full workspace. ws 8's
    /// submit must still answer.
    // The engine runs on its own thread+runtime: a pinned actor deadlocks
    // whatever runtime hosts it, and on the test's own runtime the pin
    // would stop the test's timeout from ever being polled — the failure
    // would hang the suite instead of being reported.
    #[tokio::test(flavor = "current_thread")]
    async fn a_full_waiting_probe_queue_cannot_pin_the_actor() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let dir = d.path().to_path_buf();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|e| panic!("rt: {e}"))
                .block_on(async move {
                    let eng = spawn(
                        &dir,
                        Registry::default(),
                        ShellConfig {
                            tick_ms: 5,
                            max_in_flight: 4,
                            max_in_flight_per_ws: 1,
                            limits: fast_limits(),
                            ..Default::default()
                        },
                    )
                    .unwrap_or_else(|e| panic!("{e}"));
                    let _ = tx.send(eng);
                    std::future::pending::<()>().await; // host the actor
                });
        });
        let eng = rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("engine spawn: {e}"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        eng.put_handler(
            7,
            "hang",
            Arc::new(HangOnce {
                calls: calls.clone(),
                freeze: None,
                entered: entered.clone(),
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(8, "fast", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // ws 7's only share is held by an execute that never returns;
        // when the deadline sweep reconciles it the queued probe finds
        // the workspace full and waits in `waiting_probes`. The old
        // `while let pop` drain then pops and re-pushes that one entry
        // forever — the tick never ends, and the actor can never answer
        // another workspace again.
        let _r1 = eng
            .submit_scoped(7, OperationKey("a-0".into()), spec("hang"))
            .await
            .unwrap_or_else(|e| panic!("a-0: {e}"));
        bounded(entered.notified()).await; // the attempt is in-flight
                                           // 300 ms covers the 100 ms deadline, the sweep's probe() → queue,
                                           // and the drain tick that would spin on it.
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The actor must still answer other workspaces' commands.
        let run = bounded(eng.submit_scoped(8, OperationKey("b-0".into()), spec("fast")))
            .await
            .unwrap_or_else(|e| panic!("b-0: {e}"));
        assert!(matches!(
            bounded(until_done(&eng, run)).await,
            RunState::Done
        ));
    }

    /// S2 re-review N2: a due entry that cannot dispatch (its handler is
    /// gone, its workspace at share) must not consume the per-tick
    /// budget — otherwise dead-head entries starve every later due run.
    /// ws 7's handler was deleted after its runs were submitted; ws 8's
    /// run sits behind hundreds of them in due order and must still run.
    #[tokio::test(flavor = "current_thread")]
    async fn dead_head_due_entries_do_not_starve_dispatch() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let eng = spawn(
            d.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 10,
                max_dispatch_per_tick: 1,
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(7, "ghost", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(8, "fast", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // Dozen runs whose handler is about to vanish, due together
        // shortly — `not_before` keeps every one of them queued at the
        // head of ws 8's run in due order (lower ids win ties), so none
        // drain before the victim submits. One dead-head entry would
        // suffice under per-tick=1; twelve is margin.
        let due_at = wall_ms() + 300;
        for i in 0..12 {
            eng.submit_at_scoped(7, OperationKey(format!("g-{i}")), spec("ghost"), due_at)
                .await
                .unwrap_or_else(|e| panic!("g-{i}: {e}"));
        }
        eng.delete_handler(7, "ghost")
            .unwrap_or_else(|e| panic!("{e}"));
        let run = eng
            .submit_at_scoped(8, OperationKey("b-0".into()), spec("fast"), due_at)
            .await
            .unwrap_or_else(|e| panic!("b-0: {e}"));
        assert!(matches!(
            bounded(until_done(&eng, run)).await,
            RunState::Done
        ));
    }

    /// S2 re-review N3: the per-workspace share is charged when the
    /// dispatch is journaled, not when the task spawns — otherwise every
    /// dispatch held behind the writer's fsync passes a stale check and
    /// one workspace takes the whole permit pool in a single tick.
    #[tokio::test(flavor = "current_thread")]
    async fn the_workspace_share_is_charged_at_dispatch() {
        use std::sync::atomic::Ordering::SeqCst;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let h = Gated::default();
        let eng = spawn(
            d.path(),
            Registry::default(),
            ShellConfig {
                max_in_flight: 8,
                max_in_flight_per_ws: 2,
                max_dispatch_per_tick: 8,
                ..piped(&faults, GroupCommit::default())
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(7, "block", Arc::new(h.clone()))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        // All eight runs become due together AFTER the writer's fsync is
        // frozen: every StepDispatched waits undurable in the pipeline,
        // and under the old accounting (load rises at spawn) all eight
        // pass the workspace check — no task's spawn can raise the
        // counter while the fsync is held. Submitting without
        // `not_before` would let each run dispatch and spawn during the
        // submit loop itself, closing the race window this test needs.
        let due_at = wall_ms() + 300;
        let mut runs = Vec::new();
        for i in 0..8 {
            runs.push(
                eng.submit_at_scoped(7, OperationKey(format!("a-{i}")), spec1("block"), due_at)
                    .await
                    .unwrap_or_else(|e| panic!("a-{i}: {e}")),
            );
        }
        faults.hold_syncs();
        tokio::time::sleep(Duration::from_millis(450)).await;
        faults.release_syncs();
        // The gate stays closed: every task that spawned parks inside
        // the handler's `in_flight` count. Under charge-at-spawn all 8
        // spawn and park; under charge-at-dispatch only the share of 2
        // can ever exist. Poll for the parked plateau rather than a
        // fixed sleep — spawn scheduling under load is not the
        // measurement.
        let mut stable = 0u32;
        let mut last = 0usize;
        for _ in 0..300 {
            let now = h.calls.load(SeqCst);
            if now == last {
                stable += 1;
            } else {
                stable = 0;
                last = now;
            }
            if stable >= 15 {
                break; // parked plateau: nothing else is coming
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            h.peak.load(SeqCst) <= 2,
            "ws 7 held {} in-flight calls over its share of 2",
            h.peak.load(SeqCst)
        );
        h.gate.add_permits(8);
        for r in runs {
            assert!(matches!(bounded(until_done(&eng, r)).await, RunState::Done));
        }
    }

    /// S2 re-review N4: the index byte bound is per-workspace as well as
    /// global — one tenant's max-size runs must not put every workspace
    /// at_capacity.
    #[tokio::test(flavor = "current_thread")]
    async fn one_workspace_cannot_fill_the_whole_index() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        // Bound the whole index at about four runs: under a global-only
        // bound one workspace can hold all four shares.
        let per = Limits::run_bytes(&OperationKey("seed".into()), &spec("echo"));
        let eng = spawn(
            d.path(),
            Registry::default(),
            ShellConfig {
                tick_ms: 5,
                limits: Limits {
                    max_index_bytes: per * 4,
                    ..Limits::default()
                },
                ..Default::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(7, "echo", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        eng.put_handler(8, "echo", Arc::new(Echo))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        eng.submit_scoped(7, OperationKey("seed".into()), spec("echo"))
            .await
            .unwrap_or_else(|e| panic!("seed: {e}"));
        // ws 7 can fill its own share — but not the whole index.
        let mut refused = 0;
        for i in 0..8 {
            match eng
                .submit_scoped(7, OperationKey(format!("a-{i}")), spec("echo"))
                .await
            {
                Ok(_) => {}
                Err(EngineError::Refused(durable_domain::Refusal::WorkspaceQuota)) => refused += 1,
                Err(e) => panic!("a-{i}: {e}"),
            }
        }
        assert!(refused >= 4, "ws 7 admitted past a fair share of the index");
        eng.submit_scoped(8, OperationKey("b-0".into()), spec("echo"))
            .await
            .unwrap_or_else(|e| panic!("ws 8 must still admit while ws 7 is full: {e}"));
    }
}
