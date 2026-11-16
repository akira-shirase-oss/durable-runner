//! The Tokio shell: one actor task owns `EngineCore`; handler invocations
//! run in bounded spawned tasks and answer through the same loop. Ordering
//! guarantee: `commit` fsyncs the journal before any handler is invoked, so
//! a crash can only lose what the journal already recorded.
//!
//! Idempotency: every call for a step — attempt 1 through the last retry —
//! carries the same key `{operation_key}/{step}`, so a repeating remote call
//! can be de-duplicated on the far side (contract §04, at-least-once).

use crate::{EngineCore, EngineError};
use durable_domain::{
    Effect, JobSpec, Limits, Millis, OperationKey, Run, RunId, RunState, StepIndex, StepSpec,
};
use durable_journal::JournalOptions;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
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
    Inspect {
        run: RunId,
        reply: oneshot::Sender<Option<Run>>,
    },
    List {
        reply: oneshot::Sender<Vec<Run>>,
    },
    Diag {
        reply: oneshot::Sender<crate::Diagnostics>,
    },
    Compact {
        reply: oneshot::Sender<Result<u32, EngineError>>,
    },
    Backup {
        dest: std::path::PathBuf,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

/// Handle to the running engine. Cheap to clone; the channel capacity is
/// the submitter back-pressure bound — a full channel refuses honestly.
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Cmd>,
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

    /// Every run in the live index (bounded by what the journal holds —
    /// post-expiry index size, not history).
    pub async fn list(&self) -> Result<Vec<Run>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::List { reply })?;
        rx.await.map_err(|_| EngineError::Stopped)
    }

    /// Bounded diagnostics — counts by state kind, never rows.
    pub async fn diag(&self) -> Result<crate::Diagnostics, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Diag { reply })?;
        rx.await.map_err(|_| EngineError::Stopped)
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
        self.send(Cmd::Inspect { run, reply })?;
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
    /// Domain bounds — deadline, payload sizes, attempts. Tests shrink the
    /// step deadline so crash-recovery paths don't wait 30s.
    pub limits: Limits,
    /// Journal options (tests inject storage faults here).
    pub journal: JournalOptions,
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 32,
            max_dispatch_per_tick: 64,
            tick_ms: 50,
            cmd_capacity: 1024,
            limits: Limits::default(),
            journal: JournalOptions::default(),
        }
    }
}

/// Open the journal, replay to rebuild, then spawn the actor task. The
/// returned handle is the only public interface — there is no path to the
/// journal except through it.
pub fn spawn(dir: &Path, registry: Registry, cfg: ShellConfig) -> Result<Engine, EngineError> {
    let mut core = EngineCore::open_with(dir, cfg.journal.clone())?;
    core.limits = cfg.limits;
    let (tx, rx) = mpsc::channel::<Cmd>(cfg.cmd_capacity);
    tokio::spawn(actor(core, registry, cfg, rx));
    Ok(Engine { tx })
}

/// An attempt's result comes back with the permit it ran under, so a
/// chained step (the checkpoint that moves the run to its next step) can
/// inherit it instead of racing other runs for capacity.
type AttemptJoin = (RunId, StepIndex, Outcome, OwnedSemaphorePermit);
type ProbeJoin = (RunId, StepIndex, bool, OwnedSemaphorePermit);

async fn actor(
    mut core: EngineCore,
    registry: Registry,
    cfg: ShellConfig,
    mut rx: mpsc::Receiver<Cmd>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.tick_ms));
    let mut attempts: JoinSet<AttemptJoin> = JoinSet::new();
    let mut probes: JoinSet<ProbeJoin> = JoinSet::new();
    let mut live: HashMap<(RunId, StepIndex), AbortHandle> = HashMap::new();
    let mut waiting_probes = ProbeQueue::default();
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight));

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
        if core.is_poisoned() {
            // A failed fsync is never retried and memory may now be ahead
            // of disk: stop. Dropping the core releases the writer lock;
            // dropping `rx` (on return) wakes `Engine::closed`.
            eprintln!("durable: journal writer poisoned; engine stopping so a restart replays");
            drop(core);
            return;
        }
        tokio::select! {
            cmd = rx.recv() => match cmd {
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
                    let _ = reply.send(r);
                }
                Some(Cmd::Cancel { run, reply }) => {
                    let r = core.cancel(run, wall_ms()).map(|_| ());
                    let _ = reply.send(r);
                }
                Some(Cmd::Inspect { run, reply }) => {
                    let _ = reply.send(core.run(run).cloned());
                }
                Some(Cmd::List { reply }) => {
                    let mut all: Vec<Run> = core.runs().cloned().collect();
                    all.sort_by_key(|r| r.id.0);
                    let _ = reply.send(all);
                }
                Some(Cmd::Diag { reply }) => {
                    let _ = reply.send(core.diagnostics());
                }
                Some(Cmd::Compact { reply }) => {
                    let _ = reply.send(core.compact(wall_ms()));
                }
                Some(Cmd::Backup { dest, reply }) => {
                    let _ = reply.send(core.backup_to(&dest));
                }
                None => return,
            },
            Some(Ok((run, step, outcome, permit))) = attempts.join_next() => {
                live.remove(&(run, step));
                let (out, fail) = match outcome {
                    Outcome::Checkpoint(o) => (Some(o), None),
                    Outcome::Failed => (None, Some("failed".to_string())),
                    Outcome::Lost => (None, None),
                };
                let effects = core.step_finished(run, step, out, fail, wall_ms())
                    .unwrap_or_default();
                let mut ex = exec!();
                run_effects(&mut core, &mut ex, effects, run, Some(permit));
            }
            Some(Ok((run, step, found, permit))) = probes.join_next() => {
                let effects = core.reconciled(run, step, found, wall_ms()).unwrap_or_default();
                let mut ex = exec!();
                run_effects(&mut core, &mut ex, effects, run, Some(permit));
            }
            _ = ticker.tick() => {
                let mut ex = exec!();
                tick(&mut core, &mut ex);
            }
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
}

impl Exec<'_> {
    /// Invoke a step's handler for the run's current attempt under
    /// `permit`. The journal commit that made it `Running` already
    /// happened, and it happened only because this permit was held.
    fn fire(
        &mut self,
        core: &EngineCore,
        run: RunId,
        step: StepIndex,
        permit: OwnedSemaphorePermit,
    ) {
        let Some(r) = core.run(run) else { return };
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
        let effects = core
            .step_finished(run, step, None, Some(why.to_string()), wall_ms())
            .unwrap_or_default();
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

    fn probe_with(
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
            let effects = core
                .reconciled(run, step, false, wall_ms())
                .unwrap_or_default();
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
    // Deadline sweep: an attempt out past its deadline is a lost reply —
    // it reconciles, never silently repeats. `live` only tracks attempts
    // this process fired; a run orphaned by a restart has no entry, and
    // gating on it would strand every crash-surviving Running run.
    for (run, step) in core.expired_attempts(now).into_iter().take(exec.per_tick) {
        if let Some(call) = exec.live.remove(&(run, step)) {
            call.abort(); // frees its permit; the remote effect is reconciled
        }
        let effects = core
            .step_finished(run, step, None, None, now)
            .unwrap_or_default();
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
        if core.dispatch(run, step, attempt, now).is_err() {
            continue; // a stale claim — the run moved on; the permit returns
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
        eng.list()
            .await
            .unwrap_or_default()
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
        assert!(eng.list().await.unwrap_or_default().is_empty());
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
        let mut ex = Exec {
            registry: &reg,
            sem: sem.clone(),
            attempts: &mut attempts,
            probes: &mut probes,
            live: &mut live,
            waiting_probes: &mut waiting,
            per_tick: 3,
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
            matches!(b, Err(EngineError::Journal(JournalError::SyncFailed(_)))),
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
