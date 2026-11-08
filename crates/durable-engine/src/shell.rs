//! The Tokio shell: one actor task owns `EngineCore`; handler invocations
//! run in bounded spawned tasks and answer through the same loop. Ordering
//! guarantee: `commit` fsyncs the journal before any handler is invoked, so
//! a crash can only lose what the journal already recorded.
//!
//! Idempotency: every call for a step — attempt 1 through the last retry —
//! carries the same key `{operation_key}/{step}`, so a repeating remote call
//! can be de-duplicated on the far side (contract §04, at-least-once).

use crate::{EngineCore, EngineError};
use durable_domain::{Effect, JobSpec, Millis, OperationKey, Run, RunId, StepIndex, StepSpec};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinSet;

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
}

/// Handle to the running engine. Cheap to clone; the channel capacity is
/// the submitter back-pressure bound — a full channel refuses honestly.
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Cmd>,
}

impl Engine {
    pub async fn submit(&self, key: OperationKey, spec: JobSpec) -> Result<RunId, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(Cmd::Submit { key, spec, reply })
            .map_err(|_| EngineError::Refused(durable_domain::Refusal::ReserveExhausted))?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    pub async fn cancel(&self, run: RunId) -> Result<(), EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(Cmd::Cancel { run, reply })
            .map_err(|_| EngineError::Refused(durable_domain::Refusal::ReserveExhausted))?;
        rx.await.map_err(|_| EngineError::Stopped)?
    }

    pub async fn inspect(&self, run: RunId) -> Result<Option<Run>, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(Cmd::Inspect { run, reply })
            .map_err(|_| EngineError::Refused(durable_domain::Refusal::ReserveExhausted))?;
        rx.await.map_err(|_| EngineError::Stopped)
    }
}

/// Runtime knobs, all bounded.
#[derive(Debug, Clone)]
pub struct ShellConfig {
    /// Maximum handler attempts in flight at once.
    pub max_in_flight: usize,
    /// Scheduler/deadline-sweep cadence.
    pub tick_ms: u64,
    /// Command channel capacity (submit back-pressure bound).
    pub cmd_capacity: usize,
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 32,
            tick_ms: 50,
            cmd_capacity: 1024,
        }
    }
}

/// Open the journal, replay to rebuild, then spawn the actor task. The
/// returned handle is the only public interface — there is no path to the
/// journal except through it.
pub fn spawn(dir: &Path, registry: Registry, cfg: ShellConfig) -> Result<Engine, EngineError> {
    let core = EngineCore::open(dir)?;
    let (tx, rx) = mpsc::channel::<Cmd>(cfg.cmd_capacity);
    tokio::spawn(actor(core, registry, cfg, rx));
    Ok(Engine { tx })
}

type AttemptJoin = (RunId, StepIndex, Outcome);
type ProbeJoin = (RunId, StepIndex, bool);

async fn actor(
    mut core: EngineCore,
    registry: Registry,
    cfg: ShellConfig,
    mut rx: mpsc::Receiver<Cmd>,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.tick_ms));
    let mut attempts: JoinSet<AttemptJoin> = JoinSet::new();
    let mut probes: JoinSet<ProbeJoin> = JoinSet::new();
    let mut live: HashSet<(RunId, StepIndex)> = HashSet::new();
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight));

    macro_rules! exec {
        () => {
            Exec {
                registry: &registry,
                sem: sem.clone(),
                attempts: &mut attempts,
                probes: &mut probes,
                live: &mut live,
            }
        };
    }

    loop {
        tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(Cmd::Submit { key, spec, reply }) => {
                    let _ = reply.send(core.submit(key, spec, wall_ms()));
                }
                Some(Cmd::Cancel { run, reply }) => {
                    let r = core.cancel(run, wall_ms()).map(|_| ());
                    let _ = reply.send(r);
                }
                Some(Cmd::Inspect { run, reply }) => {
                    let _ = reply.send(core.run(run).cloned());
                }
                None => return,
            },
            Some(Ok((run, step, outcome))) = attempts.join_next() => {
                live.remove(&(run, step));
                let (out, fail) = match outcome {
                    Outcome::Checkpoint(o) => (Some(o), None),
                    Outcome::Failed => (None, Some("failed".to_string())),
                    Outcome::Lost => (None, None),
                };
                let effects = core.step_finished(run, step, out, fail, wall_ms())
                    .unwrap_or_default();
                let mut ex = exec!();
                run_effects(&mut core, &mut ex, effects, run);
            }
            Some(Ok((run, step, found))) = probes.join_next() => {
                let effects = core.reconciled(run, step, found, wall_ms()).unwrap_or_default();
                let mut ex = exec!();
                run_effects(&mut core, &mut ex, effects, run);
            }
            _ = ticker.tick() => {
                let mut ex = exec!();
                tick(&mut core, &mut ex);
            }
        }
    }
}

/// The executor's plumbing, bundled so helpers stay under the arg limit.
struct Exec<'a> {
    registry: &'a Registry,
    sem: Arc<Semaphore>,
    attempts: &'a mut JoinSet<AttemptJoin>,
    probes: &'a mut JoinSet<ProbeJoin>,
    live: &'a mut HashSet<(RunId, StepIndex)>,
}

impl Exec<'_> {
    /// Invoke a step's handler for the run's current attempt. The journal
    /// commit that made it `Running` already happened — this is execution.
    fn fire(&mut self, core: &EngineCore, run: RunId, step: StepIndex) {
        let Ok(permit) = self.sem.clone().try_acquire_owned() else {
            return; // at capacity — the next tick retries the dispatch
        };
        let Some(r) = core.run(run) else { return };
        let spec = r.spec.clone();
        let name = spec
            .steps
            .get(step.0 as usize)
            .map(|s| s.handler.clone())
            .unwrap_or_default();
        let Some(handler) = self.registry.get(&name) else {
            return;
        };
        let Some(sspec) = spec.steps.get(step.0 as usize).cloned() else {
            return;
        };
        let idem = idem_key(&r.key, step);
        let input = spec.input.clone();
        self.live.insert((run, step));
        self.attempts.spawn(async move {
            let _p = permit;
            let outcome = handler.execute(&sspec, &input, &idem).await;
            (run, step, outcome)
        });
    }

    /// Fail a step's attempt (journaled; retry/terminal decided by domain).
    fn fail(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex, why: &str) {
        let effects = core
            .step_finished(run, step, None, Some(why.to_string()), wall_ms())
            .unwrap_or_default();
        run_effects(core, self, effects, run);
    }

    /// Probe for a reconciling step; the verdict returns through the loop.
    fn probe(&mut self, core: &mut EngineCore, run: RunId, step: StepIndex) {
        let Some(r) = core.run(run) else { return };
        let name = r
            .spec
            .steps
            .get(step.0 as usize)
            .map(|s| s.handler.clone())
            .unwrap_or_default();
        let Some(handler) = self.registry.get(&name) else {
            // Cannot probe an unregistered handler — reconcile as not-found
            // rather than leave the run stuck.
            let effects = core
                .reconciled(run, step, false, wall_ms())
                .unwrap_or_default();
            run_effects(core, self, effects, run);
            return;
        };
        let idem = idem_key(&r.key, step);
        let sspec = r.spec.steps[step.0 as usize].clone();
        self.probes.spawn(async move {
            let found = handler.probe(&sspec, &idem).await;
            (run, step, found)
        });
    }
}

/// The periodic pass: sweep expired attempts, then dispatch what's due.
/// Bounded in flight — a full semaphore leaves work queued, honestly.
fn tick(core: &mut EngineCore, exec: &mut Exec<'_>) {
    let now = wall_ms();
    // Deadline sweep: an attempt out past its deadline is a lost reply —
    // it reconciles, never silently repeats.
    for (run, step) in core.expired_attempts(now) {
        if exec.live.remove(&(run, step)) {
            let effects = core
                .step_finished(run, step, None, None, now)
                .unwrap_or_default();
            run_effects(core, exec, effects, run);
        }
    }
    // Dispatch: the journal commits StepDispatched BEFORE the handler is
    // invoked — a crash between the two can only lose a journaled fact.
    for (run, step, attempt) in core.due(now) {
        let Some(name) = core
            .run(run)
            .and_then(|r| r.spec.steps.get(step.0 as usize))
            .map(|s| s.handler.clone())
        else {
            continue;
        };
        if !exec.registry.contains(&name) {
            // Unregistered handler: a real failure, journaled and retried
            // like any other — it will exhaust if never fixed.
            exec.fail(core, run, step, "unregistered handler");
            continue;
        }
        if core.dispatch(run, step, attempt, now).is_err() {
            continue; // a stale claim — the run moved on
        }
        exec.fire(core, run, step);
    }
}

fn run_effects(core: &mut EngineCore, exec: &mut Exec<'_>, effects: Vec<Effect>, run: RunId) {
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
                if exec.registry.contains(&name) {
                    exec.fire(core, run, step);
                } else {
                    exec.fail(core, run, step, "unregistered handler");
                }
            }
            Effect::Reconcile { step } => exec.probe(core, run, step),
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
    async fn full_channel_backpressures() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let reg = Registry::default();
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
