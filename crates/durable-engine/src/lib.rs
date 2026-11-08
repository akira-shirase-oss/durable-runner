//! The engine binds `durable-domain`'s pure reducer to `durable-journal`'s
//! append-only log. The rule that makes it durable is ordering: every state
//! transition is appended (and fsynced) *before* it is acknowledged or acted
//! on. The in-memory index is rebuilt by replay at open and is never
//! authority.
//!
//! `EngineCore` is synchronous and clock-injected so tests are deterministic;
//! the async dispatcher (D2's Tokio shell) wraps it.

use durable_domain::{
    reduce, Effect, Event, JobSpec, Limits, Millis, OperationKey, Refusal, RetryPolicy, Run, RunId,
    RunState, StepIndex,
};
use durable_journal::{Journal, JournalError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("refused: {0}")]
    Refused(#[from] Refusal),
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
}

/// A journaled record: which run the event belongs to, plus the event.
/// Sequencing is the journal's job; the payload is domain data only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub run: u64,
    pub event: Event,
}

/// The whole runtime index, rebuilt only by replaying the journal through
/// the reducer — the same fold the live path performs.
pub struct EngineCore {
    journal: Journal,
    runs: HashMap<RunId, Run>,
    by_op: HashMap<OperationKey, RunId>,
    next_run: u64,
    pub limits: Limits,
    pub retry: RetryPolicy,
}

impl EngineCore {
    /// Open the journal at `dir` and rebuild every run from committed
    /// history. Corrupt history fails here — startup never proceeds past it.
    pub fn open(dir: &Path) -> Result<Self, EngineError> {
        let journal = Journal::open(dir)?;
        let records =
            durable_journal::replay_dir(&durable_journal::gen_dir_path(dir, journal.generation()))?;
        let mut core = Self {
            journal,
            runs: HashMap::new(),
            by_op: HashMap::new(),
            next_run: 1,
            limits: Limits::default(),
            retry: RetryPolicy::default(),
        };
        let mut max_run = 0u64;
        for r in records {
            let env: Envelope = serde_json::from_slice(&r.payload)?;
            max_run = max_run.max(env.run);
            core.apply_committed(RunId(env.run), env.event, 0)?;
        }
        core.next_run = max_run + 1;
        Ok(core)
    }

    fn apply_committed(
        &mut self,
        run_id: RunId,
        event: Event,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        // Acceptance constructs the run; every later event reduces onto it.
        if let Event::Accepted {
            key,
            spec,
            not_before,
        } = event
        {
            if self.runs.contains_key(&run_id) {
                return Err(Refusal::DuplicateKey.into());
            }
            self.by_op.insert(key.clone(), run_id);
            self.runs
                .insert(run_id, Run::accepted(run_id, key, spec, not_before));
            return Ok(vec![]);
        }
        let mut run = self.runs.remove(&run_id).ok_or(Refusal::StaleReply)?;
        let (state, effects) = reduce(&run, &event, now, &self.limits, &self.retry)?;
        run.state = state;
        self.runs.insert(run_id, run);
        Ok(effects)
    }

    /// Journal first, then fold — the only path by which state may change.
    fn commit(
        &mut self,
        run: RunId,
        event: Event,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        let payload = serde_json::to_vec(&Envelope {
            run: run.0,
            event: event.clone(),
        })?;
        self.journal.append(&payload)?; // durable BEFORE applied below
        self.apply_committed(run, event, now)
    }

    /// Submit a job under an operation key. A repeated key returns the
    /// original run id without touching the journal — the dedup contract.
    pub fn submit(
        &mut self,
        key: OperationKey,
        spec: JobSpec,
        now: Millis,
    ) -> Result<RunId, EngineError> {
        if let Some(existing) = self.by_op.get(&key) {
            return Ok(*existing); // duplicate: hand back the original run
        }
        self.limits.admits(&spec)?; // limits refuse before anything is written
        let run = RunId(self.next_run);
        self.next_run += 1;
        self.commit(
            run,
            Event::Accepted {
                key,
                spec,
                not_before: now,
            },
            now,
        )?;
        Ok(run)
    }

    /// Every run whose wait has expired at `now` — the dispatch candidates
    /// as (run, step, next attempt).
    pub fn due(&self, now: Millis) -> Vec<(RunId, StepIndex, u32)> {
        self.runs
            .iter()
            .filter_map(|(id, r)| match &r.state {
                RunState::Queued { next_run_at } if *next_run_at <= now => {
                    Some((*id, StepIndex(0), 1))
                }
                RunState::RetryWaiting {
                    step,
                    attempt,
                    next_run_at,
                } if *next_run_at <= now => Some((*id, *step, *attempt + 1)),
                _ => None,
            })
            .collect()
    }

    /// Commit a dispatch: record StepDispatched, then the caller may invoke
    /// the handler — never before this returns Ok.
    pub fn dispatch(
        &mut self,
        run: RunId,
        step: StepIndex,
        attempt: u32,
        now: Millis,
    ) -> Result<Millis, EngineError> {
        let deadline = now + self.limits.max_step_deadline_ms;
        self.commit(
            run,
            Event::StepDispatched {
                step,
                attempt,
                deadline,
            },
            now,
        )?;
        Ok(deadline)
    }

    /// A handler's result arrives — checkpoint or schedule the retry.
    pub fn step_finished(
        &mut self,
        run: RunId,
        step: StepIndex,
        output: Option<Vec<u8>>,
        failure: Option<String>,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        let event = match (output, failure) {
            (Some(out), _) => Event::StepCheckpointed { step, output: out },
            (None, Some(_)) => Event::StepFailed { step },
            (None, None) => Event::ReplyLost { step }, // vanished reply → reconcile
        };
        self.commit(run, event, now)
    }

    /// Reconciliation verdict after a lost reply: `found` = the remote call
    /// landed (advance, never repeat); `false` = safe to retry.
    pub fn reconciled(
        &mut self,
        run: RunId,
        step: StepIndex,
        found: bool,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        self.commit(
            run,
            Event::Reconciled {
                step,
                effect_found: found,
            },
            now,
        )
    }

    /// Cancellation is an honest transition, not an undo.
    pub fn cancel(&mut self, run: RunId, now: Millis) -> Result<Vec<Effect>, EngineError> {
        self.commit(run, Event::CancelRequested, now)
    }

    pub fn run(&self, id: RunId) -> Option<&Run> {
        self.runs.get(&id)
    }

    /// Millisecond clock values the dispatcher should re-arm for.
    pub fn next_wake(&self) -> Option<Millis> {
        self.runs
            .values()
            .filter_map(|r| match &r.state {
                RunState::Queued { next_run_at } | RunState::RetryWaiting { next_run_at, .. } => {
                    Some(*next_run_at)
                }
                _ => None,
            })
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use durable_domain::StepSpec;

    fn spec() -> JobSpec {
        JobSpec {
            version: 1,
            input: b"job".to_vec(),
            steps: vec![
                StepSpec {
                    name: "a".into(),
                    handler: "noop".into(),
                },
                StepSpec {
                    name: "b".into(),
                    handler: "noop".into(),
                },
            ],
        }
    }

    fn open() -> (tempfile::TempDir, EngineCore) {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        (d, e)
    }

    #[test]
    fn accepted_survives_reopen() {
        let (d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Queued { .. }
        ));
    }

    #[test]
    fn duplicate_key_returns_original_run() {
        let (_d, mut e) = open();
        let a = e
            .submit(OperationKey("same".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let b = e
            .submit(OperationKey("same".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(a, b); // duplicate key: same run, no second record
        let c = e
            .submit(OperationKey("other".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(a, c);
    }

    #[test]
    fn full_lifecycle_replays_identically() {
        let (d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let (step0, _) = {
            let (id, s, a) = e.due(0)[0];
            assert_eq!(id, run);
            (s, a)
        };
        e.dispatch(run, step0, 1, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.step_finished(run, step0, Some(b"out".to_vec()), None, 10)
            .unwrap_or_else(|e| panic!("{e}"));
        // The checkpoint record already moved the run to Running{step:1} —
        // that commit IS the next step's durable dispatch. The handler reply
        // for step 1 is what arrives next.
        e.step_finished(run, StepIndex(1), Some(b"done".to_vec()), None, 20)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Done
        ));
    }

    #[test]
    fn crash_between_dispatch_and_reply_resumes_running_then_recovers() {
        let (d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.dispatch(run, StepIndex(0), 1, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e); // "crash": handler reply never arrived
        let mut e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        // Run sits Running past its deadline; a deadline sweep turns it into
        // a lost reply and reconciles before any repeat dispatch.
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Running { .. }
        ));
        e2.step_finished(run, StepIndex(0), None, None, 60_000)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Reconciling { .. }
        ));
        e2.reconciled(run, StepIndex(0), false, 60_000)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::RetryWaiting { .. }
        ));
    }
}
