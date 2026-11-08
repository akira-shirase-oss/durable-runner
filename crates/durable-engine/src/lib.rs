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

pub mod shell;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("refused: {0}")]
    Refused(#[from] Refusal),
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("engine stopped")]
    Stopped,
    #[error("unregistered handler: {0}")]
    UnregisteredHandler(String),
}

/// A journaled record: the run, the commit clock value, and the event.
/// `at` makes replay faithful — deadlines and expiry reproduce the time
/// the transition actually happened, not the time it was replayed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub run: u64,
    pub at: Millis,
    pub event: Event,
}

/// The serialized in-memory index — what a compaction snapshot carries.
/// Live runs, their dedup keys, and the next id; history is not preserved
/// past a generation boundary — the old generation's journal keeps it.
#[derive(Debug, Serialize, Deserialize)]
struct IndexSnapshot {
    runs: HashMap<RunId, Run>,
    by_op: HashMap<OperationKey, RunId>,
    next_run: u64,
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

/// Terminal runs expire from the live index after this much time — the
/// journal's older generations still hold their history (contract §05).
const TERMINAL_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1000;

impl EngineCore {
    /// Open the journal at `dir` and rebuild every run: snapshot first if
    /// the current generation has one, then this generation's records.
    /// Corrupt history fails here — startup never proceeds past it.
    pub fn open(dir: &Path) -> Result<Self, EngineError> {
        let journal = Journal::open(dir)?;
        let gdir = durable_journal::gen_dir_path(dir, journal.generation());
        let mut core = Self {
            journal,
            runs: HashMap::new(),
            by_op: HashMap::new(),
            next_run: 1,
            limits: Limits::default(),
            retry: RetryPolicy::default(),
        };
        if let Some(snap) = durable_journal::load_snapshot(&gdir)? {
            let idx: IndexSnapshot = serde_json::from_slice(&snap.payload)?;
            core.runs = idx.runs;
            core.by_op = idx.by_op;
            core.next_run = idx.next_run;
        }
        let mut max_run = core.next_run.saturating_sub(1);
        for r in durable_journal::replay_dir(&gdir)? {
            let env: Envelope = serde_json::from_slice(&r.payload)?;
            max_run = max_run.max(env.run);
            core.apply_committed(RunId(env.run), env.event, env.at)?;
        }
        core.next_run = max_run + 1;
        Ok(core)
    }

    /// Consistent backup: generation data copied first, manifest last —
    /// a partial copy opens as a complete older generation.
    pub fn backup_to(&self, dest: &Path) -> Result<(), EngineError> {
        self.journal.backup_to(dest)?;
        Ok(())
    }

    /// Compact: expire 7-day-old terminal runs, snapshot the live index
    /// into a new generation, atomically swap the manifest. Returns the
    /// new generation number.
    pub fn compact(&mut self, now: Millis) -> Result<u32, EngineError> {
        self.runs.retain(|_, r| {
            !(r.state.is_terminal()
                && r.finished_at
                    .is_some_and(|t| now.saturating_sub(t) > TERMINAL_EXPIRY_MS))
        });
        // Dedup keys live only as long as their run — no orphan entries.
        self.by_op.retain(|_, id| self.runs.contains_key(id));
        let payload = serde_json::to_vec(&IndexSnapshot {
            runs: self.runs.clone(),
            by_op: self.by_op.clone(),
            next_run: self.next_run,
        })?;
        self.journal.compact(&durable_journal::Snapshot {
            covered_seq: 0,
            payload,
        })?;
        Ok(self.journal.generation())
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
        let run = self.runs.get(&run_id).ok_or(Refusal::StaleReply)?;
        let (state, effects) = reduce(run, &event, now, &self.limits, &self.retry)?;
        let run = self.runs.get_mut(&run_id).ok_or(Refusal::StaleReply)?;
        if state.is_terminal() && run.finished_at.is_none() {
            run.finished_at = Some(now); // the 7-day expiry clock starts
        }
        run.state = state;
        Ok(effects)
    }

    /// Journal first, then fold — the only path by which state may change.
    /// Trial-reduce before appending: a refused event must never reach the
    /// journal, because every committed record has to apply on replay.
    fn commit(
        &mut self,
        run: RunId,
        event: Event,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        if !matches!(event, Event::Accepted { .. }) {
            let run = self.runs.get(&run).ok_or(Refusal::StaleReply)?;
            reduce(run, &event, now, &self.limits, &self.retry)?;
        }
        let payload = serde_json::to_vec(&Envelope {
            run: run.0,
            at: now,
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
        self.submit_at(key, spec, now, now)
    }

    /// Submit with a delayed start: the run queues until `not_before`.
    /// Same durability and dedup contract as `submit`.
    pub fn submit_at(
        &mut self,
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
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
                not_before,
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

    /// Iterate the live index (used by the list surface; bounded by the
    /// index, never by history).
    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.values()
    }

    /// Attempts whose deadline passed at `now` — each is a lost reply until
    /// a reconciler proves otherwise.
    pub fn expired_attempts(&self, now: Millis) -> Vec<(RunId, StepIndex)> {
        self.runs
            .iter()
            .filter_map(|(id, r)| match &r.state {
                RunState::Running { step, deadline, .. } if *deadline < now => Some((*id, *step)),
                _ => None,
            })
            .collect()
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

    #[test]
    fn compaction_keeps_live_state_across_reopen() {
        let (d, mut e) = open();
        let live = e
            .submit(OperationKey("live".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let done = e
            .submit(OperationKey("done".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.cancel(done, 5).unwrap_or_else(|e| panic!("{e}"));
        // At 5ms everything is fresh — compaction preserves all of it.
        e.compact(10).unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let mut e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(e2.run(live).is_some());
        assert!(e2.run(done).is_some());
        // dedup survived compaction: resubmitting returns the same run
        let again = e2
            .submit(OperationKey("live".into()), spec(), 20)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again, live);
    }

    #[test]
    fn expired_terminal_runs_leave_the_index() {
        let (d, mut e) = open();
        let done = e
            .submit(OperationKey("old".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.cancel(done, 1).unwrap_or_else(|e| panic!("{e}"));
        // 8 days later: past the 7-day expiry — the run leaves the index
        // and its operation key is free to be used again.
        let eight_days = 8 * 24 * 60 * 60 * 1000;
        e.compact(eight_days).unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let mut e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(e2.run(done).is_none(), "expired terminal run must expire");
        let fresh = e2
            .submit(OperationKey("old".into()), spec(), eight_days)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(fresh, done);
    }

    #[test]
    fn refused_event_neither_journals_nor_loses_the_run() {
        let (d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        // StepFailed on a run that was never dispatched: refused. Critically,
        // the refusal must stay out of the journal (every committed record
        // has to apply on replay) and must not drop the run from the index.
        let err = e
            .step_finished(run, StepIndex(0), None, Some("x".into()), 1)
            .expect_err("a reply for a never-dispatched step is refused");
        assert!(matches!(err, EngineError::Refused(Refusal::StaleReply)));
        assert!(
            e.run(run).is_some(),
            "a refused event must not lose the run"
        );
        drop(e);
        let e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Queued { .. }
        ));
    }

    #[test]
    fn backup_then_open_recovers_everything() {
        let (_d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.cancel(run, 1).unwrap_or_else(|e| panic!("{e}"));
        let dest = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        e.backup_to(dest.path()).unwrap_or_else(|e| panic!("{e}"));
        drop(e); // release the writer lock on the source
        let e2 = EngineCore::open(dest.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Cancelled
        ));
    }
}
