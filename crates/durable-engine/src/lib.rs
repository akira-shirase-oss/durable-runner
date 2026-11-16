//! The engine binds `durable-domain`'s pure reducer to `durable-journal`'s
//! append-only log. The rule that makes it durable is ordering: every state
//! transition is appended (and fsynced) *before* it is acknowledged or acted
//! on. The in-memory index is rebuilt by replay at open and is never
//! authority.
//!
//! `EngineCore` is synchronous and clock-injected so tests are deterministic;
//! the async dispatcher (D2's Tokio shell) wraps it.
//!
//! Two log modes. **Direct** (startup replay, tools, core tests): `commit`
//! appends and fsyncs before it returns. **Piped** (the running shell):
//! `commit` applies the record in memory and queues it for the group-commit
//! writer thread (`writer`); the shell releases everything the record
//! decided — acks, read answers, handler calls — only after the writer
//! reports the covering fsync (D-014, D-015).

use durable_domain::{
    reduce, Effect, Event, JobSpec, Limits, Millis, OperationKey, Refusal, RetryPolicy, Run, RunId,
    RunState, StepIndex,
};
use durable_journal::{Journal, JournalError, JournalOptions, SpaceProbe};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

pub mod shell;
pub mod writer;

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
    /// The group commit holding this request's record failed. Nothing
    /// decided on the strength of it left the process; the engine stops
    /// and restart replay decides what is durable.
    #[error("journal commit failed; nothing was acknowledged: {0}")]
    CommitFailed(Arc<JournalError>),
    /// A direct-mode operation was called while the writer thread owns the
    /// journal (a programming error, never a client one).
    #[error("operation unavailable while the group-commit writer owns the journal")]
    Piped,
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

/// Bounded diagnostics: aggregate counts only — safe to answer under load.
#[derive(Debug, Clone, Serialize)]
pub struct Diagnostics {
    pub generation: u32,
    pub records_written: u64,
    pub live_runs: u64,
    pub dedup_keys: u64,
    pub states: std::collections::BTreeMap<&'static str, u64>,
    /// Whether the journal filesystem has its reserve free.
    pub space_ok: bool,
    /// Commit failures the runtime handled instead of answering a caller
    /// (a handler outcome that could not be journaled, a stale reply),
    /// by kind — counted, never silently dropped.
    pub errors: std::collections::BTreeMap<&'static str, u64>,
    /// Group commits written (one write + one fsync each) and the largest.
    pub journal_batches: u64,
    pub largest_batch: u64,
    /// Superseded generations still on disk; retention deletes them after
    /// each successful compaction, so this stays 0 unless a delete failed.
    pub stale_generations: u32,
}

/// The serialized in-memory index — what a compaction snapshot carries.
/// Live runs, their dedup keys, and the next id. Superseded generations are
/// deleted after a compaction (retention), so this snapshot plus the new
/// generation's records are the whole durable state.
#[derive(Debug, Serialize, Deserialize)]
struct IndexSnapshot {
    // Ordered by id so listing can page; serialises exactly like the
    // HashMap it replaced (a JSON object keyed by id), so old snapshots load.
    runs: BTreeMap<RunId, Run>,
    by_op: HashMap<OperationKey, RunId>,
    next_run: u64,
}

/// Where commits go: straight to disk, or to the group-commit writer.
enum Log {
    Direct(Journal),
    Piped(Pipe),
}

/// Piped mode's side of the journal: records applied in memory and waiting
/// to be handed to the writer, plus the writer's last reported state.
struct Pipe {
    pending: VecDeque<Vec<u8>>,
    /// Commit index (this process) of the newest queued record.
    last_index: u64,
    root: PathBuf,
    space: SpaceProbe,
    reserve: u64,
    status: writer::WriterStatus,
}

impl Pipe {
    /// The same admission rule as `Journal::append`, checked when the
    /// record is queued: it must fit with the reserve still free.
    fn enqueue(&mut self, payload: Vec<u8>) -> Result<(), JournalError> {
        if self.status.failed.is_some() {
            return Err(JournalError::Poisoned);
        }
        let available = self.space.available(&self.root)?;
        let need = 18 + payload.len() as u64; // frame header + payload
        if available < self.reserve.saturating_add(need) {
            return Err(JournalError::NoSpace {
                available,
                reserve: self.reserve,
            });
        }
        self.pending.push_back(payload);
        self.last_index += 1;
        Ok(())
    }

    fn space_ok(&self) -> bool {
        self.space
            .available(&self.root)
            .is_ok_and(|a| a >= self.reserve)
    }
}

/// A compaction worked out against the live index but not yet applied:
/// the snapshot for the new generation and the runs it expires.
pub struct CompactionPlan {
    pub snapshot: durable_journal::Snapshot,
    pub expired: Vec<RunId>,
}

/// The whole runtime index, rebuilt only by replaying the journal through
/// the reducer — the same fold the live path performs.
pub struct EngineCore {
    log: Log,
    runs: BTreeMap<RunId, Run>,
    by_op: HashMap<OperationKey, RunId>,
    next_run: u64,
    pub limits: Limits,
    pub retry: RetryPolicy,
}

/// Terminal runs expire from the index after this much time (contract §05);
/// the compaction that expires them also deletes the superseded generations
/// that held their records.
const TERMINAL_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1000;

impl EngineCore {
    /// Open the journal at `dir` and rebuild every run: snapshot first if
    /// the current generation has one, then this generation's records.
    /// Corrupt history fails here — startup never proceeds past it.
    pub fn open(dir: &Path) -> Result<Self, EngineError> {
        Self::open_with(dir, JournalOptions::default())
    }

    /// `open` with explicit journal options.
    pub fn open_with(dir: &Path, opts: JournalOptions) -> Result<Self, EngineError> {
        let journal = Journal::open_with(dir, opts)?;
        let gdir = durable_journal::gen_dir_path(dir, journal.generation());
        let mut core = Self {
            log: Log::Direct(journal),
            runs: BTreeMap::new(),
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

    /// Hand the journal to a group-commit writer: from now on commits are
    /// queued (`take_batch`) instead of written. Returns the journal for
    /// the writer thread to own.
    pub fn into_piped(mut self) -> (Self, Option<Journal>) {
        let log = std::mem::replace(
            &mut self.log,
            Log::Piped(Pipe {
                pending: VecDeque::new(),
                last_index: 0,
                root: PathBuf::new(),
                space: SpaceProbe::default(),
                reserve: 0,
                status: writer::WriterStatus::default(),
            }),
        );
        match log {
            Log::Direct(journal) => {
                if let Log::Piped(p) = &mut self.log {
                    p.root = journal.dir().to_path_buf();
                    p.space = journal.options().space.clone();
                    p.reserve = journal.options().reserve_bytes;
                    p.status.generation = journal.generation();
                    p.status.records_written = journal.records_written();
                }
                (self, Some(journal))
            }
            piped @ Log::Piped(_) => {
                self.log = piped;
                (self, None)
            }
        }
    }

    /// Commit index of the newest record (piped mode; 0 in direct mode,
    /// where every commit is durable when `commit` returns).
    pub fn last_index(&self) -> u64 {
        match &self.log {
            Log::Direct(_) => 0,
            Log::Piped(p) => p.last_index,
        }
    }

    /// Records queued for the writer and not yet handed over.
    pub fn pending_records(&self) -> usize {
        match &self.log {
            Log::Direct(_) => 0,
            Log::Piped(p) => p.pending.len(),
        }
    }

    /// Up to `max` queued records, oldest first, with the commit index of
    /// the last one — the unit handed to the writer.
    pub fn take_batch(&mut self, max: usize) -> Option<(Vec<Vec<u8>>, u64)> {
        let Log::Piped(p) = &mut self.log else {
            return None;
        };
        if p.pending.is_empty() {
            return None;
        }
        let n = max.max(1).min(p.pending.len());
        let records: Vec<Vec<u8>> = p.pending.drain(..n).collect();
        let upto = p.last_index - p.pending.len() as u64;
        Some((records, upto))
    }

    /// Take in the writer's latest report (generation, counts, failure).
    pub fn observe(&mut self, status: &writer::WriterStatus) {
        if let Log::Piped(p) = &mut self.log {
            p.status = status.clone();
        }
    }

    /// Consistent backup: generation data copied first, manifest last —
    /// a partial copy opens as a complete older generation. Direct mode
    /// only; the shell backs up through the writer.
    pub fn backup_to(&self, dest: &Path) -> Result<(), EngineError> {
        match &self.log {
            Log::Direct(j) => Ok(j.backup_to(dest)?),
            Log::Piped(_) => Err(EngineError::Piped),
        }
    }

    /// Work out a compaction: expire 7-day-old terminal runs and snapshot
    /// the rest. Nothing changes until `finish_compaction`, which the
    /// caller runs only once the new generation is durable — so a refused
    /// compaction (`NoSpace`) leaves memory and disk agreeing.
    pub fn plan_compaction(&self, now: Millis) -> Result<CompactionPlan, EngineError> {
        let expired: Vec<RunId> = self
            .runs
            .values()
            .filter(|r| {
                r.state.is_terminal()
                    && r.finished_at
                        .is_some_and(|t| now.saturating_sub(t) > TERMINAL_EXPIRY_MS)
            })
            .map(|r| r.id)
            .collect();
        let runs: BTreeMap<RunId, Run> = self
            .runs
            .iter()
            .filter(|(id, _)| !expired.contains(id))
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        // Dedup keys live only as long as their run — no orphan entries.
        let by_op: HashMap<OperationKey, RunId> = self
            .by_op
            .iter()
            .filter(|(_, id)| runs.contains_key(id))
            .map(|(k, id)| (k.clone(), *id))
            .collect();
        let payload = serde_json::to_vec(&IndexSnapshot {
            runs,
            by_op,
            next_run: self.next_run,
        })?;
        Ok(CompactionPlan {
            snapshot: durable_journal::Snapshot {
                covered_seq: 0,
                payload,
            },
            expired,
        })
    }

    /// Apply a durable compaction's expiry to the live index.
    pub fn finish_compaction(&mut self, expired: &[RunId]) {
        for id in expired {
            if let Some(run) = self.runs.remove(id) {
                if self.by_op.get(&run.key) == Some(id) {
                    self.by_op.remove(&run.key);
                }
            }
        }
    }

    /// Compact (direct mode): expire 7-day-old terminal runs, snapshot the
    /// live index into a new generation, atomically swap the manifest.
    /// Returns the new generation number. All or nothing.
    pub fn compact(&mut self, now: Millis) -> Result<u32, EngineError> {
        let plan = self.plan_compaction(now)?;
        let Log::Direct(journal) = &mut self.log else {
            return Err(EngineError::Piped);
        };
        journal.compact(&plan.snapshot)?;
        let generation = journal.generation();
        self.finish_compaction(&plan.expired);
        Ok(generation)
    }

    /// Whether the journal filesystem has its reserve free (readiness).
    pub fn space_ok(&self) -> bool {
        match &self.log {
            Log::Direct(j) => j.space_ok(),
            Log::Piped(p) => p.space_ok(),
        }
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
        match &mut self.log {
            // durable BEFORE applied below
            Log::Direct(j) => {
                j.append(&payload)?;
            }
            // Applied now so later decisions see it; the shell releases
            // nothing this record decides until the writer reports it
            // durable.
            Log::Piped(p) => p.enqueue(payload)?,
        }
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
        self.limits.admits_key(&key)?;
        if let Some(existing) = self.by_op.get(&key) {
            return Ok(*existing); // duplicate: hand back the original run
        }
        self.limits.admits(&spec)?; // limits refuse before anything is written
        let run = RunId(self.next_run);
        self.commit(
            run,
            Event::Accepted {
                key,
                spec,
                not_before,
            },
            now,
        )?;
        self.next_run += 1; // only a committed acceptance consumes an id
        Ok(run)
    }

    /// Every run whose wait has expired at `now` — the dispatch candidates
    /// as (run, step, next attempt), longest-waiting first so a per-tick
    /// dispatch cap cannot starve anyone.
    pub fn due(&self, now: Millis) -> Vec<(RunId, StepIndex, u32)> {
        let mut due: Vec<(Millis, RunId, StepIndex, u32)> = self
            .runs
            .iter()
            .filter_map(|(id, r)| match &r.state {
                RunState::Queued { next_run_at } if *next_run_at <= now => {
                    Some((*next_run_at, *id, StepIndex(0), 1))
                }
                RunState::RetryWaiting {
                    step,
                    attempt,
                    next_run_at,
                } if *next_run_at <= now => Some((*next_run_at, *id, *step, *attempt + 1)),
                _ => None,
            })
            .collect();
        due.sort_unstable_by_key(|(at, id, ..)| (*at, id.0));
        due.into_iter().map(|(_, id, s, a)| (id, s, a)).collect()
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

    /// A poisoned journal writer refuses every commit: the process must
    /// stop and let startup replay decide what is durable.
    pub fn is_poisoned(&self) -> bool {
        match &self.log {
            Log::Direct(j) => j.is_poisoned(),
            Log::Piped(p) => p.status.failed.is_some(),
        }
    }

    pub fn run(&self, id: RunId) -> Option<&Run> {
        self.runs.get(&id)
    }

    /// Bounded diagnostics: counts, never rows — O(number of states),
    /// safe to expose on a busy server.
    pub fn diagnostics(&self) -> Diagnostics {
        let mut states = std::collections::BTreeMap::new();
        for r in self.runs.values() {
            *states.entry(r.state.kind()).or_insert(0u64) += 1;
        }
        let (generation, records_written, journal_batches, largest_batch, stale_generations) =
            match &self.log {
                Log::Direct(j) => (
                    j.generation(),
                    j.records_written(),
                    0,
                    0,
                    j.stale_generations().unwrap_or(0),
                ),
                Log::Piped(p) => (
                    p.status.generation,
                    p.status.records_written,
                    p.status.batches,
                    p.status.largest_batch,
                    p.status.stale_generations,
                ),
            };
        Diagnostics {
            generation,
            records_written,
            live_runs: self.runs.len() as u64,
            dedup_keys: self.by_op.len() as u64,
            states,
            space_ok: self.space_ok(),
            errors: std::collections::BTreeMap::new(),
            journal_batches,
            largest_batch,
            stale_generations,
        }
    }

    /// Iterate the live index in id order (bounded by the index, never by
    /// history).
    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.values()
    }

    /// One page of the live index: up to `limit` runs with id > `after`,
    /// in id order, plus the cursor for the next page (if any).
    pub fn page(&self, after: Option<RunId>, limit: usize) -> (Vec<Run>, Option<RunId>) {
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut it = self.runs.range((start, Bound::Unbounded));
        let page: Vec<Run> = it.by_ref().take(limit).map(|(_, r)| r.clone()).collect();
        let next = match (page.last(), it.next()) {
            (Some(last), Some(_)) => Some(last.id),
            _ => None,
        };
        (page, next)
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

    /// A compaction refused for space changes nothing: the old generation
    /// stays current and expiry is not applied to the live index (memory
    /// and disk keep agreeing). With space it proceeds and expires.
    #[test]
    fn refused_compaction_changes_nothing() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        use std::sync::Arc;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let free = Arc::new(AtomicU64::new(1 << 20));
        let probe_free = free.clone();
        let opts = JournalOptions {
            reserve_bytes: 4096,
            space: durable_journal::SpaceProbe::new(move |_| Ok(probe_free.load(SeqCst))),
            ..JournalOptions::default()
        };
        let mut e = EngineCore::open_with(d.path(), opts).unwrap_or_else(|e| panic!("{e}"));
        let old = e
            .submit(OperationKey("old".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.cancel(old, 1).unwrap_or_else(|e| panic!("{e}"));
        let eight_days = 8 * 24 * 60 * 60 * 1000;

        free.store(4096 + 32, SeqCst); // room for a record, not a snapshot
        assert!(matches!(
            e.compact(eight_days),
            Err(EngineError::Journal(JournalError::NoSpace { .. }))
        ));
        assert_eq!(e.diagnostics().generation, 1);
        assert!(
            e.run(old).is_some(),
            "expiry applies only with the new generation"
        );

        free.store(1 << 20, SeqCst);
        assert_eq!(e.compact(eight_days).unwrap_or_else(|e| panic!("{e}")), 2);
        assert!(e.run(old).is_none());
    }

    /// Operation keys and step/handler names are bounded and must be
    /// non-empty: an oversized one is refused before anything is
    /// journaled (it would otherwise ride along in every record, probe URL
    /// and idempotency key forever).
    #[test]
    fn oversized_keys_and_names_are_refused_before_journaling() {
        let (_d, mut e) = open();
        let huge = "k".repeat(10_000);
        for (key, spec) in [
            (huge.clone(), spec()),
            (String::new(), spec()),
            ("ctl\nkey".to_string(), spec()),
            ("ok".to_string(), {
                let mut s = spec();
                s.steps[0].name = huge.clone();
                s
            }),
            ("ok".to_string(), {
                let mut s = spec();
                s.steps[1].handler = huge.clone();
                s
            }),
            ("ok".to_string(), {
                let mut s = spec();
                s.steps[0].name = String::new();
                s
            }),
        ] {
            let r = e.submit(OperationKey(key.clone()), spec, 0);
            assert!(
                matches!(r, Err(EngineError::Refused(Refusal::OverLimits))),
                "key len {} must be refused: {r:?}",
                key.len()
            );
        }
        assert_eq!(e.diagnostics().records_written, 0, "nothing was journaled");
        // At the bound is fine.
        e.submit(OperationKey("k".repeat(256)), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
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
            .err()
            .unwrap_or_else(|| panic!("a reply for a never-dispatched step is refused"));
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
        let dest = dest.path().join("backup");
        e.backup_to(&dest).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            matches!(
                e.backup_to(&dest),
                Err(EngineError::Journal(JournalError::BackupExists))
            ),
            "a backup never merges into an existing directory"
        );
        drop(e); // release the writer lock on the source
        let e2 = EngineCore::open(&dest).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e2.run(run).unwrap_or_else(|| panic!("run")).state,
            RunState::Cancelled
        ));
    }
}
