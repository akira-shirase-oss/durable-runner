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
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

pub mod shell;
pub mod snapshot;
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
    /// The workspace's handler registry is at its bound
    /// (`MAX_HANDLERS_PER_WS`): the registration was refused.
    #[error("handler registry is full (256 per workspace)")]
    RegistryFull,
    /// The group commit holding this request's record failed. Nothing
    /// decided on the strength of it left the process; the engine stops
    /// and restart replay decides what is durable.
    #[error("journal commit failed; nothing was acknowledged: {0}")]
    CommitFailed(Arc<JournalError>),
    /// A direct-mode operation was called while the writer thread owns the
    /// journal (a programming error, never a client one).
    #[error("operation unavailable while the group-commit writer owns the journal")]
    Piped,
    /// The snapshot a compaction would write exceeds the configured budget:
    /// refused before anything is written (raise the budget, or bound the
    /// index with the admission limits).
    #[error("snapshot would be {bytes} bytes, over the {budget}-byte budget")]
    SnapshotOverBudget { bytes: u64, budget: u64 },
    /// A compaction is already running; this request was not queued.
    #[error("a compaction is already running")]
    CompactionRunning,
    /// The engine configuration is unusable (refused at spawn).
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The engine did not answer within the caller's deadline. The request
    /// may still complete (a submit may commit): retry with the same key.
    #[error("the engine did not answer in time")]
    Timeout,
    /// A committed record attributes an event to a workspace other than
    /// the run's — corrupt committed history; startup stops rather than
    /// guessing (S2).
    #[error("record for run {run} carries workspace {record_ws} but the run belongs to {run_ws}")]
    WorkspaceMismatch {
        run: u64,
        record_ws: u64,
        run_ws: u64,
    },
}

/// A journaled record: the run, the commit clock value, and the event.
/// `at` makes replay faithful — deadlines and expiry reproduce the time
/// the transition actually happened, not the time it was replayed.
/// The record format a writer emits (D-054). v1 has no `v` and carries
/// `spec.input` as a JSON number array; v2 adds `v` and carries it as
/// base64; v3 adds `ws` (the owning workspace's internal id — S2). Readers
/// accept all three forever.
pub const ENVELOPE_VERSION: u8 = 3;
/// The highest record version this binary can read. Per D-056 a format
/// bump ships a reader one release before any writer emits it: v1.0.1
/// (S2-0) shipped the v3 reader; this binary writes v3, so a rollback to
/// v1.0.0 is refused but v1.0.1 remains a valid rollback floor.
pub const READER_VERSION: u8 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// Record format version. Records written before D-054 have no `v`
    /// and read as 1. A larger value is a record from a newer binary:
    /// refused at replay, never guessed.
    #[serde(default = "envelope_v1")]
    pub v: u8,
    /// The owning workspace's internal id (v3; v1/v2 records carry none
    /// and read as workspace 0, the default). Internal only — never an
    /// external surface (S2 opaque `public_id`).
    #[serde(default)]
    pub ws: u64,
    pub run: u64,
    pub at: Millis,
    pub event: Event,
}

fn envelope_v1() -> u8 {
    1
}

/// Decode a committed record with this binary's reader version.
fn decode_envelope(payload: &[u8]) -> Result<Envelope, EngineError> {
    decode_envelope_as(payload, READER_VERSION)
}

/// Decode with an explicit reader version and refuse one written by a
/// newer binary. Anything malformed — bad JSON, bad base64 — is the same
/// `Serde` error startup already stops on (D-054). The explicit-reader
/// form exists so tests can pin an older reader's refusal (S2-0 proves
/// the v1.0.0 reader refuses v3 without shipping the old binary).
fn decode_envelope_as(payload: &[u8], reader: u8) -> Result<Envelope, EngineError> {
    use serde::de::Error as _;
    // A shadow with `ws` optional: a v3 record that carries no ws is
    // ambiguous — ws 0 only means "a pre-v3 record", so a v3 missing the
    // field is corrupt, not workspace 0 (S3).
    #[derive(Deserialize)]
    struct Raw {
        #[serde(default = "envelope_v1")]
        v: u8,
        ws: Option<u64>,
        run: u64,
        at: Millis,
        event: Event,
    }
    let raw: Raw = serde_json::from_slice(payload)?;
    if raw.v > reader {
        return Err(serde_json::Error::custom(format_args!(
            "record version {} is newer than this binary understands ({reader})",
            raw.v
        ))
        .into());
    }
    if raw.v >= 3 && raw.ws.is_none() {
        return Err(serde_json::Error::custom("v3 record without ws").into());
    }
    Ok(Envelope {
        v: raw.v,
        ws: raw.ws.unwrap_or(0),
        run: raw.run,
        at: raw.at,
        event: raw.event,
    })
}

/// Bounded diagnostics: aggregate counts only — safe to answer under load.
#[derive(Debug, Clone, Serialize)]
pub struct Diagnostics {
    pub generation: u32,
    pub records_written: u64,
    pub live_runs: u64,
    /// Non-terminal runs (bounded by `max_live_runs`) and the index's
    /// approximate bytes (bounded by `max_index_bytes`).
    pub active_runs: u64,
    pub index_bytes: u64,
    pub dedup_keys: u64,
    /// Distinct workspaces with live runs — a count only, so diagnostics
    /// never carries an internal workspace id.
    pub workspaces_active: u64,
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
    /// Payload bytes in the largest batch (bounded by `max_batch_bytes`
    /// unless a single record is larger).
    pub largest_batch_bytes: u64,
    /// Superseded generations still on disk; retention deletes them after
    /// each successful compaction, so this stays 0 unless a delete failed.
    pub stale_generations: u32,
    /// Bytes of an incomplete final frame the last startup discarded (a
    /// torn tail, never acknowledged); 0 if the log ended cleanly.
    pub discarded_tail_bytes: u64,
    /// The highest record version present in the journal. Upgrade and
    /// rollback refuse to run a binary whose reader is older (D-056).
    pub journal_record_version_max: u8,
    /// The highest snapshot (DRIX) format version the journal holds —
    /// the same rollback gate needs it: a compacted journal can out-date
    /// an older reader even when no new record version was ever written.
    pub journal_snapshot_version_max: u16,
    /// The highest workspace id the journal carries; nonzero means a
    /// v3-writing binary's tenant work is present and a rollback must
    /// know how to park it rather than run it on workspace-0 handlers.
    pub journal_max_ws: u64,
    /// Non-terminal runs belonging to a workspace ≠ 0 — the count a
    /// rollback guard's target must be able to handle (park or execute).
    pub workspace_scoped_live_runs: u64,
}

/// One workspace's own diagnostics — its shape only, never another
/// tenant's (the S1 review: cross-tenant totals stay ops-only).
#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceDiagnostics {
    /// Runs this workspace holds in the index, terminal included.
    pub live_runs: u64,
    /// …of which are non-terminal.
    pub active_runs: u64,
    /// Run counts by state kind, this workspace only.
    pub states: std::collections::BTreeMap<&'static str, u64>,
}

/// The index as snapshots before D-021 carried it (JSON, inputs as number
/// arrays). Read only, so journals compacted by older builds still open;
/// every new snapshot is the binary `snapshot::encode` format.
#[derive(Debug, Deserialize)]
struct LegacyIndex {
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
    /// Payload bytes in `pending`.
    pending_bytes: usize,
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
        let available = self
            .space
            .available(&self.root)
            .map_err(JournalError::SpaceProbe)?;
        let need = (durable_journal::FRAME_HEADER_LEN + payload.len()) as u64;
        if available < self.reserve.saturating_add(need) {
            return Err(JournalError::NoSpace {
                available,
                reserve: self.reserve,
            });
        }
        self.pending_bytes += payload.len();
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

/// What a list shows of a run: never its spec or input, so a page costs
/// the listed fields, not up to 64 KiB of input per run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub id: RunId,
    pub key: OperationKey,
    pub state: RunState,
    pub steps: usize,
    pub finished_at: Option<Millis>,
}

impl RunSummary {
    fn of(r: &Run) -> Self {
        Self {
            id: r.id,
            key: r.key.clone(),
            state: r.state.clone(),
            steps: r.spec.steps.len(),
            finished_at: r.finished_at,
        }
    }
}

/// Records handed to the writer together, with their payload bytes and
/// the commit index of the last one.
pub struct Batch {
    pub records: Vec<Vec<u8>>,
    pub bytes: usize,
    pub upto: u64,
}

/// A compaction worked out against the live index but not yet applied:
/// a consistent view of the runs it keeps (pointers, not copies), the exact
/// size of their encoding, and the runs it expires.
pub struct CompactionPlan {
    pub view: snapshot::View,
    pub bytes: u64,
    pub expired: Vec<RunId>,
}

/// The whole runtime index, rebuilt only by replaying the journal through
/// the reducer — the same fold the live path performs.
pub struct EngineCore {
    log: Log,
    /// Copy-on-write per run: a compaction's view shares these, and a run
    /// is copied only if it changes while that view is alive.
    runs: BTreeMap<RunId, Arc<Run>>,
    /// Idempotency dedup, scoped by workspace (S2): the same key in two
    /// workspaces is two runs, never a collision.
    by_op: HashMap<(u64, OperationKey), RunId>,
    next_run: u64,
    /// Non-terminal runs and the index's approximate bytes, kept current
    /// so admission is O(1). `ws_live` is the same count per workspace —
    /// the per-tenant admission bound (S2 review #6).
    live: usize,
    ws_live: HashMap<u64, usize>,
    /// Run ids per workspace, kept current with `runs`: `page`/
    /// `page_newest` read it instead of scanning the global index —
    /// a tenant's list cost is its own size, never everyone's (S2 #6).
    ws_index: HashMap<u64, BTreeSet<RunId>>,
    index_bytes: u64,
    /// The torn tail the journal discarded when this core opened it.
    discarded_tail: Option<durable_journal::TornTail>,
    /// Highest record `v` present in the journal: bumped by replay and by
    /// every commit this binary writes (D-056's rollback guard).
    journal_record_version_max: u8,
    /// Highest snapshot (DRIX) version the loaded journal holds — bumping
    /// the record reader without the snapshot reader left rollback able
    /// to strand itself on a compacted journal (S2 review).
    snapshot_version_max: u16,
    /// Highest workspace id any replayed record or snapshot run carried.
    /// A binary that reads v3 must also know not to run those tenants'
    /// work — `journal_max_ws > 0` is what format-guard keys on.
    journal_max_ws: u64,
    /// The shape already persisted to `journal.shape` — the sidecar the
    /// format-guard reads with the server down. Written BEFORE the
    /// journal change it describes and only ever upward.
    persisted: durable_journal::Shape,
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
        let t_open = std::time::Instant::now();
        let journal = Journal::open_with(dir, opts)?;
        let t_journal_open = t_open.elapsed();
        let generation = journal.generation();
        let discarded_tail = journal.discarded_tail();
        let gdir = durable_journal::gen_dir_path(dir, generation);
        let mut core = Self {
            log: Log::Direct(journal),
            runs: BTreeMap::new(),
            by_op: HashMap::new(),
            next_run: 1,
            live: 0,
            ws_live: HashMap::new(),
            ws_index: HashMap::new(),
            index_bytes: 0,
            discarded_tail,
            journal_record_version_max: 1,
            snapshot_version_max: 1,
            journal_max_ws: 0,
            persisted: durable_journal::Shape {
                records: 0,
                snapshots: 0,
                max_ws: 0,
            },
            limits: Limits::default(),
            retry: RetryPolicy::default(),
        };
        match durable_journal::open_snapshot(&gdir)? {
            // Streamed: the index is decoded straight from a buffered reader
            // and the checksum verified before the result is used (D-028).
            Some(durable_journal::SnapshotSource::Binary(mut r)) => {
                let len = r.remaining();
                let (ver, next_run, runs) =
                    snapshot::decode_from(&mut r, len).map_err(JournalError::CorruptSnapshot)?;
                r.finish()?;
                core.snapshot_version_max = core.snapshot_version_max.max(ver);
                core.install(next_run, runs);
            }
            Some(durable_journal::SnapshotSource::Legacy(snap)) => {
                if snap.payload.starts_with(snapshot::MAGIC) {
                    let (ver, next_run, runs) =
                        snapshot::decode(&snap.payload).map_err(JournalError::CorruptSnapshot)?;
                    core.snapshot_version_max = core.snapshot_version_max.max(ver);
                    core.install(next_run, runs);
                } else {
                    let idx: LegacyIndex = serde_json::from_slice(&snap.payload)?;
                    // No checksum here: hold every run to the stored bounds
                    // the binary decoder applies (D-033).
                    if let Some(r) = idx.runs.values().find(|r| !durable_domain::STORED.holds(r)) {
                        return Err(JournalError::CorruptSnapshot(format!(
                            "legacy snapshot: run {} is past the stored bounds",
                            r.id.0
                        ))
                        .into());
                    }
                    core.runs = idx
                        .runs
                        .into_iter()
                        .map(|(id, r)| (id, Arc::new(r)))
                        .collect();
                    core.by_op = idx.by_op.into_iter().map(|(k, v)| ((0, k), v)).collect();
                    core.next_run = idx.next_run;
                }
            }
            // Only compaction makes a generation above 1, and it writes the
            // snapshot before the manifest names it: a missing one is
            // damage, never "empty state" (D-029).
            None if generation > 1 => {
                return Err(JournalError::CorruptSnapshot(format!(
                    "generation {generation} has no snapshot; every compacted generation \
                     must have one (restore from a backup)"
                ))
                .into());
            }
            None => {}
        }
        // One record at a time: restart memory is the index, not the log.
        let mut max_run = core.next_run.saturating_sub(1);
        // DURABLE_PROFILE_OPEN breaks the replay into phases (restart
        // cost is decode-bound; see perf/LOAD_MATRIX.md restart-ready).
        let prof = std::env::var_os("DURABLE_PROFILE_OPEN").is_some();
        if prof {
            let t_journal = std::time::Instant::now();
            let (mut t_read, mut t_decode, mut t_apply) = (
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
            );
            let mut n_records = 0u64;
            if let Some(records) = durable_journal::records(&gdir)? {
                for r in records {
                    let t = std::time::Instant::now();
                    let r = r?;
                    t_read += t.elapsed();
                    let t = std::time::Instant::now();
                    let env = decode_envelope(&r.payload)?;
                    t_decode += t.elapsed();
                    max_run = max_run.max(env.run);
                    core.journal_record_version_max = core.journal_record_version_max.max(env.v);
                    core.journal_max_ws = core.journal_max_ws.max(env.ws);
                    let t = std::time::Instant::now();
                    core.apply_committed(env.ws, RunId(env.run), env.event, env.at)?;
                    t_apply += t.elapsed();
                    n_records += 1;
                }
            }
            let t_replay = t_journal.elapsed();
            let t = std::time::Instant::now();
            core.next_run = max_run + 1;
            core.recount();
            eprintln!(
                "PROFILE records={n_records} journal_open={t_journal_open:?} \
                 replay_total={t_replay:?} [read={t_read:?} decode={t_decode:?} apply={t_apply:?}] recount={:?}",
                t.elapsed()
            );
            return core.finish_open(dir);
        }
        if let Some(records) = durable_journal::records(&gdir)? {
            for r in records {
                let r = r?;
                let env = decode_envelope(&r.payload)?;
                max_run = max_run.max(env.run);
                core.journal_record_version_max = core.journal_record_version_max.max(env.v);
                core.journal_max_ws = core.journal_max_ws.max(env.ws);
                core.apply_committed(env.ws, RunId(env.run), env.event, env.at)?;
            }
        }
        core.next_run = max_run + 1;
        core.recount();
        core.finish_open(dir)
    }

    /// The post-replay steps every open path shares: recount is already
    /// done; here the `journal.shape` sidecar is merged with what it
    /// already claimed (a backup could hold either) and re-published at
    /// the max — the format-guard reads it with the server down (D-056).
    /// Missing or malformed heals here; during downtime a malformed one
    /// refuses the guard instead.
    fn finish_open(mut self, dir: &Path) -> Result<Self, EngineError> {
        let disk = match durable_journal::read_shape(dir) {
            Ok(d) => d,
            // Malformed content heals at open — a rewrite publishes the
            // true maxima. A real I/O error (unreadable file) still
            // fails startup like any journal read.
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => None,
            Err(e) => return Err(JournalError::from(e).into()),
        };
        let shape = disk
            .map(|d| self.shape().merged(d))
            .unwrap_or_else(|| self.shape());
        if disk != Some(shape) {
            durable_journal::write_shape(dir, shape).map_err(JournalError::from)?;
        }
        self.persisted = shape;
        Ok(self)
    }

    /// The journal root in either log mode (the piped writer keeps it in
    /// `Pipe::root` — the sidecar is the core's file, not the writer's).
    fn journal_root(&self) -> PathBuf {
        match &self.log {
            Log::Direct(j) => j.dir().to_path_buf(),
            Log::Piped(p) => p.root.clone(),
        }
    }

    /// The shape this journal would report now.
    fn shape(&self) -> durable_journal::Shape {
        durable_journal::Shape {
            records: self.journal_record_version_max,
            snapshots: self.snapshot_version_max,
            max_ws: self.journal_max_ws,
        }
    }

    /// Persist `journal.shape` when the live maxima pass what it already
    /// claims — called BEFORE the record or manifest swap the shape
    /// describes, so a crash over-reports (fail closed).
    fn persist_shape(&mut self) -> Result<(), EngineError> {
        let want = self.shape();
        if want != self.persisted {
            durable_journal::write_shape(&self.journal_root(), want).map_err(JournalError::from)?;
            self.persisted = want;
        }
        Ok(())
    }

    /// Hand the journal to a group-commit writer: from now on commits are
    /// queued (`take_batch`) instead of written. Returns the journal for
    /// the writer thread to own.
    pub fn into_piped(mut self) -> (Self, Option<Journal>) {
        let log = std::mem::replace(
            &mut self.log,
            Log::Piped(Pipe {
                pending: VecDeque::new(),
                pending_bytes: 0,
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

    /// Payload bytes queued for the writer and not yet handed over.
    pub fn pending_bytes(&self) -> usize {
        match &self.log {
            Log::Direct(_) => 0,
            Log::Piped(p) => p.pending_bytes,
        }
    }

    /// Queued records, oldest first, up to `max_records` and `max_bytes`
    /// (always at least one, so a single oversized record still moves),
    /// with their total bytes and the commit index of the last one — the
    /// unit handed to the writer.
    pub fn take_batch(&mut self, max_records: usize, max_bytes: usize) -> Option<Batch> {
        let Log::Piped(p) = &mut self.log else {
            return None;
        };
        let mut records = Vec::new();
        let mut bytes = 0usize;
        while let Some(next) = p.pending.front() {
            let fits = records.len() < max_records.max(1) && bytes + next.len() <= max_bytes;
            if !records.is_empty() && !fits {
                break;
            }
            let Some(rec) = p.pending.pop_front() else {
                break;
            };
            bytes += rec.len();
            records.push(rec);
            if records.len() >= max_records.max(1) {
                break;
            }
        }
        if records.is_empty() {
            return None;
        }
        p.pending_bytes -= bytes;
        let upto = p.last_index - p.pending.len() as u64;
        Some(Batch {
            records,
            bytes,
            upto,
        })
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
        let gone: std::collections::HashSet<RunId> = expired.iter().copied().collect();
        // A pointer per retained run: nothing is copied or encoded here, on
        // the actor; the writer encodes the view while the actor moves on.
        let view = snapshot::View {
            next_run: self.next_run,
            runs: self
                .runs
                .iter()
                .filter(|(id, _)| !gone.contains(id))
                .map(|(_, r)| Arc::clone(r))
                .collect(),
        };
        let bytes = snapshot::encoded_len(&view);
        Ok(CompactionPlan {
            view,
            bytes,
            expired,
        })
    }

    /// Apply a durable compaction's expiry to the live index.
    pub fn finish_compaction(&mut self, expired: &[RunId]) {
        for id in expired {
            if let Some(run) = self.runs.remove(id) {
                self.index_bytes = self
                    .index_bytes
                    .saturating_sub(Limits::run_bytes(&run.key, &run.spec));
                let dk = (run.ws, run.key.clone());
                if self.by_op.get(&dk) == Some(id) {
                    self.by_op.remove(&dk);
                }
                // Only terminal runs expire — live counts are untouched;
                // the per-ws index loses the id either way.
                if let Some(set) = self.ws_index.get_mut(&run.ws) {
                    set.remove(id);
                }
            }
        }
    }

    /// Take a decoded snapshot as the index; dedup keys are exactly the
    /// retained runs' keys.
    fn install(&mut self, next_run: u64, runs: BTreeMap<RunId, Arc<Run>>) {
        self.by_op = runs
            .values()
            .map(|r| ((r.ws, r.key.clone()), r.id))
            .collect();
        self.journal_max_ws = self
            .journal_max_ws
            .max(runs.values().map(|r| r.ws).max().unwrap_or(0));
        self.runs = runs;
        self.next_run = next_run;
    }

    /// Recompute the admission counters from the index (after replay).
    fn recount(&mut self) {
        // Snapshots from before D-030 kept finished runs' inputs.
        for r in self.runs.values_mut() {
            if r.state.is_terminal() && !r.spec.input.is_empty() {
                Arc::make_mut(r).spec.input = Vec::new();
            }
        }
        self.live = self
            .runs
            .values()
            .filter(|r| !r.state.is_terminal())
            .count();
        self.ws_live.clear();
        self.ws_index.clear();
        for r in self.runs.values() {
            self.ws_index.entry(r.ws).or_default().insert(r.id);
            if !r.state.is_terminal() {
                *self.ws_live.entry(r.ws).or_default() += 1;
            }
        }
        self.index_bytes = self
            .runs
            .values()
            .map(|r| Limits::run_bytes(&r.key, &r.spec))
            .sum();
    }

    /// Compact (direct mode): expire 7-day-old terminal runs, snapshot the
    /// live index into a new generation, atomically swap the manifest.
    /// Returns the new generation number. All or nothing.
    pub fn compact(&mut self, now: Millis) -> Result<u32, EngineError> {
        let plan = self.plan_compaction(now)?;
        let shape = self.planned_compaction_shape(&plan.view);
        let Log::Direct(journal) = &mut self.log else {
            return Err(EngineError::Piped);
        };
        journal.compact_with(0, plan.bytes, |w| snapshot::encode(&plan.view, w), shape)?;
        self.persisted = self.persisted.merged(shape);
        let generation = journal.generation();
        self.finish_compaction(&plan.expired);
        Ok(generation)
    }

    /// What a snapshot of `view` will make true on disk: the journal
    /// shape — highest record version, DRIX version, and the view's max
    /// workspace — persisted by `compact_with` inside the manifest swap
    /// (S2 review: after `prune_superseded` deletes the records that
    /// carried those facts, the sidecar is the server-down proof of
    /// tenant runs in a snapshot). The in-memory maxima move with it:
    /// diagnostics and the sidecar describe the same journal.
    pub fn planned_compaction_shape(&mut self, view: &snapshot::View) -> durable_journal::Shape {
        self.snapshot_version_max = self.snapshot_version_max.max(snapshot::VERSION);
        self.journal_max_ws = self
            .journal_max_ws
            .max(view.runs.iter().map(|r| r.ws).max().unwrap_or(0));
        self.shape()
    }

    /// The writer reports a compaction durably swapped: the sidecar it
    /// wrote inside the swap is now the file's claim.
    pub fn note_shape_persisted(&mut self, shape: durable_journal::Shape) {
        self.persisted = self.persisted.merged(shape);
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
        ws: u64,
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
            self.by_op.insert((ws, key.clone()), run_id);
            self.live += 1;
            *self.ws_live.entry(ws).or_default() += 1;
            self.ws_index.entry(ws).or_default().insert(run_id);
            self.index_bytes += Limits::run_bytes(&key, &spec);
            self.runs.insert(
                run_id,
                Arc::new(Run::accepted(run_id, ws, key, spec, not_before)),
            );
            return Ok(vec![]);
        }
        let run = self.runs.get(&run_id).ok_or(Refusal::StaleReply)?;
        // A record that attributes an event to a workspace other than the
        // run's is corrupt committed history — never reduce onto it.
        if run.ws != ws {
            return Err(EngineError::WorkspaceMismatch {
                run: run_id.0,
                record_ws: ws,
                run_ws: run.ws,
            });
        }
        let (state, effects) = reduce(run, &event, now, &self.limits, &self.retry)?;
        // Copies the run only if a compaction's view still shares it.
        let run = Arc::make_mut(self.runs.get_mut(&run_id).ok_or(Refusal::StaleReply)?);
        if state.is_terminal() && run.finished_at.is_none() {
            run.finished_at = Some(now); // the 7-day expiry clock starts
        }
        if state.is_terminal() && !run.state.is_terminal() {
            self.live = self.live.saturating_sub(1);
            if let Some(n) = self.ws_live.get_mut(&run.ws) {
                *n = n.saturating_sub(1);
            }
            // A finished run keeps only what dedup and inspection need (key,
            // state, step count, finish time): its input goes, and so do its
            // bytes from the capacity bound (D-030). Replay does the same.
            let input = std::mem::take(&mut run.spec.input);
            self.index_bytes = self.index_bytes.saturating_sub(input.len() as u64);
        }
        run.state = state;
        Ok(effects)
    }

    /// Journal first, then fold — the only path by which state may change.
    /// Trial-reduce before appending: a refused event must never reach the
    /// journal, because every committed record has to apply on replay.
    /// `ws` is the record's workspace (S2): the run's own for every event
    /// after acceptance.
    fn commit_record(
        &mut self,
        ws: u64,
        run: RunId,
        event: Event,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        if !matches!(event, Event::Accepted { .. }) {
            let run = self.runs.get(&run).ok_or(Refusal::StaleReply)?;
            reduce(run, &event, now, &self.limits, &self.retry)?;
        }
        let payload = serde_json::to_vec(&Envelope {
            v: ENVELOPE_VERSION,
            ws,
            run: run.0,
            at: now,
            event: event.clone(),
        })?;
        // Shape BEFORE the append it describes — but only when a
        // maximum actually moved. Persisting unconditionally would add a
        // create-in-root to every commit: an already-open log can append
        // under a root that no longer grants create (the dir-mode fault
        // test), so the steady state must cost nothing here.
        let r = self.journal_record_version_max.max(ENVELOPE_VERSION);
        let w = self.journal_max_ws.max(ws);
        if r != self.journal_record_version_max || w != self.journal_max_ws {
            self.journal_record_version_max = r;
            self.journal_max_ws = w;
            self.persist_shape()?;
        }
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
        self.apply_committed(ws, run, event, now)
    }

    /// Commit a run-scoped event: the record carries the run's own
    /// workspace (an `Accepted` for an id not yet in the index reaches
    /// here only by programming error and fails `StaleReply`, never writes).
    fn commit(
        &mut self,
        run: RunId,
        event: Event,
        now: Millis,
    ) -> Result<Vec<Effect>, EngineError> {
        let ws = self.runs.get(&run).ok_or(Refusal::StaleReply)?.ws;
        self.commit_record(ws, run, event, now)
    }

    /// Submit a job under an operation key in the default workspace. A
    /// repeated key returns the original run id without touching the
    /// journal — the dedup contract.
    pub fn submit(
        &mut self,
        key: OperationKey,
        spec: JobSpec,
        now: Millis,
    ) -> Result<RunId, EngineError> {
        self.submit_at(0, key, spec, now, now)
    }

    /// Submit with a delayed start: the run queues until `not_before`.
    /// Same durability and dedup contract as `submit`. `ws` is the owning
    /// workspace: dedup is per `(ws, key)` — the same key in another
    /// workspace is a different job (S2).
    pub fn submit_at(
        &mut self,
        ws: u64,
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
        now: Millis,
    ) -> Result<RunId, EngineError> {
        // Dedup first: a run accepted under an older (larger) key bound
        // must still return itself. The bound refuses new keys only.
        if let Some(existing) = self.by_op.get(&(ws, key.clone())) {
            return Ok(*existing); // duplicate: hand back the original run
        }
        self.limits.admits_key(&key)?;
        self.limits.admits(&spec)?; // limits refuse before anything is written
                                    // Capacity: a bounded index, refused explicitly (contract: "cap
                                    // memory and disk; reject overload explicitly").
        if self.live >= self.limits.max_live_runs
            || self.ws_live.get(&ws).copied().unwrap_or(0) >= self.limits.max_live_runs_per_ws
            || self.index_bytes + Limits::run_bytes(&key, &spec) > self.limits.max_index_bytes
        {
            return Err(Refusal::AtCapacity.into());
        }
        let run = RunId(self.next_run);
        self.commit_record(
            ws,
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
        self.runs.get(&id).map(|r| &**r)
    }

    /// Bounded diagnostics: counts, never rows — O(number of states),
    /// safe to expose on a busy server.
    pub fn diagnostics(&self) -> Diagnostics {
        let mut states = std::collections::BTreeMap::new();
        for r in self.runs.values() {
            *states.entry(r.state.kind()).or_insert(0u64) += 1;
        }
        let (generation, records_written, stale_generations, batches) = match &self.log {
            Log::Direct(j) => (
                j.generation(),
                j.records_written(),
                j.stale_generations().unwrap_or(0),
                (0, 0, 0),
            ),
            Log::Piped(p) => (
                p.status.generation,
                p.status.records_written,
                p.status.stale_generations,
                (
                    p.status.batches,
                    p.status.largest_batch,
                    p.status.largest_batch_bytes,
                ),
            ),
        };
        let (journal_batches, largest_batch, largest_batch_bytes) = batches;
        Diagnostics {
            generation,
            records_written,
            live_runs: self.runs.len() as u64,
            active_runs: self.live as u64,
            index_bytes: self.index_bytes,
            dedup_keys: self.by_op.len() as u64,
            workspaces_active: self
                .runs
                .values()
                .map(|r| r.ws)
                .collect::<std::collections::BTreeSet<_>>()
                .len() as u64,
            states,
            space_ok: self.space_ok(),
            errors: std::collections::BTreeMap::new(),
            journal_batches,
            largest_batch,
            largest_batch_bytes,
            stale_generations,
            discarded_tail_bytes: self.discarded_tail.map_or(0, |t| t.bytes),
            journal_record_version_max: self.journal_record_version_max,
            journal_snapshot_version_max: self.snapshot_version_max,
            journal_max_ws: self.journal_max_ws,
            workspace_scoped_live_runs: self
                .runs
                .values()
                .filter(|r| r.ws != 0 && !r.state.is_terminal())
                .count() as u64,
        }
    }

    /// One workspace's diagnostics — the tenant-facing subset, computed
    /// over that workspace's runs only so nothing cross-tenant leaks.
    pub fn diagnostics_for(&self, ws: u64) -> WorkspaceDiagnostics {
        let mut states = std::collections::BTreeMap::new();
        let mut live_runs = 0u64;
        let mut active_runs = 0u64;
        for r in self.runs.values().filter(|r| r.ws == ws) {
            *states.entry(r.state.kind()).or_insert(0u64) += 1;
            live_runs += 1;
            if !r.state.is_terminal() {
                active_runs += 1;
            }
        }
        WorkspaceDiagnostics {
            live_runs,
            active_runs,
            states,
        }
    }

    /// The torn tail the journal discarded when this core opened it.
    pub fn discarded_tail(&self) -> Option<durable_journal::TornTail> {
        self.discarded_tail
    }

    /// Iterate the live index in id order (bounded by the index, never by
    /// history).
    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.values().map(|r| &**r)
    }

    /// One page of the live index for one workspace: up to `limit` runs
    /// with id > `after`, in id order, plus the cursor for the next page
    /// (if any). Runs of other workspaces are skipped inside the scan, so
    /// a page's cursor never reveals another workspace's ids.
    pub fn page(
        &self,
        ws: u64,
        after: Option<RunId>,
        limit: usize,
    ) -> (Vec<RunSummary>, Option<RunId>) {
        // The per-workspace index: a tenant's page costs O(log n +
        // limit) of *its own* runs, never a scan of everyone's.
        let Some(set) = self.ws_index.get(&ws) else {
            return (Vec::new(), None);
        };
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut it = set.range((start, Bound::Unbounded));
        let page: Vec<RunSummary> = it
            .by_ref()
            .take(limit)
            .filter_map(|id| self.runs.get(id).map(|r| RunSummary::of(r)))
            .collect();
        let next = page.last().and_then(|last| it.next().map(|_| last.id));
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

    /// One page newest first for one workspace: up to `limit` runs with
    /// id < `before` (all when `None`), ids descending, plus the cursor
    /// for the next page. See `page` for the scan note.
    pub fn page_newest(
        &self,
        ws: u64,
        before: Option<RunId>,
        limit: usize,
    ) -> (Vec<RunSummary>, Option<RunId>) {
        let Some(set) = self.ws_index.get(&ws) else {
            return (Vec::new(), None);
        };
        let end = before.map_or(Bound::Unbounded, Bound::Excluded);
        let mut it = set.range((Bound::Unbounded, end)).rev();
        let page: Vec<RunSummary> = it
            .by_ref()
            .take(limit)
            .filter_map(|id| self.runs.get(id).map(|r| RunSummary::of(r)))
            .collect();
        let next = page.last().and_then(|last| it.next().map(|_| last.id));
        (page, next)
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
    fn the_shape_sidecar_tracks_the_journal() {
        // Fresh open publishes the baseline (D-056's server-down scan).
        let (d, mut e) = open();
        let s = durable_journal::read_shape(d.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("no journal.shape"));
        assert_eq!(
            s,
            durable_journal::Shape {
                records: 1,
                snapshots: 1,
                max_ws: 0,
            }
        );

        // The first ws ≠ 0 record writes the sidecar BEFORE its journal
        // record — the file is what a downed server leaves the guard.
        e.submit_at(7, OperationKey("t".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let s = durable_journal::read_shape(d.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("no journal.shape"));
        assert_eq!(s.max_ws, 7);
        assert_eq!(s.records, ENVELOPE_VERSION);
        // Diagnostics and the sidecar describe the same journal — the
        // live commit moved journal_max_ws, not only replay (S2 review).
        assert_eq!(e.diagnostics().journal_max_ws, 7);
    }

    #[test]
    fn compaction_moves_tenant_runs_into_the_snapshot_and_the_sidecar_knows() {
        let (d, mut e) = open();
        // A parked tenant run: it survives compaction inside snapshot.bin,
        // where a grep of journal.log can no longer find its ws (S2
        // review: the down-server hole).
        let run = e
            .submit_at(9, OperationKey("t".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.compact(1).unwrap_or_else(|e| panic!("{e}"));
        let s = durable_journal::read_shape(d.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("no journal.shape"));
        assert_eq!(s.snapshots, snapshot::VERSION);
        assert_eq!(s.max_ws, 9);
        // The pruned journal.log really is silent about the tenant now.
        let gen = d
            .path()
            .join(format!("gen-{:06}", e.diagnostics().generation));
        let log =
            std::fs::read_to_string(gen.join("journal.log")).unwrap_or_else(|e| panic!("{e}"));
        assert!(!log.contains("\"ws\":9"), "{log}");
        assert_eq!(
            e.diagnostics().journal_snapshot_version_max,
            snapshot::VERSION
        );
        // Reopen still parks it — the snapshot carried the ws.
        drop(e);
        let e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e2.diagnostics().journal_max_ws, 9);
        assert!(e2.run(run).is_some());
    }

    #[test]
    fn a_malformed_sidecar_heals_at_open_but_the_guard_refuses() {
        let (d, e) = open();
        drop(e);
        std::fs::write(d.path().join("journal.shape"), "records=zzz\n")
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(durable_journal::read_shape(d.path()).is_err());
        let _e2 = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(durable_journal::read_shape(d.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .is_some());
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

    // --- D-054: record format v2 (base64 bytes) reads v1 forever ----------

    /// Write one record in the shape a pre-D-054 binary committed — no
    /// `v`, `input` as a JSON number array — straight into the journal.
    fn commit_v1_accepted(dir: &std::path::Path, run: u64, key: &str, input: &[u8]) {
        let mut j = Journal::open(dir).unwrap_or_else(|e| panic!("{e}"));
        let nums: Vec<String> = input.iter().map(u8::to_string).collect();
        let payload = format!(
            "{{\"run\":{run},\"at\":1000,\"event\":{{\"Accepted\":{{\"key\":\"{key}\",\
             \"spec\":{{\"version\":1,\"steps\":[{{\"name\":\"only\",\"handler\":\"echo\"}}],\
             \"input\":[{}]}},\"not_before\":0}}}}}}",
            nums.join(",")
        );
        j.append(payload.as_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// Copy a directory tree (the committed fixture journal) into a tempdir.
    fn copy_tree(src: &std::path::Path, dst: &std::path::Path) {
        for e in std::fs::read_dir(src).unwrap_or_else(|e| panic!("{e}")) {
            let e = e.unwrap_or_else(|e| panic!("{e}"));
            let to = dst.join(e.file_name());
            if e.file_type().unwrap_or_else(|e| panic!("{e}")).is_dir() {
                std::fs::create_dir(&to).unwrap_or_else(|e| panic!("{e}"));
                copy_tree(&e.path(), &to);
            } else {
                std::fs::copy(e.path(), &to).unwrap_or_else(|e| panic!("{e}"));
            }
        }
    }

    /// The golden fixture: a journal the a7b0a84 binary wrote (v1
    /// records, array inputs, checkpointed outputs — see
    /// tests/fixtures/v1-journal). It replays to the identical index.
    #[test]
    fn a_journal_written_by_the_old_binary_replays() {
        let src =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v1-journal");
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        copy_tree(&src, d.path());
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        for (id, key) in [
            (1, "fx-empty"),
            (2, "fx-text"),
            (3, "fx-bin"),
            (4, "fx-bytes"),
        ] {
            let r = e.run(RunId(id)).unwrap_or_else(|| panic!("run {id}"));
            assert_eq!(r.key.0, key);
            assert_eq!(r.state, RunState::Done);
        }
        assert_eq!(e.runs().count(), 4);
    }

    /// A hand-shaped v1 record: byte-exact input survives replay while
    /// the run is still queued (terminal runs drop input by design).
    #[test]
    fn a_v1_record_replays_with_its_input_bytes() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v1_accepted(d.path(), 1, "old", &[0, 127, 128, 255]);
        let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let r = e.run(RunId(1)).unwrap_or_else(|| panic!("run"));
        assert_eq!(r.spec.input, vec![0, 127, 128, 255]);
        // And the dedup index picked the v1 key up: resubmitting returns run 1.
        let again = e
            .submit(OperationKey("old".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again, RunId(1));
    }

    /// One journal holding both record versions replays both.
    #[test]
    fn a_mixed_v1_and_v2_journal_replays() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v1_accepted(d.path(), 1, "v1", &[1, 2, 3]);
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            e.submit(OperationKey("v2".into()), spec(), 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("1")).key.0, "v1");
        assert_eq!(e.run(RunId(2)).unwrap_or_else(|| panic!("2")).key.0, "v2");
        assert_eq!(
            e.run(RunId(1)).unwrap_or_else(|| panic!("1")).spec.input,
            [1, 2, 3]
        );
        // D-056's rollback guard: the journal's highest version is what
        // upgrade/rollback compare a binary's reader against.
        assert_eq!(e.diagnostics().journal_record_version_max, ENVELOPE_VERSION);
    }

    /// D-056: the version guard reads the journal's highest `v` — a v1
    /// journal reports 1, then the first v2 commit makes it 2, and a
    /// reopen keeps it (the records stay in the journal).
    #[test]
    fn diagnostics_reports_the_journals_highest_record_version() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v1_accepted(d.path(), 1, "old", &[9]);
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.diagnostics().journal_record_version_max, 1);
            e.submit(OperationKey("new".into()), spec(), 0)
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.diagnostics().journal_record_version_max, ENVELOPE_VERSION);
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.diagnostics().journal_record_version_max, ENVELOPE_VERSION);
    }

    /// A committed record whose base64 is malformed is corruption and
    /// stops startup — identical to any other corrupt committed record.
    #[test]
    fn a_corrupt_base64_input_stops_startup() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let mut j = Journal::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(
                br#"{"v":2,"run":1,"at":1,"event":{"Accepted":{"key":"k","spec":{"version":1,"steps":[],"input":"***"},"not_before":0}}}"#,
            )
            .unwrap_or_else(|e| panic!("{e}"));
        }
        let err = EngineCore::open(d.path())
            .err()
            .unwrap_or_else(|| panic!("opened"));
        assert!(matches!(err, EngineError::Serde(_)), "{err}");
    }

    // --- S2: record format v3 (ws); S2-0 read early, S2-3 writes it ------

    /// Write one record in the shape the S2 writer emits — `v:3` with a
    /// workspace — straight into the journal.
    fn commit_v3(dir: &std::path::Path, run: u64, ws: u64, event: &str) {
        let mut j = Journal::open(dir).unwrap_or_else(|e| panic!("{e}"));
        let payload =
            format!("{{\"v\":3,\"ws\":{ws},\"run\":{run},\"at\":1000,\"event\":{event}}}");
        j.append(payload.as_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// Write one record in the v1.0.x writer's shape — `v:2`, no `ws`,
    /// base64 input — straight into the journal.
    fn commit_v2(dir: &std::path::Path, run: u64, event: &str) {
        let mut j = Journal::open(dir).unwrap_or_else(|e| panic!("{e}"));
        let payload = format!("{{\"v\":2,\"run\":{run},\"at\":1000,\"event\":{event}}}");
        j.append(payload.as_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    fn v2_accepted(key: &str) -> String {
        format!(
            "{{\"Accepted\":{{\"key\":\"{key}\",\"spec\":{{\"version\":1,\
             \"steps\":[{{\"name\":\"only\",\"handler\":\"echo\"}}],\"input\":\"aGk=\"}},\
             \"not_before\":0}}}}"
        )
    }

    fn v3_accepted(key: &str) -> String {
        format!(
            "{{\"Accepted\":{{\"key\":\"{key}\",\"spec\":{{\"version\":1,\
             \"steps\":[{{\"name\":\"only\",\"handler\":\"echo\"}}],\"input\":[]}},\
             \"not_before\":0}}}}"
        )
    }

    /// The v3 reader: a journal written by the future S2 writer replays
    /// with each run in its own workspace, and dedup is per workspace —
    /// the same key under two workspaces is two runs.
    #[test]
    fn a_v3_journal_replays_with_workspaces() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("same-key"));
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("run")).ws, 7);
            // Same key, same workspace → dedup returns run 1.
            let again = e
                .submit_at(7, OperationKey("same-key".into()), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(again, RunId(1));
            // Same key, another workspace → an independent run.
            let other = e
                .submit_at(9, OperationKey("same-key".into()), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
            assert_ne!(other, RunId(1));
            assert_eq!(e.run(other).unwrap_or_else(|| panic!("run")).ws, 9);
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.diagnostics().journal_record_version_max, 3);
    }

    /// S3: a v3 record with no `ws` is ambiguous — ws 0 is a pre-v3
    /// record's meaning, not a guess for a v3 that lost the field.
    /// Startup must refuse it like any other corruption, not quietly
    /// replay the run into the operator's workspace.
    #[test]
    fn a_v3_record_without_ws_is_refused() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut j = Journal::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        // A syntactically valid v3 envelope — except `ws` is absent.
        j.append(
            format!(
                "{{\"v\":3,\"run\":1,\"at\":1000,\"event\":{}}}",
                v3_accepted("k")
            )
            .as_bytes(),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        drop(j); // release the writer lock before startup replays
        match EngineCore::open(d.path()) {
            Err(EngineError::Serde(m)) => {
                let m = m.to_string();
                assert!(m.contains("ws"), "{m}")
            }
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("a v3 record without ws must not replay as ws 0"),
        }
    }

    /// Every record a v2 writer emits decodes under the v3 reader as
    /// workspace 0 — the back-compat direction of the bump.
    #[test]
    fn a_v2_journal_reads_as_workspace_zero() {
        let (d, mut e) = open();
        let run = e
            .submit(OperationKey("k".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.run(run).unwrap_or_else(|| panic!("run")).ws, 0);
    }

    /// S2-2's isolation survives restart: scoped paging sees only its own
    /// workspace's runs after a reopen — ws is journaled, not held.
    #[test]
    fn scoped_pages_stay_scoped_across_reopen() {
        let (d, mut e) = open();
        let a = e
            .submit_at(1, OperationKey("shared".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let b = e
            .submit_at(7, OperationKey("shared".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let (one, _) = e.page(1, None, 100);
        assert_eq!(
            one.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![a],
            "ws 1's page must not list ws 7's run"
        );
        let (seven, _) = e.page(7, None, 100);
        assert_eq!(seven.iter().map(|s| s.id).collect::<Vec<_>>(), vec![b]);
        let (none, _) = e.page(9, None, 100);
        assert!(none.is_empty());
    }

    /// S2-3: the writer emits v3 — every committed record carries `v:3`
    /// and its run's workspace, including compat-mode ws 0.
    #[test]
    fn the_writer_emits_v3_records_with_their_workspace() {
        let (d, mut e) = open();
        e.submit_at(7, OperationKey("seven".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.submit(OperationKey("zero".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let recs = durable_journal::replay_dir(&durable_journal::gen_dir_path(d.path(), 1))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!recs.is_empty());
        let mut saw_seven = false;
        let mut saw_zero = false;
        for r in &recs {
            let v: serde_json::Value =
                serde_json::from_slice(&r.payload).unwrap_or_else(|e| panic!("seq {}: {e}", r.seq));
            assert_eq!(v["v"], 3, "record must be v3: {line:?}", line = v);
            assert!(v.get("ws").is_some(), "record must carry ws: {v}");
            saw_seven |= v["ws"] == 7;
            saw_zero |= v["ws"] == 0;
        }
        assert!(saw_seven && saw_zero, "workspaces 7 and 0 both journaled");
    }

    /// A journal with v2 and v3 records interleaved — the state an in-place
    /// upgrade leaves behind — replays each run into its own workspace and
    /// the writer keeps appending v3.
    #[test]
    fn a_mixed_v2_v3_journal_replays_each_workspace() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v2(d.path(), 1, &v2_accepted("old"));
        commit_v3(d.path(), 2, 7, &v3_accepted("new"));
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("1")).ws, 0);
            assert_eq!(e.run(RunId(2)).unwrap_or_else(|| panic!("2")).ws, 7);
            e.submit_at(7, OperationKey("after".into()), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.diagnostics().journal_record_version_max, 3);
        }
        // …and the journal stays coherent across a second reopen.
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let (seven, _) = e.page(7, None, 100);
        assert_eq!(seven.len(), 2);
        let (zero, _) = e.page(0, None, 100);
        assert_eq!(zero.len(), 1);
    }

    /// Crash mid-write on a mixed-version journal: the committed v2+v3
    /// head survives with its workspaces, the torn tail is discarded and
    /// the journal keeps appending v3.
    #[test]
    fn a_torn_tail_on_a_mixed_journal_is_discarded() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v2(d.path(), 1, &v2_accepted("old"));
        commit_v3(d.path(), 2, 7, &v3_accepted("new"));
        // Crash mid-append: a third v3 record's frame starts but the file
        // ends inside it — the torn tail.
        let log = durable_journal::gen_dir_path(d.path(), 1).join("journal.log");
        let committed_len = std::fs::metadata(&log)
            .unwrap_or_else(|e| panic!("{e}"))
            .len();
        commit_v3(d.path(), 3, 7, &v3_accepted("torn"));
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&log)
            .unwrap_or_else(|e| panic!("{e}"));
        f.set_len(committed_len + 20)
            .unwrap_or_else(|e| panic!("{e}"));
        drop(f);
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("1")).ws, 0);
            assert_eq!(e.run(RunId(2)).unwrap_or_else(|| panic!("2")).ws, 7);
            e.submit_at(7, OperationKey("after".into()), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.diagnostics().journal_record_version_max, 3);
        let (seven, _) = e.page(7, None, 100);
        assert_eq!(seven.len(), 2, "torn run 3 is gone; the new submit kept");
    }

    /// The released v1.0.0 binary reads up to v2 only: a v3 record must
    /// stop it cleanly at startup, never be guessed or skipped. Pinned
    /// through the same version check the release shipped.
    #[test]
    fn the_v1_0_reader_refuses_v3_cleanly() {
        let payload = format!(
            "{{\"v\":3,\"ws\":7,\"run\":1,\"at\":1,\"event\":{}}}",
            v3_accepted("k")
        );
        assert!(decode_envelope_as(payload.as_bytes(), 2).is_err());
        // …and this binary reads it.
        decode_envelope_as(payload.as_bytes(), READER_VERSION).unwrap_or_else(|e| panic!("{e}"));
    }

    /// Crash-mid-write on a v3 journal: the committed head survives, the
    /// torn tail is discarded, and the journal keeps appending.
    #[test]
    fn a_torn_v3_tail_is_discarded_and_the_journal_continues() {
        use std::io::Write as _;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("first"));
        // Crash mid-append: garbage bytes glued after the good frame.
        std::fs::OpenOptions::new()
            .append(true)
            .open(durable_journal::gen_dir_path(d.path(), 1).join("journal.log"))
            .unwrap_or_else(|e| panic!("{e}"))
            .write_all(&[0xde, 0xad, 0xbe])
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("run")).ws, 7);
            e.submit_at(9, OperationKey("after".into()), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.runs().count(), 2);
    }

    /// A v3 event stamped with a workspace other than its run's is
    /// corrupt committed history: startup stops on it.
    #[test]
    fn a_mismatched_workspace_record_stops_startup() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("k"));
        commit_v3(d.path(), 1, 9, "\"CancelRequested\"");
        let err = EngineCore::open(d.path())
            .err()
            .unwrap_or_else(|| panic!("opened"));
        assert!(
            matches!(err, EngineError::WorkspaceMismatch { .. }),
            "{err}"
        );
    }

    /// A record tagged with a version this binary does not know is from
    /// the future: refuse it instead of guessing.
    #[test]
    fn a_record_newer_than_this_binary_is_refused() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let mut j = Journal::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(
                br#"{"v":9,"run":1,"at":1,"event":{"Accepted":{"key":"k","spec":{"version":1,"steps":[],"input":[]},"not_before":0}}}"#,
            )
            .unwrap_or_else(|e| panic!("{e}"));
        }
        let err = EngineCore::open(d.path())
            .err()
            .unwrap_or_else(|| panic!("opened"));
        assert!(matches!(err, EngineError::Serde(_)), "{err}");
    }

    // --- v1.0.2 telemetry on the workspace-aware binary -------------------
    //
    // This branch's registry is (ws, name)-keyed: tenant runs dispatch to
    // their own workspace's handlers — nothing is parked here. What the
    // merge keeps is the journal-shape telemetry the rollback guard keys
    // on (D-056): a downgrade to a pre-v1.0.2 binary is refused once the
    // journal shows workspace data, because that binary cannot park it.

    /// Tenant runs are due like any other — the workspace-aware registry
    /// resolves their handlers — and diagnostics still expose the shape.
    #[test]
    fn tenant_runs_dispatch_with_journal_shape_reported() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("tenant"));
        commit_v3(d.path(), 2, 0, &v3_accepted("ops"));
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let due: Vec<RunId> = e.due(u64::MAX).into_iter().map(|(r, ..)| r).collect();
        assert_eq!(due, vec![RunId(1), RunId(2)]);
        let diag = e.diagnostics();
        assert_eq!(diag.journal_max_ws, 7);
        assert_eq!(diag.workspace_scoped_live_runs, 1);
    }

    /// Tenant attempts expire into reconciles on the same terms as
    /// workspace 0's — the probe resolves through their own registry.
    #[test]
    fn tenant_attempts_expire_like_everyone_elses() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("tenant"));
        commit_v3(
            d.path(),
            1,
            7,
            "{\"StepDispatched\":{\"step\":0,\"attempt\":1,\"deadline\":1000}}",
        );
        commit_v3(d.path(), 2, 0, &v3_accepted("ops"));
        commit_v3(
            d.path(),
            2,
            0,
            "{\"StepDispatched\":{\"step\":0,\"attempt\":1,\"deadline\":1000}}",
        );
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            e.expired_attempts(2000),
            vec![(RunId(1), StepIndex(0)), (RunId(2), StepIndex(0))]
        );
    }

    /// A compacted journal reports its DRIX version and keeps tenant
    /// scope — the guard's snapshot check keys on the same telemetry.
    #[test]
    fn a_snapshot_reports_its_drix_version_and_scope() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v3(d.path(), 1, 7, &v3_accepted("tenant"));
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            e.compact(0).unwrap_or_else(|e| panic!("{e}"));
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.run(RunId(1)).unwrap_or_else(|| panic!("run")).ws, 7);
        let diag = e.diagnostics();
        assert_eq!(diag.workspace_scoped_live_runs, 1);
        assert_eq!(diag.journal_snapshot_version_max, snapshot::VERSION);
        assert_eq!(diag.journal_max_ws, 7);
    }

    /// Compaction and its snapshot carry runs that arrived under either
    /// record version, intact across the generation swap.
    #[test]
    fn compaction_round_trips_runs_from_both_record_versions() {
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        commit_v1_accepted(d.path(), 1, "v1", &[7, 8, 9]);
        {
            let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
            e.submit(OperationKey("v2".into()), spec(), 0)
                .unwrap_or_else(|e| panic!("{e}"));
            e.compact(0).unwrap_or_else(|e| panic!("{e}"));
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        let r1 = e.run(RunId(1)).unwrap_or_else(|| panic!("1"));
        assert_eq!(r1.key.0, "v1");
        assert_eq!(r1.spec.input, [7, 8, 9]);
        assert_eq!(e.run(RunId(2)).unwrap_or_else(|| panic!("2")).key.0, "v2");
    }

    proptest::proptest! {
        /// Every byte shape — empty, odd, large — round-trips through the
        /// record encoding unchanged.
        #[test]
        fn record_bytes_round_trip(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=8192)) {
            let spec = JobSpec {
                version: 1,
                steps: vec![StepSpec { name: "s".into(), handler: "h".into() }],
                input: input.clone(),
            };
            let env = Envelope {
                v: ENVELOPE_VERSION,
                ws: 0,
                run: 1,
                at: 0,
                event: Event::Accepted {
                    key: OperationKey("k".into()),
                    spec,
                    not_before: 0,
                },
            };
            let bytes = serde_json::to_vec(&env).unwrap_or_else(|e| panic!("{e}"));
            let back = decode_envelope(&bytes).unwrap_or_else(|e| panic!("{e}"));
            let Event::Accepted { spec: got, .. } = back.event else {
                panic!("event changed kind")
            };
            assert_eq!(got.input, input);
        }
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

    /// Contract failure case "crash during compaction": killed at any step,
    /// a restart opens the old complete generation or the new complete one,
    /// never a mix. The next compaction succeeds and leaves one generation
    /// on disk.
    #[test]
    fn crash_at_any_compaction_step_reopens_one_complete_generation() {
        use durable_journal::faults::Faults;
        use std::sync::Arc;
        let eight_days = 8 * 24 * 60 * 60 * 1000;
        for step in 1..=3u64 {
            let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let faults = Arc::new(Faults::default());
            let opts = JournalOptions {
                faults: Some(faults.clone()),
                ..JournalOptions::default()
            };
            let (live, old);
            {
                let mut e = EngineCore::open_with(d.path(), opts).unwrap_or_else(|e| panic!("{e}"));
                live = e
                    .submit(OperationKey("live".into()), spec(), 0)
                    .unwrap_or_else(|e| panic!("{e}"));
                old = e
                    .submit(OperationKey("old".into()), spec(), 0)
                    .unwrap_or_else(|e| panic!("{e}"));
                e.cancel(old, 1).unwrap_or_else(|e| panic!("{e}"));
                faults.crash_compaction_at(step);
                assert!(e.compact(eight_days).is_err(), "step {step}: the crash");
            } // the process dies here

            let mut e = EngineCore::open(d.path())
                .unwrap_or_else(|e| panic!("step {step}: restart must open a generation: {e}"));
            let (generation, old_survives) = if step < 3 { (1, true) } else { (2, false) };
            assert_eq!(e.diagnostics().generation, generation, "step {step}");
            assert!(e.run(live).is_some(), "step {step}: live state survives");
            assert_eq!(
                e.run(old).is_some(),
                old_survives,
                "step {step}: old generation replays it; new snapshot expired it"
            );
            let next = e
                .compact(eight_days)
                .unwrap_or_else(|e| panic!("step {step}: the next compaction must work: {e}"));
            assert_eq!(next, generation + 1);
            let gens = std::fs::read_dir(d.path())
                .unwrap_or_else(|e| panic!("{e}"))
                .filter_map(|e| e.ok()?.file_name().into_string().ok())
                .filter(|n| n.starts_with("gen-"))
                .count();
            assert_eq!(gens, 1, "step {step}: leftovers are cleaned up");
            assert!(e.run(live).is_some());
        }
    }

    /// The review's crash sequence: a compaction fails at its manifest
    /// swap, then the next compaction is killed while building its new
    /// generation. That used to delete the live generation (10 of 10 runs
    /// lost). Now the first failure poisons the journal, the second
    /// compaction is refused without touching the disk, and every run
    /// survives the restart.
    #[test]
    fn failed_swap_then_crashed_compaction_loses_nothing() {
        use durable_journal::faults::Faults;
        use std::sync::Arc;
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let faults = Arc::new(Faults::default());
        let opts = JournalOptions {
            faults: Some(faults.clone()),
            ..JournalOptions::default()
        };
        let mut ids = Vec::new();
        {
            let mut e = EngineCore::open_with(d.path(), opts).unwrap_or_else(|e| panic!("{e}"));
            for i in 0..10 {
                ids.push(
                    e.submit(OperationKey(format!("r2-{i}")), spec(), 0)
                        .unwrap_or_else(|e| panic!("{e}")),
                );
            }
            faults.crash_compaction_at(3);
            assert!(e.compact(1).is_err());
            faults.crash_compaction_at(1);
            assert!(e.compact(2).is_err());
        }
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("restart: {e}"));
        for id in ids {
            assert!(e.run(id).is_some(), "{id:?} lost");
        }
    }

    /// Review finding 3: the snapshot was the index JSON-encoded (inputs as
    /// number arrays), then JSON-encoded again as a number array — about
    /// 12 times the raw input. It must stay close to the raw size.
    #[test]
    fn snapshot_is_compact() {
        let (d, mut e) = open();
        let input: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        for i in 0..200 {
            let mut s = spec();
            s.input = input.clone();
            e.submit(OperationKey(format!("compact-{i}")), s, 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let generation = e.compact(1).unwrap_or_else(|e| panic!("{e}"));
        let gdir = durable_journal::gen_dir_path(d.path(), generation);
        let bytes: u64 = std::fs::read_dir(&gdir)
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot"))
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum();
        let raw = 200 * 1024;
        assert!(
            bytes <= raw + raw / 4,
            "snapshot is {bytes} bytes for {raw} bytes of input"
        );
        drop(e);
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.runs().count(), 200);
        assert!(e.runs().all(|r| r.spec.input == input));
    }

    /// Journals compacted by builds before D-021 carry a JSON
    /// `snapshot.json`; they must still open.
    #[test]
    fn legacy_json_snapshot_still_opens() {
        #[derive(Serialize)]
        struct OldIndex {
            runs: BTreeMap<RunId, Run>,
            by_op: HashMap<OperationKey, RunId>,
            next_run: u64,
        }
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let run = Run::accepted(RunId(7), 0, OperationKey("legacy".into()), spec(), 0);
        let payload = serde_json::to_vec(&OldIndex {
            runs: BTreeMap::from([(RunId(7), run)]),
            by_op: HashMap::from([(OperationKey("legacy".into()), RunId(7))]),
            next_run: 8,
        })
        .unwrap_or_else(|e| panic!("{e}"));
        let g2 = durable_journal::gen_dir_path(d.path(), 2);
        std::fs::create_dir_all(&g2).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(g2.join("journal.log"), b"").unwrap_or_else(|e| panic!("{e}"));
        let snap = durable_journal::Snapshot {
            covered_seq: 0,
            payload,
        };
        std::fs::write(
            g2.join("snapshot.json"),
            serde_json::to_vec(&snap).unwrap_or_else(|e| panic!("{e}")),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(d.path().join("manifest.json"), "{\"generation\":2}")
            .unwrap_or_else(|e| panic!("{e}"));
        let mut e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(e.run(RunId(7)).is_some());
        let again = e
            .submit(OperationKey("legacy".into()), spec(), 1)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again, RunId(7), "dedup survives the format change");
        // The next compaction rewrites it in the binary format.
        e.compact(2).unwrap_or_else(|e| panic!("{e}"));
        assert!(durable_journal::gen_dir_path(d.path(), 3)
            .join("snapshot.bin")
            .exists());
    }

    /// A legacy `snapshot.json` has no checksum, so its runs are held to
    /// the same stored bounds as the binary decoder's (checklist: crafted
    /// sizes are refused, not loaded).
    #[test]
    fn legacy_snapshot_with_a_run_past_the_stored_bounds_is_refused() {
        #[derive(Serialize)]
        struct OldIndex {
            runs: BTreeMap<RunId, Run>,
            by_op: HashMap<OperationKey, RunId>,
            next_run: u64,
        }
        let d = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut wide = spec();
        wide.steps = (0..durable_domain::STORED.max_steps + 1)
            .map(|i| StepSpec {
                name: format!("s{i}"),
                handler: "noop".into(),
            })
            .collect();
        let run = Run::accepted(RunId(7), 0, OperationKey("legacy".into()), wide, 0);
        let payload = serde_json::to_vec(&OldIndex {
            runs: BTreeMap::from([(RunId(7), run)]),
            by_op: HashMap::from([(OperationKey("legacy".into()), RunId(7))]),
            next_run: 8,
        })
        .unwrap_or_else(|e| panic!("{e}"));
        let g2 = durable_journal::gen_dir_path(d.path(), 2);
        std::fs::create_dir_all(&g2).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(g2.join("journal.log"), b"").unwrap_or_else(|e| panic!("{e}"));
        let snap = durable_journal::Snapshot {
            covered_seq: 0,
            payload,
        };
        std::fs::write(
            g2.join("snapshot.json"),
            serde_json::to_vec(&snap).unwrap_or_else(|e| panic!("{e}")),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(d.path().join("manifest.json"), "{\"generation\":2}")
            .unwrap_or_else(|e| panic!("{e}"));
        match EngineCore::open(d.path()) {
            Err(EngineError::Journal(JournalError::CorruptSnapshot(_))) => {}
            Err(e) => panic!("wrong error: {e}"),
            Ok(_) => panic!("a run past the stored bounds was loaded"),
        }
    }

    /// The compaction view is consistent: a run changed after the plan was
    /// taken is copied, and the view still holds the state as planned.
    #[test]
    fn compaction_view_is_unchanged_by_later_commits() {
        let (_d, mut e) = open();
        let run = e
            .submit(OperationKey("cow".into()), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        let plan = e.plan_compaction(1).unwrap_or_else(|e| panic!("{e}"));
        e.cancel(run, 2).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            e.run(run).map(|r| &r.state),
            Some(RunState::Cancelled)
        ));
        let mut bytes = Vec::new();
        snapshot::encode(&plan.view, &mut bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(bytes.len() as u64, plan.bytes);
        let (_, _, runs) = snapshot::decode(&bytes).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            runs.get(&run).map(|r| &r.state),
            Some(RunState::Queued { .. })
        ));
    }

    /// Review finding 7: nothing bounded live runs, and accepted submits
    /// could outrun dispatch indefinitely. Beyond `max_live_runs` (or the
    /// index byte bound) a submit is refused `AtCapacity` before anything
    /// is journaled; finishing a run frees room.
    #[test]
    fn admission_is_bounded_by_live_runs_and_index_bytes() {
        let (_d, mut e) = open();
        e.limits.max_live_runs = 3;
        let ids: Vec<RunId> = (0..3)
            .map(|i| {
                e.submit(OperationKey(format!("cap-{i}")), spec(), 0)
                    .unwrap_or_else(|e| panic!("{e}"))
            })
            .collect();
        let written = e.diagnostics().records_written;
        assert!(matches!(
            e.submit(OperationKey("cap-4".into()), spec(), 0),
            Err(EngineError::Refused(Refusal::AtCapacity))
        ));
        assert_eq!(
            e.diagnostics().records_written,
            written,
            "nothing journaled"
        );
        e.cancel(ids[0], 1).unwrap_or_else(|e| panic!("{e}"));
        e.submit(OperationKey("cap-4".into()), spec(), 2)
            .unwrap_or_else(|e| panic!("a finished run frees room: {e}"));

        e.limits.max_live_runs = usize::MAX;
        e.limits.max_index_bytes = e.diagnostics().index_bytes; // already full
        assert!(matches!(
            e.submit(OperationKey("bytes".into()), spec(), 3),
            Err(EngineError::Refused(Refusal::AtCapacity))
        ));
    }

    /// Review S2 #6: one tenant can no longer fill every live slot —
    /// the per-workspace share refuses while other workspaces keep
    /// admitting, and a finished run frees the share again. Replay keeps
    /// the accounting (the counters are rebuilt from the index).
    #[test]
    fn live_run_quota_is_per_workspace() {
        let (_d, mut e) = open();
        e.limits.max_live_runs = 1_000_000; // global bound off the table
        e.limits.max_live_runs_per_ws = 3;
        for i in 0..3 {
            e.submit_at(7, OperationKey(format!("a-{i}")), spec(), 0, 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let written = e.diagnostics().records_written;
        assert!(matches!(
            e.submit_at(7, OperationKey("a-4".into()), spec(), 0, 0),
            Err(EngineError::Refused(Refusal::AtCapacity))
        ));
        assert_eq!(
            e.diagnostics().records_written,
            written,
            "nothing journaled"
        );
        // A second workspace is unaffected — its own share is empty.
        e.submit_at(8, OperationKey("b-0".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("ws 8 must admit: {e}"));
        // A finished run frees ws 7's share.
        let id = e
            .submit_at(8, OperationKey("b-1".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.cancel(id, 1).unwrap_or_else(|e| panic!("{e}"));
        let (page, _) = e.page(7, None, 10);
        e.cancel(page[0].id, 1).unwrap_or_else(|e| panic!("{e}"));
        e.submit_at(7, OperationKey("a-4".into()), spec(), 0, 0)
            .unwrap_or_else(|e| panic!("a finished run frees the share: {e}"));
    }

    /// Review S2 #6: `page`/`page_newest` read the per-workspace index —
    /// a tenant's page is its own runs only, and the cursor never
    /// yields a foreign id.
    #[test]
    fn workspace_pages_read_only_that_workspace() {
        let (_d, mut e) = open();
        let a: Vec<RunId> = (0..5)
            .map(|i| {
                e.submit_at(1, OperationKey(format!("a-{i}")), spec(), 0, 0)
                    .unwrap_or_else(|e| panic!("{e}"))
            })
            .collect();
        let _b: Vec<RunId> = (0..50)
            .map(|i| {
                e.submit_at(2, OperationKey(format!("b-{i}")), spec(), 0, 0)
                    .unwrap_or_else(|e| panic!("{e}"))
            })
            .collect();
        let (page, next) = e.page(1, None, 3);
        assert_eq!(page.len(), 3);
        assert_eq!(page[0].id, a[0]);
        let (rest, _) = e.page(1, next, 10);
        assert_eq!(rest.len(), 2, "page walks only ws-1's five runs");
        assert!(rest.iter().all(|r| a.contains(&r.id)));
        // Newest-first is the same index walked backwards.
        let (newest, _) = e.page_newest(1, None, 1);
        assert_eq!(newest[0].id, a[4]);
        // An empty workspace is an empty page, not a scan miss.
        let (empty, none) = e.page(99, None, 10);
        assert!(empty.is_empty() && none.is_none());
    }

    /// Review finding 15(b): a run accepted with a key over today's bound
    /// (journaled before the bound, or under a larger one) must still
    /// deduplicate. The key bound applies to new keys only.
    #[test]
    fn existing_over_long_key_still_deduplicates() {
        let (_d, mut e) = open();
        e.limits.max_key_bytes = 1024;
        let long = OperationKey("k".repeat(300));
        let id = e
            .submit(long.clone(), spec(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.limits.max_key_bytes = 256; // the bound tightens (an upgrade)
        let again = e
            .submit(long, spec(), 1)
            .unwrap_or_else(|e| panic!("dedup must still find the original run: {e}"));
        assert_eq!(again, id);
        assert!(matches!(
            e.submit(OperationKey("n".repeat(300)), spec(), 2),
            Err(EngineError::Refused(Refusal::OverLimits))
        ));
    }

    /// Review T1, the reviewer's repro at engine level: 50 acknowledged
    /// runs, then frame 26's length set past the end of the log. Startup
    /// used to serve 25 of 50 and truncate the rest away; it must refuse
    /// and leave every byte for a repair or a restore.
    #[test]
    fn damaged_frame_length_stops_startup_instead_of_dropping_runs() {
        let (d, mut e) = open();
        for i in 0..50 {
            e.submit(OperationKey(format!("t1-{i}")), spec(), 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        drop(e);
        let log = durable_journal::gen_dir_path(d.path(), 1).join("journal.log");
        let mut bytes = std::fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        let mut at = 0usize;
        for _ in 0..25 {
            let len =
                u32::from_be_bytes([bytes[at + 2], bytes[at + 3], bytes[at + 4], bytes[at + 5]]);
            at += durable_journal::FRAME_HEADER_LEN + len as usize;
        }
        let file_len = bytes.len() as u32;
        for bad in [16 * 1024 * 1024 - 1, file_len] {
            bytes[at + 2..at + 6].copy_from_slice(&bad.to_be_bytes());
            std::fs::write(&log, &bytes).unwrap_or_else(|e| panic!("{e}"));
            match EngineCore::open(d.path()) {
                Err(EngineError::Journal(JournalError::Corrupt { seq: 26, .. })) => {}
                Err(e) => panic!("wrong error: {e}"),
                Ok(c) => panic!("started with {} of 50 runs", c.runs().count()),
            }
            assert!(
                std::fs::read(&log).unwrap_or_else(|e| panic!("{e}")) == bytes,
                "startup must not cut the log"
            );
        }
    }

    /// Review S2: a compacted generation whose snapshot is missing (a
    /// deletion, a partial copy, filesystem damage — no crash can cause
    /// it) used to start as empty state: every run gone, run ids reused,
    /// every op_key re-executing. It must stop startup instead.
    #[test]
    fn missing_snapshot_of_a_compacted_generation_stops_startup() {
        let (d, mut e) = open();
        for i in 0..50 {
            e.submit(OperationKey(format!("s2-{i}")), spec(), 0)
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let generation = e.compact(1).unwrap_or_else(|e| panic!("{e}"));
        drop(e);
        let snap = durable_journal::gen_dir_path(d.path(), generation).join("snapshot.bin");
        std::fs::remove_file(&snap).unwrap_or_else(|e| panic!("{e}"));
        match EngineCore::open(d.path()) {
            Err(EngineError::Journal(JournalError::CorruptSnapshot(why))) => {
                assert!(why.contains("no snapshot"), "{why}");
            }
            Err(e) => panic!("wrong error: {e}"),
            Ok(c) => panic!("started with {} of 50 runs", c.runs().count()),
        }
    }

    /// Review S3: index-byte capacity came back only when runs expired (a
    /// compaction after 7 days), so a runner whose every run was done still
    /// answered at_capacity. Finishing a run keeps only what dedup and
    /// inspection need — its input goes — and frees those bytes at once.
    #[test]
    fn finishing_runs_frees_index_capacity() {
        let (d, mut e) = open();
        let input = vec![7u8; 4096];
        let job = || {
            let mut s = spec();
            s.input = input.clone();
            s
        };
        let first = e
            .submit(OperationKey("cap-0".into()), job(), 0)
            .unwrap_or_else(|e| panic!("{e}"));
        e.limits.max_index_bytes = e.diagnostics().index_bytes + 1000; // room for no second job
        assert!(matches!(
            e.submit(OperationKey("cap-1".into()), job(), 0),
            Err(EngineError::Refused(Refusal::AtCapacity))
        ));
        e.cancel(first, 1).unwrap_or_else(|e| panic!("{e}"));
        e.submit(OperationKey("cap-1".into()), job(), 2)
            .unwrap_or_else(|e| panic!("a finished run must free its input's bytes: {e}"));
        // The finished run keeps its dedup identity and its summary.
        let again = e
            .submit(OperationKey("cap-0".into()), job(), 3)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(again, first);
        let r = e.run(first).unwrap_or_else(|| panic!("tombstone kept"));
        assert!(r.spec.input.is_empty() && r.spec.steps.len() == 2);
        // Replay reproduces the same accounting.
        let bytes = e.diagnostics().index_bytes;
        drop(e);
        let e = EngineCore::open(d.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.diagnostics().index_bytes, bytes);
        assert!(e.run(first).is_some_and(|r| r.spec.input.is_empty()));
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
