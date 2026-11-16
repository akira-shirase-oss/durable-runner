//! The group-commit writer: one OS thread owns the journal file, so the
//! async actor never blocks on `write` or `fsync`. The actor queues the
//! records it has decided; the writer gathers what is waiting into one
//! bounded batch, writes it as consecutive frames, fsyncs once, and
//! reports the commit index the fsync covers. The rules it keeps are D-014's:
//!
//! - nothing a record decides is released before that report (the shell
//!   holds acks, read answers and handler calls until then);
//! - sequence numbers are contiguous, assigned in queue order by this one
//!   writer;
//! - a failed write or fsync fails the batch and everything after it: the
//!   fsync is never retried, the log is rolled back to its durable length
//!   when possible (`Journal::append_batch`), the writer stops, and the
//!   process exits so replay rebuilds memory, which is already ahead of disk;
//! - batches are bounded in records and bytes, and the wait for more
//!   records is bounded by `max_wait`;
//! - compaction and backup run here, in queue order behind earlier appends;
//!   a compaction that fails at or after its manifest swap poisons the
//!   journal and stops the writer exactly like a failed append (D-019).

use crate::EngineError;
use durable_journal::{Journal, JournalError};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

/// Messages the writer can hold before the actor keeps work in its own
/// (bounded) outbox instead.
const WRITER_QUEUE: usize = 8;

/// Group-commit bounds.
#[derive(Debug, Clone)]
pub struct GroupCommit {
    /// Most records in one write + fsync.
    pub max_batch_records: usize,
    /// Most payload bytes in one write + fsync (a single message may
    /// exceed it only if one record does).
    pub max_batch_bytes: usize,
    /// How long the writer waits for more records after the first before
    /// writing. Zero: write what is waiting; batching then comes from the
    /// records that queue up while the previous fsync runs.
    pub max_wait: Duration,
}

impl Default for GroupCommit {
    fn default() -> Self {
        Self {
            max_batch_records: 1024,
            max_batch_bytes: 4 * 1024 * 1024,
            max_wait: Duration::ZERO,
        }
    }
}

/// What the writer reports after every message it handles.
#[derive(Debug, Clone, Default)]
pub struct WriterStatus {
    /// Highest commit index made durable (this process's numbering).
    pub durable: u64,
    pub generation: u32,
    /// Durable records in the current generation.
    pub records_written: u64,
    pub batches: u64,
    pub largest_batch: u64,
    pub largest_batch_bytes: u64,
    /// Superseded generation directories still on disk (retention lag).
    pub stale_generations: u32,
    /// Set once a write or fsync failed; the writer has stopped.
    pub failed: Option<Arc<JournalError>>,
}

pub(crate) enum WriterMsg {
    /// Consecutive records (`bytes` of payload); `upto` is the commit
    /// index of the last one.
    Append {
        records: Vec<Vec<u8>>,
        bytes: usize,
        upto: u64,
    },
    /// Encode `view` (exactly `bytes`) straight into the new generation.
    Compact {
        view: crate::snapshot::View,
        bytes: u64,
        reply: oneshot::Sender<Result<u32, JournalError>>,
    },
    Backup {
        dest: PathBuf,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
}

impl WriterMsg {
    pub(crate) fn records(&self) -> usize {
        match self {
            WriterMsg::Append { records, .. } => records.len(),
            _ => 0,
        }
    }
    pub(crate) fn bytes(&self) -> usize {
        match self {
            WriterMsg::Append { bytes, .. } => *bytes,
            _ => 0,
        }
    }
}

pub(crate) struct Writer {
    pub tx: SyncSender<WriterMsg>,
    pub status: watch::Receiver<WriterStatus>,
    pub thread: JoinHandle<()>,
}

/// Start the writer thread on `journal`. `status` starts with the
/// journal's generation and record count.
pub(crate) fn spawn(journal: Journal, cfg: GroupCommit) -> std::io::Result<Writer> {
    let (tx, rx) = mpsc::sync_channel(WRITER_QUEUE);
    let (status_tx, status) = watch::channel(WriterStatus {
        generation: journal.generation(),
        records_written: journal.records_written(),
        stale_generations: journal.stale_generations().unwrap_or(0),
        ..WriterStatus::default()
    });
    let thread = std::thread::Builder::new()
        .name("durable-journal-writer".into())
        .spawn(move || run(journal, rx, status_tx, cfg))?;
    Ok(Writer { tx, status, thread })
}

fn run(
    mut journal: Journal,
    rx: Receiver<WriterMsg>,
    status: watch::Sender<WriterStatus>,
    cfg: GroupCommit,
) {
    let mut st = status.borrow().clone();
    let mut carry: Option<WriterMsg> = None;
    loop {
        let msg = match carry.take() {
            Some(m) => m,
            None => match rx.recv() {
                Ok(m) => m,
                Err(_) => return, // the actor is gone: release the journal
            },
        };
        match msg {
            WriterMsg::Append {
                records,
                bytes,
                upto,
            } => {
                let (batch, bytes, upto, next) = gather(&rx, records, bytes, upto, &cfg);
                carry = next;
                match journal.append_batch(&batch) {
                    Ok(_) => {
                        st.durable = upto;
                        st.records_written = journal.records_written();
                        st.batches += 1;
                        st.largest_batch = st.largest_batch.max(batch.len() as u64);
                        st.largest_batch_bytes = st.largest_batch_bytes.max(bytes as u64);
                    }
                    Err(e) => {
                        // Every waiter in this batch, and every later one,
                        // fails; the fsync is not retried; stop.
                        st.failed = Some(Arc::new(e));
                        status.send_replace(st);
                        return;
                    }
                }
            }
            WriterMsg::Compact { view, bytes, reply } => {
                // Streamed: memory stays a buffer, not the snapshot's size.
                let r = journal
                    .compact_with(0, bytes, |w| crate::snapshot::encode(&view, w))
                    .map(|()| journal.generation());
                drop(view); // release the shared runs promptly
                match r {
                    Ok(generation) => {
                        st.generation = generation;
                        st.records_written = 0;
                        if let Ok(n) = journal.stale_generations() {
                            st.stale_generations = n;
                        }
                        let _ = reply.send(Ok(generation));
                    }
                    // Failed at or after the manifest swap: the journal is
                    // poisoned. Same rule as a failed append — stop, so the
                    // actor fails every waiter and the process exits.
                    Err(e) if journal.is_poisoned() => {
                        let _ = reply.send(Err(JournalError::Poisoned));
                        st.failed = Some(Arc::new(e));
                        status.send_replace(st);
                        return;
                    }
                    // Failed before the swap: nothing moved, carry on.
                    Err(e) => {
                        let _ = reply.send(Err(e)); // the actor may have stopped meanwhile
                    }
                }
            }
            WriterMsg::Backup { dest, reply } => {
                let _ = reply.send(journal.backup_to(&dest).map_err(EngineError::from));
            }
        }
        // Report after every message, so the actor always learns when the
        // writer is free for its next batch.
        status.send_replace(st.clone());
    }
}

/// Extend a batch with appends already waiting (or arriving within
/// `max_wait`), never past the record and byte bounds. A message that does
/// not fit, or a control message, is handed back to run next.
fn gather(
    rx: &Receiver<WriterMsg>,
    mut batch: Vec<Vec<u8>>,
    mut bytes: usize,
    mut upto: u64,
    cfg: &GroupCommit,
) -> (Vec<Vec<u8>>, usize, u64, Option<WriterMsg>) {
    let deadline = Instant::now() + cfg.max_wait;
    loop {
        if batch.len() >= cfg.max_batch_records || bytes >= cfg.max_batch_bytes {
            return (batch, bytes, upto, None);
        }
        let next = match rx.try_recv() {
            Ok(m) => m,
            Err(TryRecvError::Disconnected) => return (batch, bytes, upto, None),
            Err(TryRecvError::Empty) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return (batch, bytes, upto, None);
                }
                match rx.recv_timeout(left) {
                    Ok(m) => m,
                    Err(_) => return (batch, bytes, upto, None),
                }
            }
        };
        match next {
            WriterMsg::Append {
                records,
                bytes: b,
                upto: u,
            } if batch.len() + records.len() <= cfg.max_batch_records
                && bytes + b <= cfg.max_batch_bytes =>
            {
                bytes += b;
                batch.extend(records);
                upto = u;
            }
            other => return (batch, bytes, upto, Some(other)),
        }
    }
}
