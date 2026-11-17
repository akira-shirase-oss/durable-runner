//! The durable journal: framed append-only records behind one exclusive
//! writer. Layout:
//!
//! ```text
//! <dir>/LOCK                      exclusive writer lock (flock, non-blocking)
//! <dir>/manifest.json             { "generation": N }
//! <dir>/gen-00000N/journal.log    append-only framed records
//! <dir>/gen-00000N/snapshot.bin   checksummed checkpoint written by compaction
//! ```
//!
//! Frame v2 (D-032), big-endian:
//!
//! ```text
//! [version u16 = 2][len u32][seq u64][batch_len u32][payload_crc u32]
//! [header_crc u32 over the 22 bytes before it][payload: len bytes]
//! ```
//!
//! `batch_len` is the byte length of the whole group commit (one write, one
//! fsync) the frame was written in, headers included; the frames of a batch
//! tile it exactly. Frame v1 (`[version u16 = 1][len u32][seq u64][crc32
//! u32]`, no header checksum, no batch) is still read, so journals written
//! before D-032 open; a log may hold v1 frames followed by v2 frames, never
//! the reverse.
//!
//! Replay trusts committed history absolutely: only a demonstrably
//! incomplete *tail* may be discarded — the file ends inside the last frame,
//! whose header (when complete) checks out, and nothing that looks like a
//! later frame follows it. That cut is never more than one frame, so never
//! more than the last batch. Anything else stops startup (contract §02).

use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "fault-injection"))]
use std::sync::Arc;
use thiserror::Error;

/// Scripted storage failures for tests. Compiled only for this crate's own
/// tests or with the `fault-injection` feature, which only dev-dependencies
/// enable — a release build has no injection points.
#[cfg(any(test, feature = "fault-injection"))]
pub mod faults {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Condvar, Mutex, PoisonError};

    /// Each armed fault fires once, on the next matching operation.
    #[derive(Debug, Default)]
    pub struct Faults {
        // 0 = disarmed; n = write n-1 bytes of the next write, then fail.
        partial_write: AtomicU64,
        fail_sync: AtomicBool,
        fail_truncate: AtomicBool,
        sync_calls: AtomicU64,
        held: Mutex<bool>,
        released: Condvar,
        // 0 = disarmed; n = the process "dies" at compaction step n.
        compaction_crash: AtomicU64,
        dir_syncs: Mutex<Vec<std::path::PathBuf>>,
        backup_crash: AtomicBool,
    }

    impl Faults {
        /// Every log fsync blocks until `release_syncs` — lets a test
        /// observe what the process does while a commit is not yet durable.
        pub fn hold_syncs(&self) {
            *self.held.lock().unwrap_or_else(PoisonError::into_inner) = true;
        }
        pub fn release_syncs(&self) {
            *self.held.lock().unwrap_or_else(PoisonError::into_inner) = false;
            self.released.notify_all();
        }
        /// The next compaction fails at `step` — 1: the new generation's
        /// log exists; 2: its snapshot is written; 3: the manifest has just
        /// been renamed to name it (its directory fsync not yet done). Drop
        /// the journal to model a crash there, or keep running to model an
        /// I/O error there.
        pub fn crash_compaction_at(&self, step: u64) {
            self.compaction_crash.store(step, Ordering::SeqCst);
        }
        pub(crate) fn compaction_step(&self, step: u64) -> std::io::Result<()> {
            if self
                .compaction_crash
                .compare_exchange(step, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Err(std::io::Error::other("injected crash during compaction"));
            }
            Ok(())
        }
        /// The next backup stops dead after copying the generation data,
        /// before its manifest — what a kill -9 mid-backup leaves.
        pub fn crash_next_backup(&self) {
            self.backup_crash.store(true, Ordering::SeqCst);
        }
        pub(crate) fn take_backup_crash(&self) -> bool {
            self.backup_crash.swap(false, Ordering::SeqCst)
        }
        /// Directories fsynced at open, in order.
        pub fn dir_syncs(&self) -> Vec<std::path::PathBuf> {
            self.dir_syncs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
        pub(crate) fn record_dir_sync(&self, dir: &std::path::Path) {
            self.dir_syncs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(dir.to_path_buf());
        }
        /// How many times the log was fsynced (rollbacks excluded).
        pub fn sync_calls(&self) -> u64 {
            self.sync_calls.load(Ordering::SeqCst)
        }
        pub(crate) fn before_sync(&self) {
            let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
            while *held {
                held = self
                    .released
                    .wait(held)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            self.sync_calls.fetch_add(1, Ordering::SeqCst);
        }
        /// The next log write puts `bytes` bytes on disk and then fails,
        /// like a write cut short by ENOSPC or EIO.
        pub fn fail_next_write_after(&self, bytes: usize) {
            self.partial_write.store(bytes as u64 + 1, Ordering::SeqCst);
        }
        /// The next fsync of the log fails after its write succeeded.
        pub fn fail_next_sync(&self) {
            self.fail_sync.store(true, Ordering::SeqCst);
        }
        /// The next truncation (the rollback after a failed write) fails.
        pub fn fail_next_truncate(&self) {
            self.fail_truncate.store(true, Ordering::SeqCst);
        }
        pub(crate) fn take_write(&self) -> Option<usize> {
            match self.partial_write.swap(0, Ordering::SeqCst) {
                0 => None,
                n => Some((n - 1) as usize),
            }
        }
        pub(crate) fn take_sync(&self) -> bool {
            self.fail_sync.swap(false, Ordering::SeqCst)
        }
        pub(crate) fn take_truncate(&self) -> bool {
            self.fail_truncate.swap(false, Ordering::SeqCst)
        }
    }
}

/// Default bytes that must stay free on the journal filesystem.
pub const DEFAULT_RESERVE_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

/// Reports free bytes on the filesystem holding a path. Production uses
/// `statvfs` (via `fs2`); tests inject a value to emulate a full disk.
#[derive(Clone)]
pub struct SpaceProbe(std::sync::Arc<ProbeFn>);

type ProbeFn = dyn Fn(&Path) -> io::Result<u64> + Send + Sync;

impl SpaceProbe {
    pub fn new(f: impl Fn(&Path) -> io::Result<u64> + Send + Sync + 'static) -> Self {
        Self(std::sync::Arc::new(f))
    }
    pub fn available(&self, path: &Path) -> io::Result<u64> {
        (self.0)(path)
    }
}

impl Default for SpaceProbe {
    fn default() -> Self {
        Self::new(|p| fs2::available_space(p))
    }
}

impl std::fmt::Debug for SpaceProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpaceProbe")
    }
}

/// How a journal is opened. `Default` is the production configuration.
#[derive(Debug, Clone)]
pub struct JournalOptions {
    /// Every write — appends, compaction snapshots, backups — refuses with
    /// `NoSpace` rather than leave fewer than this many bytes free.
    pub reserve_bytes: u64,
    pub space: SpaceProbe,
    /// Scripted failures (tests only).
    #[cfg(any(test, feature = "fault-injection"))]
    pub faults: Option<Arc<faults::Faults>>,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            reserve_bytes: DEFAULT_RESERVE_BYTES,
            space: SpaceProbe::default(),
            #[cfg(any(test, feature = "fault-injection"))]
            faults: None,
        }
    }
}

/// The frame version this build writes (D-032).
pub const FORMAT_VERSION: u16 = 2;
/// The frame version journals before D-032 hold; read, never written.
const V1: u16 = 1;
/// v1 header: version, len, seq, payload crc.
const V1_HEADER_LEN: usize = 2 + 4 + 8 + 4;
/// v2 header: version, len, seq, batch_len, payload crc, header crc.
pub const FRAME_HEADER_LEN: usize = 2 + 4 + 8 + 4 + 4 + 4;
const HEADER_LEN: usize = FRAME_HEADER_LEN;
/// The bytes the header checksum covers (everything before it).
const HEADER_CRC_SPAN: usize = FRAME_HEADER_LEN - 4;
const MAX_PAYLOAD: u64 = 16 * 1024 * 1024; // frames bigger than this are corruption
/// The largest batch one write can carry: its length is a u32 in every
/// frame header.
pub const MAX_BATCH_FRAME_BYTES: u64 = u32::MAX as u64;

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("journal already has a writer (LOCK held)")]
    SecondWriter,
    #[error("corrupt committed history at seq {seq}: {why}")]
    Corrupt { seq: u64, why: String },
    #[error("manifest unreadable: {0}")]
    Manifest(String),
    #[error("admission reserve: {available} bytes free, below {reserve}")]
    NoSpace { available: u64, reserve: u64 },
    /// A log write failed. The bytes it may have left were cut back to the
    /// last durable offset (`rolled_back`), so the record does not exist
    /// and its sequence number is free. If the rollback failed too, the
    /// writer is poisoned.
    #[error("journal write failed (rolled back: {rolled_back}): {source}")]
    WriteFailed {
        #[source]
        source: io::Error,
        rolled_back: bool,
    },
    /// A log fsync failed. It is never retried: after a failed fsync the
    /// kernel may have dropped the dirty pages and a second fsync can
    /// report success for data that is gone. The writer is poisoned; the
    /// process must exit and let startup replay decide what is durable.
    #[error("journal fsync failed; writer poisoned, restart to replay: {0}")]
    SyncFailed(#[source] io::Error),
    /// An earlier write or fsync failure poisoned this writer.
    #[error("journal writer poisoned by an earlier failure; restart to replay")]
    Poisoned,
    /// Compaction failed at or after its manifest swap: the manifest may
    /// already name the new generation while this writer still points at
    /// the old one, so continuing would acknowledge records no restart
    /// replays. The writer is poisoned; restart opens what the manifest
    /// durably names.
    #[error("compaction failed at its manifest swap; writer poisoned, restart to replay: {0}")]
    ManifestSwapFailed(#[source] io::Error),
    /// The durable manifest does not name the generation this writer is
    /// on: something moved it underneath. Nothing is deleted or written;
    /// the writer is poisoned and a restart opens what the manifest names.
    #[error("manifest names generation {on_disk}, writer is on {in_memory}; poisoned, restart to replay")]
    ManifestMismatch { on_disk: u32, in_memory: u32 },
    /// The free-space probe could not answer, so nothing was written
    /// (unknown free space is treated as not enough).
    #[error("free space unknown: {0}")]
    SpaceProbe(#[source] io::Error),
    /// The snapshot (the only authority for compacted state) failed its
    /// checksum or framing: startup stops rather than trust it.
    #[error("snapshot is damaged: {0}")]
    CorruptSnapshot(String),
    /// A backup never merges into an existing directory.
    #[error("backup destination already exists")]
    BackupExists,
    /// One batch must fit the u32 length every frame records; refused
    /// before anything is written.
    #[error("batch of {bytes} bytes is over the {MAX_BATCH_FRAME_BYTES}-byte frame limit")]
    BatchTooLarge { bytes: u64 },
    /// A restore never merges into a directory that already holds a
    /// journal: two histories' generations would mix.
    #[error("restore target already holds a journal (move it aside first)")]
    RestoreTargetNotEmpty,
    /// A backup holds something restore will not copy (a symlink, a
    /// special file): it copies only the backup's own regular files.
    #[error("backup is not restorable: {0}")]
    BadBackup(String),
}

/// A torn tail `Journal::open` discarded: the file ended inside its last
/// frame (never acknowledged, D-014).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TornTail {
    /// Where the incomplete frame started (the log's new length).
    pub offset: u64,
    /// Bytes cut (always less than one frame).
    pub bytes: u64,
}

/// One committed record as replayed: its sequence and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    generation: u32,
}

/// The generation the durable manifest names (`None` if there is none).
fn read_manifest(root: &Path) -> Result<Option<u32>, JournalError> {
    match fs::read_to_string(root.join("manifest.json")) {
        Ok(text) => serde_json::from_str::<Manifest>(&text)
            .map(|m| Some(m.generation))
            .map_err(|e| JournalError::Manifest(e.to_string())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The generation number of a `gen-NNNNNN` directory name.
fn parse_gen(name: &std::ffi::OsStr) -> Option<u32> {
    name.to_str()?.strip_prefix("gen-")?.parse().ok()
}

/// The on-disk directory for a generation under a journal root.
pub fn gen_dir_path(root: &Path, generation: u32) -> PathBuf {
    root.join(format!("gen-{generation:06}"))
}

fn crc(payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(payload);
    h.finalize()
}

fn be_u32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_be_bytes(a)
}

fn be_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_be_bytes(a)
}

/// Append one v2 frame. `batch_len` is the byte length of the whole batch
/// it is written in.
fn put_frame(out: &mut Vec<u8>, seq: u64, batch_len: u32, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&batch_len.to_be_bytes());
    out.extend_from_slice(&crc(payload).to_be_bytes());
    let header_crc = crc(&out[start..]);
    out.extend_from_slice(&header_crc.to_be_bytes());
    out.extend_from_slice(payload);
}

/// Frame `payloads` (sequence numbers from `first_seq`) as one batch.
fn frame_batch(first_seq: u64, payloads: &[Vec<u8>]) -> Result<Vec<u8>, JournalError> {
    let mut total = 0u64;
    for (i, p) in payloads.iter().enumerate() {
        if p.len() as u64 > MAX_PAYLOAD {
            return Err(JournalError::Corrupt {
                seq: first_seq + i as u64,
                why: "payload over cap".into(),
            });
        }
        total += (HEADER_LEN + p.len()) as u64;
    }
    let batch_len =
        u32::try_from(total).map_err(|_| JournalError::BatchTooLarge { bytes: total })?;
    let mut bytes = Vec::with_capacity(total as usize);
    for (i, p) in payloads.iter().enumerate() {
        put_frame(&mut bytes, first_seq + i as u64, batch_len, p);
    }
    Ok(bytes)
}

/// Whether `region` holds anything that checks out as a frame: a v2 header
/// with a valid header checksum, or a complete v1 frame with a valid
/// payload checksum and a sequence number after `after_seq`. Frame headers
/// start with a 0x00 byte, which the engine's JSON payloads never contain,
/// so candidates inside a payload are rare and each costs one checksum.
fn holds_a_frame(region: &[u8], after_seq: u64) -> bool {
    let n = region.len();
    let mut i = 0;
    while i + V1_HEADER_LEN <= n {
        if region[i] == 0 {
            match region[i + 1] {
                2 if i + HEADER_LEN <= n => {
                    let h = &region[i..i + HEADER_LEN];
                    if crc(&h[..HEADER_CRC_SPAN]) == be_u32(&h[HEADER_CRC_SPAN..]) {
                        return true;
                    }
                }
                1 => {
                    let h = &region[i..i + V1_HEADER_LEN];
                    let len = be_u32(&h[2..]) as usize;
                    let seq = be_u64(&h[6..]);
                    let end = i + V1_HEADER_LEN + len;
                    if len > 0
                        && seq > after_seq
                        && end <= n
                        && crc(&region[i + V1_HEADER_LEN..end]) == be_u32(&h[14..])
                    {
                        return true;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    false
}

/// Replay every committed record in a generation's journal.log, in order.
/// A torn tail (the file ends inside a frame) is discarded; anything else
/// wrong — bad version, bad checksum, a sequence gap, a truncated non-tail
/// frame — is corruption and stops the caller. Collects everything; for a
/// bounded-memory pass use `records`.
pub fn replay_dir(dir: &Path) -> Result<Vec<Record>, JournalError> {
    match records(dir)? {
        Some(rs) => rs.collect(),
        None => Ok(Vec::new()),
    }
}

/// The committed records of a generation's journal.log, one at a time
/// (`None` if there is no log). Memory stays one record plus a buffer, not
/// the whole history — restart replays through this (D-028).
pub fn records(dir: &Path) -> Result<Option<Records>, JournalError> {
    match File::open(dir.join("journal.log")) {
        Ok(f) => Ok(Some(Records::new(f)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// A streaming frame reader with replay's rules: a demonstrably torn tail
/// ends it; any other damage is `Corrupt`.
pub struct Records {
    r: io::BufReader<File>,
    file_len: u64,
    /// End of the last good frame.
    pos: u64,
    expected_seq: u64,
    /// The current v2 batch: where it ends and the length its frames carry.
    /// At `pos == batch_end` the next frame starts a new batch.
    batch_end: u64,
    batch_len: u32,
    /// A v2 frame was read: a v1 frame after it is corruption.
    seen_v2: bool,
    /// Set when the reader stopped at a torn tail.
    torn: Option<TornTail>,
    done: bool,
}

impl Records {
    fn new(f: File) -> io::Result<Self> {
        let file_len = f.metadata()?.len();
        Ok(Self {
            r: io::BufReader::with_capacity(256 * 1024, f),
            file_len,
            pos: 0,
            expected_seq: 1,
            batch_end: 0,
            batch_len: 0,
            seen_v2: false,
            torn: None,
            done: false,
        })
    }

    /// Bytes covered by the good frames read so far.
    pub fn good_len(&self) -> u64 {
        self.pos
    }

    /// The torn tail the reader stopped at, if it stopped at one.
    pub fn torn_tail(&self) -> Option<TornTail> {
        self.torn
    }

    fn corrupt(&self, why: impl std::fmt::Display) -> JournalError {
        // A hint for the one case an operator can repair by hand: damage
        // confined to the last batch is what a power loss before its fsync
        // leaves (D-014), and nothing in that batch was acknowledged then.
        let hint = if self.pos < self.batch_end && self.file_len <= self.batch_end {
            "; the damage lies inside the log's last batch"
        } else {
            ""
        };
        JournalError::Corrupt {
            seq: self.expected_seq,
            why: format!("{why} (frame at offset {}{hint})", self.pos),
        }
    }

    /// Stop at a torn tail: the file ends inside the frame at `pos`.
    fn tear(&mut self) -> Result<Option<u64>, JournalError> {
        self.done = true;
        self.torn = Some(TornTail {
            offset: self.pos,
            bytes: self.file_len - self.pos,
        });
        Ok(None)
    }

    /// A frame whose header checks out but whose payload runs past the end
    /// is a torn tail only if nothing after its header looks like a frame:
    /// a later frame means the length was damaged, not cut short. The
    /// region is less than one frame (at most 16 MiB), read once.
    fn tear_unless_followed(&mut self, header: usize) -> Result<Option<u64>, JournalError> {
        let from = self.pos + header as u64;
        let mut region = Vec::new();
        self.r.seek(SeekFrom::Start(from))?;
        (&mut self.r)
            .take(self.file_len - from)
            .read_to_end(&mut region)?;
        if holds_a_frame(&region, self.expected_seq) {
            return Err(
                self.corrupt("length runs past the end of the log but a later frame follows it")
            );
        }
        self.tear()
    }

    /// Read the next committed frame's payload into `buf`; its sequence
    /// number, or `None` at the end (or at a torn tail).
    fn advance(&mut self, buf: &mut Vec<u8>) -> Result<Option<u64>, JournalError> {
        if self.done || self.pos == self.file_len {
            self.done = true;
            return Ok(None);
        }
        if self.file_len - self.pos < V1_HEADER_LEN as u64 {
            return self.tear(); // fewer bytes than any header
        }
        let mut hdr = [0u8; HEADER_LEN];
        self.r.read_exact(&mut hdr[..V1_HEADER_LEN])?;
        let version = u16::from_be_bytes([hdr[0], hdr[1]]);
        match version {
            FORMAT_VERSION => self.advance_v2(hdr, buf),
            V1 if !self.seen_v2 => self.advance_v1(hdr, buf),
            V1 => Err(self.corrupt("a v1 frame after v2 frames")),
            v => Err(self.corrupt(format_args!("version {v}"))),
        }
    }

    fn advance_v2(
        &mut self,
        mut hdr: [u8; HEADER_LEN],
        buf: &mut Vec<u8>,
    ) -> Result<Option<u64>, JournalError> {
        if self.file_len - self.pos < HEADER_LEN as u64 {
            return self.tear(); // an incomplete v2 header
        }
        self.r.read_exact(&mut hdr[V1_HEADER_LEN..])?;
        if crc(&hdr[..HEADER_CRC_SPAN]) != be_u32(&hdr[HEADER_CRC_SPAN..]) {
            // Length, sequence or batch damaged: never trusted, never cut.
            return Err(self.corrupt("frame header checksum"));
        }
        let len = u64::from(be_u32(&hdr[2..]));
        let seq = be_u64(&hdr[6..]);
        let batch_len = be_u32(&hdr[14..]);
        let want_crc = be_u32(&hdr[18..]);
        if len > MAX_PAYLOAD {
            return Err(self.corrupt(format_args!("len {len} over cap")));
        }
        if seq != self.expected_seq {
            return Err(self.corrupt(format_args!("sequence {seq}")));
        }
        let frame_end = self.pos + HEADER_LEN as u64 + len;
        if self.pos >= self.batch_end {
            // The first frame of a batch: the batch must hold it.
            if u64::from(batch_len) < HEADER_LEN as u64 + len {
                return Err(self.corrupt("frame larger than its batch"));
            }
            self.batch_end = self.pos + u64::from(batch_len);
            self.batch_len = batch_len;
        } else if batch_len != self.batch_len || frame_end > self.batch_end {
            return Err(self.corrupt("frame does not fit the batch it was written in"));
        }
        self.seen_v2 = true;
        if frame_end > self.file_len {
            return self.tear_unless_followed(HEADER_LEN);
        }
        buf.clear();
        buf.resize(len as usize, 0);
        self.r.read_exact(buf)?;
        if crc(buf) != want_crc {
            return Err(self.corrupt("payload checksum"));
        }
        self.expected_seq += 1;
        self.pos = frame_end;
        Ok(Some(seq))
    }

    /// A pre-D-032 frame: no header checksum, so a length that runs past
    /// the end is a tear only if its sequence is the expected one and
    /// nothing after it looks like a frame.
    fn advance_v1(
        &mut self,
        hdr: [u8; HEADER_LEN],
        buf: &mut Vec<u8>,
    ) -> Result<Option<u64>, JournalError> {
        let len = u64::from(be_u32(&hdr[2..]));
        let seq = be_u64(&hdr[6..]);
        let want_crc = be_u32(&hdr[14..]);
        if len > MAX_PAYLOAD {
            return Err(self.corrupt(format_args!("len {len} over cap")));
        }
        let frame_end = self.pos + V1_HEADER_LEN as u64 + len;
        if frame_end > self.file_len {
            if seq != self.expected_seq {
                return Err(self.corrupt(format_args!("sequence {seq}")));
            }
            return self.tear_unless_followed(V1_HEADER_LEN);
        }
        buf.clear();
        buf.resize(len as usize, 0);
        self.r.read_exact(buf)?;
        if crc(buf) != want_crc {
            return Err(self.corrupt("payload checksum"));
        }
        if seq != self.expected_seq {
            return Err(self.corrupt(format_args!("sequence {seq}")));
        }
        self.expected_seq += 1;
        self.pos = frame_end;
        self.batch_end = frame_end; // v1 frames carry no batch
        Ok(Some(seq))
    }
}

impl Iterator for Records {
    type Item = Result<Record, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut payload = Vec::new();
        match self.advance(&mut payload) {
            Ok(Some(seq)) => Some(Ok(Record { seq, payload })),
            Ok(None) => None,
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// The single writer. Open = lock + read manifest + verify history +
/// discard a torn tail + position at the end for appends.
#[derive(Debug)]
pub struct Journal {
    root: PathBuf,
    generation: u32,
    file: File,
    next_seq: u64,
    /// Length of the log covered by successful fsyncs: every byte before
    /// it is committed, every byte after it is not.
    durable_len: u64,
    /// Set by a failed fsync or a failed rollback; refuses every append.
    poisoned: bool,
    /// The torn tail this open discarded, if any.
    discarded: Option<TornTail>,
    opts: JournalOptions,
    // Held for the process lifetime; the flock releases when this drops.
    _lock: File,
}

impl Journal {
    /// Open (creating if needed) the journal rooted at `dir`. Fails
    /// `SecondWriter` if another live writer holds the lock, `Corrupt` on
    /// damaged committed history.
    pub fn open(dir: &Path) -> Result<Self, JournalError> {
        Self::open_with(dir, JournalOptions::default())
    }

    /// `open` with explicit options.
    pub fn open_with(dir: &Path, opts: JournalOptions) -> Result<Self, JournalError> {
        // Directory entries created here are fsynced before the first
        // append can be acknowledged (D-025): a new file or directory is
        // durable only once the directory holding its entry is.
        let root_existed = dir.exists();
        fs::create_dir_all(dir)?;
        if !root_existed {
            let parent = dir
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            sync_dir(&opts, parent)?;
        }
        let lock_path = dir.join("LOCK");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|_| JournalError::SecondWriter)?;

        // Manifest: keep the last valid one; create generation 1 on first open.
        let manifest_path = dir.join("manifest.json");
        let generation = match fs::read_to_string(&manifest_path) {
            Ok(text) => {
                serde_json::from_str::<Manifest>(&text)
                    .map_err(|e| JournalError::Manifest(e.to_string()))?
                    .generation
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // First open writes the manifest before any generation, so
                // generations without one are damage or an interrupted
                // restore — never a fresh journal to start over in.
                for entry in fs::read_dir(dir)? {
                    if parse_gen(&entry?.file_name()).is_some() {
                        return Err(JournalError::Manifest(
                            "generation directories but no manifest.json: a damaged journal \
                             or an interrupted restore (remove it and restore again)"
                                .into(),
                        ));
                    }
                }
                let m = Manifest { generation: 1 };
                let tmp = dir.join("manifest.json.tmp");
                fs::write(&tmp, serde_json::to_string(&m).map_err(io::Error::other)?)?;
                File::open(&tmp)?.sync_data()?;
                fs::rename(&tmp, &manifest_path)?;
                sync_dir(&opts, dir)?;
                1
            }
            Err(e) => return Err(e.into()),
        };

        let gdir = gen_dir_path(dir, generation);
        let gen_existed = gdir.exists();
        fs::create_dir_all(&gdir)?;
        let log_path = gdir.join("journal.log");
        let log_existed = log_path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;
        if !log_existed {
            sync_dir(&opts, &gdir)?; // journal.log's entry
        }
        if !gen_existed {
            sync_dir(&opts, dir)?; // the generation directory's entry
        }

        // Verify committed history frame by frame (one payload buffer, never
        // the whole log in memory), then truncate a torn tail only.
        let mut scan = Records::new(File::open(&log_path)?)?;
        let mut buf = Vec::new();
        let mut last_seq = 0;
        while let Some(seq) = scan.advance(&mut buf)? {
            last_seq = seq;
        }
        let good_len = scan.good_len();
        let discarded = scan.torn_tail();
        drop(scan);
        if file.metadata()?.len() != good_len {
            // Only a demonstrably torn tail reaches here: the file ended
            // inside its last frame and nothing after it is a frame. Every
            // other anomaly returned `Corrupt` above, leaving the log as is.
            file.set_len(good_len)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::End(0))?;
        let next_seq = last_seq + 1;
        Ok(Self {
            root: dir.into(),
            generation,
            file,
            next_seq,
            durable_len: good_len,
            poisoned: false,
            discarded,
            opts,
            _lock: lock_file,
        })
    }

    /// Refuse with `NoSpace` unless `need` more bytes fit on the
    /// filesystem holding `path` with the reserve still free afterwards
    /// (contract: full disk fails admission safely, not silently).
    fn ensure_space(&self, path: &Path, need: u64) -> Result<u64, JournalError> {
        let available = self
            .opts
            .space
            .available(path)
            .map_err(JournalError::SpaceProbe)?;
        let reserve = self.opts.reserve_bytes;
        if available < reserve.saturating_add(need) {
            return Err(JournalError::NoSpace { available, reserve });
        }
        Ok(available)
    }

    /// Whether the journal filesystem currently has its reserve free —
    /// the readiness signal. A probe error counts as "no" (unknown is not
    /// ready).
    pub fn space_ok(&self) -> bool {
        self.ensure_space(&self.root, 0).is_ok()
    }

    /// Append one record: frame, write, fsync — only then is it committed.
    /// Returns the assigned sequence number. A failure leaves no record
    /// behind and consumes no sequence number (see `WriteFailed`,
    /// `SyncFailed`); a poisoned writer refuses with `Poisoned`.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        self.ensure_space(&self.root, (HEADER_LEN + payload.len()) as u64)?;
        if payload.len() as u64 > MAX_PAYLOAD {
            return Err(JournalError::Corrupt {
                seq: self.next_seq,
                why: "payload over cap".into(),
            });
        }
        let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
        // One frame is a batch of one: it was the only thing in its fsync.
        put_frame(
            &mut frame,
            self.next_seq,
            (HEADER_LEN + payload.len()) as u32,
            payload,
        );
        self.write_durably(&frame)?;
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    /// Group commit: append consecutive records as one write and one
    /// fsync. Sequence numbers are contiguous and assigned in order; the
    /// last one is returned. The failure rules are `append`'s (D-009): a
    /// failed write is rolled back, a failed fsync poisons, and no record
    /// of the batch is committed or consumes a sequence number.
    ///
    /// No reserve check here: the caller admitted these records against
    /// its last free-space reading, and the reserve absorbs the difference
    /// (a real ENOSPC still fails the write safely).
    pub fn append_batch(&mut self, payloads: &[Vec<u8>]) -> Result<u64, JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        if payloads.is_empty() {
            return Ok(self.next_seq.saturating_sub(1));
        }
        // Every frame records the batch's length, so replay can tell where
        // the one write an fsync covered ends (D-032).
        let bytes = frame_batch(self.next_seq, payloads)?;
        self.write_durably(&bytes)?;
        self.next_seq += payloads.len() as u64;
        Ok(self.next_seq - 1)
    }

    /// The options this journal was opened with.
    pub fn options(&self) -> &JournalOptions {
        &self.opts
    }

    /// Whether an earlier failure poisoned this writer.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// The torn tail `open` cut off (an incomplete final frame, never
    /// acknowledged), for the operator's log.
    pub fn discarded_tail(&self) -> Option<TornTail> {
        self.discarded
    }

    /// Write then fsync `bytes` at the durable offset. On failure the log
    /// is left exactly at its last durable length, or the writer poisons.
    fn write_durably(&mut self, bytes: &[u8]) -> Result<(), JournalError> {
        if let Err(source) = self.write_log(bytes) {
            // Part of the frame may be on disk. Cut it off so the next
            // frame, which reuses this sequence number, starts on a clean
            // boundary. No fsync failed here, so the writer may continue —
            // unless the rollback itself fails.
            let rolled_back = self.rollback().is_ok();
            if !rolled_back {
                self.poisoned = true;
            }
            return Err(JournalError::WriteFailed {
                source,
                rolled_back,
            });
        }
        if let Err(e) = self.sync_log() {
            // Never retry the fsync. Poison first; the rollback is best
            // effort and nothing is acknowledged on the strength of it — if
            // it fails, startup replay discards the torn tail or keeps a
            // complete-but-unacknowledged record (at-least-once, deduped
            // by operation key).
            self.poisoned = true;
            if self.rollback().is_err() {
                // Already poisoned; the replay rules above cover the file.
            }
            return Err(JournalError::SyncFailed(e));
        }
        // the durable point: acks follow this line only
        self.durable_len += bytes.len() as u64;
        Ok(())
    }

    /// Truncate the log to its durable length and make that durable.
    fn rollback(&mut self) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if self.opts.faults.as_ref().is_some_and(|f| f.take_truncate()) {
            return Err(io::Error::other("injected truncate failure"));
        }
        self.file.set_len(self.durable_len)?;
        self.file.sync_data()?;
        // An append-mode handle ignores the position; a read-write one
        // (first open) needs it moved back.
        self.file.seek(SeekFrom::Start(self.durable_len))?;
        Ok(())
    }

    /// Crash points inside `compact` (fault seam for tests; free otherwise).
    fn compaction_step(&self, _step: u64) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(f) = self.opts.faults.as_ref() {
            f.compaction_step(_step)?;
        }
        Ok(())
    }

    /// The one place log bytes are written (fault seam for tests).
    fn write_log(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(n) = self.opts.faults.as_ref().and_then(|f| f.take_write()) {
            self.file.write_all(&bytes[..n.min(bytes.len())])?;
            return Err(io::Error::other("injected write failure"));
        }
        self.file.write_all(bytes)
    }

    /// The one place the log is fsynced (fault seam for tests).
    fn sync_log(&mut self) -> io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        if let Some(f) = self.opts.faults.as_ref() {
            f.before_sync();
            if f.take_sync() {
                return Err(io::Error::other("injected fsync failure"));
            }
        }
        self.file.sync_data()
    }

    /// Records written so far in this generation (next seq to assign - 1).
    pub fn records_written(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn dir(&self) -> &Path {
        &self.root
    }

    /// Compact into a new generation: build `gen-(N+1)` complete — snapshot
    /// covering live state plus a fresh empty log — then atomically swap
    /// the manifest. A crash at any point opens either the old complete
    /// generation or the new one, never a mixture (contract §02).
    pub fn compact(&mut self, snapshot: &Snapshot) -> Result<(), JournalError> {
        let payload = &snapshot.payload;
        self.compact_with(snapshot.covered_seq, payload.len() as u64, |w| {
            w.write_all(payload)
        })
    }

    /// `compact`, with the snapshot payload streamed by `write` (exactly
    /// `payload_len` bytes) straight into the new generation's checksummed
    /// `snapshot.bin` — the caller never needs the whole payload in memory.
    pub fn compact_with(
        &mut self,
        covered_seq: u64,
        payload_len: u64,
        write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
    ) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        // The new generation's snapshot must fit with the reserve intact —
        // checked before anything is written, so a refusal leaves no trace.
        self.ensure_space(&self.root, SNAPSHOT_HEADER_LEN + payload_len)?;
        let next = self.generation + 1;
        let gdir = gen_dir_path(&self.root, next);
        // A `gen-(N+1)` already on disk is what a compaction killed before
        // its manifest swap left behind — but only if the *durable*
        // manifest still names N. Check the file, never assume from memory:
        // if it names anything else, `gen-(N+1)` may be the live generation.
        let on_disk = read_manifest(&self.root)?;
        if on_disk != Some(self.generation) {
            self.poisoned = true;
            return Err(JournalError::ManifestMismatch {
                on_disk: on_disk.unwrap_or(0),
                in_memory: self.generation,
            });
        }
        // Not authority, so remove it rather than refuse every later
        // compaction (`create_new` below would fail on its log).
        match fs::remove_dir_all(&gdir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::create_dir_all(&gdir)?;

        // 1. Fresh log, created and fsynced before the manifest moves. This
        //    handle becomes the writer's file after the swap, so nothing
        //    has to be opened past the point of no return.
        let log_path = gdir.join("journal.log");
        let mut new_log = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // a reused generation is corruption, refuse it
            .open(&log_path)?;
        new_log.sync_data()?;
        self.compaction_step(1)?;

        // 2. Snapshot for the new generation (tmp + rename + dir fsync).
        write_snapshot_stream(&gdir, covered_seq, payload_len, write)?;
        self.compaction_step(2)?;

        // 3. The atomic manifest swap — the single point that moves the
        //    world from generation N to N+1, and the point of no return.
        //    From the rename on, an error leaves the manifest possibly
        //    naming N+1 while this writer points at N: poison, never
        //    continue (appends would go where no restart looks).
        let tmp = self.root.join("manifest.json.tmp");
        fs::write(
            &tmp,
            serde_json::to_vec(&Manifest { generation: next }).map_err(io::Error::other)?,
        )?;
        File::open(&tmp)?.sync_data()?;
        let swapped = fs::rename(&tmp, self.root.join("manifest.json"))
            .and_then(|()| self.compaction_step(3))
            .and_then(|()| sync_parent(&tmp))
            .and_then(|()| new_log.seek(SeekFrom::End(0)).map(|_| ()));
        if let Err(e) = swapped {
            self.poisoned = true;
            return Err(JournalError::ManifestSwapFailed(e));
        }

        // 4. Only now does the writer move: new generation, seq restarts.
        self.file = new_log;
        self.generation = next;
        self.next_seq = 1;
        self.durable_len = 0;

        // 5. Retention: the new generation and its directory entries are
        //    durable and the manifest names it, so every older generation
        //    is superseded. Delete them. A crash part-way leaves leftovers
        //    the next compaction removes; a deletion error never undoes the
        //    compaction — it shows up as `stale_generations`.
        self.prune_superseded();
        Ok(())
    }

    /// Delete every generation directory older than the current one.
    fn prune_superseded(&self) {
        let Ok(entries) = fs::read_dir(&self.root) else {
            return;
        };
        let mut removed = false;
        for entry in entries.flatten() {
            let older = parse_gen(&entry.file_name()).is_some_and(|g| g < self.generation);
            if older && fs::remove_dir_all(entry.path()).is_ok() {
                removed = true;
            }
        }
        if removed {
            // Best effort: make the removals durable too.
            if File::open(&self.root).and_then(|d| d.sync_data()).is_err() {
                // Leftovers after a crash are removed by the next compaction.
            }
        }
    }

    /// Generation directories other than the current one still on disk —
    /// 0 when retention is keeping up.
    pub fn stale_generations(&self) -> Result<u32, JournalError> {
        let mut n = 0;
        for entry in fs::read_dir(&self.root)? {
            if parse_gen(&entry?.file_name()).is_some_and(|g| g != self.generation) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// A consistent copy for backup into `dest`, which must not exist yet
    /// (its parent must), made in `.<name>.partial` and renamed into place
    /// when complete: generation data first, manifest LAST —
    /// a copy interrupted mid-flight still opens as a complete older
    /// generation, never a torn mixed one.
    pub fn backup_to(&self, dest: &Path) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned); // the log may hold an unrolled tail
        }
        // The copy must fit on the destination with the reserve intact.
        let parent = dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = dest
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("backup destination has no usable name"))?;
        self.ensure_space(parent, self.backup_bytes()?)?;
        // A fresh name only: copying into an existing backup could mix
        // generations from two of them.
        if fs::symlink_metadata(dest).is_ok() {
            return Err(JournalError::BackupExists);
        }
        // Copy into a hidden partial directory and rename it into place only
        // when complete, so the name never holds a half backup (a kill -9
        // mid-copy leaves `.<name>.partial`, which the next attempt — or
        // `sweep_partial_backups` at startup — removes; it was never valid).
        let partial = parent.join(format!(".{name}{PARTIAL_SUFFIX}"));
        match fs::remove_dir_all(&partial) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fs::create_dir(&partial)?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let gname = entry.file_name();
            let Some(n) = gname.to_str() else { continue };
            if n.starts_with("gen-") && entry.file_type()?.is_dir() {
                let gd = partial.join(n);
                fs::create_dir_all(&gd)?;
                for f in SNAPSHOT_FILES {
                    let src = entry.path().join(f);
                    if src.exists() {
                        fs::copy(&src, gd.join(f))?;
                        File::open(gd.join(f))?.sync_data()?;
                    }
                }
                File::open(&gd)?.sync_data()?;
            }
        }
        #[cfg(any(test, feature = "fault-injection"))]
        if self
            .opts
            .faults
            .as_ref()
            .is_some_and(|f| f.take_backup_crash())
        {
            return Err(io::Error::other("injected crash during backup").into());
        }
        // Manifest last: even the partial copy opens as one generation.
        fs::copy(
            self.root.join("manifest.json"),
            partial.join("manifest.json"),
        )?;
        File::open(partial.join("manifest.json"))?.sync_data()?;
        File::open(&partial)?.sync_data()?;
        if fs::symlink_metadata(dest).is_ok() {
            return Err(JournalError::BackupExists); // appeared meanwhile
        }
        fs::rename(&partial, dest)?;
        File::open(parent)?.sync_data()?;
        Ok(())
    }
}

impl Journal {
    /// Bytes a backup copies: every generation's log and snapshot plus the
    /// manifest.
    fn backup_bytes(&self) -> Result<u64, JournalError> {
        let mut total = fs::metadata(self.root.join("manifest.json"))?.len();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let is_gen = entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("gen-"));
            if is_gen && entry.file_type()?.is_dir() {
                for f in SNAPSHOT_FILES {
                    match fs::metadata(entry.path().join(f)) {
                        Ok(m) => total += m.len(),
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Ok(total)
    }
}

/// A snapshot accelerates replay: the engine may persist reduced state here
/// and then only journal records after `covered_seq` need replaying. It is a
/// cache, never authority — replay without it must yield the same state.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub covered_seq: u64,
    pub payload: Vec<u8>,
}

/// Suffix of a backup still being copied (hidden: `.<name>.partial`).
const PARTIAL_SUFFIX: &str = ".partial";

/// Remove backups that were interrupted mid-copy (`.<name>.partial`)
/// under `root`; returns how many. They were never complete, and a
/// finished backup never carries the suffix.
pub fn sweep_partial_backups(root: &Path) -> Result<usize, JournalError> {
    let mut removed = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let partial = name
            .to_str()
            .is_some_and(|n| n.starts_with('.') && n.ends_with(PARTIAL_SUFFIX));
        if partial && entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Restore the backup at `from` into `to`, the way `backup_to` wrote it:
/// generation data first, manifest last. `to` is locked like a journal for
/// the whole copy, so a server running on it refuses the restore
/// (`SecondWriter`) and no server can open it half-restored; a `to` that
/// already holds a journal is refused (`RestoreTargetNotEmpty`). Only the
/// backup's own regular files are copied — a symlink or special file in it
/// is refused, never followed.
pub fn restore_backup(from: &Path, to: &Path) -> Result<(), JournalError> {
    let manifest = from.join("manifest.json");
    match fs::symlink_metadata(&manifest) {
        Ok(m) if m.file_type().is_file() => {}
        Ok(_) => {
            return Err(JournalError::BadBackup(
                "manifest.json is not a file".into(),
            ))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(JournalError::BadBackup(
                "no manifest.json: not a journal backup".into(),
            ))
        }
        Err(e) => return Err(e.into()),
    }
    fs::create_dir_all(to)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(to.join("LOCK"))?;
    fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| JournalError::SecondWriter)?;
    for entry in fs::read_dir(to)? {
        let name = entry?.file_name();
        let n = name.to_string_lossy();
        if n == "manifest.json" || n.starts_with("gen-") {
            return Err(JournalError::RestoreTargetNotEmpty);
        }
    }
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let name = entry.file_name();
        if parse_gen(&name).is_none() {
            continue;
        }
        if !entry.file_type()?.is_dir() {
            return Err(JournalError::BadBackup(format!(
                "{} is not a directory",
                name.to_string_lossy()
            )));
        }
        let gd = to.join(&name);
        fs::create_dir(&gd)?;
        for f in fs::read_dir(entry.path())? {
            let f = f?;
            let fname = f.file_name();
            let known = SNAPSHOT_FILES.iter().any(|k| fname == *k);
            if !known {
                continue; // a leftover `.tmp`: never part of a generation
            }
            if !f.file_type()?.is_file() {
                return Err(JournalError::BadBackup(format!(
                    "{}/{} is not a regular file",
                    name.to_string_lossy(),
                    fname.to_string_lossy()
                )));
            }
            copy_file_durably(&f.path(), &gd.join(&fname))?;
        }
        File::open(&gd)?.sync_data()?;
    }
    // Manifest last, by rename: until it lands, `to` opens as nothing.
    let tmp = to.join("manifest.json.tmp");
    copy_file_durably(&manifest, &tmp)?;
    fs::rename(&tmp, to.join("manifest.json"))?;
    File::open(to)?.sync_data()?;
    drop(lock);
    Ok(())
}

/// Copy a regular file into a new file (never through a symlink at the
/// destination) and fsync it.
fn copy_file_durably(src: &Path, dest: &Path) -> io::Result<()> {
    let mut input = OpenOptions::new().read(true).open(src)?;
    let mut out = new_file(dest)?;
    io::copy(&mut input, &mut out)?;
    out.sync_data()
}

/// Create a new file the journal owns: refuses an existing path (so never
/// writes through a planted symlink) and, on Unix, is owner-only (0600).
fn new_file(path: &Path) -> io::Result<File> {
    let mut o = OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
    o.open(path)
}

/// Files a generation directory may hold (backup copies exactly these).
const SNAPSHOT_FILES: [&str; 3] = ["journal.log", "snapshot.bin", "snapshot.json"];

/// `snapshot.bin`: `[magic 8][covered_seq u64][payload_len u64][crc32 u32]
/// [payload]`, big-endian. The magic carries the frame version. The snapshot
/// is the only authority for compacted state, so its checksum is checked on
/// every load; a mismatch stops startup.
const SNAPSHOT_MAGIC: &[u8; 8] = b"DRSNAP\x00\x02";
const SNAPSHOT_HEADER_LEN: u64 = 8 + 8 + 8 + 4;

/// Counts and checksums what passes through to the file.
struct Checked<W> {
    inner: W,
    crc: crc32fast::Hasher,
    len: u64,
}

impl<W: Write> Write for Checked<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Atomically write the generation's snapshot (tmp + rename + dir fsync).
pub fn write_snapshot(dir: &Path, snap: &Snapshot) -> Result<(), JournalError> {
    let payload = &snap.payload;
    write_snapshot_stream(dir, snap.covered_seq, payload.len() as u64, |w| {
        w.write_all(payload)
    })
}

fn write_snapshot_stream(
    dir: &Path,
    covered_seq: u64,
    payload_len: u64,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> Result<(), JournalError> {
    let tmp = dir.join("snapshot.bin.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&[0u8; SNAPSHOT_HEADER_LEN as usize])?; // patched below
    let (len, crc) = {
        let mut out = Checked {
            inner: io::BufWriter::with_capacity(256 * 1024, &mut file),
            crc: crc32fast::Hasher::new(),
            len: 0,
        };
        write(&mut out)?;
        let Checked { inner, crc, len } = out;
        inner.into_inner().map_err(io::IntoInnerError::into_error)?;
        (len, crc.finalize())
    };
    if len != payload_len {
        return Err(JournalError::CorruptSnapshot(format!(
            "encoder wrote {len} bytes, promised {payload_len}"
        )));
    }
    let mut header = Vec::with_capacity(SNAPSHOT_HEADER_LEN as usize);
    header.extend_from_slice(SNAPSHOT_MAGIC);
    header.extend_from_slice(&covered_seq.to_be_bytes());
    header.extend_from_slice(&len.to_be_bytes());
    header.extend_from_slice(&crc.to_be_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.sync_data()?;
    drop(file);
    fs::rename(&tmp, dir.join("snapshot.bin"))?;
    sync_parent(&tmp)?;
    Ok(())
}

/// A generation's snapshot as found on disk.
pub enum SnapshotSource {
    /// `snapshot.bin`, streamed: read the payload, then `finish` verifies
    /// its checksum — before anything decoded from it is used.
    Binary(SnapshotReader),
    /// A legacy `snapshot.json` (before D-021), read whole; no checksum.
    Legacy(Snapshot),
}

/// The payload of a `snapshot.bin`, read through a bounded buffer with a
/// running CRC: memory stays a buffer, not the snapshot's size (D-028).
pub struct SnapshotReader {
    r: io::BufReader<File>,
    left: u64,
    crc: crc32fast::Hasher,
    want: u32,
    covered_seq: u64,
}

impl SnapshotReader {
    /// Payload bytes not yet read.
    pub fn remaining(&self) -> u64 {
        self.left
    }

    pub fn covered_seq(&self) -> u64 {
        self.covered_seq
    }

    /// Verify that the whole payload was read and its checksum matches.
    pub fn finish(self) -> Result<u64, JournalError> {
        if self.left != 0 {
            return Err(JournalError::CorruptSnapshot(format!(
                "{} payload bytes were not read",
                self.left
            )));
        }
        if self.crc.finalize() != self.want {
            return Err(JournalError::CorruptSnapshot("checksum mismatch".into()));
        }
        Ok(self.covered_seq)
    }
}

impl Read for SnapshotReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let room = buf
            .len()
            .min(usize::try_from(self.left).unwrap_or(usize::MAX));
        if room == 0 {
            return Ok(0);
        }
        let n = self.r.read(&mut buf[..room])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "snapshot shorter than its header says",
            ));
        }
        self.crc.update(&buf[..n]);
        self.left -= n as u64;
        Ok(n)
    }
}

/// Open the generation's snapshot, if any, for a streamed, verified read.
pub fn open_snapshot(dir: &Path) -> Result<Option<SnapshotSource>, JournalError> {
    match File::open(dir.join("snapshot.bin")) {
        Ok(f) => return open_binary(f).map(|r| Some(SnapshotSource::Binary(r))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    match fs::read(dir.join("snapshot.json")) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(|s| Some(SnapshotSource::Legacy(s)))
            .map_err(|e| JournalError::CorruptSnapshot(format!("legacy snapshot: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn open_binary(f: File) -> Result<SnapshotReader, JournalError> {
    let bad = |why: &str| JournalError::CorruptSnapshot(why.to_string());
    let file_len = f.metadata()?.len();
    if file_len < SNAPSHOT_HEADER_LEN {
        return Err(bad("shorter than its header"));
    }
    let mut r = io::BufReader::with_capacity(256 * 1024, f);
    let mut header = [0u8; SNAPSHOT_HEADER_LEN as usize];
    r.read_exact(&mut header)?;
    if &header[..8] != SNAPSHOT_MAGIC {
        return Err(bad("unknown magic or frame version"));
    }
    let word = |range: std::ops::Range<usize>| -> u64 {
        header[range]
            .iter()
            .fold(0u64, |acc, b| (acc << 8) | u64::from(*b))
    };
    let (covered_seq, len, want) = (word(8..16), word(16..24), word(24..28) as u32);
    if file_len - SNAPSHOT_HEADER_LEN != len {
        return Err(bad("payload length does not match its header"));
    }
    Ok(SnapshotReader {
        r,
        left: len,
        crc: crc32fast::Hasher::new(),
        want,
        covered_seq,
    })
}

/// Load the generation's snapshot whole, verified (tests and tools; the
/// engine streams through `open_snapshot`).
pub fn load_snapshot(dir: &Path) -> Result<Option<Snapshot>, JournalError> {
    match open_snapshot(dir)? {
        None => Ok(None),
        Some(SnapshotSource::Legacy(s)) => Ok(Some(s)),
        Some(SnapshotSource::Binary(mut r)) => {
            let mut payload = Vec::new();
            r.read_to_end(&mut payload)?;
            let covered_seq = r.finish()?;
            Ok(Some(Snapshot {
                covered_seq,
                payload,
            }))
        }
    }
}

/// Replay only records after `from_seq` (used with a snapshot).
pub fn replay_dir_from(dir: &Path, from_seq: u64) -> Result<Vec<Record>, JournalError> {
    Ok(replay_dir(dir)?
        .into_iter()
        .filter(|r| r.seq > from_seq)
        .collect())
}

/// fsync a directory, so the entries created in it are durable.
fn sync_dir(_opts: &JournalOptions, dir: &Path) -> io::Result<()> {
    #[cfg(any(test, feature = "fault-injection"))]
    if let Some(f) = _opts.faults.as_ref() {
        f.record_dir_sync(dir);
    }
    File::open(dir)?.sync_data()
}

fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_data()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_replay_round_trips() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let seq;
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            seq = [
                j.append(b"one").unwrap_or_else(|e| panic!("{e}")),
                j.append(b"two").unwrap_or_else(|e| panic!("{e}")),
            ];
        }
        assert_eq!(seq, [1, 2]);
        let rs = replay_dir(&gen_dir_path(dir.path(), 1)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].payload, b"one");
    }

    #[test]
    fn second_writer_refused() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let _j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            Journal::open(dir.path()),
            Err(JournalError::SecondWriter)
        ));
    }

    #[test]
    fn torn_tail_is_discarded_history_is_kept() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(b"committed").unwrap_or_else(|e| panic!("{e}"));
            j.append(b"also committed")
                .unwrap_or_else(|e| panic!("{e}"));
        }
        // Simulate a crash mid-append: garbage bytes glued on the end.
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap_or_else(|e| panic!("{e}"))
            .write_all(&[0xde, 0xad, 0xbe])
            .unwrap_or_else(|e| panic!("{e}"));
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(b"three").unwrap_or_else(|e| panic!("{e}"));
        }
        let rs = replay_dir(&gen_dir_path(dir.path(), 1)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(rs.len(), 3);
        assert_eq!(rs[2].payload, b"three");
    }

    #[test]
    fn corrupt_committed_history_stops_startup() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(b"first").unwrap_or_else(|e| panic!("{e}"));
            j.append(b"second").unwrap_or_else(|e| panic!("{e}"));
        }
        // Flip a byte in the middle of record 1's payload.
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log)
            .unwrap_or_else(|e| panic!("{e}"));
        f.seek(SeekFrom::Start(HEADER_LEN as u64))
            .unwrap_or_else(|e| panic!("{e}"));
        f.write_all(&[0xFF]).unwrap_or_else(|e| panic!("{e}"));
        match Journal::open(dir.path()) {
            Err(JournalError::Corrupt { seq: 1, .. }) => {}
            other => panic!("expected Corrupt at seq 1, got {other:?}"),
        }
    }

    fn open_faulty(dir: &Path) -> (Journal, Arc<faults::Faults>) {
        let f = Arc::new(faults::Faults::default());
        let j = Journal::open_with(
            dir,
            JournalOptions {
                faults: Some(f.clone()),
                ..JournalOptions::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        (j, f)
    }

    fn payloads(dir: &Path) -> Vec<(u64, Vec<u8>)> {
        replay_dir(&gen_dir_path(dir, 1))
            .unwrap_or_else(|e| panic!("history must replay cleanly: {e}"))
            .into_iter()
            .map(|r| (r.seq, r.payload))
            .collect()
    }

    /// A write cut short (ENOSPC, EIO) leaves part of a frame on disk.
    /// Without a rollback the next append lands behind those bytes with the
    /// same sequence number and the next startup refuses the journal as
    /// corrupt. The partial bytes must be cut back to the durable offset.
    #[test]
    fn partial_write_is_rolled_back_and_history_stays_replayable() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let (mut j, f) = open_faulty(dir.path());
            assert_eq!(j.append(b"one").unwrap_or_else(|e| panic!("{e}")), 1);
            f.fail_next_write_after(7); // header half-written
            assert!(
                matches!(
                    j.append(b"two"),
                    Err(JournalError::WriteFailed {
                        rolled_back: true,
                        ..
                    })
                ),
                "a failed write is not a commit"
            );
            assert!(!j.is_poisoned(), "a rolled-back write needs no restart");
            assert_eq!(
                j.append(b"three").unwrap_or_else(|e| panic!("{e}")),
                2,
                "the failed record never consumed a sequence number"
            );
        }
        let rs = payloads(dir.path());
        assert_eq!(rs, vec![(1, b"one".to_vec()), (2, b"three".to_vec())]);
        Journal::open(dir.path()).unwrap_or_else(|e| panic!("reopen: {e}"));
    }

    /// After a failed fsync the kernel may already have dropped the dirty
    /// pages, so a retried fsync can report success for lost data. The
    /// writer must refuse every later append (poisoned) so the process
    /// restarts and replays; the record it could not make durable must not
    /// share a sequence number with anything written later.
    #[test]
    fn failed_fsync_poisons_the_writer_and_replay_stays_clean() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let (mut j, f) = open_faulty(dir.path());
            j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
            f.fail_next_sync();
            assert!(
                matches!(j.append(b"two"), Err(JournalError::SyncFailed(_))),
                "an unsynced record is not a commit"
            );
            assert!(
                matches!(j.append(b"three"), Err(JournalError::Poisoned)),
                "a writer whose fsync failed must refuse further appends"
            );
            assert!(j.is_poisoned());
        }
        // Restart: replay sees only durable history, and the writer works.
        assert_eq!(payloads(dir.path()), vec![(1, b"one".to_vec())]);
        let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("reopen: {e}"));
        assert_eq!(j.append(b"four").unwrap_or_else(|e| panic!("{e}")), 2);
    }

    /// If the rollback itself fails the file may still hold partial bytes:
    /// the writer must poison rather than append behind them.
    #[test]
    fn failed_rollback_poisons_the_writer() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let (mut j, f) = open_faulty(dir.path());
            j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
            f.fail_next_write_after(5);
            f.fail_next_truncate();
            assert!(matches!(
                j.append(b"two"),
                Err(JournalError::WriteFailed {
                    rolled_back: false,
                    ..
                })
            ));
            assert!(
                matches!(j.append(b"three"), Err(JournalError::Poisoned)),
                "partial bytes may remain: appending behind them corrupts history"
            );
        }
        // The partial frame is a torn tail: discarded at reopen, history kept.
        let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("reopen: {e}"));
        assert_eq!(j.append(b"four").unwrap_or_else(|e| panic!("{e}")), 2);
        assert_eq!(
            payloads(dir.path()),
            vec![(1, b"one".to_vec()), (2, b"four".to_vec())]
        );
    }

    /// A disk that is "full" by a tiny reserve: an injected probe reports
    /// a fixed number of free bytes.
    fn open_with_free(dir: &Path, free: Arc<std::sync::atomic::AtomicU64>) -> Journal {
        Journal::open_with(
            dir,
            JournalOptions {
                reserve_bytes: 4096,
                space: SpaceProbe::new(move |_| Ok(free.load(std::sync::atomic::Ordering::SeqCst))),
                ..JournalOptions::default()
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Appends, compaction and backup all keep the reserve: each refuses
    /// with `NoSpace` before writing anything when what it would write
    /// does not fit above the reserve.
    #[test]
    fn appends_compaction_and_backup_respect_the_reserve() {
        use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let free = Arc::new(AtomicU64::new(1 << 20));
        let mut j = open_with_free(dir.path(), free.clone());
        j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
        assert!(j.space_ok());

        // 64 bytes of headroom: a small record fits, a big snapshot or a
        // backup of the whole journal does not.
        free.store(4096 + 64, SeqCst);
        j.append(b"two").unwrap_or_else(|e| panic!("{e}"));
        let big = Snapshot {
            covered_seq: 0,
            payload: vec![7; 512],
        };
        assert!(matches!(j.compact(&big), Err(JournalError::NoSpace { .. })));
        assert!(
            !gen_dir_path(dir.path(), 2).exists(),
            "a refused compaction leaves no partial generation"
        );
        assert_eq!(j.generation(), 1);
        free.store(4096 + 16, SeqCst);
        let dest = dir.path().join("backup");
        assert!(matches!(
            j.backup_to(&dest),
            Err(JournalError::NoSpace { .. })
        ));
        assert!(!dest.exists(), "a refused backup leaves nothing behind");

        // Below the reserve nothing is appended and readiness says so.
        free.store(4000, SeqCst);
        assert!(!j.space_ok());
        assert!(matches!(
            j.append(b"three"),
            Err(JournalError::NoSpace { .. })
        ));

        // Space returns: everything works again, history is intact.
        free.store(1 << 20, SeqCst);
        assert!(j.space_ok());
        assert_eq!(
            payloads(dir.path()).len(),
            2,
            "no refused write left a record"
        );
        j.compact(&big).unwrap_or_else(|e| panic!("{e}"));
        j.backup_to(&dest).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(j.generation(), 2);
        assert!(dest.join("gen-000002/snapshot.bin").exists());
    }

    /// A batch is consecutive frames with contiguous sequence numbers,
    /// written and fsynced once; a failed batch commits none of them and
    /// consumes no sequence numbers.
    #[test]
    fn batch_appends_are_contiguous_and_all_or_nothing() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        {
            let (mut j, f) = open_faulty(dir.path());
            assert_eq!(j.append(b"a").unwrap_or_else(|e| panic!("{e}")), 1);
            let batch = vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
            assert_eq!(j.append_batch(&batch).unwrap_or_else(|e| panic!("{e}")), 4);
            assert_eq!(f.sync_calls(), 2, "one fsync per batch");
            f.fail_next_write_after(30); // inside the second frame
            assert!(matches!(
                j.append_batch(&[b"x".to_vec(), b"y".to_vec()]),
                Err(JournalError::WriteFailed {
                    rolled_back: true,
                    ..
                })
            ));
            assert_eq!(
                j.append_batch(&[b"e".to_vec()])
                    .unwrap_or_else(|e| panic!("{e}")),
                5
            );
            f.fail_next_sync();
            assert!(matches!(
                j.append_batch(&[b"z".to_vec()]),
                Err(JournalError::SyncFailed(_))
            ));
            let syncs = f.sync_calls();
            assert!(matches!(
                j.append_batch(&[b"w".to_vec()]),
                Err(JournalError::Poisoned)
            ));
            assert_eq!(f.sync_calls(), syncs, "a failed fsync is never retried");
        }
        let got: Vec<Vec<u8>> = payloads(dir.path()).into_iter().map(|(_, p)| p).collect();
        assert_eq!(got, [&b"a"[..], b"b", b"c", b"d", b"e"].map(<[u8]>::to_vec));
    }

    fn gen_dirs(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| n.starts_with("gen-"))
            .collect();
        v.sort();
        v
    }

    /// Retention: once a compaction's new generation is current and
    /// durable, every older generation is superseded and deleted — the
    /// disk holds one generation, not every one ever written.
    #[test]
    fn compaction_deletes_superseded_generations() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        let snap = || Snapshot {
            covered_seq: 0,
            payload: b"state".to_vec(),
        };
        j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
        j.compact(&snap()).unwrap_or_else(|e| panic!("{e}"));
        j.append(b"two").unwrap_or_else(|e| panic!("{e}"));
        j.compact(&snap()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(gen_dirs(dir.path()), vec!["gen-000003".to_string()]);
        drop(j);
        let j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(j.generation(), 3);
    }

    /// Review finding (high): the leftover cleanup used to delete
    /// `gen-(N+1)` on the strength of the in-memory generation alone. If the
    /// durable manifest already names N+1, that directory is the live
    /// generation. The cleanup must check the manifest on disk, refuse, and
    /// leave it untouched.
    #[test]
    fn leftover_cleanup_never_deletes_the_generation_the_manifest_names() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
        // The durable manifest moves to generation 2 behind this writer's
        // back (what a failed swap used to leave), with real content.
        let g2 = gen_dir_path(dir.path(), 2);
        fs::create_dir_all(&g2).unwrap_or_else(|e| panic!("{e}"));
        fs::write(g2.join("journal.log"), b"").unwrap_or_else(|e| panic!("{e}"));
        write_snapshot(
            &g2,
            &Snapshot {
                covered_seq: 0,
                payload: b"the live state".to_vec(),
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        fs::write(dir.path().join("manifest.json"), "{\"generation\":2}")
            .unwrap_or_else(|e| panic!("{e}"));

        let snap = Snapshot {
            covered_seq: 0,
            payload: b"stale".to_vec(),
        };
        assert!(
            j.compact(&snap).is_err(),
            "memory and manifest disagree: refuse"
        );
        assert!(j.is_poisoned(), "and never write again");
        let kept = load_snapshot(&g2)
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("the live generation's snapshot was deleted"));
        assert_eq!(kept.payload, b"the live state");
    }

    /// The snapshot is the only authority for compacted state, so it is
    /// checksummed: a damaged one stops startup instead of being trusted.
    #[test]
    fn damaged_snapshot_is_refused_not_trusted() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        let snap = Snapshot {
            covered_seq: 0,
            payload: vec![7u8; 4096],
        };
        j.compact(&snap).unwrap_or_else(|e| panic!("{e}"));
        drop(j);
        let file = gen_dir_path(dir.path(), 2).join("snapshot.bin");
        let mut bytes = fs::read(&file).unwrap_or_else(|e| panic!("binary snapshot: {e}"));
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01; // one flipped bit in the payload
        fs::write(&file, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            matches!(
                load_snapshot(&gen_dir_path(dir.path(), 2)),
                Err(JournalError::CorruptSnapshot(_))
            ),
            "a damaged snapshot must not load"
        );
    }

    /// Review finding 8 (D-014's named gap): on first open the new
    /// generation directory and its `journal.log` were created but their
    /// directory entries never fsynced, so under power loss jobs
    /// acknowledged before the first compaction could vanish with the
    /// entry. Both directories must be fsynced before the first append.
    #[test]
    fn first_open_makes_its_directory_entries_durable() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let root = dir.path().join("journal");
        let (_j, f) = open_faulty(&root);
        let synced = f.dir_syncs();
        let gdir = gen_dir_path(&root, 1);
        assert!(
            synced.contains(&gdir),
            "gen-000001 (holding journal.log's entry) not fsynced: {synced:?}"
        );
        let last_root = synced.iter().rposition(|p| *p == root);
        let gen_created = synced.iter().position(|p| *p == gdir);
        assert!(
            matches!((last_root, gen_created), (Some(r), Some(g)) if r > g),
            "the root (holding gen-000001's entry) must be fsynced after the generation exists: {synced:?}"
        );
    }

    /// Review finding 12: a backup killed mid-copy left a directory with no
    /// manifest under the backup's name, forever (later attempts got
    /// `BackupExists`). A backup's name must appear only once it is
    /// complete, and an interrupted one must leave the name free.
    #[test]
    fn interrupted_backup_leaves_no_half_backup_and_the_name_free() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let out = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let (mut j, f) = open_faulty(dir.path());
        j.append(b"one").unwrap_or_else(|e| panic!("{e}"));
        let dest = out.path().join("nightly");
        f.crash_next_backup();
        assert!(j.backup_to(&dest).is_err());
        assert!(!dest.exists(), "a half backup sits under the name");
        j.backup_to(&dest)
            .unwrap_or_else(|e| panic!("the name stays free for a retry: {e}"));
        assert!(dest.join("manifest.json").exists());
        let leftovers: Vec<_> = fs::read_dir(out.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("nightly")]);
    }

    #[test]
    fn partial_backups_are_swept_and_finished_ones_kept() {
        let out = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        for d in [".t0.partial", ".t1.partial", "nightly", ".keep"] {
            fs::create_dir(out.path().join(d)).unwrap_or_else(|e| panic!("{e}"));
        }
        assert_eq!(
            sweep_partial_backups(out.path()).unwrap_or_else(|e| panic!("{e}")),
            2
        );
        assert!(out.path().join("nightly").exists());
        assert!(out.path().join(".keep").exists());
        assert!(!out.path().join(".t0.partial").exists());
    }

    /// The streamed snapshot reader refuses a file whose length disagrees
    /// with its header before reading the payload, and verifies the
    /// checksum only once every byte has been read.
    #[test]
    fn streamed_snapshot_checks_length_and_checksum() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let snap = Snapshot {
            covered_seq: 3,
            payload: (0..5000u32).map(|i| (i % 251) as u8).collect(),
        };
        write_snapshot(dir.path(), &snap).unwrap_or_else(|e| panic!("{e}"));
        let Some(SnapshotSource::Binary(mut r)) =
            open_snapshot(dir.path()).unwrap_or_else(|e| panic!("{e}"))
        else {
            panic!("binary snapshot expected")
        };
        let mut half = vec![0u8; 2500];
        r.read_exact(&mut half).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            matches!(r.finish(), Err(JournalError::CorruptSnapshot(_))),
            "a partly read snapshot is not verified"
        );
        let file = dir.path().join("snapshot.bin");
        let mut bytes = fs::read(&file).unwrap_or_else(|e| panic!("{e}"));
        bytes.pop();
        fs::write(&file, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            open_snapshot(dir.path()),
            Err(JournalError::CorruptSnapshot(_))
        ));
    }

    /// A JSON-like payload, as the engine writes (never a 0x00 byte).
    fn record(i: usize) -> Vec<u8> {
        format!("{{\"run\":{i},\"at\":1700000000000,\"event\":\"accepted-{i:04}\"}}").into_bytes()
    }

    /// A gen-1 log of `n` single-record appends; returns each frame's
    /// start offset and the log path.
    fn log_of(dir: &Path, n: usize) -> (Vec<u64>, PathBuf) {
        let mut j = Journal::open(dir).unwrap_or_else(|e| panic!("{e}"));
        let mut starts = Vec::new();
        let mut at = 0u64;
        for i in 0..n {
            starts.push(at);
            let p = record(i);
            at += (HEADER_LEN + p.len()) as u64;
            j.append(&p).unwrap_or_else(|e| panic!("{e}"));
        }
        (starts, gen_dir_path(dir, 1).join("journal.log"))
    }

    fn set_len_field(log: &Path, frame_start: u64, len: u32) {
        let mut f = OpenOptions::new()
            .write(true)
            .open(log)
            .unwrap_or_else(|e| panic!("{e}"));
        f.seek(SeekFrom::Start(frame_start + 2))
            .unwrap_or_else(|e| panic!("{e}"));
        f.write_all(&len.to_be_bytes())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// Review T1 (high): a corrupted length field in a committed frame
    /// that points past the end of the file was taken for a torn tail.
    /// Startup dropped that record and every acknowledged one after it,
    /// and truncated them away (the reviewer: 25 of 50 served, the log
    /// cut from 10,181 to 5,081 bytes). A damaged length anywhere — the
    /// first frame, the reviewer's frame 26, the last — must stop startup
    /// with `Corrupt` and leave the log byte for byte as it was.
    #[test]
    fn corrupted_frame_length_stops_startup_and_leaves_the_log_unchanged() {
        for target in [0usize, 25, 48, 49] {
            let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
            let (starts, log) = log_of(dir.path(), 50);
            let file_len = fs::metadata(&log).unwrap_or_else(|e| panic!("{e}")).len();
            let real = record(target).len() as u32;
            for bad in [
                16 * 1024 * 1024 - 1, // the reviewer's value: under the cap, past the end
                file_len as u32,      // the reviewer's other value
                real + 1,             // just past its own payload
                real * 3,
            ] {
                set_len_field(&log, starts[target], bad);
                let before = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
                match Journal::open(dir.path()) {
                    Err(JournalError::Corrupt { .. }) => {}
                    other => {
                        panic!("frame {target} with length {bad}: expected Corrupt, got {other:?}")
                    }
                }
                let after = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
                assert!(
                    before == after,
                    "frame {target} with length {bad}: startup changed the log ({} -> {} bytes)",
                    before.len(),
                    after.len()
                );
            }
        }
    }

    /// A genuine torn tail — the file ends inside the last frame, at any
    /// byte — is still discarded, every complete frame before it is kept,
    /// and the writer appends on a clean boundary.
    #[test]
    fn a_genuine_torn_tail_is_still_recovered_at_every_cut() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let (starts, log) = log_of(dir.path(), 10);
        let whole = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        let last = starts[9] as usize;
        for cut in last + 1..whole.len() {
            fs::write(&log, &whole[..cut]).unwrap_or_else(|e| panic!("{e}"));
            {
                let mut j = Journal::open(dir.path())
                    .unwrap_or_else(|e| panic!("cut at {cut} of {}: {e}", whole.len()));
                assert_eq!(j.records_written(), 9, "cut at {cut}");
                assert_eq!(j.append(&record(9)).unwrap_or_else(|e| panic!("{e}")), 10);
            }
            let rs = replay_dir(&gen_dir_path(dir.path(), 1)).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(rs.len(), 10, "cut at {cut}");
            assert_eq!(rs[9].payload, record(9));
        }
    }

    /// A batch torn by process death keeps its complete frames: the kernel
    /// keeps written bytes in order, so the file ends inside one frame.
    #[test]
    fn a_batch_torn_mid_write_keeps_its_complete_frames() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append_batch(&(0..4).map(record).collect::<Vec<_>>())
                .unwrap_or_else(|e| panic!("{e}"));
            j.append_batch(&(4..9).map(record).collect::<Vec<_>>())
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let whole = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        let frame = |i: usize| (HEADER_LEN + record(i).len()) as u64;
        let seventh: u64 = (0..6).map(frame).sum(); // inside the second batch
        fs::write(&log, &whole[..seventh as usize + 5]).unwrap_or_else(|e| panic!("{e}"));
        let j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(j.records_written(), 6);
        assert_eq!(
            fs::metadata(&log).unwrap_or_else(|e| panic!("{e}")).len(),
            seventh
        );
    }

    /// A gen-1 journal whose log holds exactly `bytes` (hand-framed).
    fn journal_with_log(bytes: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        fs::create_dir_all(log.parent().unwrap_or_else(|| panic!("parent")))
            .unwrap_or_else(|e| panic!("{e}"));
        fs::write(dir.path().join("manifest.json"), "{\"generation\":1}")
            .unwrap_or_else(|e| panic!("{e}"));
        fs::write(&log, bytes).unwrap_or_else(|e| panic!("{e}"));
        (dir, log)
    }

    /// A frame as builds before D-032 wrote it.
    fn v1_frame(out: &mut Vec<u8>, seq: u64, payload: &[u8]) {
        out.extend_from_slice(&V1.to_be_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&crc(payload).to_be_bytes());
        out.extend_from_slice(payload);
    }

    /// An interrupted restore leaves generations without a manifest; that
    /// directory must not open as a fresh (or half) journal.
    #[test]
    fn generations_without_a_manifest_never_open_as_a_new_journal() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let g = gen_dir_path(dir.path(), 1);
        fs::create_dir_all(&g).unwrap_or_else(|e| panic!("{e}"));
        let mut half = Vec::new();
        put_frame(&mut half, 1, (HEADER_LEN + 3) as u32, b"one");
        fs::write(g.join("journal.log"), &half[..half.len() - 1]).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            Journal::open(dir.path()),
            Err(JournalError::Manifest(_))
        ));
        assert!(!dir.path().join("manifest.json").exists());
    }

    #[test]
    fn sequence_gap_is_corruption() {
        // Hand-frame two records with seq 1 then 3.
        let mut bytes = Vec::new();
        for (seq, payload) in [(1u64, &b"a"[..]), (3u64, &b"b"[..])] {
            put_frame(&mut bytes, seq, (HEADER_LEN + 1) as u32, payload);
        }
        let (dir, _log) = journal_with_log(&bytes);
        match Journal::open(dir.path()) {
            Err(JournalError::Corrupt { seq: 2, why }) if why.contains("sequence 3") => {}
            other => panic!("expected Corrupt where seq 2 belongs, got {other:?}"),
        }
    }

    /// D-032 migration: a journal written before frame v2 still opens and
    /// replays; new appends are v2 frames after the v1 ones, and the mixed
    /// log reopens.
    #[test]
    fn a_v1_journal_opens_and_continues_in_v2() {
        let mut bytes = Vec::new();
        for i in 0..5 {
            v1_frame(&mut bytes, i as u64 + 1, &record(i));
        }
        let (dir, log) = journal_with_log(&bytes);
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(j.records_written(), 5);
            assert_eq!(j.append(&record(5)).unwrap_or_else(|e| panic!("{e}")), 6);
            j.append_batch(&[record(6), record(7)])
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let rs = replay_dir(&gen_dir_path(dir.path(), 1)).unwrap_or_else(|e| panic!("{e}"));
        let want: Vec<Vec<u8>> = (0..8).map(record).collect();
        assert_eq!(rs.into_iter().map(|r| r.payload).collect::<Vec<_>>(), want);
        let on_disk = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            &on_disk[..bytes.len()],
            &bytes[..],
            "the v1 frames are untouched"
        );
        assert_eq!(on_disk[bytes.len() + 1], 2, "new frames are v2");
        Journal::open(dir.path()).unwrap_or_else(|e| panic!("reopen: {e}"));
    }

    /// v1 frames carry no header checksum, so their length cannot be
    /// trusted. The reviewer's repro on a v1 log: a length pointing past the
    /// end with later frames behind it is corruption, not a torn tail.
    #[test]
    fn a_v1_frame_with_a_damaged_length_and_frames_after_it_stops_startup() {
        let mut bytes = Vec::new();
        let mut starts = Vec::new();
        for i in 0..50 {
            starts.push(bytes.len() as u64);
            v1_frame(&mut bytes, i as u64 + 1, &record(i));
        }
        for bad in [16 * 1024 * 1024 - 1, bytes.len() as u32] {
            let (dir, log) = journal_with_log(&bytes);
            set_len_field(&log, starts[25], bad);
            let before = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
            assert!(
                matches!(
                    Journal::open(dir.path()),
                    Err(JournalError::Corrupt { seq: 26, .. })
                ),
                "length {bad}"
            );
            assert!(before == fs::read(&log).unwrap_or_else(|e| panic!("{e}")));
        }
        // A v1 log torn inside its last frame is still a torn tail.
        let (dir, log) = journal_with_log(&bytes[..bytes.len() - 3]);
        let j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(j.records_written(), 49);
        assert_eq!(
            j.discarded_tail().map(|t| t.offset),
            Some(starts[49]),
            "the cut is reported"
        );
        assert_eq!(
            fs::metadata(&log).unwrap_or_else(|e| panic!("{e}")).len(),
            starts[49]
        );
    }

    /// A damaged sequence number or batch length is caught by the header
    /// checksum, wherever the frame is; so is a v1 frame after v2 ones.
    #[test]
    fn header_damage_and_impossible_layouts_are_corruption() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let (starts, log) = log_of(dir.path(), 5);
        let whole = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        for (field, at) in [("seq", 6usize), ("batch_len", 14), ("payload crc", 18)] {
            for frame in [0usize, 2, 4] {
                let mut bytes = whole.clone();
                bytes[starts[frame] as usize + at + 1] ^= 0x10;
                fs::write(&log, &bytes).unwrap_or_else(|e| panic!("{e}"));
                assert!(
                    matches!(Journal::open(dir.path()), Err(JournalError::Corrupt { .. })),
                    "{field} damaged in frame {frame}"
                );
                assert!(bytes == fs::read(&log).unwrap_or_else(|e| panic!("{e}")));
            }
        }
        // A v1 frame after a v2 one: no build writes that.
        let mut bytes = whole.clone();
        v1_frame(&mut bytes, 6, &record(5));
        fs::write(&log, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            Journal::open(dir.path()),
            Err(JournalError::Corrupt { .. })
        ));
        // A frame claiming a batch it does not fit (headers re-checksummed,
        // so only the layout rule can catch it).
        let mut bytes = Vec::new();
        put_frame(&mut bytes, 1, 2 * (HEADER_LEN + 1) as u32, b"a");
        put_frame(&mut bytes, 2, (HEADER_LEN + 1) as u32, b"b");
        let (dir, _log) = journal_with_log(&bytes);
        assert!(matches!(
            Journal::open(dir.path()),
            Err(JournalError::Corrupt { .. })
        ));
    }

    /// Frames record their batch: a batch written in one call tiles its
    /// length exactly, and a single append is a batch of one.
    #[test]
    fn frames_record_the_batch_they_were_written_in() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        {
            let mut j = Journal::open(dir.path()).unwrap_or_else(|e| panic!("{e}"));
            j.append(b"x").unwrap_or_else(|e| panic!("{e}"));
            j.append_batch(&[b"ab".to_vec(), b"cde".to_vec()])
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let b = fs::read(&log).unwrap_or_else(|e| panic!("{e}"));
        let batch_len = |at: usize| be_u32(&b[at + 14..]);
        let one = HEADER_LEN + 1;
        assert_eq!(batch_len(0) as usize, one);
        let two = 2 * HEADER_LEN + 5;
        assert_eq!(batch_len(one) as usize, two);
        assert_eq!(batch_len(one + HEADER_LEN + 2) as usize, two);
        assert_eq!(b.len(), one + two);
    }
}
