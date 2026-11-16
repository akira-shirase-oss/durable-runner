//! The durable journal: framed append-only records behind one exclusive
//! writer. Layout:
//!
//! ```text
//! <dir>/LOCK                      exclusive writer lock (flock, non-blocking)
//! <dir>/manifest.json             { "generation": N }
//! <dir>/gen-00000N/journal.log    append-only framed records
//! <dir>/gen-00000N/snapshot.json  optional checkpoint (accelerator only)
//! ```
//!
//! Frame: `[version u16][len u32][seq u64][crc32 u32][payload len]` — big
//! endian headers. Replay trusts committed history absolutely: only a
//! demonstrably incomplete *tail* frame may be discarded; corruption earlier
//! in the file stops startup (contract §02).

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

    /// Each armed fault fires once, on the next matching operation.
    #[derive(Debug, Default)]
    pub struct Faults {
        // 0 = disarmed; n = write n-1 bytes of the next write, then fail.
        partial_write: AtomicU64,
        fail_sync: AtomicBool,
        fail_truncate: AtomicBool,
    }

    impl Faults {
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

/// How a journal is opened. `Default` is the production configuration.
#[derive(Debug, Clone, Default)]
pub struct JournalOptions {
    /// Scripted failures (tests only).
    #[cfg(any(test, feature = "fault-injection"))]
    pub faults: Option<Arc<faults::Faults>>,
}

pub const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 2 + 4 + 8 + 4;
const MAX_PAYLOAD: u64 = 16 * 1024 * 1024; // frames bigger than this are corruption

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
    /// A backup never merges into an existing directory.
    #[error("backup destination already exists")]
    BackupExists,
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

/// The on-disk directory for a generation under a journal root.
pub fn gen_dir_path(root: &Path, generation: u32) -> PathBuf {
    root.join(format!("gen-{generation:06}"))
}

fn crc(payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(payload);
    h.finalize()
}

/// Replay every committed record in a generation's journal.log, in order.
/// A torn tail (the file ends inside a frame) is discarded; anything else
/// wrong — bad version, bad checksum, a sequence gap, a truncated non-tail
/// frame — is corruption and stops the caller.
pub fn replay_dir(dir: &Path) -> Result<Vec<Record>, JournalError> {
    let log = dir.join("journal.log");
    if !log.exists() {
        return Ok(Vec::new());
    }
    let mut f = File::open(&log)?;
    replay(&mut f)
}

fn replay(f: &mut File) -> Result<Vec<Record>, JournalError> {
    let file_len = f.metadata()?.len();
    let mut records = Vec::new();
    let mut pos = 0u64;
    let mut expected_seq = 1u64;
    loop {
        if pos == file_len {
            return Ok(records);
        }
        // Torn tail: fewer than HEADER_LEN bytes remain.
        if file_len - pos < HEADER_LEN as u64 {
            return Ok(records);
        }
        let mut hdr = [0u8; HEADER_LEN];
        f.read_exact(&mut hdr)?;
        let version =
            u16::from_be_bytes(hdr[0..2].try_into().map_err(|_| io::Error::other("hdr"))?);
        let len = u32::from_be_bytes(hdr[2..6].try_into().map_err(|_| io::Error::other("hdr"))?);
        let seq = u64::from_be_bytes(hdr[6..14].try_into().map_err(|_| io::Error::other("hdr"))?);
        let want_crc = u32::from_be_bytes(
            hdr[14..18]
                .try_into()
                .map_err(|_| io::Error::other("hdr"))?,
        );
        if version != FORMAT_VERSION {
            return Err(JournalError::Corrupt {
                seq,
                why: format!("version {version}"),
            });
        }
        if len as u64 > MAX_PAYLOAD {
            return Err(JournalError::Corrupt {
                seq,
                why: format!("len {len} over cap"),
            });
        }
        let frame_end = pos + HEADER_LEN as u64 + len as u64;
        if frame_end > file_len {
            // Torn tail: header complete, payload short — safe to discard.
            return Ok(records);
        }
        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload)?;
        if crc(&payload) != want_crc {
            return Err(JournalError::Corrupt {
                seq,
                why: "checksum".into(),
            });
        }
        if seq != expected_seq {
            return Err(JournalError::Corrupt {
                seq,
                why: format!("expected seq {expected_seq}"),
            });
        }
        records.push(Record { seq, payload });
        expected_seq += 1;
        pos = frame_end;
        f.seek(SeekFrom::Start(pos))?;
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
        fs::create_dir_all(dir)?;
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
                let m = Manifest { generation: 1 };
                let tmp = dir.join("manifest.json.tmp");
                fs::write(&tmp, serde_json::to_string(&m).map_err(io::Error::other)?)?;
                File::open(&tmp)?.sync_data()?;
                fs::rename(&tmp, &manifest_path)?;
                sync_parent(&manifest_path)?;
                1
            }
            Err(e) => return Err(e.into()),
        };

        let gdir = gen_dir_path(dir, generation);
        fs::create_dir_all(&gdir)?;
        let log_path = gdir.join("journal.log");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;

        // Replay: verify committed history, then truncate a torn tail only.
        let records = replay(&mut file)?;
        let good_len = records
            .iter()
            .fold(0u64, |p, r| p + HEADER_LEN as u64 + r.payload.len() as u64);
        if file.metadata()?.len() != good_len {
            file.set_len(good_len)?; // discard the demonstrably-torn tail
            file.sync_data()?;
        }
        file.seek(SeekFrom::End(0))?;
        let next_seq = records.last().map_or(1, |r| r.seq + 1);
        Ok(Self {
            root: dir.into(),
            generation,
            file,
            next_seq,
            durable_len: good_len,
            poisoned: false,
            opts,
            _lock: lock_file,
        })
    }

    /// Bytes that must stay free on the journal filesystem — when free
    /// space falls below this, admission refuses rather than append into
    /// a full disk (contract: full disk fails safely, not silently).
    pub const FREE_SPACE_RESERVE: u64 = 64 * 1024 * 1024; // 64 MiB

    /// Append one record: frame, write, fsync — only then is it committed.
    /// Returns the assigned sequence number. A failure leaves no record
    /// behind and consumes no sequence number (see `WriteFailed`,
    /// `SyncFailed`); a poisoned writer refuses with `Poisoned`.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let available = fs2::available_space(&self.root)?;
        if available < Self::FREE_SPACE_RESERVE {
            return Err(JournalError::NoSpace {
                available,
                reserve: Self::FREE_SPACE_RESERVE,
            });
        }
        if payload.len() as u64 > MAX_PAYLOAD {
            return Err(JournalError::Corrupt {
                seq: self.next_seq,
                why: "payload over cap".into(),
            });
        }
        let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
        frame.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&self.next_seq.to_be_bytes());
        frame.extend_from_slice(&crc(payload).to_be_bytes());
        frame.extend_from_slice(payload);
        self.write_durably(&frame)?;
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    /// Whether an earlier failure poisoned this writer.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
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
        if self.opts.faults.as_ref().is_some_and(|f| f.take_sync()) {
            return Err(io::Error::other("injected fsync failure"));
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
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let next = self.generation + 1;
        let gdir = gen_dir_path(&self.root, next);
        fs::create_dir_all(&gdir)?;

        // 1. Fresh log, created and fsynced before the manifest moves.
        let log_path = gdir.join("journal.log");
        let new_log = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // a reused generation is corruption, refuse it
            .open(&log_path)?;
        new_log.sync_data()?;

        // 2. Snapshot for the new generation (tmp + rename + dir fsync).
        write_snapshot(&gdir, snapshot)?;

        // 3. The atomic manifest swap — the single point that moves the
        //    world from generation N to N+1.
        let tmp = self.root.join("manifest.json.tmp");
        fs::write(
            &tmp,
            serde_json::to_vec(&Manifest { generation: next }).map_err(io::Error::other)?,
        )?;
        File::open(&tmp)?.sync_data()?;
        fs::rename(&tmp, self.root.join("manifest.json"))?;
        sync_parent(&tmp)?;

        // 4. Only now does the writer move: new generation, seq restarts.
        self.file = OpenOptions::new().read(true).append(true).open(&log_path)?;
        self.file.seek(SeekFrom::End(0))?;
        self.generation = next;
        self.next_seq = 1;
        self.durable_len = 0;
        Ok(())
    }

    /// A consistent copy for backup into `dest`, which must not exist yet
    /// (its parent must): generation data first, manifest LAST —
    /// a copy interrupted mid-flight still opens as a complete older
    /// generation, never a torn mixed one.
    pub fn backup_to(&self, dest: &Path) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned); // the log may hold an unrolled tail
        }
        // A fresh directory only: copying into an existing one could mix
        // generations from two backups. `create_dir` is the atomic check.
        match fs::create_dir(dest) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(JournalError::BackupExists)
            }
            Err(e) => return Err(e.into()),
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(n) = name.to_str() else { continue };
            if n.starts_with("gen-") && entry.file_type()?.is_dir() {
                let gd = dest.join(n);
                fs::create_dir_all(&gd)?;
                for f in ["journal.log", "snapshot.json"] {
                    let src = entry.path().join(f);
                    if src.exists() {
                        fs::copy(&src, gd.join(f))?;
                        File::open(gd.join(f))?.sync_data()?;
                    }
                }
            }
        }
        fs::copy(self.root.join("manifest.json"), dest.join("manifest.json"))?;
        File::open(dest.join("manifest.json"))?.sync_data()?;
        sync_parent(&dest.join("manifest.json"))?;
        Ok(())
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

/// Atomically write the generation's snapshot (tmp + rename + dir fsync).
pub fn write_snapshot(dir: &Path, snap: &Snapshot) -> Result<(), JournalError> {
    let tmp = dir.join("snapshot.json.tmp");
    fs::write(&tmp, serde_json::to_vec(snap).map_err(io::Error::other)?)?;
    File::open(&tmp)?.sync_data()?;
    fs::rename(&tmp, dir.join("snapshot.json"))?;
    sync_parent(&tmp)?;
    Ok(())
}

/// Load the generation's snapshot if one exists.
pub fn load_snapshot(dir: &Path) -> Result<Option<Snapshot>, JournalError> {
    let p = dir.join("snapshot.json");
    match fs::read(&p) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| JournalError::Manifest(format!("snapshot: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Replay only records after `from_seq` (used with a snapshot).
pub fn replay_dir_from(dir: &Path, from_seq: u64) -> Result<Vec<Record>, JournalError> {
    Ok(replay_dir(dir)?
        .into_iter()
        .filter(|r| r.seq > from_seq)
        .collect())
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

    #[test]
    fn sequence_gap_is_corruption() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let log = gen_dir_path(dir.path(), 1).join("journal.log");
        fs::create_dir_all(log.parent().unwrap_or_else(|| panic!("parent")))
            .unwrap_or_else(|e| panic!("{e}"));
        fs::write(dir.path().join("manifest.json"), "{\"generation\":1}")
            .unwrap_or_else(|e| panic!("{e}"));
        // Hand-frame two records with seq 1 then 3.
        let mut file = File::create(&log).unwrap_or_else(|e| panic!("{e}"));
        for (seq, payload) in [(1u64, &b"a"[..]), (3u64, &b"b"[..])] {
            let mut frame = Vec::new();
            frame.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&seq.to_be_bytes());
            frame.extend_from_slice(&crc(payload).to_be_bytes());
            frame.extend_from_slice(payload);
            file.write_all(&frame).unwrap_or_else(|e| panic!("{e}"));
        }
        file.sync_data().unwrap_or_else(|e| panic!("{e}"));
        drop(file);
        match Journal::open(dir.path()) {
            Err(JournalError::Corrupt { seq: 3, .. }) => {}
            other => panic!("expected Corrupt at seq 3, got {other:?}"),
        }
    }
}
