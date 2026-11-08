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
use thiserror::Error;

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
    // Held for the process lifetime; the flock releases when this drops.
    _lock: File,
}

impl Journal {
    /// Open (creating if needed) the journal rooted at `dir`. Fails
    /// `SecondWriter` if another live writer holds the lock, `Corrupt` on
    /// damaged committed history.
    pub fn open(dir: &Path) -> Result<Self, JournalError> {
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
            _lock: lock_file,
        })
    }

    /// Append one record: frame, write, fsync — only then is it committed.
    /// Returns the assigned sequence number.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, JournalError> {
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
        self.file.write_all(&frame)?;
        self.file.sync_data()?; // the durable point: acks follow this line only
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn dir(&self) -> &Path {
        &self.root
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
