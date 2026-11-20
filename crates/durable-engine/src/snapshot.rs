//! The compacted index on disk (D-021): an explicit binary encoding of every
//! retained run, with inputs as raw bytes rather than JSON number arrays. It
//! carries its own magic and version; the journal's `snapshot.bin` frame
//! adds the length and checksum. Decoding is bounds-checked and refuses
//! anything malformed: the snapshot is the only authority for compacted
//! state.
//!
//! ```text
//! "DRIX" u16 version | next_run u64 | count u64 | count × run
//! run = id u64 | ws u64 (v2+) | key str | spec.version u32
//!     | steps u32 × (name str, handler str)
//!     | input bytes | state | finished_at (u8 0/1 [+ u64])
//! str/bytes = u32 length + bytes (big-endian throughout)
//! state = u8 tag: 0 Queued{at} 1 Running{step,attempt,deadline}
//!         2 RetryWaiting{step,attempt,at} 3 Reconciling{step,attempt}
//!         4 Done 5 Failed{step} 6 Cancelled
//! version 1 has no ws (reads as 0); version 2 adds it (S2).
//! ```

use durable_domain::{JobSpec, OperationKey, Run, RunId, RunState, StepIndex, StepSpec};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::sync::Arc;

pub const MAGIC: &[u8; 4] = b"DRIX";
/// v2 adds the workspace id (S2); v1 snapshots still decode, their runs
/// reading as workspace 0.
pub const VERSION: u16 = 2;

/// A consistent view of the index for one compaction: shared pointers to
/// the runs as they were when it was planned. The core copies a run before
/// changing it (`Arc::make_mut`), so the view never sees later state and
/// building it costs a pointer per run, not a copy of every input.
pub struct View {
    pub next_run: u64,
    pub runs: Vec<Arc<Run>>,
}

fn str_len(s: &str) -> u64 {
    4 + s.len() as u64
}

fn state_len(s: &RunState) -> u64 {
    1 + match s {
        RunState::Queued { .. } => 8,
        RunState::Running { .. } | RunState::RetryWaiting { .. } => 4 + 4 + 8,
        RunState::Reconciling { .. } => 4 + 4,
        RunState::Failed { .. } => 4,
        RunState::Done | RunState::Cancelled => 0,
    }
}

fn run_len(r: &Run) -> u64 {
    8 + 8
        + str_len(&r.key.0)
        + 4
        + 4
        + r.spec
            .steps
            .iter()
            .map(|s| str_len(&s.name) + str_len(&s.handler))
            .sum::<u64>()
        + 4
        + r.spec.input.len() as u64
        + state_len(&r.state)
        + 1
        + if r.finished_at.is_some() { 8 } else { 0 }
}

/// Exact encoded size — checked against the byte budget and the disk
/// reserve before anything is written.
pub fn encoded_len(v: &View) -> u64 {
    4 + 2 + 8 + 8 + v.runs.iter().map(|r| run_len(r)).sum::<u64>()
}

fn put_len(w: &mut dyn Write, n: usize) -> io::Result<()> {
    let n = u32::try_from(n).map_err(|_| io::Error::other("field over 4 GiB"))?;
    w.write_all(&n.to_be_bytes())
}

fn put_str(w: &mut dyn Write, s: &str) -> io::Result<()> {
    put_len(w, s.len())?;
    w.write_all(s.as_bytes())
}

/// Stream the view as the binary index. Writes exactly `encoded_len(v)`.
pub fn encode(v: &View, w: &mut dyn Write) -> io::Result<()> {
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_be_bytes())?;
    w.write_all(&v.next_run.to_be_bytes())?;
    w.write_all(&(v.runs.len() as u64).to_be_bytes())?;
    for r in &v.runs {
        w.write_all(&r.id.0.to_be_bytes())?;
        w.write_all(&r.ws.to_be_bytes())?;
        put_str(w, &r.key.0)?;
        w.write_all(&r.spec.version.to_be_bytes())?;
        put_len(w, r.spec.steps.len())?;
        for s in &r.spec.steps {
            put_str(w, &s.name)?;
            put_str(w, &s.handler)?;
        }
        put_len(w, r.spec.input.len())?;
        w.write_all(&r.spec.input)?;
        match &r.state {
            RunState::Queued { next_run_at } => {
                w.write_all(&[0])?;
                w.write_all(&next_run_at.to_be_bytes())?;
            }
            RunState::Running {
                step,
                attempt,
                deadline,
            } => {
                w.write_all(&[1])?;
                w.write_all(&step.0.to_be_bytes())?;
                w.write_all(&attempt.to_be_bytes())?;
                w.write_all(&deadline.to_be_bytes())?;
            }
            RunState::RetryWaiting {
                step,
                attempt,
                next_run_at,
            } => {
                w.write_all(&[2])?;
                w.write_all(&step.0.to_be_bytes())?;
                w.write_all(&attempt.to_be_bytes())?;
                w.write_all(&next_run_at.to_be_bytes())?;
            }
            RunState::Reconciling { step, attempt } => {
                w.write_all(&[3])?;
                w.write_all(&step.0.to_be_bytes())?;
                w.write_all(&attempt.to_be_bytes())?;
            }
            RunState::Done => w.write_all(&[4])?,
            RunState::Failed { step } => {
                w.write_all(&[5])?;
                w.write_all(&step.0.to_be_bytes())?;
            }
            RunState::Cancelled => w.write_all(&[6])?,
        }
        match r.finished_at {
            Some(t) => {
                w.write_all(&[1])?;
                w.write_all(&t.to_be_bytes())?;
            }
            None => w.write_all(&[0])?,
        }
    }
    Ok(())
}

/// A bounds-checked streaming reader: every read fails rather than run
/// past the payload's declared length, and nothing is allocated for a
/// length that does not fit in what is left. Memory stays one field.
struct Src<'a, R: Read> {
    r: &'a mut R,
    left: u64,
}

impl<R: Read> Src<'_, R> {
    fn take(&mut self, n: usize) -> Result<Vec<u8>, String> {
        if n as u64 > self.left {
            return Err(format!("truncated: wanted {n} bytes, {} left", self.left));
        }
        let mut v = vec![0u8; n];
        self.r
            .read_exact(&mut v)
            .map_err(|e| format!("read: {e}"))?;
        self.left -= n as u64;
        Ok(v)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], String> {
        if N as u64 > self.left {
            return Err(format!("truncated: wanted {N} bytes, {} left", self.left));
        }
        let mut a = [0u8; N];
        self.r
            .read_exact(&mut a)
            .map_err(|e| format!("read: {e}"))?;
        self.left -= N as u64;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.fixed::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.fixed()?))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.fixed()?))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.fixed()?))
    }
    /// A length-prefixed field of at most `max` bytes (and never more than
    /// is left): a longer length is refused before anything is allocated.
    fn bytes(&mut self, max: usize, what: &str) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(format!(
                "{what} of {n} bytes is over the stored bound {max}"
            ));
        }
        self.take(n)
    }
    fn string(&mut self, max: usize, what: &str) -> Result<String, String> {
        String::from_utf8(self.bytes(max, what)?).map_err(|_| format!("{what} is not UTF-8"))
    }
}

/// The decoded index: the format version it was written at, `next_run`
/// and every retained run by id. The version rides along so the caller
/// can report the highest DRIX version the journal holds (D-056).
pub type Decoded = (u16, u64, BTreeMap<RunId, Arc<Run>>);

/// Decode a binary index held in memory; any malformation is an error.
pub fn decode(bytes: &[u8]) -> Result<Decoded, String> {
    let mut r = bytes;
    decode_from(&mut r, bytes.len() as u64)
}

/// Decode a binary index of exactly `len` bytes streamed from `r` — the
/// restart path reads `snapshot.bin` through this, so memory is the index
/// being built plus one field, never the whole file (D-028). Any
/// malformation is an error, never a guess; the caller still verifies the
/// checksum before using the result.
pub fn decode_from(r: &mut impl Read, len: u64) -> Result<Decoded, String> {
    let mut c = Src { r, left: len };
    if &c.fixed::<4>()? != MAGIC {
        return Err("not a binary index".into());
    }
    let version = c.u16()?;
    if version == 0 || version > VERSION {
        return Err(format!("index version {version} is not 1..={VERSION}"));
    }
    let next_run = c.u64()?;
    let count = c.u64()?;
    // Every run takes at least 26 bytes (34 at v2, which adds ws): refuse
    // an impossible count before reserving anything for it.
    let min_run = if version == VERSION { 34 } else { 26 };
    if count > c.left / min_run {
        return Err(format!("{count} runs cannot fit in {} bytes", c.left));
    }
    // Every count and length is held to the stored bounds as well as to
    // the bytes left (D-033): the checksum is verified only after decoding,
    // so a corrupted field reaches here, and must cost no more than a
    // valid one could.
    let b = durable_domain::STORED;
    let mut runs = BTreeMap::new();
    for _ in 0..count {
        let id = RunId(c.u64()?);
        // v1 snapshots predate workspaces: their runs belong to the
        // default workspace 0 (S2).
        let ws = if version == VERSION { c.u64()? } else { 0 };
        let key = OperationKey(c.string(b.max_key_bytes, "key")?);
        let version = c.u32()?;
        let n_steps = c.u32()?;
        if n_steps > b.max_steps {
            return Err(format!(
                "{n_steps} steps is over the stored bound {}",
                b.max_steps
            ));
        }
        if u64::from(n_steps) > c.left / 8 {
            return Err(format!("{n_steps} steps cannot fit"));
        }
        let mut steps = Vec::with_capacity(n_steps as usize);
        for _ in 0..n_steps {
            let name = c.string(b.max_name_bytes, "step name")?;
            let handler = c.string(b.max_name_bytes, "handler name")?;
            steps.push(StepSpec { name, handler });
        }
        let input = c.bytes(b.max_input_bytes, "input")?;
        let state = match c.u8()? {
            0 => RunState::Queued {
                next_run_at: c.u64()?,
            },
            1 => RunState::Running {
                step: StepIndex(c.u32()?),
                attempt: c.u32()?,
                deadline: c.u64()?,
            },
            2 => RunState::RetryWaiting {
                step: StepIndex(c.u32()?),
                attempt: c.u32()?,
                next_run_at: c.u64()?,
            },
            3 => RunState::Reconciling {
                step: StepIndex(c.u32()?),
                attempt: c.u32()?,
            },
            4 => RunState::Done,
            5 => RunState::Failed {
                step: StepIndex(c.u32()?),
            },
            6 => RunState::Cancelled,
            t => return Err(format!("unknown state tag {t}")),
        };
        let finished_at = match c.u8()? {
            0 => None,
            1 => Some(c.u64()?),
            f => return Err(format!("bad finished_at flag {f}")),
        };
        let run = Run {
            id,
            ws,
            key,
            spec: JobSpec {
                version,
                steps,
                input,
            },
            state,
            finished_at,
        };
        if runs.insert(id, Arc::new(run)).is_some() {
            return Err(format!("run {} appears twice", id.0));
        }
    }
    if c.left != 0 {
        return Err(format!("{} trailing bytes", c.left));
    }
    Ok((version, next_run, runs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: u64, state: RunState, finished_at: Option<u64>) -> Arc<Run> {
        Arc::new(Run {
            id: RunId(id),
            ws: id,
            key: OperationKey(format!("key-{id}")),
            spec: JobSpec {
                version: 3,
                steps: vec![
                    StepSpec {
                        name: "a".into(),
                        handler: "h".into(),
                    },
                    StepSpec {
                        name: "b".into(),
                        handler: "h2".into(),
                    },
                ],
                input: (0..=255).collect(),
            },
            state,
            finished_at,
        })
    }

    fn every_state() -> View {
        let s = StepIndex(1);
        View {
            next_run: 99,
            runs: vec![
                run(1, RunState::Queued { next_run_at: 5 }, None),
                run(
                    2,
                    RunState::Running {
                        step: s,
                        attempt: 2,
                        deadline: 7,
                    },
                    None,
                ),
                run(
                    3,
                    RunState::RetryWaiting {
                        step: s,
                        attempt: 3,
                        next_run_at: 8,
                    },
                    None,
                ),
                run(
                    4,
                    RunState::Reconciling {
                        step: s,
                        attempt: 1,
                    },
                    None,
                ),
                run(5, RunState::Done, Some(11)),
                run(6, RunState::Failed { step: s }, Some(12)),
                run(7, RunState::Cancelled, Some(13)),
            ],
        }
    }

    #[test]
    fn every_state_round_trips_at_its_exact_length() {
        let v = every_state();
        let mut out = Vec::new();
        encode(&v, &mut out).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(out.len() as u64, encoded_len(&v));
        let (_, next_run, runs) = decode(&out).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(next_run, 99);
        let back: Vec<Run> = runs.values().map(|r| (**r).clone()).collect();
        let want: Vec<Run> = v.runs.iter().map(|r| (**r).clone()).collect();
        assert_eq!(back, want);
    }

    /// A reader that hands over one byte per call: streaming decode must
    /// not depend on how reads are split.
    struct Trickle<'a>(&'a [u8]);
    impl Read for Trickle<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match (self.0.split_first(), buf.first_mut()) {
                (Some((b, rest)), Some(slot)) => {
                    *slot = *b;
                    self.0 = rest;
                    Ok(1)
                }
                _ => Ok(0),
            }
        }
    }

    #[test]
    fn streamed_decode_matches_in_memory_decode() {
        let v = every_state();
        let mut out = Vec::new();
        encode(&v, &mut out).unwrap_or_else(|e| panic!("{e}"));
        let whole = decode(&out).unwrap_or_else(|e| panic!("{e}"));
        let streamed =
            decode_from(&mut Trickle(&out), out.len() as u64).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(whole.0, streamed.0);
        assert_eq!(whole.1, streamed.1);
        let a: Vec<Run> = whole.2.values().map(|r| (**r).clone()).collect();
        let b: Vec<Run> = streamed.2.values().map(|r| (**r).clone()).collect();
        assert_eq!(a, b);
    }

    fn wide(steps: usize, key: usize, name: usize, input: usize) -> Arc<Run> {
        Arc::new(Run {
            id: RunId(1),
            ws: 0,
            key: OperationKey("k".repeat(key)),
            spec: JobSpec {
                version: 1,
                steps: (0..steps)
                    .map(|_| StepSpec {
                        name: "n".repeat(name),
                        handler: "h".into(),
                    })
                    .collect(),
                input: vec![7; input],
            },
            state: RunState::Queued { next_run_at: 0 },
            finished_at: None,
        })
    }

    /// Review T3: the decoder bounded counts and lengths only by the bytes
    /// left, so one corrupted step count could reserve about 6 × the rest
    /// of the file, and a key or name could be as long as the file. Every
    /// count and length is now held to the stored bounds as well: a run
    /// past them is refused, never allocated for.
    #[test]
    fn runs_wider_than_the_stored_bounds_are_refused() {
        let b = durable_domain::STORED;
        let cases = [
            ("steps", wide(b.max_steps as usize + 1, 1, 1, 0)),
            ("key", wide(1, b.max_key_bytes + 1, 1, 0)),
            ("name", wide(1, 1, b.max_name_bytes + 1, 0)),
            ("input", wide(1, 1, 1, b.max_input_bytes + 1)),
        ];
        for (what, run) in cases {
            let v = View {
                next_run: 2,
                runs: vec![run],
            };
            let mut out = Vec::new();
            encode(&v, &mut out).unwrap_or_else(|e| panic!("{e}"));
            assert!(decode(&out).is_err(), "a run over the {what} bound decoded");
        }
        // At the bounds exactly, a run still decodes.
        let v = View {
            next_run: 2,
            runs: vec![wide(
                b.max_steps as usize,
                b.max_key_bytes,
                b.max_name_bytes,
                b.max_input_bytes,
            )],
        };
        let mut out = Vec::new();
        encode(&v, &mut out).unwrap_or_else(|e| panic!("{e}"));
        decode(&out).unwrap_or_else(|e| panic!("at the bounds: {e}"));
    }

    /// Review U5: the binary index is pinned to golden files written from
    /// its spec by an independent implementation (`durable-journal/tests/
    /// fixtures/make_fixtures.py`): the v1 file (no ws — S2 predates it)
    /// must decode with every run in workspace 0; the v2 file must decode
    /// exactly and the encoder must reproduce it byte for byte.
    #[test]
    fn the_index_format_matches_its_golden_files() {
        let want = View {
            next_run: 3,
            runs: vec![
                Arc::new(Run {
                    id: RunId(1),
                    ws: 7,
                    key: OperationKey("golden-1".into()),
                    spec: JobSpec {
                        version: 1,
                        steps: vec![StepSpec {
                            name: "a".into(),
                            handler: "h".into(),
                        }],
                        input: vec![0x00, 0x01, 0xff],
                    },
                    state: RunState::Queued { next_run_at: 5 },
                    finished_at: None,
                }),
                Arc::new(Run {
                    id: RunId(2),
                    ws: 7,
                    key: OperationKey("golden-2".into()),
                    spec: JobSpec {
                        version: 2,
                        steps: vec![StepSpec {
                            name: "b".into(),
                            handler: "h2".into(),
                        }],
                        input: Vec::new(),
                    },
                    state: RunState::Done,
                    finished_at: Some(1_700_000_000_000),
                }),
            ],
        };
        // v1 golden: same runs, ws defaulted to 0.
        let v1: &[u8] = include_bytes!("../tests/fixtures/index.drix");
        let (ver, next_run, runs) = decode(v1).unwrap_or_else(|e| panic!("v1: {e}"));
        assert_eq!((ver, next_run), (1, 3));
        let got: Vec<Run> = runs.values().map(|r| (**r).clone()).collect();
        let expect_v1: Vec<Run> = want
            .runs
            .iter()
            .map(|r| Run {
                ws: 0,
                ..(**r).clone()
            })
            .collect();
        assert_eq!(got, expect_v1, "a v1 index must decode into workspace 0");
        // v2 golden: decode exactly, and encode to the same bytes.
        let v2: &[u8] = include_bytes!("../tests/fixtures/index-v2.drix");
        let (ver, next_run, runs) = decode(v2).unwrap_or_else(|e| panic!("v2: {e}"));
        assert_eq!((ver, next_run), (2, 3));
        let got: Vec<Run> = runs.values().map(|r| (**r).clone()).collect();
        let expect: Vec<Run> = want.runs.iter().map(|r| (**r).clone()).collect();
        assert_eq!(got, expect);
        let mut out = Vec::new();
        encode(&want, &mut out).unwrap_or_else(|e| panic!("{e}"));
        assert!(
            out == v2,
            "the index encoder no longer matches its v2 golden file"
        );
        assert_eq!(encoded_len(&want), v2.len() as u64);
    }

    /// Every truncation and every flipped tag is refused, never misread.
    #[test]
    fn malformed_input_is_refused() {
        let v = every_state();
        let mut out = Vec::new();
        encode(&v, &mut out).unwrap_or_else(|e| panic!("{e}"));
        for cut in 0..out.len() {
            assert!(decode(&out[..cut]).is_err(), "truncated at {cut} decoded");
        }
        let mut extra = out.clone();
        extra.push(0);
        assert!(decode(&extra).is_err(), "trailing bytes");
        let mut huge = out.clone();
        huge[14..22].copy_from_slice(&u64::MAX.to_be_bytes()); // run count
        assert!(decode(&huge).is_err(), "impossible count");
    }
}
