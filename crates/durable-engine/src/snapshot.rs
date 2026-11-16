//! The compacted index on disk (D-021): an explicit binary encoding of every
//! retained run, with inputs as raw bytes rather than JSON number arrays. It
//! carries its own magic and version; the journal's `snapshot.bin` frame
//! adds the length and checksum. Decoding is bounds-checked and refuses
//! anything malformed: the snapshot is the only authority for compacted
//! state.
//!
//! ```text
//! "DRIX" u16 version | next_run u64 | count u64 | count × run
//! run = id u64 | key str | spec.version u32 | steps u32 × (name str, handler str)
//!     | input bytes | state | finished_at (u8 0/1 [+ u64])
//! str/bytes = u32 length + bytes (big-endian throughout)
//! state = u8 tag: 0 Queued{at} 1 Running{step,attempt,deadline}
//!         2 RetryWaiting{step,attempt,at} 3 Reconciling{step,attempt}
//!         4 Done 5 Failed{step} 6 Cancelled
//! ```

use durable_domain::{JobSpec, OperationKey, Run, RunId, RunState, StepIndex, StepSpec};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::Arc;

pub const MAGIC: &[u8; 4] = b"DRIX";
pub const VERSION: u16 = 1;

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
    8 + str_len(&r.key.0)
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

/// A bounds-checked reader: every read fails rather than run past the end.
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if n > self.buf.len() {
            return Err(format!(
                "truncated: wanted {n} bytes, {} left",
                self.buf.len()
            ));
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, String> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64, String> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_be_bytes(a))
    }
    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> Result<String, String> {
        let b = self.bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| "string is not UTF-8".to_string())
    }
}

/// The decoded index: `next_run` and every retained run by id.
pub type Decoded = (u64, BTreeMap<RunId, Arc<Run>>);

/// Decode a binary index; any malformation is an error, never a guess.
pub fn decode(bytes: &[u8]) -> Result<Decoded, String> {
    let mut c = Cursor { buf: bytes };
    if c.take(4)? != MAGIC {
        return Err("not a binary index".into());
    }
    let version = c.u16()?;
    if version != VERSION {
        return Err(format!("index version {version} is not {VERSION}"));
    }
    let next_run = c.u64()?;
    let count = c.u64()?;
    // Every run takes at least 26 bytes: refuse an impossible count before
    // reserving anything for it.
    if count > (c.buf.len() / 26) as u64 {
        return Err(format!("{count} runs cannot fit in {} bytes", c.buf.len()));
    }
    let mut runs = BTreeMap::new();
    for _ in 0..count {
        let id = RunId(c.u64()?);
        let key = OperationKey(c.string()?);
        let version = c.u32()?;
        let n_steps = c.u32()? as usize;
        if n_steps > c.buf.len() / 8 {
            return Err(format!("{n_steps} steps cannot fit"));
        }
        let mut steps = Vec::with_capacity(n_steps);
        for _ in 0..n_steps {
            let name = c.string()?;
            let handler = c.string()?;
            steps.push(StepSpec { name, handler });
        }
        let input = c.bytes()?.to_vec();
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
    if !c.buf.is_empty() {
        return Err(format!("{} trailing bytes", c.buf.len()));
    }
    Ok((next_run, runs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: u64, state: RunState, finished_at: Option<u64>) -> Arc<Run> {
        Arc::new(Run {
            id: RunId(id),
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
        let (next_run, runs) = decode(&out).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(next_run, 99);
        let back: Vec<Run> = runs.values().map(|r| (**r).clone()).collect();
        let want: Vec<Run> = v.runs.iter().map(|r| (**r).clone()).collect();
        assert_eq!(back, want);
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
