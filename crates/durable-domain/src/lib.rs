//! The pure state machine: job/run/step identities, the transition table with
//! named invalid-state refusals, retry policy, limits — no I/O anywhere.
//! The coordinator replays journal records through `reduce` and trusts only
//! what the journal committed; this crate decides what *may* be committed.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------- identities

/// The caller's deduplication key: one business identity across every attempt.
/// Retrying a submission key must return the original run (contract F1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperationKey(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RunId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepIndex(pub u32);

/// A named step executed by an allowlisted HTTP handler (D-003).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSpec {
    pub name: String,
    pub handler: String,
}

/// Versioned job definition submitted with an operation key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    pub version: u32,
    pub steps: Vec<StepSpec>,
    pub input: Vec<u8>,
}

// --------------------------------------------------------------------- state

/// Milliseconds since the epoch, supplied by the caller's clock (fake in tests).
pub type Millis = u64;

/// Terminal states are the only ones whose history may expire (D-005).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    /// Accepted, waiting for its (possibly delayed) first run.
    Queued {
        next_run_at: Millis,
    },
    /// A step attempt is in flight; `deadline` bounds it.
    Running {
        step: StepIndex,
        attempt: u32,
        deadline: Millis,
    },
    /// Failed attempt, bounded backoff scheduled.
    RetryWaiting {
        step: StepIndex,
        attempt: u32,
        next_run_at: Millis,
    },
    /// Waiting to reconcile a vanished handler reply before any repeat (F2).
    Reconciling {
        step: StepIndex,
        attempt: u32,
    },
    Done,
    /// Retries exhausted or an unretryable failure.
    Failed {
        step: StepIndex,
    },
    /// Cancelled: stops scheduling — never undoes a remote action (D-004).
    Cancelled,
}

impl RunState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunState::Done | RunState::Failed { .. } | RunState::Cancelled
        )
    }

    /// The state's family name — diagnostics group by this, not by the
    /// step/attempt fields (counts stay bounded).
    pub fn kind(&self) -> &'static str {
        match self {
            RunState::Queued { .. } => "queued",
            RunState::Running { .. } => "running",
            RunState::RetryWaiting { .. } => "retry_waiting",
            RunState::Reconciling { .. } => "reconciling",
            RunState::Done => "done",
            RunState::Failed { .. } => "failed",
            RunState::Cancelled => "cancelled",
        }
    }
}

/// One run of a job spec under one operation key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub key: OperationKey,
    pub spec: JobSpec,
    pub state: RunState,
    /// When the run reached a terminal state — the 7-day expiry clock.
    pub finished_at: Option<Millis>,
}

impl Run {
    /// The run as its `Accepted` record creates it: queued for `not_before`.
    /// The reducer never builds a Run — acceptance is construction, not a
    /// transition; a second `Accepted` for an existing run is `DuplicateKey`.
    pub fn accepted(id: RunId, key: OperationKey, spec: JobSpec, not_before: Millis) -> Self {
        Self {
            id,
            key,
            spec,
            state: RunState::Queued {
                next_run_at: not_before,
            },
            finished_at: None,
        }
    }
}

// ------------------------------------------------------------------- events

/// Every fact the journal may record. The reducer maps (run, event) ->
/// (new state, effects); nothing reaches the journal except through it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// Durable acceptance: acknowledged only after the journal fsyncs.
    /// `not_before` supports delayed starts; `key` is the dedup identity.
    Accepted {
        key: OperationKey,
        spec: JobSpec,
        not_before: Millis,
    },
    /// A step attempt started; the handler has `deadline` to answer.
    StepDispatched {
        step: StepIndex,
        attempt: u32,
        deadline: Millis,
    },
    /// The handler answered in time; checkpoint its output.
    StepCheckpointed { step: StepIndex, output: Vec<u8> },
    /// The attempt ended without a usable answer (fail, timeout, 5xx).
    StepFailed { step: StepIndex },
    /// The handler succeeded but the reply vanished — reconcile before repeat.
    ReplyLost { step: StepIndex },
    /// Uncertain effect checked: `effect_found` = the remote call did land
    /// (advance, never repeat it); `false` = it did not — retry is safe.
    Reconciled { step: StepIndex, effect_found: bool },
    /// Cancellation requested; honoured only from non-terminal states.
    CancelRequested,
}

/// Side effects the reducer asks the executor/scheduler to perform. They are
/// data, not actions — ordering and bounds are decided here, execution there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    DispatchStep {
        step: StepIndex,
        attempt: u32,
        deadline: Millis,
    },
    ScheduleWake {
        at: Millis,
    },
    CommitCheckpoint {
        step: StepIndex,
        output: Vec<u8>,
    },
    Reconcile {
        step: StepIndex,
    },
    Finish,
    EmitTerminal,
}

// ------------------------------------------------------------------ refusals

/// Every refused transition, named. A refusal is information, not an error.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
pub enum Refusal {
    #[error("duplicate operation key")]
    DuplicateKey,
    #[error("job spec over limits")]
    OverLimits,
    #[error("run is terminal; nothing may be scheduled")]
    Terminal,
    #[error("no such step in the spec")]
    NoSuchStep,
    #[error("dispatch does not match the scheduled step or attempt")]
    StaleDispatch,
    #[error("a reply may only arrive for the in-flight attempt")]
    StaleReply,
    #[error("cannot cancel a terminal run")]
    CancelTerminal,
    #[error("reconciliation is only valid for a lost reply")]
    NothingToReconcile,
    #[error("admission reserve exhausted")]
    ReserveExhausted,
    #[error("at capacity: live-run or index-byte bound reached")]
    AtCapacity,
}

// -------------------------------------------------------------------- limits

/// The widest a *stored* run may be (D-033). Stored runs — journal records
/// and snapshots — are held to these when they are decoded, so a wider
/// field is corruption, never an allocation. Steps and inputs have been
/// admitted only up to these values since D0. Keys and names have been
/// bounded only since D-013 (256 and 128 bytes), so their stored bound is
/// wider: a run accepted before then must still load. Admission `Limits`
/// may be tightened at will but never raised past these
/// (`Limits::validate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredBounds {
    pub max_steps: u32,
    pub max_input_bytes: usize,
    pub max_key_bytes: usize,
    pub max_name_bytes: usize,
}

pub const STORED: StoredBounds = StoredBounds {
    max_steps: 64,
    max_input_bytes: 64 * 1024,
    max_key_bytes: 64 * 1024,
    max_name_bytes: 64 * 1024,
};

impl StoredBounds {
    /// Whether a stored run is within these bounds.
    pub fn holds(&self, run: &Run) -> bool {
        run.key.0.len() <= self.max_key_bytes
            && run.spec.steps.len() <= self.max_steps as usize
            && run.spec.input.len() <= self.max_input_bytes
            && run.spec.steps.iter().all(|s| {
                s.name.len() <= self.max_name_bytes && s.handler.len() <= self.max_name_bytes
            })
    }
}

/// Hard bounds on admission and execution (bounded everything).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_steps: u32,
    pub max_attempts: u32,
    pub max_step_deadline_ms: u64,
    /// Operation keys ride in every record, probe URL and idempotency key.
    pub max_key_bytes: usize,
    /// Step and handler names.
    pub max_name_bytes: usize,
    /// Non-terminal runs the index may hold; submits beyond it are refused
    /// `AtCapacity` (dispatch drains them at a bounded rate).
    pub max_live_runs: usize,
    /// Approximate bytes of every retained run (terminal ones stay seven
    /// days with their inputs); submits that would pass it are refused.
    pub max_index_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024,
            max_output_bytes: 256 * 1024,
            max_steps: 64,
            max_attempts: 5,
            max_step_deadline_ms: 30_000,
            max_key_bytes: 256,
            max_name_bytes: 128,
            max_live_runs: 100_000,
            // Sized for a 2 GB host (D-031): peak RSS has been measured at
            // up to 1.6 × index bytes, plus up to one snapshot during a
            // compaction, plus ~100 MiB.
            max_index_bytes: 512 * 1024 * 1024,
        }
    }
}

impl Limits {
    /// Admission bounds may be tightened freely, but a run admitted past
    /// the stored bounds would be refused when its snapshot is read back
    /// (D-033): refuse such a configuration up front.
    pub fn validate(&self) -> Result<(), String> {
        let s = STORED;
        if self.max_steps > s.max_steps
            || self.max_input_bytes > s.max_input_bytes
            || self.max_key_bytes > s.max_key_bytes
            || self.max_name_bytes > s.max_name_bytes
        {
            return Err(format!(
                "admission limits must stay within the stored bounds: at most {} steps, \
                 {} input bytes, {} key bytes, {} name bytes",
                s.max_steps, s.max_input_bytes, s.max_key_bytes, s.max_name_bytes
            ));
        }
        Ok(())
    }

    pub fn admits(&self, spec: &JobSpec) -> Result<(), Refusal> {
        let bad_name = |n: &str| n.is_empty() || n.len() > self.max_name_bytes;
        if spec.input.len() > self.max_input_bytes
            || spec.steps.len() as u32 > self.max_steps
            || spec.steps.is_empty()
            || spec.version == 0
            || spec
                .steps
                .iter()
                .any(|s| bad_name(&s.name) || bad_name(&s.handler))
        {
            return Err(Refusal::OverLimits);
        }
        Ok(())
    }

    /// The bytes a run with this key and spec costs the index (an
    /// approximation: payloads plus a fixed overhead per run and step).
    pub fn run_bytes(key: &OperationKey, spec: &JobSpec) -> u64 {
        let steps: usize = spec
            .steps
            .iter()
            .map(|s| 48 + s.name.len() + s.handler.len())
            .sum();
        (160 + key.0.len() + spec.input.len() + steps) as u64
    }

    /// An operation key is non-empty, at most `max_key_bytes`, and free of
    /// control characters (it is echoed into logs, URLs and headers).
    pub fn admits_key(&self, key: &OperationKey) -> Result<(), Refusal> {
        if key.0.is_empty()
            || key.0.len() > self.max_key_bytes
            || key.0.chars().any(char::is_control)
        {
            return Err(Refusal::OverLimits);
        }
        Ok(())
    }
}

// -------------------------------------------------------------------- retry

/// Bounded retries: finite count, capped jitter, per-attempt deadline (D-004).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_cap_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_ms: 500,
            max_delay_ms: 30_000,
            jitter_cap_ms: 250,
        }
    }
}

impl RetryPolicy {
    /// Deterministic backoff: base * 2^(attempt-1) clamped, plus a bounded
    /// jitter derived from the run id + attempt so replays agree.
    pub fn next_run_at(&self, now: Millis, attempt: u32, run: RunId) -> Millis {
        let exp = self
            .base_delay_ms
            .saturating_mul(1u64 << attempt.saturating_sub(1).min(20));
        let delay = exp.min(self.max_delay_ms);
        let jitter =
            (run.0.wrapping_mul(31).wrapping_add(attempt as u64)) % (self.jitter_cap_ms + 1);
        now + delay + jitter
    }
}

// ------------------------------------------------------------------ reducer

/// The pure reducer. `reduce` is total and deterministic: same run + event +
/// `now` -> same outcome. The coordinator supplies `now` (fake clock in tests),
/// the retry policy and the limits; the journal supplies the run state.
pub fn reduce(
    run: &Run,
    event: &Event,
    now: Millis,
    limits: &Limits,
    retry: &RetryPolicy,
) -> Result<(RunState, Vec<Effect>), Refusal> {
    use RunState as S;
    match (&run.state, event) {
        // --- terminal states refuse everything new --------------------------
        (s, _) if s.is_terminal() => match event {
            Event::CancelRequested => Err(Refusal::CancelTerminal),
            _ => Err(Refusal::Terminal),
        },

        // --- acceptance ------------------------------------------------------
        (
            S::Queued { .. } | S::Running { .. } | S::RetryWaiting { .. } | S::Reconciling { .. },
            Event::Accepted { .. },
        ) => Err(Refusal::DuplicateKey),

        // --- dispatch --------------------------------------------------------
        (
            S::Queued { .. } | S::RetryWaiting { .. },
            Event::StepDispatched {
                step,
                attempt,
                deadline,
            },
        ) => {
            if step.0 as usize >= run.spec.steps.len() {
                return Err(Refusal::NoSuchStep);
            }
            let expected_attempt = match &run.state {
                S::Queued { .. } => 1,
                S::RetryWaiting { attempt, .. } => attempt + 1,
                _ => unreachable!(),
            };
            if *attempt != expected_attempt {
                return Err(Refusal::StaleDispatch);
            }
            Ok((
                S::Running {
                    step: *step,
                    attempt: *attempt,
                    deadline: *deadline,
                },
                vec![],
            ))
        }
        (S::Running { .. } | S::Reconciling { .. }, Event::StepDispatched { .. }) => {
            Err(Refusal::StaleDispatch)
        }

        // --- checkpoint --------------------------------------------------------
        (S::Running { step, .. }, Event::StepCheckpointed { step: s, output }) => {
            if *s != *step {
                return Err(Refusal::StaleReply);
            }
            if output.len() > limits.max_output_bytes {
                return Err(Refusal::OverLimits);
            }
            if (step.0 as usize + 1) < run.spec.steps.len() {
                let next = StepIndex(step.0 + 1);
                let deadline = now + limits.max_step_deadline_ms;
                Ok((
                    S::Running {
                        step: next,
                        attempt: 1,
                        deadline,
                    },
                    vec![
                        Effect::CommitCheckpoint {
                            step: *s,
                            output: output.clone(),
                        },
                        Effect::DispatchStep {
                            step: next,
                            attempt: 1,
                            deadline,
                        },
                    ],
                ))
            } else {
                Ok((
                    S::Done,
                    vec![
                        Effect::CommitCheckpoint {
                            step: *s,
                            output: output.clone(),
                        },
                        Effect::Finish,
                    ],
                ))
            }
        }
        (_, Event::StepCheckpointed { .. }) => Err(Refusal::StaleReply),

        // --- failure -> bounded backoff --------------------------------------
        (S::Running { step, attempt, .. }, Event::StepFailed { step: s }) => {
            if *s != *step {
                return Err(Refusal::StaleReply);
            }
            if *attempt >= retry.max_attempts {
                Ok((S::Failed { step: *step }, vec![Effect::EmitTerminal]))
            } else {
                let at = retry.next_run_at(now, *attempt, run.id);
                Ok((
                    S::RetryWaiting {
                        step: *step,
                        attempt: *attempt,
                        next_run_at: at,
                    },
                    vec![Effect::ScheduleWake { at }],
                ))
            }
        }
        (_, Event::StepFailed { .. }) => Err(Refusal::StaleReply),

        // --- lost reply -> reconcile, never repeat blindly ---------------------
        (S::Running { step, attempt, .. }, Event::ReplyLost { step: s }) => {
            if *s != *step {
                return Err(Refusal::StaleReply);
            }
            Ok((
                S::Reconciling {
                    step: *step,
                    attempt: *attempt,
                },
                vec![Effect::Reconcile { step: *s }],
            ))
        }
        (_, Event::ReplyLost { .. }) => Err(Refusal::StaleReply),

        (
            S::Reconciling { step, attempt, .. },
            Event::Reconciled {
                step: s,
                effect_found,
            },
        ) => {
            if *s != *step {
                return Err(Refusal::StaleReply);
            }
            if *effect_found {
                // The vanished reply's remote effect did land — advance with
                // an empty checkpoint, never repeat the remote call.
                let output = Vec::new();
                if (step.0 as usize + 1) < run.spec.steps.len() {
                    let next = StepIndex(step.0 + 1);
                    let deadline = now + limits.max_step_deadline_ms;
                    Ok((
                        S::Running {
                            step: next,
                            attempt: 1,
                            deadline,
                        },
                        vec![
                            Effect::CommitCheckpoint { step: *s, output },
                            Effect::DispatchStep {
                                step: next,
                                attempt: 1,
                                deadline,
                            },
                        ],
                    ))
                } else {
                    Ok((
                        S::Done,
                        vec![
                            Effect::CommitCheckpoint { step: *s, output },
                            Effect::Finish,
                        ],
                    ))
                }
            } else if *attempt >= retry.max_attempts {
                Ok((S::Failed { step: *step }, vec![Effect::EmitTerminal]))
            } else {
                let at = retry.next_run_at(now, *attempt, run.id);
                Ok((
                    S::RetryWaiting {
                        step: *step,
                        attempt: *attempt,
                        next_run_at: at,
                    },
                    vec![Effect::ScheduleWake { at }],
                ))
            }
        }
        (_, Event::Reconciled { .. }) => Err(Refusal::NothingToReconcile),

        // --- cancellation ------------------------------------------------------
        (_, Event::CancelRequested) => Ok((S::Cancelled, vec![Effect::EmitTerminal])),

        // --- anything else is a transition that does not belong to this state --
        (_, _) => Err(Refusal::StaleReply),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> JobSpec {
        JobSpec {
            version: 1,
            input: b"x".to_vec(),
            steps: vec![
                StepSpec {
                    name: "a".into(),
                    handler: "h".into(),
                },
                StepSpec {
                    name: "b".into(),
                    handler: "h".into(),
                },
            ],
        }
    }

    fn run(state: RunState) -> Run {
        Run {
            id: RunId(7),
            key: OperationKey("k".into()),
            spec: spec(),
            state,
            finished_at: None,
        }
    }

    fn disp(step: u32, attempt: u32) -> Event {
        Event::StepDispatched {
            step: StepIndex(step),
            attempt,
            deadline: 1000,
        }
    }

    const NOW: Millis = 100;

    #[test]
    fn happy_path_two_steps() {
        let l = Limits::default();
        let r = RetryPolicy::default();
        let (s, _e) = reduce(
            &run(RunState::Queued { next_run_at: 0 }),
            &disp(0, 1),
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            s,
            RunState::Running {
                step: StepIndex(0),
                attempt: 1,
                deadline: 1000
            }
        );
        let (s, eff) = reduce(
            &run(s),
            &Event::StepCheckpointed {
                step: StepIndex(0),
                output: vec![1],
            },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(s, RunState::Running { step, .. } if step == StepIndex(1)));
        assert!(
            matches!(eff.last(), Some(Effect::DispatchStep { step, .. }) if *step == StepIndex(1))
        );
        let (s, eff) = reduce(
            &run(s),
            &Event::StepCheckpointed {
                step: StepIndex(1),
                output: vec![2],
            },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(s, RunState::Done);
        assert!(matches!(eff.last(), Some(Effect::Finish)));
    }

    #[test]
    fn terminal_states_refuse_everything() {
        let l = Limits::default();
        let r = RetryPolicy::default();
        for t in [
            RunState::Done,
            RunState::Failed { step: StepIndex(0) },
            RunState::Cancelled,
        ] {
            assert_eq!(
                reduce(&run(t.clone()), &disp(0, 1), NOW, &l, &r),
                Err(Refusal::Terminal)
            );
            assert_eq!(
                reduce(&run(t), &Event::CancelRequested, NOW, &l, &r),
                Err(Refusal::CancelTerminal)
            );
        }
    }

    #[test]
    fn stale_dispatch_and_stale_reply_named() {
        let l = Limits::default();
        let r = RetryPolicy::default();
        let running = RunState::Running {
            step: StepIndex(0),
            attempt: 1,
            deadline: 1000,
        };
        assert_eq!(
            reduce(&run(running.clone()), &disp(0, 2), NOW, &l, &r),
            Err(Refusal::StaleDispatch)
        );
        assert_eq!(
            reduce(
                &run(RunState::Queued { next_run_at: 0 }),
                &disp(0, 2),
                NOW,
                &l,
                &r
            ),
            Err(Refusal::StaleDispatch)
        );
        assert_eq!(
            reduce(
                &run(running.clone()),
                &Event::StepCheckpointed {
                    step: StepIndex(1),
                    output: vec![]
                },
                NOW,
                &l,
                &r
            ),
            Err(Refusal::StaleReply)
        );
        assert_eq!(
            reduce(
                &run(running),
                &Event::ReplyLost { step: StepIndex(1) },
                NOW,
                &l,
                &r
            ),
            Err(Refusal::StaleReply)
        );
    }

    #[test]
    fn retries_bounded_then_failed() {
        let l = Limits::default();
        let r = RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        };
        let running = RunState::Running {
            step: StepIndex(0),
            attempt: 1,
            deadline: 1,
        };
        let (s, _) = reduce(
            &run(running),
            &Event::StepFailed { step: StepIndex(0) },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(s, RunState::RetryWaiting { attempt: 1, .. }));
        let running2 = RunState::Running {
            step: StepIndex(0),
            attempt: 2,
            deadline: 1,
        };
        let (s, _) = reduce(
            &run(running2),
            &Event::StepFailed { step: StepIndex(0) },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(s, RunState::Failed { step: StepIndex(0) });
    }

    #[test]
    fn lost_reply_reconciles_before_retry() {
        let l = Limits::default();
        let r = RetryPolicy::default();
        let running = RunState::Running {
            step: StepIndex(0),
            attempt: 1,
            deadline: 1,
        };
        let (s, eff) = reduce(
            &run(running),
            &Event::ReplyLost { step: StepIndex(0) },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(s, RunState::Reconciling { .. }));
        assert_eq!(eff, vec![Effect::Reconcile { step: StepIndex(0) }]);
        assert_eq!(
            reduce(
                &run(s.clone()),
                &Event::StepCheckpointed {
                    step: StepIndex(0),
                    output: vec![]
                },
                NOW,
                &l,
                &r
            ),
            Err(Refusal::StaleReply)
        );
        let (s, _) = reduce(
            &run(s),
            &Event::Reconciled {
                step: StepIndex(0),
                effect_found: false,
            },
            NOW,
            &l,
            &r,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(s, RunState::RetryWaiting { attempt: 1, .. }));
    }

    #[test]
    fn admission_limits_must_stay_within_the_stored_bounds() {
        assert!(Limits::default().validate().is_ok());
        let looser_key = Limits {
            max_key_bytes: 1024, // looser than the default, within the stored bound
            ..Limits::default()
        };
        assert!(looser_key.validate().is_ok());
        for wide in [
            Limits {
                max_steps: STORED.max_steps + 1,
                ..Limits::default()
            },
            Limits {
                max_input_bytes: STORED.max_input_bytes + 1,
                ..Limits::default()
            },
            Limits {
                max_name_bytes: STORED.max_name_bytes + 1,
                ..Limits::default()
            },
        ] {
            assert!(wide.validate().is_err(), "{wide:?}");
        }
    }

    #[test]
    fn limits_refuse_bad_specs() {
        let l = Limits {
            max_input_bytes: 2,
            max_steps: 1,
            ..Limits::default()
        };
        assert_eq!(l.admits(&spec()), Err(Refusal::OverLimits));
        let mut empty = spec();
        empty.steps.clear();
        assert_eq!(Limits::default().admits(&empty), Err(Refusal::OverLimits));
    }

    /// D-031's sizing rule for the default index bound: the worst peak it
    /// allows (2.6 × max_index_bytes + 100 MiB: restart and steady state
    /// measured at up to 1.6 ×, plus up to one snapshot during a
    /// compaction) must leave 0.5 GiB of a 2 GiB host to the OS.
    #[test]
    fn default_index_bound_fits_a_2_gb_host() {
        let bound = Limits::default().max_index_bytes as f64;
        let worst_peak = 2.6 * bound + 100.0 * 1024.0 * 1024.0;
        let budget = 1.5 * 1024.0 * 1024.0 * 1024.0;
        assert!(
            worst_peak <= budget,
            "default max_index_bytes {bound} allows a {worst_peak} B peak over {budget} B"
        );
    }

    #[test]
    fn backoff_is_deterministic() {
        let r = RetryPolicy::default();
        let a = r.next_run_at(NOW, 1, RunId(7));
        let b = r.next_run_at(NOW, 1, RunId(7));
        assert_eq!(a, b);
        assert!(r.next_run_at(NOW, 40, RunId(7)) <= NOW + r.max_delay_ms + r.jitter_cap_ms);
    }
}
