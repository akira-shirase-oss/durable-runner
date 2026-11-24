// A run's timeline, pure: the server's events (GET /jobs/{id}/events) turned into step attempts for the Gantt,
// annotated rows for the vertical timeline, and the layout maths for both. Nothing here invents an event: a gap
// the server reports (compaction, eviction, the per-run cap) stays a gap, and an attempt whose answer is missing
// is drawn as unknown, not as a success.
import type { RunEvent } from './api.ts';
import { fmtMs as fmtSpan } from './format.ts';

export type AttemptOutcome =
	/** checkpointed */
	| 'ok'
	/** step_failed: error, 5xx or timeout */
	| 'failed'
	/** reply_lost: the effect is uncertain until reconciled */
	| 'lost'
	/** dispatched, no answer yet, run still live */
	| 'open'
	/** no answer and none will come (the run finished, or the timeline lost the answer) */
	| 'unknown';

export interface Attempt {
	step: number;
	attempt: number;
	start: number;
	/** The answer's time; null while open. */
	end: number | null;
	outcome: AttemptOutcome;
	deadline: number | null;
	/** reply_lost → reconciled: when the probe answered and what it found. */
	reconciledAt: number | null;
	effectFound: boolean | null;
}

/** Attempts per step from the events, in dispatch order. A `finished` event closes anything left open. `gapAt`
 *  is the index of the first event after a hole the server reported (gapIndex): attempts open across it are
 *  closed as unknown at the last event before it, because their answers fell into the hole, and an answer after
 *  it is never matched to a dispatch before it. Zero-length steps are normal: a step's dispatch shares its `at`
 *  with the previous step's checkpoint. */
export function buildAttempts(events: readonly RunEvent[], gapAt = -1): Attempt[] {
	const out: Attempt[] = [];
	const open = new Map<number, Attempt>(); // step → the attempt awaiting its answer
	const lost = new Map<number, Attempt>(); // step → the attempt awaiting reconciliation
	for (let i = 0; i < events.length; i++) {
		const e = events[i]!;
		if (i === gapAt && i > 0) {
			const lastAt = events[i - 1]!.at;
			for (const a of open.values()) close(a, lastAt, 'unknown');
			open.clear();
			lost.clear();
		}
		const step = typeof e.step === 'number' ? e.step : null;
		switch (e.kind) {
			case 'dispatched': {
				if (step === null) break;
				const prev = open.get(step);
				if (prev) close(prev, e.at, 'unknown');
				const a: Attempt = {
					step,
					attempt: typeof e.attempt === 'number' ? e.attempt : 1,
					start: e.at,
					end: null,
					outcome: 'open',
					deadline: typeof e.deadline === 'number' ? e.deadline : null,
					reconciledAt: null,
					effectFound: null,
				};
				out.push(a);
				open.set(step, a);
				lost.delete(step);
				break;
			}
			case 'checkpointed':
			case 'step_failed':
			case 'reply_lost': {
				if (step === null) break;
				const a = open.get(step);
				if (!a) break; // the dispatch is outside the timeline (compacted or truncated)
				close(a, e.at, e.kind === 'checkpointed' ? 'ok' : e.kind === 'step_failed' ? 'failed' : 'lost');
				open.delete(step);
				if (e.kind === 'reply_lost') lost.set(step, a);
				break;
			}
			case 'reconciled': {
				if (step === null) break;
				const a = lost.get(step);
				if (!a) break;
				a.reconciledAt = e.at;
				a.effectFound = e.effect_found === true;
				lost.delete(step);
				break;
			}
			case 'finished': {
				for (const a of open.values()) close(a, e.at, 'unknown');
				open.clear();
				break;
			}
		}
	}
	return out;
}

function close(a: Attempt, at: number, outcome: AttemptOutcome) {
	a.end = at;
	a.outcome = outcome;
}

/** Whether an attempt's effect happened: checkpointed, or reconciled with the effect found. */
export function attemptSucceeded(a: Attempt): boolean {
	return a.outcome === 'ok' || (a.outcome === 'lost' && a.effectFound === true);
}

/** The Gantt's time domain. It starts at the first dispatch (a run that waited in the queue would otherwise squash
 *  every bar into the right edge; the wait is reported separately) and ends at the last answer, or `now` while an
 *  attempt is open. Never empty: at least `minSpan` wide. */
export function ganttDomain(attempts: readonly Attempt[], now: number, minSpan = 20): [number, number] | null {
	if (attempts.length === 0) return null;
	let start = Infinity;
	let end = -Infinity;
	for (const a of attempts) {
		start = Math.min(start, a.start);
		end = Math.max(end, a.end ?? now, a.reconciledAt ?? -Infinity);
	}
	if (end - start < minSpan) end = start + minSpan;
	return [start, end];
}

export interface Bar {
	x: number;
	w: number;
	/** The reconcile probe after a lost reply: from the bar's end to the probe's answer. */
	probeX: number | null;
	probeW: number;
}

/** A bar's pixels in a plot `width` wide: at least `minW` so an instant answer stays visible and hoverable. */
export function barLayout(a: Attempt, domain: [number, number], width: number, now: number, minW = 3): Bar {
	const [d0, d1] = domain;
	const k = width / Math.max(1, d1 - d0);
	const x = (a.start - d0) * k;
	const end = a.end ?? now;
	const w = Math.max(minW, (end - a.start) * k);
	let probeX: number | null = null;
	let probeW = 0;
	if (a.reconciledAt !== null && a.end !== null) {
		probeX = x + w;
		probeW = Math.max(minW, (a.reconciledAt - a.end) * k);
	}
	return { x: clamp(x, 0, width), w: Math.min(w, Math.max(minW, width - x)), probeX, probeW };
}

const DURATION_STEPS = [
	1, 2, 5, 10, 20, 50, 100, 200, 250, 500, 1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000,
	600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000, 21_600_000, 43_200_000, 86_400_000,
];

/** Offsets (ms from the domain start) for a relative time axis: the smallest clean step giving ≤ `maxTicks`. */
export function offsetTicks(span: number, maxTicks: number): number[] {
	if (!(span > 0) || !(maxTicks >= 1)) return [0];
	const step = DURATION_STEPS.find((s) => span / s <= maxTicks) ?? Math.ceil(span / maxTicks / 86_400_000) * 86_400_000;
	const out: number[] = [];
	for (let v = 0; v <= span + 1e-9; v += step) out.push(v);
	return out;
}

export type Tone = 'neutral' | 'run' | 'ok' | 'fail' | 'probe' | 'warn' | 'cancel';

/** One row of the vertical timeline. */
export interface TimelineRow {
	event: RunEvent;
	/** ms since the first event shown */
	offset: number;
	title: string;
	tone: Tone;
	/** extra facts: "answered in 120 ms", "after 2.10 s backoff", "deadline 10 s", "effect found" */
	notes: string[];
	/** For an answer: ms since its dispatch. */
	answeredIn: number | null;
}

/** The vertical timeline's rows, with the durations the server's events imply. Durations never reach across a
 *  hole the server reported (`gapAt`, as for buildAttempts). */
export function timelineRows(
	events: readonly RunEvent[],
	stepName: (i: number) => string | null = () => null,
	gapAt = -1,
): TimelineRow[] {
	const t0 = events[0]?.at ?? 0;
	const dispatchedAt = new Map<number, number>();
	const answeredAt = new Map<number, number>();
	const lostAt = new Map<number, number>();
	return events.map((e, i) => {
		if (i === gapAt) {
			dispatchedAt.clear();
			answeredAt.clear();
			lostAt.clear();
		}
		const step = typeof e.step === 'number' ? e.step : null;
		const name = step !== null ? stepName(step) : null;
		const label = step !== null ? `step ${step}${name ? ` (${name})` : ''}` : '';
		const notes: string[] = [];
		let answeredIn: number | null = null;
		let title: string;
		let tone: Tone = 'neutral';
		switch (e.kind) {
			case 'accepted':
				title = 'Accepted';
				if (typeof e.not_before === 'number' && e.not_before > e.at) notes.push(`held until ${fmtSpan(e.not_before - e.at)} later (not_before)`);
				break;
			case 'dispatched': {
				title = `Dispatched ${label}, attempt ${e.attempt ?? 1}`;
				tone = 'run';
				if (step !== null) {
					const prev = answeredAt.get(step) ?? lostAt.get(step);
					if (prev !== undefined && (e.attempt ?? 1) > 1) notes.push(`retry after ${fmtSpan(e.at - prev)}`);
					dispatchedAt.set(step, e.at);
				}
				if (typeof e.deadline === 'number') notes.push(`deadline ${fmtSpan(e.deadline - e.at)}`);
				break;
			}
			case 'checkpointed':
			case 'step_failed':
			case 'reply_lost': {
				title =
					e.kind === 'checkpointed'
						? `Checkpointed ${label}`
						: e.kind === 'step_failed'
							? `Failed attempt on ${label}`
							: `Reply lost on ${label}`;
				tone = e.kind === 'checkpointed' ? 'ok' : e.kind === 'step_failed' ? 'fail' : 'probe';
				if (step !== null) {
					const d = dispatchedAt.get(step);
					if (d !== undefined) {
						answeredIn = e.at - d;
						notes.push(`${e.kind === 'reply_lost' ? 'lost after' : 'answered in'} ${fmtSpan(answeredIn)}`);
						dispatchedAt.delete(step);
					}
					if (e.kind === 'reply_lost') lostAt.set(step, e.at);
					else answeredAt.set(step, e.at);
				}
				if (e.kind === 'reply_lost') notes.push('effect uncertain: reconciling before any retry');
				break;
			}
			case 'reconciled': {
				title = `Reconciled ${label}`;
				tone = e.effect_found ? 'ok' : 'warn';
				notes.push(e.effect_found ? 'effect found: counted as done, not repeated' : 'no effect found: safe to retry');
				if (step !== null) {
					const l = lostAt.get(step);
					if (l !== undefined) notes.push(`probe took ${fmtSpan(e.at - l)}`);
					lostAt.delete(step);
					answeredAt.set(step, e.at);
				}
				break;
			}
			case 'cancel_requested':
				title = 'Cancel requested';
				tone = 'cancel';
				notes.push('an effect already sent cannot be undone');
				break;
			case 'finished':
				title = e.outcome === 'done' ? 'Finished: done' : e.outcome === 'failed' ? 'Finished: failed' : e.outcome === 'cancelled' ? 'Finished: cancelled' : `Finished: ${e.outcome ?? 'unknown'}`;
				tone = e.outcome === 'done' ? 'ok' : e.outcome === 'failed' ? 'fail' : 'cancel';
				break;
			default:
				title = String(e.kind);
		}
		return { event: e, offset: e.at - t0, title, tone, notes, answeredIn };
	});
}

/** Where the vertical timeline marks a gap: before the first row (compacted or evicted) or after the 16th row
 *  (truncated: the server keeps the first 16 and the last 80 events). -1 when the timeline is complete. */
export const TRUNCATE_HEAD = 16;
export function gapIndex(complete: boolean, reason: string | null, count: number): number {
	if (complete) return -1;
	if (reason === 'truncated') return count > TRUNCATE_HEAD ? TRUNCATE_HEAD : count;
	return 0;
}

/** Step progress from the run's state and step count: per step, done / current / failed / pending. */
export type StepState = 'done' | 'now' | 'retry' | 'probe' | 'failed' | 'pending' | 'cancelled';
export function stepStates(state: string, steps: number, events: readonly RunEvent[] = []): StepState[] {
	const kind = state.split(':')[0] ?? '';
	const m = /(?:^|:)step:(\d+)/.exec(state);
	const at = m ? Number(m[1]) : null;
	const checkpointed = new Set<number>();
	for (const e of events) {
		if (e.kind === 'checkpointed' && typeof e.step === 'number') checkpointed.add(e.step);
		if (e.kind === 'reconciled' && e.effect_found && typeof e.step === 'number') checkpointed.add(e.step);
	}
	const out: StepState[] = [];
	for (let i = 0; i < Math.max(0, steps); i++) {
		if (kind === 'done') out.push('done');
		else if (at !== null && i < at) out.push('done');
		else if (at !== null && i === at)
			out.push(kind === 'failed' ? 'failed' : kind === 'retry_waiting' ? 'retry' : kind === 'reconciling' ? 'probe' : kind === 'running' ? 'now' : 'pending');
		else if (checkpointed.has(i)) out.push('done');
		else out.push(kind === 'cancelled' ? 'cancelled' : 'pending');
	}
	return out;
}

function clamp(v: number, lo: number, hi: number): number {
	return Math.min(hi, Math.max(lo, v));
}
