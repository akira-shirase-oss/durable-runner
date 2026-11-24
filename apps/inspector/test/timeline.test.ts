import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { RunEvent } from '../src/lib/api.ts';
import { attemptSucceeded, barLayout, buildAttempts, ganttDomain, gapIndex, offsetTicks, stepStates, timelineRows } from '../src/lib/timeline.ts';

// The contract's example: step 0 ok; step 1 fails, retries, loses its reply and is reconciled with the effect found.
const T = 1_764_000_000_000;
const EXAMPLE: RunEvent[] = [
	{ at: T, kind: 'accepted', not_before: T },
	{ at: T + 50, kind: 'dispatched', step: 0, attempt: 1, deadline: T + 10_050 },
	{ at: T + 170, kind: 'checkpointed', step: 0 },
	{ at: T + 200, kind: 'dispatched', step: 1, attempt: 1, deadline: T + 10_200 },
	{ at: T + 2200, kind: 'step_failed', step: 1 },
	{ at: T + 4300, kind: 'dispatched', step: 1, attempt: 2, deadline: T + 14_300 },
	{ at: T + 4900, kind: 'reply_lost', step: 1 },
	{ at: T + 5100, kind: 'reconciled', step: 1, effect_found: true },
	{ at: T + 5100, kind: 'finished', outcome: 'done' },
];

test('attempts: one per dispatch, closed by its own answer', () => {
	const a = buildAttempts(EXAMPLE);
	assert.deepEqual(
		a.map((x) => [x.step, x.attempt, x.start - T, x.end === null ? null : x.end - T, x.outcome, x.effectFound]),
		[
			[0, 1, 50, 170, 'ok', null],
			[1, 1, 200, 2200, 'failed', null],
			[1, 2, 4300, 4900, 'lost', true],
		],
	);
	assert.equal(a[2]!.reconciledAt, T + 5100);
	assert.deepEqual(a.map(attemptSucceeded), [true, false, true]);
});

test('an unanswered attempt stays open while the run lives, and is unknown once it finished', () => {
	const live = buildAttempts([{ at: T, kind: 'dispatched', step: 0, attempt: 1 }]);
	assert.equal(live[0]!.outcome, 'open');
	assert.equal(live[0]!.end, null);
	const ended = buildAttempts([
		{ at: T, kind: 'dispatched', step: 0, attempt: 1 },
		{ at: T + 10, kind: 'cancel_requested' },
		{ at: T + 900, kind: 'finished', outcome: 'cancelled' },
	]);
	assert.deepEqual([ended[0]!.outcome, ended[0]!.end], ['unknown', T + 900]);
	// a second dispatch with no answer between (an answer the timeline lost) closes the first as unknown
	const gap = buildAttempts([
		{ at: T, kind: 'dispatched', step: 3, attempt: 1 },
		{ at: T + 5, kind: 'dispatched', step: 3, attempt: 2 },
	]);
	assert.deepEqual(gap.map((x) => x.outcome), ['unknown', 'open']);
});

test('answers whose dispatch fell outside the timeline are not invented into attempts', () => {
	assert.deepEqual(buildAttempts([{ at: T, kind: 'checkpointed', step: 4 }, { at: T, kind: 'reconciled', step: 4, effect_found: false }]), []);
});

test('the Gantt domain starts at the first dispatch and ends at the last answer or now', () => {
	const a = buildAttempts(EXAMPLE);
	assert.deepEqual(ganttDomain(a, T + 99_999), [T + 50, T + 5100]); // the probe's answer extends the end
	const open = buildAttempts([{ at: T, kind: 'dispatched', step: 0 }]);
	assert.deepEqual(ganttDomain(open, T + 4000), [T, T + 4000]);
	assert.deepEqual(ganttDomain(open, T + 10), [T, T + 20]); // never narrower than minSpan (20 ms)
	assert.equal(ganttDomain([], T), null);
});

test('bars: proportional, at least 3 px, the probe after a lost reply', () => {
	const a = buildAttempts(EXAMPLE);
	const d = ganttDomain(a, T)!;
	const w = 505; // 5050 ms → 0.1 px/ms
	const bars = a.map((x) => barLayout(x, d, w, T));
	assert.deepEqual(bars[0], { x: 0, w: 12, probeX: null, probeW: 0 });
	assert.deepEqual([bars[1]!.x, bars[1]!.w], [15, 200]);
	assert.deepEqual([bars[2]!.x, bars[2]!.w, bars[2]!.probeX, bars[2]!.probeW], [425, 60, 485, 20]);
	const instant = barLayout({ ...a[0]!, end: a[0]!.start }, d, w, T);
	assert.equal(instant.w, 3);
});

test('offset ticks use clean steps', () => {
	assert.deepEqual(offsetTicks(5050, 6), [0, 1000, 2000, 3000, 4000, 5000]);
	assert.deepEqual(offsetTicks(120, 5), [0, 50, 100]);
	assert.deepEqual(offsetTicks(0, 5), [0]);
	assert.equal(offsetTicks(7_200_000, 6).length, 5); // 30-minute steps
});

test('timeline rows carry the durations the events imply', () => {
	const rows = timelineRows(EXAMPLE, (i) => ['fetch', 'render'][i] ?? null);
	assert.deepEqual(rows.map((r) => r.title), [
		'Accepted',
		'Dispatched step 0 (fetch), attempt 1',
		'Checkpointed step 0 (fetch)',
		'Dispatched step 1 (render), attempt 1',
		'Failed attempt on step 1 (render)',
		'Dispatched step 1 (render), attempt 2',
		'Reply lost on step 1 (render)',
		'Reconciled step 1 (render)',
		'Finished: done',
	]);
	assert.deepEqual(rows.map((r) => r.tone), ['neutral', 'run', 'ok', 'run', 'fail', 'run', 'probe', 'ok', 'ok']);
	assert.deepEqual(rows.map((r) => r.answeredIn), [null, null, 120, null, 2000, null, 600, null, null]);
	assert.ok(rows[1]!.notes.includes('deadline 10 s'));
	assert.ok(rows[2]!.notes.includes('answered in 120 ms'));
	assert.ok(rows[5]!.notes.includes('retry after 2.1 s'), rows[5]!.notes.join('|'));
	assert.ok(rows[7]!.notes.includes('effect found: counted as done, not repeated'));
	assert.ok(rows[7]!.notes.includes('probe took 200 ms'));
	assert.deepEqual(rows.map((r) => r.offset), [0, 50, 170, 200, 2200, 4300, 4900, 5100, 5100]);
});

test('gaps: truncated after the first 16, compacted or evicted at the top', () => {
	assert.equal(gapIndex(true, null, 30), -1);
	assert.equal(gapIndex(false, 'truncated', 96), 16);
	assert.equal(gapIndex(false, 'truncated', 10), 10);
	assert.equal(gapIndex(false, 'compacted', 12), 0);
	assert.equal(gapIndex(false, 'evicted', 0), 0);
});

test('step progress from the state the server reports', () => {
	assert.deepEqual(stepStates('running:step:1:attempt:2', 4), ['done', 'now', 'pending', 'pending']);
	assert.deepEqual(stepStates('retry_waiting:step:0:attempt:3', 2), ['retry', 'pending']);
	assert.deepEqual(stepStates('reconciling:step:2', 3), ['done', 'done', 'probe']);
	assert.deepEqual(stepStates('failed:step:2', 3), ['done', 'done', 'failed']);
	assert.deepEqual(stepStates('done', 3), ['done', 'done', 'done']);
	assert.deepEqual(stepStates('queued', 2), ['pending', 'pending']);
	assert.deepEqual(stepStates('cancelled', 3, EXAMPLE.slice(0, 3)), ['done', 'cancelled', 'cancelled']);
	assert.deepEqual(stepStates('running:step:0:attempt:1', 0), []);
});

test('a hole the server reported is never bridged: no attempt or duration spans it', () => {
	const ev: RunEvent[] = [
		{ at: T, kind: 'dispatched', step: 1, attempt: 7 },
		// … events dropped by the per-run cap …
		{ at: T + 9000, kind: 'step_failed', step: 1 },
		{ at: T + 9500, kind: 'dispatched', step: 1, attempt: 10 },
		{ at: T + 9900, kind: 'checkpointed', step: 1 },
	];
	const a = buildAttempts(ev, 1);
	assert.deepEqual(a.map((x) => [x.attempt, x.outcome, x.end === null ? null : x.end - T]), [[7, 'unknown', 0], [10, 'ok', 9900]]);
	const rows = timelineRows(ev, () => null, 1);
	assert.equal(rows[1]!.answeredIn, null); // not "answered in 9 s"
	assert.deepEqual(rows[1]!.notes, []);
	assert.equal(rows[3]!.answeredIn, 400);
	// without the hole the same events would pair up
	assert.equal(timelineRows(ev)[1]!.answeredIn, 9000);
});

test('zero-length steps: a dispatch at the same instant as the previous checkpoint', () => {
	// GET /jobs/1/events from a real server (obs-check): steps 6 and 9 ms long, the second dispatched at the
	// checkpoint's own millisecond
	const ev: RunEvent[] = [
		{ at: 1795540014541, kind: 'accepted', not_before: 1795540014541 },
		{ at: 1795540014560, attempt: 1, deadline: 1795540044560, kind: 'dispatched', step: 0 },
		{ at: 1795540014566, kind: 'checkpointed', step: 0 },
		{ at: 1795540014566, attempt: 1, deadline: 1795540044566, kind: 'dispatched', step: 1 },
		{ at: 1795540014575, kind: 'checkpointed', step: 1 },
		{ at: 1795540014575, kind: 'finished', outcome: 'done' },
	];
	const a = buildAttempts(ev);
	assert.deepEqual(a.map((x) => [x.step, x.outcome, x.end! - x.start]), [[0, 'ok', 6], [1, 'ok', 9]]);
	assert.deepEqual(ganttDomain(a, 0), [1795540014560, 1795540014580]); // 15 ms, widened to 20
	const rows = timelineRows(ev);
	assert.deepEqual(rows.map((r) => r.offset), [0, 19, 25, 25, 34, 34]);
	assert.ok(rows[4]!.notes.includes('answered in 9 ms'));
});
