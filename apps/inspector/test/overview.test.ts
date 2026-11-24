import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { MetricPoint, Metrics } from '../src/lib/api.ts';
import { column, coveredMs, displayBins, orderedStates, overviewModel } from '../src/lib/overview.ts';
import { binLabel, niceFactor } from '../src/lib/series.ts';

function pt(t: number, o: Partial<MetricPoint> = {}): MetricPoint {
	return {
		t, accepted: 0, deduplicated: 0, refused: 0, completed: 0, failed: 0, cancelled: 0, dispatched: 0, steps_ok: 0,
		steps_failed: 0, replies_lost: 0, reconciled: 0, live_runs: null, active_runs: null, in_flight: null,
		step_ms: { p50: null, p95: null, max: null }, ...o,
	};
}

test('bins merge to round durations', () => {
	assert.equal(niceFactor(10_000, 1), 1);
	assert.equal(niceFactor(10_000, 2), 3); // 30 s
	assert.equal(niceFactor(10_000, 4), 6); // 1 min
	assert.equal(niceFactor(60_000, 4), 5); // 5 min
	assert.equal(niceFactor(60_000, 12), 15);
	assert.equal(niceFactor(60_000, 5000), 5000);
	assert.equal(binLabel(10_000), '10 s');
	assert.equal(binLabel(30_000), '30 s');
	assert.equal(binLabel(300_000), '5 min');
	assert.equal(binLabel(3_600_000), '1 h');
	const m = { bucket_ms: 10_000, points: Array.from({ length: 360 }, (_, i) => pt(i * 10_000)) };
	const d = displayBins(m, 100); // 360 / 100 → at least 4 → 6 (1 min)
	assert.equal(d.binMs, 60_000);
	assert.equal(d.bins.length, 60);
});

test('the overview model: rates over the window, success without cancellations, newest gauges', () => {
	const points = [
		pt(0, { accepted: 30, completed: 20, failed: 1, live_runs: 100, active_runs: 5, in_flight: 2 }),
		pt(10_000, { accepted: 30, completed: 30, cancelled: 4, live_runs: null, active_runs: 6, in_flight: null }),
	];
	const m: Metrics = {
		now: 20_000, since: 0, window_ms: 60_000, bucket_ms: 10_000, points,
		totals: { accepted: 60, deduplicated: 0, refused: 0, completed: 50, failed: 1, cancelled: 4, dispatched: 0, steps_ok: 0, steps_failed: 0, replies_lost: 0, reconciled: 0, step_ms: { p50: 120, p95: 480, max: 900 } },
		states: {}, handlers: [],
	};
	const o = overviewModel(m, 120);
	assert.equal(o.liveRuns, 100); // the newest SAMPLED value
	assert.equal(o.activeRuns, 6);
	assert.equal(o.inFlight, 2);
	// the counters cover 0 … now (20 s), not the whole 60 s window: 50 in 20 s is 150/min, not 50/min
	assert.equal(o.completedPerMin, 150);
	assert.equal(o.acceptedPerMin, 180);
	assert.equal(o.success, 50 / 51);
	assert.equal(o.finished, 55);
	assert.deepEqual([o.p50, o.p95, o.max], [120, 480, 900]);
	assert.deepEqual(o.successSeries, [20 / 21, 1]);
	assert.equal(o.binText, '10 s');
});

test('states in lifecycle order, unknown ones kept', () => {
	assert.deepEqual(orderedStates({ done: 3, queued: 1, paused: 2 }).map(([k, v]) => `${k}=${v}`), [
		'queued=1', 'running=0', 'retry_waiting=0', 'reconciling=0', 'done=3', 'failed=0', 'cancelled=0', 'paused=2',
	]);
});

test('the bucket in progress is partial; bins before the server started carry no data', () => {
	// 10 s buckets, server started at 25 s, now 52 s: the bucket at 50 s is in progress
	const points = [0, 10_000, 20_000, 30_000, 40_000, 50_000].map((t) => pt(t, { accepted: 1, live_runs: t >= 30_000 ? 4 : null }));
	const { bins } = displayBins({ points, bucket_ms: 10_000, now: 52_000, since: 25_000 }, 120);
	assert.deepEqual(bins.map((b) => [b.t, b.before, b.partial]), [
		[0, true, false], [10_000, true, false], [20_000, false, false], [30_000, false, false], [40_000, false, false], [50_000, false, true],
	]);
	assert.deepEqual(column(bins, 'accepted'), [null, null, 1, 1, 1, 1]); // before the start: no data, not zero
	assert.deepEqual(column(bins, 'live_runs'), [null, null, null, 4, 4, 4]); // never sampled: a gap
	// merged bins: the newest bin holds the bucket in progress
	const merged = displayBins({ points, bucket_ms: 10_000, now: 52_000, since: 25_000 }, 2).bins;
	assert.deepEqual(merged.map((b) => [b.t, b.end, b.partial, b.before]), [[0, 30_000, false, false], [30_000, 60_000, true, false]]);
});

test('rates cover the time the counters do', () => {
	const base = { points: [pt(0), pt(10_000)], window_ms: 60_000 };
	assert.equal(coveredMs({ ...base, now: 15_000, since: -1 }), 15_000);
	assert.equal(coveredMs({ ...base, now: 15_000, since: 5_000 }), 10_000); // a new server: from its start
	assert.equal(coveredMs({ ...base, now: 10, since: 0 }), 1000); // never below a second
});
