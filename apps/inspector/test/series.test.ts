import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { MetricPoint } from '../src/lib/api.ts';
import { areaPath, columnPath, lastValue, linePath, mergeFactor, mergePoints, niceScale, parseWindow, peak, perMinute, stackedMax, stackSegments, successRate, timeTicks, total } from '../src/lib/series.ts';

function pt(t: number, o: Partial<MetricPoint> = {}): MetricPoint {
	return {
		t, accepted: 0, deduplicated: 0, refused: 0, completed: 0, failed: 0, cancelled: 0, dispatched: 0, steps_ok: 0,
		steps_failed: 0, replies_lost: 0, reconciled: 0, live_runs: null, active_runs: null, in_flight: null,
		step_ms: { p50: null, p95: null, max: null }, ...o,
	};
}

test('windows parse strictly', () => {
	assert.equal(parseWindow('6h'), '6h');
	assert.equal(parseWindow('2h'), null);
	assert.equal(parseWindow(undefined), null);
});

test('merge factor keeps at most maxBins', () => {
	assert.equal(mergeFactor(90, 120), 1); // 15m: every 10 s bucket fits
	assert.equal(mergeFactor(360, 120), 3); // 1h → 30 s bins
	assert.equal(mergeFactor(1440, 120), 12); // 24h → 12 min bins
	assert.equal(mergeFactor(361, 120), 4);
	assert.equal(mergeFactor(0, 120), 1);
	assert.equal(mergeFactor(360, 0), 1);
});

test('bins: counters sum, gauges keep the last sample, latency keeps the worst bucket', () => {
	const pts = [
		pt(0, { accepted: 1, live_runs: 5, step_ms: { p50: 10, p95: 40, max: 50 } }),
		pt(10, { accepted: 2, completed: 1, live_runs: null, step_ms: { p50: 30, p95: 20, max: 90 } }),
		pt(20, { accepted: 4, failed: 1, live_runs: 7 }),
		pt(30, { accepted: 8, step_ms: { p50: null, p95: null, max: null } }),
	];
	const bins = mergePoints(pts, 10, 2);
	assert.equal(bins.length, 2);
	assert.deepEqual(
		bins.map((b) => [b.t, b.end, b.buckets, b.accepted, b.completed, b.failed, b.live_runs, b.p50, b.p95, b.max]),
		[
			[0, 20, 2, 3, 1, 0, 5, 30, 40, 90],
			[20, 40, 2, 12, 0, 1, 7, null, null, null],
		],
	);
});

test('bins are cut from the newest end: only the oldest bin may be partial', () => {
	const pts = [0, 10, 20, 30, 40].map((t) => pt(t, { accepted: 1 }));
	const bins = mergePoints(pts, 10, 2);
	assert.deepEqual(bins.map((b) => [b.t, b.buckets, b.accepted]), [[0, 1, 1], [10, 2, 2], [30, 2, 2]]);
	assert.deepEqual(mergePoints([], 10, 3), []);
	// factor 1 is the identity on counters
	assert.equal(mergePoints(pts, 10, 1).length, 5);
});

test('a nice y-axis: clean steps, never a collapsed plot', () => {
	assert.deepEqual(niceScale(38), { max: 40, step: 10, ticks: [0, 10, 20, 30, 40] });
	assert.deepEqual(niceScale(0), { max: 1, step: 1, ticks: [0, 1] });
	assert.deepEqual(niceScale(7), { max: 8, step: 2, ticks: [0, 2, 4, 6, 8] });
	assert.deepEqual(niceScale(480), { max: 600, step: 200, ticks: [0, 200, 400, 600] });
	assert.deepEqual(niceScale(9), { max: 10, step: 5, ticks: [0, 5, 10] }); // counts: no 2.5 step
	assert.deepEqual(niceScale(90), { max: 100, step: 25, ticks: [0, 25, 50, 75, 100] }); // 25 is whole
	assert.deepEqual(niceScale(3), { max: 3, step: 1, ticks: [0, 1, 2, 3] });
	assert.deepEqual(niceScale(Number.NaN), { max: 1, step: 1, ticks: [0, 1] });
	// a fractional floor allows fractional steps (rates)
	assert.deepEqual(niceScale(0.3, 3, 0.1).ticks, [0, 0.1, 0.2, 0.3]);
});

test('time ticks fall on local boundaries of a clean step', () => {
	const start = Date.UTC(2026, 10, 24, 14, 42, 10);
	const end = start + 3_600_000;
	const ticks = timeTicks(start, end, 6, 0);
	assert.deepEqual(ticks.map((t) => new Date(t).toISOString().slice(11, 16)), ['14:50', '15:00', '15:10', '15:20', '15:30', '15:40']);
	// a +05:30 zone aligns to ITS half hours
	const ist = timeTicks(start, start + 6 * 3_600_000, 6, -330);
	for (const t of ist) assert.equal((t + 330 * 60_000) % 3_600_000, 0);
	assert.deepEqual(timeTicks(10, 10, 4), []);
});

test('success rate ignores cancellations and is null when nothing finished', () => {
	assert.equal(successRate(98, 2), 0.98);
	assert.equal(successRate(0, 0), null);
	assert.equal(successRate(0, 3), 0);
	assert.equal(perMinute(120, 3_600_000), 2);
	assert.equal(perMinute(5, 0), 0);
});

test('series helpers skip nulls', () => {
	assert.equal(lastValue([1, 2, null]), 2);
	assert.equal(lastValue([null, null]), null);
	assert.deepEqual(peak([1, 9, null, 9, 3]), { i: 3, v: 9 });
	assert.equal(peak([null]), null);
	assert.equal(total([1, null, 2]), 3);
	assert.equal(stackedMax([[1, 5], [2, null], [0, 1]]), 6);
	assert.equal(stackedMax([]), 0);
});

test('stacked segments leave a 2 px gap between neighbours and keep tiny values visible', () => {
	const y = (v: number) => 100 - v; // 1 px per unit, baseline at 100
	assert.deepEqual(stackSegments([10, 0, 5, null, 3], y), [
		{ top: 90, bottom: 100 },
		null,
		{ top: 85, bottom: 88 },
		null,
		{ top: 82, bottom: 83 },
	]);
	assert.deepEqual(stackSegments([0.2], y), [{ top: 99, bottom: 100 }]); // min 1 px
	assert.deepEqual(stackSegments([], y), []);
});

test('columns: rounded data end, square base, nothing for an empty bar', () => {
	assert.equal(columnPath(0, 10, 50, 100), 'M0 100V54A4 4 0 0 1 4 50H6A4 4 0 0 1 10 54V100Z');
	assert.equal(columnPath(0, 2, 99, 100), 'M0 100V100A1 1 0 0 1 1 99H1A1 1 0 0 1 2 100V100Z'); // radius ≤ half width, ≤ height
	assert.equal(columnPath(0, 10, 50, 100, 0), 'M0 100V50H10V100Z');
	assert.equal(columnPath(0, 10, 100, 100), '');
	assert.equal(columnPath(0, 0, 50, 100), '');
});

test('lines and areas break at null samples', () => {
	assert.equal(linePath([0, 10, 20, 30], [5, 6, null, 8]), 'M0 5L10 6M30 8h0');
	assert.equal(linePath([0, 10], [null, null]), '');
	assert.equal(areaPath([0, 10, 20, 30], [5, 6, null, 8], 100), 'M0 100L0 5L10 6L10 100Z');
});
