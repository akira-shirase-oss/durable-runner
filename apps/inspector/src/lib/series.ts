// Time-series maths for the charts, pure. The server sends every bucket of the window (360 or 1,440 of them);
// a chart shows at most one bin per few pixels, so consecutive buckets merge into display bins here.
import type { MetricCounters, MetricPoint, Window } from './api.ts';

export const WINDOWS: readonly Window[] = ['15m', '1h', '6h', '24h'];

export const WINDOW_LABEL: Record<Window, string> = {
	'15m': 'last 15 minutes',
	'1h': 'last hour',
	'6h': 'last 6 hours',
	'24h': 'last 24 hours',
};

export function parseWindow(s: string | null | undefined): Window | null {
	return (WINDOWS as readonly string[]).includes(s ?? '') ? (s as Window) : null;
}

const COUNTERS: (keyof MetricCounters)[] = [
	'accepted',
	'deduplicated',
	'refused',
	'completed',
	'failed',
	'cancelled',
	'dispatched',
	'steps_ok',
	'steps_failed',
	'replies_lost',
	'reconciled',
];

/** One display bin: `factor` consecutive server buckets. Counters are summed; gauges keep the bin's last sample;
 *  latency keeps the WORST bucket's percentiles (percentiles of merged buckets cannot be recovered exactly, and the
 *  worst one never hides a slow spell). */
export interface Bin extends MetricCounters {
	t: number;
	end: number;
	buckets: number;
	/** holds the bucket in progress (it contains the server's `now`): its counters are partial */
	partial: boolean;
	/** ends at or before `since` (the server's start): empty by construction, not idle */
	before: boolean;
	live_runs: number | null;
	active_runs: number | null;
	in_flight: number | null;
	p50: number | null;
	p95: number | null;
	max: number | null;
}

/** How many server buckets go into one display bin so that at most `maxBins` remain. */
export function mergeFactor(n: number, maxBins: number): number {
	if (!(n > 0) || !(maxBins >= 1)) return 1;
	return Math.max(1, Math.ceil(n / Math.floor(maxBins)));
}

/** Merge buckets into bins of `factor`. Groups are cut from the NEWEST end, so the latest bin is always whole and
 *  only the oldest one may hold fewer buckets. The server's last bucket is the one in progress (it holds `now`);
 *  the bin holding it is `partial`. Bins ending by `since` (the server's start) are `before`. */
export function mergePoints(
	points: readonly MetricPoint[],
	bucketMs: number,
	factor: number,
	now: number = Infinity,
	since: number = -Infinity,
): Bin[] {
	const f = Math.max(1, Math.floor(factor));
	const out: Bin[] = [];
	const n = points.length;
	const first = n % f === 0 ? f : n % f;
	for (let start = 0, size = first; start < n; start += size, size = f) {
		const group = points.slice(start, start + size);
		const head = group[0]!;
		const last = group[group.length - 1]!;
		const bin: Bin = {
			t: head.t,
			end: last.t + bucketMs,
			buckets: group.length,
			partial: last.t + bucketMs > now,
			before: last.t + bucketMs <= since,
			accepted: 0,
			deduplicated: 0,
			refused: 0,
			completed: 0,
			failed: 0,
			cancelled: 0,
			dispatched: 0,
			steps_ok: 0,
			steps_failed: 0,
			replies_lost: 0,
			reconciled: 0,
			live_runs: null,
			active_runs: null,
			in_flight: null,
			p50: null,
			p95: null,
			max: null,
		};
		for (const p of group) {
			for (const k of COUNTERS) bin[k] += num(p[k]);
			if (p.live_runs !== null && p.live_runs !== undefined) bin.live_runs = p.live_runs;
			if (p.active_runs !== null && p.active_runs !== undefined) bin.active_runs = p.active_runs;
			if (p.in_flight !== null && p.in_flight !== undefined) bin.in_flight = p.in_flight;
			bin.p50 = worst(bin.p50, p.step_ms?.p50);
			bin.p95 = worst(bin.p95, p.step_ms?.p95);
			bin.max = worst(bin.max, p.step_ms?.max);
		}
		out.push(bin);
	}
	return out;
}

/** A y-axis from 0 to a clean maximum at or above `max`: steps of 1, 2, 2.5 or 5 × 10ⁿ. A zero or empty series
 *  still gets an axis (0 … `floor`), so an idle chart shows a baseline, not a collapsed plot. */
export function niceScale(max: number, ticks = 4, floor = 1): { max: number; step: number; ticks: number[] } {
	const target = Math.max(Number.isFinite(max) ? max : 0, floor, Number.MIN_VALUE);
	const rough = target / Math.max(1, ticks);
	const mag = 10 ** Math.floor(Math.log10(rough));
	const norm = rough / mag;
	const mult = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10;
	let step = mult * mag;
	// whole-number data (counts) never gets a fractional step: 2.5 becomes 5, anything under 1 becomes 1
	if (floor >= 1 && !Number.isInteger(step)) step = step < 1 ? 1 : 5 * mag;
	const top = Math.ceil(target / step - 1e-9) * step;
	const out: number[] = [];
	for (let v = 0; v <= top + step / 2; v += step) out.push(round(v));
	return { max: round(top), step: round(step), ticks: out };
}

const MIN = 60_000;
const HOUR = 60 * MIN;
const TICK_STEPS = [10_000, 15_000, 30_000, MIN, 2 * MIN, 5 * MIN, 10 * MIN, 15 * MIN, 30 * MIN, HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR];

/** Clock ticks inside [start, end]: the smallest step from a fixed ladder that yields at most `maxTicks`, each tick
 *  on a local-time boundary of that step (15:30, 15:45, …), `tzOffsetMin` being Date#getTimezoneOffset. */
export function timeTicks(start: number, end: number, maxTicks: number, tzOffsetMin = new Date(start).getTimezoneOffset()): number[] {
	if (!(end > start) || !(maxTicks >= 1)) return [];
	const span = end - start;
	const step = TICK_STEPS.find((s) => span / s <= maxTicks) ?? Math.ceil(span / maxTicks / HOUR) * HOUR;
	const shift = -tzOffsetMin * MIN; // local = utc + shift
	const first = Math.ceil((start + shift) / step) * step - shift;
	const out: number[] = [];
	for (let t = first; t <= end; t += step) out.push(t);
	return out;
}

/** Success over a window: completed ÷ (completed + failed). Cancelled runs were stopped by someone, not failed by
 *  the runner, so they count on neither side. null when nothing finished. */
export function successRate(completed: number, failed: number): number | null {
	const d = completed + failed;
	return d > 0 ? completed / d : null;
}

/** A count over `ms` as a rate per minute. */
export function perMinute(count: number, ms: number): number {
	return ms > 0 ? (count * MIN) / ms : 0;
}

/** The newest non-null value. */
export function lastValue(values: readonly (number | null)[]): number | null {
	for (let i = values.length - 1; i >= 0; i--) {
		const v = values[i];
		if (v !== null && v !== undefined && Number.isFinite(v)) return v;
	}
	return null;
}

/** The largest value and where it is (the latest on a tie); null when every value is null. */
export function peak(values: readonly (number | null)[]): { i: number; v: number } | null {
	let bi = -1;
	let bv = -Infinity;
	for (let i = 0; i < values.length; i++) {
		const v = values[i];
		if (v !== null && v !== undefined && Number.isFinite(v) && v >= bv) {
			bi = i;
			bv = v;
		}
	}
	return bi < 0 ? null : { i: bi, v: bv };
}

export function total(values: readonly (number | null)[]): number {
	let s = 0;
	for (const v of values) if (v !== null && Number.isFinite(v)) s += v;
	return s;
}

/** The largest stack in a set of stacked series, per bin (for a stacked chart's y-axis). */
export function stackedMax(series: readonly (readonly (number | null)[])[]): number {
	const n = Math.max(0, ...series.map((s) => s.length));
	let m = 0;
	for (let i = 0; i < n; i++) {
		let s = 0;
		for (const vs of series) s += num(vs[i]);
		m = Math.max(m, s);
	}
	return m;
}

function num(v: number | null | undefined): number {
	return v !== null && v !== undefined && Number.isFinite(v) ? v : 0;
}

function worst(a: number | null, b: number | null | undefined): number | null {
	if (b === null || b === undefined || !Number.isFinite(b)) return a;
	return a === null ? b : Math.max(a, b);
}

function round(v: number): number {
	return Math.round(v * 1e6) / 1e6;
}

/** One stacked bar's segments in pixels, bottom to top, one per value (null for a zero or missing value). Every
 *  segment resting on another leaves a `gap` px surface gap under it; a non-zero value is never thinner than
 *  `minH` px. `y` maps a value to a pixel (larger values → smaller y). */
export function stackSegments(
	values: readonly (number | null)[],
	y: (v: number) => number,
	gap = 2,
	minH = 1,
): ({ top: number; bottom: number } | null)[] {
	let cum = 0;
	let below = false;
	const out: ({ top: number; bottom: number } | null)[] = [];
	for (const v of values) {
		if (v === null || !(v > 0)) {
			out.push(null);
			continue;
		}
		const base = y(cum);
		cum += v;
		const bottom = below ? base - gap : base;
		let top = y(cum);
		if (bottom - top < minH) top = bottom - minH;
		out.push({ top, bottom });
		below = true;
	}
	return out;
}

/** A path for a column with its data end rounded (radius ≤ r, ≤ half the width, ≤ the height) and its base
 *  square, from `top` down to `bottom` at x … x+w. */
export function columnPath(x: number, w: number, top: number, bottom: number, r = 4): string {
	const h = bottom - top;
	if (!(w > 0) || !(h > 0)) return '';
	const rr = Math.max(0, Math.min(r, w / 2, h));
	const f = (n: number) => Math.round(n * 100) / 100;
	if (rr === 0) return `M${f(x)} ${f(bottom)}V${f(top)}H${f(x + w)}V${f(bottom)}Z`;
	return (
		`M${f(x)} ${f(bottom)}V${f(top + rr)}A${f(rr)} ${f(rr)} 0 0 1 ${f(x + rr)} ${f(top)}` +
		`H${f(x + w - rr)}A${f(rr)} ${f(rr)} 0 0 1 ${f(x + w)} ${f(top + rr)}V${f(bottom)}Z`
	);
}

/** An SVG line through the points, broken where a value is null (a gauge not yet sampled): one "M…L…" run per
 *  unbroken stretch. A lone point becomes a 0-length run (drawn as a dot by a round cap). */
export function linePath(xs: readonly number[], ys: readonly (number | null)[]): string {
	const f = (n: number) => Math.round(n * 100) / 100;
	let d = '';
	let pen = false;
	for (let i = 0; i < xs.length; i++) {
		const y = ys[i];
		if (y === null || y === undefined || !Number.isFinite(y)) {
			pen = false;
			continue;
		}
		d += `${pen ? 'L' : 'M'}${f(xs[i]!)} ${f(y)}`;
		if (!pen && (i + 1 >= xs.length || ys[i + 1] === null || ys[i + 1] === undefined)) d += `h0`;
		pen = true;
	}
	return d;
}

/** The filled wash under a line, down to `base`, one closed shape per unbroken stretch. */
export function areaPath(xs: readonly number[], ys: readonly (number | null)[], base: number): string {
	const f = (n: number) => Math.round(n * 100) / 100;
	let d = '';
	let run: [number, number][] = [];
	const flush = () => {
		if (run.length > 1) {
			d += `M${f(run[0]![0])} ${f(base)}` + run.map(([x, y]) => `L${f(x)} ${f(y)}`).join('') + `L${f(run[run.length - 1]![0])} ${f(base)}Z`;
		}
		run = [];
	};
	for (let i = 0; i < xs.length; i++) {
		const y = ys[i];
		if (y === null || y === undefined || !Number.isFinite(y)) flush();
		else run.push([xs[i]!, y]);
	}
	flush();
	return d;
}

const LADDER_10S = [1, 3, 6, 12, 18, 30, 60, 90, 180, 360];
const LADDER_60S = [1, 2, 3, 5, 10, 15, 20, 30, 60, 120, 240, 360, 720, 1440];

/** The smallest merge factor ≥ `min` that gives a bin a round duration: 10 s buckets merge into 30 s, 1, 2, 3, 5,
 *  10 min…; 60 s buckets into 2, 3, 5, 10, 15 min…. */
export function niceFactor(bucketMs: number, min: number): number {
	const ladder = bucketMs >= 60_000 ? LADDER_60S : LADDER_10S;
	const want = Math.max(1, Math.ceil(min));
	return ladder.find((f) => f >= want) ?? want;
}

/** A bin's duration in words for a chart's subtitle: "10 s", "30 s", "1 min", "12 min", "1 h". */
export function binLabel(ms: number): string {
	if (ms < 60_000) return `${Math.round(ms / 1000)} s`;
	if (ms < 3_600_000) return `${Math.round(ms / 60_000)} min`;
	return `${Math.round((ms / 3_600_000) * 10) / 10} h`;
}
