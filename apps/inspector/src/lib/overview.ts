// The Overview page's numbers, pure: the KPI tiles and the chart series, all from one GET /metrics answer.
import type { Metrics } from './api.ts';
import { binLabel, mergeFactor, mergePoints, niceFactor, perMinute, successRate, type Bin } from './series.ts';

export interface OverviewModel {
	bins: Bin[];
	binMs: number;
	binText: string;
	/** the newest gauge samples (null before the server sampled them) */
	liveRuns: number | null;
	activeRuns: number | null;
	inFlight: number | null;
	completedPerMin: number;
	acceptedPerMin: number;
	success: number | null;
	finished: number;
	p50: number | null;
	p95: number | null;
	max: number | null;
	/** per-bin success rate (null where nothing finished) */
	successSeries: (number | null)[];
}

/** Display bins for a chart about `maxBins` wide, at a round bin duration. */
export function displayBins(
	m: Pick<Metrics, 'points' | 'bucket_ms'> & Partial<Pick<Metrics, 'now' | 'since'>>,
	maxBins: number,
): { bins: Bin[]; binMs: number } {
	const factor = niceFactor(m.bucket_ms, mergeFactor(m.points.length, maxBins));
	return { bins: mergePoints(m.points, m.bucket_ms, factor, m.now, m.since), binMs: m.bucket_ms * factor };
}

/** One series for a chart: a counter or gauge per bin, null for bins before the server started (no data, which
 *  is not the same as zero) and, for gauges, null where the server never sampled. */
export function column(bins: readonly Bin[], key: keyof Bin): (number | null)[] {
	return bins.map((b) => {
		if (b.before) return null;
		const v = b[key];
		return typeof v === 'number' ? v : null;
	});
}

/** The span the window's counters actually cover: from the first bucket (or the server's start, if later) to
 *  the server's `now`, so a rate never reads low because the bucket in progress is partial or the server is new. */
export function coveredMs(m: Pick<Metrics, 'points' | 'now' | 'since' | 'window_ms'>): number {
	const first = m.points[0]?.t ?? m.now - m.window_ms;
	return Math.max(1000, m.now - Math.max(first, m.since ?? first));
}

export function overviewModel(m: Metrics, maxBins: number): OverviewModel {
	const { bins, binMs } = displayBins(m, maxBins);
	const last = <K extends 'live_runs' | 'active_runs' | 'in_flight'>(k: K): number | null => {
		for (let i = m.points.length - 1; i >= 0; i--) {
			const v = m.points[i]![k];
			if (v !== null && v !== undefined) return v;
		}
		return null;
	};
	const t = m.totals;
	return {
		bins,
		binMs,
		binText: binLabel(binMs),
		liveRuns: last('live_runs'),
		activeRuns: last('active_runs'),
		inFlight: last('in_flight'),
		// never extrapolated from less than a minute: a server up for 7 s does not "complete 130 per minute"
		completedPerMin: perMinute(t.completed, Math.max(60_000, coveredMs(m))),
		acceptedPerMin: perMinute(t.accepted, Math.max(60_000, coveredMs(m))),
		success: successRate(t.completed, t.failed),
		finished: t.completed + t.failed + t.cancelled,
		p50: t.step_ms?.p50 ?? null,
		p95: t.step_ms?.p95 ?? null,
		max: t.step_ms?.max ?? null,
		successSeries: bins.map((b) => (b.before ? null : successRate(b.completed, b.failed))),
	};
}

/** The seven run states in lifecycle order, with any unknown state the server adds kept at the end. */
export const STATE_ORDER = ['queued', 'running', 'retry_waiting', 'reconciling', 'done', 'failed', 'cancelled'];
export function orderedStates(states: Record<string, number>): [string, number][] {
	const known = STATE_ORDER.map((k) => [k, states[k] ?? 0] as [string, number]);
	const extra = Object.entries(states).filter(([k]) => !STATE_ORDER.includes(k));
	return [...known, ...extra];
}
