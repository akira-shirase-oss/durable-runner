// Polling arithmetic, pure: how long to wait before the next request of one resource. The live controller
// (live.svelte.ts) schedules with setTimeout after each answer, so one resource never has two requests in flight.

/** Every page refreshes its data this often while the tab is visible. */
export const POLL_MS = 5_000;
/** Errors double the wait up to this cap. */
export const MAX_BACKOFF_MS = 60_000;

/** The wait after `errors` consecutive failures: base, 2×, 4×, … capped. Never below base, never NaN. */
export function backoffDelay(base: number, errors: number, cap = MAX_BACKOFF_MS): number {
	const n = Number.isFinite(errors) && errors > 0 ? Math.min(Math.floor(errors), 16) : 0;
	return Math.min(Math.max(base, cap), base * 2 ** n);
}

/** A 429's Retry-After in ms from `now`: delta-seconds ("7") or an HTTP date. Invalid → null, past → 0. */
export function parseRetryAfter(value: string | null | undefined, now: number): number | null {
	if (value === null || value === undefined) return null;
	const v = value.trim();
	if (v === '') return null;
	if (/^\d+$/.test(v)) {
		const s = Number(v);
		return Number.isSafeInteger(s) ? Math.min(s, 86_400) * 1000 : null;
	}
	// an HTTP date always names its month ("Tue, 24 Nov 2026 15:00:30 GMT"); Date.parse alone accepts "-5"
	if (!/[a-z]{3}/i.test(v)) return null;
	const at = Date.parse(v);
	if (Number.isNaN(at)) return null;
	return Math.max(0, at - now);
}

/** The next poll: the backoff for the current error streak, but never sooner than the server asked. */
export function nextDelay(base: number, errors: number, retryAfterMs: number | null): number {
	return Math.max(backoffDelay(base, errors), retryAfterMs ?? 0);
}
