// The API base is the server origin minus the /inspector mount — the UI is
// served same-origin by durable-server, so plain relative calls work.
import { parseRetryAfter } from './poll.ts';

export interface RunItem {
	id: number;
	key: string;
	state: string;
	steps: number;
	finished_at: number | null;
}

export interface RunPage {
	runs: RunItem[];
	/** Cursor for the next page; which key exists depends on `order`. */
	next_before?: number | null;
	next_after?: number | null;
}

export interface Diag {
	generation: number;
	records_written: number;
	live_runs: number;
	active_runs: number;
	index_bytes: number;
	dedup_keys: number;
	states: Record<string, number>;
	space_ok: boolean;
	errors: Record<string, number>;
}

export interface Spec {
	version: number;
	steps: { name: string; handler: string }[];
	input_b64: string;
}

const API = '';

/** A refused or failed call. `code` is the server's stable error code (`{"error", "code"}`), or null when the
 *  answer carried none — a 404 without a code means this server does not serve the route at all (an older
 *  durable-server), which `unsupported` names. `retryAfterMs` comes from a 429's Retry-After. */
export class ApiError extends Error {
	readonly status: number;
	readonly code: string | null;
	readonly retryAfterMs: number | null;
	constructor(message: string, status: number, code: string | null = null, retryAfterMs: number | null = null) {
		super(message);
		this.name = 'ApiError';
		this.status = status;
		this.code = code;
		this.retryAfterMs = retryAfterMs;
	}
	/** The route is not served here: an older server answers 404 with no error envelope. */
	get unsupported(): boolean {
		return (this.status === 404 || this.status === 405) && this.code === null;
	}
	/** A key is missing or too weak for this call: polling waits for a new key instead of retrying. */
	get needsKey(): boolean {
		return this.status === 401 || this.status === 403;
	}
}

async function req<T>(path: string, key: string, init: RequestInit = {}): Promise<T> {
	let r: Response;
	try {
		r = await fetch(`${API}${path}`, {
			...init,
			headers: { ...(key ? { authorization: `Bearer ${key}` } : {}), ...(init.headers ?? {}) },
		});
	} catch (e) {
		if (e instanceof DOMException && e.name === 'AbortError') throw e;
		throw new ApiError('the server did not answer (network error)', 0);
	}
	// a refusal's body is never needed, but it is always drained: an unread body keeps the request open (and its
	// connection busy) until the response is garbage-collected
	if (r.status === 401 || r.status === 403) {
		await r.text().catch(() => '');
		if (r.status === 401) throw new ApiError('needs a key: the read key for reads, the API key for everything else', 401, 'unauthorized');
		throw new ApiError('this key may only read; this operation needs the API key', 403, 'forbidden');
	}
	if (!r.ok) {
		let detail = `${r.status}`;
		let code: string | null = null;
		try {
			const body = (await r.json()) as { error?: string; code?: string };
			if (body.error) detail = body.error;
			if (typeof body.code === 'string') code = body.code;
		} catch {
			/* not JSON: a route this server does not have, or a proxy's page */
		}
		// 429 rate_limited carries Retry-After; a 503 (busy, at_capacity) may: either way polling waits that long
		const retryAfterMs = r.status === 429 || r.status === 503 ? parseRetryAfter(r.headers.get('retry-after'), Date.now()) : null;
		throw new ApiError(detail, r.status, code, retryAfterMs);
	}
	return (await r.json()) as T;
}

// --- keys -------------------------------------------------------------------
// Two slots, both in this tab's sessionStorage only: the read key views,
// the API key submits, cancels, inspects specs and administers (D-037/051).
export type KeyKind = 'read' | 'api';
const SLOTS: Record<KeyKind, string> = { read: 'durable-read-key', api: 'durable-api-key' };

export function loadKey(kind: KeyKind): string {
	try {
		return sessionStorage.getItem(SLOTS[kind]) ?? '';
	} catch {
		return '';
	}
}

export function saveKey(kind: KeyKind, key: string): void {
	try {
		if (key) sessionStorage.setItem(SLOTS[kind], key);
		else sessionStorage.removeItem(SLOTS[kind]);
	} catch {
		// storage unavailable: the key lasts only for this page view
	}
}

export function forgetKeys(): void {
	saveKey('read', '');
	saveKey('api', '');
}

// --- reads ------------------------------------------------------------------
export async function health(signal?: AbortSignal): Promise<{ ok: boolean }> {
	return req('/health', '', { signal });
}

/** /ready answers 200 when ready and 503 with `{"ready": false, "code": <reason>}` while not: both are answers. */
export async function ready(signal?: AbortSignal): Promise<{ ready: boolean; code: string | null }> {
	let r: Response;
	try {
		r = await fetch(`${API}/ready`, { signal });
	} catch (e) {
		if (e instanceof DOMException && e.name === 'AbortError') throw e;
		throw new ApiError('the server did not answer (network error)', 0);
	}
	if (r.status === 200 || r.status === 503) {
		try {
			const b = (await r.json()) as { ready?: boolean; code?: string | null };
			return { ready: b.ready === true, code: typeof b.code === 'string' ? b.code : null };
		} catch {
			/* fall through */
		}
	}
	await r.text().catch(() => '');
	throw new ApiError(`${r.status}`, r.status);
}

export async function listRuns(
	key: string,
	order: 'asc' | 'desc',
	cursor: number | null = null,
	limit = 50,
	signal?: AbortSignal,
): Promise<RunPage> {
	const q = new URLSearchParams({ order, limit: String(limit) });
	if (cursor !== null) q.set(order === 'desc' ? 'before' : 'after', String(cursor));
	return req(`/jobs?${q}`, key, { signal });
}

export async function inspect(key: string, id: number, signal?: AbortSignal): Promise<RunItem> {
	return req(`/jobs/${id}`, key, { signal });
}

// The spec is API-key only (D-051): the input may be customer content.
export async function spec(apiKey: string, id: number): Promise<{ spec: Spec }> {
	return req(`/jobs/${id}/spec`, apiKey);
}

// --- mutations (API key) -----------------------------------------------------
export async function submitJob(
	apiKey: string,
	body: { op_key: string; spec: { version: number; steps: { name: string; handler: string }[]; input_b64: string }; not_before?: number },
): Promise<{ run_id: number }> {
	return req('/jobs', apiKey, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey}` }, body: JSON.stringify(body) });
}

export async function cancelRun(id: number, apiKey: string): Promise<void> {
	await req(`/jobs/${id}/cancel`, apiKey, { method: 'POST' });
}

export async function diagnostics(apiKey: string, signal?: AbortSignal): Promise<Diag> {
	return req('/admin/diagnostics', apiKey, { signal });
}

export async function compact(apiKey: string): Promise<{ generation: number }> {
	return req('/admin/compact', apiKey, { method: 'POST' });
}

export async function backup(apiKey: string, name: string): Promise<{ backup: string }> {
	return req('/admin/backup', apiKey, {
		method: 'POST',
		headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey}` },
		body: JSON.stringify({ name }),
	});
}

// --- observability (D-058): derived, in memory and bounded; the journal stays the authority -------------------
// A server without these routes answers 404 with no error envelope: callers see ApiError.unsupported.
export type Window = '15m' | '1h' | '6h' | '24h';

export interface Percentiles {
	p50: number | null;
	p95: number | null;
	max: number | null;
}

export interface MetricCounters {
	accepted: number;
	deduplicated: number;
	refused: number;
	completed: number;
	failed: number;
	cancelled: number;
	dispatched: number;
	steps_ok: number;
	steps_failed: number;
	replies_lost: number;
	reconciled: number;
}

export interface MetricPoint extends MetricCounters {
	/** The bucket's start (ms). */
	t: number;
	live_runs: number | null;
	active_runs: number | null;
	in_flight: number | null;
	step_ms: Percentiles;
}

export interface HandlerStats {
	name: string;
	calls: number;
	ok: number;
	failed: number;
	replies_lost: number;
	step_ms: Percentiles;
	last_ok_at: number | null;
	last_failure_at: number | null;
}

export interface Metrics {
	now: number;
	since: number;
	window_ms: number;
	bucket_ms: number;
	points: MetricPoint[];
	totals: MetricCounters & { step_ms: Percentiles };
	states: Record<string, number>;
	handlers: HandlerStats[];
}

export type EventKind =
	| 'accepted'
	| 'dispatched'
	| 'checkpointed'
	| 'step_failed'
	| 'reply_lost'
	| 'reconciled'
	| 'cancel_requested'
	| 'finished';

export interface RunEvent {
	at: number;
	kind: EventKind | string;
	step?: number;
	attempt?: number;
	deadline?: number;
	not_before?: number;
	effect_found?: boolean;
	outcome?: string;
}

export interface RunTimeline {
	run: number;
	/** null when the run has left the index (expired) */
	key: string | null;
	state: string;
	/** false when compaction, eviction or the per-run cap shortened the timeline; `reason` says which. */
	complete: boolean;
	reason: 'compacted' | 'evicted' | 'truncated' | string | null;
	events: RunEvent[];
}

export interface FeedEvent extends RunEvent {
	run: number;
	/** null when the run has left the index (expired) */
	key: string | null;
}

export interface Feed {
	/** the server's start; replayed events may be older */
	since: number;
	events: FeedEvent[];
}

export async function metrics(key: string, window: Window, signal?: AbortSignal): Promise<Metrics> {
	return req(`/metrics?window=${window}`, key, { signal });
}

export async function runEvents(key: string, id: number, signal?: AbortSignal): Promise<RunTimeline> {
	return req(`/jobs/${id}/events`, key, { signal });
}

export async function events(key: string, limit = 50, signal?: AbortSignal): Promise<Feed> {
	return req(`/events?limit=${limit}`, key, { signal });
}
