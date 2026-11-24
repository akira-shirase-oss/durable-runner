// The API base is the server origin minus the UI mount: the UI is served same-origin by durable-server, so plain
// relative calls work. Two ways to authenticate, one transport:
//   - compat (no account store): a pasted key as `Authorization: Bearer`, exactly as before (D-053);
//   - signed in: the session cookie the browser sends by itself, plus the headers http.ts shapes (CSRF on
//     mutations, the workspace on flat routes). No Authorization header is ever sent then.
import { parseRetryAfter } from './poll.ts';
import { parseEnvelope, sessionHeaders, type SessionCred } from './http.ts';

export type { SessionCred } from './http.ts';

export interface RunItem {
	id: string;
	key: string;
	state: string;
	steps: number;
	finished_at: number | null;
}

export interface RunPage {
	runs: RunItem[];
	/** Cursor for the next page; which key exists depends on `order`. */
	next_before?: string | null;
	next_after?: string | null;
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

/** GET /diagnostics: the caller's own workspace's shape. */
export interface WorkspaceDiag {
	live_runs: number;
	active_runs: number;
	states: Record<string, number>;
}

export interface Spec {
	version: number;
	steps: { name: string; handler: string }[];
	input_b64: string;
}

const API = '';

/** A key (compat mode; '' for none) or the signed-in session. */
export type Cred = string | SessionCred;

/** A refused or failed call. `code` is the server's stable error code (`{"error", "code"}`), or null when the
 *  answer carried none — a 404 without a code means this server does not serve the route at all (an older
 *  durable-server), which `unsupported` names. `retryAfterMs` comes from a 429's Retry-After; `extra` keeps the
 *  envelope's other fields (a plan_limit's `limit`, `plan`, `used`, `cap`). */
export class ApiError extends Error {
	readonly status: number;
	readonly code: string | null;
	readonly retryAfterMs: number | null;
	readonly extra: Record<string, unknown>;
	constructor(message: string, status: number, code: string | null = null, retryAfterMs: number | null = null, extra: Record<string, unknown> = {}) {
		super(message);
		this.name = 'ApiError';
		this.status = status;
		this.code = code;
		this.retryAfterMs = retryAfterMs;
		this.extra = extra;
	}
	/** The route is not served here: an older server answers 404 with no error envelope. */
	get unsupported(): boolean {
		return (this.status === 404 || this.status === 405) && this.code === null;
	}
	/** A key is missing or too weak for this call (or, signed in, the role is): polling waits instead of retrying. */
	get needsKey(): boolean {
		return this.status === 401 || this.status === 403;
	}
}

/** Called when a signed-in request answers 401: the account store re-checks the session (it may have ended). */
let sessionLost: (() => void) | null = null;
export function onSessionLost(fn: (() => void) | null): void {
	sessionLost = fn;
}

async function req<T>(path: string, cred: Cred, init: RequestInit = {}, opts: { quiet401?: boolean } = {}): Promise<T> {
	const signedIn = typeof cred !== 'string';
	const auth: Record<string, string> = signedIn ? sessionHeaders(init.method, path, cred) : cred ? { authorization: `Bearer ${cred}` } : {};
	let r: Response;
	try {
		r = await fetch(`${API}${path}`, {
			...init,
			credentials: 'same-origin',
			headers: { ...auth, ...((init.headers as Record<string, string> | undefined) ?? {}) },
		});
	} catch (e) {
		if (e instanceof DOMException && e.name === 'AbortError') throw e;
		throw new ApiError('the server did not answer (network error)', 0);
	}
	// a refusal's body is never needed with a key, but it is always drained: an unread body keeps the request
	// open (and its connection busy) until the response is garbage-collected
	if (!signedIn && (r.status === 401 || r.status === 403)) {
		await r.text().catch(() => '');
		if (r.status === 401) throw new ApiError('needs a key: the read key for reads, the API key for everything else', 401, 'unauthorized');
		throw new ApiError('this key may only read; this operation needs the API key', 403, 'forbidden');
	}
	if (!r.ok) {
		let body: unknown = null;
		try {
			body = await r.json();
		} catch {
			/* not JSON: a route this server does not have, or a proxy's page */
		}
		const env = parseEnvelope(body);
		// 429 rate_limited carries Retry-After; a 503 (busy, at_capacity) may: either way polling waits that long
		const retryAfterMs = r.status === 429 || r.status === 503 ? parseRetryAfter(r.headers.get('retry-after'), Date.now()) : null;
		if (signedIn && r.status === 401 && !opts.quiet401) sessionLost?.();
		throw new ApiError(env.message ?? `${r.status}`, r.status, env.code, retryAfterMs, env.extra);
	}
	if (r.status === 204) return undefined as T;
	const text = await r.text();
	if (text === '') return undefined as T;
	try {
		return JSON.parse(text) as T;
	} catch {
		throw new ApiError('the server answered something that is not JSON', r.status);
	}
}

function jsonInit(method: string, body?: unknown, headers: Record<string, string> = {}): RequestInit {
	return body === undefined
		? { method, headers }
		: { method, headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) };
}

/** No session yet: the open /auth routes (Origin-checked by the server, JSON only). */
const OPEN: SessionCred = { kind: 'session', csrf: '', ws: null };

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
	key: Cred,
	order: 'asc' | 'desc',
	cursor: string | null = null,
	limit = 50,
	signal?: AbortSignal,
): Promise<RunPage> {
	const q = new URLSearchParams({ order, limit: String(limit) });
	if (cursor !== null) q.set(order === 'desc' ? 'before' : 'after', String(cursor));
	return req(`/jobs?${q}`, key, { signal });
}

export async function inspect(key: Cred, id: string, signal?: AbortSignal): Promise<RunItem> {
	return req(`/jobs/${id}`, key, { signal });
}

// The spec is API-key only (D-051): the input may be customer content.
export async function spec(apiKey: Cred, id: string): Promise<{ spec: Spec }> {
	return req(`/jobs/${id}/spec`, apiKey);
}

// --- mutations (API key) -----------------------------------------------------
export async function submitJob(
	cred: Cred,
	body: { op_key: string; spec: { version: number; steps: { name: string; handler: string }[]; input_b64: string }; not_before?: number },
): Promise<{ run_id: string }> {
	return req('/jobs', cred, jsonInit('POST', body));
}

export async function cancelRun(id: string, apiKey: Cred): Promise<void> {
	await req(`/jobs/${id}/cancel`, apiKey, { method: 'POST' });
}

export async function diagnostics(apiKey: string, signal?: AbortSignal): Promise<Diag> {
	return req('/admin/diagnostics', apiKey, { signal });
}

export async function compact(apiKey: string): Promise<{ generation: number }> {
	return req('/admin/compact', apiKey, { method: 'POST' });
}

export async function backup(apiKey: string, name: string): Promise<{ backup: string }> {
	return req('/admin/backup', apiKey, jsonInit('POST', { name }));
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
	/** when this handler's counting began (later than the window's start if its stats were dropped and restarted) */
	since?: number;
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
	/** the run's opaque public id (S2): never the internal number */
	run: string;
	/** null when the run has left the index (expired) */
	key: string | null;
	state: string;
	/** false when compaction, eviction or the per-run cap shortened the timeline; `reason` says which. */
	complete: boolean;
	reason: 'compacted' | 'evicted' | 'truncated' | string | null;
	events: RunEvent[];
}

export interface FeedEvent extends RunEvent {
	/** the run's opaque public id (S2) */
	run: string;
	/** null when the run has left the index (expired) */
	key: string | null;
}

export interface Feed {
	/** the server's start; replayed events may be older */
	since: number;
	events: FeedEvent[];
}

export async function metrics(key: Cred, window: Window, signal?: AbortSignal): Promise<Metrics> {
	return req(`/metrics?window=${window}`, key, { signal });
}

export async function runEvents(key: Cred, id: string, signal?: AbortSignal): Promise<RunTimeline> {
	return req(`/jobs/${id}/events`, key, { signal });
}

export async function events(key: Cred, limit = 50, signal?: AbortSignal): Promise<Feed> {
	return req(`/events?limit=${limit}`, key, { signal });
}

// --- signed in (S3): accounts, workspaces, team, keys, handlers, usage, workflows -----------------------------
// Shapes follow docs/S3-PLAN.md "API surface (new)" and, where they differ, the server's own handlers (the
// session routes of S3-0..S3-2). saas.ts reads every answer tolerantly; these only send.

/** GET /auth/session decides the mode, so its status is the answer, not an error: 200 with the bootstrap,
 *  401 signed out, 404 no account store (compat). Anything else is returned too; the caller decides. */
export async function probeSession(signal?: AbortSignal): Promise<{ status: number; body: unknown; code: string | null }> {
	let r: Response;
	try {
		r = await fetch(`${API}/auth/session`, { credentials: 'same-origin', headers: { accept: 'application/json' }, signal });
	} catch (e) {
		if (e instanceof DOMException && e.name === 'AbortError') throw e;
		throw new ApiError('the server did not answer (network error)', 0);
	}
	let body: unknown = null;
	try {
		body = await r.json();
	} catch {
		/* a 404 from a server without the route has no body */
	}
	return { status: r.status, body, code: parseEnvelope(body).code };
}

export async function signup(email: string, password: string, inviteToken: string | null): Promise<{ status: string }> {
	return req('/auth/signup', OPEN, jsonInit('POST', { email, password, ...(inviteToken ? { invite_token: inviteToken } : {}) }), { quiet401: true });
}

export async function login(email: string, password: string): Promise<{ account: unknown; csrf: string }> {
	return req('/auth/login', OPEN, jsonInit('POST', { email, password }), { quiet401: true });
}

export async function logout(cred: SessionCred): Promise<void> {
	await req('/auth/logout', cred, { method: 'POST' }, { quiet401: true });
}

export async function verifyEmail(token: string): Promise<{ verified: boolean }> {
	return req('/auth/verify', OPEN, jsonInit('POST', { token }), { quiet401: true });
}

export async function resendVerification(email: string): Promise<{ status: string }> {
	return req('/auth/resend-verification', OPEN, jsonInit('POST', { email }), { quiet401: true });
}

export async function forgotPassword(email: string): Promise<{ status: string }> {
	return req('/auth/forgot', OPEN, jsonInit('POST', { email }), { quiet401: true });
}

export async function resetPassword(token: string, password: string): Promise<{ reset?: boolean }> {
	return req('/auth/reset', OPEN, jsonInit('POST', { token, password }), { quiet401: true });
}

// --- me ---
export async function meSessions(cred: SessionCred, signal?: AbortSignal): Promise<unknown> {
	return req('/me/sessions', cred, { signal });
}

export async function revokeSession(cred: SessionCred, id: string): Promise<void> {
	await req(`/me/sessions/${encodeURIComponent(id)}`, cred, { method: 'DELETE' });
}

/** Sign out everywhere else. The server's route is `DELETE /me/sessions` (the plan said POST …/revoke-all). */
export async function revokeOtherSessions(cred: SessionCred): Promise<{ revoked?: number }> {
	return req('/me/sessions', cred, { method: 'DELETE' });
}

/** Revokes every session and answers a fresh one (cookie + its csrf). Field names are the server's. */
export async function changePassword(cred: SessionCred, current: string, next: string): Promise<{ changed?: boolean; csrf?: string }> {
	return req('/me/password', cred, jsonInit('POST', { current_password: current, new_password: next }));
}

// --- workspaces + team ---
const ws = (pid: string, rest = '') => `/workspaces/${encodeURIComponent(pid)}${rest}`;

export async function createWorkspace(cred: SessionCred, name: string): Promise<{ public_id: string; name: string; handler_secret?: string }> {
	return req('/workspaces', cred, jsonInit('POST', { name }));
}

export async function getWorkspace(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid), cred, { signal });
}

export async function renameWorkspace(cred: SessionCred, pid: string, name: string): Promise<unknown> {
	return req(ws(pid), cred, jsonInit('PATCH', { name }));
}

export async function deleteWorkspace(cred: SessionCred, pid: string, confirm: string): Promise<void> {
	await req(ws(pid), cred, jsonInit('DELETE', { confirm }));
}

export async function listMembers(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/members'), cred, { signal });
}

export async function inviteMember(cred: SessionCred, pid: string, email: string, role: string): Promise<{ invite?: unknown; invite_url?: string }> {
	return req(ws(pid, '/members'), cred, jsonInit('POST', { email, role }));
}

export async function changeRole(cred: SessionCred, pid: string, accountId: string, role: string): Promise<unknown> {
	return req(ws(pid, `/members/${encodeURIComponent(accountId)}`), cred, jsonInit('PATCH', { role }));
}

export async function removeMember(cred: SessionCred, pid: string, accountId: string): Promise<void> {
	await req(ws(pid, `/members/${encodeURIComponent(accountId)}`), cred, { method: 'DELETE' });
}

export async function listInvites(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/invites'), cred, { signal });
}

export async function revokeInvite(cred: SessionCred, pid: string, id: string): Promise<void> {
	await req(ws(pid, `/invites/${encodeURIComponent(id)}`), cred, { method: 'DELETE' });
}

export async function acceptInvite(cred: SessionCred, token: string): Promise<{ workspace?: unknown }> {
	return req('/invites/accept', cred, jsonInit('POST', { token }));
}

export async function declineInvite(cred: SessionCred, token: string): Promise<void> {
	await req('/invites/decline', cred, jsonInit('POST', { token }));
}

// --- keys ---
export async function listKeys(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/keys'), cred, { signal });
}

export async function createKey(cred: SessionCred, pid: string, name: string, scope: 'read' | 'write'): Promise<{ key: string }> {
	return req(ws(pid, '/keys'), cred, jsonInit('POST', { name, scope }));
}

export async function revokeKey(cred: SessionCred, pid: string, id: string): Promise<void> {
	await req(ws(pid, `/keys/${encodeURIComponent(id)}`), cred, { method: 'DELETE' });
}

export async function rotateKey(cred: SessionCred, pid: string, id: string): Promise<{ key: string }> {
	return req(ws(pid, `/keys/${encodeURIComponent(id)}/rotate`), cred, { method: 'POST' });
}

// --- handlers (flat routes: the workspace rides x-durable-workspace) ---
export async function listHandlers(cred: Cred, signal?: AbortSignal): Promise<unknown> {
	return req('/handlers', cred, { signal });
}

export async function putHandler(cred: Cred, name: string, url: string): Promise<unknown> {
	return req(`/handlers/${encodeURIComponent(name)}`, cred, jsonInit('PUT', { url }));
}

export async function deleteHandler(cred: Cred, name: string): Promise<void> {
	await req(`/handlers/${encodeURIComponent(name)}`, cred, { method: 'DELETE' });
}

export async function testHandler(cred: Cred, name: string, inputB64: string): Promise<unknown> {
	return req(`/handlers/${encodeURIComponent(name)}/test`, cred, jsonInit('POST', inputB64 ? { input_b64: inputB64 } : {}));
}

export async function rotateHandlerSecret(cred: Cred): Promise<{ secret: string }> {
	return req('/handler-secret', cred, { method: 'POST' });
}

export async function workspaceDiagnostics(cred: Cred, signal?: AbortSignal): Promise<WorkspaceDiag> {
	return req('/diagnostics', cred, { signal });
}

// --- usage, onboarding ---
export async function usage(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/usage'), cred, { signal });
}

export async function onboarding(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/onboarding'), cred, { signal });
}

// --- workflows ---
export interface WorkflowSpecWire {
	version: number;
	steps: { name: string; handler: string }[];
	input_b64: string;
}

export async function listWorkflows(cred: SessionCred, pid: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, '/workflows'), cred, { signal });
}

export async function createWorkflow(cred: SessionCred, pid: string, name: string, spec: WorkflowSpecWire): Promise<unknown> {
	return req(ws(pid, '/workflows'), cred, jsonInit('POST', { name, spec }));
}

export async function getWorkflow(cred: SessionCred, pid: string, id: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, `/workflows/${encodeURIComponent(id)}`), cred, { signal });
}

export async function updateWorkflow(cred: SessionCred, pid: string, id: string, patch: { name?: string; spec?: WorkflowSpecWire }): Promise<unknown> {
	return req(ws(pid, `/workflows/${encodeURIComponent(id)}`), cred, jsonInit('PATCH', patch));
}

export async function archiveWorkflow(cred: SessionCred, pid: string, id: string): Promise<void> {
	await req(ws(pid, `/workflows/${encodeURIComponent(id)}`), cred, { method: 'DELETE' });
}

/** Run now. `idempotencyKey` is minted once per click, so a retried request is the same run, never a second one. */
export async function runWorkflow(cred: SessionCred, pid: string, id: string, inputB64: string | null, idempotencyKey: string): Promise<{ run_id: string; version?: number }> {
	return req(
		ws(pid, `/workflows/${encodeURIComponent(id)}/run`),
		cred,
		jsonInit('POST', inputB64 === null ? {} : { input_b64: inputB64 }, { 'idempotency-key': idempotencyKey }),
	);
}

export async function workflowRuns(cred: SessionCred, pid: string, id: string, signal?: AbortSignal): Promise<unknown> {
	return req(ws(pid, `/workflows/${encodeURIComponent(id)}/runs`), cred, { signal });
}
