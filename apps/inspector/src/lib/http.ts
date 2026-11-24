// How a signed-in request is shaped, pure. The session cookie (`__Host-drs`, HttpOnly) rides along by itself on
// these same-origin calls; what the page adds is:
//   - `x-durable-csrf: <token>` on every mutation (anything but GET/HEAD): the synchroniser token from
//     /auth/session or /auth/login (D-061). Reads never carry it.
//   - `x-durable-workspace: <public_id>` on the flat routes only (/jobs, /metrics, /events, /handlers, …), which
//     take the workspace from it (D-062). `/workspaces/{pid}/…` routes take the workspace from the PATH and get
//     no header at all, so a stale selection can never disagree with the path (review #1: a mismatch is 400).
//   - never an Authorization header: a bearer always wins over the cookie on the server, so sending one would
//     silently change who is asking.

export interface SessionCred {
	kind: 'session';
	csrf: string;
	/** the selected workspace's public_id; null before one exists */
	ws: string | null;
}

/** Routes that take the workspace from `x-durable-workspace` for a session principal. */
const FLAT = ['/jobs', '/metrics', '/events', '/handlers', '/handler-secret', '/diagnostics', '/whoami'];

export function isFlatWorkspaceRoute(path: string): boolean {
	const p = path.split('?')[0] ?? '';
	return FLAT.some((f) => p === f || p.startsWith(`${f}/`));
}

export function isMutation(method: string | undefined): boolean {
	const m = (method ?? 'GET').toUpperCase();
	return m !== 'GET' && m !== 'HEAD';
}

/** The headers a signed-in request adds (the caller's own content-type etc. are merged on top). */
export function sessionHeaders(method: string | undefined, path: string, cred: SessionCred): Record<string, string> {
	const h: Record<string, string> = {};
	if (isMutation(method) && cred.csrf) h['x-durable-csrf'] = cred.csrf;
	if (cred.ws && isFlatWorkspaceRoute(path)) h['x-durable-workspace'] = cred.ws;
	return h;
}

/** The parts of an error envelope (`{"error", "code", …}`) the pages read. `extra` keeps the plan_limit fields
 *  (`limit`, `plan`, `used`, `cap`) and anything else the server adds. */
export interface Envelope {
	message: string | null;
	code: string | null;
	extra: Record<string, unknown>;
}

export function parseEnvelope(body: unknown): Envelope {
	if (typeof body !== 'object' || body === null || Array.isArray(body)) return { message: null, code: null, extra: {} };
	const b = body as Record<string, unknown>;
	const extra: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(b)) if (k !== 'error' && k !== 'code') extra[k] = v;
	return {
		message: typeof b.error === 'string' && b.error ? b.error : null,
		code: typeof b.code === 'string' && b.code ? b.code : null,
		extra,
	};
}

/** The dashboard's sentence for a refusal: what the code means for the person looking at the page. The server's
 *  own message is the fallback, so a code this build does not know still reads as the server wrote it. */
const COPY: Record<string, string> = {
	unauthenticated: 'Your session has ended. Sign in again.',
	invalid_credentials: 'That email and password do not match an account.',
	email_unverified: 'Verify your email address first: this needs a verified account.',
	csrf_failed: 'The request was refused as cross-site. Reload the page and try again.',
	forbidden: 'Your role in this workspace cannot do this.',
	owner_required: 'Only the workspace owner can do this.',
	last_owner: 'A workspace keeps its owner: the owner cannot be removed or demoted.',
	invite_pending: 'An invite to that address is already pending. Revoke it first to send a new one.',
	invite_email_mismatch: 'This invite was sent to a different address than the one you are signed in with.',
	invite_required: 'Sign-up is invite-only right now. Use the link in your invite.',
	token_gone: 'This link is no longer valid. Request a fresh one.',
	invalid_or_expired_token: 'This link is no longer valid. Request a fresh one.',
	name_taken: 'That name is already used in this workspace.',
	workspace_closed: 'This workspace has been deleted.',
	workspace_required: 'Pick a workspace first.',
	workspace_mismatch: 'The workspace in the request did not match the page. Reload and try again.',
	unregistered_handler: 'A step names a handler that is not registered in this workspace.',
	registry_full: 'The handler registry is full for this workspace.',
	accounts_unavailable: 'The account store is not answering right now. Retry shortly.',
	busy: 'The server is busy. Retry shortly.',
	rate_limited: 'Too many attempts. Retry shortly.',
	not_found: 'Not found. It may have been removed.',
};

export function copyFor(code: string | null, fallback: string): string {
	return (code && COPY[code]) || fallback;
}

/** plan_limit copy, one sentence per limit, with the numbers the 429 carried. */
const LIMIT_NAMES: Record<string, [one: string, many: string, fix: string]> = {
	api_keys: ['API key', 'API keys', 'Revoke one you no longer need'],
	members: ['member', 'members', 'Remove a member or revoke a pending invite'],
	handlers: ['handler', 'handlers', 'Remove one you no longer use'],
	workflows: ['workflow', 'workflows', 'Archive one you no longer run'],
	workspaces_per_account: ['workspace per account', 'workspaces per account', 'Delete one you no longer need'],
	jobs_per_min: ['job per minute', 'jobs per minute', 'Wait for the minute to roll over'],
	test_calls_per_hour: ['test call per hour', 'test calls per hour', 'Wait for the hour to roll over'],
	live_runs: ['unfinished run', 'unfinished runs', 'Let some finish or cancel them'],
	max_input_bytes: ['byte of input per job', 'bytes of input per job', 'Send a smaller input'],
	retained_bytes: ['byte of retained run data', 'bytes of retained run data', 'Let finished runs expire'],
};

export function planLimitCopy(extra: Record<string, unknown>, retryAfterMs: number | null): string {
	const limit = typeof extra.limit === 'string' ? extra.limit : null;
	const cap = typeof extra.cap === 'number' ? extra.cap : null;
	const plan = typeof extra.plan === 'string' && extra.plan ? extra.plan : 'free';
	const planName = plan.charAt(0).toUpperCase() + plan.slice(1);
	const names = limit ? LIMIT_NAMES[limit] : undefined;
	const allows =
		cap === null || !names
			? `The ${planName} plan's ${limit ? `${limit.replaceAll('_', ' ')} ` : ''}limit is reached`
			: `The ${planName} plan allows ${cap.toLocaleString('en-US')} ${cap === 1 ? names[0] : names[1]}`;
	const fix = names?.[2] ?? 'Free up room';
	const retry = retryAfterMs !== null && retryAfterMs > 0 ? ` Retry in ${Math.ceil(retryAfterMs / 1000)} s.` : '';
	return `${allows}. ${fix}, or wait for a paid plan: every limit is already in the plan table.${retry}`;
}
