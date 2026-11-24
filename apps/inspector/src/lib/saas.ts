// The signed-in dashboard's data, pure: roles and what each may do (the plan's authorization matrix, used only
// to hide controls — the server enforces it), tolerant readers for the account/workspace answers, the usage
// meters and the onboarding checklist. The readers accept both a bare list and `{"<name>": [...]}`, `id` or
// `public_id`, `*_ms` numbers or ISO strings: the routes land slice by slice, and a reader that trusted one exact
// shape would turn a harmless difference into an empty page.

export type Role = 'owner' | 'admin' | 'member' | 'viewer';
export const ROLES: readonly Role[] = ['owner', 'admin', 'member', 'viewer'];
const RANK: Record<Role, number> = { viewer: 0, member: 1, admin: 2, owner: 3 };

export function asRole(v: unknown): Role | null {
	return typeof v === 'string' && (ROLES as readonly string[]).includes(v) ? (v as Role) : null;
}

/** What a role may do, per the matrix: `write` = submit/cancel/spec, handlers, test calls, workflows (member+);
 *  `admin` = rename, team, keys, the signing secret (admin+); `owner` = delete the workspace. */
export type Action = 'read' | 'write' | 'admin' | 'owner';
const NEED: Record<Action, number> = { read: 0, write: 1, admin: 2, owner: 3 };

export function can(role: Role | null | undefined, action: Action): boolean {
	if (!role) return false;
	return RANK[role] >= NEED[action];
}

export const ROLE_LABEL: Record<Role, string> = { owner: 'Owner', admin: 'Admin', member: 'Member', viewer: 'Viewer' };
export const ROLE_HINT: Record<Role, string> = {
	owner: 'Everything, including deleting the workspace',
	admin: 'Team, API keys, the signing secret and settings',
	member: 'Runs, workflows and handlers',
	viewer: 'Reads everything, changes nothing',
};

// --- tolerant readers ---------------------------------------------------------------------------------------

type Obj = Record<string, unknown>;
function obj(v: unknown): Obj {
	return typeof v === 'object' && v !== null && !Array.isArray(v) ? (v as Obj) : {};
}
function str(v: unknown): string | null {
	if (typeof v === 'string' && v !== '') return v;
	if (typeof v === 'number' && Number.isFinite(v)) return String(v);
	return null;
}

/** A list answer: the array itself, or the array under `key`. Anything else is empty. */
export function listOf(body: unknown, key: string): unknown[] {
	if (Array.isArray(body)) return body;
	const v = obj(body)[key];
	return Array.isArray(v) ? v : [];
}

/** A timestamp as ms since the epoch: a number is taken as ms; a string as an ISO date or a number. */
export function toMs(v: unknown): number | null {
	if (typeof v === 'number') return Number.isFinite(v) ? v : null;
	if (typeof v === 'string' && v !== '') {
		if (/^\d+$/.test(v)) return Number(v);
		const t = Date.parse(v);
		return Number.isNaN(t) ? null : t;
	}
	return null;
}

function first(o: Obj, ...keys: string[]): unknown {
	for (const k of keys) if (o[k] !== undefined && o[k] !== null) return o[k];
	return null;
}

export interface WorkspaceRef {
	public_id: string;
	name: string;
	role: Role;
	plan: string;
}

export function normWorkspace(raw: unknown): WorkspaceRef | null {
	const o = obj(raw);
	const id = str(first(o, 'public_id', 'id'));
	if (!id) return null;
	return { public_id: id, name: str(o.name) ?? 'Untitled', role: asRole(o.role) ?? 'viewer', plan: str(o.plan) ?? 'free' };
}

export interface Me {
	public_id: string;
	email: string;
	verified: boolean;
}

export function normMe(raw: unknown): Me | null {
	const o = obj(raw);
	const email = str(o.email);
	if (!email) return null;
	return { public_id: str(o.public_id) ?? '', email, verified: o.verified === true };
}

export type Scope = 'read' | 'write';

export interface KeyRow {
	id: string;
	name: string;
	prefix: string;
	scope: Scope;
	createdAt: number | null;
	lastUsedAt: number | null;
	revoked: boolean;
	/** the account public id that minted it (S3-5), when the server says */
	createdBy: string | null;
}

export function normKey(raw: unknown): KeyRow | null {
	const o = obj(raw);
	const id = str(first(o, 'public_id', 'id'));
	if (!id) return null;
	const scope = str(first(o, 'scope', 'permission'));
	return {
		id,
		name: str(o.name) ?? '',
		prefix: str(o.prefix) ?? '',
		// "admin" is the stored word, "write" the API/UI word (ruling 3)
		scope: scope === 'read' ? 'read' : 'write',
		createdAt: toMs(first(o, 'created_at_ms', 'created_at')),
		lastUsedAt: toMs(first(o, 'last_used_at_ms', 'last_used_at')),
		revoked: o.revoked === true || first(o, 'revoked_at_ms', 'revoked_at') !== null,
		createdBy: str(o.created_by),
	};
}

export interface MemberRow {
	accountId: string;
	email: string;
	role: Role;
	since: number | null;
}

export function normMember(raw: unknown): MemberRow | null {
	const o = obj(raw);
	// the plan nests `account: {public_id, email}`; the store's MemberInfo is flat (`account_public_id`, `email`)
	const a = o.account !== undefined ? obj(o.account) : o;
	const id = str(first(a, 'account_public_id', 'public_id', 'id'));
	const email = str(a.email);
	if (!id || !email) return null;
	return { accountId: id, email, role: asRole(o.role) ?? 'viewer', since: toMs(first(o, 'since_ms', 'since', 'created_at_ms', 'created_at')) };
}

export interface InviteRow {
	id: string;
	email: string;
	role: Role;
	expiresAt: number | null;
}

export function normInvite(raw: unknown): InviteRow | null {
	const o = obj(raw);
	const id = str(first(o, 'public_id', 'id'));
	const email = str(o.email);
	if (!id || !email) return null;
	return { id, email, role: asRole(o.role) ?? 'member', expiresAt: toMs(first(o, 'expires_at_ms', 'expires_at')) };
}

export interface SessionRow {
	id: string;
	current: boolean;
	createdAt: number | null;
	lastSeenAt: number | null;
	userAgent: string | null;
	ip: string | null;
}

export function normSession(raw: unknown): SessionRow | null {
	const o = obj(raw);
	const id = str(first(o, 'public_id', 'id'));
	if (!id) return null;
	return {
		id,
		current: o.current === true,
		createdAt: toMs(first(o, 'created_at_ms', 'created_at')),
		lastSeenAt: toMs(first(o, 'last_seen_at_ms', 'last_seen_at')),
		userAgent: str(o.user_agent),
		ip: str(o.ip),
	};
}

/** A short device name from a user agent ("Firefox on macOS"), or the raw string's start. */
export function deviceName(ua: string | null): string {
	if (!ua) return 'Unknown device';
	const browser = /Edg\//.test(ua) ? 'Edge' : /Firefox\//.test(ua) ? 'Firefox' : /Chrome\//.test(ua) ? 'Chrome' : /Safari\//.test(ua) ? 'Safari' : /curl\//.test(ua) ? 'curl' : null;
	const os = /iPhone|iPad/.test(ua) ? 'iOS' : /Android/.test(ua) ? 'Android' : /Mac OS X|Macintosh/.test(ua) ? 'macOS' : /Windows/.test(ua) ? 'Windows' : /Linux/.test(ua) ? 'Linux' : null;
	if (browser && os) return `${browser} on ${os}`;
	if (browser) return browser;
	return ua.length > 40 ? `${ua.slice(0, 40)}…` : ua;
}

export interface HandlerHealth {
	lastTestAt: number | null;
	lastTestStatus: string | null;
	lastTestLatencyMs: number | null;
	p50: number | null;
	p95: number | null;
}

export interface HandlerRow {
	name: string;
	url: string | null;
	/** "flag": wired by the operator with --handler, not editable here */
	source: string | null;
	health: HandlerHealth | null;
}

function num(v: unknown): number | null {
	return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

export function normHandler(raw: unknown): HandlerRow | null {
	const o = obj(raw);
	const name = str(o.name);
	if (!name) return null;
	const h = o.health === undefined || o.health === null ? null : obj(o.health);
	return {
		name,
		url: str(o.url),
		source: str(o.source),
		health: h && {
			lastTestAt: toMs(first(h, 'last_test_at_ms', 'last_test_at')),
			lastTestStatus: str(h.last_test_status),
			lastTestLatencyMs: num(h.last_test_latency_ms),
			p50: num(first(h, 'p50_ms', 'p50')),
			p95: num(first(h, 'p95_ms', 'p95')),
		},
	};
}

/** The in-process sample handler (ruling 5′): labelled "sample" everywhere, never a network call. */
export const SAMPLE_HANDLER = 'builtin:echo';
export function isSample(h: { name: string; url: string | null }): boolean {
	return h.name.startsWith('builtin:') || (h.url ?? '').startsWith('builtin:');
}

export interface TestResult {
	status: 'ok' | 'failed' | 'refused' | 'lost' | string;
	httpStatus: number | null;
	latencyMs: number | null;
	replyB64: string | null;
}

export function normTest(raw: unknown): TestResult {
	const o = obj(raw);
	return {
		status: str(o.status) ?? 'unknown',
		httpStatus: num(o.http_status),
		latencyMs: num(o.latency_ms),
		replyB64: str(first(o, 'reply_excerpt_b64', 'reply_b64')),
	};
}

export interface StepSpec {
	name: string;
	handler: string;
}

export interface WorkflowSpec {
	steps: StepSpec[];
	input_b64: string;
}

export interface WorkflowRow {
	id: string;
	name: string;
	currentVersion: number | null;
	updatedAt: number | null;
	lastRun: { runId: string; at: number | null; state: string | null } | null;
}

export function normWorkflow(raw: unknown): WorkflowRow | null {
	const o = obj(raw);
	const id = str(first(o, 'public_id', 'id'));
	if (!id) return null;
	const lr = o.last_run === undefined || o.last_run === null ? null : obj(o.last_run);
	const runId = lr ? str(first(lr, 'pubid', 'run_id', 'id')) : null;
	return {
		id,
		name: str(o.name) ?? '',
		currentVersion: num(first(o, 'current_version', 'version')),
		updatedAt: toMs(first(o, 'updated_at_ms', 'updated_at')),
		lastRun: lr && runId ? { runId, at: toMs(first(lr, 'at_ms', 'at', 'submitted_at')), state: str(lr.state) } : null,
	};
}

export function normSpec(raw: unknown): WorkflowSpec {
	const o = obj(raw);
	const steps = listOf(o.steps, 'steps')
		.map((s) => {
			const so = obj(s);
			return { name: str(so.name) ?? '', handler: str(so.handler) ?? '' };
		})
		.filter((s) => s.name || s.handler);
	return { steps, input_b64: typeof o.input_b64 === 'string' ? o.input_b64 : '' };
}

export interface WorkflowVersion {
	version: number;
	spec: WorkflowSpec;
	createdAt: number | null;
}

export interface WorkflowDetail extends WorkflowRow {
	versions: WorkflowVersion[];
}

export function normWorkflowDetail(raw: unknown): WorkflowDetail | null {
	const o = obj(raw);
	const base = normWorkflow(o.workflow !== undefined ? o.workflow : o);
	if (!base) return null;
	const versions = listOf(o.versions, 'versions')
		.map((v) => {
			const vo = obj(v);
			const n = num(vo.version);
			return n === null ? null : { version: n, spec: normSpec(vo.spec), createdAt: toMs(first(vo, 'created_at_ms', 'created_at')) };
		})
		.filter((v): v is WorkflowVersion => v !== null)
		.sort((a, b) => b.version - a.version);
	return { ...base, currentVersion: base.currentVersion ?? versions[0]?.version ?? null, versions };
}

export interface WorkflowRun {
	runId: string;
	submittedAt: number | null;
	state: string | null;
	finishedAt: number | null;
}

export function normWorkflowRun(raw: unknown): WorkflowRun | null {
	const o = obj(raw);
	const runId = str(first(o, 'run_id', 'pubid', 'id'));
	if (!runId) return null;
	return { runId, submittedAt: toMs(first(o, 'submitted_at_ms', 'submitted_at')), state: str(o.state), finishedAt: toMs(first(o, 'finished_at_ms', 'finished_at')) };
}

/** The dashboard link for an invite: the server's `invite_url` may be its own API path
 *  (`/invites/accept?token=dri_…`), which a browser cannot open, so the token is lifted out and put in the
 *  dashboard's fragment route, where it never reaches a server log or a mail prefetcher. Unrecognised: as given. */
export function inviteLink(inviteUrl: string, appUrl: string): string {
	const m = /[?&#]token=([A-Za-z0-9_-]{8,256})/.exec(inviteUrl);
	if (!m) return inviteUrl;
	return `${appUrl}#/accept-invite?token=${m[1]}`;
}

// --- usage ----------------------------------------------------------------------------------------------------

export interface Meter {
	id: string;
	label: string;
	used: number | null;
	cap: number | null;
	/** 0–100 of the cap, null when either number is missing */
	pct: number | null;
	level: 'ok' | 'warn' | 'full' | 'unknown';
	note: string;
}

const METERS: { used: string; limit: string; label: string; note: string }[] = [
	{ used: 'jobs_this_minute', limit: 'jobs_per_min', label: 'Jobs this minute', note: 'Submits and Run now share one per-workspace bucket.' },
	{ used: 'live_runs', limit: 'live_runs', label: 'Unfinished runs', note: 'Accepted and not yet finished.' },
	{ used: 'in_flight', limit: 'in_flight', label: 'Handler calls in flight', note: 'Beyond this, steps wait their turn; nothing is refused.' },
	{ used: 'handlers', limit: 'handlers', label: 'Handlers', note: 'Registered in this workspace.' },
	{ used: 'api_keys', limit: 'api_keys', label: 'API keys', note: 'Live keys; revoked keys do not count.' },
	{ used: 'members', limit: 'members', label: 'Members', note: 'The owner included; pending invites count.' },
	{ used: 'workflows', limit: 'workflows', label: 'Workflows', note: 'Archived workflows do not count.' },
	{ used: 'test_calls_hour', limit: 'test_calls_per_hour', label: 'Test calls this hour', note: 'Handler test calls, rolling hour.' },
];

export function meters(usage: unknown): Meter[] {
	const u = obj(usage);
	const used = obj(u.used);
	const limits = obj(u.limits);
	return METERS.map((m) => {
		const a = num(used[m.used]);
		const c = num(limits[m.limit]);
		const pct = a !== null && c !== null && c > 0 ? Math.min(100, (a / c) * 100) : null;
		const level: Meter['level'] = pct === null ? 'unknown' : a !== null && c !== null && a >= c ? 'full' : pct >= 80 ? 'warn' : 'ok';
		return { id: m.limit, label: m.label, used: a, cap: c, pct, level, note: m.note };
	});
}

export interface Allowance {
	id: string;
	label: string;
	value: number | null;
	unit: 'bytes' | 'days' | 'count';
}

export function allowances(usage: unknown): Allowance[] {
	const limits = obj(obj(usage).limits);
	return [
		{ id: 'max_input_bytes', label: 'Input per job', value: num(limits.max_input_bytes), unit: 'bytes' as const },
		{ id: 'retained_bytes', label: 'Retained run data', value: num(limits.retained_bytes), unit: 'bytes' as const },
		{ id: 'retention_days', label: 'Finished runs kept', value: num(limits.retention_days), unit: 'days' as const },
		{ id: 'workspaces_per_account', label: 'Workspaces per account', value: num(limits.workspaces_per_account), unit: 'count' as const },
	].filter((a) => a.value !== null);
}

// --- onboarding -----------------------------------------------------------------------------------------------

export type StepId = 'api_key' | 'handler' | 'test_call' | 'first_run';
/** The checklist's order on the page: key, handler (or the sample), a test call that answers ok, the first job. */
export const STEP_ORDER: readonly StepId[] = ['api_key', 'handler', 'test_call', 'first_run'];

export interface OnboardingStep {
	id: StepId;
	done: boolean;
	detail: string | null;
}

/** The server's checklist (`GET …/onboarding`), in the page's order; a step it does not list is not done. */
export function normOnboarding(body: unknown): OnboardingStep[] {
	const byId = new Map<string, Obj>();
	for (const s of listOf(body, 'steps')) {
		const o = obj(s);
		const id = str(o.id);
		if (id) byId.set(id, o);
	}
	return STEP_ORDER.map((id) => {
		const o = byId.get(id);
		return { id, done: o?.done === true, detail: o ? str(o.detail) : null };
	});
}

export interface OnboardingFacts {
	/** null: the list could not be read, so the step cannot be proved */
	keys: KeyRow[] | null;
	handlers: HandlerRow[] | null;
	/** terminal runs in this workspace (done, failed or cancelled), from GET /diagnostics */
	finishedRuns: number | null;
	/** this tab saw a test call answer ok (the server's own answer, not a flag) */
	testOkSeen: boolean;
}

/** The checklist from facts the page can read, for servers without the onboarding route yet. A step is done only
 *  when a fact proves it. */
export function deriveOnboarding(f: OnboardingFacts): OnboardingStep[] {
	const liveKeys = f.keys?.filter((k) => !k.revoked) ?? null;
	const tested = f.handlers?.find((h) => h.health?.lastTestStatus === 'ok') ?? null;
	const steps: Record<StepId, OnboardingStep> = {
		api_key: { id: 'api_key', done: !!liveKeys && liveKeys.length > 0, detail: liveKeys && liveKeys.length > 0 ? liveKeys[0]!.name || liveKeys[0]!.prefix : null },
		handler: { id: 'handler', done: !!f.handlers && f.handlers.length > 0, detail: f.handlers?.[0]?.name ?? null },
		test_call: { id: 'test_call', done: !!tested || f.testOkSeen, detail: tested?.name ?? null },
		first_run: { id: 'first_run', done: f.finishedRuns !== null && f.finishedRuns > 0, detail: null },
	};
	return STEP_ORDER.map((id) => steps[id]);
}

/** Terminal runs from a GET /diagnostics `states` map. */
export function finishedFromStates(states: unknown): number | null {
	const s = obj(states);
	if (Object.keys(s).length === 0 && states === undefined) return null;
	let n = 0;
	for (const k of ['done', 'failed', 'cancelled']) n += num(s[k]) ?? 0;
	return n;
}

export function onboardingDone(steps: OnboardingStep[]): boolean {
	return steps.length > 0 && steps.every((s) => s.done);
}
