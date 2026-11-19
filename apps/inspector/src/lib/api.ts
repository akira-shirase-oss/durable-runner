// The API base is the server origin minus the /inspector mount — the UI is
// served same-origin by durable-server, so plain relative calls work.
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

async function req<T>(path: string, key: string, init: RequestInit = {}): Promise<T> {
	const r = await fetch(`${API}${path}`, {
		...init,
		headers: { ...(key ? { authorization: `Bearer ${key}` } : {}), ...(init.headers ?? {}) },
	});
	if (r.status === 401) throw new Error('needs a key: the read key for reads, the API key for everything else');
	if (r.status === 403) throw new Error('this key may only read; this operation needs the API key');
	if (!r.ok) {
		let detail = `${r.status}`;
		try {
			const body = (await r.json()) as { error?: string };
			if (body.error) detail = body.error;
		} catch {
			/* not JSON */
		}
		throw new Error(detail);
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
export async function health(): Promise<{ ok: boolean }> {
	return req('/health', '');
}

export async function ready(): Promise<{ ready: boolean; code: string | null }> {
	return req('/ready', '');
}

export async function listRuns(
	key: string,
	order: 'asc' | 'desc',
	cursor: number | null = null,
	limit = 50,
): Promise<RunPage> {
	const q = new URLSearchParams({ order, limit: String(limit) });
	if (cursor !== null) q.set(order === 'desc' ? 'before' : 'after', String(cursor));
	return req(`/jobs?${q}`, key);
}

export async function inspect(key: string, id: number): Promise<RunItem> {
	return req(`/jobs/${id}`, key);
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

export async function diagnostics(apiKey: string): Promise<Diag> {
	return req('/admin/diagnostics', apiKey);
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
