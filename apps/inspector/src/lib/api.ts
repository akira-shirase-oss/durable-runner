// The API base is the server origin minus the /inspector mount — the UI is
// served same-origin by durable-server, so plain relative calls work.
export interface RunItem {
	id: number;
	key: string;
	state: string;
	steps: number;
	finished_at: number | null;
}

const API = '';

export interface RunPage {
	runs: RunItem[];
	/** Cursor for the next (older) page; null on the last page. */
	next_before: number | null;
}

function bearer(apiKey: string): Record<string, string> {
	return apiKey ? { authorization: `Bearer ${apiKey}` } : {};
}

// `GET /jobs?order=desc` pages newest first (at most 1000 per page);
// `before` continues from the previous page's `next_before`. Reads need a
// key — the server's read-only key (DURABLE_READ_KEY) or its API key —
// unless the server runs with --open-reads on loopback.
export async function listRuns(
	apiKey: string,
	before: number | null = null,
	limit = 50,
): Promise<RunPage> {
	const q = new URLSearchParams({ order: 'desc', limit: String(limit) });
	if (before !== null) q.set('before', String(before));
	const r = await fetch(`${API}/jobs?${q}`, { headers: bearer(apiKey) });
	if (r.status === 401) throw new Error('viewing runs needs the server read key or API key (enter it above)');
	if (!r.ok) throw new Error(`list failed: ${r.status}`);
	return (await r.json()) as RunPage;
}

// The key lives only in this tab's sessionStorage, never in the build.
// The read key views; cancel (like every mutating route) needs the API key.
const KEY_SLOT = 'durable-api-key';

export function loadApiKey(): string {
	try {
		return sessionStorage.getItem(KEY_SLOT) ?? '';
	} catch {
		return '';
	}
}

export function saveApiKey(key: string): void {
	try {
		if (key) sessionStorage.setItem(KEY_SLOT, key);
		else sessionStorage.removeItem(KEY_SLOT);
	} catch {
		// storage unavailable: the key lasts only for this page view
	}
}

export async function cancelRun(id: number, apiKey: string): Promise<void> {
	const r = await fetch(`${API}/jobs/${id}/cancel`, { method: 'POST', headers: bearer(apiKey) });
	if (r.status === 401) throw new Error('cancel needs the server API key (enter it above)');
	if (r.status === 403) throw new Error('this key may only read; cancel needs the server API key');
	if (!r.ok) throw new Error(`cancel failed: ${r.status}`);
}
