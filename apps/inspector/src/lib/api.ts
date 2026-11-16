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
	next_after: number | null;
}

// `GET /jobs` is paged (ids ascending, at most 1000 per page).
export async function listRuns(after: number | null = null, limit = 100): Promise<RunPage> {
	const q = new URLSearchParams({ limit: String(limit) });
	if (after !== null) q.set('after', String(after));
	const r = await fetch(`${API}/jobs?${q}`);
	if (!r.ok) throw new Error(`list failed: ${r.status}`);
	return (await r.json()) as RunPage;
}

// Reads are open; cancel (like every mutating route) needs the server's
// API key. It lives only in this tab's sessionStorage, never in the build.
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
	const headers: Record<string, string> = apiKey ? { authorization: `Bearer ${apiKey}` } : {};
	const r = await fetch(`${API}/jobs/${id}/cancel`, { method: 'POST', headers });
	if (r.status === 401) throw new Error('cancel needs the server API key (enter it above)');
	if (!r.ok) throw new Error(`cancel failed: ${r.status}`);
}
